import threading
import unittest
import uuid
import os
from unittest.mock import patch
from pathlib import Path

_db_url = os.environ.get("DATABASE_URL", "")
_db_path = _db_url.removeprefix("sqlite:///") if _db_url.startswith("sqlite:///") else ""
if (
    os.environ.get("HUBFLO_MC_ISOLATED") != "1"
    or not _db_url.startswith("sqlite:///")
    or not _db_path
    or Path(_db_path).name in ("hubflo.db", "")
    or not (Path(_db_path).parent.name.startswith("hubflo-mc-") or
            Path(_db_path).parent.name.startswith("hubflo-c3-") or
            Path(_db_path).parent.name.startswith("hubflo-c4-") or
            Path(_db_path).parent.name.startswith("hubflo-c5-") or
            Path(_db_path).parent.name.startswith("hubflo-c6-"))
):
    raise RuntimeError("multi-context tests require an explicitly isolated temporary SQLite DATABASE_URL")

import storage
import app as hubflo_app


class MultiContextIdentityTests(unittest.TestCase):
    """Runs only against the test runner's isolated DATABASE_URL; never drops it."""
    def setUp(self):
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        sender_patch = patch.object(
            hubflo_app, "send_whatsapp_text", return_value=(True, {})
        )
        sender_patch.start()
        self.addCleanup(sender_patch.stop)
        self.sender = "mc-" + uuid.uuid4().hex
        with storage.SessionLocal() as s:
            self.user = storage.User(client_id=7, wa_id=self.sender, role="pm",
                                     project_code="FL", active=True)
            s.add(self.user); s.commit(); s.refresh(self.user)

    def add_memberships(self):
        with storage.SessionLocal() as s:
            s.query(storage.SenderMembership).filter_by(user_id=self.user.id).delete()
            s.add_all([
                storage.SenderMembership(user_id=self.user.id, context_kind="client", client_id=7, context_label="Florida", role="pm", project_code="FL", authority_basis="fixture"),
                storage.SenderMembership(user_id=self.user.id, context_kind="client", client_id=8, context_label="SA", role="sub", project_code="SA", authority_basis="fixture"),
            ]); s.commit()

    def ingress(self, text, event):
        return self.client.post("/webhook", json={"entry":[{"changes":[{"value":{
            "metadata":{"phone_number_id":"mc-channel"}, "contacts":[{"wa_id":self.sender}],
            "messages":[{"id":event,"from":self.sender,"type":"text","text":{"body":text}}]
        }}]}]})

    def test_mc01_legacy_single_client(self):
        self.assertEqual(storage.resolve_sender_context(self.sender)["membership"]["client_id"], 7)
    def test_mc02_no_selection_fails_closed(self):
        self.add_memberships()
        with storage.SessionLocal() as s:
            before = s.query(storage.Task).count()
        response = self.ingress("hello", "mc02")
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.Task).count(), before)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(storage.resolve_sender_context(self.sender)["status"], "selection_required")
    def test_mc03_select_client(self):
        self.add_memberships(); self.ingress("Florida", "mc03"); self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["client_id"], 7)
    def test_mc04_persist_selection(self):
        self.add_memberships(); self.ingress("Florida", "mc04"); self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["client_id"],7)
    def test_mc05_switch_client(self):
        self.add_memberships(); self.ingress("Florida", "mc05a"); self.ingress("SA", "mc05b"); self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["client_id"],8)
    def test_mc06_hub_is_platform(self):
        self.add_memberships()
        with storage.SessionLocal() as s: s.add(storage.SenderMembership(user_id=self.user.id,context_kind="platform",client_id=None,context_label="Hub",role="owner",authority_basis="fixture")); s.commit()
        self.ingress("Hub", "mc06a"); m=storage.resolve_sender_context(self.sender,"mc-channel")["membership"]; self.assertEqual(m["context_kind"],"platform"); self.assertIsNone(m["client_id"])
        with storage.SessionLocal() as s: before = s.query(storage.Task).count()
        self.ingress("create task", "mc06b")
        with storage.SessionLocal() as s: self.assertEqual(s.query(storage.Task).count(), before)
        self.assertEqual(storage.client_id_for_sender(self.sender), None)
    def test_mc07_unauthorized_explicit_switch_denied(self):
        self.add_memberships(); self.ingress("Florida", "mc07a")
        with storage.SessionLocal() as s: before = s.query(storage.Task).count()
        response = self.ingress("switch to Texas", "mc07b")
        with storage.SessionLocal() as s: self.assertEqual(s.query(storage.Task).count(), before)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["client_id"],7)
    def test_mc08_revocation(self):
        self.add_memberships(); m=storage.resolve_sender_context(self.sender,text="Florida")["membership"]; storage.commit_context_selection(self.sender,"wa",m)
        with storage.SessionLocal() as s: s.query(storage.SenderMembership).filter_by(user_id=self.user.id, client_id=7).update({"active":False}); s.commit()
        self.assertEqual(storage.resolve_sender_context(self.sender,"wa")["membership"]["client_id"],8)
    def test_mc09_state_scope_isolation(self):
        self.add_memberships(); self.ingress("Florida", "mc09a")
        state = storage.save_pending_conversation_state({"sender":self.sender,"client_id":7,"project_code":"FL","state_kind":"await","continuation_key":"mc09"})
        self.assertIsNotNone(state)
        self.ingress("SA", "mc09b")
        self.assertIsNone(storage.get_pending_conversation_state(self.sender,8,"SA"))
        self.ingress("Florida", "mc09c")
        self.assertIsNotNone(storage.get_pending_conversation_state(self.sender,7,"FL","mc09"))
    def test_mc10_selector_precedes_await(self):
        self.add_memberships(); self.ingress("Florida", "mc10a")
        task = storage.create_task(self.sender, "[await:item]\nFlorida pending", tag="order", project_code="FL")
        storage.save_pending_conversation_state({"sender":self.sender,"client_id":7,"project_code":"FL","state_kind":"await","continuation_key":f"business-state:{task['id']}","expected_field":"item"})
        self.ingress("SA", "mc10b")
        self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["client_id"], 8)
        self.assertIsNotNone(storage.get_pending_conversation_state(self.sender,7,"FL",f"business-state:{task['id']}"))
    def test_mc11_role_project_selected_only(self):
        self.add_memberships(); m=storage.resolve_sender_context(self.sender,text="SA")["membership"]; storage.set_effective_membership(m); self.assertEqual(storage.get_user_role(self.sender)["role"],"sub"); storage.clear_effective_membership()
    def test_mc12_same_labels_are_context_bound(self):
        self.add_memberships(); self.ingress("Florida", "mc12a")
        storage.create_task(self.sender, "same protected label", tag="task", project_code="FL")
        self.ingress("SA", "mc12b")
        storage.create_task(self.sender, "same protected label", tag="task", project_code="SA")
        self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["project_code"], "SA")
        self.ingress("Florida", "mc12c")
        self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["project_code"], "FL")
    def test_mc13a_selection_replay_binding(self):
        self.add_memberships(); self.ingress("Florida", "e1")
        self.ingress("SA", "e2")
        self.ingress("Florida", "e1")
        self.assertEqual(storage.resolve_sender_context(self.sender,"mc-channel")["membership"]["client_id"],8)
    def test_mc13b_business_replay_not_retargeted(self):
        self.add_memberships(); self.ingress("Florida", "mc13b-select")
        with storage.SessionLocal() as s: before = s.query(storage.Task).count()
        self.ingress("create task for me", "e3")
        with storage.SessionLocal() as s:
            after_first = s.query(storage.Task).count()
            claim = s.query(storage.MultiContextInboundClaim).filter_by(event_id="e3").one()
            self.assertEqual(claim.client_id, 7)
            self.assertEqual(claim.processing_state, "released")
        self.assertGreater(after_first, before)
        self.ingress("SA", "mc13b-switch")
        self.ingress("create task for me", "e3")
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.Task).count(), after_first)
            self.assertEqual(s.query(storage.MultiContextInboundClaim).filter_by(event_id="e3").one().client_id, 7)
    def test_mc14_cached_context_cannot_restore(self):
        self.add_memberships(); m=storage.resolve_sender_context(self.sender,text="Florida")["membership"]; storage.commit_context_selection(self.sender,"wa",m)
        with storage.SessionLocal() as s: s.query(storage.SenderMembership).filter_by(user_id=self.user.id, client_id=7).update({"active":False}); s.commit()
        with storage.SessionLocal() as s: s.query(storage.SenderMembership).filter_by(user_id=self.user.id, client_id=7).update({"active":True}); s.commit()
        self.assertEqual(storage.resolve_sender_context(self.sender,"wa")["status"], "selection_required")
        storage.set_effective_membership(m); storage.clear_effective_membership()
        self.assertIsNone(storage.get_user_role(self.sender))
    def test_mc15_migration_idempotent(self):
        storage.init_db(); storage.init_db();
        with storage.SessionLocal() as s: self.assertEqual(s.query(storage.SenderMembership).filter_by(user_id=self.user.id).count(),1)
        bare = "bare-" + uuid.uuid4().hex
        with storage.SessionLocal() as s:
            row = storage.User(wa_id=bare, name="Bare identity", active=True)
            s.add(row); s.commit(); s.refresh(row); bare_id = row.id
        storage.init_db()
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.SenderMembership).filter_by(user_id=bare_id).count(), 0)
    def test_mc16_unknown_no_default_authority(self): self.assertIsNone(storage.client_id_for_sender("unknown-"+uuid.uuid4().hex))
    def test_mc17_competing_claims_one_winner(self):
        m=storage.resolve_sender_context(self.sender)["membership"]; results=[]
        def claim(): results.append(storage.claim_multi_context_inbound("race",self.sender,"wa","resolved",m)["status"])
        ts=[threading.Thread(target=claim) for _ in range(2)]; [t.start() for t in ts]; [t.join() for t in ts]; self.assertEqual(results.count("claimed"),1)
    def test_mc17_revoke_wins_claim_race(self):
        self.add_memberships(); m=storage.resolve_sender_context(self.sender,text="Florida")["membership"]
        revoked = threading.Event(); result = []
        def revoke():
            with storage.SessionLocal() as s:
                s.query(storage.SenderMembership).filter_by(user_id=self.user.id, id=m["id"]).update({"active":False}); s.commit()
            revoked.set()
        def claim():
            revoked.wait()
            result.append(storage.claim_multi_context_inbound("revoke-race", self.sender, "wa", "resolved", m)["status"])
        ts = [threading.Thread(target=revoke), threading.Thread(target=claim)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(result, ["stale-or-unauthorized"])
    @unittest.skip("Agent Layer real sender ingress not yet wired")
    def test_mc18_agent_ingress_limitation_is_explicit(self):
        pass


if __name__ == "__main__": unittest.main()
