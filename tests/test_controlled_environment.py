import os
from pathlib import Path
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

_url = os.environ.get("DATABASE_URL", "")
_path = _url.removeprefix("sqlite:///") if _url.startswith("sqlite:///") else ""
if (os.environ.get("HUBFLO_CONTROLLED_ISOLATED") != "1" or not _path or
        Path(_path).name in ("", "hubflo.db")):
    raise RuntimeError("controlled-environment tests require an isolated SQLite DATABASE_URL")

import storage
import app as hubflo_app
from controlled_environment import (
    CHANNEL, FIXTURE_TAG, PROJECTS, SA_PROJECTS, SENDERS, setup_controlled_fixture,
)
from agent_layer.contracts import Principal, Scope
from agent_layer.persistence import AgentRepository
from agent_layer.models import AgentConfiguration, AgentEntitlement


class ControlledEnvironmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = setup_controlled_fixture()
        cls.client = hubflo_app.app.test_client()
        cls.florida = cls.state["client_ids"]["Florida"]
        cls.sa = cls.state["client_ids"]["SA"]

    def membership(self, name):
        with storage.SessionLocal() as s:
            user = s.query(storage.User).filter_by(wa_id=SENDERS[name]).one()
            return s.query(storage.SenderMembership).filter_by(user_id=user.id).all()

    def test_01_neville_is_one_user_with_exact_memberships(self):
        with storage.SessionLocal() as s:
            users = s.query(storage.User).filter_by(wa_id=SENDERS["Neville"]).all()
            self.assertEqual(len(users), 1)
            memberships = s.query(storage.SenderMembership).filter_by(user_id=users[0].id, active=True).all()
            self.assertEqual({(m.context_label, m.context_kind, m.client_id) for m in memberships},
                             {("Hub", "platform", None), ("Florida", "client", self.florida), ("SA", "client", self.sa)})

    def test_02_hub_is_platform_scope(self):
        result = storage.resolve_sender_context(SENDERS["Neville"], CHANNEL, "Hub")
        self.assertEqual(result["membership"]["context_kind"], "platform")
        self.assertIsNone(result["membership"]["client_id"])

    def test_03_clients_are_distinct(self):
        self.assertNotEqual(self.florida, self.sa)

    def test_04_dolan_only_florida(self):
        self.assertEqual({m.context_label for m in self.membership("Dolan")}, {"Florida"})

    def test_05_paul_only_sa(self):
        self.assertEqual({m.context_label for m in self.membership("Paul")}, {"SA"})

    def test_06_unauthorized_selection_denied(self):
        result = storage.resolve_sender_context(SENDERS["Dolan"], CHANNEL, "switch to SA")
        self.assertEqual(result["status"], "selection_denied")

    def test_07_neville_switch_uses_current_selection_without_new_user(self):
        sender = SENDERS["Neville"]
        for label, cid in (("Hub", None), ("Florida", self.florida), ("SA", self.sa)):
            result = storage.resolve_sender_context(sender, CHANNEL, label)
            storage.commit_context_selection(sender, CHANNEL, result["membership"])
            current = storage.resolve_sender_context(sender, CHANNEL)
            self.assertEqual(current["membership"]["context_label"], label)
            self.assertEqual(current["membership"]["client_id"], cid)
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.User).filter_by(wa_id=sender).count(), 1)
            self.assertEqual(s.query(storage.CurrentContextSelection).filter_by(sender=sender, channel_id=CHANNEL).count(), 1)

    def test_08_florida_records_not_in_sa(self):
        result = storage.resolve_sender_context(SENDERS["Dolan"], CHANNEL, "Florida")
        storage.commit_context_selection(SENDERS["Dolan"], CHANNEL, result["membership"])
        self.assertEqual(storage.resolve_sender_context(SENDERS["Dolan"], CHANNEL)["status"], "resolved")
        context = storage.get_personal_responsibilities(SENDERS["Dolan"], self.florida, ["Site 1"])
        self.assertTrue(context["tasks"])
        self.assertTrue(all(row["project_code"] == "Site 1" for row in context["tasks"]))
        self.assertFalse(any("Branch" in row["text"] for row in context["tasks"]))

    def test_09_sa_records_not_in_florida(self):
        result = storage.resolve_sender_context(SENDERS["Paul"], CHANNEL, "SA")
        storage.commit_context_selection(SENDERS["Paul"], CHANNEL, result["membership"])
        self.assertEqual(storage.resolve_sender_context(SENDERS["Paul"], CHANNEL)["status"], "resolved")
        context = storage.get_personal_responsibilities(SENDERS["Paul"], self.sa, ["Branch 1"])
        self.assertTrue(context["tasks"])
        self.assertTrue(all(row["project_code"] == "Branch 1" for row in context["tasks"]))
        self.assertFalse(any("Site" in row["text"] for row in context["tasks"]))

    def test_10_hub_does_not_inherit_client_data(self):
        captured = self._webhook_principals("Neville", ("Hub",))[0]
        self.assertIsNone(captured.scope.client_id)
        self.assertIsNone(captured.scope.project_code)
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.Task).filter_by(client_id=None).count(), 0)

    def test_11_agent_principal_follows_membership_scope(self):
        principals = self._webhook_principals("Neville", ("Florida", "SA"))
        self.assertEqual([p.principal_id for p in principals], [principals[0].principal_id] * 2)
        self.assertEqual([p.scope.client_id for p in principals], [self.florida, self.sa])
        self.assertEqual([p.scope.project_code for p in principals], [None, None])

    def test_12_agent_grants_and_entitlements_are_client_bound(self):
        repo = AgentRepository()
        dolan = "user:%s" % self.state["user_ids"]["Dolan"]
        paul = "user:%s" % self.state["user_ids"]["Paul"]
        neville = "user:%s" % self.state["user_ids"]["Neville"]
        self.assertIsNotNone(repo.current_grant(dolan, "manager_pa.assist", self.florida, None))
        self.assertIsNone(repo.current_grant(dolan, "manager_pa.assist", self.sa, None))
        self.assertIsNotNone(repo.current_grant(paul, "manager_pa.assist", self.sa, None))
        self.assertIsNone(repo.current_grant(paul, "manager_pa.assist", self.florida, None))
        self.assertIsNotNone(repo.entitlement(self.florida, "capability", "manager_pa.assist", dolan))
        self.assertIsNotNone(repo.entitlement(self.sa, "capability", "manager_pa.assist", paul))
        self.assertIsNotNone(repo.entitlement(self.florida, "capability", "manager_pa.assist", neville))
        self.assertIsNotNone(repo.entitlement(self.sa, "capability", "manager_pa.assist", neville))
        with storage.SessionLocal() as s:
            configurations = s.query(AgentConfiguration).filter(
                AgentConfiguration.proposer == FIXTURE_TAG).all()
            self.assertEqual({row.client_id for row in configurations}, {None, self.florida, self.sa})
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(AgentEntitlement).filter(AgentEntitlement.client_id == self.sa, AgentEntitlement.authority_basis == FIXTURE_TAG).count(), 2)

    def test_13_florida_authoritative_facts_persisted(self):
        with storage.SessionLocal() as s:
            tasks = s.query(storage.Task).filter_by(client_id=self.florida, project_code="Site 1").all()
            self.assertTrue(any(t.due_date > datetime.utcnow() for t in tasks))
            overdue = [t for t in tasks if t.due_date and t.due_date < datetime.utcnow()]
            self.assertGreaterEqual(len(overdue), 5)
            self.assertTrue(any(t.started_at for t in tasks)); self.assertTrue(any(not t.started_at for t in tasks))
            self.assertTrue(any(t.overrun_days and t.overrun_days > 0 for t in tasks))
            change = next(t for t in tasks if t.tag == "change")
            self.assertTrue(change.approval_required and change.approved_at and change.cost >= 1000)
            self.assertTrue(any(t.tag == "order" and t.order_state == "ordered" for t in tasks))
            self.assertTrue(any(t.tag == "delivery" and t.order_state == "delivered" for t in tasks))
            self.assertTrue(any(t.is_rework for t in tasks))
            self.assertEqual(s.query(storage.TaskGroup).count(), 3)
            self.assertEqual(s.query(storage.DelayLog).filter_by(client_id=self.florida, project_code="Site 1").count(), 2)
            self.assertEqual(s.query(storage.Inspection).filter_by(client_id=self.florida, project_code="Site 1").count(), 1)
            self.assertEqual(s.query(storage.Meeting).filter_by(client_id=self.florida, project_code="Site 1").count(), 1)
            self.assertTrue(any(i.supplier_name == "Materials1" for i in s.query(storage.StockItem).filter_by(client_id=self.florida, project_code="Site 1").all()))
            self.assertTrue(any(t.status == "completed" and t.subcontractor_name == "Electrical1" for t in tasks))

    def test_14_sa_authoritative_facts_persisted(self):
        with storage.SessionLocal() as s:
            tasks = s.query(storage.Task).filter_by(client_id=self.sa, project_code="Branch 1").all()
            self.assertTrue({t.subtype for t in tasks} >= {"customer_commitment", "workshop_commitment", "supplier_commitment", "supplier_performance", "future_activity"})
            self.assertTrue(any(t.status == "completed" for t in tasks))
            self.assertTrue(any(t.approval_required and t.approved_at for t in tasks))
            self.assertTrue(any(t.tag == "urgent" for t in tasks))
            self.assertEqual(s.query(storage.DelayLog).filter_by(client_id=self.sa, project_code="Branch 1").count(), 2)
            self.assertTrue(any(t.subcontractor_name == "Parts Supplier1" and t.status == "completed" for t in tasks))
            self.assertTrue(any(t.due_date > datetime.utcnow() for t in tasks))
            self.assertTrue(any(i.supplier_name == "Tyre Supplier1" for i in s.query(storage.StockItem).filter_by(client_id=self.sa, project_code="Branch 1").all()))

    def test_15_setup_is_idempotent(self):
        before = {model.__tablename__: self._controlled_count(model) for model in (storage.User, storage.SenderMembership, storage.Task, storage.Inspection, storage.Meeting, storage.StockItem)}
        with storage.SessionLocal() as s:
            s.add(storage.Task(client_id=987654321, project_code="unrelated-high", sender="unrelated", text="keep"))
            s.commit()
        again = setup_controlled_fixture()
        after = {model.__tablename__: self._controlled_count(model) for model in (storage.User, storage.SenderMembership, storage.Task, storage.Inspection, storage.Meeting, storage.StockItem)}
        self.assertEqual(again["client_ids"], self.state["client_ids"])
        self.assertEqual(before, after)

    def test_16_reset_is_fixture_scoped(self):
        with storage.SessionLocal() as s:
            unrelated = storage.Task(client_id=987654, project_code="unrelated", sender="unrelated", text="keep")
            s.add(unrelated); s.commit()
        setup_controlled_fixture()
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.Task).filter_by(client_id=987654, project_code="unrelated").count(), 1)

    @staticmethod
    def _controlled_count(model):
        with storage.SessionLocal() as s:
            if model is storage.User:
                return s.query(model).filter(model.wa_id.in_(tuple(SENDERS.values()))).count()
            if model is storage.SenderMembership:
                ids = [v for v in s.query(storage.User.id).filter(storage.User.wa_id.in_(tuple(SENDERS.values()))).all()]
                return s.query(model).filter(model.user_id.in_([v[0] for v in ids])).count()
            return s.query(model).filter(model.client_id.in_((ControlledEnvironmentTests.florida, ControlledEnvironmentTests.sa))).count()

    def _webhook_principals(self, name, labels):
        captured = []

        class ControlledOrchestrator:
            def handle(inner, text, principal, membership, event_id):
                captured.append(principal)
                return {"status": "degraded", "code": "controlled-test"}

        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", ControlledOrchestrator()), \
             patch.object(hubflo_app, "send_whatsapp_text", return_value=(True, {})):
            for index, label in enumerate(labels):
                channel = f"{CHANNEL}-{name}-{index}"
                event_prefix = label.lower().replace(" ", "-")
                self._webhook(label, f"ctx-{name}-{event_prefix}", channel)
                self._webhook("florp blim", f"agent-{name}-{event_prefix}", channel)
        return captured

    def _webhook(self, text, event_id, channel=CHANNEL):
        return self.client.post("/webhook", json={"entry": [{"changes": [{"value": {
            "metadata": {"phone_number_id": channel},
            "contacts": [{"wa_id": SENDERS["Neville"]}],
            "messages": [{"id": event_id, "from": SENDERS["Neville"], "type": "text",
                          "text": {"body": text}}]
        }}]}]})


if __name__ == "__main__":
    unittest.main()
