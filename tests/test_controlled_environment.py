import os
from pathlib import Path
import unittest

_url = os.environ.get("DATABASE_URL", "")
_path = _url.removeprefix("sqlite:///") if _url.startswith("sqlite:///") else ""
if (os.environ.get("HUBFLO_CONTROLLED_ISOLATED") != "1" or not _path or
        Path(_path).name in ("", "hubflo.db")):
    raise RuntimeError("controlled-environment tests require an isolated SQLite DATABASE_URL")

import storage
from controlled_environment import (
    CHANNEL, FIXTURE_TAG, PROJECTS, SA_PROJECTS, SENDERS, setup_controlled_fixture,
)
from agent_layer.contracts import Principal, Scope
from agent_layer.persistence import AgentRepository
from agent_layer.models import AgentAuthorityGrant, AgentEntitlement


class ControlledEnvironmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = setup_controlled_fixture()
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
        with storage.SessionLocal() as s:
            self.assertGreater(s.query(storage.Task).filter_by(client_id=self.florida).count(), 0)
            self.assertEqual(s.query(storage.Task).filter_by(client_id=self.sa, project_code="Site 1").count(), 0)

    def test_09_sa_records_not_in_florida(self):
        with storage.SessionLocal() as s:
            self.assertGreater(s.query(storage.Task).filter_by(client_id=self.sa).count(), 0)
            self.assertEqual(s.query(storage.Task).filter_by(client_id=self.florida, project_code="Branch 1").count(), 0)

    def test_10_hub_does_not_inherit_client_data(self):
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.Task).filter_by(client_id=None).count(), 0)

    def test_11_agent_principal_follows_membership_scope(self):
        repo = AgentRepository()
        for label, cid in (("Florida", self.florida), ("SA", self.sa)):
            resolved = storage.resolve_sender_context(SENDERS["Neville"], CHANNEL, label)["membership"]
            principal = Principal("user:%s" % resolved["user_id"], "user", Scope(cid))
            self.assertIsNotNone(repo.current_grant(principal.principal_id, "manager_pa.assist", cid, None))

    def test_12_agent_grants_and_entitlements_are_client_bound(self):
        repo = AgentRepository()
        florida_grant = repo.current_grant("user:%s" % self.state["user_ids"]["Dolan"], "manager_pa.assist", self.florida, None)
        self.assertIsNotNone(florida_grant)
        self.assertIsNone(repo.current_grant("user:%s" % self.state["user_ids"]["Dolan"], "manager_pa.assist", self.sa, None))
        self.assertIsNotNone(repo.entitlement(self.florida, "capability", "manager_pa.assist",
                                              "user:%s" % self.state["user_ids"]["Dolan"]))
        self.assertIsNone(repo.entitlement(self.sa + 100000, "capability", "manager_pa.assist"))
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(AgentEntitlement).filter(AgentEntitlement.client_id == self.sa, AgentEntitlement.authority_basis == FIXTURE_TAG).count(), 2)

    def test_13_florida_authoritative_facts_persisted(self):
        with storage.SessionLocal() as s:
            self.assertGreater(s.query(storage.Task).filter_by(client_id=self.florida).count(), 0)
            self.assertGreater(s.query(storage.Inspection).filter_by(client_id=self.florida).count(), 0)
            self.assertGreater(s.query(storage.Meeting).filter_by(client_id=self.florida).count(), 0)
            self.assertGreater(s.query(storage.StockItem).filter_by(client_id=self.florida).count(), 0)

    def test_14_sa_authoritative_facts_persisted(self):
        with storage.SessionLocal() as s:
            self.assertGreater(s.query(storage.Task).filter_by(client_id=self.sa).count(), 0)
            self.assertGreater(s.query(storage.Inspection).filter_by(client_id=self.sa).count(), 0)
            self.assertGreater(s.query(storage.Meeting).filter_by(client_id=self.sa).count(), 0)
            self.assertGreater(s.query(storage.StockItem).filter_by(client_id=self.sa).count(), 0)

    def test_15_setup_is_idempotent(self):
        before = {model.__tablename__: self._controlled_count(model) for model in (storage.User, storage.SenderMembership, storage.Task, storage.Inspection, storage.Meeting, storage.StockItem)}
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


if __name__ == "__main__":
    unittest.main()
