import datetime as dt
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import app as hubflo_app
import storage
from tests.test_mu14_webhook import inbound


class AnnexAControlledFixture(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender = "15550000500"
        with storage.SessionLocal() as session:
            session.add(storage.User(
                client_id=20,
                wa_id=self.sender,
                name="Annex A PM",
                role="pm",
                project_code="PROJECT_A1",
                timezone="America/New_York",
                active=True,
            ))
            session.flush()
            user = session.query(storage.User).filter(
                storage.User.wa_id == self.sender
            ).one()
            session.add(storage.PMProjectMap(
                client_id=20,
                pm_user_id=user.id,
                project_code="PROJECT_A1",
                primary_pm=True,
            ))
            session.commit()

    def send(self, text, message_id):
        return self.client.post(
            "/webhook", json=inbound(self.sender, text, message_id)
        )

    def make_await(self, marker, body="fixture"):
        separator = " " if marker in ("new_stock_qty", "stock_unit") else "\n"
        task = storage.create_task(
            self.sender,
            f"[await:{marker}]{separator}{body}",
            tag="order" if marker in ("item", "quantity") else "stock",
            project_code="PROJECT_A1",
        )
        return task

    def make_retired_legacy_order_await(self):
        task = self.make_await("item", "Retained legacy order")
        state = storage.save_pending_conversation_state({
            "client_id": 20,
            "sender": self.sender,
            "project_code": "PROJECT_A1",
            "state_kind": "await",
            "expected_field": "item",
            "original_request": "Retained legacy order",
            "structured_context": {"source_record_id": task["id"]},
            "candidate_metadata": {},
            "continuation": {
                "source_record_id": task["id"],
                "source_record_type": "pending_business_state",
            },
            "continuation_key": f"business-state:{task['id']}",
        })
        storage.retire_conversation_state(
            state["id"], self.sender, 20, "PROJECT_A1", "cancelled"
        )
        return task, state

    def assert_retired_legacy_order_await(self, task_id, state_id):
        with storage.SessionLocal() as session:
            task = session.get(storage.Task, task_id)
            state = session.get(storage.ConversationState, state_id)
            self.assertEqual(task.status, "open")
            self.assertTrue(task.text.startswith("[await:item]"))
            self.assertFalse(state.active)
            self.assertEqual(state.status, "cancelled")
            self.assertEqual(state.expected_field, "item")

    def make_reminder(self, label, project="PROJECT_A1"):
        return storage.create_pm_reminder({
            "pm_wa": self.sender,
            "recipient_wa": self.sender,
            "project_code": project,
            "text": label,
            "timezone": "America/New_York",
            "next_run": dt.datetime.utcnow() + dt.timedelta(days=2),
            "rule": "once",
        })

    def create_ambiguous_reminder_state(self):
        alpha = self.make_reminder("Stage 2 inspector alpha")
        beta = self.make_reminder("Stage 2 inspector beta")
        response = self.send(
            "Cancel Stage 2 inspector reminder", "clarification-initial"
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            state = session.query(storage.ConversationState).filter(
                storage.ConversationState.active == True
            ).one()
        return alpha, beta, state.id

    def test_legacy_order_and_stock_await_compatibility(self):
        order = self.make_await("item", "Order materials")
        self.send("cement", "legacy-order-item")
        with storage.SessionLocal() as session:
            row = session.get(storage.Task, order["id"])
            self.assertTrue(row.text.startswith("[await:quantity]"))
            state = session.query(storage.ConversationState).filter(
                storage.ConversationState.continuation_key
                == f"business-state:{order['id']}"
            ).one()
            self.assertEqual(state.expected_field, "quantity")
            order_state_id = state.id

        storage.retire_conversation_state(
            order_state_id, self.sender, 20, "PROJECT_A1", "cancelled"
        )

        stock = self.make_await(
            "new_stock_qty", "material=cement;unit=bags"
        )
        self.send("5", "legacy-stock-quantity")
        with storage.SessionLocal() as session:
            row = session.get(storage.Task, stock["id"])
            self.assertEqual(row.status, "done")
            item = session.query(storage.StockItem).filter(
                storage.StockItem.name == "cement"
            ).one()
            self.assertEqual(item.current_qty, 5)
            self.assertEqual(item.client_id, 20)

    def test_await_bypass_resume_cancel_restart_and_abandonment(self):
        stock = self.make_await(
            "new_stock_qty", "material=grout;unit=bags"
        )
        original_text = stock["text"]
        self.send(
            "Book inspection for drywall tomorrow", "await-inspection-bypass"
        )
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.Task, stock["id"]).text, original_text)
            self.assertEqual(session.query(storage.Inspection).count(), 1)
        self.send("7", "await-resume")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.Task, stock["id"]).status, "done")

        for index, (phrase, status) in enumerate((
            ("cancel", "cancelled"),
            ("start over", "restarted"),
            ("never mind", "abandoned"),
        )):
            task = self.make_await("quantity", "Item: timber")
            state = storage.save_pending_conversation_state({
                "client_id": 20,
                "sender": self.sender,
                "project_code": "PROJECT_A1",
                "state_kind": "await",
                "expected_field": "quantity",
                "original_request": "Order timber",
                "structured_context": {"source_record_id": task["id"]},
                "continuation": {"source_record_id": task["id"]},
                "continuation_key": f"business-state:{task['id']}",
            })
            before = task["text"]
            self.send(phrase, f"lifecycle-{index}")
            with storage.SessionLocal() as session:
                conversation = session.get(storage.ConversationState, state["id"])
                business = session.get(storage.Task, task["id"])
                self.assertEqual(conversation.status, status)
                self.assertEqual(business.text, before)
                self.assertEqual(business.status, "open")
            if phrase == "never mind":
                self.send("20", "abandoned-former-answer")
                with storage.SessionLocal() as session:
                    business = session.get(storage.Task, task["id"])
                    self.assertEqual(business.text, before)
                    self.assertEqual(business.status, "open")

    def test_reminder_zero_one_many_invalid_and_continuation(self):
        self.send("Cancel nonexistent reminder", "reminder-zero")
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.PMReminder).count(), 0)

        unique = self.make_reminder("Unique inspector reminder")
        self.send("Cancel Unique inspector reminder", "reminder-one")
        with storage.SessionLocal() as session:
            self.assertEqual(
                session.get(storage.PMReminder, unique["id"]).status,
                "cancelled",
            )

        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "active")
            self.assertEqual(session.get(storage.PMReminder, beta["id"]).status, "active")
        self.send("something else", "clarification-invalid")
        with storage.SessionLocal() as session:
            self.assertTrue(session.get(storage.ConversationState, state_id).active)
        self.send("alpha", "clarification-valid")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "cancelled")
            self.assertEqual(session.get(storage.PMReminder, beta["id"]).status, "active")
            self.assertFalse(session.get(storage.ConversationState, state_id).active)

    def test_retired_await_bypass_task_note_delivery_search_and_status_routes(self):
        legacy, retired = self.make_retired_legacy_order_await()
        cases = (
            ("Create a task to check the east gate", "task", "assigned"),
            ("Note for PROJECT_A1: east gate checked", "note", "note"),
            ("Pin note for PROJECT_A1: keep east gate clear", "note", "pinned"),
            ("Record delivery of cement at north gate", "delivery", "assigned"),
            ("Log a delivery of grout at west gate", "delivery", "assigned"),
        )
        for index, (text, tag, subtype) in enumerate(cases):
            before = None
            with storage.SessionLocal() as session:
                before = session.query(storage.Task).count()
            self.assertEqual(self.send(
                text, f"retired-await-route-{index}"
            ).status_code, 200)
            with storage.SessionLocal() as session:
                self.assertEqual(session.query(storage.Task).count(), before + 1)
                created = session.query(storage.Task).order_by(
                    storage.Task.id.desc()
                ).first()
                self.assertEqual(created.tag, tag)
                self.assertEqual(created.subtype, subtype)
                self.assertEqual(created.client_id, 20)
                self.assertEqual(created.project_code, "PROJECT_A1")
                self.assertEqual(session.query(storage.Audit).filter(
                    storage.Audit.action == "create",
                    storage.Audit.ref_type == "task",
                    storage.Audit.ref_id == created.id,
                ).count(), 1)
            self.assert_retired_legacy_order_await(
                legacy["id"], retired["id"]
            )

        with storage.SessionLocal() as session:
            before = session.query(storage.Task).count()
        self.send("Search for cement tasks", "retired-await-search")
        self.send("What is the status for PROJECT_A1", "retired-await-status")
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Task).count(), before)
        self.assert_retired_legacy_order_await(legacy["id"], retired["id"])

    def test_retired_await_bypass_inspection_delay_and_meeting_routes(self):
        source = storage.create_task(
            self.sender,
            "Framing crew",
            tag="task",
            project_code="PROJECT_A1",
        )
        legacy, retired = self.make_retired_legacy_order_await()

        self.send(
            "Book inspection for drywall tomorrow",
            "retired-await-inspection",
        )
        with storage.SessionLocal() as session:
            inspection = session.query(storage.Inspection).one()
            self.assertEqual(inspection.client_id, 20)
            self.assertEqual(inspection.project_code, "PROJECT_A1")
        self.assert_retired_legacy_order_await(legacy["id"], retired["id"])

        self.send(
            "Framing crew is delayed by 2 days - weather",
            "retired-await-delay",
        )
        with storage.SessionLocal() as session:
            delay = session.query(storage.DelayLog).one()
            self.assertEqual(delay.task_id, source["id"])
            self.assertEqual(delay.client_id, 20)
            self.assertEqual(delay.project_code, "PROJECT_A1")
        self.assert_retired_legacy_order_await(legacy["id"], retired["id"])

        self.send(
            "Schedule site meeting tomorrow at 2 PM",
            "retired-await-meeting",
        )
        with storage.SessionLocal() as session:
            meeting = session.query(storage.Meeting).one()
            self.assertEqual(meeting.client_id, 20)
            self.assertEqual(meeting.project_code, "PROJECT_A1")
        self.assert_retired_legacy_order_await(legacy["id"], retired["id"])

    def test_retired_await_preserved_during_reminder_routes(self):
        reminder = self.make_reminder("Retired-await lifecycle reminder")
        legacy, retired = self.make_retired_legacy_order_await()

        self.send(
            "Remind me tomorrow at 9 AM to call the inspector",
            "retired-await-reminder-create",
        )
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.PMReminder).count(), 2)
        self.assert_retired_legacy_order_await(legacy["id"], retired["id"])

        self.send(
            f"Cancel reminder {reminder['id']}",
            "retired-await-reminder-cancel",
        )
        with storage.SessionLocal() as session:
            cancelled = session.get(storage.PMReminder, reminder["id"])
            self.assertFalse(cancelled.active)
            self.assertEqual(cancelled.status, "cancelled")
        self.assert_retired_legacy_order_await(legacy["id"], retired["id"])

    def test_retired_await_bypass_new_stock_item_route(self):
        legacy, retired = self.make_retired_legacy_order_await()
        self.send("Add new stock item: grout", "retired-await-new-stock")
        with storage.SessionLocal() as session:
            stock_await = session.query(storage.Task).filter(
                storage.Task.id != legacy["id"],
                storage.Task.tag == "stock",
            ).one()
            self.assertTrue(stock_await.text.startswith("[await:new_stock_unit]"))
            stock_state = session.query(storage.ConversationState).filter(
                storage.ConversationState.id != retired["id"],
                storage.ConversationState.active == True,
            ).one()
            self.assertEqual(stock_state.expected_field, "new_stock_unit")
        self.assert_retired_legacy_order_await(legacy["id"], retired["id"])

    def test_persisted_candidate_universe_cannot_broaden(self):
        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        gamma = self.make_reminder("Stage 2 inspector gamma")
        self.send("gamma", "broadened-candidate")
        with storage.SessionLocal() as session:
            self.assertTrue(session.get(storage.ConversationState, state_id).active)
            self.assertEqual(session.get(storage.PMReminder, gamma["id"]).status, "active")
            state = session.get(storage.ConversationState, state_id)
            candidates = json.loads(state.candidate_metadata_json)["reminder_candidates"]
            self.assertEqual({row["id"] for row in candidates}, {alpha["id"], beta["id"]})

    def test_current_authorization_and_record_attributes_narrow_candidates(self):
        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        with storage.SessionLocal() as session:
            reminder = session.get(storage.PMReminder, alpha["id"])
            reminder.project_code = "PROJECT_A2"
            session.commit()
        self.send("alpha", "narrowed-candidate")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "active")
            self.assertTrue(session.get(storage.ConversationState, state_id).active)

    def test_concurrent_duplicate_continuation_enters_handler_once(self):
        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        barrier = threading.Barrier(2)

        def continue_once(index):
            barrier.wait()
            client = hubflo_app.app.test_client()
            return client.post(
                "/webhook",
                json=inbound(self.sender, "alpha", f"concurrent-{index}"),
            ).status_code

        with ThreadPoolExecutor(max_workers=2) as executor:
            statuses = list(executor.map(continue_once, range(2)))
        self.assertEqual(statuses, [200, 200])
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "cancelled")
            self.assertEqual(session.get(storage.PMReminder, beta["id"]).status, "active")
            audits = session.query(storage.Audit).filter(
                storage.Audit.action == "reminder_cancel",
                storage.Audit.ref_id == alpha["id"],
            ).count()
            self.assertEqual(audits, 1)
            self.assertFalse(session.get(storage.ConversationState, state_id).active)

    def test_sequential_replay_and_retry_after_completion_do_not_repeat_handler(self):
        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        self.send("alpha", "sequential-resolution")
        self.send("alpha", "sequential-resolution")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "cancelled")
            self.assertEqual(session.get(storage.PMReminder, beta["id"]).status, "active")
            self.assertEqual(session.query(storage.Audit).filter(
                storage.Audit.action == "reminder_cancel",
                storage.Audit.ref_id == alpha["id"],
            ).count(), 1)
            self.assertFalse(session.get(storage.ConversationState, state_id).active)

    def test_clarification_bypass_still_ambiguous_and_resume(self):
        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        with storage.SessionLocal() as session:
            before = session.get(storage.ConversationState, state_id).candidate_metadata_json
        self.send(
            "Book inspection for drywall tomorrow", "clarification-bypass"
        )
        with storage.SessionLocal() as session:
            state = session.get(storage.ConversationState, state_id)
            self.assertTrue(state.active)
            self.assertEqual(state.candidate_metadata_json, before)
            self.assertEqual(session.query(storage.Inspection).count(), 1)
        self.send("Stage 2 inspector", "still-ambiguous")
        with storage.SessionLocal() as session:
            self.assertTrue(session.get(storage.ConversationState, state_id).active)
        self.send("alpha", "resume-after-bypass")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "cancelled")
            self.assertEqual(session.get(storage.PMReminder, beta["id"]).status, "active")

    def test_default_and_generic_order_vocabulary_do_not_bypass_clarification(self):
        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        with storage.SessionLocal() as session:
            task_count = session.query(storage.Task).count()
        self.send("hello there", "default-during-clarification")
        self.send(
            "supplier quantity delivery north gate",
            "generic-order-during-clarification",
        )
        with storage.SessionLocal() as session:
            self.assertTrue(session.get(storage.ConversationState, state_id).active)
            self.assertEqual(session.query(storage.Task).count(), task_count)
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "active")
            self.assertEqual(session.get(storage.PMReminder, beta["id"]).status, "active")

    def test_conversation_state_uses_exact_canonical_client_scope(self):
        state = storage.save_pending_conversation_state({
            "client_id": 20,
            "sender": self.sender,
            "project_code": "PROJECT_A1",
            "state_kind": "clarification",
            "expected_field": "fixture",
            "original_request": "fixture",
            "continuation_key": "canonical-client-fixture",
        })
        self.assertIsNotNone(storage.get_pending_conversation_state(
            self.sender, 20, "PROJECT_A1"
        ))
        self.assertIsNone(storage.get_pending_conversation_state(
            self.sender, 21, "PROJECT_A1"
        ))
        denied = storage.claim_conversation_state_continuation(
            state["id"], self.sender, 21, "PROJECT_A1"
        )
        self.assertNotEqual(denied.get("status_result"), "claimed")

    def test_expired_clarification_cannot_mutate(self):
        alpha, beta, state_id = self.create_ambiguous_reminder_state()
        with storage.SessionLocal() as session:
            state = session.get(storage.ConversationState, state_id)
            state.last_activity_at = dt.datetime.utcnow() - dt.timedelta(hours=24)
            session.commit()
        self.send("alpha", "expired-followup")
        with storage.SessionLocal() as session:
            state = session.get(storage.ConversationState, state_id)
            self.assertEqual(state.status, "expired")
            self.assertEqual(session.get(storage.PMReminder, alpha["id"]).status, "active")
            self.assertEqual(session.get(storage.PMReminder, beta["id"]).status, "active")

    def test_numeric_id_reminder_and_delay_compatibility(self):
        reminder = self.make_reminder("Numeric reminder fixture")
        self.send(f"Cancel reminder {reminder['id']}", "numeric-reminder")
        task = storage.create_task(
            self.sender, "Numeric delay fixture", tag="task",
            project_code="PROJECT_A1",
        )
        self.send(
            f"Task {task['id']} is delayed by 2 days - weather",
            "numeric-delay",
        )
        with storage.SessionLocal() as session:
            self.assertEqual(
                session.get(storage.PMReminder, reminder["id"]).status,
                "cancelled",
            )
            delay = session.query(storage.DelayLog).one()
            self.assertEqual(delay.task_id, task["id"])
            self.assertEqual(delay.client_id, 20)
            self.assertEqual(delay.days, 2)

    def test_natural_delay_and_inspection_persist_tenant_scoped_results(self):
        task = storage.create_task(
            self.sender, "Framing crew", tag="task", project_code="PROJECT_A1"
        )
        self.send(
            "Framing crew is delayed by 2 days - weather", "natural-delay"
        )
        self.send(
            "Book inspection for drywall tomorrow", "controlled-inspection"
        )
        with storage.SessionLocal() as session:
            delay = session.query(storage.DelayLog).one()
            inspection = session.query(storage.Inspection).one()
            self.assertEqual(delay.task_id, task["id"])
            self.assertEqual(delay.client_id, 20)
            self.assertEqual(inspection.client_id, 20)
            self.assertEqual(inspection.project_code, "PROJECT_A1")
            self.assertEqual(inspection.phase, "drywall")

    def test_search_and_status_are_scoped_read_only_controlled_results(self):
        own = storage.create_task(
            self.sender, "Install drywall", tag="task", project_code="PROJECT_A1"
        )
        foreign_sender = "15550000599"
        with storage.SessionLocal() as session:
            session.add(storage.User(
                client_id=21, wa_id=foreign_sender, name="Other Client",
                role="pm", project_code="PROJECT_A1", active=True,
            ))
            session.commit()
        foreign = storage.create_task(
            foreign_sender, "Foreign drywall secret", tag="task",
            project_code="PROJECT_A1",
        )
        outputs = []

        def capture(_phone_id, _to, body):
            outputs.append(body)
            return True, {}

        with patch.object(hubflo_app, "send_whatsapp_text", side_effect=capture):
            self.send("Search for drywall", "controlled-search")
            self.send("What is the status for PROJECT_A1", "controlled-status")
        combined = "\n".join(outputs)
        self.assertIn(str(own["id"]), combined)
        self.assertNotIn("Foreign drywall secret", combined)
        self.assertNotIn(str(foreign["id"]) + ")", combined)
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Task).count(), 2)

    def test_unauthorized_project_approval_arrives_but_does_not_mutate(self):
        outside = storage.create_task(
            self.sender, "Outside project order", tag="order",
            project_code="PROJECT_A2", order_state="pending_approval",
        )
        response = self.send(
            f"Approve change order {outside['id']}", "unauthorized-project"
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            task = session.get(storage.Task, outside["id"])
            self.assertEqual(task.status, "open")
            self.assertIsNone(task.approved_at)


if __name__ == "__main__":
    unittest.main()
