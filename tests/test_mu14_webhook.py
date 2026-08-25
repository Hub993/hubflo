import datetime as dt
import unittest

import app as hubflo_app
import storage


def inbound(sender, text, message_id="mu14-message"):
    return {
        "entry": [{
            "changes": [{
                "value": {
                    "metadata": {"phone_number_id": "9122-test"},
                    "contacts": [{"wa_id": sender}],
                    "messages": [{
                        "id": message_id,
                        "from": sender,
                        "type": "text",
                        "text": {"body": text},
                    }],
                }
            }]
        }]
    }


class MU14WebhookLifecycleTests(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender = "15550000100"
        with storage.SessionLocal() as session:
            session.add(storage.User(
                client_id=7,
                wa_id=self.sender,
                name="MU14 Sender",
                role="pm",
                project_code="PROJECT_A1",
                active=True,
            ))
            session.commit()

    def _state(self, key, state_kind="clarification"):
        return storage.save_pending_conversation_state({
            "client_id": 7,
            "sender": self.sender,
            "project_code": "PROJECT_A1",
            "state_kind": state_kind,
            "expected_field": "reminder_record",
            "original_request": "cancel the inspector reminder",
            "structured_context": {"action": "cancel"},
            "candidate_metadata": {"ids": [41, 42]},
            "continuation": {"kind": "reminder_lifecycle", "action": "cancel"},
            "continuation_key": key,
        })

    def test_each_canonical_control_retires_without_changing_candidates(self):
        cases = (
            ("cancel", "cancelled"),
            ("start over", "restarted"),
            ("never mind", "abandoned"),
        )
        for index, (message, status) in enumerate(cases):
            state = self._state(f"mu14-webhook:{index}")
            response = self.client.post(
                "/webhook",
                json=inbound(self.sender, message, f"control-{index}"),
            )
            self.assertEqual(response.status_code, 200)
            with storage.SessionLocal() as session:
                row = session.get(storage.ConversationState, state["id"])
                self.assertFalse(row.active)
                self.assertEqual(row.status, status)
                self.assertEqual(row.candidate_metadata_json, '{"ids":[41,42]}')

    def test_noncanonical_phrase_does_not_abandon(self):
        state = self._state("mu14-noncanonical")
        response = self.client.post(
            "/webhook",
            json=inbound(self.sender, "never mind please"),
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            row = session.get(storage.ConversationState, state["id"])
            self.assertTrue(row.active)
            self.assertEqual(row.status, "active")

    def test_conversation_cancel_does_not_cancel_underlying_reminder(self):
        with storage.SessionLocal() as session:
            reminder = storage.PMReminder(
                pm_wa=self.sender,
                recipient_wa=self.sender,
                project_code="PROJECT_A1",
                text="Stage 2 inspector reminder",
                next_run=dt.datetime.utcnow() + dt.timedelta(days=1),
                status="active",
                active=True,
            )
            session.add(reminder)
            session.commit()
            reminder_id = reminder.id
        state = self._state("mu14-cancel-not-business")
        with storage.SessionLocal() as session:
            row = session.get(storage.ConversationState, state["id"])
            row.candidate_metadata_json = '{"ids":[%d]}' % reminder_id
            session.commit()

        response = self.client.post(
            "/webhook",
            json=inbound(self.sender, "cancel", "conversation-cancel"),
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            conversation = session.get(storage.ConversationState, state["id"])
            reminder = session.get(storage.PMReminder, reminder_id)
            self.assertEqual(conversation.status, "cancelled")
            self.assertTrue(reminder.active)
            self.assertEqual(reminder.status, "active")

    def test_unrelated_deterministic_bypass_does_not_extend_activity(self):
        state = self._state("mu14-bypass")
        activity = dt.datetime.utcnow() - dt.timedelta(hours=3)
        with storage.SessionLocal() as session:
            row = session.get(storage.ConversationState, state["id"])
            row.last_activity_at = activity
            session.commit()

        response = self.client.post(
            "/webhook",
            json=inbound(
                self.sender,
                "Book inspection for drywall tomorrow",
                "deterministic-bypass",
            ),
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            row = session.get(storage.ConversationState, state["id"])
            self.assertTrue(row.active)
            self.assertEqual(row.last_activity_at, activity)


if __name__ == "__main__":
    unittest.main()
