import datetime as dt
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

import storage
from core.conversation import ConversationRequest, CoreConversation
from industries.construction import ConstructionIndustryModule


class MU14LifecycleStorageTests(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()

    def _state(self, sender="15550000001", kind="clarification"):
        return storage.save_pending_conversation_state(
            {
                "client_id": 7,
                "sender": sender,
                "project_code": "PROJECT_A1",
                "state_kind": kind,
                "expected_field": "reminder_record",
                "original_request": "cancel the inspector reminder",
                "structured_context": {"action": "cancel"},
                "candidate_metadata": {"ids": [11, 12]},
                "continuation": {"kind": "reminder_lifecycle"},
                "continuation_key": f"test:{sender}:{kind}",
            }
        )

    def _set_activity(self, state_id, value):
        with storage.SessionLocal() as session:
            row = session.get(storage.ConversationState, state_id)
            row.last_activity_at = value
            row.updated_at = value
            session.commit()

    def test_expiry_boundary_is_exact_and_retires_stale_state(self):
        for index, kind in enumerate(("await", "clarification")):
            state = self._state(sender=f"155500009{index}", kind=kind)
            base = dt.datetime(2026, 8, 25, 12, 0, 0)
            self._set_activity(state["id"], base)

            active = storage.get_pending_conversation_state(
                state["sender"], 7, "PROJECT_A1",
                now_utc=base + dt.timedelta(hours=23, minutes=59, seconds=59),
            )
            self.assertIsNotNone(active)

            expired = storage.get_pending_conversation_state(
                state["sender"], 7, "PROJECT_A1",
                now_utc=base + dt.timedelta(hours=24),
            )
            self.assertIsNone(expired)
            with storage.SessionLocal() as session:
                row = session.get(storage.ConversationState, state["id"])
                self.assertFalse(row.active)
                self.assertEqual(row.status, "expired")
                self.assertEqual(row.retirement_reason, "expired")
                self.assertEqual(row.candidate_metadata_json, '{"ids":[11,12]}')

            still_expired = storage.get_pending_conversation_state(
                state["sender"], 7, "PROJECT_A1",
                now_utc=base + dt.timedelta(days=2),
            )
            self.assertIsNone(still_expired)

    def test_continuation_activity_extends_expiry_but_reads_do_not(self):
        state = self._state()
        base = dt.datetime.utcnow() - dt.timedelta(hours=20)
        self._set_activity(state["id"], base)

        read_at = base + dt.timedelta(hours=10)
        found = storage.get_pending_conversation_state(
            state["sender"], 7, "PROJECT_A1", now_utc=read_at,
        )
        self.assertEqual(found["last_activity_at"], base)

        attempt_at = base + dt.timedelta(hours=20)
        touched = storage.touch_conversation_state_activity(
            state["id"], state["sender"], 7, "PROJECT_A1",
            now_utc=attempt_at,
        )
        self.assertEqual(touched["status_result"], "touched")
        self.assertEqual(touched["last_activity_at"], attempt_at)
        self.assertIsNotNone(storage.get_pending_conversation_state(
            state["sender"], 7, "PROJECT_A1",
            now_utc=attempt_at + dt.timedelta(hours=23, minutes=59),
        ))

    def test_cancel_restart_and_abandon_retire_only_conversation_state(self):
        for index, reason in enumerate(("cancelled", "restarted", "abandoned")):
            state = self._state(sender=f"1555000001{index}")
            before_context = state["structured_context"]
            before_candidates = state["candidate_metadata"]
            result = storage.retire_conversation_state(
                state["id"], state["sender"], 7, "PROJECT_A1", reason,
            )
            self.assertEqual(result["status_result"], "retired")
            self.assertFalse(result["active"])
            self.assertEqual(result["status"], reason)
            self.assertEqual(result["retirement_reason"], reason)
            self.assertEqual(result["structured_context"], before_context)
            self.assertEqual(result["candidate_metadata"], before_candidates)
            self.assertIsNone(storage.get_pending_conversation_state(
                state["sender"], 7, "PROJECT_A1",
            ))

    def test_atomic_claim_allows_exactly_one_concurrent_winner(self):
        state = self._state()
        barrier = threading.Barrier(2)

        def claim():
            barrier.wait()
            return storage.claim_conversation_state_continuation(
                state["id"], state["sender"], 7, "PROJECT_A1",
            ).get("status_result")

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: claim(), range(2)))
        self.assertEqual(results.count("claimed"), 1)
        self.assertEqual(results.count(None), 1)


class MU14LifecycleIntentTests(unittest.TestCase):
    def setUp(self):
        self.core = CoreConversation(ConstructionIndustryModule())

    def action(self, text):
        result = self.core.interpret_core(
            ConversationRequest(
                capability="conversation_lifecycle",
                text=text,
                context={"candidate": "control_intent"},
            )
        )
        return result.action if result.handled else None

    def test_locked_control_phrases(self):
        self.assertEqual(self.action(" cancel "), "cancelled")
        self.assertEqual(self.action("START OVER"), "restarted")
        self.assertEqual(self.action(" Never Mind "), "abandoned")

    def test_only_surrounding_whitespace_and_case_are_normalized(self):
        self.assertIsNone(self.action("never mind please"))
        self.assertIsNone(self.action("cancel reminder 12"))
        self.assertIsNone(self.action("start over now"))


if __name__ == "__main__":
    unittest.main()
