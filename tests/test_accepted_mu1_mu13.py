import datetime as dt
import pathlib
import unittest
from zoneinfo import ZoneInfo

import app as hubflo_app
from core.conversation import ConversationRequest, CoreConversation
from core.industry import IndustryRequest
from industries.construction import ConstructionIndustryModule


class AcceptedMU1ToMU13Regression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = CoreConversation(ConstructionIndustryModule())

    def core_result(self, capability, text, candidate, **context):
        return self.core.interpret_core(ConversationRequest(
            capability=capability,
            text=text,
            context={"candidate": candidate, **context},
        ))

    def test_inspection_recognition_and_shared_date_consumer(self):
        parsed = hubflo_app.parse_inspection_request(
            "Book inspection for drywall tomorrow",
            today=dt.date(2026, 8, 25),
        )
        self.assertEqual(parsed["phase"], "drywall")
        self.assertEqual(parsed["required_date"], dt.datetime(2026, 8, 26))

    def test_critical_path_delay_positive_and_negative_recognition(self):
        self.assertTrue(hubflo_app.classify_delay(
            "Framing crew is delayed by 2 days - weather"
        ))
        self.assertFalse(hubflo_app.classify_delay("There is no delay"))

    def test_construction_work_reference_terminology_is_bounded(self):
        cases = {
            "Framing crew is delayed by 2 days": "framing",
            "Framing team is delayed by 2 days": "framing",
            "Framers are delayed by 2 days": "framing",
            "Drywall crew is delayed by 2 days": "drywall",
            "Roofers are delayed by 2 days": "roofing",
            "Concrete team is delayed by 2 days": "concrete",
        }
        industry = ConstructionIndustryModule()
        for text, expected in cases.items():
            result = industry.interpret(IndustryRequest(
                capability="domain_recognition",
                text=text,
                context={"candidate": "work_reference_terminology"},
            ))
            self.assertTrue(result.handled, text)
            self.assertEqual(result.entities["canonical_reference"], expected)

        unrelated = industry.interpret(IndustryRequest(
            capability="domain_recognition",
            text="The site is delayed by 2 days",
            context={"candidate": "work_reference_terminology"},
        ))
        self.assertFalse(unrelated.handled)

    def test_reminder_creation_time_date_duration_and_recurrence(self):
        now = dt.datetime(2026, 8, 25, 8, 0, tzinfo=ZoneInfo("America/New_York"))
        tomorrow = hubflo_app.parse_pm_reminder_request(
            "Remind me tomorrow at 9 AM to call the inspector",
            now_local=now,
        )
        self.assertEqual(tomorrow["next_run"], dt.datetime(2026, 8, 26, 13, 0))

        duration = hubflo_app.parse_pm_reminder_request(
            "Remind me in 2 hours to call the inspector",
            now_local=now,
        )
        self.assertEqual(duration["next_run"], dt.datetime(2026, 8, 25, 14, 0))

        recurring = hubflo_app.parse_pm_reminder_request(
            "Remind me every weekday at 9 AM to check reports",
            now_local=now,
        )
        self.assertTrue(recurring["recurring"])
        self.assertEqual(recurring["recurrence_rule"], "weekdays")

    def test_reminder_lifecycle_and_numeric_id_compatibility(self):
        self.assertEqual(
            hubflo_app.classify_pm_reminder_lifecycle("Cancel reminder 42"),
            "cancel",
        )
        self.assertEqual(hubflo_app._pm_reminder_id_from_text(
            "Cancel reminder 42"
        ), 42)
        self.assertEqual(
            hubflo_app.classify_pm_reminder_lifecycle("Snooze reminder 42"),
            "snooze",
        )
        self.assertEqual(
            hubflo_app.classify_pm_reminder_lifecycle("Reassign reminder 42 to Jordan"),
            "redirect",
        )

    def test_neutral_zero_one_many_record_resolution(self):
        records = [
            {"id": 1, "label": "Stage 2 inspector reminder"},
            {"id": 2, "label": "Stage 2 inspector followup"},
        ]
        zero = self.core_result(
            "record_resolution", "plumbing", "text_reference", records=records,
        )
        self.assertEqual(zero.metadata["resolution"], "not_found")

        one = self.core_result(
            "record_resolution", "reminder", "text_reference",
            records=[records[0]], resolve_single_unqualified=True,
        )
        self.assertEqual(one.metadata["resolution"], "resolved")
        self.assertEqual(one.entities["record_id"], 1)

        many = self.core_result(
            "record_resolution", "Stage 2 inspector", "text_reference",
            records=records,
        )
        self.assertEqual(many.metadata["resolution"], "ambiguous")
        self.assertEqual(len(many.metadata["matches"]), 2)

    def test_await_routing_arbitration_preserves_default_fallback_rule(self):
        recognized = self.core_result(
            "routing_arbitration", "", "await_vs_normal_route",
            deterministic_recognition=True,
        )
        self.assertEqual(recognized.action, "normal_route")
        ambiguous = self.core_result(
            "routing_arbitration", "", "await_vs_normal_route",
            deterministic_recognition=False,
        )
        self.assertEqual(ambiguous.action, "pending_await")

    def test_core_has_no_construction_dependency(self):
        core_source = pathlib.Path("core/conversation.py").read_text()
        self.assertNotIn("industries.construction", core_source)
        self.assertNotIn("ConstructionIndustry", core_source)


if __name__ == "__main__":
    unittest.main()
