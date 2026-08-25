import datetime as dt
import unittest
from zoneinfo import ZoneInfo

import app as hubflo_app
import storage
from core.conversation import ConversationRequest, CoreConversation
from industries.construction import ConstructionIndustryModule
from tests.test_mu14_webhook import inbound


class MU15SharedDateTimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = CoreConversation(ConstructionIndustryModule())
        cls.reference = dt.date(2026, 8, 25)

    def calendar(self, text, date_order=None):
        result = self.core.interpret_core(ConversationRequest(
            capability="shared_datetime",
            text=text,
            context={
                "candidate": "calendar_date",
                "reference_date": self.reference.isoformat(),
                "date_order": date_order,
            },
        ))
        return result.metadata

    def clock(self, text):
        return self.core.interpret_core(ConversationRequest(
            capability="shared_datetime",
            text=text,
            context={"candidate": "time_of_day"},
        )).metadata

    def assert_date(self, metadata, expected):
        self.assertTrue(metadata["valid"])
        self.assertEqual(
            dt.date(metadata["year"], metadata["month"], metadata["day"]),
            expected,
        )

    def test_iso_us_day_first_and_named_month_forms(self):
        self.assert_date(self.calendar("2026-08-25"), self.reference)
        self.assert_date(
            self.calendar("08/25/2026", "month_first"), self.reference,
        )
        self.assert_date(
            self.calendar("25/08/2026", "day_first"), self.reference,
        )
        self.assert_date(self.calendar("25 August 2026"), self.reference)

    def test_ambiguous_numeric_requires_or_obeys_policy(self):
        unresolved = self.calendar("04/05/2027")
        self.assertFalse(unresolved["valid"])
        self.assertTrue(unresolved["ambiguous"])
        self.assert_date(
            self.calendar("04/05/2027", "month_first"),
            dt.date(2027, 4, 5),
        )
        self.assert_date(
            self.calendar("04/05/2027", "day_first"),
            dt.date(2027, 5, 4),
        )

    def test_common_separators_have_equivalent_unambiguous_meaning(self):
        for value in ("25/08/2026", "25-08-2026", "25.08.2026"):
            self.assert_date(self.calendar(value), self.reference)

    def test_relative_days_weekday_and_regional_inspection_consumer(self):
        self.assert_date(self.calendar("today"), dt.date(2026, 8, 25))
        self.assert_date(self.calendar("tomorrow"), dt.date(2026, 8, 26))
        self.assert_date(self.calendar("Friday"), dt.date(2026, 8, 28))
        inspection = hubflo_app.parse_inspection_request(
            "Book inspection for drywall on 25/08/2026",
            today=dt.date(2026, 8, 20),
            date_order="day_first",
        )
        self.assertEqual(inspection["phase"], "drywall")
        self.assertEqual(inspection["required_date"], dt.datetime(2026, 8, 25))

    def test_12_hour_24_hour_and_voice_style_clocks(self):
        self.assertEqual(self.clock("9 AM"), {"hour": 9, "minute": 0})
        self.assertEqual(self.clock("9 PM"), {"hour": 21, "minute": 0})
        self.assertEqual(self.clock("21:30"), {"hour": 21, "minute": 30})
        self.assertEqual(
            self.clock("tomorrow morning at nine"),
            {"hour": 9, "minute": 0},
        )
        self.assertEqual(
            self.clock("tomorrow evening at quarter past nine"),
            {"hour": 21, "minute": 15},
        )

    def test_timezone_controls_relative_day_at_boundary(self):
        instant = dt.datetime(2026, 8, 26, 3, 30, tzinfo=dt.timezone.utc)
        new_york = hubflo_app.parse_pm_reminder_request(
            "Remind me tomorrow at 9 AM to check",
            timezone_name="America/New_York",
            now_local=instant,
        )
        london = hubflo_app.parse_pm_reminder_request(
            "Remind me tomorrow at 9 AM to check",
            timezone_name="Europe/London",
            now_local=instant,
        )
        ny_local = new_york["next_run"].replace(
            tzinfo=dt.timezone.utc
        ).astimezone(ZoneInfo("America/New_York"))
        london_local = london["next_run"].replace(
            tzinfo=dt.timezone.utc
        ).astimezone(ZoneInfo("Europe/London"))
        self.assertEqual(ny_local.date(), dt.date(2026, 8, 26))
        self.assertEqual(london_local.date(), dt.date(2026, 8, 27))

    def test_configured_display_preserves_canonical_value(self):
        value = dt.datetime(2026, 8, 25, 21, 30)
        self.assertEqual(
            hubflo_app.format_configured_datetime(
                value, "UTC", "month_first", "12h"
            ),
            "08/25/2026 09:30 PM",
        )
        self.assertEqual(
            hubflo_app.format_configured_datetime(
                value, "UTC", "day_first", "24h"
            ),
            "25/08/2026 21:30",
        )


class MU15PersistentClarificationTests(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender = "15550000200"
        with storage.SessionLocal() as session:
            user = storage.User(
                client_id=8,
                wa_id=self.sender,
                name="No Date Policy",
                role="pm",
                project_code="PROJECT_A1",
                timezone="America/New_York",
                active=True,
            )
            session.add(user)
            session.commit()
            user.date_order = None
            session.commit()

    def test_ambiguous_date_persists_then_resolves_once(self):
        original = "Remind me on 04/05/2027 at 9 AM to call the inspector"
        first = self.client.post(
            "/webhook", json=inbound(self.sender, original, "ambiguous-date")
        )
        self.assertEqual(first.status_code, 200)
        with storage.SessionLocal() as session:
            state = session.query(storage.ConversationState).one()
            self.assertTrue(state.active)
            self.assertEqual(state.expected_field, "calendar_date")
            self.assertEqual(session.query(storage.PMReminder).count(), 0)
            state_id = state.id

        second = self.client.post(
            "/webhook",
            json=inbound(self.sender, "April 5 2027", "date-followup"),
        )
        self.assertEqual(second.status_code, 200)
        with storage.SessionLocal() as session:
            state = session.get(storage.ConversationState, state_id)
            reminders = session.query(storage.PMReminder).all()
            self.assertFalse(state.active)
            self.assertEqual(state.status, "resolved")
            self.assertEqual(len(reminders), 1)
            local = reminders[0].next_run.replace(
                tzinfo=dt.timezone.utc
            ).astimezone(ZoneInfo("America/New_York"))
            self.assertEqual(local, dt.datetime(
                2027, 4, 5, 9, 0, tzinfo=ZoneInfo("America/New_York")
            ))

        replay = self.client.post(
            "/webhook",
            json=inbound(self.sender, "April 5 2027", "date-followup"),
        )
        self.assertEqual(replay.status_code, 200)
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.PMReminder).count(), 1)


if __name__ == "__main__":
    unittest.main()
