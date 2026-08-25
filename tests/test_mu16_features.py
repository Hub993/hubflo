import datetime as dt
import unittest
from zoneinfo import ZoneInfo

import app as hubflo_app
import storage
from tests.test_mu14_webhook import inbound


class MU16StructuredInterpretationTests(unittest.TestCase):
    def test_complete_natural_order_extracts_supplied_fields(self):
        result = hubflo_app.parse_natural_order(
            "Order 20 bags of cement for PROJECT_A1 for delivery Friday to the north gate",
            "PROJECT_A1",
        )
        self.assertEqual(result["intent"], "order")
        self.assertEqual(result["fields"]["quantity"], "20 bags")
        self.assertEqual(result["fields"]["item"], "cement")
        self.assertEqual(result["fields"]["delivery_date"], "Friday")
        self.assertEqual(result["fields"]["drop_location"], "north gate")
        self.assertEqual(result["missing_fields"], [])

    def test_order_reports_only_fields_actually_missing(self):
        result = hubflo_app.parse_natural_order(
            "Order 20 bags of cement for delivery Friday",
        )
        self.assertEqual(result["missing_fields"], ["drop_location"])
        self.assertEqual(result["fields"]["item"], "cement")
        self.assertEqual(result["fields"]["quantity"], "20 bags")
        self.assertEqual(result["fields"]["delivery_date"], "Friday")

    def test_natural_meeting_has_structured_schedule(self):
        now = dt.datetime(2026, 8, 25, 8, 0, tzinfo=ZoneInfo("America/New_York"))
        result = hubflo_app.parse_natural_meeting(
            "Schedule site meeting tomorrow at 2 PM",
            now_local=now,
        )
        self.assertEqual(result["intent"], "meeting")
        self.assertEqual(result["title"].lower(), "site meeting")
        self.assertEqual(result["scheduled_for"], dt.datetime(2026, 8, 26, 18, 0))

    def test_full_reminder_lifecycle_is_exposed_in_ordinary_language(self):
        cases = {
            "Acknowledge reminder 12": "acknowledge",
            "Snooze reminder 12 for 30 minutes": "snooze",
            "Reassign reminder 12 to Jordan": "redirect",
            "Cancel the inspector reminder": "cancel",
        }
        for text, expected in cases.items():
            self.assertEqual(
                hubflo_app.classify_pm_reminder_lifecycle(text), expected
            )


class MU16WebhookFeatureTests(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender = "15550000300"
        with storage.SessionLocal() as session:
            session.add(storage.User(
                client_id=9,
                wa_id=self.sender,
                name="MU16 PM",
                role="pm",
                project_code="PROJECT_A1",
                timezone="America/New_York",
                active=True,
            ))
            session.commit()

    def test_complete_order_mutates_once_without_clarification(self):
        text = (
            "Order 20 bags of cement for PROJECT_A1 "
            "for delivery Friday to the north gate"
        )
        response = self.client.post(
            "/webhook", json=inbound(self.sender, text, "complete-order")
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            orders = session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).all()
            self.assertEqual(len(orders), 1)
            self.assertEqual(orders[0].status, "pending_approval")
            self.assertIn("Item: cement", orders[0].text)
            self.assertIn("Quantity: 20 bags", orders[0].text)
            self.assertIn("Delivery Date: Friday", orders[0].text)
            self.assertIn("Drop Location: north gate", orders[0].text)
            self.assertEqual(session.query(storage.ConversationState).count(), 0)

    def test_incomplete_order_preserves_fields_and_asks_only_missing(self):
        text = "Order 20 bags of cement for delivery Friday"
        first = self.client.post(
            "/webhook", json=inbound(self.sender, text, "incomplete-order")
        )
        self.assertEqual(first.status_code, 200)
        with storage.SessionLocal() as session:
            order = session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).one()
            state = session.query(storage.ConversationState).one()
            self.assertTrue(order.text.startswith("[await:drop_location]"))
            self.assertIn("Item: cement", order.text)
            self.assertIn("Quantity: 20 bags", order.text)
            self.assertIn("Delivery Date: Friday", order.text)
            self.assertEqual(state.expected_field, "drop_location")
            order_id = order.id
            state_id = state.id

        second = self.client.post(
            "/webhook",
            json=inbound(self.sender, "north gate", "order-drop-location"),
        )
        self.assertEqual(second.status_code, 200)
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            state = session.get(storage.ConversationState, state_id)
            self.assertEqual(order.status, "pending_approval")
            self.assertIn("Drop Location: north gate", order.text)
            self.assertNotIn("[await:", order.text)
            self.assertFalse(state.active)
            self.assertEqual(state.status, "resolved")
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).count(), 1)

    def test_natural_meeting_routes_to_existing_meeting_handler(self):
        response = self.client.post(
            "/webhook",
            json=inbound(
                self.sender,
                "Schedule site meeting tomorrow at 2 PM",
                "natural-meeting",
            ),
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            meetings = session.query(storage.Meeting).all()
            self.assertEqual(len(meetings), 1)
            self.assertEqual(meetings[0].project_code, "PROJECT_A1")
            self.assertEqual(meetings[0].created_by, self.sender)
            self.assertEqual(session.query(storage.Task).count(), 0)


if __name__ == "__main__":
    unittest.main()
