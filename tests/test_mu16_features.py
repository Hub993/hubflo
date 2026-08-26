import datetime as dt
import json
import unittest
from zoneinfo import ZoneInfo

import app as hubflo_app
import storage
from tests.test_mu14_webhook import inbound


class MU16StructuredInterpretationTests(unittest.TestCase):
    def test_complete_natural_order_extracts_supplied_fields(self):
        result = hubflo_app.parse_natural_order(
            "Order 20 bags of cement from Buildit for PROJECT_A1 "
            "for delivery Friday to the north gate",
            "PROJECT_A1",
        )
        self.assertEqual(result["intent"], "order")
        self.assertEqual(result["fields"]["quantity"], "20 bags")
        self.assertEqual(result["fields"]["item"], "cement")
        self.assertEqual(result["fields"]["supplier"], "Buildit")
        self.assertEqual(result["fields"]["delivery_date"], "Friday")
        self.assertEqual(result["fields"]["drop_location"], "north gate")
        self.assertEqual(result["missing_fields"], [])

    def test_order_reports_only_fields_actually_missing(self):
        result = hubflo_app.parse_natural_order(
            "Order 20 bags of cement from Buildit for delivery Friday",
        )
        self.assertEqual(result["missing_fields"], ["drop_location"])
        self.assertEqual(result["fields"]["item"], "cement")
        self.assertEqual(result["fields"]["quantity"], "20 bags")
        self.assertEqual(result["fields"]["supplier"], "Buildit")
        self.assertEqual(result["fields"]["delivery_date"], "Friday")

    def test_order_reports_supplier_as_only_missing(self):
        result = hubflo_app.parse_natural_order(
            "Order 12 bags of mortar for delivery Saturday to the south gate.",
        )
        self.assertEqual(result["missing_fields"], ["supplier"])
        self.assertEqual(result["fields"]["item"], "mortar")
        self.assertEqual(result["fields"]["quantity"], "12 bags")
        self.assertEqual(result["fields"]["delivery_date"], "Saturday")
        self.assertEqual(result["fields"]["drop_location"], "south gate")

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

    def send(self, text, message_id):
        return self.client.post(
            "/webhook", json=inbound(self.sender, text, message_id)
        )

    def start_supplier_missing_order(self, message_id="supplier-missing"):
        original = (
            "Order 12 bags of mortar for delivery Saturday "
            "to the south gate."
        )
        response = self.send(original, message_id)
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            order = session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).one()
            state = session.query(storage.ConversationState).one()
            return original, order.id, state.id

    def make_retired_legacy_order_await(self):
        task = storage.create_task(
            self.sender,
            "[await:item]\nRetained legacy order",
            tag="order",
            project_code="PROJECT_A1",
        )
        state = storage.save_pending_conversation_state({
            "client_id": 9,
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
            state["id"], self.sender, 9, "PROJECT_A1", "cancelled"
        )
        return task, state

    def test_complete_order_mutates_once_without_clarification(self):
        text = (
            "Order 20 bags of cement from Buildit for PROJECT_A1 "
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
            self.assertIn("Supplier: Buildit", orders[0].text)
            self.assertIn("Delivery Date: Friday", orders[0].text)
            self.assertIn("Drop Location: north gate", orders[0].text)
            self.assertEqual(session.query(storage.ConversationState).count(), 0)

    def test_complete_orders_bypass_retired_legacy_await(self):
        legacy, retired_state = self.make_retired_legacy_order_await()
        created_order_ids = []
        messages = (
            (
                "Order 16 bags of sand from Buildit for delivery Tuesday "
                "to the east gate.",
                "complete-order-east",
                "Item: sand",
                "Quantity: 16 bags",
                "Delivery Date: Tuesday",
                "Drop Location: east gate",
            ),
            (
                "Order 9 pallets of block from Buildit for delivery Thursday "
                "to the west gate.",
                "complete-order-west",
                "Item: block",
                "Quantity: 9 pallets",
                "Delivery Date: Thursday",
                "Drop Location: west gate",
            ),
        )
        for text, message_id, item, quantity, delivery, location in messages:
            self.assertEqual(self.send(text, message_id).status_code, 200)
            with storage.SessionLocal() as session:
                order = session.query(storage.Task).filter(
                    storage.Task.tag == "order"
                ).order_by(storage.Task.id.desc()).first()
                self.assertEqual(order.status, "pending_approval")
                self.assertIn(item, order.text)
                self.assertIn(quantity, order.text)
                self.assertIn("Supplier: Buildit", order.text)
                self.assertIn(delivery, order.text)
                self.assertIn(location, order.text)
                created_order_ids.append(order.id)

        with storage.SessionLocal() as session:
            preserved = session.get(
                storage.ConversationState, retired_state["id"]
            )
            retained_task = session.get(storage.Task, legacy["id"])
            self.assertFalse(preserved.active)
            self.assertEqual(preserved.status, "cancelled")
            self.assertEqual(preserved.expected_field, "item")
            self.assertEqual(retained_task.status, "open")
            self.assertTrue(retained_task.text.startswith("[await:item]"))
            self.assertEqual(session.query(storage.ConversationState).filter(
                storage.ConversationState.active == True
            ).count(), 0)
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.tag == "order",
                storage.Task.status == "pending_approval",
            ).count(), 2)
            self.assertEqual(session.query(storage.Audit).filter(
                storage.Audit.action == "create",
                storage.Audit.ref_type == "task",
                storage.Audit.ref_id.in_(created_order_ids),
            ).count(), 2)

    def test_incomplete_order_bypasses_retired_legacy_await_and_continues(self):
        legacy, retired_state = self.make_retired_legacy_order_await()
        original = (
            "Order 10 bags of grout for delivery Saturday to the west gate."
        )
        self.assertEqual(self.send(
            original, "incomplete-order-retired-await"
        ).status_code, 200)

        with storage.SessionLocal() as session:
            order = session.query(storage.Task).filter(
                storage.Task.tag == "order",
                storage.Task.id != legacy["id"],
            ).one()
            pending = session.query(storage.ConversationState).filter(
                storage.ConversationState.active == True
            ).one()
            retired = session.get(
                storage.ConversationState, retired_state["id"]
            )
            self.assertEqual(order.status, "open")
            self.assertTrue(order.text.startswith("[await:supplier]"))
            self.assertIn("Item: grout", order.text)
            self.assertIn("Quantity: 10 bags", order.text)
            self.assertIn("Delivery Date: Saturday", order.text)
            self.assertIn("Drop Location: west gate", order.text)
            self.assertEqual(pending.expected_field, "supplier")
            self.assertEqual(pending.original_request, original)
            self.assertFalse(retired.active)
            self.assertEqual(retired.status, "cancelled")
            order_id = order.id
            pending_id = pending.id

        self.assertEqual(self.send(
            "Buildit", "incomplete-order-supplier-followup"
        ).status_code, 200)
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            pending = session.get(storage.ConversationState, pending_id)
            retired = session.get(
                storage.ConversationState, retired_state["id"]
            )
            retained_task = session.get(storage.Task, legacy["id"])
            self.assertEqual(order.status, "pending_approval")
            self.assertIn("Supplier: Buildit", order.text)
            self.assertIn("Delivery Date: Saturday", order.text)
            self.assertNotIn("[await:", order.text)
            self.assertFalse(pending.active)
            self.assertEqual(pending.status, "resolved")
            self.assertFalse(retired.active)
            self.assertEqual(retired.status, "cancelled")
            self.assertEqual(retained_task.status, "open")
            self.assertTrue(retained_task.text.startswith("[await:item]"))
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.id == order_id
            ).count(), 1)

    def test_incomplete_order_preserves_fields_and_asks_only_missing(self):
        text = "Order 20 bags of cement from Buildit for delivery Friday"
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
            self.assertIn("Supplier: Buildit", order.text)
            self.assertIn("Delivery Date: Friday", order.text)
            self.assertEqual(order.status, "open")
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
            self.assertIn("Supplier: Buildit", order.text)
            self.assertIn("Drop Location: north gate", order.text)
            self.assertNotIn("[await:", order.text)
            self.assertFalse(state.active)
            self.assertEqual(state.status, "resolved")
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).count(), 1)

    def test_supplier_missing_persists_only_supplier_and_original_meaning(self):
        original, order_id, state_id = self.start_supplier_missing_order()
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            state = session.get(storage.ConversationState, state_id)
            context = json.loads(state.structured_context_json)
            self.assertEqual(order.status, "open")
            self.assertEqual(order.order_state, "requested")
            self.assertTrue(order.text.startswith("[await:supplier]"))
            self.assertIn("Item: mortar", order.text)
            self.assertIn("Quantity: 12 bags", order.text)
            self.assertIn("Delivery Date: Saturday", order.text)
            self.assertIn("Drop Location: south gate", order.text)
            self.assertNotIn("Supplier:", order.text)
            self.assertTrue(state.active)
            self.assertEqual(state.status, "active")
            self.assertEqual(state.expected_field, "supplier")
            self.assertEqual(state.original_request, original)
            self.assertEqual(context["missing_fields"], ["supplier"])
            self.assertEqual(context["order_fields"], {
                "item": "mortar",
                "quantity": "12 bags",
                "supplier": None,
                "delivery_date": "Saturday",
                "drop_location": "south gate",
            })

    def test_direct_supplier_followup_completes_once_and_preserves_fields(self):
        _, order_id, state_id = self.start_supplier_missing_order()
        before = None
        with storage.SessionLocal() as session:
            before = session.get(storage.Task, order_id).text

        response = self.send("Buildit", "supplier-followup")
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            state = session.get(storage.ConversationState, state_id)
            self.assertEqual(order.status, "pending_approval")
            self.assertIn("Item: mortar", order.text)
            self.assertIn("Quantity: 12 bags", order.text)
            self.assertIn("Supplier: Buildit", order.text)
            self.assertIn("Delivery Date: Saturday", order.text)
            self.assertIn("Drop Location: south gate", order.text)
            self.assertNotIn("[await:", order.text)
            self.assertFalse(state.active)
            self.assertEqual(state.status, "resolved")
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).count(), 1)
            self.assertNotEqual(order.text, before)

    def test_invalid_supplier_followup_does_not_mutate_or_finalize(self):
        _, order_id, state_id = self.start_supplier_missing_order()
        with storage.SessionLocal() as session:
            before = session.get(storage.Task, order_id).text

        response = self.send("???", "invalid-supplier-followup")
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            state = session.get(storage.ConversationState, state_id)
            self.assertEqual(order.text, before)
            self.assertEqual(order.status, "open")
            self.assertTrue(state.active)
            self.assertEqual(state.status, "active")
            self.assertEqual(state.expected_field, "supplier")

    def test_unrelated_deterministic_command_bypasses_supplier_state(self):
        _, order_id, state_id = self.start_supplier_missing_order()
        with storage.SessionLocal() as session:
            before = session.get(storage.Task, order_id).text

        response = self.send("Search for mortar tasks", "supplier-bypass")
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            state = session.get(storage.ConversationState, state_id)
            self.assertEqual(order.text, before)
            self.assertEqual(order.status, "open")
            self.assertTrue(state.active)
            self.assertEqual(state.expected_field, "supplier")
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).count(), 1)

    def test_replayed_supplier_continuation_does_not_duplicate_order(self):
        _, order_id, state_id = self.start_supplier_missing_order()
        self.send("Buildit", "supplier-replay")
        self.send("Buildit", "supplier-replay")
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            state = session.get(storage.ConversationState, state_id)
            self.assertEqual(order.status, "pending_approval")
            self.assertEqual(order.text.count("Supplier: Buildit"), 1)
            self.assertFalse(state.active)
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).count(), 1)

    def test_client_change_blocks_pending_order_continuation(self):
        _, order_id, state_id = self.start_supplier_missing_order()
        with storage.SessionLocal() as session:
            sender = session.query(storage.User).filter(
                storage.User.wa_id == self.sender
            ).one()
            sender.client_id = 99
            session.commit()
        self.send("Buildit", "supplier-client-change")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.Task, order_id).status, "open")
            self.assertTrue(session.get(storage.ConversationState, state_id).active)

    def test_project_change_blocks_pending_order_continuation(self):
        _, order_id, state_id = self.start_supplier_missing_order()
        with storage.SessionLocal() as session:
            sender = session.query(storage.User).filter(
                storage.User.wa_id == self.sender
            ).one()
            sender.project_code = "PROJECT_A2"
            session.commit()
        self.send("Buildit", "supplier-project-change")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.Task, order_id).status, "open")
            self.assertTrue(session.get(storage.ConversationState, state_id).active)

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
