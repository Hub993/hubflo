import datetime as dt
import unittest

import app as hubflo_app
import storage
from tests.test_mu14_webhook import inbound


class MU17RoutingMatrixTests(unittest.TestCase):
    def route(self, text, project="PROJECT_A1"):
        return hubflo_app.interpret_supported_message(text, project)

    def test_complete_supported_route_matrix(self):
        cases = {
            "Create a task to fix the door": "task",
            "Assign Jordan Unique to fix the door": "task",
            "Create a task for me to check the door": "task",
            "Urgent fix the kitchen door": "task",
            "Urgent: fix the kitchen door": "task",
            "Note for PROJECT_A1: opening is 32 inches": "note",
            "Pin note for PROJECT_A1: opening is 32 inches": "pinned_note",
            "Pin note: scaffold inspection booked for tomorrow": "pinned_note",
            "PIN NOTE scaffold inspection booked for tomorrow": "pinned_note",
            "Order 20 bags of cement for delivery Friday to north gate": "order",
            "Record delivery of cement at north gate": "delivery",
            "Approve change order 12": "approval",
            "Add 10 bags of cement to stock": "stock",
            "Book inspection for drywall tomorrow": "inspection",
            "Framing crew is delayed by 2 days - weather": "delay",
            "Remind me tomorrow at 9 AM to call": "reminder",
            "Schedule site meeting tomorrow at 2 PM": "meeting",
            "Search for drywall tasks": "search",
            "What is the status for PROJECT_A1": "status",
        }
        for text, expected in cases.items():
            self.assertEqual(self.route(text)["route"], expected, text)

    def test_task_subtypes_and_context_entities_converge(self):
        self.assertEqual(self.route(
            "Assign Jordan Unique to fix the door"
        )["subtype"], "assigned")
        self.assertEqual(self.route(
            "Create a task for me to check the door"
        )["subtype"], "self")
        self.assertEqual(self.route(
            "Urgent fix the kitchen door"
        )["subtype"], "urgent")
        self.assertEqual(self.route(
            "Urgent: fix the kitchen door"
        )["subtype"], "urgent")
        result = self.route(
            "Create a task for phase framing in zone Z2 for trade electrical "
            "on project PROJECT_A1"
        )
        self.assertEqual(result["entities"]["project"], "project_a1")
        self.assertEqual(result["entities"]["phase"], "framing")
        self.assertEqual(result["entities"]["zone"], "z2")
        self.assertEqual(result["entities"]["trade"], "electrical")

    def test_ordinary_fallback_and_generic_order_words_do_not_false_route(self):
        self.assertEqual(self.route("The supplier mentioned a quantity today")["route"],
                         "ordinary_fallback")
        self.assertEqual(self.route("Hello there")["route"], "ordinary_fallback")


class MU17WebhookConvergenceTests(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender_a = "15550000400"
        self.sender_b = "15550000401"
        self.jordan = "15550000402"
        with storage.SessionLocal() as session:
            session.add_all([
                storage.User(
                    client_id=10, wa_id=self.sender_a, name="Client A PM",
                    role="pm", project_code="PROJECT_A1", active=True,
                ),
                storage.User(
                    client_id=11, wa_id=self.sender_b, name="Client B PM",
                    role="pm", project_code="PROJECT_A1", active=True,
                ),
                storage.User(
                    client_id=10, wa_id=self.jordan, name="Jordan Unique",
                    role="sub", project_code="PROJECT_A1", active=True,
                ),
            ])
            session.commit()

    def send(self, sender, text, message_id):
        return self.client.post(
            "/webhook", json=inbound(sender, text, message_id)
        )

    def test_task_note_delivery_and_assignment_routes_use_task_handler(self):
        messages = (
            ("Create a task to check the door", "task", "assigned", "open"),
            ("Create a task for me to check the door", "task", "self", "open"),
            ("Urgent fix the kitchen door", "urgent", "urgent", "open"),
            ("Urgent: fix the kitchen door", "urgent", "urgent", "open"),
            ("Note for PROJECT_A1: opening is 32 inches", "note", "note", "open"),
            ("Pin note for PROJECT_A1: opening is 32 inches", "note", "pinned", "open"),
            ("Record delivery of cement at north gate", "delivery", "recorded", "done"),
        )
        for index, (text, tag, subtype, status) in enumerate(messages):
            with storage.SessionLocal() as session:
                before = session.query(storage.Task).count()
            self.assertEqual(self.send(
                self.sender_a, text, f"route-{index}"
            ).status_code, 200)
            with storage.SessionLocal() as session:
                self.assertEqual(session.query(storage.Task).count(), before + 1)
                row = session.query(storage.Task).order_by(
                    storage.Task.id.desc()
                ).first()
                self.assertEqual(row.tag, tag)
                self.assertEqual(row.subtype, subtype)
                self.assertEqual(row.status, status)
                self.assertEqual(row.client_id, 10)
                self.assertEqual(row.project_code, "PROJECT_A1")
                self.assertEqual(session.query(storage.Audit).filter(
                    storage.Audit.action == "create",
                    storage.Audit.ref_type == "task",
                    storage.Audit.ref_id == row.id,
                ).count(), 1)

        with storage.SessionLocal() as session:
            before = session.query(storage.Task).count()
        self.assertEqual(self.send(
            self.sender_a,
            "Assign Jordan Unique to fix the kitchen door",
            "assigned-task",
        ).status_code, 200)
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Task).count(), before + 1)
            assigned = session.query(storage.Task).order_by(
                storage.Task.id.desc()
            ).first()
            self.assertEqual(assigned.pm_wa_id, self.jordan)
            self.assertEqual(assigned.client_id, 10)

    def test_pinned_note_precedence_preserves_content_and_isolation(self):
        text = "Pin note: scaffold inspection booked for tomorrow"
        self.assertEqual(self.send(
            self.sender_a, text, "pinned-inspection-note"
        ).status_code, 200)

        with storage.SessionLocal() as session:
            notes = session.query(storage.Task).filter(
                storage.Task.tag == "note",
                storage.Task.subtype == "pinned",
            ).all()
            self.assertEqual(len(notes), 1)
            note = notes[0]
            self.assertEqual(note.text, text)
            self.assertEqual(note.client_id, 10)
            self.assertEqual(note.project_code, "PROJECT_A1")
            self.assertEqual(session.query(storage.Audit).filter(
                storage.Audit.action == "create",
                storage.Audit.ref_type == "task",
                storage.Audit.ref_id == note.id,
            ).count(), 1)
            self.assertEqual(session.query(storage.Inspection).count(), 0)
            self.assertEqual(session.query(storage.DelayLog).count(), 0)
            self.assertEqual(session.query(storage.StockItem).count(), 0)
            self.assertEqual(session.query(storage.PMReminder).count(), 0)
            self.assertEqual(session.query(storage.Meeting).count(), 0)

    def test_approval_enforces_client_and_project_before_handler(self):
        own = storage.create_task(
            self.sender_a, "Change order fixture", tag="order",
            project_code="PROJECT_A1", order_state="pending_approval",
        )
        foreign = storage.create_task(
            self.sender_b, "Foreign change order", tag="order",
            project_code="PROJECT_A1", order_state="pending_approval",
        )
        self.send(self.sender_a, f"Approve change order {foreign['id']}", "foreign-approval")
        self.send(self.sender_a, f"Approve change order {own['id']}", "own-approval")
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.Task, foreign["id"]).status, "open")
            self.assertEqual(session.get(storage.Task, own["id"]).status, "approved")

    def test_same_named_stock_is_isolated_by_client(self):
        first = storage.adjust_stock({
            "name": "cement", "unit": "bags", "delta": 10,
            "actor": self.sender_a, "project_code": "PROJECT_A1",
        })
        second = storage.adjust_stock({
            "name": "cement", "unit": "bags", "delta": 3,
            "actor": self.sender_b, "project_code": "PROJECT_A1",
        })
        self.assertNotEqual(first["item_id"], second["item_id"])
        with storage.SessionLocal() as session:
            rows = session.query(storage.StockItem).order_by(
                storage.StockItem.client_id
            ).all()
            self.assertEqual([(row.client_id, row.current_qty) for row in rows],
                             [(10, 10.0), (11, 3.0)])

    def test_natural_stock_case_variants_reuse_client_catalogue_identity(self):
        mixed_name = "RTW6-50AA0DF STOCK FIXTURE CEMENT"
        own = storage.create_stock_item({
            "name": mixed_name,
            "unit": "bags",
            "opening_qty": 100,
            "actor": self.sender_a,
            "project_code": "PROJECT_A1",
        })
        foreign = storage.create_stock_item({
            "name": mixed_name,
            "unit": "bags",
            "opening_qty": 40,
            "actor": self.sender_b,
            "project_code": "PROJECT_A1",
        })

        with storage.SessionLocal() as session:
            task_count = session.query(storage.Task).count()
            own_movements = session.query(storage.StockMovement).filter(
                storage.StockMovement.stock_item_id == own["id"]
            ).count()

        commands = (
            "Add 2 bags of rtw6-50aa0df stock fixture cement to stock",
            "Add 3 bags of RTW6-50AA0DF STOCK FIXTURE CEMENT to stock",
        )
        for index, command in enumerate(commands):
            self.assertEqual(self.send(
                self.sender_a, command, f"stock-case-{index}"
            ).status_code, 200)
            with storage.SessionLocal() as session:
                own_row = session.get(storage.StockItem, own["id"])
                foreign_row = session.get(storage.StockItem, foreign["id"])
                self.assertEqual(session.query(storage.StockItem).filter(
                    storage.StockItem.client_id == 10
                ).count(), 1)
                self.assertEqual(own_row.current_qty, 102.0 + (3.0 * index))
                self.assertEqual(foreign_row.current_qty, 40.0)
                movements = session.query(storage.StockMovement).filter(
                    storage.StockMovement.stock_item_id == own["id"]
                ).order_by(storage.StockMovement.id.asc()).all()
                self.assertEqual(len(movements), own_movements + index + 1)
                self.assertEqual(movements[-1].qty_change, 2.0 + index)
                self.assertEqual(session.query(storage.Task).count(), task_count)

        self.assertEqual(self.send(
            self.sender_a,
            "Add 4 bags of rtw6-50aa0df genuinely new aggregate to stock",
            "stock-new-name",
        ).status_code, 200)
        with storage.SessionLocal() as session:
            own_rows = session.query(storage.StockItem).filter(
                storage.StockItem.client_id == 10
            ).all()
            self.assertEqual(len(own_rows), 2)
            created = next(row for row in own_rows if row.id != own["id"])
            self.assertEqual(created.name, "rtw6-50aa0df genuinely new aggregate")
            self.assertEqual(created.current_qty, 4.0)
            movements = session.query(storage.StockMovement).filter(
                storage.StockMovement.stock_item_id == created.id
            ).all()
            self.assertEqual(len(movements), 1)
            self.assertEqual(movements[0].qty_change, 4.0)
            self.assertEqual(session.query(storage.Task).count(), task_count)

    def test_cross_client_delay_numeric_id_is_denied(self):
        foreign = storage.create_task(
            self.sender_b, "Framing", tag="task", project_code="PROJECT_A1"
        )
        result = storage.log_delay({
            "task_id": foreign["id"], "project_code": "PROJECT_A1",
            "reporter": self.sender_a, "days": 2, "reason": "weather",
        })
        self.assertEqual(result["code"], "client_mismatch")
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.DelayLog).count(), 0)

    def test_status_is_read_only(self):
        storage.create_task(
            self.sender_a, "Status fixture", tag="task", project_code="PROJECT_A1"
        )
        with storage.SessionLocal() as session:
            before = session.query(storage.Task).count()
        response = self.send(
            self.sender_a, "What is the status for PROJECT_A1", "status-read"
        )
        self.assertEqual(response.status_code, 200)
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Task).count(), before)


if __name__ == "__main__":
    unittest.main()
