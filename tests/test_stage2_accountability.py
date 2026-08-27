import datetime as dt
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, inspect, text

import app as hubflo_app
import storage
from tests.test_mu14_webhook import inbound


class Stage2AccountabilityTests(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender = "15550000700"
        self.jordan = "15550000701"
        with storage.SessionLocal() as session:
            sender = storage.User(
                client_id=30, wa_id=self.sender, name="Accountable PM",
                role="pm", project_code="PROJECT_A1", active=True,
                timezone="America/New_York",
            )
            jordan = storage.User(
                client_id=30, wa_id=self.jordan, name="Jordan Unique",
                role="sub", project_code="PROJECT_A1", active=True,
                timezone="America/New_York",
            )
            session.add_all([sender, jordan])
            session.flush()
            session.add(storage.PMProjectMap(
                client_id=30, pm_user_id=sender.id,
                project_code="PROJECT_A1", primary_pm=True,
            ))
            session.commit()
        sender_patch = patch.object(
            hubflo_app, "send_whatsapp_text", return_value=(True, {})
        )
        sender_patch.start()
        self.addCleanup(sender_patch.stop)

    def send(self, text, message_id):
        return self.client.post(
            "/webhook", json=inbound(self.sender, text, message_id)
        )

    def responsibilities(self, accountable_wa, projects=("PROJECT_A1",)):
        return storage.get_personal_responsibilities(
            accountable_wa, 30, projects
        )

    def retired_legacy_await(self, reason, marker="drop_location"):
        task = storage.create_task(
            self.sender,
            f"[await:{marker}]\nLegacy compatibility fixture",
            tag="order",
            project_code="PROJECT_A1",
        )
        state = storage.save_pending_conversation_state({
            "client_id": 30,
            "sender": self.sender,
            "project_code": "PROJECT_A1",
            "state_kind": "await",
            "expected_field": marker,
            "original_request": "Legacy compatibility fixture",
            "structured_context": {"source_record_id": task["id"]},
            "candidate_metadata": {},
            "continuation": {
                "source_record_id": task["id"],
                "source_record_type": "pending_business_state",
            },
            "continuation_key": f"business-state:{task['id']}",
        })
        retired = storage.retire_conversation_state(
            state["id"], self.sender, 30, "PROJECT_A1", reason
        )
        self.assertEqual(retired["status_result"], "retired")
        return task, state

    def test_ordinary_actionable_is_sender_owned_and_factual_is_not_task(self):
        self.assertEqual(self.send(
            "Check the generator", "accountability-ordinary"
        ).status_code, 200)
        for index, factual in enumerate((
            "Crew discussed tomorrow's access",
            "The generator was checked this morning",
            "Paint is dry",
            "Review was completed",
            "Update from site: the roof is complete",
        )):
            self.assertEqual(self.send(
                factual, f"accountability-factual-{index}"
            ).status_code, 200)

        with storage.SessionLocal() as session:
            tasks = session.query(storage.Task).all()
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0].text, "Check the generator")
            self.assertEqual(tasks[0].pm_wa_id, self.sender)
        responsibilities = self.responsibilities(self.sender)
        self.assertEqual([row["id"] for row in responsibilities["tasks"]], [1])

    def test_authoritative_industry_actionability_variants(self):
        messages = (
            "Check the generator",
            "Please check the generator",
            "I will check the generator",
            "Jordan Unique check the generator",
        )
        for index, message in enumerate(messages):
            self.send(message, f"accountability-actionability-{index}")
        with storage.SessionLocal() as session:
            rows = session.query(storage.Task).order_by(storage.Task.id).all()
            self.assertEqual(len(rows), 4)
            self.assertEqual(
                [row.pm_wa_id for row in rows],
                [self.sender, self.sender, self.sender, self.jordan],
            )
            self.assertEqual(rows[2].subtype, "self")

    def test_assigned_self_urgent_and_project_oversight_visibility(self):
        messages = (
            "Assign Jordan Unique to check the generator",
            "Jordan Unique check the loading gate",
            "Create a task for me to check the gate",
            "Urgent fix the roof leak",
        )
        for index, text in enumerate(messages):
            self.send(text, f"accountability-task-{index}")

        with storage.SessionLocal() as session:
            rows = session.query(storage.Task).order_by(storage.Task.id).all()
            self.assertEqual(
                [row.pm_wa_id for row in rows],
                [self.jordan, self.jordan, self.sender, self.sender],
            )
            self.assertEqual(rows[2].subtype, "self")
            self.assertEqual(rows[3].tag, "urgent")

        jordan = self.responsibilities(self.jordan)
        self.assertEqual([row["id"] for row in jordan["tasks"]], [1, 2])
        sub_digest = self.client.get(f"/admin/digest/sub?sender={self.jordan}")
        self.assertEqual(sub_digest.status_code, 200)
        self.assertEqual(
            [row["id"] for row in sub_digest.get_json()["tasks"]], [2, 1]
        )
        pm_digest = self.client.get(f"/admin/digest/pm?pm={self.sender}")
        self.assertEqual(pm_digest.status_code, 200)
        self.assertEqual(pm_digest.get_json()["total_open"], 4)

    def test_factual_default_does_not_bypass_await_or_clarification(self):
        awaiting = storage.create_task(
            self.sender,
            "[await:new_stock_qty] material=grout;unit=bags",
            tag="stock", project_code="PROJECT_A1", subtype="assigned",
        )
        state = storage.save_pending_conversation_state({
            "client_id": 30,
            "sender": self.sender,
            "project_code": "PROJECT_A1",
            "state_kind": "await",
            "expected_field": "new_stock_qty",
            "original_request": "Add new stock item: grout",
            "structured_context": {"source_record_id": awaiting["id"]},
            "candidate_metadata": {},
            "continuation": {"source_record_id": awaiting["id"]},
            "continuation_key": f"business-state:{awaiting['id']}",
        })
        before = awaiting["text"]
        self.send("Crew discussed tomorrow's access", "factual-await")
        self.send("Check the generator", "actionable-default-await")
        self.send(
            "Book inspection for drywall tomorrow",
            "deterministic-specialized-await-bypass",
        )
        with storage.SessionLocal() as session:
            self.assertEqual(session.get(storage.Task, awaiting["id"]).text, before)
            self.assertTrue(session.get(storage.ConversationState, state["id"]).active)
            self.assertEqual(session.query(storage.Task).count(), 1)
            self.assertEqual(session.query(storage.Inspection).count(), 1)

        storage.retire_conversation_state(
            state["id"], self.sender, 30, "PROJECT_A1", "cancelled"
        )
        clarification = storage.save_pending_conversation_state({
            "client_id": 30,
            "sender": self.sender,
            "project_code": "PROJECT_A1",
            "state_kind": "clarification",
            "expected_field": "fixture",
            "original_request": "fixture",
            "structured_context": {},
            "candidate_metadata": {"candidates": [1, 2]},
            "continuation": {"kind": "fixture"},
            "continuation_key": "accountability-clarification",
        })
        self.send("Crew discussed tomorrow's access", "factual-clarification")
        self.send("Please check the generator", "actionable-default-clarification")
        with storage.SessionLocal() as session:
            current = session.get(storage.ConversationState, clarification["id"])
            self.assertTrue(current.active)
            self.assertEqual(session.query(storage.Task).count(), 1)

    def test_retired_legacy_await_tombstones_cannot_consume_normal_tasks(self):
        for index, reason in enumerate(("abandoned", "cancelled", "restarted")):
            legacy, state = self.retired_legacy_await(reason)
            message = f"Check the generator {index + 1}"
            self.send(message, f"retired-await-actionable-{reason}")
            with storage.SessionLocal() as session:
                compatibility = session.get(storage.Task, legacy["id"])
                lifecycle = session.get(storage.ConversationState, state["id"])
                created = session.query(storage.Task).filter(
                    storage.Task.text == message
                ).one()
                self.assertEqual(compatibility.status, "open")
                self.assertTrue(compatibility.text.startswith("[await:drop_location]"))
                self.assertFalse(lifecycle.active)
                self.assertEqual(lifecycle.status, reason)
                self.assertEqual(created.tag, "task")
                self.assertEqual(created.pm_wa_id, self.sender)

        legacy, state = self.retired_legacy_await("abandoned")
        before_count = None
        with storage.SessionLocal() as session:
            before_count = session.query(storage.Task).count()
        self.send(
            "Crew discussed tomorrow's access",
            "retired-await-factual",
        )
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Task).count(), before_count)
            self.assertTrue(
                session.get(storage.Task, legacy["id"]).text.startswith(
                    "[await:drop_location]"
                )
            )
            lifecycle = session.get(storage.ConversationState, state["id"])
            self.assertFalse(lifecycle.active)
            self.assertEqual(lifecycle.status, "abandoned")

    def test_order_continuation_and_change_order_keep_originating_owner(self):
        self.send(
            "Order 10 bags of grout for delivery Saturday to the west gate",
            "accountability-order",
        )
        with storage.SessionLocal() as session:
            order = session.query(storage.Task).filter(
                storage.Task.tag == "order"
            ).one()
            self.assertEqual(order.pm_wa_id, self.sender)
            order_id = order.id
        self.send("Buildit", "accountability-order-supplier")
        with storage.SessionLocal() as session:
            order = session.get(storage.Task, order_id)
            self.assertEqual(order.pm_wa_id, self.sender)
            self.assertEqual(order.status, "pending_approval")

        prerequisite = storage.create_task(
            self.sender, "Open order prerequisite", tag="order",
            project_code="PROJECT_A1", assignee_wa=self.sender,
        )
        self.send("Change order: add temporary shoring", "accountability-change")
        with storage.SessionLocal() as session:
            change = session.query(storage.Task).filter(
                storage.Task.tag == "change"
            ).one()
            self.assertEqual(change.pm_wa_id, self.sender)
            change_id = change.id
        self.send(f"Approve change order {change_id}", "accountability-approve")
        with storage.SessionLocal() as session:
            change = session.get(storage.Task, change_id)
            self.assertEqual(change.pm_wa_id, self.sender)
            self.assertEqual(change.status, "approved")
            audit = session.query(storage.Audit).filter(
                storage.Audit.action == "approve",
                storage.Audit.ref_id == change_id,
            ).one()
            self.assertEqual(audit.actor, self.sender)
            self.assertNotEqual(prerequisite["id"], change_id)

    def test_inspection_and_meeting_accountability_visibility(self):
        self.send(
            "Book inspection for drywall tomorrow", "accountability-inspection"
        )
        self.send(
            "Schedule site meeting tomorrow at 2 PM", "accountability-meeting"
        )
        with storage.SessionLocal() as session:
            inspection = session.query(storage.Inspection).one()
            meeting = session.query(storage.Meeting).one()
            self.assertEqual(inspection.accountable_wa, self.sender)
            self.assertIsNone(inspection.inspector)
            self.assertEqual(meeting.created_by, self.sender)
        spoofed = storage.create_inspection({
            "actor": self.sender,
            "client_id": 999,
            "accountable_wa": self.jordan,
            "project_code": "PROJECT_A1",
            "phase": "roofing",
            "required_date": dt.datetime.utcnow(),
        })
        denied = storage.create_inspection({
            "actor": self.sender,
            "project_code": "PROJECT_B1",
            "phase": "roofing",
            "required_date": dt.datetime.utcnow(),
        })
        self.assertIn("id", spoofed)
        self.assertEqual(denied, {"error": "inspection project not authorized"})
        with storage.SessionLocal() as session:
            canonical = session.get(storage.Inspection, spoofed["id"])
            self.assertEqual(canonical.client_id, 30)
            self.assertEqual(canonical.accountable_wa, self.sender)
        responsibilities = self.responsibilities(self.sender)
        self.assertEqual(len(responsibilities["inspections"]), 2)
        self.assertEqual(len(responsibilities["meetings"]), 1)
        endpoint = self.client.get(
            f"/admin/responsibility.json?wa={self.sender}"
        )
        self.assertEqual(endpoint.status_code, 200)
        self.assertEqual(endpoint.get_json()["accountable_wa"], self.sender)

    def test_delivery_action_and_record_have_distinct_accountability(self):
        self.send(
            "Deliver the cement to the north gate tomorrow",
            "accountability-delivery-action",
        )
        self.send(
            "Assign Jordan Unique to deliver grout to the east gate tomorrow",
            "accountability-delivery-assigned",
        )
        self.send(
            "Record delivery of cement at the north gate",
            "accountability-delivery-record",
        )
        self.send(
            "Cement was delivered at the south gate",
            "accountability-delivery-factual",
        )
        with storage.SessionLocal() as session:
            rows = session.query(storage.Task).filter(
                storage.Task.tag == "delivery"
            ).order_by(storage.Task.id).all()
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[0].pm_wa_id, self.sender)
            self.assertEqual(rows[0].status, "open")
            self.assertEqual(rows[1].pm_wa_id, self.jordan)
            self.assertEqual(rows[1].status, "open")
            for row in rows[2:]:
                self.assertEqual(row.subtype, "recorded")
                self.assertEqual(row.status, "done")
                self.assertIsNotNone(row.completed_at)
                self.assertIsNone(row.pm_wa_id)
            self.assertEqual(session.query(storage.Task).filter(
                storage.Task.tag == "delivery",
                storage.Task.status == "open",
                storage.Task.pm_wa_id == None,
            ).count(), 0)

    def test_pending_approval_is_visible_to_owner_and_project_pm(self):
        task = storage.create_task(
            self.sender, "Pending approval accountability", tag="order",
            project_code="PROJECT_A1", assignee_wa=self.sender,
            status="pending_approval",
        )
        responsibilities = self.responsibilities(self.sender)
        self.assertIn(task["id"], [row["id"] for row in responsibilities["tasks"]])
        pm_digest = self.client.get(f"/admin/digest/pm?pm={self.sender}")
        self.assertIn(str(task["id"]), pm_digest.get_json()["preview_text"])

    def test_backfill_is_deterministic_auditable_idempotent_and_bounded(self):
        safe_rows = [
            storage.create_task(
                self.sender, "Check the generator", tag="task",
                subtype="assigned", project_code="PROJECT_A1",
            ),
            storage.create_task(
                self.sender, "I will check the gate", tag="task",
                subtype="self", project_code="PROJECT_A1",
            ),
            storage.create_task(
                self.sender, "Urgent repair", tag="urgent",
                subtype="urgent", project_code="PROJECT_A1",
            ),
            storage.create_task(
                self.sender, "Legacy order", tag="order",
                subtype="assigned", project_code="PROJECT_A1",
            ),
            storage.create_task(
                self.sender, "Legacy change", tag="change",
                subtype="assigned", project_code="PROJECT_A1",
            ),
        ]
        ambiguous_assigned = storage.create_task(
            self.sender, "Assign Jordan Unique to check the roof", tag="task",
            subtype="assigned", project_code="PROJECT_A1",
        )
        factual = storage.create_task(
            self.sender, "Crew discussed tomorrow's access", tag="task",
            subtype="assigned", project_code="PROJECT_A1",
        )
        delivery = storage.create_task(
            self.sender, "Legacy delivery", tag="delivery",
            subtype="assigned", project_code="PROJECT_A1",
        )
        unauthorized_project = storage.create_task(
            self.sender, "Check the unauthorized gate", tag="task",
            subtype="assigned", project_code="PROJECT_B1",
        )
        with storage.SessionLocal() as session:
            wrong_tenant = storage.Task(
                client_id=31, sender=self.sender,
                text="Check the foreign gate", tag="task",
                subtype="assigned", project_code="PROJECT_A1",
            )
            session.add(wrong_tenant)
            session.commit()
            wrong_tenant_id = wrong_tenant.id
        with storage.SessionLocal() as session:
            legacy_inspection = storage.Inspection(
                client_id=30, project_code="PROJECT_A1",
                phase="legacy", required_date=dt.datetime.utcnow(),
                inspector=None, notes="legacy",
            )
            session.add(legacy_inspection)
            session.commit()
            inspection_id = legacy_inspection.id

        plan = hubflo_app.reconcile_stage2_accountability(dry_run=True)
        expected_safe = {row["id"] for row in safe_rows}
        self.assertEqual(
            {row["id"] for row in plan["safe_backfill"]}, expected_safe
        )
        review = {
            (row["object_type"], row["id"], row["reason"])
            for row in plan["requires_review"]
        }
        self.assertIn(
            ("task", ambiguous_assigned["id"], "legacy_assigned_intent_ambiguous"),
            review,
        )
        self.assertIn(
            ("task", factual["id"], "legacy_actionability_ambiguous"), review
        )
        self.assertIn(
            ("task", delivery["id"], "historical_delivery_semantics_ambiguous"),
            review,
        )
        self.assertIn(
            ("inspection", inspection_id, "accountable_creator_not_persisted"),
            review,
        )
        self.assertIn(
            (
                "task", unauthorized_project["id"],
                "sender_project_not_authorized",
            ),
            review,
        )
        self.assertIn(
            ("task", wrong_tenant_id, "sender_tenant_not_authorized"),
            review,
        )

        applied = hubflo_app.reconcile_stage2_accountability(dry_run=False)
        self.assertEqual(set(applied["changed_task_ids"]), expected_safe)
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Audit).filter(
                storage.Audit.action == "accountability_backfill"
            ).count(), len(expected_safe))
            for task_id in expected_safe:
                self.assertEqual(session.get(storage.Task, task_id).pm_wa_id,
                                 self.sender)
            self.assertIsNone(
                session.get(storage.Task, ambiguous_assigned["id"]).pm_wa_id
            )
            self.assertIsNone(session.get(storage.Task, factual["id"]).pm_wa_id)
            self.assertIsNone(session.get(storage.Task, delivery["id"]).pm_wa_id)

        repeated = hubflo_app.reconcile_stage2_accountability(dry_run=False)
        self.assertEqual(repeated["changed_task_ids"], [])
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Audit).filter(
                storage.Audit.action == "accountability_backfill"
            ).count(), len(expected_safe))

    def test_informational_records_are_excluded_from_personal_work(self):
        storage.create_task(
            self.sender, "Pinned fact", tag="note", subtype="pinned",
            project_code="PROJECT_A1",
        )
        storage.create_task(
            self.sender, "Completed delivery fact", tag="delivery",
            subtype="recorded", project_code="PROJECT_A1", status="done",
            completed_at=dt.datetime.utcnow(),
        )
        responsibilities = self.responsibilities(self.sender)
        self.assertEqual(responsibilities["tasks"], [])

    def test_responsibility_views_are_client_and_project_scoped(self):
        foreign_sender = "15550000702"
        with storage.SessionLocal() as session:
            session.add(storage.User(
                client_id=31, wa_id=foreign_sender, name="Jordan Unique",
                role="sub", project_code="PROJECT_A1", active=True,
            ))
            session.commit()
        authorized = storage.create_task(
            self.sender, "Check the authorized gate", tag="task",
            project_code="PROJECT_A1", assignee_wa=self.jordan,
        )
        storage.create_task(
            self.sender, "Check the unauthorized project", tag="task",
            project_code="PROJECT_B1", assignee_wa=self.jordan,
        )
        storage.create_task(
            foreign_sender, "Check the foreign tenant", tag="task",
            project_code="PROJECT_A1", assignee_wa=self.jordan,
        )
        with storage.SessionLocal() as session:
            session.add_all([
                storage.Inspection(
                    client_id=30, project_code="PROJECT_A1",
                    phase="authorized", accountable_wa=self.jordan,
                ),
                storage.Inspection(
                    client_id=30, project_code="PROJECT_B1",
                    phase="unauthorized", accountable_wa=self.jordan,
                ),
                storage.Inspection(
                    client_id=31, project_code="PROJECT_A1",
                    phase="foreign", accountable_wa=self.jordan,
                ),
                storage.Meeting(
                    client_id=30, project_code="PROJECT_A1",
                    title="Authorized", created_by=self.jordan,
                    status="scheduled",
                ),
                storage.Meeting(
                    client_id=30, project_code="PROJECT_B1",
                    title="Unauthorized", created_by=self.jordan,
                    status="scheduled",
                ),
                storage.Meeting(
                    client_id=31, project_code="PROJECT_A1",
                    title="Foreign", created_by=self.jordan,
                    status="scheduled",
                ),
            ])
            session.commit()

        direct = self.responsibilities(self.jordan)
        self.assertEqual(
            [row["id"] for row in direct["tasks"]], [authorized["id"]]
        )
        self.assertEqual(
            [row["phase"] for row in direct["inspections"]], ["authorized"]
        )
        self.assertEqual(
            [row["title"] for row in direct["meetings"]], ["Authorized"]
        )
        endpoint = self.client.get(
            f"/admin/responsibility.json?wa={self.jordan}"
        )
        self.assertEqual(
            [row["id"] for row in endpoint.get_json()["tasks"]],
            [authorized["id"]],
        )
        sub_digest = self.client.get(
            f"/admin/digest/sub?sender={self.jordan}"
        )
        self.assertEqual(
            [row["id"] for row in sub_digest.get_json()["tasks"]],
            [authorized["id"]],
        )
        pm_digest = self.client.get(f"/admin/digest/pm?pm={self.sender}")
        self.assertEqual(pm_digest.get_json()["total_open"], 1)

    def test_inspection_migration_is_idempotent_verified_and_fail_safe(self):
        legacy_engine = create_engine("sqlite://")
        with legacy_engine.begin() as connection:
            connection.execute(text(
                "CREATE TABLE inspections (id INTEGER PRIMARY KEY, notes TEXT)"
            ))
        with patch.object(storage, "ENGINE", legacy_engine):
            first = storage.migrate_inspection_accountability()
            second = storage.migrate_inspection_accountability()
            schema = inspect(legacy_engine)
            self.assertEqual(first["status"], "ok")
            self.assertEqual(
                set(first["changed"]),
                {"inspections.accountable_wa", "ix_inspections_accountable_wa"},
            )
            self.assertEqual(second, {"status": "ok", "changed": []})
            self.assertIn(
                "accountable_wa",
                {column["name"] for column in schema.get_columns("inspections")},
            )
            self.assertIn(
                "ix_inspections_accountable_wa",
                {index["name"] for index in schema.get_indexes("inspections")},
            )

        with patch.object(
            storage,
            "migrate_inspection_accountability",
            side_effect=RuntimeError("migration failure"),
        ):
            with self.assertRaisesRegex(RuntimeError, "migration failure"):
                storage.init_db()


if __name__ == "__main__":
    unittest.main()
