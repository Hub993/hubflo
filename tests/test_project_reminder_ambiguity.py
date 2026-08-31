import datetime as dt
import json
import unittest

import app as hubflo_app
import storage
from tests.test_mu14_webhook import inbound


class ProjectReminderAmbiguityTests(unittest.TestCase):
    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender = "15550000800"
        with storage.SessionLocal() as session:
            user = storage.User(
                client_id=80,
                wa_id=self.sender,
                name="Project Reminder PM",
                role="pm",
                project_code="PROJECT_A1",
                timezone="America/New_York",
                active=True,
            )
            session.add(user)
            session.flush()
            self.user_id = user.id
            session.add(storage.PMProjectMap(
                client_id=80,
                pm_user_id=user.id,
                project_code="PROJECT_A1",
                primary_pm=True,
            ))
            session.commit()

    def send(self, text, message_id):
        response = self.client.post(
            "/webhook",
            json=inbound(self.sender, text, message_id),
        )
        self.assertEqual(response.status_code, 200)
        return response

    def authorize_project(self, project_code):
        with storage.SessionLocal() as session:
            session.add(storage.PMProjectMap(
                client_id=80,
                pm_user_id=self.user_id,
                project_code=project_code,
                primary_pm=False,
            ))
            session.commit()

    def remove_project_authorization(self, project_code):
        with storage.SessionLocal() as session:
            session.query(storage.PMProjectMap).filter(
                storage.PMProjectMap.client_id == 80,
                storage.PMProjectMap.pm_user_id == self.user_id,
                storage.PMProjectMap.project_code == project_code,
            ).delete(synchronize_session=False)
            session.commit()

    def make_reminder(self, project_code, text="Concrete pour check"):
        return storage.create_pm_reminder({
            "pm_wa": self.sender,
            "recipient_wa": self.sender,
            "project_code": project_code,
            "text": text,
            "timezone": "America/New_York",
            "next_run": dt.datetime.utcnow() + dt.timedelta(days=2),
            "rule": "once",
        })

    def active_state(self):
        with storage.SessionLocal() as session:
            state = session.query(storage.ConversationState).filter(
                storage.ConversationState.active == True,
            ).one()
            return {
                "id": state.id,
                "active": state.active,
                "status": state.status,
                "candidates": json.loads(state.candidate_metadata_json),
            }

    def create_oakridge_ambiguity(self):
        north = "Oakridge Site North"
        south = "Oakridge Site South"
        self.authorize_project(north)
        self.authorize_project(south)
        north_reminder = self.make_reminder(north)
        south_reminder = self.make_reminder(south)
        unrelated = self.make_reminder("PROJECT_A1")
        self.send(
            "Cancel the concrete pour reminder for Oakridge Site",
            "project-reminder-ambiguous",
        )
        return north, south, north_reminder, south_reminder, unrelated

    def assert_reminder_status(self, reminder_id, expected):
        with storage.SessionLocal() as session:
            self.assertEqual(
                session.get(storage.PMReminder, reminder_id).status,
                expected,
            )

    def test_matching_project_universe_narrows_and_natural_followup_executes_once(self):
        north, south, north_reminder, south_reminder, unrelated = (
            self.create_oakridge_ambiguity()
        )
        state = self.active_state()
        candidates = state["candidates"]
        self.assertEqual(
            {record["id"] for record in candidates["project_candidates"]},
            {north, south},
        )
        self.assertEqual(
            {record["id"] for record in candidates["reminder_candidates"]},
            {north_reminder["id"], south_reminder["id"]},
        )
        self.assertEqual(
            set(candidates["authorization_scope"]["project_codes"]),
            {north, south},
        )
        self.assertNotIn(unrelated["id"], {
            record["id"] for record in candidates["reminder_candidates"]
        })

        before_candidates = json.dumps(candidates, sort_keys=True)
        self.send(
            "Book inspection for drywall tomorrow",
            "project-reminder-bypass",
        )
        with storage.SessionLocal() as session:
            current = session.get(storage.ConversationState, state["id"])
            self.assertTrue(current.active)
            self.assertEqual(
                json.dumps(json.loads(current.candidate_metadata_json), sort_keys=True),
                before_candidates,
            )
            self.assertEqual(session.query(storage.Inspection).count(), 1)

        self.send("Oakridge Site", "project-reminder-still-ambiguous")
        self.send("Unknown Project", "project-reminder-invalid")
        self.assert_reminder_status(north_reminder["id"], "active")
        self.assert_reminder_status(south_reminder["id"], "active")

        self.send(north, "project-reminder-natural-resolution")
        self.send(north, "project-reminder-natural-resolution")
        with storage.SessionLocal() as session:
            self.assertEqual(
                session.get(storage.PMReminder, north_reminder["id"]).status,
                "cancelled",
            )
            self.assertEqual(
                session.get(storage.PMReminder, south_reminder["id"]).status,
                "active",
            )
            self.assertEqual(
                session.get(storage.PMReminder, unrelated["id"]).status,
                "active",
            )
            resolved = session.get(storage.ConversationState, state["id"])
            self.assertFalse(resolved.active)
            self.assertEqual(resolved.status, "resolved")
            self.assertEqual(session.query(storage.Audit).filter(
                storage.Audit.action == "reminder_cancel",
                storage.Audit.ref_id == north_reminder["id"],
            ).count(), 1)

    def test_unique_project_resolution_scopes_immediate_action(self):
        north = "Oakridge Site North"
        self.authorize_project(north)
        north_reminder = self.make_reminder(north, "North concrete check")
        unrelated = self.make_reminder("PROJECT_A1", "North concrete check")

        self.send(
            "Cancel the North concrete reminder for Oakridge Site North",
            "project-reminder-unique",
        )
        self.assert_reminder_status(north_reminder["id"], "cancelled")
        self.assert_reminder_status(unrelated["id"], "active")

    def test_zero_project_match_never_mutates_a_wrong_project(self):
        unique = self.make_reminder(
            "PROJECT_A1",
            "Unique concrete pour check",
        )

        self.send(
            "Cancel the Unique concrete pour check reminder for Missing Site",
            "project-reminder-zero",
        )
        self.assert_reminder_status(unique["id"], "active")
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.ConversationState).count(), 0)
            self.assertEqual(session.query(storage.Audit).filter(
                storage.Audit.action == "reminder_cancel",
                storage.Audit.ref_id == unique["id"],
            ).count(), 0)

    def test_ordinary_unique_reminder_without_project_still_executes(self):
        unique = self.make_reminder(
            "PROJECT_A1",
            "Ordinary unique concrete check",
        )

        self.send(
            "Cancel Ordinary unique concrete check reminder",
            "projectless-reminder-unique",
        )
        self.assert_reminder_status(unique["id"], "cancelled")

        subject_after_reminder = self.make_reminder(
            "PROJECT_A1",
            "Concrete formwork check",
        )
        self.send(
            "Cancel reminder for Concrete formwork check",
            "projectless-reminder-for-subject",
        )
        self.assert_reminder_status(subject_after_reminder["id"], "cancelled")

    def test_current_authorization_narrows_and_later_expansion_cannot_broaden(self):
        north, south, north_reminder, south_reminder, unrelated = (
            self.create_oakridge_ambiguity()
        )
        state = self.active_state()
        east = "Oakridge Site East"
        self.authorize_project(east)
        east_reminder = self.make_reminder(east)
        self.remove_project_authorization(north)

        self.send(north, "project-reminder-no-longer-authorized")
        self.assert_reminder_status(north_reminder["id"], "active")
        with storage.SessionLocal() as session:
            current = session.get(storage.ConversationState, state["id"])
            self.assertTrue(current.active)
            candidates = json.loads(current.candidate_metadata_json)
            self.assertEqual(
                {record["id"] for record in candidates["reminder_candidates"]},
                {north_reminder["id"], south_reminder["id"]},
            )
            self.assertNotIn(east_reminder["id"], {
                record["id"] for record in candidates["reminder_candidates"]
            })

        self.send(east, "project-reminder-expanded-not-permitted")
        self.assert_reminder_status(east_reminder["id"], "active")
        self.send(south, "project-reminder-current-authorized")
        with storage.SessionLocal() as session:
            self.assertEqual(
                session.get(storage.PMReminder, south_reminder["id"]).status,
                "cancelled",
            )
            self.assertEqual(
                session.get(storage.PMReminder, north_reminder["id"]).status,
                "active",
            )
            self.assertEqual(
                session.get(storage.PMReminder, unrelated["id"]).status,
                "active",
            )
            self.assertEqual(
                session.get(storage.PMReminder, east_reminder["id"]).status,
                "active",
            )
            self.assertFalse(
                session.get(storage.ConversationState, state["id"]).active
            )


if __name__ == "__main__":
    unittest.main()
