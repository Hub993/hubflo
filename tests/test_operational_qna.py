import json
import os
from pathlib import Path
import datetime as dt
import unittest
import uuid
from unittest.mock import patch

from sqlalchemy import event

_db_url = os.environ.get("DATABASE_URL", "")
_db_path = _db_url.removeprefix("sqlite:///") if _db_url.startswith("sqlite:///") else ""
if (os.environ.get("HUBFLO_QNA_ISOLATED") != "1" or not _db_path or
        Path(_db_path).name in ("", "hubflo.db")):
    raise RuntimeError("operational Q&A tests require an isolated SQLite DATABASE_URL")

import app as hubflo_app
import storage
from agent_layer.contracts import Principal, Scope
from agent_layer.conversational import ConversationalOrchestrator
from agent_layer.models import AgentAuthorityGrant, AgentProviderPolicy
from agent_layer.operational_evidence import OperationalEvidenceAssembler
from agent_layer.persistence import scope_key
from agent_layer.providers import (
    DeterministicProvider, OpenAIResponsesProvider, ProviderRegistry,
    register_configured_provider,
)
from controlled_environment import CHANNEL, SENDERS, setup_controlled_fixture


class OperationalQATests(unittest.TestCase):
    """Q01-Q20: real webhook, real Stage 2 rows, real governed runtime."""

    provider_id = "qna-controlled-provider"

    def setUp(self):
        self.state = setup_controlled_fixture()
        self.florida = self.state["client_ids"]["Florida"]
        self.sa = self.state["client_ids"]["SA"]
        self.client = hubflo_app.app.test_client()
        self.sent = []
        self.requests = []
        self._ensure_provider_policy()
        hubflo_app.AGENT_RUNTIME.providers.register(DeterministicProvider(
            self.provider_id, "controlled-1", self._respond,
        ))
        self.orchestrator = ConversationalOrchestrator(
            hubflo_app.AGENT_RUNTIME,
            self.provider_id,
            evidence_assembler=OperationalEvidenceAssembler(),
        )
        self.send_patch = patch.object(
            hubflo_app, "send_whatsapp_text",
            side_effect=lambda phone, sender, message: (
                self.sent.append(str(message)) or (True, {})),
        )
        self.send_patch.start()
        self.addCleanup(self.send_patch.stop)

    def _ensure_provider_policy(self):
        with storage.SessionLocal() as session:
            if session.query(AgentProviderPolicy).filter_by(
                    provider_id=self.provider_id, version=1).first():
                return
            session.add(AgentProviderPolicy(
                provider_id=self.provider_id, version=1,
                allowed_data_classes_json='["SD3"]',
                allowed_confidentiality_json='["restricted","internal"]',
                permitted_uses_json='["reason"]',
                training_permitted=False, retention_mode="zero-retention",
                retention_max_seconds=0, access_controls_json='[]',
                allowed_regions_json='[]', audit_required=True,
                attribution_required=True, deletion_supported=False,
                withdrawal_supported=False,
                allowed_distribution_uses_json='[]',
                terms_ref="fixture://qna-controlled-provider", allowed=True,
                authority_basis="QNA-CONTROLLED-TEST",
            ))
            session.commit()

    @staticmethod
    def _selection(request):
        question = request.context["classified_context"][
            next(key for key in request.context["classified_context"]
                 if key.startswith("conversational-request:"))
        ]
        return {
            "selection": "selected", "capability_id": "manager_pa.assist",
            "arguments": {"request": question, "assistance_mode": "explain",
                          "evidence_refs": []},
            "evidence_refs": [],
        }

    def _respond(self, request):
        self.requests.append(request)
        if request.operation == "conversational.select":
            return self._selection(request)
        items = request.context["classified_items"]
        payload = next(value for key, value in items.items()
                       if key.startswith("conversational-payload:"))
        question = payload["request"].lower()
        evidence = [(key, value) for key, value in items.items()
                    if key.startswith("hubflo:")]
        tasks = [(key, value) for key, value in evidence
                 if key.startswith("hubflo:tasks:")]
        if "inspection" in question:
            chosen = [(key, value) for key, value in evidence
                      if key.startswith("hubflo:inspections:") and
                      value["derived_outstanding"]]
            content = "Outstanding inspections: " + ", ".join(
                "%s (%s)" % (value["project_code"], value["phase"])
                for _, value in chosen)
        elif "deliver" in question or "order" in question:
            chosen = [(key, value) for key, value in tasks
                      if value.get("order_state") or value.get("subtype") == "order"]
            content = "Orders and deliveries: " + ", ".join(
                "%s — %s (%s)" % (value["project_code"], value["text"],
                                   value.get("order_state"))
                for _, value in chosen)
        elif "supplier" in question:
            chosen = [(key, value) for key, value in tasks
                      if value.get("subcontractor_name") and
                      (value.get("derived_due_state") == "overdue" or
                       value.get("subtype") in ("supplier_commitment",
                                                "supplier_performance"))]
            names = sorted({value["subcontractor_name"] for _, value in chosen})
            content = (
                "Facts: supplier-linked overdue/history records exist for %s. "
                "Inference: these suppliers may need attention; the records do not "
                "by themselves establish fault." % ", ".join(names)
            )
        elif "compare" in question:
            chosen = [(key, value) for key, value in tasks
                      if value["project_code"] in ("Site 1", "Site 3")]
            counts = {project: sum(
                1 for _, value in chosen
                if value["project_code"] == project and
                value.get("derived_due_state") == "overdue"
            ) for project in ("Site 1", "Site 3")}
            content = "Site 1 has %s overdue records; Site 3 has %s." % (
                counts["Site 1"], counts["Site 3"])
        elif "overdue" in question:
            chosen = [(key, value) for key, value in tasks
                      if value.get("derived_due_state") == "overdue"]
            content = "Overdue: " + "; ".join(value["text"] for _, value in chosen)
        elif "attention" in question:
            chosen = [(key, value) for key, value in tasks
                      if value.get("derived_attention_indicators")]
            content = "Needs attention: " + "; ".join(
                "%s [%s]" % (value["text"], ",".join(
                    value["derived_attention_indicators"]))
                for _, value in chosen)
        else:
            chosen = [(key, value) for key, value in tasks
                      if not value.get("derived_completed")]
            projects = sorted({value["project_code"] for _, value in chosen
                               if value.get("project_code")})
            content = "Current operational locations: " + ", ".join(projects)
        refs = [key for key, _ in chosen]
        return {"assistance_type": "explanation", "content": content,
                "evidence_refs": refs, "proposed_action": {"executed": False}}

    def _inbound(self, sender, text, event=None, channel=CHANNEL):
        event = event or "qna-" + uuid.uuid4().hex
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR",
                          self.orchestrator):
            response = self.client.post("/webhook", json={
                "entry": [{"changes": [{"value": {
                    "metadata": {"phone_number_id": channel},
                    "contacts": [{"wa_id": sender}],
                    "messages": [{"id": event, "from": sender, "type": "text",
                                  "text": {"body": text}}],
                }}]}],
            })
        self.assertEqual(response.status_code, 200)
        return self.sent[-1]

    def _assist_request(self):
        return [request for request in self.requests
                if request.operation == "manager_pa.assist"][-1]

    @staticmethod
    def _stage2_counts():
        with storage.SessionLocal() as session:
            return {model.__tablename__: session.query(model).count() for model in (
                storage.Task, storage.DelayLog, storage.TaskGroup, storage.Inspection,
                storage.Meeting, storage.StockItem, storage.StockMovement,
            )}

    def test_q01_florida_sites_actual_evidence_and_no_mutation(self):
        before = self._stage2_counts()
        answer = self._inbound(SENDERS["Dolan"], "what sites are ongoing")
        self.assertIn("Site 1", answer); self.assertIn("Site 3", answer)
        self.assertNotIn("Branch", repr(self._assist_request().context))
        self.assertEqual(before, self._stage2_counts())

    def test_q02_natural_wording_uses_same_generic_path(self):
        answer = self._inbound(SENDERS["Dolan"], "which sites are active at the moment?")
        self.assertIn("Site 2", answer)
        self.assertTrue(any(key.startswith("hubflo:tasks:")
                            for key in self._assist_request().context["classified_items"]))

    def test_q03_overdue_is_grounded(self):
        answer = self._inbound(SENDERS["Dolan"], "What is overdue?")
        self.assertIn("overdue", answer.lower()); self.assertIn("Site 1", answer)

    def test_q04_attention_is_read_only_prioritization(self):
        before = self._stage2_counts()
        answer = self._inbound(SENDERS["Dolan"], "What needs my attention?")
        self.assertIn("overrun", answer); self.assertEqual(before, self._stage2_counts())

    def test_q05_outstanding_inspections(self):
        answer = self._inbound(SENDERS["Dolan"], "What inspections are outstanding?")
        self.assertIn("operational", answer); self.assertIn("Site 1", answer)

    def test_q06_orders_and_deliveries_use_task_semantics(self):
        answer = self._inbound(SENDERS["Dolan"], "What deliveries/orders do we have?")
        self.assertIn("ordered", answer); self.assertIn("delivered", answer)

    def test_q07_supplier_fact_and_inference_are_distinguished(self):
        answer = self._inbound(SENDERS["Dolan"], "Which supplier has problems?")
        self.assertIn("Facts:", answer); self.assertIn("Inference:", answer)

    def test_q08_compare_two_authorized_projects(self):
        answer = self._inbound(SENDERS["Dolan"], "Compare Site 1 and Site 3.")
        self.assertIn("Site 1", answer); self.assertIn("Site 3", answer)
        context = repr(self._assist_request().context)
        self.assertNotIn("Branch 1", context)

    def test_q09_switch_to_sa_has_no_florida_carryover(self):
        channel = CHANNEL + "-q09"
        self._inbound(SENDERS["Neville"], "Florida", channel=channel)
        self._inbound(SENDERS["Neville"], "what sites are ongoing", channel=channel)
        self._inbound(SENDERS["Neville"], "SA", channel=channel)
        answer = self._inbound(SENDERS["Neville"], "what branches are ongoing?",
                               channel=channel)
        self.assertIn("Branch 1", answer)
        context = repr(self._assist_request().context)
        self.assertNotIn("Site 1", context); self.assertIn("Branch 2", context)

    def test_q10_sa_supplier_commitment_delays(self):
        answer = self._inbound(SENDERS["Paul"],
                               "What supplier commitment issues exist?")
        self.assertIn("Parts Supplier1", answer); self.assertIn("Inference", answer)

    def test_q11_cross_client_sentinels_never_cross(self):
        with storage.SessionLocal() as session:
            session.add_all([
                storage.Task(client_id=self.florida, project_code="Site 1",
                             sender=SENDERS["Dolan"], text="FL-UNIQUE-SENTINEL"),
                storage.Task(client_id=self.sa, project_code="Branch 1",
                             sender=SENDERS["Paul"], text="SA-UNIQUE-SENTINEL"),
            ]); session.commit()
        self._inbound(SENDERS["Dolan"], "give me an operational overview")
        self.assertIn("FL-UNIQUE-SENTINEL", repr(self._assist_request().context))
        self.assertNotIn("SA-UNIQUE-SENTINEL", repr(self._assist_request().context))
        self._inbound(SENDERS["Paul"], "give me an operational overview")
        self.assertIn("SA-UNIQUE-SENTINEL", repr(self._assist_request().context))
        self.assertNotIn("FL-UNIQUE-SENTINEL", repr(self._assist_request().context))

    def test_q11_other_client_taskgroups_are_filtered_by_the_query(self):
        with storage.SessionLocal() as session:
            sa_tasks = session.query(storage.Task).filter_by(
                client_id=self.sa, project_code="Branch 1").order_by(
                    storage.Task.id).limit(2).all()
            foreign = storage.TaskGroup(
                parent_id=sa_tasks[0].id, child_id=sa_tasks[1].id)
            session.add(foreign); session.commit(); foreign_id = foreign.id
            foreign_task_ids = {sa_tasks[0].id, sa_tasks[1].id}
        membership = storage.resolve_sender_context(SENDERS["Dolan"])["membership"]
        principal = Principal(
            "user:%s" % membership["user_id"], "user", Scope(self.florida, None))
        observed = []

        def capture(_connection, _cursor, statement, parameters, _context, _many):
            if "from task_groups" in statement.lower():
                observed.append((statement, parameters))

        event.listen(storage.ENGINE, "before_cursor_execute", capture)
        try:
            items = OperationalEvidenceAssembler().assemble(principal, membership)
        finally:
            event.remove(storage.ENGINE, "before_cursor_execute", capture)
        self.assertEqual(len(observed), 1)
        statement, parameters = observed[0]
        normalized = " ".join(statement.lower().split())
        self.assertIn("task_groups.parent_id in", normalized)
        self.assertIn("task_groups.child_id in", normalized)
        self.assertTrue(foreign_task_ids.isdisjoint(set(parameters)))
        self.assertNotIn("hubflo:task_groups:%s" % foreign_id,
                         {item.reference for item in items})

    def test_q12_project_restricted_principal_gets_only_project(self):
        with storage.SessionLocal() as session:
            user = session.query(storage.User).filter_by(wa_id=SENDERS["Dolan"]).one()
            membership = session.query(storage.SenderMembership).filter_by(
                user_id=user.id, client_id=self.florida).one()
            membership.project_code = "Site 1"; session.commit()
            membership_id = membership.id; user_id = user.id
        items = OperationalEvidenceAssembler().assemble(
            Principal("user:%s" % user_id, "user", Scope(self.florida, "Site 1")),
            {"id": membership_id, "user_id": user_id},
        )
        self.assertTrue(items)
        self.assertTrue(all(item.project_code == "Site 1" for item in items))
        self.assertNotIn("Site 2", repr(items))

    def test_q12_broad_scope_preserves_justified_record_project_metadata(self):
        membership = storage.resolve_sender_context(SENDERS["Dolan"])["membership"]
        principal = Principal(
            "user:%s" % membership["user_id"], "user", Scope(self.florida, None))
        with storage.SessionLocal() as session:
            parent = session.query(storage.Task).filter_by(
                client_id=self.florida, project_code="Site 1").first()
            child = session.query(storage.Task).filter_by(
                client_id=self.florida, project_code="Site 2").first()
            relationship = storage.TaskGroup(parent_id=parent.id, child_id=child.id)
            session.add(relationship); session.commit(); relationship_id = relationship.id
        items = OperationalEvidenceAssembler().assemble(principal, membership)
        intrinsic_tables = {"tasks", "inspections", "meetings", "stock_items",
                            "delay_logs"}
        intrinsic = [item for item in items
                     if item.provenance.get("table") in intrinsic_tables]
        self.assertTrue(intrinsic)
        self.assertTrue(all(item.project_code == item.value["project_code"]
                            for item in intrinsic))
        cross_project = next(item for item in items
                             if item.reference == "hubflo:task_groups:%s" % relationship_id)
        self.assertIsNone(cross_project.project_code)

    def test_q13_revoked_membership_cannot_retrieve_or_reason(self):
        result = storage.resolve_sender_context(SENDERS["Dolan"])
        membership = result["membership"]
        principal = Principal("user:%s" % membership["user_id"], "user",
                              Scope(self.florida, None))
        with storage.SessionLocal() as session:
            session.query(storage.SenderMembership).filter_by(
                id=membership["id"]).update({"active": False}); session.commit()
        with self.assertRaises(Exception):
            OperationalEvidenceAssembler().assemble(principal, membership)
        before = len(self.requests)
        self._inbound(SENDERS["Dolan"], "what is overdue?")
        self.assertEqual(before, len(self.requests))

    def _assert_grant_change_during_selection_blocks_retrieval(self, change):
        principal_id = "user:%s" % self.state["user_ids"]["Dolan"]

        def responder(request):
            self.requests.append(request)
            if request.operation != "conversational.select":
                self.fail("protected operational reasoning occurred after grant change")
            with storage.SessionLocal() as session:
                grant = session.query(AgentAuthorityGrant).filter(
                    AgentAuthorityGrant.principal_id == principal_id,
                    AgentAuthorityGrant.capability_id == "manager_pa.assist",
                    AgentAuthorityGrant.client_id == self.florida,
                    AgentAuthorityGrant.revoked_at == None,
                ).one()
                if change == "revoke":
                    grant.revoked_at = dt.datetime.utcnow()
                else:
                    grant.project_code = "Site 1"
                    grant.scope_key = scope_key(self.florida, "Site 1")
                self._changed_grant_id = grant.id
                session.commit()
            return self._selection(request)

        hubflo_app.AGENT_RUNTIME.providers.register(DeterministicProvider(
            self.provider_id, "grant-change", responder))
        assembler = self.orchestrator.evidence_assembler
        try:
            with patch.object(assembler, "assemble", wraps=assembler.assemble) as assemble:
                answer = self._inbound(SENDERS["Dolan"], "what is overdue?")
        finally:
            with storage.SessionLocal() as session:
                grant = session.get(AgentAuthorityGrant, self._changed_grant_id)
                grant.scope_key = scope_key(self.florida)
                grant.project_code = None
                grant.revoked_at = None
                session.commit()
        self.assertIn("can’t provide", answer)
        assemble.assert_not_called()
        self.assertEqual([request.operation for request in self.requests],
                         ["conversational.select"])
        self.assertNotIn("hubflo:", repr(self.requests))

    def test_q13_grant_revoked_after_selection_gate_blocks_retrieval(self):
        self._assert_grant_change_during_selection_blocks_retrieval("revoke")

    def test_q13_grant_narrowed_after_selection_gate_blocks_stale_scope(self):
        self._assert_grant_change_during_selection_blocks_retrieval("narrow")

    def test_q14_no_current_context_preserves_selection_required(self):
        answer = self._inbound(SENDERS["Neville"], "what is overdue?",
                               channel=CHANNEL + "-q14")
        self.assertIn("Which context?", answer)
        self.assertFalse(self.requests)

    def test_q15_absent_provider_degrades_and_stage2_still_works(self):
        unavailable = ConversationalOrchestrator(
            hubflo_app.AGENT_RUNTIME, "not-registered",
            evidence_assembler=OperationalEvidenceAssembler())
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", unavailable):
            self.client.post("/webhook", json={"entry": [{"changes": [{"value": {
                "metadata": {"phone_number_id": CHANNEL},
                "contacts": [{"wa_id": SENDERS["Dolan"]}],
                "messages": [{"id": "q15-agent", "from": SENDERS["Dolan"],
                              "type": "text", "text": {"body": "what is overdue?"}}],
            }}]}]})
        self.assertIn("can’t provide", self.sent[-1])
        before = self._stage2_counts()["tasks"]
        self._inbound(SENDERS["Dolan"], "Please check the q15 generator")
        self.assertEqual(before + 1, self._stage2_counts()["tasks"])

    def test_q16_invalid_provider_output_is_not_success(self):
        def invalid(request):
            return self._selection(request) if request.operation == "conversational.select" else {
                "assistance_type": "explanation", "content": "unsupported success"
            }
        hubflo_app.AGENT_RUNTIME.providers.register(DeterministicProvider(
            self.provider_id, "invalid", invalid))
        answer = self._inbound(SENDERS["Dolan"], "what is overdue?")
        self.assertIn("can’t provide", answer)

    def test_q17_s2_s3_s4_selection_is_blocked(self):
        before = self._stage2_counts()
        for capability_id in (
                "configuration.propose", "channel.deliver", "configuration.commit"):
            def select_outside(request, selected=capability_id):
                return {"selection": "selected", "capability_id": selected,
                        "arguments": {}, "evidence_refs": []}
            hubflo_app.AGENT_RUNTIME.providers.register(DeterministicProvider(
                self.provider_id, "outside", select_outside))
            answer = self._inbound(SENDERS["Dolan"], "perform outside capability")
            self.assertIn("can’t provide", answer)
        self.assertEqual(before, self._stage2_counts())

    def test_q18_question_text_never_becomes_task(self):
        question = "what sites are ongoing q18 sentinel"
        self._inbound(SENDERS["Dolan"], question)
        with storage.SessionLocal() as session:
            self.assertEqual(session.query(storage.Task).filter_by(text=question).count(), 0)

    def test_q19_repeat_is_operationally_idempotent(self):
        before = self._stage2_counts()
        self._inbound(SENDERS["Dolan"], "what sites are ongoing")
        self._inbound(SENDERS["Dolan"], "what sites are ongoing")
        self.assertEqual(before, self._stage2_counts())

    def test_q20_provider_context_is_bounded_and_authoritative(self):
        self._inbound(SENDERS["Dolan"], "What needs my attention?")
        request = self._assist_request()
        items = request.context["classified_items"]
        self.assertTrue(any(key.startswith("conversational-membership:") for key in items))
        self.assertTrue(any(key.startswith("conversational-eligible:") for key in items))
        self.assertTrue(any(key.startswith("hubflo:") for key in items))
        self.assertFalse(any("secret" in key.lower() for key in items))
        self.assertNotIn("SA", repr(items))


class ProviderAdapterTests(unittest.TestCase):
    def test_adapter_registration_is_explicit_and_does_not_install_policy(self):
        registry = ProviderRegistry()
        self.assertIsNone(register_configured_provider(registry, {}))
        values = {"HUBFLO_AGENT_PROVIDER_ADAPTER": "openai-responses",
                  "HUBFLO_AGENT_PROVIDER_ID": "configured-openai",
                  "HUBFLO_OPENAI_MODEL": "configured-model",
                  "OPENAI_API_KEY": "controlled-secret"}
        self.assertEqual("configured-openai", register_configured_provider(registry, values))
        self.assertIsInstance(registry.resolve("configured-openai"), OpenAIResponsesProvider)

    def test_adapter_parses_structured_output_and_fails_closed(self):
        class Response:
            def __init__(self, value): self.value = value
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return json.dumps(self.value).encode()
        def transport(request, timeout):
            body = json.loads(request.data)
            self.assertEqual(body["text"]["format"]["type"], "json_schema")
            self.assertNotIn("controlled-secret", request.data.decode())
            return Response({"status": "completed", "model": "configured-model",
                "output": [{"type": "message", "content": [{"type": "output_text",
                    "text": '{"answer":"grounded"}'}]}]})
        provider = OpenAIResponsesProvider(
            "configured-openai", "configured-model", "controlled-secret",
            transport=transport)
        from agent_layer.contracts import ProviderRequest
        result = provider.invoke(ProviderRequest("reason", {"fact": 1}, {"answer": str}))
        self.assertEqual(result.output, {"answer": "grounded"})
        malformed = OpenAIResponsesProvider(
            "configured-openai", "configured-model", "controlled-secret",
            transport=lambda request, timeout: Response({"status": "completed", "output": []}))
        with self.assertRaises(Exception):
            malformed.invoke(ProviderRequest("reason", {}, {"answer": str}))


if __name__ == "__main__":
    unittest.main()
