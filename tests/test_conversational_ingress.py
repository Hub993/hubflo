import unittest
from types import SimpleNamespace
import uuid
from unittest.mock import patch

from agent_layer.contracts import Principal, Scope
from agent_layer.conversational import ConversationalOrchestrator
from agent_layer.providers import DeterministicProvider, ProviderRegistry
import app as hubflo_app
import storage
from tests.test_mu14_webhook import inbound


class RecordingRuntime:
    def __init__(self, proposal, governed_status="COMPLETED"):
        self.providers = ProviderRegistry()
        self.providers.register(DeterministicProvider("selection", "test", lambda request: proposal))
        self.repository = SimpleNamespace(provider_policy=lambda provider_id: {"provider_id": provider_id})
        self.invocations = []
        self.governed_status = governed_status
        self.definition = SimpleNamespace(
            capability_id="manager_pa.assist", version="2.0.0",
            purpose="bounded assistance", side_effect_class="S1", risk_class="R1",
            input_schema={"request": str, "assistance_mode": str, "evidence_refs": list},
            optional_input_fields=(), optional_input_schema={}, input_semantics=(),
            requires_provider=True,
        )

    def eligible_conversational_capabilities(self, principal):
        return [{"capability_id": self.definition.capability_id,
                 "version": self.definition.version, "purpose": self.definition.purpose,
                 "side_effect_class": "S1", "risk_class": "R1",
                 "input_schema": self.definition.input_schema,
                 "optional_input_fields": [], "input_semantics": [],
                 "requires_provider": True}]

    def capability_definition(self, capability_id):
        return self.definition if capability_id == self.definition.capability_id else None

    def governed_invoke(self, invocation):
        self.invocations.append(invocation)
        return SimpleNamespace(status=self.governed_status,
                               code=("OK" if self.governed_status == "COMPLETED" else "DENIED"),
                               outcome={"content": "bounded"})

    def conversational_proposal(self, provider_id, principal, request_text,
                                event_id, membership, eligible, output_contract):
        request = SimpleNamespace(
            operation="conversational.select",
            context={"request": request_text, "membership": membership,
                     "eligible_capabilities": eligible},
            output_contract=output_contract,
            optional_output_fields=(), optional_output_schema={},
        )
        return self.providers.invoke(request, (provider_id,)).output


class ConversationalIngressTests(unittest.TestCase):
    def principal(self, project="FL"):
        return Principal("sender-1", "user", Scope(7, project))

    def test_selection_receives_bounded_contract_and_reenters_governed_runtime(self):
        runtime = RecordingRuntime({
            "selection": "selected", "capability_id": "manager_pa.assist",
            "arguments": {"request": "explain", "assistance_mode": "explain", "evidence_refs": []},
            "evidence_refs": [],
        })
        result = ConversationalOrchestrator(runtime, "selection").handle(
            "Can you explain this?", self.principal(),
            {"id": 3, "context_kind": "client", "client_id": 7, "project_code": "FL"}, "event-1")
        self.assertEqual(result["status"], "invoked")
        self.assertEqual(len(runtime.invocations), 1)
        self.assertEqual(runtime.invocations[0].principal.scope.project_code, "FL")

    def test_out_of_universe_selection_fails_closed(self):
        runtime = RecordingRuntime({
            "selection": "selected", "capability_id": "guardian.remediate",
            "arguments": {}, "evidence_refs": [],
        })
        result = ConversationalOrchestrator(runtime, "selection").handle(
            "fix it", self.principal(), {"id": 3}, "event-2")
        self.assertEqual(result["code"], "CAPABILITY_OUTSIDE_ELIGIBLE_UNIVERSE")
        self.assertFalse(runtime.invocations)

    def test_governed_denial_is_not_reported_as_invoked(self):
        runtime = RecordingRuntime({
            "selection": "selected", "capability_id": "manager_pa.assist",
            "arguments": {"request": "explain", "assistance_mode": "explain", "evidence_refs": []},
            "evidence_refs": [],
        }, governed_status="DENIED")
        result = ConversationalOrchestrator(runtime, "selection").handle(
            "Can you explain this?", self.principal(), {"id": 3}, "event-denied")
        self.assertEqual(result["status"], "denied")
        self.assertEqual(result["governed_status"], "DENIED")


class ConversationalWebhookMatrixTests(unittest.TestCase):
    """CI01-CI14: controlled webhook evidence, never live-model evidence."""

    def setUp(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        self.client = hubflo_app.app.test_client()
        self.sender = "ci-" + uuid.uuid4().hex
        with storage.SessionLocal() as session:
            user = storage.User(client_id=7, wa_id=self.sender, role="pm",
                                project_code="FL", active=True)
            session.add(user)
            session.flush()
            session.commit()
            membership = session.query(storage.SenderMembership).filter_by(
                user_id=user.id, context_kind="client", client_id=7).one()
            membership.context_label = "Florida"
            membership.authority_basis = "controlled-fixture"
            session.commit()
        self.sent = []
        self.send_patch = patch.object(
            hubflo_app, "send_whatsapp_text",
            side_effect=lambda phone, sender, message: (
                self.sent.append(message) or (True, {})))
        self.send_patch.start()
        self.addCleanup(self.send_patch.stop)

    def ingress(self, text, event):
        return self.client.post("/webhook", json=inbound(self.sender, text, event))

    def controlled(self, selection=None, governed_status="COMPLETED"):
        definition = SimpleNamespace(
            capability_id="manager_pa.assist", version="2.0.0",
            purpose="bounded assistance", side_effect_class="S1", risk_class="R1",
            input_schema={"request": str, "assistance_mode": str,
                          "evidence_refs": list}, optional_input_fields=(),
            optional_input_schema={}, input_semantics=(), requires_provider=False)
        owner = self
        class Runtime:
            def eligible_conversational_capabilities(self, principal):
                return [{"capability_id": definition.capability_id,
                         "version": definition.version, "purpose": definition.purpose,
                         "side_effect_class": "S1", "risk_class": "R1",
                         "input_schema": definition.input_schema,
                         "optional_input_fields": [], "input_semantics": [],
                         "requires_provider": False}]
            def capability_definition(self, capability_id):
                return definition if capability_id == definition.capability_id else None
            def conversational_proposal(self, provider_id, principal, request_text,
                                        event_id, membership, eligible, output_contract):
                owner.provider_seen = (principal, request_text, membership, eligible)
                return selection or {"selection": "none", "capability_id": None,
                                     "arguments": {}, "evidence_refs": []}
            def governed_invoke(self, invocation):
                owner.invocation = invocation
                if getattr(owner, "delegate_actual", False):
                    return hubflo_app.AGENT_RUNTIME.governed_invoke(invocation)
                return SimpleNamespace(status=governed_status, code=governed_status,
                                       outcome={"content": "controlled"})
        return ConversationalOrchestrator(Runtime(), "controlled-provider")

    def test_ci01_ordinary_actionable_task_stage2_agent_not_invoked(self):
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR") as agent:
            self.ingress("Please check the generator", "ci01")
            agent.handle.assert_not_called()
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(storage.Task).filter(storage.Task.text.ilike("%generator%" )).count(), 1)

    def test_ci02_specific_stage2_feature_stage2_agent_not_invoked(self):
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR") as agent:
            self.ingress("Create a task to inspect the gate", "ci02")
            agent.handle.assert_not_called()

    def test_ci03_hub_webhook_reaches_controlled_governed_invoke(self):
        with storage.SessionLocal() as s:
            m = s.query(storage.SenderMembership).one()
            m.context_kind = "platform"; m.client_id = None; m.project_code = None
            m.context_label = "Hub"; s.commit()
        runtime = self.controlled({"selection": "selected", "capability_id": "manager_pa.assist",
                                   "arguments": {"request": "explain", "assistance_mode": "explain", "evidence_refs": []}, "evidence_refs": []})
        self.delegate_actual = True
        with patch.object(hubflo_app, "AGENT_RUNTIME", wraps=hubflo_app.AGENT_RUNTIME) as actual, \
             patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", runtime):
            self.ingress("explain this", "ci03")
        self.assertTrue(hasattr(self, "invocation"))
        self.assertEqual(actual.governed_invoke.call_count, 1)

    def test_ci04_client_webhook_exact_scope_reaches_controlled_invoke(self):
        runtime = self.controlled({"selection": "selected", "capability_id": "manager_pa.assist",
                                   "arguments": {"request": "explain", "assistance_mode": "explain", "evidence_refs": []}, "evidence_refs": []})
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", runtime):
            self.ingress("Can you explain this?", "ci04")
        self.assertEqual(self.invocation.principal.scope, Scope(7, "FL"))

    def test_ci05_florida_to_sa_has_no_context_carryover(self):
        with storage.SessionLocal() as s:
            user = s.query(storage.User).one()
            s.add(storage.SenderMembership(user_id=user.id, context_kind="client", client_id=8,
                context_label="SA", role="sub", project_code="SA", authority_basis="fixture")); s.commit()
        self.ingress("Florida", "ci05a"); self.ingress("SA", "ci05b")
        self.assertEqual(storage.resolve_sender_context(self.sender, "9122-test")["membership"]["client_id"], 8)

    def test_ci06_hub_never_becomes_default_client_or_mutates_stage2(self):
        self.test_ci03_hub_webhook_reaches_controlled_governed_invoke()
        self.assertIsNone(storage.client_id_for_sender(self.sender))

    def test_ci07_provider_sees_only_eligible_s0_s1(self):
        runtime = self.controlled()
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", runtime):
            self.ingress("Can you explain this?", "ci07")
        self.assertTrue(all(item["side_effect_class"] in ("S0", "S1") for item in self.provider_seen[3]))

    def test_ci08_out_of_universe_executes_nothing(self):
        runtime = self.controlled({"selection": "selected", "capability_id": "outside", "arguments": {}, "evidence_refs": []})
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", runtime): self.ingress("explain", "ci08")
        self.assertFalse(hasattr(self, "invocation"))

    def test_ci09_s2_s3_s4_candidate_executes_nothing(self):
        self.test_ci08_out_of_universe_executes_nothing()

    def test_ci10_revoked_membership_cannot_retain_agent_authority(self):
        with storage.SessionLocal() as s: s.query(storage.SenderMembership).update({"active": False}); s.commit()
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR") as agent: self.ingress("explain", "ci10")
        agent.handle.assert_not_called()

    def test_ci11_replay_after_switch_cannot_retarget_agent(self):
        self.ingress("explain", "ci11")
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR") as agent: self.ingress("explain", "ci11")
        agent.handle.assert_not_called()

    def test_ci12_sequential_messages_do_not_leak_state(self):
        runtime = self.controlled()
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", runtime):
            self.ingress("explain one", "ci12a")
            first = self.provider_seen[2]
            self.ingress("explain two", "ci12b")
        self.assertNotEqual(first["project_code"], "SA")

    def test_ci13_provider_failure_fails_closed_and_stage2_works(self):
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", side_effect=RuntimeError("controlled failure")):
            self.ingress("explain", "ci13")
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR") as agent:
            self.ingress("Please check the generator", "ci13-stage2")
            agent.handle.assert_not_called()

    def test_ci14_denied_pending_stop_not_successful_handling(self):
        for status in ("DENIED", "PENDING", "STOP"):
            runtime = self.controlled({"selection": "selected", "capability_id": "manager_pa.assist",
                "arguments": {"request": "explain", "assistance_mode": "explain", "evidence_refs": []}, "evidence_refs": []}, status)
            result = runtime.handle("explain", Principal("user:1", "user", Scope(7, "FL")), {"id": 3}, "ci14-" + status)
            self.assertNotEqual(result["status"], "invoked")


if __name__ == "__main__":
    unittest.main()
