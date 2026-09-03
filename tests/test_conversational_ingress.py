import unittest
import datetime as dt
from types import SimpleNamespace
import uuid
from unittest.mock import patch

from agent_layer.contracts import Principal, Scope
from agent_layer.conversational import ConversationalOrchestrator
from agent_layer.providers import DeterministicProvider, ProviderRegistry
from agent_layer.persistence import AgentRepository
from agent_layer.runtime import CAPABILITY_CATALOG
from agent_layer.models import AgentEntitlement, AgentExecution
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
            membership = session.query(storage.SenderMembership).filter_by(
                user_id=user.id, context_kind="client", client_id=7).one()
            membership.context_label = "Florida"
            membership.authority_basis = "controlled-fixture"
            session.commit()
            self.user_id = user.id
        self._install_actual_agent_fixture()
        self.sent = []
        self.send_patch = patch.object(
            hubflo_app, "send_whatsapp_text",
            side_effect=lambda phone, sender, message: (
                self.sent.append(message) or (True, {})))
        self.send_patch.start()
        self.addCleanup(self.send_patch.stop)

    def _install_actual_agent_fixture(self):
        """Install only controlled authority/provider evidence for the real runtime."""
        repo = hubflo_app.AGENT_RUNTIME.repository
        authority = Principal("ci-authority", "service", Scope())
        repo.init_schema()
        for definition in CAPABILITY_CATALOG:
            repo.upsert_capability(definition)

        def actor(principal_class, group, permissions=()):
            return {"principal_class": principal_class, "independence_group": group,
                    "permissions": list(permissions), "capabilities": ["*"],
                    "risk_classes": ["*"], "max_autonomy": 5,
                    "actions": ["*"], "confidentiality": ["restricted", "internal", "public"],
                    "industry_keys": ["construction"], "information_permissions": ["*"],
                    "domains": ["*"]}

        principals = {"ci-authority": actor("service", "authority", ("provider.configure", "capability.control", "authority.delegate", "entitlement.configure")),
                      "user:%s" % self.user_id: actor("user", "operators")}
        for key in ("platform", "client:7", "client:7/project:FL", "client:8", "client:8/project:SA"):
            repo.install_authority_value("AB-AUTH-001", key, {"principals": principals},
                                         "CI-FIXTURE", "ci-authority", dt.datetime.utcnow(),
                                         proof_ref="fixture://ci")
        repo.set_capability_state("manager_pa.assist", authority, enabled=True, healthy=True)
        definition = next(item for item in CAPABILITY_CATALOG if item.capability_id == "manager_pa.assist")
        for client_id, project in ((7, "FL"), (8, "SA")):
            repo.grant_authority(
                "ci-grant:%s:%s" % (self.user_id, project), "user:%s" % self.user_id,
                "user", "manager_pa.assist", "CI-GRANT", client_id, project,
                max_autonomy=2, information_permissions=("reason", "read"),
                action_permissions=(definition.required_action,), allowed_domains=("SD3", "SD4"),
                granting_actor=authority)
            repo.assign_entitlement("ci-ent:%s" % project, client_id, "capability",
                                    "manager_pa.assist", {"enabled": True}, 1,
                                    "CI-ENTITLEMENT", "user:%s" % self.user_id,
                                    granting_actor=authority)
        repo.install_provider_policy(
            "ci-provider", 1, ("SD3",), False, "zero-retention", True, "CI-PROVIDER",
            granting_actor=authority, allowed_confidentiality=("restricted", "internal"),
            permitted_uses=("reason",), retention_max_seconds=0,
            access_controls=("tenant-isolation",), allowed_regions=("ci",),
            terms_ref="fixture://ci-provider")
        hubflo_app.AGENT_RUNTIME.providers.register(DeterministicProvider(
            "ci-provider", "1", lambda request: {
                "assistance_type": "explanation", "content": "controlled",
                "evidence_refs": [], "proposed_action": {}}))
    def ingress(self, text, event):
        return self.client.post("/webhook", json=inbound(self.sender, text, event))

    def actual_orchestrator(self, selection=None):
        selection = selection or {"selection": "selected", "capability_id": "manager_pa.assist",
                                  "arguments": {"request": "explain", "assistance_mode": "explain",
                                                 "evidence_refs": []}, "evidence_refs": []}
        proposal = patch.object(hubflo_app.AGENT_RUNTIME, "conversational_proposal",
                                return_value=selection)
        proposal.start()
        self.addCleanup(proposal.stop)
        return ConversationalOrchestrator(hubflo_app.AGENT_RUNTIME, "ci-provider")

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
            response = self.ingress("Show all tasks for project FL", "ci02")
            agent.handle.assert_not_called()
        self.assertEqual(response.status_code, 200)
        self.assertIn("No projects mapped", self.sent[-1])
        with storage.SessionLocal() as s:
            claim = s.query(storage.MultiContextInboundClaim).filter_by(event_id="ci02").one()
            self.assertEqual(claim.processing_state, "released")
            self.assertEqual(claim.outcome, "released-to-stage2")

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
        orchestrator = self.actual_orchestrator()
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", orchestrator), \
             patch.object(hubflo_app.AGENT_RUNTIME, "governed_invoke",
                          wraps=hubflo_app.AGENT_RUNTIME.governed_invoke) as governed:
            self.ingress("Can you explain this?", "ci04")
        self.assertEqual(governed.call_count, 1)
        invocation = governed.call_args.args[0]
        self.assertEqual(invocation.principal.principal_id, "user:%s" % self.user_id)
        self.assertEqual(invocation.principal.scope, Scope(7, "FL"))

    def test_ci05_florida_to_sa_has_no_context_carryover(self):
        with storage.SessionLocal() as s:
            user = s.query(storage.User).one()
            s.add(storage.SenderMembership(user_id=user.id, context_kind="client", client_id=8,
                context_label="SA", role="sub", project_code="SA", authority_basis="fixture")); s.commit()
        orchestrator = self.actual_orchestrator()
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", orchestrator), \
             patch.object(hubflo_app.AGENT_RUNTIME, "governed_invoke",
                          wraps=hubflo_app.AGENT_RUNTIME.governed_invoke) as governed:
            self.ingress("Florida", "ci05-select-fl")
            self.ingress("explain Florida", "ci05-fl")
            self.ingress("SA", "ci05-select-sa")
            self.ingress("explain SA", "ci05-sa")
        self.assertEqual(governed.call_count, 2)
        calls = [call.args[0] for call in governed.call_args_list]
        self.assertEqual([i.principal.scope for i in calls], [Scope(7, "FL"), Scope(8, "SA")])
        self.assertEqual([i.principal.principal_id for i in calls],
                         ["user:%s" % self.user_id] * 2)
        self.assertNotIn("Florida", repr(calls[1].payload))
        self.assertNotIn("FL", repr(calls[1].principal.scope))

    def test_ci06_hub_never_becomes_default_client_or_mutates_stage2(self):
        self.test_ci03_hub_webhook_reaches_controlled_governed_invoke()
        self.assertIsNone(storage.client_id_for_sender(self.sender))

    def test_ci07_provider_sees_only_eligible_s0_s1(self):
        principal = Principal("user:%s" % self.user_id, "user", Scope(7, "FL"))
        eligible = hubflo_app.AGENT_RUNTIME.eligible_conversational_capabilities(principal)
        ids = {item["capability_id"] for item in eligible}
        self.assertTrue(ids)
        self.assertIn("manager_pa.assist", ids)
        self.assertTrue(all(item["side_effect_class"] in ("S0", "S1") for item in eligible))
        self.assertNotIn("stage2.invoke", ids)
        self.assertNotIn("channel.deliver", ids)
        repo = hubflo_app.AGENT_RUNTIME.repository
        repo.set_capability_state("capacity.assess", Principal("ci-authority", "service", Scope()), healthy=False)
        repo.set_capability_state("flo.client.reason", Principal("ci-authority", "service", Scope()), enabled=False)
        self.assertNotIn("capacity.assess", {x["capability_id"] for x in hubflo_app.AGENT_RUNTIME.eligible_conversational_capabilities(principal)})
        self.assertNotIn("flo.client.reason", {x["capability_id"] for x in hubflo_app.AGENT_RUNTIME.eligible_conversational_capabilities(principal)})
        with storage.SessionLocal() as s:
            s.query(AgentEntitlement).filter_by(client_id=7, subject="manager_pa.assist").update({"value_json": '{"enabled":false}'})
            s.commit()
        self.assertNotIn("manager_pa.assist", {x["capability_id"] for x in hubflo_app.AGENT_RUNTIME.eligible_conversational_capabilities(principal)})
        self.assertTrue(repo.revoke_entitlement("ci-ent:FL", 7))
        self.assertIsNone(repo.entitlement(7, "capability", "manager_pa.assist", principal.principal_id))
        self.assertNotIn("manager_pa.assist", {x["capability_id"] for x in hubflo_app.AGENT_RUNTIME.eligible_conversational_capabilities(principal)})

    def test_ci08_out_of_universe_executes_nothing(self):
        runtime = self.controlled({"selection": "selected", "capability_id": "outside", "arguments": {}, "evidence_refs": []})
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", runtime): self.ingress("explain", "ci08")
        self.assertFalse(hasattr(self, "invocation"))

    def test_ci09_s2_s3_s4_candidate_executes_nothing(self):
        repo = hubflo_app.AGENT_RUNTIME.repository
        authority = Principal("ci-authority", "service", Scope())
        repo.set_capability_state("channel.deliver", authority, enabled=True, healthy=True)
        repo.grant_authority("ci-s4", "user:%s" % self.user_id, "user", "channel.deliver", "CI",
                             7, "FL", max_autonomy=5, information_permissions=("reason", "read"),
                             action_permissions=("deliver",), allowed_domains=("SD3",), granting_actor=authority)
        principal = Principal("user:%s" % self.user_id, "user", Scope(7, "FL"))
        self.assertNotIn("channel.deliver", {x["capability_id"] for x in hubflo_app.AGENT_RUNTIME.eligible_conversational_capabilities(principal)})
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR",
                          self.actual_orchestrator({"selection":"selected", "capability_id":"channel.deliver",
                          "arguments":{}, "evidence_refs":[]})) as agent:
            self.ingress("deliver the report", "ci09")
        with storage.SessionLocal() as s:
            self.assertEqual(s.query(AgentExecution).filter_by(client_id=7, project_code="FL").count(), 0)

    def test_ci10_revoked_membership_cannot_retain_agent_authority(self):
        with storage.SessionLocal() as s: s.query(storage.SenderMembership).update({"active": False}); s.commit()
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR") as agent: self.ingress("explain", "ci10")
        agent.handle.assert_not_called()

    def test_ci11_replay_after_switch_cannot_retarget_agent(self):
        with storage.SessionLocal() as s:
            user = s.query(storage.User).one()
            s.add(storage.SenderMembership(user_id=user.id, context_kind="client", client_id=8,
                context_label="SA", role="sub", project_code="SA", authority_basis="fixture")); s.commit()
        orchestrator = self.actual_orchestrator()
        with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", orchestrator), \
             patch.object(hubflo_app.AGENT_RUNTIME, "governed_invoke",
                          wraps=hubflo_app.AGENT_RUNTIME.governed_invoke) as governed:
            self.ingress("Florida", "ci11-select-fl")
            self.ingress("explain", "ci11-event")
            self.ingress("SA", "ci11-select-sa")
            self.ingress("explain", "ci11-event")
        self.assertEqual(governed.call_count, 1)
        with storage.SessionLocal() as s:
            claims = s.query(storage.MultiContextInboundClaim).filter_by(event_id="ci11-event").all()
            self.assertEqual(len(claims), 1)
            self.assertEqual(claims[0].client_id, 7)
            self.assertNotIn("SA", claims[0].outcome or "")

    def test_ci12_sequential_messages_do_not_leak_state(self):
        with storage.SessionLocal() as s:
            user = s.query(storage.User).one()
            s.add(storage.SenderMembership(user_id=user.id, context_kind="client", client_id=8,
                context_label="SA", role="sub", project_code="SA", authority_basis="fixture")); s.commit()
        seen = []
        orchestrator = self.actual_orchestrator()
        original = hubflo_app.AGENT_RUNTIME.conversational_proposal
        def capture(provider, principal, request, event, membership, eligible, contract):
            seen.append((principal, dict(membership), [x["capability_id"] for x in eligible]))
            return original(provider, principal, request, event, membership, eligible, contract)
        with patch.object(hubflo_app.AGENT_RUNTIME, "conversational_proposal", side_effect=capture), \
             patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", orchestrator):
            self.ingress("Florida", "ci12-select-fl")
            self.ingress("explain one", "ci12a")
            self.ingress("SA", "ci12-select-sa")
            self.ingress("explain two", "ci12b")
        self.assertEqual([x[0].scope for x in seen], [Scope(7, "FL"), Scope(8, "SA")])
        self.assertEqual([x[1]["project_code"] for x in seen], ["FL", "SA"])
        self.assertEqual(seen[0][2], seen[1][2])

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
            with patch.object(hubflo_app, "AGENT_CONVERSATIONAL_ORCHESTRATOR", runtime):
                response = self.ingress("explain", "ci14-" + status)
            self.assertEqual(response.status_code, 200)
            self.assertTrue(self.sent[-1].startswith("I can’t provide"))
            with storage.SessionLocal() as s:
                claim = s.query(storage.MultiContextInboundClaim).filter_by(event_id="ci14-" + status).one()
                self.assertIn("agent-%s" % ("pending" if status == "PENDING" else "denied"), claim.outcome)


if __name__ == "__main__":
    unittest.main()
