import unittest
from types import SimpleNamespace

from agent_layer.contracts import Principal, Scope
from agent_layer.conversational import ConversationalOrchestrator
from agent_layer.providers import DeterministicProvider, ProviderRegistry


class RecordingRuntime:
    def __init__(self, proposal):
        self.providers = ProviderRegistry()
        self.providers.register(DeterministicProvider("selection", "test", lambda request: proposal))
        self.repository = SimpleNamespace(provider_policy=lambda provider_id: {"provider_id": provider_id})
        self.invocations = []
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
        return SimpleNamespace(status="COMPLETED", code="OK", outcome={"content": "bounded"})


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


if __name__ == "__main__":
    unittest.main()
