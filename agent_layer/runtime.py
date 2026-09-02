"""Agent Layer orchestration, deterministic policy gates and durable execution."""

import datetime as dt
import uuid
from typing import Any, Dict, Iterable, Mapping, Optional

from .contracts import (
    CapabilityDefinition,
    AuthorityError,
    ConflictError,
    ContractError,
    Invocation,
    InvocationResult,
    LEARNING_SCOPES,
    Principal,
    ProviderError,
    ProviderRequest,
    SecurityError,
    VerificationHandler,
    VERIFICATION_RESULTS,
    validate_structured,
)
from .persistence import AgentRepository, stable_hash
from .providers import ProviderRegistry
from .security import ContextAssembler, derive_composed_scope


OPTIONAL_FIELD_TYPES = {
    "operation": str,
    "target_capability_id": str, "context_refs": list, "reason": str,
    "expected_effective_version": (int, type(None)), "desired_outcome": str,
    "success_criteria": dict, "expected_version": int, "status": str,
    "dependencies": list, "evidence_refs": list, "risk_class": str,
    "outcome": dict, "user_id": str, "industry_key": str,
    "source_learning_id": int,
    "supersedes_id": int, "confidentiality": str, "confidence": dict,
    "validation": dict, "novelty_status": str, "channel": str,
    "output_type": str, "principal_id": str, "value": dict,
    "dimension": str, "subject": str,
    "version": int, "implicated_principal": str, "containment": dict,
}
OPTIONAL_OUTPUT_FIELD_TYPES = {"entitlement": (dict, type(None))}


def _definition(
    capability_id, family, purpose, schema, output, side="S1", risk="R1",
    concurrency="coalesced by idempotency key", claim=False,
    uncertain="not applicable: no consequential side effect",
    no_agent="STOP fail-closed for judgment; deterministic state remains available",
    optional=(), output_optional=(), input_semantics=(), output_semantics=(),
    action="invoke", entitlement=False,
    provider=False, approval=False, verification=False,
    shadow_applicability="supported",
    eligible=("user", "service", "agent"),
):
    execution_path = "provider" if provider else "agent_layer"
    if side in ("S3", "S4"):
        execution_path = "authoritative_adapter"
    return CapabilityDefinition(
        capability_id=capability_id, version="2.0.0", family=family,
        purpose=purpose, side_effect_class=side, risk_class=risk,
        eligible_principal_classes=eligible,
        input_schema=schema, optional_input_fields=tuple(optional),
        output_schema=output, optional_output_fields=tuple(output_optional),
        input_semantics=tuple(input_semantics or ("typed fields are exhaustive; additional fields denied",)),
        output_semantics=tuple(output_semantics or ("typed fields are exhaustive; additional fields denied",)),
        information_contract={
            "scope": "tenant/client/project intersection",
            "classification_required_for_provider": bool(provider),
            "security_domains": ("SD1", "SD3", "SD4"),
            "sensitive_value_handling": "credential material prohibited",
        },
        required_permission=action,
        required_configuration=("capability.enabled", "capability.healthy"),
        execution_path=execution_path,
        preconditions=("current authority", "current security policy", "scope isolation"),
        concurrency=concurrency, claim_required=claim,
        idempotency="scope-bound durable request hash",
        approval_contract=("authorized independent approver bound to request/scope/risk"
                           if approval else "not required"),
        audit_contract=("eligibility decision", "execution outcome", "authority/config provenance"),
        failure_contract={"before_effect": "deny or retry under policy",
                          "after_possible_effect": uncertain},
        uncertain_outcome=uncertain, no_agent_behavior=no_agent,
        verification_contract=("authorized independent verifier required"
                               if verification else "not required"),
        regression_dependencies=("accepted-stage-2", "tenant-isolation", "security-domain-isolation"),
        required_action=action,
        requires_entitlement=entitlement, requires_provider=provider,
        requires_approval=approval, requires_verification=verification,
        enabled=False,
        optional_input_schema={name: OPTIONAL_FIELD_TYPES[name] for name in optional
                               if name in OPTIONAL_FIELD_TYPES},
        optional_output_schema={name: OPTIONAL_OUTPUT_FIELD_TYPES[name] for name in output_optional
                                if name in OPTIONAL_OUTPUT_FIELD_TYPES},
        shadow_applicability=shadow_applicability,
    )


CAPABILITY_CATALOG = (
    _definition("control.supervise", "AL-CP-001", "Scoped management-by-exception and bounded delegation",
                {"operation": str}, {"status": str, "effective": dict},
                optional=("target_capability_id",), input_semantics=("known supervision operation",)),
    _definition("flo.industry.reason", "AL-FLO-001", "Industry-scoped vocabulary and interpretation",
                {"query": str, "industry_key": str, "evidence_refs": list},
                {"interpretation": str, "evidence_refs": list, "inference_state": str},
                input_semantics=("non-empty query", "industry scope matches principal"),
                output_semantics=("evidence-linked interpretation",), provider=True),
    _definition("flo.client.reason", "AL-FLO-001", "Client-private reasoning and learned context",
                {"query": str, "evidence_refs": list},
                {"interpretation": str, "evidence_refs": list, "inference_state": str},
                optional=("context_refs",), input_semantics=("non-empty query", "client scope required"),
                output_semantics=("evidence-linked interpretation",), provider=True),
    _definition("takeon.propose", "AL-TAKEON-001", "Non-authoritative Industry/Client Take-on or evolution proposal",
                {"phase": str, "requested_outcomes": list, "discovered_requirements": dict},
                {"proposal": dict, "consequences": list, "authority_status": str},
                input_semantics=("phase is industry/client/evolution", "requested outcomes non-empty"),
                output_semantics=("proposal is non-authoritative",), provider=True),
    _definition("configuration.propose", "AL-CFG-001", "Create a versioned non-effective configuration proposal",
                {"subject_key": str, "state": dict}, {"status": str, "configuration": dict},
                side="S2", action="propose", optional=("reason",),
                input_semantics=("non-empty subject", "non-empty change",)),
    _definition("configuration.commit", "AL-CFG-001", "Commit configuration through deterministic authority",
                {"subject_key": str, "version": int},
                {"status": str, "configuration": dict, "authoritative_evidence": dict},
                side="S4", risk="R4", claim=True,
                uncertain="inspect committed subject/version; unknown outcome STOP/ESCALATE", action="commit", approval=True, verification=True,
                optional=("expected_effective_version", "operation"), input_semantics=("positive version",)),
    _definition("manager_pa.assist", "AL-PA-001", "Scoped practical help, explanation and action proposal",
                {"request": str, "assistance_mode": str, "evidence_refs": list},
                {"assistance_type": str, "content": str, "evidence_refs": list,
                 "proposed_action": dict},
                input_semantics=("non-empty request", "supported assistance mode"),
                output_semantics=("proposal cannot claim execution",), provider=True, entitlement=True),
    _definition("guardian.diagnose", "AL-PLAT-001", "Observe, diagnose, escalate and recommend governed remediation",
                {"affected_component": str, "observations": list, "evidence_refs": list},
                {"diagnosis": str, "evidence_refs": list, "affected_scope": dict,
                 "confidence": dict, "escalation": str, "recommendation": str},
                input_semantics=("observations and evidence required",),
                output_semantics=("diagnosis cannot claim remediation",), provider=True),
    _definition("guardian.remediate", "AL-PLAT-001", "Separately governed bounded remediation",
                {"remediation_action": str, "target": str, "parameters": dict},
                {"status": str, "authoritative_evidence": dict}, side="S4", risk="R4", claim=True,
                uncertain="inspect remediation outcome; unknown outcome STOP/ESCALATE", approval=True, verification=True,
                shadow_applicability="not_applicable: remediation planning requires authoritative adapter"),
    _definition("capacity.assess", "AL-CAP-001", "Evidence-linked capacity, resilience, cost and architecture assessment",
                {"metric_samples": list, "horizon": str, "evidence_refs": list},
                {"assessment": str, "evidence_refs": list, "assumptions": list,
                 "recommendation": str}, input_semantics=("metric samples required", "non-empty horizon"),
                output_semantics=("recommendation does not authorize migration",), provider=True),
    _definition("channel.deliver", "AL-CHAN-001", "Channel-neutral governed delivery",
                {"recipient_id": str, "channel": str, "message_ref": str, "content_ref": str},
                {"delivery_id": str, "status": str, "authoritative_evidence": dict},
                side="S3", risk="R2", claim=True,
                uncertain="inspect channel delivery evidence; unknown outcome remains pending",
                shadow_applicability="not_applicable: external channel delivery requires live adapter"),
    _definition("objective.manage", "AL-OBJ-001", "Create and transition durable objectives",
                {"operation": str, "objective_key": str}, {"status": str, "objective": dict},
                side="S2", action="manage",
                optional=("desired_outcome", "success_criteria", "expected_version", "status",
                          "dependencies", "evidence_refs", "risk_class"),
                input_semantics=("operation-specific fields required",)),
    _definition("critical_path.analyze", "AL-CRIT-001", "Evidence-linked dependency and critical-path inference",
                {"operational_refs": list, "known_dependencies": list, "horizon": str},
                {"known_facts": list, "inferred_dependencies": list, "blockers": list,
                 "risk": dict, "evidence_refs": list},
                input_semantics=("operational evidence required", "non-empty horizon"),
                output_semantics=("facts and inference separated",), provider=True),
    _definition("consequence.analyze", "AL-CONS-001", "Consequence and intervention recommendation without mutation",
                {"event_ref": str, "time_horizon": str, "candidate_capabilities": list},
                {"consequence": str, "severity": str, "confidence": dict,
                 "intervention": dict, "required_authority": list,
                 "expected_benefit": str, "risk": str},
                input_semantics=("event and horizon required",),
                output_semantics=("intervention references registered capability",), provider=True),
    _definition("performance.analyze", "AL-PERF-001", "Scoped performance prediction and outcome comparison",
                {"metric": str, "window": str, "evidence_refs": list},
                {"prediction": dict, "evidence_refs": list, "source_age": str,
                 "comparison": dict, "confidence": dict},
                input_semantics=("metric/evidence/window required",), provider=True),
    _definition("learning.persist", "AL-LEARN-001", "Persist scoped outcome-linked learning",
                {"learning_key": str, "learning_scope": str, "observation_refs": list,
                 "finding": dict, "provenance": dict, "retention_basis": str},
                {"status": str, "learning": dict}, side="S2", action="learn",
                optional=("outcome", "user_id", "industry_key", "supersedes_id",
                          "operation", "source_learning_id")),
    _definition("intelligence.persist", "AL-INTEL-001", "Persist/refine/supersede derived intelligence",
                {"intelligence_key": str, "intelligence_type": str, "source_refs": list,
                 "content": dict, "provenance": dict, "retention_basis": str},
                {"status": str, "intelligence": dict}, side="S2", action="persist",
                optional=("industry_key", "confidentiality", "confidence", "validation",
                          "novelty_status", "supersedes_id")),
    _definition("distribution.decide", "AL-DIST-001", "Separate awareness, use, supervision, distribution and recipient eligibility",
                {"intelligence_key": str, "decision_kind": str, "target_principal_id": str},
                {"status": str, "distribution_decision": dict}, optional=("channel", "output_type"),
                input_semantics=("known decision kind", "resolved recipient identity"),
                output_semantics=("authority resolved from current state",), side="S2", action="decide"),
    _definition("distribution.deliver", "AL-DIST-001", "Governed recipient delivery",
                {"intelligence_key": str, "recipient_id": str, "channel": str,
                 "decision_ref": str}, {"delivery_id": str, "status": str,
                 "authoritative_evidence": dict}, side="S3", risk="R2", claim=True,
                uncertain="inspect authoritative delivery receipt; unknown outcome remains pending", action="deliver", entitlement=True,
                shadow_applicability="not_applicable: external distribution delivery requires live adapter"),
    _definition("entitlement.evaluate", "AL-ENT-001", "Deterministic commercial/product eligibility evaluation",
                {"dimension": str, "subject": str}, {"status": str, "entitled": bool},
                output_optional=("entitlement",), side="S0", risk="R0", optional=("principal_id",),
                no_agent="continue deterministically"),
    _definition("entitlement.configure", "AL-ENT-001", "Versioned entitlement assignment/revocation",
                {"operation": str, "entitlement_key": str},
                {"status": str, "entitlement": dict, "authoritative_evidence": dict},
                side="S4", risk="R4", claim=True,
                uncertain="inspect current assignment version; unknown outcome STOP", action="configure", approval=True, verification=True,
                optional=("dimension", "subject", "value", "version", "principal_id")),
    _definition("autonomy.evaluate", "AL-AUTO-001", "Deterministic autonomy/approval/disable evaluation",
                {"target_capability_id": str, "requested_level": int},
                {"status": str, "assigned_level": int, "decision": str, "limits": dict},
                side="S0", risk="R0",
                no_agent="continue deterministically"),
    _definition("stage2.invoke", "AL-TOOL-001", "Invoke an injected authoritative Stage 2 capability without duplicating it",
                {"stage2_capability": str, "structured_input": dict},
                {"status": str, "authoritative_evidence": dict}, side="S3", risk="R2", claim=True,
                uncertain="inspect Stage 2 authoritative idempotency/outcome evidence; unknown outcome STOP/ESCALATE",
                shadow_applicability="not_applicable: Stage 2 consequence is external authoritative behavior"),
    _definition("graph.execute", "AL-GRAPH-001", "Durable bounded DAG/fan-out/fan-in execution",
                {"objective_key": str, "required_work_keys": list}, {"status": str, "fan_in": dict},
                side="S2", action="execute", input_semantics=("at least one required work key",)),
    _definition("verification.verify", "AL-VERIFY-001", "Independent structured PASS/FAIL/INCONCLUSIVE verification",
                {"execution_id": int}, {"status": str, "execution_id": int}, side="S1", action="verify"),
    _definition("provider.reason", "AL-PROV-001", "Provider-neutral structured reasoning",
                {"purpose": str, "question": str, "evidence_refs": list},
                {"analysis": dict, "evidence_refs": list},
                input_semantics=("purpose/question/evidence required",), provider=True),
    _definition("noagent.continuity", "AL-NOAI-001", "Deterministic no-agent continuity and explicit degradation",
                {"requested_operation": str}, {"status": str}, side="S0", risk="R0",
                no_agent="continue deterministically"),
    _definition("market.analyze", "AL-MARKET-001", "Attributable freshness-aware market/technology intelligence",
                {"query": str, "source_refs": list, "observed_at": str, "retrieved_at": str},
                {"finding": str, "source_refs": list, "observed_at": str,
                 "retrieved_at": str, "recommendation": str, "authorizes_action": bool},
                input_semantics=("attributed sources and freshness timestamps required",),
                output_semantics=("cannot authorize action",), provider=True),
    _definition("help.discover", "AL-HELP-001", "Discover only effective enabled and authorized functions",
                {"request": str}, {"status": str, "capabilities": list}, side="S0", risk="R0",
                no_agent="continue deterministically"),
    _definition("security.observe", "AL-SEC-001", "Record protected immutable security observations",
                {"event_key": str, "event_type": str, "severity": str,
                 "security_domain": str, "evidence": dict}, {"status": str, "security_event": dict},
                side="S2", risk="R2", action="observe",
                optional=("implicated_principal", "containment"),
                no_agent="continue deterministically"),
    _definition("security.contain", "AL-SEC-001", "Deterministic governed incident containment",
                {"event_key": str, "actions": dict},
                {"status": str, "security_event": dict, "authoritative_evidence": dict},
                side="S4", risk="R4", claim=True,
                uncertain="inspect capability/provider disable state; unknown outcome STOP/ESCALATE", action="contain", approval=True, verification=True,
                no_agent="continue deterministic containment when separately authorized",
                shadow_applicability="not_applicable: containment requires authoritative event mutation"),
)


class AgentRuntime:
    """Configurable runtime with no implicit authority or provider approval."""

    def __init__(self, repository=None, providers=None, runtime_id=None):
        self.repository = repository or AgentRepository()
        self.providers = providers or ProviderRegistry()
        self.runtime_id = runtime_id or "runtime-%s" % uuid.uuid4().hex
        self.context_assembler = ContextAssembler()
        self._definitions = {item.capability_id: item for item in CAPABILITY_CATALOG}
        self._handlers = {}
        self._outcome_inspectors = {}
        self._verifiers = {}
        self.repository.init_schema()
        for definition in CAPABILITY_CATALOG:
            self.repository.upsert_capability(definition)
        self._builtin_handlers = {
            "control.supervise": self._execute_control,
            "configuration.propose": self._execute_configuration_propose,
            "configuration.commit": self._execute_configuration_commit,
            "objective.manage": self._execute_objective,
            "learning.persist": self._execute_learning,
            "intelligence.persist": self._execute_intelligence,
            "distribution.decide": self._execute_distribution_decision,
            "entitlement.evaluate": self._execute_entitlement,
            "entitlement.configure": self._execute_entitlement_configure,
            "autonomy.evaluate": self._execute_autonomy,
            "graph.execute": self._execute_graph,
            "verification.verify": self._execute_verification_status,
            "noagent.continuity": self._execute_noagent,
            "help.discover": self._execute_help,
            "security.observe": self._execute_security_observe,
            "security.contain": self._execute_security_contain,
        }

    def register_handler(self, capability_id, handler, outcome_inspector=None) -> None:
        if capability_id not in self._definitions:
            raise ContractError("unregistered capability")
        self._handlers[capability_id] = handler
        if outcome_inspector is not None:
            self._outcome_inspectors[capability_id] = outcome_inspector

    def register_verifier(self, capability_id: str, verifier: VerificationHandler) -> None:
        if capability_id not in self._definitions:
            raise ContractError("unregistered capability")
        self._verifiers[capability_id] = verifier

    def effective_state(self, principal: Principal, capability_id: str) -> Dict[str, Any]:
        capability = self.repository.capability(capability_id)
        if capability is None:
            return {"decision": "DENY", "reason": "UNREGISTERED_CAPABILITY"}
        subjects = ["platform"]
        if principal.scope.client_id is not None:
            subjects.append("client:%s" % principal.scope.client_id)
        if principal.scope.project_code:
            subjects.append("project:%s:%s" % (principal.scope.client_id, principal.scope.project_code))
        subjects.append("role:%s" % principal.principal_class)
        subjects.append("capability:%s" % capability_id)
        configured = self.repository.effective_configuration(
            subjects, principal.scope.client_id, principal.scope.project_code)
        state = configured["effective"]
        definition = self._definitions.get(capability_id)
        disable_reasons = []
        if state.get("agent_layer_disabled", False):
            disable_reasons.append("global")
        if capability_id in set(state.get("disabled_capabilities", [])):
            disable_reasons.append("capability")
        if principal.principal_class in set(state.get("disabled_agent_roles", [])):
            disable_reasons.append("agent_role")
        if definition and definition.required_action in set(state.get("disabled_actions", [])):
            disable_reasons.append("action")
        if capability["risk_class"] in set(state.get("disabled_risk_classes", [])):
            disable_reasons.append("risk")
        disabled = bool(disable_reasons)
        grant = self.repository.current_grant(
            principal.principal_id, capability_id,
            principal.scope.client_id, principal.scope.project_code,
        )
        principal_authority = self.repository.actor_authority(
            principal, principal.scope.client_id, principal.scope.project_code, exact_scope=True)
        principal_current = bool(principal_authority)
        if principal_authority and definition:
            capabilities = set(principal_authority.get("capabilities", []))
            principal_current = ("*" in capabilities or capability_id in capabilities)
        return {
            "decision": "ALLOW" if capability["enabled"] and capability["healthy"] and not disabled and grant and principal_current else "DENY",
            "capability": capability,
            "grant": grant,
            "principal_authority": principal_authority if principal_current else None,
            "configuration": configured,
            "disabled": disabled, "disable_reasons": disable_reasons,
            "explanation": {
                "registered": True, "enabled": capability["enabled"],
                "healthy": capability["healthy"], "current_grant": bool(grant),
                "configuration_disabled": disabled, "disable_reasons": disable_reasons,
                "current_principal_authority": principal_current,
            },
        }

    def governed_invoke(self, invocation: Invocation) -> InvocationResult:
        request_hash = stable_hash({
            "capability_id": invocation.capability_id,
            "principal": invocation.principal.principal_id,
            "client_id": invocation.principal.scope.client_id,
            "project_code": invocation.principal.scope.project_code,
            "payload": invocation.payload,
            **({"evidence_refs": tuple(invocation.evidence_refs)}
               if invocation.evidence_refs else {}),
            "requested_autonomy": invocation.requested_autonomy,
            "shadow_mode": invocation.shadow_mode,
        })
        effective = self.effective_state(invocation.principal, invocation.capability_id)
        denial = self._gate(invocation, effective, request_hash)
        if denial:
            return denial
        prior = self.repository.execution(
            invocation.idempotency_key, invocation.principal.scope.client_id,
            invocation.principal.scope.project_code,
        )
        if prior:
            return self._handle_prior(invocation, prior, request_hash)

        capability = effective["capability"]
        definition = self._definitions[invocation.capability_id]
        try:
            validate_structured(invocation.payload, definition.input_schema,
                                definition.optional_input_fields,
                                optional_schema=definition.optional_input_schema)
            self._validate_input_semantics(invocation)
        except ContractError as exc:
            return self._deny(invocation, "INVALID_INPUT_CONTRACT", str(exc), effective)
        if invocation.capability_id == "configuration.commit" and invocation.payload.get("operation", "commit") == "commit":
            try:
                self.repository.plan_configuration_commit(
                    invocation.payload["subject_key"], invocation.payload["version"],
                    invocation.payload.get("expected_effective_version"),
                    invocation.principal.scope.client_id, invocation.principal.scope.project_code)
            except ConflictError as exc:
                return self._deny(invocation, "CONFIGURATION_PRECONDITION_FAILED", str(exc), effective)
        if invocation.capability_id == "configuration.commit" and invocation.payload.get("operation") == "revoke":
            try:
                self.repository.plan_configuration_revoke(
                    invocation.payload["subject_key"], invocation.payload["version"],
                    invocation.principal.scope.client_id, invocation.principal.scope.project_code)
            except ConflictError as exc:
                return self._deny(invocation, "CONFIGURATION_PRECONDITION_FAILED", str(exc), effective)
        if invocation.capability_id == "objective.manage" and invocation.payload.get("operation") == "transition":
            objective = self.repository.objective(
                invocation.payload["objective_key"], invocation.principal.scope.client_id,
                invocation.principal.scope.project_code)
            allowed = {"pending": {"active", "cancelled"}, "active": {"blocked", "completed", "cancelled"},
                       "blocked": {"active", "cancelled"}, "completed": set(), "cancelled": set()}
            if (objective is None or objective["version"] != invocation.payload["expected_version"] or
                    invocation.payload["status"] not in allowed.get(objective["status"], set())):
                return self._deny(invocation, "OBJECTIVE_PRECONDITION_FAILED",
                                  "invalid or stale objective transition", effective)

        provider_result = None
        provider_context = None
        if capability["requires_provider"]:
            try:
                provider_result, provider_context = self._reason(invocation, effective)
            except (ProviderError, SecurityError, ContractError) as exc:
                return self._deny(invocation, "PENDING_PROVIDER_UNAVAILABLE", str(exc), effective)

        claim = self.repository.claim_execution(
            invocation, capability, request_hash, self.runtime_id
        )
        if claim["status"] != "claimed":
            return self._handle_prior(invocation, claim["execution"], request_hash)
        execution = claim["execution"]

        if invocation.shadow_mode:
            # Providers already ran the real reasoning path above. For local
            # deterministic capabilities, execute only a side-effect-free
            # preview; consequential handlers are deliberately never called.
            outcome = self._execute_shadow(invocation, provider_result, effective)
            validate_structured(outcome, definition.output_schema,
                                definition.optional_output_fields,
                                optional_schema=definition.optional_output_schema)
            self._validate_output_semantics(invocation, outcome)
            evidence = {"kind": "shadow-proposed-result", "mutation_prevented": True,
                        "request_hash": request_hash, "proposed_outcome": dict(outcome)}
            completed = self.repository.complete_execution(
                execution["id"], claim["claim_token"], outcome, evidence)
            audit_id = self.repository.audit(
                "capability_shadow", invocation.principal.principal_id, "SHADOWED",
                {"execution_id": execution["id"], "would_have_done": dict(outcome)},
                invocation.principal.scope.client_id, invocation.principal.scope.project_code,
                invocation.capability_id)
            return InvocationResult("COMPLETED", "SHADOWED", execution["id"],
                                    completed["outcome"], audit_id)

        verification_id = None
        if capability["requires_verification"]:
            verification = self._verify(invocation, execution["id"])
            if verification["result"] != "PASS":
                self.repository.mark_execution_uncertain(
                    execution["id"], claim["claim_token"],
                    "VERIFICATION_%s" % verification["result"],
                )
                return self._result(invocation, "STOP", "VERIFICATION_%s" % verification["result"], execution["id"])
            verification_id = verification["id"]

        try:
            if capability["side_effect_class"] in ("S3", "S4"):
                handler = self._handlers.get(invocation.capability_id)
                if handler is None and invocation.capability_id in (
                    "configuration.commit", "entitlement.configure", "security.contain"
                ):
                    handler = lambda payload: self._execute_builtin(invocation, payload)
                if handler is None:
                    raise ContractError("authoritative handler unavailable")
                # Revocation/disable is checked again after claim and before consequence.
                post_claim = self.effective_state(invocation.principal, invocation.capability_id)
                post_denial = self._gate(invocation, post_claim, request_hash, audit=False)
                if post_denial:
                    self.repository.mark_execution_uncertain(
                        execution["id"], claim["claim_token"], "AUTHORITY_REVOKED_AFTER_CLAIM"
                    )
                    return self._result(invocation, "STOP", "AUTHORITY_REVOKED_AFTER_CLAIM", execution["id"])
                outcome = handler(dict(invocation.payload))
                if not isinstance(outcome, Mapping):
                    raise ContractError("handler must return structured authoritative outcome")
                authoritative_evidence = outcome.get("authoritative_evidence")
                if not isinstance(authoritative_evidence, Mapping):
                    raise ContractError("consequential handler omitted authoritative outcome evidence")
            elif provider_result is not None:
                outcome = dict(provider_result.output)
                authoritative_evidence = {
                    "kind": "derived-provider-output",
                    "provider": provider_result.provider_id,
                    "version": provider_result.provider_version,
                }
            else:
                handler = self._handlers.get(invocation.capability_id)
                outcome = (handler(dict(invocation.payload)) if handler
                           else self._execute_builtin(invocation, invocation.payload))
                if not isinstance(outcome, Mapping):
                    raise ContractError("handler must return structured output")
                authoritative_evidence = {"kind": "deterministic-agent-layer-state"}

            validate_structured(outcome, definition.output_schema,
                                definition.optional_output_fields,
                                optional_schema=definition.optional_output_schema)
            self._validate_output_semantics(invocation, outcome)

            approval = self.repository.approval(
                invocation.approval_key, invocation.principal.scope.client_id,
                invocation.principal.scope.project_code,
            ) if invocation.approval_key else None
            completed = self.repository.complete_execution(
                execution["id"], claim["claim_token"], outcome,
                authoritative_evidence,
                provider_id=provider_result.provider_id if provider_result else None,
                provider_version=provider_result.provider_version if provider_result else None,
                approval_id=approval["id"] if approval else None,
                verification_id=verification_id,
            )
            audit_id = self.repository.audit(
                "capability_invocation", invocation.principal.principal_id,
                "COMPLETED", {
                    "execution_id": execution["id"], "idempotency_key": invocation.idempotency_key,
                    "claim": claim["claim_token"], "approval_id": approval["id"] if approval else None,
                    "verification_id": verification_id, "provider_id": provider_result.provider_id if provider_result else None,
                    "evidence_refs": list(invocation.evidence_refs),
                }, invocation.principal.scope.client_id, invocation.principal.scope.project_code,
                invocation.capability_id,
            )
            return InvocationResult(
                status="COMPLETED", code="OK", execution_id=execution["id"],
                outcome=completed["outcome"], audit_id=audit_id,
            )
        except Exception as exc:
            if capability["side_effect_class"] in ("S3", "S4"):
                self.repository.mark_execution_uncertain(
                    execution["id"], claim["claim_token"], type(exc).__name__)
                return self._result(invocation, "ESCALATE", "OUTCOME_UNCERTAIN", execution["id"])
            self.repository.fail_execution(execution["id"], claim["claim_token"], type(exc).__name__)
            return self._result(invocation, "DENIED", "INVALID_OUTPUT_CONTRACT", execution["id"])

    def _gate(self, invocation, effective, request_hash, audit=True):
        capability = effective.get("capability")
        reason = None
        if capability is None:
            reason = "UNREGISTERED_CAPABILITY"
        elif not capability["enabled"]:
            reason = "CAPABILITY_DISABLED"
        elif not capability["healthy"]:
            reason = "CAPABILITY_UNHEALTHY"
        elif effective.get("disabled"):
            reason = "POLICY_DISABLED"
        elif not effective.get("grant"):
            reason = "UNAUTHORIZED"
        elif not effective.get("principal_authority"):
            reason = "CURRENT_PRINCIPAL_AUTHORITY_REQUIRED"
        elif invocation.principal.principal_class not in set(
            self._definitions[invocation.capability_id].eligible_principal_classes
        ):
            reason = "PRINCIPAL_CLASS_DENIED"
        else:
            grant = effective["grant"]
            principal_authority = effective["principal_authority"]
            if grant["client_id"] is not None and grant["client_id"] != invocation.principal.scope.client_id:
                reason = "CLIENT_SCOPE_DENIED"
            elif grant["project_code"] is not None and grant["project_code"] != invocation.principal.scope.project_code:
                reason = "PROJECT_SCOPE_DENIED"
            elif invocation.requested_autonomy > grant["max_autonomy"]:
                reason = "AUTONOMY_CEILING"
            elif invocation.requested_autonomy > int(principal_authority.get("max_autonomy", 0)):
                reason = "PRINCIPAL_AUTONOMY_CEILING"
            elif self._definitions[invocation.capability_id].required_action not in grant["action_permissions"]:
                reason = "ACTION_NOT_GRANTED"
            elif (invocation.capability_id == "learning.persist" and
                  invocation.payload.get("operation") == "promote"):
                if "learning.promote" not in set(principal_authority.get("permissions", [])):
                    reason = "SOURCE_PROMOTION_AUTHORITY_REQUIRED"
                target_authority = self.repository.actor_authority(
                    invocation.principal, None, None, exact_scope=True)
                if (reason is None and (target_authority is None or
                        "learning.promote" not in set(target_authority.get("permissions", [])))):
                    reason = "TARGET_PROMOTION_AUTHORITY_REQUIRED"
                if reason is None and invocation.payload.get("learning_scope") == "industry":
                    if invocation.payload.get("industry_key") not in set(
                            target_authority.get("industry_keys", [])):
                        reason = "TARGET_INDUSTRY_AUTHORITY_REQUIRED"
                if reason is None:
                    policy = self.learning_promotion_eligibility(
                        invocation.principal, "client", invocation.payload.get("learning_scope"))
                    if policy.get("status") != "ELIGIBLE_FOR_GOVERNED_PROMOTION":
                        reason = "PROMOTION_POLICY_REQUIRED"
            elif capability["requires_entitlement"]:
                entitlement = self.repository.entitlement(
                    invocation.principal.scope.client_id, "capability",
                    invocation.capability_id, invocation.principal.principal_id,
                )
                if entitlement is None or not bool(entitlement["value"].get("enabled", False)):
                    reason = "NOT_ENTITLED"
            if reason is None and invocation.capability_id == "distribution.deliver":
                decision = self.repository.distribution(
                    invocation.payload.get("decision_ref"),
                    invocation.principal.scope.client_id,
                    invocation.principal.scope.project_code)
                intelligence = self.repository.intelligence(
                    invocation.payload.get("intelligence_key"),
                    invocation.principal.scope.client_id,
                    invocation.principal.scope.project_code)
                if (decision is None or decision["status"] != "ALLOW-DISTRIBUTION" or
                        decision.get("recipient_id") != invocation.payload.get("recipient_id") or
                        decision.get("channel") != invocation.payload.get("channel") or
                        not intelligence or intelligence[0]["id"] != decision["intelligence_id"]):
                    reason = "AUTHORITATIVE_DISTRIBUTION_DECISION_REQUIRED"
            if reason is None and invocation.shadow_mode and not grant.get("shadow_allowed"):
                reason = "SHADOW_NOT_AUTHORIZED"
            if reason is None and capability["side_effect_class"] in ("S3", "S4") and not invocation.shadow_mode:
                if invocation.requested_autonomy < 3:
                    reason = "APPROVAL_REQUIRED"
                elif invocation.requested_autonomy >= 4 and not self._within_autonomy_limits(
                        invocation.payload, grant.get("autonomy_limits") or {}):
                    reason = "AUTONOMY_LIMIT_DENIED"
            approval_required = (capability["requires_approval"] or capability["risk_class"] == "R4" or
                                 (capability["side_effect_class"] in ("S3", "S4") and
                                  invocation.requested_autonomy == 3))
            if reason is None and approval_required and not invocation.shadow_mode:
                approval = self.repository.approval(
                    invocation.approval_key, invocation.principal.scope.client_id,
                    invocation.principal.scope.project_code,
                ) if invocation.approval_key else None
                if approval is None:
                    reason = "APPROVAL_REQUIRED"
                elif approval["capability_id"] != invocation.capability_id:
                    reason = "APPROVAL_CAPABILITY_MISMATCH"
                elif approval["client_id"] != invocation.principal.scope.client_id:
                    reason = "APPROVAL_SCOPE_MISMATCH"
                elif approval["project_code"] != invocation.principal.scope.project_code:
                    reason = "APPROVAL_SCOPE_MISMATCH"
                elif approval["request_hash"] != request_hash:
                    reason = "APPROVAL_PARAMETERS_CHANGED"
                elif approval["principal_id"] == invocation.principal.principal_id:
                    reason = "SELF_APPROVAL_DENIED"
                else:
                    try:
                        self.repository.require_actor(
                            approval["principal_id"], "action.approve",
                            invocation.principal.scope.client_id,
                            invocation.principal.scope.project_code,
                            invocation.capability_id, capability["risk_class"])
                    except AuthorityError:
                        reason = "APPROVER_AUTHORITY_REVOKED"
        if reason:
            return self._deny(invocation, reason, reason, effective, audit=audit)
        return None

    def _reason(self, invocation, effective):
        provider_id = invocation.provider_id
        if not provider_id:
            raise ProviderError("no provider configured")
        primary_policy = self.repository.provider_policy(provider_id)
        if primary_policy is None:
            raise ProviderError("provider not approved")
        if primary_policy["training_permitted"]:
            raise ProviderError("shared/general provider training mode is incompatible")
        grant = effective["grant"]
        if invocation.payload_item is None:
            raise SecurityError("classified invocation payload item required for provider exposure")
        if invocation.payload_item.value != dict(invocation.payload):
            raise SecurityError("classified payload does not match invocation payload")
        items = (invocation.payload_item,) + tuple(invocation.protected_context)
        context = self.context_assembler.assemble(
            invocation.principal, items, grant["allowed_domains"], "reason",
            for_provider=True, provider_policy=primary_policy,
        )
        classes = {item.security_domain for item in items}
        provider_ids = [provider_id]
        for fallback_id in invocation.fallback_provider_ids:
            fallback_policy = self.repository.provider_policy(str(fallback_id))
            if fallback_policy is None or fallback_policy["training_permitted"]:
                continue
            if not self._provider_policy_preserves(primary_policy, fallback_policy, items):
                continue
            provider_ids.append(str(fallback_id))
        output_contract = self._definitions[invocation.capability_id].output_schema
        request = ProviderRequest(
            operation=invocation.capability_id,
            context={"classified_items": context},
            output_contract=output_contract,
            optional_output_fields=self._definitions[invocation.capability_id].optional_output_fields,
            optional_output_schema=self._definitions[invocation.capability_id].optional_output_schema,
        )
        result = self.providers.invoke(request, provider_ids)
        return result, context

    def _execute_shadow(self, invocation, provider_result, effective):
        if provider_result is not None:
            return dict(provider_result.output)
        shadow_contract = effective["capability"].get("contract", {})
        if shadow_contract.get("shadow_applicability", "supported").startswith("not_applicable"):
            reason = shadow_contract["shadow_applicability"]
            if invocation.capability_id == "security.contain":
                return {"status": "SHADOW-NOT-APPLICABLE", "security_event": {},
                        "authoritative_evidence": {"kind": "shadow-non-applicable",
                                                    "reason": reason}}
            if invocation.capability_id == "guardian.remediate":
                return {"status": "SHADOW-NOT-APPLICABLE",
                        "authoritative_evidence": {"kind": "shadow-non-applicable",
                                                    "reason": reason}}
            if invocation.capability_id in ("distribution.deliver", "channel.deliver"):
                return {"delivery_id": stable_hash(invocation.payload)[:32],
                        "status": "SHADOW-NOT-APPLICABLE",
                        "authoritative_evidence": {"kind": "shadow-non-applicable",
                                                    "reason": reason}}
            if invocation.capability_id == "stage2.invoke":
                return {"status": "SHADOW-NOT-APPLICABLE",
                        "authoritative_evidence": {"kind": "shadow-non-applicable",
                                                    "reason": reason}}
        builtin = self._builtin_handlers.get(invocation.capability_id)
        if builtin is not None:
            return builtin(invocation, dict(invocation.payload), shadow=True)
        # External consequential adapters must expose no mutation in shadow;
        # their contract-shaped proposal is attributed as unresolved rather
        # than invoking the authoritative handler.
        if invocation.capability_id == "stage2.invoke":
            return {"status": "SHADOW-NOT-APPLICABLE",
                    "authoritative_evidence": {"kind": "shadow-non-applicable",
                                                "reason": shadow_contract["shadow_applicability"]}}
        if invocation.capability_id == "channel.deliver":
            return {"delivery_id": stable_hash(invocation.payload)[:32], "status": "PROPOSED",
                    "authoritative_evidence": {"kind": "delivery-proposal"}}
        raise ContractError("shadow planner unavailable for capability")

    def _verify(self, invocation, execution_id):
        verifier = self._verifiers.get(invocation.capability_id)
        if verifier is None:
            return {"id": None, "result": "INCONCLUSIVE", "independent": False}
        response = verifier(invocation)
        result = response.get("result")
        if result not in VERIFICATION_RESULTS:
            result = "INCONCLUSIVE"
        verifier_principal = response.get("verifier_principal")
        if not isinstance(verifier_principal, Principal):
            return {"id": None, "result": "INCONCLUSIVE", "independent": False}
        try:
            return self.repository.record_verification(
                execution_id, verifier_principal,
                str(response.get("verifier_kind") or "unknown"), result,
                response.get("evidence") or {}, invocation.principal,
            )
        except (AuthorityError, SecurityError):
            return {"id": None, "result": "INCONCLUSIVE", "independent": False}

    @staticmethod
    def _provider_policy_preserves(primary, fallback, items):
        """A fallback may only preserve or tighten every applicable data constraint."""
        subset_fields = (
            "allowed_data_classes", "allowed_confidentiality", "permitted_uses",
            "allowed_regions", "allowed_distribution_uses",
        )
        if any(not set(fallback.get(field, [])).issubset(set(primary.get(field, [])))
               for field in subset_fields):
            return False
        if not set(primary.get("access_controls", [])).issubset(
            set(fallback.get("access_controls", []))
        ):
            return False
        primary_max = primary.get("retention_max_seconds")
        fallback_max = fallback.get("retention_max_seconds")
        if primary_max is not None and (fallback_max is None or fallback_max > primary_max):
            return False
        for item in items:
            if item.security_domain not in set(fallback.get("allowed_data_classes", [])):
                return False
            if item.confidentiality not in set(fallback.get("allowed_confidentiality", [])):
                return False
            if "reason" not in set(fallback.get("permitted_uses", [])):
                return False
            regions = set(fallback.get("allowed_regions", []))
            if regions and item.region not in regions:
                return False
            if not set(item.access_requirements).issubset(set(fallback.get("access_controls", []))):
                return False
        for flag in ("audit_required", "attribution_required"):
            if primary.get(flag) and not fallback.get(flag):
                return False
        for flag in ("deletion_supported", "withdrawal_supported"):
            if any(getattr(item, flag.replace("_supported", "_required")) for item in items):
                if not fallback.get(flag):
                    return False
        return fallback.get("retention_mode") == primary.get("retention_mode")

    def _execute_builtin(self, invocation, payload):
        handler = self._builtin_handlers.get(invocation.capability_id)
        if handler is None:
            raise ContractError("capability-owned execution path unavailable")
        return handler(invocation, dict(payload))

    def _execute_control(self, invocation, payload):
        return {"status": "SUPERVISION_STATE", "effective": self.effective_state(
            invocation.principal, invocation.capability_id)}

    def _execute_configuration_propose(self, invocation, payload, shadow=False):
        if shadow:
            return {"status": "PROPOSED", "configuration": {
                "subject_key": payload["subject_key"], "state": payload["state"],
                "status": "proposed", "client_id": invocation.principal.scope.client_id,
                "project_code": invocation.principal.scope.project_code}}
        row = self.repository.propose_configuration(
            payload["subject_key"], payload["state"], invocation.principal.principal_id,
            invocation.idempotency_key, invocation.principal.scope.client_id,
            invocation.principal.scope.project_code, payload.get("reason"),
        )
        return {"status": "PROPOSED", "configuration": row}

    def _execute_configuration_commit(self, invocation, payload, shadow=False):
        if shadow:
            if payload.get("operation") == "revoke":
                planned = self.repository.plan_configuration_revoke(
                    payload["subject_key"], payload["version"],
                    invocation.principal.scope.client_id, invocation.principal.scope.project_code)
                planned["status"] = "revoked"
            else:
                planned = self.repository.plan_configuration_commit(
                    payload["subject_key"], payload["version"], payload.get("expected_effective_version"),
                    invocation.principal.scope.client_id, invocation.principal.scope.project_code)
                planned["status"] = "effective"
            return {"status": "REVOKED" if payload.get("operation") == "revoke" else "COMMITTED", "configuration": {
                **planned},
                    "authoritative_evidence": {"kind": "configuration-proposal"}}
        approval = self.repository.approval(
            invocation.approval_key, invocation.principal.scope.client_id,
            invocation.principal.scope.project_code,
        )
        if payload.get("operation", "commit") == "revoke":
            revoked = self.repository.revoke_configuration(
                payload["subject_key"], payload["version"], approval["principal_id"],
                invocation.principal.scope.client_id, invocation.principal.scope.project_code)
            row = {"subject_key": payload["subject_key"], "version": payload["version"],
                   "revoked": revoked, "status": "revoked" if revoked else "not_found"}
            return {"status": "REVOKED", "configuration": row,
                    "authoritative_evidence": {"subject_key": payload["subject_key"],
                                               "version": payload["version"], "revoked": revoked}}
        row = self.repository.commit_configuration(
            payload["subject_key"], payload["version"],
            payload.get("expected_effective_version"), approval["principal_id"],
            approval["authority_basis"], invocation.principal.scope.client_id,
            invocation.principal.scope.project_code,
        )
        return {"status": "COMMITTED", "configuration": row,
                "authoritative_evidence": {"subject_key": payload["subject_key"],
                                           "version": payload["version"]}}

    def _execute_objective(self, invocation, payload, shadow=False):
        scope = invocation.principal.scope
        if payload["operation"] == "create":
            if shadow:
                return {"status": "OBJECTIVE_CREATE", "objective": dict(payload)}
            row = self.repository.create_objective(
                payload["objective_key"], invocation.principal.principal_id, scope.client_id,
                payload["desired_outcome"], payload["success_criteria"], scope.project_code,
                payload.get("dependencies", ()), payload.get("evidence_refs", ()),
                payload.get("risk_class", "R1"),
            )
        elif payload["operation"] == "transition":
            if shadow:
                return {"status": "OBJECTIVE_TRANSITION", "objective": dict(payload)}
            row = self.repository.transition_objective(
                payload["objective_key"], payload["expected_version"], payload["status"],
                invocation.principal.principal_id, scope.client_id, scope.project_code,
            )
        else:
            raise ContractError("unsupported objective operation")
        return {"status": "OBJECTIVE_%s" % payload["operation"].upper(), "objective": row}

    def _execute_learning(self, invocation, payload, shadow=False):
        scope = invocation.principal.scope
        if shadow:
            return {"status": "PERSISTED", "learning": dict(payload)}
        if payload.get("operation") == "promote":
            source = self.repository.learning_by_id(payload["source_learning_id"])
            if (source is None or source["learning_scope"] != "client" or
                    source["client_id"] != scope.client_id or
                    source["project_code"] != scope.project_code):
                raise SecurityError("promotion source scope denied")
            provenance = dict(source.get("provenance") or {})
            provenance["origin_scope"] = {
                "client_id": source["client_id"], "project_code": source["project_code"],
                "learning_id": source["id"]}
            provenance["target_scope"] = {
                "learning_scope": payload["learning_scope"],
                "industry_key": payload.get("industry_key")}
            row = self.repository.persist_learning(
                source["learning_key"], payload["learning_scope"], source["observation_refs"],
                source["finding"], provenance, source["retention_basis"],
                client_id=None, project_code=None,
                industry_key=payload.get("industry_key"), outcome=source.get("outcome"))
            return {"status": "PERSISTED", "learning": row}
        row = self.repository.persist_learning(
            payload["learning_key"], payload["learning_scope"], payload["observation_refs"],
            payload["finding"], payload["provenance"], payload["retention_basis"],
            scope.client_id, scope.project_code, payload.get("user_id"),
            payload.get("industry_key") or scope.industry_key, payload.get("outcome"),
            payload.get("supersedes_id"),
        )
        return {"status": "PERSISTED", "learning": row}

    def _execute_intelligence(self, invocation, payload, shadow=False):
        scope = invocation.principal.scope
        if shadow:
            return {"status": "PERSISTED", "intelligence": dict(payload)}
        row = self.repository.persist_intelligence(
            payload["intelligence_key"], payload["intelligence_type"], payload["source_refs"],
            payload["content"], payload["provenance"], payload["retention_basis"],
            scope.client_id, scope.project_code, payload.get("industry_key") or scope.industry_key,
            payload.get("confidentiality", "restricted"), payload.get("confidence"),
            payload.get("validation"), payload.get("novelty_status", "novel"),
            payload.get("supersedes_id"),
        )
        return {"status": "PERSISTED", "intelligence": row}

    def _execute_distribution_decision(self, invocation, payload, persist=True, shadow=False):
        records = self.repository.intelligence(
            payload["intelligence_key"], invocation.principal.scope.client_id,
            invocation.principal.scope.project_code,
        )
        if not records:
            raise SecurityError("scoped intelligence unavailable")
        decision = self.distribution_decision(
            records[0], payload["target_principal_id"], payload["decision_kind"],
            payload.get("channel"), payload.get("output_type"))
        if persist and not shadow:
            self.repository.record_distribution(
                invocation.idempotency_key, records[0]["id"], payload["decision_kind"],
                payload["target_principal_id"], decision["decision"],
                decision.get("authority_basis") or decision.get("reason") or "DENIED",
                invocation.principal.scope.client_id, invocation.principal.scope.project_code,
                payload["target_principal_id"], payload.get("channel"))
        return {"status": "DECIDED", "distribution_decision": decision}

    def _execute_entitlement(self, invocation, payload):
        row = self.repository.entitlement(
            invocation.principal.scope.client_id, payload["dimension"], payload["subject"],
            payload.get("principal_id") or invocation.principal.principal_id,
        )
        return {"status": "EVALUATED", "entitled": bool(row and row["value"].get("enabled")),
                "entitlement": row}

    def _execute_entitlement_configure(self, invocation, payload, shadow=False):
        approval = self.repository.approval(
            invocation.approval_key, invocation.principal.scope.client_id,
            invocation.principal.scope.project_code,
        )
        if shadow:
            return {"status": "ASSIGNED" if payload["operation"] == "assign" else "REVOKED",
                    "entitlement": dict(payload),
                    "authoritative_evidence": {"kind": "entitlement-proposal"}}
        if payload["operation"] == "assign":
            row = self.repository.assign_entitlement(
                payload["entitlement_key"], invocation.principal.scope.client_id,
                payload["dimension"], payload["subject"], payload["value"],
                payload["version"], approval["authority_basis"], payload.get("principal_id"),
                granting_actor=approval["principal_id"],
            )
            status = "ASSIGNED"
        elif payload["operation"] == "revoke":
            row = {"revoked": self.repository.revoke_entitlement(
                payload["entitlement_key"], invocation.principal.scope.client_id)}
            status = "REVOKED"
        else:
            raise ContractError("unsupported entitlement operation")
        return {"status": status, "entitlement": row,
                "authoritative_evidence": {"entitlement_key": payload["entitlement_key"]}}

    def _execute_autonomy(self, invocation, payload):
        state = self.effective_state(invocation.principal, payload["target_capability_id"])
        grant = state.get("grant") or {}
        level = min(int(grant.get("max_autonomy", 0)),
                    int((state.get("principal_authority") or {}).get("max_autonomy", 0)))
        decision = "ALLOW" if state.get("decision") == "ALLOW" and payload["requested_level"] <= level else "DENY"
        return {"status": "EVALUATED", "assigned_level": level,
                "decision": decision, "limits": grant.get("autonomy_limits") or {}}

    def _execute_graph(self, invocation, payload):
        return {"status": "GRAPH_EVALUATED", "fan_in": self.fan_in(
            payload["objective_key"], invocation.principal, payload["required_work_keys"])}

    def _execute_verification_status(self, invocation, payload):
        return {"status": "VERIFICATION_REQUEST_REGISTERED", "execution_id": payload["execution_id"]}

    def _execute_noagent(self, invocation, payload):
        return {"status": "CONTINUE_DETERMINISTIC"}

    def _execute_help(self, invocation, payload):
        return {"status": "DISCOVERED", "capabilities": [
            item["capability_id"] for item in self.discover_functions(invocation.principal)]}

    def _execute_security_observe(self, invocation, payload):
        row = self.repository.security_event(
            payload["event_key"], payload["event_type"], payload["severity"],
            payload["security_domain"], payload["evidence"],
            payload.get("implicated_principal"), invocation.principal.scope.client_id,
            invocation.principal.scope.project_code, payload.get("containment"),
        )
        return {"status": "OBSERVED", "security_event": row}

    def _execute_security_contain(self, invocation, payload, shadow=False):
        if shadow:
            return {"status": "CONTAINED", "security_event": {},
                    "authoritative_evidence": {"kind": "containment-proposal"}}
        approval = self.repository.approval(
            invocation.approval_key, invocation.principal.scope.client_id,
            invocation.principal.scope.project_code,
        )
        row = self.repository.contain_security_event(
            payload["event_key"], approval["principal_id"], payload["actions"],
            invocation.principal.scope.client_id, invocation.principal.scope.project_code,
        )
        return {"status": "CONTAINED", "security_event": row,
                "authoritative_evidence": {"security_event_id": row["id"]}}

    def _handle_prior(self, invocation, prior, request_hash):
        if prior["request_hash"] != request_hash:
            return self._result(invocation, "CONFLICT", "IDEMPOTENCY_KEY_CONFLICT", prior["id"])
        if prior["status"] == "completed":
            return self._result(invocation, "COMPLETED", "DUPLICATE_REPLAY", prior["id"], prior["outcome"])
        if prior["status"] == "outcome_uncertain":
            inspector = self._outcome_inspectors.get(invocation.capability_id)
            if inspector is None:
                return self._result(invocation, "ESCALATE", "OUTCOME_UNCERTAIN", prior["id"])
            inspected = inspector(invocation)
            state = inspected.get("state")
            if state == "completed" and isinstance(inspected.get("authoritative_evidence"), Mapping):
                resolved = self.repository.resolve_uncertain_execution(
                    prior["id"], inspected.get("outcome") or {}, inspected["authoritative_evidence"]
                )
                return self._result(invocation, "COMPLETED", "PRIOR_COMPLETION_ESTABLISHED", prior["id"], resolved["outcome"])
            if state == "not_completed":
                self.repository.abandon_uncertain_execution(prior["id"], "PRIOR_NON_COMPLETION_ESTABLISHED")
                return self._result(invocation, "STOP", "RETRY_REQUIRES_NEW_GOVERNED_INTENT", prior["id"])
            return self._result(invocation, "ESCALATE", "OUTCOME_CANNOT_BE_ESTABLISHED", prior["id"])
        return self._result(invocation, "PENDING", "COMPETING_CLAIM", prior["id"])

    def _deny(self, invocation, code, detail, effective, audit=True):
        audit_id = None
        if audit:
            audit_id = self.repository.audit(
                "capability_invocation", invocation.principal.principal_id,
                "DENIED", {"code": code, "detail": detail,
                           "effective": effective.get("explanation", {})},
                invocation.principal.scope.client_id,
                invocation.principal.scope.project_code,
                invocation.capability_id,
            )
        return InvocationResult(status="DENIED", code=code, audit_id=audit_id)

    def _result(self, invocation, status, code, execution_id, outcome=None):
        return InvocationResult(status=status, code=code, execution_id=execution_id, outcome=outcome or {})

    def discover_functions(self, principal: Principal):
        return [item for item in self.repository.capabilities()
                if self.effective_state(principal, item["capability_id"])["decision"] == "ALLOW"]

    def compose(self, principal: Principal, components: Iterable[Mapping[str, Any]]):
        return derive_composed_scope(principal, components)

    def fan_in(self, objective_key: str, principal: Principal, required_work_keys: Iterable[str]):
        work = {item["work_key"]: item for item in self.repository.work_units(
            objective_key, principal.scope.client_id, principal.scope.project_code
        )}
        required = list(required_work_keys)
        missing = [key for key in required if key not in work]
        failed = [key for key in required if key in work and work[key]["status"] != "completed"]
        wrong_scope = [key for key in required if key in work and (
            work[key]["client_id"] != principal.scope.client_id or
            (principal.scope.project_code is not None and work[key]["project_code"] != principal.scope.project_code)
        )]
        if missing or failed or wrong_scope:
            return {"status": "INCOMPLETE", "missing": missing, "failed": failed,
                    "scope_denied": wrong_scope, "artifacts": []}
        return {"status": "COMPLETED", "artifacts": [work[key]["artifact"] for key in required],
                "work_keys": required}

    def distribution_decision(
        self, intelligence: Mapping[str, Any], target_principal_id: str,
        decision_kind: str, channel: Optional[str] = None,
        output_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        target_authority = self.repository.actor_authority(
            target_principal_id, intelligence.get("client_id"),
            intelligence.get("project_code"), exact_scope=True)
        if target_authority is None:
            return {"decision": "DENY", "reason": "RECIPIENT_IDENTITY_UNESTABLISHED"}
        target_principal = Principal(target_principal_id,
                                     target_authority["principal_class"],
                                     self._scope_for_intelligence(intelligence))
        if intelligence.get("client_id") not in (None, target_principal.scope.client_id):
            return {"decision": "DENY", "reason": "CLIENT_SCOPE_DENIED"}
        if intelligence.get("project_code") not in (None, target_principal.scope.project_code):
            return {"decision": "DENY", "reason": "PROJECT_SCOPE_DENIED"}
        if intelligence.get("withdrawn_at") is not None:
            return {"decision": "DENY", "reason": "INTELLIGENCE_WITHDRAWN"}
        permissions = set(target_authority.get("permissions", []))
        permission = {"awareness": "information.awareness", "internal_use": "information.use",
                      "supervisory": "information.supervise", "distribution": "information.receive",
                      "recipient": "information.receive"}.get(decision_kind)
        if permission is None or permission not in permissions:
            return {"decision": "DENY", "reason": "RECIPIENT_AUTHORITY_DENIED"}
        domains = set(target_authority.get("domains", []))
        allowed_confidentiality = set(target_authority.get("confidentiality", []) or
                                      target_authority.get("allowed_confidentiality", []))
        if "*" not in domains and intelligence.get("security_domain") not in domains:
            return {"decision": "HOLD/QUARANTINE", "reason": "SECURITY_DOMAIN_UNAUTHORIZED"}
        if intelligence.get("confidentiality") not in allowed_confidentiality:
            return {"decision": "DENY", "reason": "CONFIDENTIALITY_MISMATCH"}
        if intelligence.get("confidentiality") in (None, "") or not intelligence.get("provenance"):
            return {"decision": "HOLD/QUARANTINE", "reason": "CLASSIFICATION_PROVENANCE_UNRESOLVED"}
        if intelligence.get("novelty_status") == "novel" and decision_kind in ("distribution", "recipient"):
            return {"decision": "HOLD/QUARANTINE", "reason": "NOVEL_INTELLIGENCE"}
        if decision_kind == "awareness":
            return {"decision": "ALLOW-AWARENESS", "authority_basis": target_authority["authority_basis"]}
        if decision_kind == "internal_use":
            return {"decision": "ALLOW-INTERNAL-USE", "authority_basis": target_authority["authority_basis"]}
        if decision_kind == "supervisory":
            return {"decision": "SUPERVISORY-REVIEW-ELIGIBLE",
                    "authority_basis": target_authority["authority_basis"]}
        scope = target_principal.scope
        config = self.repository.effective_configuration(
            ["platform", "client:%s" % scope.client_id,
             "project:%s:%s" % (scope.client_id, scope.project_code)],
            scope.client_id, scope.project_code)
        policy = config["effective"].get("distribution") or {}
        allowed = (policy.get("enabled") is True and target_principal_id in set(policy.get("recipients", [])) and
                   channel in set(policy.get("channels", [])) and
                   intelligence.get("confidentiality") in set(policy.get("confidentiality", [])) and
                   intelligence.get("intelligence_type") in set(policy.get("intelligence_types", [])))
        if not allowed:
            return {"decision": "DENY", "reason": "DISTRIBUTION_CONFIGURATION_DENIED"}
        if policy.get("entitlement_required"):
            subject = policy.get("entitlement_subject")
            entitlement = self.repository.entitlement(scope.client_id, "recipient", subject,
                                                      target_principal_id)
            if not entitlement or not entitlement["value"].get("enabled"):
                return {"decision": "DENY", "reason": "RECIPIENT_NOT_ENTITLED"}
        return {"decision": "ALLOW-DISTRIBUTION", "recipient": target_principal_id,
                "channel": channel, "output_type": output_type,
                "authority_basis": target_authority["authority_basis"],
                "configuration_provenance": config["provenance"]}

    @staticmethod
    def _scope_for_intelligence(intelligence):
        from .contracts import Scope
        return Scope(intelligence.get("client_id"), intelligence.get("project_code"),
                     intelligence.get("industry_key"))

    @staticmethod
    def _within_autonomy_limits(payload, limits):
        if not limits or not isinstance(limits, Mapping):
            return False
        if limits.get("allow_all") is True:
            return True
        fields = limits.get("fields")
        if not isinstance(fields, Mapping) or not fields:
            return False
        for name, rule in fields.items():
            if name not in payload or not isinstance(rule, Mapping):
                return False
            value = payload[name]
            if "allowed" in rule and value not in rule["allowed"]:
                return False
            if "equals" in rule and value != rule["equals"]:
                return False
            if "max" in rule and (not isinstance(value, (int, float)) or value > rule["max"]):
                return False
            if "min" in rule and (not isinstance(value, (int, float)) or value < rule["min"]):
                return False
        return True

    def _validate_input_semantics(self, invocation):
        payload = invocation.payload
        for key, value in payload.items():
            if isinstance(value, str) and not value.strip():
                raise ContractError("field %s must be non-empty" % key)
        capability_id = invocation.capability_id
        if capability_id == "distribution.decide" and payload["decision_kind"] not in (
                "awareness", "internal_use", "supervisory", "distribution", "recipient"):
            raise ContractError("unknown distribution decision kind")
        if capability_id == "learning.persist":
            if payload["learning_scope"] not in LEARNING_SCOPES:
                raise ContractError("unknown learning scope")
            requested = payload["learning_scope"]
            principal_scope = invocation.principal.scope
            if requested in ("platform", "industry") and principal_scope.client_id is not None:
                if payload.get("operation") != "promote":
                    raise ContractError("broader learning scope requires explicit governed promotion")
                if "source_learning_id" not in payload:
                    raise ContractError("promotion source is required")
                if requested == "industry" and not payload.get("industry_key"):
                    raise ContractError("promotion industry target is required")
                if (requested == "industry" and
                        payload.get("industry_key") != principal_scope.industry_key):
                    raise ContractError("promotion target industry outside principal scope")
            if payload["learning_scope"] == "individual" and not payload.get("user_id"):
                raise ContractError("individual learning requires user_id")
        if capability_id == "takeon.propose" and payload["phase"] not in ("industry", "client", "evolution"):
            raise ContractError("unknown take-on phase")
        if capability_id == "objective.manage":
            if payload["operation"] == "create" and not all(k in payload for k in ("desired_outcome", "success_criteria")):
                raise ContractError("objective create fields required")
            if payload["operation"] == "transition" and not all(k in payload for k in ("expected_version", "status")):
                raise ContractError("objective transition fields required")
            if payload["operation"] not in ("create", "transition"):
                raise ContractError("unknown objective operation")
        if capability_id == "configuration.commit" and payload.get("operation", "commit") not in ("commit", "revoke"):
            raise ContractError("unknown configuration operation")
        if capability_id == "entitlement.configure":
            operation = payload["operation"]
            if operation not in ("assign", "revoke"):
                raise ContractError("unknown entitlement operation")
            if operation == "assign":
                required = ("dimension", "subject", "value", "version")
                if any(field not in payload for field in required):
                    raise ContractError("entitlement assignment fields required")
            elif set(payload).difference({"operation", "entitlement_key", "principal_id"}):
                raise ContractError("revoke accepts no assignment fields")
        required_lists = {
            "capacity.assess": ("metric_samples", "evidence_refs"),
            "critical_path.analyze": ("operational_refs",),
            "performance.analyze": ("evidence_refs",),
            "provider.reason": ("evidence_refs",),
            "market.analyze": ("source_refs",),
        }
        for field in required_lists.get(capability_id, ()):
            if not payload.get(field):
                raise ContractError("field %s must be non-empty" % field)

    def _validate_output_semantics(self, invocation, outcome):
        capability_id = invocation.capability_id
        if capability_id == "market.analyze" and outcome["authorizes_action"] is not False:
            raise ContractError("market intelligence cannot authorize action")
        if capability_id == "takeon.propose" and outcome["authority_status"] not in (
                "PROPOSAL_ONLY", "AUTHORITY_VALUE_REQUIRED"):
            raise ContractError("take-on output cannot establish authority")
        if capability_id == "consequence.analyze":
            candidate = outcome.get("intervention", {}).get("capability_id")
            if candidate and candidate not in self._definitions:
                raise ContractError("intervention capability is unregistered")

    def no_agent_status(self, capability_id: str):
        capability = self.repository.capability(capability_id)
        if not capability:
            return {"status": "STOP", "reason": "UNREGISTERED_CAPABILITY"}
        behavior = capability["contract"]["no_agent_behavior"]
        if "continue deterministically" in behavior:
            return {"status": "CONTINUE_DETERMINISTIC", "behavior": behavior}
        return {"status": "PENDING_OR_STOP", "behavior": behavior}

    def validate_restore_manifest(self, principal: Principal, manifest: Mapping[str, Any]):
        """Validate a backup/restore boundary without resurrecting authority.

        Backup technology and RPO/RTO remain authority-owned.  This gate makes
        the required security semantics deterministic for any later adapter.
        """
        if manifest.get("client_id") != principal.scope.client_id:
            return {"decision": "DENY", "reason": "RESTORE_CLIENT_SCOPE"}
        if manifest.get("project_code") not in (None, principal.scope.project_code):
            return {"decision": "DENY", "reason": "RESTORE_PROJECT_SCOPE"}
        prohibited = {"authority_grants", "authority_values", "provider_policies",
                      "capability_enablement", "approvals"}
        included = set(manifest.get("record_classes") or [])
        if included.intersection(prohibited):
            return {"decision": "DENY", "reason": "CURRENT_AUTHORITY_REVALIDATION_REQUIRED",
                    "excluded": sorted(included.intersection(prohibited))}
        return {"decision": "ALLOW_DATA_RESTORE", "revalidate_current_authority": True,
                "reactivate_revoked": False, "record_classes": sorted(included)}

    def takeon_proposal(
        self, principal: Principal, phase: str, proposed_state: Mapping[str, Any],
        idempotency_key: str, reason: str,
    ) -> Dict[str, Any]:
        if phase not in ("industry", "client", "evolution"):
            return {"status": "REJECTED", "reason": "INVALID_TAKEON_PHASE"}
        effective = self.effective_state(principal, "takeon.propose")
        if effective["decision"] != "ALLOW":
            return {"status": "REJECTED", "reason": "TAKEON_PROPOSAL_UNAUTHORIZED"}
        subject = "takeon:%s:%s:%s" % (
            phase, principal.scope.client_id,
            principal.scope.industry_key or principal.scope.project_code or "scope",
        )
        proposal = self.repository.propose_configuration(
            subject, dict(proposed_state), principal.principal_id,
            idempotency_key, principal.scope.client_id,
            principal.scope.project_code, reason,
        )
        return {"status": "VALIDATED-PROPOSAL", "proposal": proposal,
                "authoritative": False}

    def takeon_commit_eligibility(self, principal: Principal) -> Dict[str, Any]:
        scope_keys = ["client:%s" % principal.scope.client_id, "platform"]
        instrument = self.repository.authority_value("AB-AUTH-017", scope_keys)
        if instrument is None:
            return {"status": "AUTHORITY VALUE REQUIRED", "family": "AB-AUTH-017",
                    "enabled": False}
        return {"status": "ELIGIBLE_FOR_GOVERNED_CONFIGURATION_COMMIT",
                "authority": instrument, "enabled": True}

    def autonomy_promotion_eligibility(
        self, principal: Principal, target_principal_id: str,
        capability_id: str, proposed_level: int,
    ) -> Dict[str, Any]:
        if target_principal_id == principal.principal_id:
            return {"status": "DENIED", "reason": "SELF_PROMOTION_DENIED"}
        threshold = self.repository.authority_value(
            "AB-AUTH-003", ["client:%s" % principal.scope.client_id, "platform"]
        )
        if threshold is None:
            return {"status": "AUTHORITY VALUE REQUIRED", "family": "AB-AUTH-003"}
        if proposed_level < 1 or proposed_level > 5:
            return {"status": "DENIED", "reason": "INVALID_AUTONOMY_LEVEL"}
        return {"status": "REQUIRES_GOVERNED_CONFIGURATION_COMMIT",
                "threshold_authority": threshold, "capability_id": capability_id,
                "proposed_level": proposed_level}

    def learning_promotion_eligibility(
        self, principal: Principal, source_scope: str, target_scope: str,
    ) -> Dict[str, Any]:
        if source_scope == "client" and target_scope in ("industry", "platform"):
            generalization = self.repository.authority_value("SDIP-AV-004", ["platform"])
            if generalization is None:
                return {"status": "DENIED", "reason": "CROSS_CLIENT_GENERALIZATION_DISABLED"}
        policy = self.repository.authority_value(
            "AB-AUTH-008", ["client:%s" % principal.scope.client_id, "platform"]
        )
        if policy is None:
            return {"status": "AUTHORITY VALUE REQUIRED", "family": "AB-AUTH-008"}
        return {"status": "ELIGIBLE_FOR_GOVERNED_PROMOTION", "policy": policy}

    def promote_learning(self, principal: Principal, source: Mapping[str, Any],
                         target_scope: str, industry_key: Optional[str] = None,
                         idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        """Submit promotion as a normal governed, claimed invocation."""
        if not isinstance(source, Mapping) or source.get("learning_scope") != "client":
            raise ContractError("client-scoped source learning required")
        if target_scope not in ("industry", "platform"):
            raise ContractError("unsupported learning promotion target")
        if target_scope == "industry" and not industry_key:
            raise SecurityError("target industry is required")
        eligibility = self.learning_promotion_eligibility(principal, "client", target_scope)
        if eligibility.get("status") != "ELIGIBLE_FOR_GOVERNED_PROMOTION":
            raise AuthorityError("learning promotion authority unresolved")
        key = idempotency_key or "learning-promotion:%s:%s:%s" % (
            source.get("id"), target_scope, industry_key)
        result = self.governed_invoke(Invocation(
            "learning.persist", principal, key, {
                "operation": "promote", "learning_key": source["learning_key"],
                "learning_scope": target_scope, "observation_refs": source.get("observation_refs", []),
                "finding": source.get("finding", {}), "provenance": source.get("provenance", {}),
                "retention_basis": source.get("retention_basis"),
                "industry_key": industry_key, "source_learning_id": source["id"]}))
        if result.status != "COMPLETED":
            raise AuthorityError(result.code)
        return result.outcome["learning"]

    def validate_external_intelligence(self, evidence: Mapping[str, Any]):
        required = ("source", "observed_at", "retrieved_at", "content_hash")
        missing = [field for field in required if not evidence.get(field)]
        if missing:
            return {"status": "REJECTED", "reason": "ATTRIBUTION_OR_FRESHNESS_MISSING",
                    "missing": missing}
        return {"status": "VALIDATED_EXTERNAL_EVIDENCE", "authorizes_action": False,
                "evidence": dict(evidence)}
