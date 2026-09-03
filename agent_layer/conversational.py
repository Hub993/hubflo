"""Bounded conversational selection over the existing Agent Layer."""

import hashlib
from typing import Any, Mapping, Optional

from .contracts import Invocation, Principal, ProtectedItem, validate_structured


SELECTION_OUTPUT = {
    "selection": str,
    "capability_id": (str, type(None)),
    "arguments": dict,
    "evidence_refs": list,
}


class ConversationalOrchestrator:
    """Reason only over the deterministic, current eligible universe."""

    def __init__(self, runtime, provider_id: Optional[str] = None):
        self.runtime = runtime
        self.provider_id = provider_id

    def _proposal(self, principal: Principal, request_text: str, event_id: str,
                  membership: Mapping[str, Any], eligible: list) -> Mapping[str, Any]:
        return self.runtime.conversational_proposal(
            self.provider_id, principal, request_text, event_id, membership, eligible,
            SELECTION_OUTPUT,
        )

    def handle(self, request_text: str, principal: Principal,
               membership: Mapping[str, Any], event_id: str):
        eligible = self.runtime.eligible_conversational_capabilities(principal)
        by_id = {item["capability_id"]: item for item in eligible}
        proposal = self._proposal(principal, request_text, event_id, membership, eligible)
        selection = str(proposal["selection"]).strip().lower()
        capability_id = proposal.get("capability_id")
        arguments = proposal["arguments"]
        if selection not in ("selected", "none", "clarification"):
            return {"status": "degraded", "code": "INVALID_SELECTION"}
        if selection != "selected" or not capability_id:
            return {"status": "clarification", "code": "NO_CAPABILITY_SELECTED"}
        item = by_id.get(capability_id)
        if item is None:
            return {"status": "denied", "code": "CAPABILITY_OUTSIDE_ELIGIBLE_UNIVERSE"}
        definition = self.runtime.capability_definition(capability_id)
        try:
            validate_structured(arguments, definition.input_schema,
                                definition.optional_input_fields,
                                optional_schema=definition.optional_input_schema)
        except Exception:
            return {"status": "denied", "code": "INVALID_CAPABILITY_ARGUMENTS"}
        evidence_refs = tuple(str(ref) for ref in proposal["evidence_refs"])
        invocation = Invocation(
            capability_id=capability_id,
            principal=principal,
            idempotency_key="conversational:" + hashlib.sha256(
                (str(event_id) + ":" + str(membership.get("id"))).encode()
            ).hexdigest(),
            payload=arguments,
            requested_autonomy=1,
            provider_id=self.provider_id if definition.requires_provider else None,
            payload_item=(ProtectedItem(
                reference="conversational-payload:" + str(event_id),
                value=dict(arguments), security_domain="SD3",
                client_id=principal.scope.client_id,
                project_code=principal.scope.project_code,
                classification="conversational-request",
                permitted_uses=("reason",), provider_eligible=True,
                retention_max_seconds=0,
                provenance={"source_ref": "whatsapp-event:" + str(event_id)},
            ) if definition.requires_provider else None),
            evidence_refs=evidence_refs,
        )
        result = self.runtime.governed_invoke(invocation)
        status = str(getattr(result, "status", "FAILED")).upper()
        if status == "COMPLETED":
            outcome_status = "invoked"
        elif status in ("DENIED", "STOP"):
            outcome_status = "denied"
        elif status in ("PENDING", "ESCALATE"):
            outcome_status = "pending"
        else:
            outcome_status = "degraded"
        return {"status": outcome_status, "code": getattr(result, "code", status),
                "governed_status": status, "result": result, "invocation": invocation}
