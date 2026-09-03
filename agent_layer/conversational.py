"""Bounded conversational selection over the existing Agent Layer."""

import hashlib
from typing import Any, Mapping, Optional

from .contracts import (Invocation, Principal, ProtectedItem, ProviderError,
                        ProviderRequest, Scope, validate_structured)
from .security import contains_secret, safe_evidence


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
        if not self.provider_id:
            raise ProviderError("conversational provider is not configured")
        if contains_secret(request_text) or contains_secret(membership):
            raise ProviderError("secret-bearing conversational input denied")
        if (hasattr(self.runtime, "repository") and
                self.runtime.repository.provider_policy(self.provider_id) is None):
            raise ProviderError("provider is not approved")
        request = ProviderRequest(
            operation="conversational.select",
            context={
                "request": request_text,
                "principal": {"principal_id": principal.principal_id,
                              "principal_class": principal.principal_class},
                "scope": {"client_id": principal.scope.client_id,
                          "project_code": principal.scope.project_code,
                          "industry_key": principal.scope.industry_key},
                "membership": safe_evidence(dict(membership)),
                "provider_event_id": event_id,
                "eligible_capabilities": [
                    {**item, "input_schema": {
                        key: getattr(value, "__name__", str(value))
                        for key, value in item["input_schema"].items()
                    }} for item in eligible
                ],
            },
            output_contract=SELECTION_OUTPUT,
        )
        result = self.runtime.providers.invoke(request, (self.provider_id,))
        return result.output

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
        return {"status": "invoked", "result": result, "invocation": invocation}
