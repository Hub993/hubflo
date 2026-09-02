"""Stable structured contracts for Agent Layer boundaries."""

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol


AUTHORITY_FAMILIES = tuple("AB-AUTH-%03d" % value for value in range(1, 20))
SECURITY_VALUE_FAMILIES = tuple("SDIP-AV-%03d" % value for value in range(1, 11))
SECURITY_DOMAINS = ("SD1", "SD2", "SD3", "SD4")
LEARNING_SCOPES = ("platform", "industry", "client", "individual")
SIDE_EFFECT_CLASSES = ("S0", "S1", "S2", "S3", "S4")
RISK_CLASSES = ("R0", "R1", "R2", "R3", "R4")
VERIFICATION_RESULTS = ("PASS", "FAIL", "INCONCLUSIVE")


@dataclass(frozen=True)
class Scope:
    client_id: Optional[int] = None
    project_code: Optional[str] = None
    industry_key: Optional[str] = None


@dataclass(frozen=True)
class Principal:
    principal_id: str
    principal_class: str
    scope: Scope


@dataclass(frozen=True)
class ProtectedItem:
    reference: str
    value: Any
    security_domain: str
    client_id: Optional[int] = None
    project_code: Optional[str] = None
    classification: str = "unclassified"
    confidentiality: str = "restricted"
    permitted_uses: tuple = ()
    provider_eligible: bool = False
    learning_eligible: bool = False
    distribution_eligible: bool = False
    authoritative: bool = False
    retention_max_seconds: Optional[int] = None
    region: Optional[str] = None
    access_requirements: tuple = ()
    deletion_required: bool = False
    withdrawal_required: bool = False
    distribution_uses: tuple = ()
    provenance: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Invocation:
    capability_id: str
    principal: Principal
    idempotency_key: str
    payload: Mapping[str, Any]
    requested_autonomy: int = 2
    work_key: Optional[str] = None
    approval_key: Optional[str] = None
    provider_id: Optional[str] = None
    fallback_provider_ids: tuple = ()
    payload_item: Optional[ProtectedItem] = None
    protected_context: tuple = ()
    evidence_refs: tuple = ()
    shadow_mode: bool = False


@dataclass(frozen=True)
class InvocationResult:
    status: str
    code: str
    execution_id: Optional[int] = None
    outcome: Mapping[str, Any] = field(default_factory=dict)
    audit_id: Optional[int] = None


@dataclass(frozen=True)
class CapabilityDefinition:
    capability_id: str
    version: str
    family: str
    purpose: str
    side_effect_class: str
    risk_class: str
    eligible_principal_classes: tuple
    input_schema: Mapping[str, type]
    optional_input_fields: tuple
    output_schema: Mapping[str, type]
    optional_output_fields: tuple
    input_semantics: tuple
    output_semantics: tuple
    information_contract: Mapping[str, Any]
    required_permission: str
    required_configuration: tuple
    execution_path: str
    preconditions: tuple
    concurrency: str
    claim_required: bool
    idempotency: str
    approval_contract: str
    audit_contract: tuple
    failure_contract: Mapping[str, str]
    uncertain_outcome: str
    no_agent_behavior: str
    verification_contract: str
    regression_dependencies: tuple
    required_action: str = "invoke"
    requires_entitlement: bool = False
    requires_provider: bool = False
    requires_approval: bool = False
    requires_verification: bool = False
    enabled: bool = False
    optional_input_schema: Mapping[str, type] = field(default_factory=dict)
    optional_output_schema: Mapping[str, type] = field(default_factory=dict)
    shadow_applicability: str = "supported"


@dataclass(frozen=True)
class ProviderRequest:
    operation: str
    context: Mapping[str, Any]
    output_contract: Mapping[str, type]
    optional_output_fields: tuple = ()
    optional_output_schema: Mapping[str, type] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderResult:
    provider_id: str
    provider_version: str
    output: Mapping[str, Any]


class ReasoningProvider(Protocol):
    provider_id: str
    provider_version: str

    def invoke(self, request: ProviderRequest) -> ProviderResult:
        ...


class AuthoritativeHandler(Protocol):
    def __call__(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        ...


class OutcomeInspector(Protocol):
    def __call__(self, invocation: Invocation) -> Mapping[str, Any]:
        """Return state=completed|not_completed|unknown and evidence/outcome."""
        ...


class VerificationHandler(Protocol):
    def __call__(self, invocation: Invocation) -> Mapping[str, Any]:
        """Return result=PASS|FAIL|INCONCLUSIVE plus evidence."""
        ...


class AgentLayerError(Exception):
    code = "AGENT_LAYER_ERROR"


class ContractError(AgentLayerError):
    code = "INVALID_CONTRACT"


class SecurityError(AgentLayerError):
    code = "SECURITY_POLICY_DENIED"


class AuthorityError(AgentLayerError):
    code = "AUTHORITY_DENIED"


class ConflictError(AgentLayerError):
    code = "CONFLICT_REVALIDATION_REQUIRED"


class ProviderError(AgentLayerError):
    code = "PROVIDER_UNAVAILABLE"


def validate_structured(
    value: Mapping[str, Any], schema: Mapping[str, type],
    optional_fields=(), allow_additional=False, optional_schema=None,
) -> None:
    if not isinstance(value, Mapping):
        raise ContractError("structured object required")
    for name, expected in schema.items():
        if name not in value or not isinstance(value[name], expected):
            raise ContractError("field %s must be %s" % (name, expected.__name__))
    optional_schema = optional_schema or {name: Any for name in optional_fields}
    for name, expected in optional_schema.items():
        if name in value and expected is not Any and not isinstance(value[name], expected):
            expected_name = (expected.__name__ if hasattr(expected, "__name__")
                             else "/".join(item.__name__ for item in expected))
            raise ContractError("field %s must be %s" % (name, expected_name))
    if not allow_additional:
        allowed = set(schema).union(optional_schema)
        extra = set(value).difference(allowed)
        if extra:
            raise ContractError("unexpected fields: %s" % sorted(extra))
