"""Durable HUBFLO-owned Agent Layer state.

All JSON columns contain structured state, never credentials or private
chain-of-thought.  Tenant/project filters are enforced by the repository and
runtime before rows are returned or provider context is assembled.
"""

import datetime as dt

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from storage import Base


def utcnow():
    return dt.datetime.utcnow()


class AgentAuthorityValue(Base):
    __tablename__ = "al_authority_values"
    __table_args__ = (
        UniqueConstraint("family", "scope_key", "version", name="uq_al_authority_value"),
    )

    id = Column(Integer, primary_key=True)
    family = Column(String(48), nullable=False, index=True)
    scope_key = Column(String(256), nullable=False, default="platform", index=True)
    version = Column(Integer, nullable=False)
    value_json = Column(Text, nullable=True)
    status = Column(String(24), nullable=False, default="effective", index=True)
    authority_instrument = Column(String(256), nullable=False)
    proof_ref = Column(String(256), nullable=False)
    approved_by = Column(String(128), nullable=False)
    effective_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentPrincipalAuthority(Base):
    """Deterministic projection of an effective AB-AUTH-001 instrument."""

    __tablename__ = "al_principal_authorities"
    __table_args__ = (
        UniqueConstraint("scope_key", "principal_id", "version", name="uq_al_principal_authority"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    principal_id = Column(String(128), nullable=False, index=True)
    principal_class = Column(String(48), nullable=False)
    version = Column(Integer, nullable=False)
    authority_json = Column(Text, nullable=False)
    authority_basis = Column(String(256), nullable=False)
    independence_group = Column(String(128), nullable=False)
    effective_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentConfiguration(Base):
    __tablename__ = "al_configurations"
    __table_args__ = (
        UniqueConstraint("scope_key", "subject_key", "version", name="uq_al_configuration_version"),
        UniqueConstraint("scope_key", "idempotency_key", name="uq_al_configuration_idempotency"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    subject_key = Column(String(256), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    state_json = Column(Text, nullable=False)
    status = Column(String(24), nullable=False, index=True)
    proposer = Column(String(128), nullable=False)
    approver = Column(String(128), nullable=True)
    authority_basis = Column(String(256), nullable=True)
    reason = Column(Text, nullable=True)
    parent_version = Column(Integer, nullable=True)
    idempotency_key = Column(String(128), nullable=False)
    effective_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentCapability(Base):
    __tablename__ = "al_capabilities"

    capability_id = Column(String(128), primary_key=True)
    version = Column(String(32), nullable=False)
    family = Column(String(32), nullable=False, index=True)
    purpose = Column(Text, nullable=False)
    side_effect_class = Column(String(4), nullable=False)
    risk_class = Column(String(4), nullable=False)
    contract_json = Column(Text, nullable=False)
    enabled = Column(Boolean, nullable=False, default=False, index=True)
    healthy = Column(Boolean, nullable=False, default=True)
    requires_entitlement = Column(Boolean, nullable=False, default=False)
    requires_provider = Column(Boolean, nullable=False, default=False)
    requires_approval = Column(Boolean, nullable=False, default=False)
    requires_verification = Column(Boolean, nullable=False, default=False)
    kill_reason = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


class AgentAuthorityGrant(Base):
    __tablename__ = "al_authority_grants"
    __table_args__ = (
        UniqueConstraint("scope_key", "grant_key", name="uq_al_authority_grant_key"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    grant_key = Column(String(128), nullable=False)
    principal_id = Column(String(128), nullable=False, index=True)
    principal_class = Column(String(48), nullable=False)
    capability_id = Column(String(128), nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    max_autonomy = Column(Integer, nullable=False, default=2)
    information_permissions_json = Column(Text, nullable=False, default="[]")
    action_permissions_json = Column(Text, nullable=False, default="[]")
    allowed_domains_json = Column(Text, nullable=False, default="[]")
    autonomy_limits_json = Column(Text, nullable=False, default="{}")
    shadow_allowed = Column(Boolean, nullable=False, default=False)
    authority_basis = Column(String(256), nullable=False)
    delegator_id = Column(String(128), nullable=True)
    effective_at = Column(DateTime, default=utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentEntitlement(Base):
    __tablename__ = "al_entitlements"
    __table_args__ = (
        UniqueConstraint("scope_key", "entitlement_key", name="uq_al_entitlement_key"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    entitlement_key = Column(String(128), nullable=False)
    client_id = Column(Integer, nullable=False, index=True)
    principal_id = Column(String(128), nullable=True, index=True)
    dimension = Column(String(64), nullable=False, index=True)
    subject = Column(String(256), nullable=False)
    value_json = Column(Text, nullable=False)
    version = Column(Integer, nullable=False)
    authority_basis = Column(String(256), nullable=False)
    effective_at = Column(DateTime, default=utcnow, nullable=False)
    revoked_at = Column(DateTime, nullable=True, index=True)


class AgentProviderPolicy(Base):
    __tablename__ = "al_provider_policies"
    __table_args__ = (
        UniqueConstraint("provider_id", "version", name="uq_al_provider_policy"),
    )

    id = Column(Integer, primary_key=True)
    provider_id = Column(String(128), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    allowed_data_classes_json = Column(Text, nullable=False, default="[]")
    allowed_confidentiality_json = Column(Text, nullable=False, default="[]")
    permitted_uses_json = Column(Text, nullable=False, default="[]")
    training_permitted = Column(Boolean, nullable=False, default=False)
    retention_mode = Column(String(64), nullable=False)
    retention_max_seconds = Column(Integer, nullable=True)
    access_controls_json = Column(Text, nullable=False, default="[]")
    allowed_regions_json = Column(Text, nullable=False, default="[]")
    audit_required = Column(Boolean, nullable=False, default=True)
    attribution_required = Column(Boolean, nullable=False, default=True)
    deletion_supported = Column(Boolean, nullable=False, default=False)
    withdrawal_supported = Column(Boolean, nullable=False, default=False)
    allowed_distribution_uses_json = Column(Text, nullable=False, default="[]")
    terms_ref = Column(String(256), nullable=False)
    allowed = Column(Boolean, nullable=False, default=False, index=True)
    authority_basis = Column(String(256), nullable=False)
    effective_at = Column(DateTime, default=utcnow, nullable=False)
    revoked_at = Column(DateTime, nullable=True)


class AgentObjective(Base):
    __tablename__ = "al_objectives"
    __table_args__ = (
        UniqueConstraint("scope_key", "objective_key", name="uq_al_objective_key"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    objective_key = Column(String(128), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    owner_principal = Column(String(128), nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    desired_outcome = Column(Text, nullable=False)
    success_criteria_json = Column(Text, nullable=False)
    dependency_refs_json = Column(Text, nullable=False, default="[]")
    evidence_refs_json = Column(Text, nullable=False, default="[]")
    state_json = Column(Text, nullable=False, default="{}")
    status = Column(String(24), nullable=False, default="pending", index=True)
    risk_class = Column(String(4), nullable=False, default="R1")
    deadline = Column(DateTime, nullable=True)
    responsible_capability = Column(String(128), nullable=True)
    responsible_version = Column(String(32), nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)
    closed_at = Column(DateTime, nullable=True)


class AgentWorkUnit(Base):
    __tablename__ = "al_work_units"
    __table_args__ = (
        UniqueConstraint("scope_key", "work_key", name="uq_al_work_key"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    work_key = Column(String(128), nullable=False)
    objective_key = Column(String(128), nullable=False, index=True)
    parent_work_key = Column(String(128), nullable=True, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    capability_id = Column(String(128), nullable=False)
    principal_id = Column(String(128), nullable=False)
    dependencies_json = Column(Text, nullable=False, default="[]")
    input_json = Column(Text, nullable=False)
    artifact_json = Column(Text, nullable=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    failure_policy = Column(String(24), nullable=False, default="STOP")
    max_attempts = Column(Integer, nullable=False, default=1)
    attempts = Column(Integer, nullable=False, default=0)
    claim_token = Column(String(128), nullable=True, index=True)
    claimed_at = Column(DateTime, nullable=True)
    checkpoint_json = Column(Text, nullable=True)
    error_code = Column(String(128), nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)


class AgentExecution(Base):
    __tablename__ = "al_executions"
    __table_args__ = (
        UniqueConstraint("scope_key", "idempotency_key", name="uq_al_execution_idempotency"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    idempotency_key = Column(String(128), nullable=False)
    capability_id = Column(String(128), nullable=False, index=True)
    capability_version = Column(String(32), nullable=False)
    principal_id = Column(String(128), nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    work_key = Column(String(128), nullable=True, index=True)
    request_hash = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, index=True)
    claim_token = Column(String(128), nullable=True, index=True)
    claim_owner = Column(String(128), nullable=True)
    request_json = Column(Text, nullable=False)
    outcome_json = Column(Text, nullable=True)
    authoritative_evidence_json = Column(Text, nullable=True)
    approval_id = Column(Integer, nullable=True)
    verification_id = Column(Integer, nullable=True)
    provider_id = Column(String(128), nullable=True)
    provider_version = Column(String(64), nullable=True)
    error_code = Column(String(128), nullable=True)
    shadow_mode = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)


class AgentApproval(Base):
    __tablename__ = "al_approvals"
    __table_args__ = (
        UniqueConstraint("scope_key", "approval_key", name="uq_al_approval_key"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    approval_key = Column(String(128), nullable=False)
    principal_id = Column(String(128), nullable=False)
    capability_id = Column(String(128), nullable=False)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True)
    request_hash = Column(String(64), nullable=False)
    risk_class = Column(String(4), nullable=False)
    decision = Column(String(24), nullable=False)
    evidence_refs_json = Column(Text, nullable=False, default="[]")
    authority_basis = Column(String(256), nullable=False)
    expires_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentVerification(Base):
    __tablename__ = "al_verifications"

    id = Column(Integer, primary_key=True)
    execution_id = Column(Integer, nullable=True, index=True)
    verifier_principal = Column(String(128), nullable=False)
    verifier_authority_basis = Column(String(256), nullable=False)
    verifier_independence_group = Column(String(128), nullable=False)
    verifier_kind = Column(String(32), nullable=False)
    result = Column(String(16), nullable=False)
    evidence_json = Column(Text, nullable=False)
    independent = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentLearning(Base):
    __tablename__ = "al_learning"
    __table_args__ = (
        UniqueConstraint("scope_key", "learning_key", "version", name="uq_al_learning_version"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    learning_key = Column(String(128), nullable=False)
    version = Column(Integer, nullable=False)
    learning_scope = Column(String(24), nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True)
    user_id = Column(String(128), nullable=True, index=True)
    industry_key = Column(String(128), nullable=True, index=True)
    observation_refs_json = Column(Text, nullable=False)
    finding_json = Column(Text, nullable=False)
    outcome_json = Column(Text, nullable=True)
    provenance_json = Column(Text, nullable=False)
    status = Column(String(24), nullable=False, default="current")
    retention_basis = Column(String(256), nullable=False)
    withdrawn_at = Column(DateTime, nullable=True)
    supersedes_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentIntelligence(Base):
    __tablename__ = "al_intelligence"
    __table_args__ = (
        UniqueConstraint("scope_key", "intelligence_key", "version", name="uq_al_intelligence_version"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    intelligence_key = Column(String(128), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    intelligence_type = Column(String(64), nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    industry_key = Column(String(128), nullable=True)
    security_domain = Column(String(4), nullable=False, default="SD4")
    confidentiality = Column(String(32), nullable=False, default="restricted")
    source_refs_json = Column(Text, nullable=False)
    content_json = Column(Text, nullable=False)
    provenance_json = Column(Text, nullable=False)
    confidence_json = Column(Text, nullable=True)
    validation_json = Column(Text, nullable=True)
    lifecycle_status = Column(String(24), nullable=False, default="current", index=True)
    novelty_status = Column(String(24), nullable=False, default="novel", index=True)
    retention_basis = Column(String(256), nullable=False)
    supersedes_id = Column(Integer, nullable=True)
    withdrawn_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentDistribution(Base):
    __tablename__ = "al_distributions"
    __table_args__ = (
        UniqueConstraint("scope_key", "delivery_key", name="uq_al_distribution_delivery"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    delivery_key = Column(String(128), nullable=False)
    intelligence_id = Column(Integer, nullable=False, index=True)
    decision_kind = Column(String(32), nullable=False)
    target_principal = Column(String(128), nullable=False, index=True)
    recipient_id = Column(String(128), nullable=True, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True)
    channel = Column(String(64), nullable=True)
    status = Column(String(24), nullable=False, index=True)
    policy_basis = Column(String(256), nullable=False)
    entitlement_basis = Column(String(256), nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentRetentionState(Base):
    __tablename__ = "al_retention_states"
    __table_args__ = (
        UniqueConstraint("scope_key", "object_type", "object_key", name="uq_al_retention_object"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    object_type = Column(String(64), nullable=False)
    object_key = Column(String(128), nullable=False)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    retention_basis = Column(String(256), nullable=False)
    state = Column(String(24), nullable=False, default="active", index=True)
    withdrawal_basis = Column(String(256), nullable=True)
    withdrawn_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


class AgentContextArtifact(Base):
    """Tenant-scoped cache/index/embedding or structured evidence reference."""

    __tablename__ = "al_context_artifacts"
    __table_args__ = (
        UniqueConstraint("scope_key", "artifact_key", "version", name="uq_al_context_artifact_version"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    artifact_key = Column(String(128), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    artifact_kind = Column(String(32), nullable=False, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    security_domain = Column(String(4), nullable=False)
    content_json = Column(Text, nullable=False)
    provenance_json = Column(Text, nullable=False)
    retention_basis = Column(String(256), nullable=False)
    expires_at = Column(DateTime, nullable=True)
    withdrawn_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class AgentAuditEvent(Base):
    __tablename__ = "al_audit_events"

    id = Column(Integer, primary_key=True)
    event_type = Column(String(64), nullable=False, index=True)
    principal_id = Column(String(128), nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    capability_id = Column(String(128), nullable=True, index=True)
    configuration_ref = Column(String(256), nullable=True)
    evidence_json = Column(Text, nullable=False)
    decision = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False, index=True)


class AgentSecurityEvent(Base):
    __tablename__ = "al_security_events"

    __table_args__ = (
        UniqueConstraint("scope_key", "event_key", name="uq_al_security_event"),
    )

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(256), nullable=False, index=True)
    event_key = Column(String(128), nullable=False)
    event_type = Column(String(64), nullable=False, index=True)
    severity = Column(String(16), nullable=False)
    implicated_principal = Column(String(128), nullable=True, index=True)
    client_id = Column(Integer, nullable=True, index=True)
    project_code = Column(String(128), nullable=True)
    security_domain = Column(String(4), nullable=False)
    evidence_json = Column(Text, nullable=False)
    containment_json = Column(Text, nullable=False, default="{}")
    status = Column(String(24), nullable=False, default="open", index=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    resolved_at = Column(DateTime, nullable=True)
