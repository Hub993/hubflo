"""Transactional persistence and current-state queries for Agent Layer 2.0."""

import datetime as dt
import hashlib
import json
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Optional

from sqlalchemy import and_, func, or_
from sqlalchemy.exc import IntegrityError

from storage import Base, ENGINE, SessionLocal

from .contracts import AuthorityError, ConflictError, Principal, SecurityError
from .models import (
    AgentApproval,
    AgentAuditEvent,
    AgentAuthorityGrant,
    AgentAuthorityValue,
    AgentPrincipalAuthority,
    AgentCapability,
    AgentConfiguration,
    AgentContextArtifact,
    AgentDistribution,
    AgentEntitlement,
    AgentExecution,
    AgentIntelligence,
    AgentLearning,
    AgentObjective,
    AgentProviderPolicy,
    AgentRetentionState,
    AgentSecurityEvent,
    AgentVerification,
    AgentWorkUnit,
)
from .security import contains_secret, safe_evidence


def utcnow():
    return dt.datetime.utcnow()


def dumps(value: Any) -> str:
    if contains_secret(value):
        raise SecurityError("secret-bearing values cannot enter Agent Layer durable state")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def loads(value: Optional[str], default=None):
    if not value:
        return {} if default is None else default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return {} if default is None else default


def stable_hash(value: Any) -> str:
    return hashlib.sha256(dumps(value).encode("utf-8")).hexdigest()


def scope_key(client_id=None, project_code=None) -> str:
    if client_id is None:
        if project_code is not None:
            raise SecurityError("project scope requires client scope")
        return "platform"
    return "client:%s/project:%s" % (client_id, project_code) if project_code else "client:%s" % client_id


def scope_candidates(client_id=None, project_code=None):
    keys = []
    if client_id is not None and project_code:
        keys.append(scope_key(client_id, project_code))
    if client_id is not None:
        keys.append(scope_key(client_id))
    keys.append("platform")
    return keys


class AgentRepository:
    """HUBFLO-owned durable state with tenant-scoped query boundaries."""

    def init_schema(self) -> None:
        Base.metadata.create_all(ENGINE)

    def audit(
        self,
        event_type: str,
        principal_id: str,
        decision: str,
        evidence: Mapping[str, Any],
        client_id: Optional[int] = None,
        project_code: Optional[str] = None,
        capability_id: Optional[str] = None,
        configuration_ref: Optional[str] = None,
        session=None,
    ) -> int:
        own = session is None
        active = session or SessionLocal()
        try:
            row = AgentAuditEvent(
                event_type=event_type,
                principal_id=principal_id,
                client_id=client_id,
                project_code=project_code,
                capability_id=capability_id,
                configuration_ref=configuration_ref,
                evidence_json=dumps(safe_evidence(evidence)),
                decision=decision,
            )
            active.add(row)
            active.flush()
            row_id = row.id
            if own:
                active.commit()
            return row_id
        finally:
            if own:
                active.close()

    def install_authority_value(
        self,
        family: str,
        scope_key: str,
        value: Any,
        authority_instrument: str,
        approved_by: str,
        effective_at: dt.datetime,
        proof_ref: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not authority_instrument or not proof_ref or not approved_by or effective_at is None:
            raise ValueError("versioned authority instrument, proof, approver and effective time required")
        with SessionLocal() as session:
            current = (
                session.query(AgentAuthorityValue)
                .filter(
                    AgentAuthorityValue.family == family,
                    AgentAuthorityValue.scope_key == scope_key,
                    AgentAuthorityValue.status == "effective",
                )
                .order_by(AgentAuthorityValue.version.desc())
                .first()
            )
            if current is not None and current.authority_instrument == authority_instrument:
                return self._authority_dict(current)
            latest_version = session.query(func.max(AgentAuthorityValue.version)).filter(
                AgentAuthorityValue.family == family,
                AgentAuthorityValue.scope_key == scope_key,
            ).scalar() or 0
            version = latest_version + 1
            if current:
                current.status = "superseded"
                if family == "AB-AUTH-001":
                    session.query(AgentPrincipalAuthority).filter(
                        AgentPrincipalAuthority.scope_key == scope_key,
                        AgentPrincipalAuthority.version == current.version,
                        AgentPrincipalAuthority.revoked_at == None,
                    ).update({AgentPrincipalAuthority.revoked_at: utcnow()})
            row = AgentAuthorityValue(
                family=family,
                scope_key=scope_key,
                version=version,
                value_json=dumps(value),
                status="effective",
                authority_instrument=authority_instrument,
                proof_ref=proof_ref,
                approved_by=approved_by,
                effective_at=effective_at,
            )
            session.add(row)
            session.flush()
            if family == "AB-AUTH-001":
                principals = value.get("principals", {}) if isinstance(value, Mapping) else {}
                if not isinstance(principals, Mapping):
                    raise ValueError("AB-AUTH-001 principals must be a mapping")
                for principal_id, authority in principals.items():
                    if not isinstance(authority, Mapping):
                        raise ValueError("principal authority must be structured")
                    principal_class = authority.get("principal_class")
                    independence_group = authority.get("independence_group")
                    if not principal_class or not independence_group:
                        raise ValueError("principal class and independence group required")
                    session.add(AgentPrincipalAuthority(
                        scope_key=scope_key, principal_id=str(principal_id),
                        principal_class=str(principal_class), version=version,
                        authority_json=dumps(authority), authority_basis=authority_instrument,
                        independence_group=str(independence_group), effective_at=effective_at,
                    ))
            self.audit(
                "authority_value",
                approved_by,
                "INSTALLED",
                {"family": family, "scope_key": scope_key, "version": version},
                session=session,
            )
            session.commit()
            return self._authority_dict(row)

    def authority_value(self, family: str, scope_keys: Iterable[str]) -> Optional[Dict[str, Any]]:
        now = utcnow()
        ordered = list(scope_keys)
        with SessionLocal() as session:
            rows = (
                session.query(AgentAuthorityValue)
                .filter(
                    AgentAuthorityValue.family == family,
                    AgentAuthorityValue.scope_key.in_(ordered),
                    AgentAuthorityValue.status == "effective",
                    AgentAuthorityValue.revoked_at == None,
                    AgentAuthorityValue.effective_at <= now,
                )
                .all()
            )
        by_scope = {row.scope_key: row for row in rows}
        for scope_key in ordered:
            if scope_key in by_scope:
                return self._authority_dict(by_scope[scope_key])
        return None

    def revoke_authority_value(self, family: str, scope_key: str, actor: str) -> bool:
        actor_id = actor.principal_id if isinstance(actor, Principal) else str(actor)
        client_id = None
        project_code = None
        if scope_key.startswith("client:"):
            parts = scope_key.split("/project:", 1)
            client_id = int(parts[0].split(":", 1)[1])
            project_code = parts[1] if len(parts) == 2 else None
        self.require_actor(actor, "authority.revoke", client_id, project_code)
        with SessionLocal() as session:
            row = (
                session.query(AgentAuthorityValue)
                .filter(
                    AgentAuthorityValue.family == family,
                    AgentAuthorityValue.scope_key == scope_key,
                    AgentAuthorityValue.status == "effective",
                )
                .order_by(AgentAuthorityValue.version.desc())
                .first()
            )
            if row is None:
                return False
            row.status = "revoked"
            row.revoked_at = utcnow()
            if family == "AB-AUTH-001":
                session.query(AgentPrincipalAuthority).filter(
                    AgentPrincipalAuthority.scope_key == scope_key,
                    AgentPrincipalAuthority.version == row.version,
                    AgentPrincipalAuthority.revoked_at == None,
                ).update({AgentPrincipalAuthority.revoked_at: row.revoked_at})
            self.audit(
                "authority_value", actor_id, "REVOKED",
                {"family": family, "scope_key": scope_key, "version": row.version},
                session=session,
            )
            session.commit()
            return True

    @staticmethod
    def _authority_dict(row: AgentAuthorityValue) -> Dict[str, Any]:
        return {
            "family": row.family,
            "scope_key": row.scope_key,
            "version": row.version,
            "value": loads(row.value_json),
            "status": row.status,
            "authority_instrument": row.authority_instrument,
            "proof_ref": row.proof_ref,
            "approved_by": row.approved_by,
            "effective_at": row.effective_at,
        }

    def actor_authority(self, actor, client_id=None, project_code=None, exact_scope=False):
        principal_id = actor.principal_id if isinstance(actor, Principal) else str(actor)
        principal_class = actor.principal_class if isinstance(actor, Principal) else None
        now = utcnow()
        with SessionLocal() as session:
            candidates = [scope_key(client_id, project_code)] if exact_scope else scope_candidates(
                client_id, project_code)
            rows = session.query(AgentPrincipalAuthority).filter(
                AgentPrincipalAuthority.principal_id == principal_id,
                AgentPrincipalAuthority.scope_key.in_(candidates),
                AgentPrincipalAuthority.effective_at <= now,
                AgentPrincipalAuthority.revoked_at == None,
            ).order_by(AgentPrincipalAuthority.version.desc()).all()
            by_scope = {}
            for row in rows:
                by_scope.setdefault(row.scope_key, row)
            for key in candidates:
                row = by_scope.get(key)
                if row and (principal_class is None or row.principal_class == principal_class):
                    value = loads(row.authority_json)
                    value.update({"principal_id": row.principal_id,
                                  "principal_class": row.principal_class,
                                  "authority_basis": row.authority_basis,
                                  "independence_group": row.independence_group,
                                  "scope_key": row.scope_key})
                    return value
        return None

    def require_actor(self, actor, permission, client_id=None, project_code=None,
                      capability_id=None, risk_class=None):
        authority = self.actor_authority(actor, client_id, project_code)
        if authority is None or permission not in set(authority.get("permissions", [])):
            raise AuthorityError("actor lacks current %s authority" % permission)
        capabilities = set(authority.get("capabilities", []))
        if capability_id and "*" not in capabilities and capability_id not in capabilities:
            raise AuthorityError("actor authority excludes capability")
        risks = set(authority.get("risk_classes", []))
        if risk_class and "*" not in risks and risk_class not in risks:
            raise AuthorityError("actor authority excludes risk class")
        return authority

    def propose_configuration(
        self,
        subject_key: str,
        state: Mapping[str, Any],
        proposer: str,
        idempotency_key: str,
        client_id: Optional[int] = None,
        project_code: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            existing = session.query(AgentConfiguration).filter(
                AgentConfiguration.scope_key == scoped,
                AgentConfiguration.idempotency_key == idempotency_key
            ).first()
            if existing:
                if existing.subject_key != subject_key or loads(existing.state_json) != dict(state):
                    raise ConflictError("idempotency key reused for different configuration")
                return self._configuration_dict(existing)
            latest = session.query(func.max(AgentConfiguration.version)).filter(
                AgentConfiguration.scope_key == scoped,
                AgentConfiguration.subject_key == subject_key
            ).scalar() or 0
            row = AgentConfiguration(
                scope_key=scoped,
                client_id=client_id,
                project_code=project_code,
                subject_key=subject_key,
                version=latest + 1,
                state_json=dumps(state),
                status="proposed",
                proposer=proposer,
                reason=reason,
                parent_version=latest or None,
                idempotency_key=idempotency_key,
            )
            session.add(row)
            session.flush()
            self.audit(
                "configuration", proposer, "VALIDATED-PROPOSAL",
                {"subject_key": subject_key, "version": row.version},
                client_id, project_code, configuration_ref="%s@%s" % (subject_key, row.version),
                session=session,
            )
            session.commit()
            return self._configuration_dict(row)

    def commit_configuration(
        self,
        subject_key: str,
        version: int,
        expected_effective_version: Optional[int],
        approver: str,
        authority_basis: str,
        client_id=None,
        project_code=None,
    ) -> Dict[str, Any]:
        with SessionLocal() as session:
            proposal = session.query(AgentConfiguration).filter(
                AgentConfiguration.subject_key == subject_key,
                AgentConfiguration.version == version,
                AgentConfiguration.scope_key == scope_key(client_id, project_code),
            ).first()
            if proposal is None:
                raise ValueError("proposal not found")
            scoped = proposal.scope_key
            if proposal.status == "effective":
                return self._configuration_dict(proposal)
            if proposal.status != "proposed":
                raise ConflictError("configuration is not a committable proposal")
            effective = (
                session.query(AgentConfiguration)
                .filter(
                    AgentConfiguration.subject_key == subject_key,
                    AgentConfiguration.scope_key == scoped,
                    AgentConfiguration.status == "effective",
                    AgentConfiguration.revoked_at == None,
                )
                .order_by(AgentConfiguration.version.desc())
                .first()
            )
            actual = effective.version if effective else None
            if actual != expected_effective_version:
                self.audit(
                    "configuration", approver, "CONFLICT/REVALIDATION REQUIRED",
                    {"subject_key": subject_key, "expected": expected_effective_version, "actual": actual},
                    proposal.client_id, proposal.project_code, session=session,
                )
                session.commit()
                raise ConflictError("stale configuration proposal")
            if effective:
                effective.status = "superseded"
            proposal.status = "effective"
            proposal.approver = approver
            proposal.authority_basis = authority_basis
            proposal.effective_at = utcnow()
            self.audit(
                "configuration", approver, "COMMITTED",
                {"subject_key": subject_key, "before": actual, "after": version},
                proposal.client_id, proposal.project_code,
                configuration_ref="%s@%s" % (subject_key, version), session=session,
            )
            session.commit()
            return self._configuration_dict(proposal)

    def plan_configuration_commit(self, subject_key: str, version: int,
                                  expected_effective_version: Optional[int],
                                  client_id=None, project_code=None) -> Dict[str, Any]:
        """Read-only version/conflict validation shared by commit and shadow."""
        with SessionLocal() as session:
            proposal = session.query(AgentConfiguration).filter(
                AgentConfiguration.subject_key == subject_key,
                AgentConfiguration.version == version,
                AgentConfiguration.scope_key == scope_key(client_id, project_code),
            ).first()
            if proposal is None:
                raise ConflictError("configuration proposal not found")
            if proposal.status != "proposed" or proposal.revoked_at is not None:
                raise ConflictError("configuration is not a committable proposal")
            effective = session.query(AgentConfiguration).filter(
                AgentConfiguration.subject_key == subject_key,
                AgentConfiguration.scope_key == proposal.scope_key,
                AgentConfiguration.status == "effective",
                AgentConfiguration.revoked_at == None,
            ).order_by(AgentConfiguration.version.desc()).first()
            actual = effective.version if effective else None
            if actual != expected_effective_version:
                raise ConflictError("stale configuration proposal")
            return self._configuration_dict(proposal)

    def plan_configuration_revoke(self, subject_key: str, version: int,
                                  client_id=None, project_code=None) -> Dict[str, Any]:
        """Read-only validation for the effective configuration revoke path."""
        with SessionLocal() as session:
            row = session.query(AgentConfiguration).filter(
                AgentConfiguration.subject_key == subject_key,
                AgentConfiguration.version == version,
                AgentConfiguration.scope_key == scope_key(client_id, project_code),
                AgentConfiguration.status == "effective",
                AgentConfiguration.revoked_at == None,
            ).first()
            if row is None:
                raise ConflictError("configuration is not an effective revocable version")
            return self._configuration_dict(row)

    def effective_configuration(self, subject_keys: Iterable[str], client_id=None,
                                project_code=None) -> Dict[str, Any]:
        result = {}
        provenance = []
        with SessionLocal() as session:
            for subject_key in subject_keys:
                if subject_key == "platform" or subject_key.startswith("capability:"):
                    scoped = "platform"
                elif subject_key.startswith("project:"):
                    scoped = scope_key(client_id, project_code)
                elif subject_key.startswith("client:"):
                    scoped = scope_key(client_id)
                else:
                    scoped = scope_key(client_id, project_code)
                row = (
                    session.query(AgentConfiguration)
                    .filter(
                        AgentConfiguration.subject_key == subject_key,
                        AgentConfiguration.scope_key == scoped,
                        AgentConfiguration.status == "effective",
                        AgentConfiguration.revoked_at == None,
                    )
                    .order_by(AgentConfiguration.version.desc())
                    .first()
                )
                if row:
                    state = loads(row.state_json)
                    result = self._deep_merge(result, state)
                    provenance.append({"subject_key": subject_key, "version": row.version})
        return {"effective": result, "provenance": provenance}

    def revoke_configuration(self, subject_key: str, version: int, actor: str,
                             client_id=None, project_code=None) -> bool:
        if isinstance(actor, Principal):
            if client_id is None:
                client_id, project_code = actor.scope.client_id, actor.scope.project_code
            elif (client_id, project_code) != (actor.scope.client_id, actor.scope.project_code):
                raise AuthorityError("configuration revocation scope mismatch")
        actor_id = actor.principal_id if isinstance(actor, Principal) else str(actor)
        with SessionLocal() as session:
            query = session.query(AgentConfiguration).filter(
                AgentConfiguration.subject_key == subject_key,
                AgentConfiguration.version == version,
                AgentConfiguration.status == "effective",
            )
            if client_id is not None or project_code is not None:
                query = query.filter(AgentConfiguration.scope_key == scope_key(client_id, project_code))
            rows = query.all()
            if len(rows) != 1:
                # An unscoped legacy call is safe only when the identity is
                # unambiguous; never choose among clients/projects.
                if len(rows) > 1 or client_id is not None or project_code is not None:
                    raise AuthorityError("exact configuration scope required")
                return False
            row = rows[0]
            self.require_actor(actor, "authority.revoke", row.client_id, row.project_code)
            if row is None:
                return False
            row.status = "revoked"
            row.revoked_at = utcnow()
            self.audit(
                "configuration", actor_id, "REVOKED",
                {"subject_key": subject_key, "version": version},
                row.client_id, row.project_code, session=session,
            )
            session.commit()
            return True

    @staticmethod
    def _deep_merge(base: Dict[str, Any], addition: Mapping[str, Any]) -> Dict[str, Any]:
        result = dict(base)
        for key, value in addition.items():
            if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
                result[key] = AgentRepository._deep_merge(dict(result[key]), value)
            else:
                result[key] = value
        return result

    @staticmethod
    def _configuration_dict(row: AgentConfiguration) -> Dict[str, Any]:
        return {
            "subject_key": row.subject_key,
            "version": row.version,
            "state": loads(row.state_json),
            "status": row.status,
            "client_id": row.client_id,
            "project_code": row.project_code,
            "proposer": row.proposer,
            "approver": row.approver,
            "authority_basis": row.authority_basis,
            "parent_version": row.parent_version,
        }

    def upsert_capability(self, definition, actor: str = "installation") -> Dict[str, Any]:
        contract = {
            "identity": {"capability_id": definition.capability_id, "version": definition.version},
            "eligible_principal_classes": list(definition.eligible_principal_classes),
            "concurrency": definition.concurrency,
            "claim_required": definition.claim_required,
            "idempotency": definition.idempotency,
            "uncertain_outcome": definition.uncertain_outcome,
            "no_agent_behavior": definition.no_agent_behavior,
            "input_schema": {key: value.__name__ for key, value in definition.input_schema.items()},
            "optional_input_fields": list(definition.optional_input_fields),
            "output_schema": {key: value.__name__ for key, value in definition.output_schema.items()},
            "optional_output_fields": list(definition.optional_output_fields),
            "shadow_applicability": definition.shadow_applicability,
            "input_semantics": list(definition.input_semantics),
            "output_semantics": list(definition.output_semantics),
            "information_contract": definition.information_contract,
            "required_permission": definition.required_permission,
            "required_configuration": list(definition.required_configuration),
            "execution_path": definition.execution_path,
            "preconditions": list(definition.preconditions),
            "approval_contract": definition.approval_contract,
            "audit_contract": list(definition.audit_contract),
            "failure_contract": definition.failure_contract,
            "verification_contract": definition.verification_contract,
            "regression_dependencies": list(definition.regression_dependencies),
            "required_action": definition.required_action,
        }
        with SessionLocal() as session:
            row = session.get(AgentCapability, definition.capability_id)
            created = row is None
            if row is None:
                row = AgentCapability(capability_id=definition.capability_id)
                session.add(row)
            row.version = definition.version
            row.family = definition.family
            row.purpose = definition.purpose
            row.side_effect_class = definition.side_effect_class
            row.risk_class = definition.risk_class
            row.contract_json = dumps(contract)
            if created:
                row.enabled = bool(definition.enabled)
            row.requires_entitlement = bool(definition.requires_entitlement)
            row.requires_provider = bool(definition.requires_provider)
            row.requires_approval = bool(definition.requires_approval)
            row.requires_verification = bool(definition.requires_verification)
            session.commit()
            return self.capability(definition.capability_id)

    def capability(self, capability_id: str) -> Optional[Dict[str, Any]]:
        with SessionLocal() as session:
            row = session.get(AgentCapability, capability_id)
            return self._capability_dict(row) if row else None

    def capabilities(self) -> List[Dict[str, Any]]:
        with SessionLocal() as session:
            return [self._capability_dict(row) for row in session.query(AgentCapability).all()]

    def set_capability_state(
        self, capability_id: str, actor, enabled: Optional[bool] = None,
        healthy: Optional[bool] = None, reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        authority = self.require_actor(actor, "capability.control", capability_id=capability_id)
        actor_id = actor.principal_id if isinstance(actor, Principal) else str(actor)
        with SessionLocal() as session:
            row = session.get(AgentCapability, capability_id)
            if row is None:
                raise ValueError("capability not found")
            if enabled is not None:
                row.enabled = enabled
            if healthy is not None:
                row.healthy = healthy
            row.kill_reason = reason
            self.audit(
                "capability_control", actor_id, "UPDATED",
                {"capability_id": capability_id, "enabled": row.enabled, "healthy": row.healthy, "reason": reason},
                capability_id=capability_id, session=session,
            )
            session.commit()
            return self._capability_dict(row)

    @staticmethod
    def _capability_dict(row: AgentCapability) -> Dict[str, Any]:
        return {
            "capability_id": row.capability_id, "version": row.version,
            "family": row.family, "purpose": row.purpose,
            "side_effect_class": row.side_effect_class, "risk_class": row.risk_class,
            "contract": loads(row.contract_json), "enabled": bool(row.enabled),
            "healthy": bool(row.healthy), "requires_entitlement": bool(row.requires_entitlement),
            "requires_provider": bool(row.requires_provider), "requires_approval": bool(row.requires_approval),
            "requires_verification": bool(row.requires_verification), "kill_reason": row.kill_reason,
        }

    def grant_authority(
        self, grant_key: str, principal_id: str, principal_class: str,
        capability_id: str, authority_basis: str, client_id: Optional[int] = None,
        project_code: Optional[str] = None, max_autonomy: int = 2,
        information_permissions: Iterable[str] = (), action_permissions: Iterable[str] = (),
        allowed_domains: Iterable[str] = (), delegator_id: Optional[str] = None,
        expires_at: Optional[dt.datetime] = None, granting_actor=None,
        autonomy_limits: Optional[Mapping[str, Any]] = None,
        shadow_allowed: bool = False,
    ) -> Dict[str, Any]:
        if max_autonomy < 1 or max_autonomy > 5:
            raise ValueError("autonomy must be 1..5")
        if granting_actor is None:
            raise AuthorityError("granting actor required")
        authority = self.require_actor(granting_actor, "authority.delegate", client_id,
                                       project_code, capability_id)
        actor_id = granting_actor.principal_id if isinstance(granting_actor, Principal) else str(granting_actor)
        if delegator_id is not None and delegator_id != actor_id:
            raise AuthorityError("delegator identity does not match authenticated actor")
        ceiling = int(authority.get("max_autonomy", 0))
        if max_autonomy > ceiling:
            raise AuthorityError("delegation exceeds autonomy ceiling")
        for requested, field in ((set(action_permissions), "actions"),
                                 (set(information_permissions), "information_permissions"),
                                 (set(allowed_domains), "domains")):
            allowed_values = set(authority.get(field, []))
            if "*" not in allowed_values and not requested.issubset(allowed_values):
                raise AuthorityError("delegation exceeds %s ceiling" % field)
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            existing = session.query(AgentAuthorityGrant).filter(
                AgentAuthorityGrant.scope_key == scoped,
                AgentAuthorityGrant.grant_key == grant_key
            ).first()
            if existing:
                return self._grant_dict(existing)
            row = AgentAuthorityGrant(
                scope_key=scoped,
                grant_key=grant_key, principal_id=principal_id, principal_class=principal_class,
                capability_id=capability_id, client_id=client_id, project_code=project_code,
                max_autonomy=max_autonomy, information_permissions_json=dumps(list(information_permissions)),
                action_permissions_json=dumps(list(action_permissions)),
                allowed_domains_json=dumps(list(allowed_domains)), authority_basis=authority_basis,
                autonomy_limits_json=dumps(autonomy_limits or {}),
                shadow_allowed=bool(shadow_allowed),
                delegator_id=actor_id, expires_at=expires_at,
            )
            session.add(row)
            session.commit()
            return self._grant_dict(row)

    def current_grant(
        self, principal_id: str, capability_id: str,
        client_id: Optional[int], project_code: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        now = utcnow()
        with SessionLocal() as session:
            rows = session.query(AgentAuthorityGrant).filter(
                AgentAuthorityGrant.principal_id == principal_id,
                AgentAuthorityGrant.capability_id == capability_id,
                AgentAuthorityGrant.revoked_at == None,
                AgentAuthorityGrant.effective_at <= now,
                or_(AgentAuthorityGrant.expires_at == None, AgentAuthorityGrant.expires_at > now),
                or_(AgentAuthorityGrant.client_id == None, AgentAuthorityGrant.client_id == client_id),
                or_(AgentAuthorityGrant.project_code == None, AgentAuthorityGrant.project_code == project_code),
            ).all()
            if not rows:
                return None
            rows.sort(key=lambda row: (row.client_id is not None, row.project_code is not None), reverse=True)
            return self._grant_dict(rows[0])

    def revoke_grant(self, grant_key: str, actor, client_id=None, project_code=None) -> bool:
        authority = self.require_actor(actor, "authority.revoke", client_id, project_code)
        actor_id = actor.principal_id if isinstance(actor, Principal) else str(actor)
        with SessionLocal() as session:
            row = session.query(AgentAuthorityGrant).filter(
                AgentAuthorityGrant.scope_key == scope_key(client_id, project_code),
                AgentAuthorityGrant.grant_key == grant_key,
                AgentAuthorityGrant.revoked_at == None,
            ).first()
            if row is None:
                return False
            row.revoked_at = utcnow()
            self.audit(
                "authority_grant", actor_id, "REVOKED",
                {"grant_key": grant_key, "principal_id": row.principal_id},
                row.client_id, row.project_code, row.capability_id, session=session,
            )
            session.commit()
            return True

    @staticmethod
    def _grant_dict(row: AgentAuthorityGrant) -> Dict[str, Any]:
        return {
            "grant_key": row.grant_key, "principal_id": row.principal_id,
            "principal_class": row.principal_class, "capability_id": row.capability_id,
            "client_id": row.client_id, "project_code": row.project_code,
            "max_autonomy": row.max_autonomy,
            "information_permissions": loads(row.information_permissions_json, []),
            "action_permissions": loads(row.action_permissions_json, []),
            "allowed_domains": loads(row.allowed_domains_json, []),
            "autonomy_limits": loads(row.autonomy_limits_json),
            "shadow_allowed": bool(row.shadow_allowed),
            "authority_basis": row.authority_basis, "delegator_id": row.delegator_id,
            "expires_at": row.expires_at, "revoked_at": row.revoked_at,
        }

    def assign_entitlement(
        self, entitlement_key: str, client_id: int, dimension: str, subject: str,
        value: Any, version: int, authority_basis: str,
        principal_id: Optional[str] = None, granting_actor=None,
    ) -> Dict[str, Any]:
        if granting_actor is None:
            raise AuthorityError("entitlement granting actor required")
        self.require_actor(granting_actor, "entitlement.configure", client_id)
        scoped = scope_key(client_id)
        with SessionLocal() as session:
            row = session.query(AgentEntitlement).filter(
                AgentEntitlement.scope_key == scoped,
                AgentEntitlement.entitlement_key == entitlement_key
            ).first()
            if row is None:
                row = AgentEntitlement(
                    scope_key=scoped,
                    entitlement_key=entitlement_key, client_id=client_id,
                    principal_id=principal_id, dimension=dimension, subject=subject,
                    value_json=dumps(value), version=version, authority_basis=authority_basis,
                )
                session.add(row)
                session.commit()
            return self._entitlement_dict(row)

    def entitlement(self, client_id: int, dimension: str, subject: str, principal_id=None):
        with SessionLocal() as session:
            row = session.query(AgentEntitlement).filter(
                AgentEntitlement.client_id == client_id,
                AgentEntitlement.dimension == dimension,
                AgentEntitlement.subject == subject,
                AgentEntitlement.revoked_at == None,
                or_(AgentEntitlement.principal_id == None, AgentEntitlement.principal_id == principal_id),
            ).order_by(AgentEntitlement.version.desc()).first()
            return self._entitlement_dict(row) if row else None

    def revoke_entitlement(self, entitlement_key: str, client_id: int) -> bool:
        with SessionLocal() as session:
            row = session.query(AgentEntitlement).filter(
                AgentEntitlement.entitlement_key == entitlement_key,
                AgentEntitlement.scope_key == scope_key(client_id),
                AgentEntitlement.revoked_at == None,
            ).first()
            if not row:
                return False
            row.revoked_at = utcnow()
            session.commit()
            return True

    @staticmethod
    def _entitlement_dict(row):
        return {"entitlement_key": row.entitlement_key, "client_id": row.client_id,
                "principal_id": row.principal_id, "dimension": row.dimension,
                "subject": row.subject, "value": loads(row.value_json),
                "version": row.version, "authority_basis": row.authority_basis}

    def install_provider_policy(
        self, provider_id: str, version: int, allowed_data_classes: Iterable[str],
        training_permitted: bool, retention_mode: str, allowed: bool,
        authority_basis: str, granting_actor=None, *, allowed_confidentiality=(),
        permitted_uses=(), retention_max_seconds=None, access_controls=(),
        allowed_regions=(), audit_required=True, attribution_required=True,
        deletion_supported=False, withdrawal_supported=False,
        allowed_distribution_uses=(), terms_ref=None,
    ) -> Dict[str, Any]:
        if granting_actor is None:
            raise AuthorityError("provider-policy granting actor required")
        self.require_actor(granting_actor, "provider.configure")
        if not terms_ref:
            raise SecurityError("provider terms reference required")
        with SessionLocal() as session:
            existing = session.query(AgentProviderPolicy).filter(
                AgentProviderPolicy.provider_id == provider_id,
                AgentProviderPolicy.version == version,
            ).first()
            if existing is None:
                existing = AgentProviderPolicy(
                    provider_id=provider_id, version=version,
                    allowed_data_classes_json=dumps(list(allowed_data_classes)),
                    allowed_confidentiality_json=dumps(list(allowed_confidentiality)),
                    permitted_uses_json=dumps(list(permitted_uses)),
                    training_permitted=training_permitted, retention_mode=retention_mode,
                    retention_max_seconds=retention_max_seconds,
                    access_controls_json=dumps(list(access_controls)),
                    allowed_regions_json=dumps(list(allowed_regions)),
                    audit_required=audit_required, attribution_required=attribution_required,
                    deletion_supported=deletion_supported,
                    withdrawal_supported=withdrawal_supported,
                    allowed_distribution_uses_json=dumps(list(allowed_distribution_uses)),
                    terms_ref=terms_ref,
                    allowed=allowed, authority_basis=authority_basis,
                )
                session.add(existing)
                session.commit()
            return self._provider_policy_dict(existing)

    def provider_policy(self, provider_id: str) -> Optional[Dict[str, Any]]:
        with SessionLocal() as session:
            row = session.query(AgentProviderPolicy).filter(
                AgentProviderPolicy.provider_id == provider_id,
                AgentProviderPolicy.allowed == True,
                AgentProviderPolicy.revoked_at == None,
            ).order_by(AgentProviderPolicy.version.desc()).first()
            return self._provider_policy_dict(row) if row else None

    def revoke_provider(self, provider_id: str) -> int:
        with SessionLocal() as session:
            count = session.query(AgentProviderPolicy).filter(
                AgentProviderPolicy.provider_id == provider_id,
                AgentProviderPolicy.revoked_at == None,
            ).update({AgentProviderPolicy.revoked_at: utcnow(), AgentProviderPolicy.allowed: False})
            session.commit()
            return count

    @staticmethod
    def _provider_policy_dict(row):
        return {"provider_id": row.provider_id, "version": row.version,
                "allowed_data_classes": loads(row.allowed_data_classes_json, []),
                "allowed_confidentiality": loads(row.allowed_confidentiality_json, []),
                "permitted_uses": loads(row.permitted_uses_json, []),
                "training_permitted": bool(row.training_permitted),
                "retention_mode": row.retention_mode,
                "retention_max_seconds": row.retention_max_seconds,
                "access_controls": loads(row.access_controls_json, []),
                "allowed_regions": loads(row.allowed_regions_json, []),
                "audit_required": bool(row.audit_required),
                "attribution_required": bool(row.attribution_required),
                "deletion_supported": bool(row.deletion_supported),
                "withdrawal_supported": bool(row.withdrawal_supported),
                "allowed_distribution_uses": loads(row.allowed_distribution_uses_json, []),
                "terms_ref": row.terms_ref, "allowed": bool(row.allowed),
                "authority_basis": row.authority_basis}

    def create_objective(
        self, objective_key: str, owner_principal: str, client_id: int,
        desired_outcome: str, success_criteria: Mapping[str, Any],
        project_code: Optional[str] = None, dependencies=(), evidence_refs=(),
        risk_class: str = "R1", deadline=None, responsible_capability=None,
        responsible_version=None,
    ) -> Dict[str, Any]:
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            existing = session.query(AgentObjective).filter(
                AgentObjective.scope_key == scoped,
                AgentObjective.objective_key == objective_key
            ).first()
            if existing:
                return self._objective_dict(existing)
            row = AgentObjective(
                scope_key=scoped,
                objective_key=objective_key, owner_principal=owner_principal,
                client_id=client_id, project_code=project_code,
                desired_outcome=desired_outcome, success_criteria_json=dumps(success_criteria),
                dependency_refs_json=dumps(list(dependencies)), evidence_refs_json=dumps(list(evidence_refs)),
                state_json=dumps({"history": [{"status": "pending", "at": utcnow().isoformat()}]}),
                risk_class=risk_class, deadline=deadline,
                responsible_capability=responsible_capability,
                responsible_version=responsible_version,
            )
            session.add(row)
            session.commit()
            return self._objective_dict(row)

    def objective(self, objective_key: str, client_id: int, project_code=None):
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            row = session.query(AgentObjective).filter(
                AgentObjective.scope_key == scoped,
                AgentObjective.objective_key == objective_key,
                AgentObjective.client_id == client_id,
                AgentObjective.project_code == project_code,
            ).first()
            return self._objective_dict(row) if row else None

    def transition_objective(self, objective_key: str, expected_version: int, status: str, actor: str,
                             client_id=None, project_code=None):
        allowed = {"pending": {"active", "cancelled"}, "active": {"blocked", "completed", "cancelled"},
                   "blocked": {"active", "cancelled"}, "completed": set(), "cancelled": set()}
        with SessionLocal() as session:
            row = session.query(AgentObjective).filter(
                AgentObjective.objective_key == objective_key,
                AgentObjective.version == expected_version,
                AgentObjective.scope_key == scope_key(client_id, project_code),
            ).first()
            if not row or status not in allowed.get(row.status, set()):
                raise ConflictError("invalid or stale objective transition")
            state = loads(row.state_json)
            state.setdefault("history", []).append({"status": status, "actor": actor, "at": utcnow().isoformat()})
            row.state_json = dumps(state)
            row.status = status
            row.version += 1
            if status in ("completed", "cancelled"):
                row.closed_at = utcnow()
            session.commit()
            return self._objective_dict(row)

    @staticmethod
    def _objective_dict(row):
        return {"objective_key": row.objective_key, "version": row.version,
                "owner_principal": row.owner_principal, "client_id": row.client_id,
                "project_code": row.project_code, "desired_outcome": row.desired_outcome,
                "success_criteria": loads(row.success_criteria_json),
                "dependencies": loads(row.dependency_refs_json, []),
                "evidence_refs": loads(row.evidence_refs_json, []),
                "state": loads(row.state_json), "status": row.status,
                "risk_class": row.risk_class, "closed_at": row.closed_at}

    def create_work_unit(
        self, work_key: str, objective_key: str, client_id: int,
        capability_id: str, principal_id: str, input_data: Mapping[str, Any],
        project_code=None, parent_work_key=None, dependencies=(),
        failure_policy="STOP", max_attempts=1,
    ) -> Dict[str, Any]:
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            objective = session.query(AgentObjective).filter(
                AgentObjective.scope_key == scoped,
                AgentObjective.objective_key == objective_key).first()
            if objective is None:
                raise SecurityError("scoped objective unavailable")
            references = ([parent_work_key] if parent_work_key else []) + list(dependencies)
            if references:
                found = {value for value, in session.query(AgentWorkUnit.work_key).filter(
                    AgentWorkUnit.scope_key == scoped,
                    AgentWorkUnit.work_key.in_(references)).all()}
                if found != set(references):
                    raise SecurityError("work dependency outside authoritative scope")
            existing = session.query(AgentWorkUnit).filter(
                AgentWorkUnit.scope_key == scoped, AgentWorkUnit.work_key == work_key).first()
            if existing:
                return self._work_dict(existing)
            row = AgentWorkUnit(
                scope_key=scoped,
                work_key=work_key, objective_key=objective_key, parent_work_key=parent_work_key,
                client_id=client_id, project_code=project_code, capability_id=capability_id,
                principal_id=principal_id, dependencies_json=dumps(list(dependencies)),
                input_json=dumps(input_data), failure_policy=failure_policy, max_attempts=max_attempts,
            )
            session.add(row)
            session.commit()
            return self._work_dict(row)

    def claim_work(self, work_key: str, claimant: str, client_id=None, project_code=None) -> Dict[str, Any]:
        token = "%s:%s" % (claimant, uuid.uuid4().hex)
        with SessionLocal() as session:
            row = session.query(AgentWorkUnit).filter(
                AgentWorkUnit.work_key == work_key,
                AgentWorkUnit.scope_key == scope_key(client_id, project_code)).first()
            if row is None:
                return {"status": "not_found"}
            dependencies = loads(row.dependencies_json, [])
            if dependencies:
                states = dict(session.query(AgentWorkUnit.work_key, AgentWorkUnit.status).filter(
                    AgentWorkUnit.scope_key == row.scope_key,
                    AgentWorkUnit.work_key.in_(dependencies)
                ).all())
                if any(states.get(key) != "completed" for key in dependencies):
                    return {"status": "dependency_pending", "dependencies": states}
            updated = session.query(AgentWorkUnit).filter(
                AgentWorkUnit.id == row.id,
                AgentWorkUnit.status == "pending",
                AgentWorkUnit.claim_token == None,
            ).update({AgentWorkUnit.status: "claimed", AgentWorkUnit.claim_token: token,
                      AgentWorkUnit.claimed_at: utcnow(), AgentWorkUnit.attempts: AgentWorkUnit.attempts + 1},
                     synchronize_session=False)
            session.commit()
            if updated != 1:
                current = session.get(AgentWorkUnit, row.id)
                return {"status": "already_%s" % current.status, "claim_token": current.claim_token}
            return {"status": "claimed", "claim_token": token}

    def complete_work(self, work_key: str, claim_token: str, artifact: Mapping[str, Any],
                      client_id=None, project_code=None) -> Dict[str, Any]:
        with SessionLocal() as session:
            updated = session.query(AgentWorkUnit).filter(
                AgentWorkUnit.work_key == work_key,
                AgentWorkUnit.scope_key == scope_key(client_id, project_code),
                AgentWorkUnit.status == "claimed",
                AgentWorkUnit.claim_token == claim_token,
            ).update({AgentWorkUnit.status: "completed", AgentWorkUnit.artifact_json: dumps(artifact),
                      AgentWorkUnit.checkpoint_json: dumps({"completed": True}),
                      AgentWorkUnit.completed_at: utcnow()}, synchronize_session=False)
            session.commit()
            if updated != 1:
                row = session.query(AgentWorkUnit).filter(
                    AgentWorkUnit.work_key == work_key,
                    AgentWorkUnit.scope_key == scope_key(client_id, project_code)).first()
                return {"status": "already_%s" % row.status if row else "not_found"}
            row = session.query(AgentWorkUnit).filter(
                AgentWorkUnit.work_key == work_key,
                AgentWorkUnit.scope_key == scope_key(client_id, project_code)).first()
            return self._work_dict(row)

    def fail_work(self, work_key: str, claim_token: str, error_code: str,
                  client_id=None, project_code=None) -> Dict[str, Any]:
        with SessionLocal() as session:
            row = session.query(AgentWorkUnit).filter(
                AgentWorkUnit.work_key == work_key,
                AgentWorkUnit.scope_key == scope_key(client_id, project_code),
                AgentWorkUnit.status == "claimed",
                AgentWorkUnit.claim_token == claim_token,
            ).first()
            if row is None:
                return {"status": "claim_lost"}
            if row.attempts < row.max_attempts and row.failure_policy == "RETRY":
                row.status = "pending"
                row.claim_token = None
                row.claimed_at = None
            else:
                row.status = "failed" if row.failure_policy != "SKIP" else "skipped"
            row.error_code = error_code
            session.commit()
            return self._work_dict(row)

    def work_units(self, objective_key: str, client_id: int, project_code=None) -> List[Dict[str, Any]]:
        with SessionLocal() as session:
            rows = session.query(AgentWorkUnit).filter(
                AgentWorkUnit.objective_key == objective_key,
                AgentWorkUnit.client_id == client_id,
                AgentWorkUnit.scope_key == scope_key(client_id, project_code),
            ).all()
            return [self._work_dict(row) for row in rows]

    @staticmethod
    def _work_dict(row):
        return {"work_key": row.work_key, "objective_key": row.objective_key,
                "parent_work_key": row.parent_work_key, "client_id": row.client_id,
                "project_code": row.project_code, "capability_id": row.capability_id,
                "principal_id": row.principal_id, "dependencies": loads(row.dependencies_json, []),
                "input": loads(row.input_json), "artifact": loads(row.artifact_json) if row.artifact_json else None,
                "status": row.status, "failure_policy": row.failure_policy,
                "attempts": row.attempts, "max_attempts": row.max_attempts,
                "claim_token": row.claim_token, "checkpoint": loads(row.checkpoint_json) if row.checkpoint_json else None,
                "error_code": row.error_code}

    def execution(self, idempotency_key: str, client_id=None, project_code=None) -> Optional[Dict[str, Any]]:
        with SessionLocal() as session:
            row = session.query(AgentExecution).filter(
                AgentExecution.idempotency_key == idempotency_key
                , AgentExecution.scope_key == scope_key(client_id, project_code)
            ).first()
            return self._execution_dict(row) if row else None

    def claim_execution(self, invocation, capability: Mapping[str, Any], request_hash: str, claimant: str):
        token = "%s:%s" % (claimant, uuid.uuid4().hex)
        scoped = scope_key(invocation.principal.scope.client_id, invocation.principal.scope.project_code)
        with SessionLocal() as session:
            existing = session.query(AgentExecution).filter(
                AgentExecution.idempotency_key == invocation.idempotency_key
                , AgentExecution.scope_key == scoped
            ).first()
            if existing:
                if existing.request_hash != request_hash:
                    return {"status": "conflict", "execution": self._execution_dict(existing)}
                return {"status": "duplicate", "execution": self._execution_dict(existing)}
            row = AgentExecution(
                scope_key=scoped,
                idempotency_key=invocation.idempotency_key,
                capability_id=invocation.capability_id,
                capability_version=capability["version"], principal_id=invocation.principal.principal_id,
                client_id=invocation.principal.scope.client_id,
                project_code=invocation.principal.scope.project_code,
                work_key=invocation.work_key, request_hash=request_hash,
                status="claimed", claim_token=token, claim_owner=claimant,
                request_json=dumps(invocation.payload),
                shadow_mode=bool(invocation.shadow_mode),
            )
            session.add(row)
            try:
                session.commit()
                return {"status": "claimed", "claim_token": token, "execution": self._execution_dict(row)}
            except IntegrityError:
                session.rollback()
                existing = session.query(AgentExecution).filter(
                    AgentExecution.idempotency_key == invocation.idempotency_key
                    , AgentExecution.scope_key == scoped
                ).first()
                return {"status": "duplicate", "execution": self._execution_dict(existing)}

    def complete_execution(
        self, execution_id: int, claim_token: str, outcome: Mapping[str, Any],
        authoritative_evidence: Mapping[str, Any], provider_id=None, provider_version=None,
        approval_id=None, verification_id=None,
    ) -> Dict[str, Any]:
        with SessionLocal() as session:
            updated = session.query(AgentExecution).filter(
                AgentExecution.id == execution_id, AgentExecution.status == "claimed",
                AgentExecution.claim_token == claim_token,
            ).update({AgentExecution.status: "completed", AgentExecution.outcome_json: dumps(outcome),
                      AgentExecution.authoritative_evidence_json: dumps(authoritative_evidence),
                      AgentExecution.provider_id: provider_id, AgentExecution.provider_version: provider_version,
                      AgentExecution.approval_id: approval_id, AgentExecution.verification_id: verification_id,
                      AgentExecution.completed_at: utcnow()}, synchronize_session=False)
            session.commit()
            row = session.get(AgentExecution, execution_id)
            if updated != 1:
                return {"status": "claim_lost", "execution": self._execution_dict(row)}
            return self._execution_dict(row)

    def mark_execution_uncertain(self, execution_id: int, claim_token: str, error_code: str):
        with SessionLocal() as session:
            session.query(AgentExecution).filter(
                AgentExecution.id == execution_id, AgentExecution.status == "claimed",
                AgentExecution.claim_token == claim_token,
            ).update({AgentExecution.status: "outcome_uncertain", AgentExecution.error_code: error_code})
            session.commit()

    def fail_execution(self, execution_id: int, claim_token: str, error_code: str):
        with SessionLocal() as session:
            session.query(AgentExecution).filter(
                AgentExecution.id == execution_id, AgentExecution.status == "claimed",
                AgentExecution.claim_token == claim_token,
            ).update({AgentExecution.status: "stopped", AgentExecution.error_code: error_code})
            session.commit()

    def resolve_uncertain_execution(
        self, execution_id: int, outcome: Mapping[str, Any],
        authoritative_evidence: Mapping[str, Any],
    ) -> Dict[str, Any]:
        with SessionLocal() as session:
            updated = session.query(AgentExecution).filter(
                AgentExecution.id == execution_id,
                AgentExecution.status == "outcome_uncertain",
            ).update({AgentExecution.status: "completed",
                      AgentExecution.outcome_json: dumps(outcome),
                      AgentExecution.authoritative_evidence_json: dumps(authoritative_evidence),
                      AgentExecution.completed_at: utcnow()}, synchronize_session=False)
            session.commit()
            row = session.get(AgentExecution, execution_id)
            if updated != 1:
                return {"status": "not_uncertain", "execution": self._execution_dict(row)}
            return self._execution_dict(row)

    def abandon_uncertain_execution(self, execution_id: int, code: str) -> Dict[str, Any]:
        with SessionLocal() as session:
            session.query(AgentExecution).filter(
                AgentExecution.id == execution_id,
                AgentExecution.status == "outcome_uncertain",
            ).update({AgentExecution.status: "stopped", AgentExecution.error_code: code})
            session.commit()
            return self._execution_dict(session.get(AgentExecution, execution_id))

    @staticmethod
    def _execution_dict(row):
        return {"id": row.id, "idempotency_key": row.idempotency_key,
                "capability_id": row.capability_id, "capability_version": row.capability_version,
                "principal_id": row.principal_id, "client_id": row.client_id,
                "project_code": row.project_code, "work_key": row.work_key,
                "request_hash": row.request_hash, "status": row.status,
                "claim_token": row.claim_token, "outcome": loads(row.outcome_json) if row.outcome_json else {},
                "authoritative_evidence": loads(row.authoritative_evidence_json) if row.authoritative_evidence_json else {},
                "approval_id": row.approval_id, "verification_id": row.verification_id,
                "provider_id": row.provider_id, "provider_version": row.provider_version,
                "error_code": row.error_code, "shadow_mode": bool(row.shadow_mode)}

    def record_approval(
        self, approval_key: str, principal_id: str, capability_id: str,
        client_id: int, request_hash: str, risk_class: str, decision: str,
        authority_basis: str, project_code=None, evidence_refs=(), expires_at=None,
        approver_actor=None,
    ) -> Dict[str, Any]:
        if approver_actor is None:
            raise AuthorityError("authorized approver required")
        authority = self.require_actor(approver_actor, "action.approve", client_id,
                                       project_code, capability_id, risk_class)
        actor_id = approver_actor.principal_id if isinstance(approver_actor, Principal) else str(approver_actor)
        if principal_id != actor_id:
            raise AuthorityError("approver identity does not match authenticated actor")
        if authority_basis != authority["authority_basis"]:
            raise AuthorityError("approval authority basis mismatch")
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            row = session.query(AgentApproval).filter(
                AgentApproval.scope_key == scoped,
                AgentApproval.approval_key == approval_key).first()
            if row is None:
                row = AgentApproval(
                    scope_key=scoped,
                    approval_key=approval_key, principal_id=principal_id,
                    capability_id=capability_id, client_id=client_id, project_code=project_code,
                    request_hash=request_hash, risk_class=risk_class, decision=decision,
                    evidence_refs_json=dumps(list(evidence_refs)), authority_basis=authority_basis,
                    expires_at=expires_at,
                )
                session.add(row)
                session.commit()
            return {"id": row.id, "approval_key": row.approval_key,
                    "principal_id": row.principal_id, "capability_id": row.capability_id,
                    "client_id": row.client_id, "project_code": row.project_code,
                    "request_hash": row.request_hash, "risk_class": row.risk_class,
                    "decision": row.decision, "authority_basis": row.authority_basis,
                    "expires_at": row.expires_at, "revoked_at": row.revoked_at}

    def approval(self, approval_key: str, client_id=None, project_code=None) -> Optional[Dict[str, Any]]:
        now = utcnow()
        with SessionLocal() as session:
            row = session.query(AgentApproval).filter(
                AgentApproval.approval_key == approval_key,
                AgentApproval.scope_key == scope_key(client_id, project_code),
                AgentApproval.decision == "APPROVED", AgentApproval.revoked_at == None,
                or_(AgentApproval.expires_at == None, AgentApproval.expires_at > now),
            ).first()
            if not row:
                return None
            return {"id": row.id, "principal_id": row.principal_id,
                    "capability_id": row.capability_id, "client_id": row.client_id,
                    "project_code": row.project_code, "request_hash": row.request_hash,
                    "risk_class": row.risk_class, "decision": row.decision,
                    "authority_basis": row.authority_basis}

    def record_verification(self, execution_id, verifier, verifier_kind, result, evidence,
                            executor_principal=None):
        execution = None
        with SessionLocal() as session:
            execution = session.get(AgentExecution, execution_id)
            if execution is None:
                raise ValueError("execution not found")
            authority = self.require_actor(verifier, "action.verify", execution.client_id,
                                           execution.project_code, execution.capability_id)
            verifier_id = verifier.principal_id if isinstance(verifier, Principal) else str(verifier)
            executor_authority = self.actor_authority(executor_principal or execution.principal_id,
                                                      execution.client_id, execution.project_code)
            independent = bool(verifier_id != execution.principal_id and executor_authority and
                               authority["independence_group"] != executor_authority["independence_group"])
            if not independent:
                result = "INCONCLUSIVE"
            row = AgentVerification(execution_id=execution_id, verifier_principal=verifier_id,
                                    verifier_authority_basis=authority["authority_basis"],
                                    verifier_independence_group=authority["independence_group"],
                                    verifier_kind=verifier_kind, result=result,
                                    evidence_json=dumps(evidence), independent=independent)
            session.add(row)
            session.commit()
            return {"id": row.id, "result": row.result, "independent": bool(row.independent)}

    def persist_learning(
        self, learning_key: str, learning_scope: str, observation_refs,
        finding, provenance, retention_basis: str, client_id=None,
        project_code=None, user_id=None, industry_key=None, outcome=None,
        supersedes_id=None,
    ) -> Dict[str, Any]:
        if not retention_basis:
            raise SecurityError("retention basis required")
        if learning_scope == "client" and client_id is None:
            raise SecurityError("client learning requires client scope")
        if learning_scope == "individual" and (client_id is None or user_id is None):
            raise SecurityError("individual learning requires user and client scope")
        if learning_scope == "individual":
            authority_scopes = scope_candidates(client_id, project_code)
            if self.authority_value("AB-AUTH-018", authority_scopes) is None:
                raise AuthorityError("AB-AUTH-018 individual adaptation authority required")
            if self.authority_value("SDIP-AV-005", authority_scopes) is None:
                raise SecurityError("SDIP-AV-005 individual adaptation policy required")
        if learning_scope == "industry" and industry_key is None:
            raise SecurityError("industry learning requires configured industry scope")
        if learning_scope == "platform":
            scoped = "platform"
        elif learning_scope == "industry":
            scoped = "industry:%s" % industry_key
        else:
            scoped = scope_key(client_id, project_code)
            if learning_scope == "individual":
                scoped += "/user:%s" % user_id
        with SessionLocal() as session:
            version = (session.query(func.max(AgentLearning.version)).filter(
                AgentLearning.scope_key == scoped,
                AgentLearning.learning_key == learning_key
            ).scalar() or 0) + 1
            if supersedes_id:
                prior = session.get(AgentLearning, supersedes_id)
                if prior is None or prior.scope_key != scoped or prior.learning_key != learning_key:
                    raise SecurityError("learning supersession reference crosses scope or identity")
                if prior.status != "current" or prior.withdrawn_at is not None:
                    raise ConflictError("learning supersession target is not current")
                prior.status = "superseded"
            row = AgentLearning(
                scope_key=scoped,
                learning_key=learning_key, version=version, learning_scope=learning_scope,
                client_id=client_id, project_code=project_code, user_id=user_id,
                industry_key=industry_key, observation_refs_json=dumps(list(observation_refs)),
                finding_json=dumps(finding), outcome_json=dumps(outcome) if outcome is not None else None,
                provenance_json=dumps(provenance), retention_basis=retention_basis,
                supersedes_id=supersedes_id,
            )
            session.add(row)
            session.commit()
            return self._learning_dict(row)

    def learning(self, learning_key: str, client_id=None, project_code=None,
                 user_id=None, industry_key=None):
        with SessionLocal() as session:
            rows = session.query(AgentLearning).filter(
                AgentLearning.learning_key == learning_key,
                AgentLearning.status == "current", AgentLearning.withdrawn_at == None,
            ).order_by(AgentLearning.version.desc()).all()
            eligible = []
            for row in rows:
                if row.learning_scope == "client" and row.client_id != client_id:
                    continue
                if row.learning_scope == "client" and row.project_code != project_code:
                    continue
                if row.learning_scope == "individual" and (row.client_id != client_id or row.user_id != user_id):
                    continue
                if row.learning_scope == "individual" and row.project_code != project_code:
                    continue
                if row.learning_scope == "individual":
                    authority_scopes = scope_candidates(client_id, project_code)
                    if self.authority_value("AB-AUTH-018", authority_scopes) is None:
                        continue
                    if self.authority_value("SDIP-AV-005", authority_scopes) is None:
                        continue
                if row.learning_scope == "industry" and row.industry_key != industry_key:
                    continue
                eligible.append(self._learning_dict(row))
            return eligible

    def learning_by_id(self, learning_id: int) -> Optional[Dict[str, Any]]:
        with SessionLocal() as session:
            row = session.get(AgentLearning, learning_id)
            return self._learning_dict(row) if row else None

    @staticmethod
    def _learning_dict(row):
        return {"id": row.id, "learning_key": row.learning_key, "version": row.version,
                "learning_scope": row.learning_scope, "client_id": row.client_id,
                "project_code": row.project_code, "user_id": row.user_id,
                "industry_key": row.industry_key, "observation_refs": loads(row.observation_refs_json, []),
                "finding": loads(row.finding_json), "outcome": loads(row.outcome_json) if row.outcome_json else None,
                "provenance": loads(row.provenance_json), "status": row.status,
                "retention_basis": row.retention_basis, "supersedes_id": row.supersedes_id}

    def persist_intelligence(
        self, intelligence_key: str, intelligence_type: str, source_refs,
        content, provenance, retention_basis: str, client_id=None,
        project_code=None, industry_key=None, confidentiality="restricted",
        confidence=None, validation=None, novelty_status="novel", supersedes_id=None,
    ) -> Dict[str, Any]:
        if not retention_basis:
            raise SecurityError("retention basis required")
        scoped = scope_key(client_id, project_code) if client_id is not None else (
            "industry:%s" % industry_key if industry_key else "platform")
        with SessionLocal() as session:
            version = (session.query(func.max(AgentIntelligence.version)).filter(
                AgentIntelligence.scope_key == scoped,
                AgentIntelligence.intelligence_key == intelligence_key
            ).scalar() or 0) + 1
            if supersedes_id:
                prior = session.get(AgentIntelligence, supersedes_id)
                if prior is None or prior.scope_key != scoped or prior.intelligence_key != intelligence_key:
                    raise SecurityError("intelligence supersession reference crosses scope or identity")
                if prior.lifecycle_status != "current" or prior.withdrawn_at is not None:
                    raise ConflictError("intelligence supersession target is not current")
                prior.lifecycle_status = "superseded"
            row = AgentIntelligence(
                scope_key=scoped,
                intelligence_key=intelligence_key, version=version,
                intelligence_type=intelligence_type, client_id=client_id,
                project_code=project_code, industry_key=industry_key,
                confidentiality=confidentiality, source_refs_json=dumps(list(source_refs)),
                content_json=dumps(content), provenance_json=dumps(provenance),
                confidence_json=dumps(confidence) if confidence is not None else None,
                validation_json=dumps(validation) if validation is not None else None,
                retention_basis=retention_basis, novelty_status=novelty_status,
                supersedes_id=supersedes_id,
            )
            session.add(row)
            session.commit()
            return self._intelligence_dict(row)

    def intelligence(self, intelligence_key: str, client_id=None, project_code=None):
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            rows = session.query(AgentIntelligence).filter(
                AgentIntelligence.scope_key == scoped,
                AgentIntelligence.intelligence_key == intelligence_key,
                AgentIntelligence.lifecycle_status == "current",
                AgentIntelligence.withdrawn_at == None,
                AgentIntelligence.client_id == client_id,
                AgentIntelligence.project_code == project_code,
            ).order_by(AgentIntelligence.version.desc()).all()
            return [self._intelligence_dict(row) for row in rows]

    @staticmethod
    def _intelligence_dict(row):
        return {"id": row.id, "intelligence_key": row.intelligence_key,
                "version": row.version, "intelligence_type": row.intelligence_type,
                "client_id": row.client_id, "project_code": row.project_code,
                "industry_key": row.industry_key, "security_domain": row.security_domain,
                "confidentiality": row.confidentiality,
                "source_refs": loads(row.source_refs_json, []), "content": loads(row.content_json),
                "provenance": loads(row.provenance_json),
                "confidence": loads(row.confidence_json) if row.confidence_json else None,
                "validation": loads(row.validation_json) if row.validation_json else None,
                "lifecycle_status": row.lifecycle_status, "novelty_status": row.novelty_status,
                "retention_basis": row.retention_basis, "supersedes_id": row.supersedes_id}

    def record_distribution(
        self, delivery_key: str, intelligence_id: int, decision_kind: str,
        target_principal: str, status: str, policy_basis: str,
        client_id=None, project_code=None, recipient_id=None, channel=None,
        entitlement_basis=None,
    ) -> Dict[str, Any]:
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            intelligence = session.get(AgentIntelligence, intelligence_id)
            if (intelligence is None or intelligence.scope_key != scoped or
                    intelligence.withdrawn_at is not None):
                raise SecurityError("distribution intelligence reference unavailable in scope")
            existing = session.query(AgentDistribution).filter(
                AgentDistribution.scope_key == scoped,
                AgentDistribution.delivery_key == delivery_key
            ).first()
            if existing:
                return self._distribution_dict(existing)
            row = AgentDistribution(
                scope_key=scoped,
                delivery_key=delivery_key, intelligence_id=intelligence_id,
                decision_kind=decision_kind, target_principal=target_principal,
                recipient_id=recipient_id, client_id=client_id,
                project_code=project_code, channel=channel, status=status,
                policy_basis=policy_basis, entitlement_basis=entitlement_basis,
                delivered_at=utcnow() if status == "delivered" else None,
            )
            session.add(row)
            session.commit()
            return self._distribution_dict(row)

    @staticmethod
    def _distribution_dict(row):
        return {"id": row.id, "delivery_key": row.delivery_key,
                "intelligence_id": row.intelligence_id, "decision_kind": row.decision_kind,
                "target_principal": row.target_principal, "recipient_id": row.recipient_id,
                "client_id": row.client_id, "project_code": row.project_code,
                "channel": row.channel, "status": row.status,
                "policy_basis": row.policy_basis, "entitlement_basis": row.entitlement_basis}

    def distribution(self, delivery_key: str, client_id=None, project_code=None):
        if not delivery_key:
            return None
        with SessionLocal() as session:
            row = session.query(AgentDistribution).filter(
                AgentDistribution.scope_key == scope_key(client_id, project_code),
                AgentDistribution.delivery_key == delivery_key).first()
            return self._distribution_dict(row) if row else None

    def withdraw(self, object_type: str, object_key: str, client_id: Optional[int], basis: str,
                 project_code=None):
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            state = session.query(AgentRetentionState).filter(
                AgentRetentionState.scope_key == scoped,
                AgentRetentionState.object_type == object_type,
                AgentRetentionState.object_key == object_key,
            ).first()
            if state is None:
                state = AgentRetentionState(
                    scope_key=scoped,
                    object_type=object_type, object_key=object_key, client_id=client_id,
                    project_code=project_code,
                    retention_basis=basis, state="restricted", withdrawal_basis=basis,
                    withdrawn_at=utcnow(),
                )
                session.add(state)
            else:
                state.state = "restricted"
                state.withdrawal_basis = basis
                state.withdrawn_at = utcnow()
            if object_type == "learning":
                session.query(AgentLearning).filter(AgentLearning.scope_key == scoped,
                                                    AgentLearning.learning_key == object_key).update(
                    {AgentLearning.withdrawn_at: utcnow(), AgentLearning.status: "withdrawn"})
            elif object_type == "intelligence":
                session.query(AgentIntelligence).filter(
                    AgentIntelligence.scope_key == scoped,
                    AgentIntelligence.intelligence_key == object_key
                ).update({AgentIntelligence.withdrawn_at: utcnow(),
                          AgentIntelligence.lifecycle_status: "withdrawn"})
            session.commit()
            return {"object_type": object_type, "object_key": object_key, "state": state.state}

    def store_context_artifact(
        self, artifact_key: str, artifact_kind: str, client_id: int,
        security_domain: str, content: Any, provenance: Mapping[str, Any],
        retention_basis: str, project_code=None, expires_at=None,
    ) -> Dict[str, Any]:
        if artifact_kind not in ("cache", "embedding", "index", "evidence"):
            raise ValueError("unsupported context artifact kind")
        if security_domain == "SD2":
            raise SecurityError("secrets cannot enter context artifacts")
        if not retention_basis:
            raise SecurityError("retention basis required")
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            version = (session.query(func.max(AgentContextArtifact.version)).filter(
                AgentContextArtifact.scope_key == scoped,
                AgentContextArtifact.artifact_key == artifact_key
            ).scalar() or 0) + 1
            row = AgentContextArtifact(
                scope_key=scoped,
                artifact_key=artifact_key, version=version, artifact_kind=artifact_kind,
                client_id=client_id, project_code=project_code,
                security_domain=security_domain, content_json=dumps(content),
                provenance_json=dumps(provenance), retention_basis=retention_basis,
                expires_at=expires_at,
            )
            session.add(row)
            session.commit()
            return self._context_artifact_dict(row)

    def context_artifacts(
        self, artifact_kind: str, client_id: int, project_code=None,
        artifact_key: Optional[str] = None,
    ):
        now = utcnow()
        with SessionLocal() as session:
            query = session.query(AgentContextArtifact).filter(
                AgentContextArtifact.scope_key == scope_key(client_id, project_code),
                AgentContextArtifact.artifact_kind == artifact_kind,
                AgentContextArtifact.client_id == client_id,
                AgentContextArtifact.withdrawn_at == None,
                or_(AgentContextArtifact.expires_at == None, AgentContextArtifact.expires_at > now),
                AgentContextArtifact.project_code == project_code,
            )
            if artifact_key:
                query = query.filter(AgentContextArtifact.artifact_key == artifact_key)
            return [self._context_artifact_dict(row) for row in query.all()]

    @staticmethod
    def _context_artifact_dict(row):
        return {"id": row.id, "artifact_key": row.artifact_key,
                "version": row.version, "artifact_kind": row.artifact_kind,
                "client_id": row.client_id, "project_code": row.project_code,
                "security_domain": row.security_domain,
                "content": loads(row.content_json), "provenance": loads(row.provenance_json),
                "retention_basis": row.retention_basis, "expires_at": row.expires_at}

    def security_event(
        self, event_key: str, event_type: str, severity: str,
        security_domain: str, evidence: Mapping[str, Any],
        implicated_principal=None, client_id=None, project_code=None,
        containment=None,
    ) -> Dict[str, Any]:
        scoped = scope_key(client_id, project_code)
        with SessionLocal() as session:
            existing = session.query(AgentSecurityEvent).filter(
                AgentSecurityEvent.scope_key == scoped,
                AgentSecurityEvent.event_key == event_key
            ).first()
            if existing:
                return self._security_dict(existing)
            row = AgentSecurityEvent(
                scope_key=scoped,
                event_key=event_key, event_type=event_type, severity=severity,
                implicated_principal=implicated_principal, client_id=client_id,
                project_code=project_code, security_domain=security_domain,
                evidence_json=dumps(safe_evidence(evidence)),
                containment_json=dumps(containment or {}),
            )
            session.add(row)
            session.commit()
            return self._security_dict(row)

    def contain_security_event(self, event_key: str, actor, actions: Mapping[str, Any],
                               client_id=None, project_code=None):
        authority = self.require_actor(actor, "security.contain", client_id, project_code,
                                       "security.contain", "R4")
        actor_id = actor.principal_id if isinstance(actor, Principal) else str(actor)
        with SessionLocal() as session:
            row = session.query(AgentSecurityEvent).filter(
                AgentSecurityEvent.scope_key == scope_key(client_id, project_code),
                AgentSecurityEvent.event_key == event_key
            ).first()
            if row is None:
                raise ValueError("security event not found")
            if actor_id == row.implicated_principal:
                raise SecurityError("implicated principal cannot suppress or resolve event")
            row.containment_json = dumps(actions)
            row.status = "contained"
            session.commit()
            return self._security_dict(row)

    @staticmethod
    def _security_dict(row):
        return {"id": row.id, "event_key": row.event_key, "event_type": row.event_type,
                "severity": row.severity, "implicated_principal": row.implicated_principal,
                "client_id": row.client_id, "project_code": row.project_code,
                "security_domain": row.security_domain, "evidence": loads(row.evidence_json),
                "containment": loads(row.containment_json), "status": row.status}

    def audit_events(self, principal_client_id: Optional[int], platform_view=False):
        with SessionLocal() as session:
            query = session.query(AgentAuditEvent)
            if not platform_view:
                query = query.filter(AgentAuditEvent.client_id == principal_client_id)
            return [{"id": row.id, "event_type": row.event_type,
                     "principal_id": row.principal_id, "client_id": row.client_id,
                     "project_code": row.project_code, "capability_id": row.capability_id,
                     "configuration_ref": row.configuration_ref,
                     "evidence": loads(row.evidence_json), "decision": row.decision}
                    for row in query.order_by(AgentAuditEvent.id).all()]
