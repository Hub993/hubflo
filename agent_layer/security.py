"""Deterministic information controls and security observability helpers."""

import re
from typing import Any, Dict, Iterable, List, Mapping

from .contracts import Principal, ProtectedItem, SecurityError


_SECRET_KEY = re.compile(
    r"(?:secret|password|passwd|api[_-]?key|access[_-]?token|private[_-]?key|credential)",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|sk-[A-Za-z0-9_-]{16,}|Bearer\s+[A-Za-z0-9._-]{12,})",
    re.IGNORECASE,
)


def contains_secret(value: Any, path: str = "") -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = "%s.%s" % (path, key) if path else str(key)
            normalized_key = str(key).lower()
            opaque_reference = normalized_key.endswith(("_ref", "_reference", "_handle"))
            if (_SECRET_KEY.search(str(key)) and not opaque_reference) or contains_secret(child, child_path):
                return True
        return False
    if isinstance(value, (list, tuple, set)):
        return any(contains_secret(child, path) for child in value)
    return bool(_SECRET_VALUE.search(str(value)))


def safe_evidence(value: Any) -> Any:
    """Produce audit evidence without copying secrets or uncontrolled payloads."""
    if contains_secret(value):
        return {"redacted": True, "reason": "secret-pattern-detected"}
    if isinstance(value, Mapping):
        return {
            str(key): safe_evidence(child)
            for key, child in value.items()
            if not _SECRET_KEY.search(str(key))
        }
    if isinstance(value, (list, tuple)):
        return [safe_evidence(child) for child in value]
    if isinstance(value, str) and len(value) > 1000:
        return value[:1000] + "...[truncated]"
    return value


class ContextAssembler:
    """Assemble minimum provider/reasoning context after scope enforcement."""

    def assemble(
        self,
        principal: Principal,
        items: Iterable[ProtectedItem],
        permitted_domains: Iterable[str],
        purpose: str,
        for_provider: bool = False,
        provider_policy: Mapping[str, Any] = None,
    ) -> Dict[str, Any]:
        domains = set(permitted_domains)
        result = {}
        for item in items:
            if item.security_domain == "SD2" or contains_secret(item.value):
                raise SecurityError("credentials/secrets cannot enter reasoning context")
            if item.security_domain not in domains:
                raise SecurityError("security-domain access denied")
            if item.client_id is not None and item.client_id != principal.scope.client_id:
                raise SecurityError("cross-client context denied")
            if (
                item.project_code is not None
                and principal.scope.project_code is not None
                and item.project_code != principal.scope.project_code
            ):
                raise SecurityError("cross-project context denied")
            if purpose not in item.permitted_uses:
                raise SecurityError("purpose not permitted")
            if for_provider:
                if not item.provider_eligible:
                    raise SecurityError("provider use not permitted")
                if not provider_policy:
                    raise SecurityError("effective provider policy required")
                if item.classification in (None, "", "unknown", "unclassified"):
                    raise SecurityError("provider data classification required")
                if not item.provenance:
                    raise SecurityError("provider data provenance required")
                if item.security_domain not in set(provider_policy.get("allowed_data_classes", [])):
                    raise SecurityError("provider data class denied")
                if item.confidentiality not in set(provider_policy.get("allowed_confidentiality", [])):
                    raise SecurityError("provider confidentiality denied")
                if purpose not in set(provider_policy.get("permitted_uses", [])):
                    raise SecurityError("provider purpose denied")
                controls = set(provider_policy.get("access_controls", []))
                if not set(item.access_requirements).issubset(controls):
                    raise SecurityError("provider access controls insufficient")
                regions = set(provider_policy.get("allowed_regions", []))
                if regions and (not item.region or item.region not in regions):
                    raise SecurityError("provider location denied or unresolved")
                maximum = provider_policy.get("retention_max_seconds")
                if item.retention_max_seconds is not None and (
                    maximum is None or item.retention_max_seconds > maximum
                ):
                    raise SecurityError("provider retention constraint insufficient")
                if item.deletion_required and not provider_policy.get("deletion_supported"):
                    raise SecurityError("provider deletion requirement unsupported")
                if item.withdrawal_required and not provider_policy.get("withdrawal_supported"):
                    raise SecurityError("provider withdrawal requirement unsupported")
                if not set(item.distribution_uses).issubset(
                    set(provider_policy.get("allowed_distribution_uses", []))
                ):
                    raise SecurityError("provider distribution/use constraint denied")
                if provider_policy.get("attribution_required") and not (
                    item.provenance.get("source_ref") or item.provenance.get("source")
                ):
                    raise SecurityError("provider attribution requirement unresolved")
            result[item.reference] = item.value
        return result


def derive_composed_scope(principal: Principal, components: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Intersection-like constraint propagation for a new composition."""
    client_id = principal.scope.client_id
    project_code = principal.scope.project_code
    domains = None
    max_autonomy = 5
    distributable = True
    provider_eligible = True
    for component in components:
        component_client = component.get("client_id")
        component_project = component.get("project_code")
        if component_client is not None and component_client != client_id:
            raise SecurityError("composition crosses client boundary")
        if project_code is not None and component_project not in (None, project_code):
            raise SecurityError("composition crosses project boundary")
        component_domains = set(component.get("domains") or [])
        domains = component_domains if domains is None else domains.intersection(component_domains)
        max_autonomy = min(max_autonomy, int(component.get("max_autonomy", 2)))
        distributable = distributable and bool(component.get("distributable", False))
        provider_eligible = provider_eligible and bool(component.get("provider_eligible", False))
    return {
        "client_id": client_id,
        "project_code": project_code,
        "domains": sorted(domains or []),
        "max_autonomy": max_autonomy,
        "distributable": distributable,
        "provider_eligible": provider_eligible,
    }
