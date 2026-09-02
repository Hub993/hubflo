"""Provider-neutral reasoning adapters with strict structured validation."""

from typing import Dict, Iterable, Optional

from .contracts import (
    ProviderError,
    ProviderRequest,
    ProviderResult,
    ReasoningProvider,
    validate_structured,
)


class ProviderRegistry:
    def __init__(self):
        self._providers: Dict[str, ReasoningProvider] = {}

    def register(self, provider: ReasoningProvider) -> None:
        self._providers[provider.provider_id] = provider

    def resolve(self, provider_id: str) -> ReasoningProvider:
        provider = self._providers.get(provider_id)
        if provider is None:
            raise ProviderError("provider is unavailable")
        return provider

    def invoke(
        self,
        request: ProviderRequest,
        provider_ids: Iterable[str],
    ) -> ProviderResult:
        last_error = None
        for provider_id in provider_ids:
            try:
                provider = self.resolve(provider_id)
                result = provider.invoke(request)
                if result.provider_id != provider_id:
                    raise ProviderError("provider attribution mismatch")
                validate_structured(result.output, request.output_contract,
                                    request.optional_output_fields,
                                    optional_schema=request.optional_output_schema)
                return result
            except Exception as exc:
                last_error = exc
        raise ProviderError("no eligible provider succeeded: %s" % last_error)


class DeterministicProvider:
    """Controlled test/shadow provider; never treated as live model evidence."""

    def __init__(self, provider_id: str, provider_version: str, responder):
        self.provider_id = provider_id
        self.provider_version = provider_version
        self._responder = responder

    def invoke(self, request: ProviderRequest) -> ProviderResult:
        return ProviderResult(
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            output=self._responder(request),
        )
