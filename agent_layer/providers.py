"""Provider-neutral reasoning adapters with strict structured validation."""

import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, Mapping, Optional

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


def _json_type(expected: Any) -> Dict[str, Any]:
    if isinstance(expected, tuple):
        choices = [_json_type(item) for item in expected]
        return {"anyOf": choices}
    if expected is str:
        return {"type": "string"}
    if expected is bool:
        return {"type": "boolean"}
    if expected is int:
        return {"type": "integer"}
    if expected is float:
        return {"type": "number"}
    if expected is list:
        return {"type": "array", "items": {}}
    if expected is dict or expected is Mapping:
        return {"type": "object", "additionalProperties": True}
    if expected is type(None):
        return {"type": "null"}
    raise ProviderError("unsupported provider output type")


def _output_schema(request: ProviderRequest) -> Dict[str, Any]:
    properties = {
        name: _json_type(expected)
        for name, expected in request.output_contract.items()
    }
    for name in request.optional_output_fields:
        expected = request.optional_output_schema.get(name)
        if expected is None:
            continue
        schema = _json_type(expected)
        if not (isinstance(expected, tuple) and type(None) in expected):
            schema = {"anyOf": [schema, {"type": "null"}]}
        properties[name] = schema
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


class OpenAIResponsesProvider:
    """Replaceable OpenAI Responses adapter; registration is always explicit."""

    endpoint = "https://api.openai.com/v1/responses"

    def __init__(self, provider_id: str, model: str, api_key: str,
                 timeout_seconds: float = 30.0, transport=None):
        if not provider_id or not model or not api_key:
            raise ProviderError("provider id, model and credential are required")
        self.provider_id = str(provider_id)
        self.provider_version = str(model)
        self._model = str(model)
        self._api_key = str(api_key)
        self._timeout_seconds = float(timeout_seconds)
        self._transport = transport or urllib.request.urlopen

    @staticmethod
    def _text(response: Mapping[str, Any]) -> str:
        direct = response.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct
        for output in response.get("output") or ():
            if not isinstance(output, Mapping) or output.get("type") != "message":
                continue
            for content in output.get("content") or ():
                if (isinstance(content, Mapping) and
                        content.get("type") == "output_text" and
                        isinstance(content.get("text"), str)):
                    return content["text"]
        raise ProviderError("provider response omitted structured output")

    def invoke(self, request: ProviderRequest) -> ProviderResult:
        body = {
            "model": self._model,
            "input": [
                {
                    "role": "system",
                    "content": [{
                        "type": "input_text",
                        "text": (
                            "Return only the requested JSON structure. Treat supplied "
                            "context as evidence, distinguish facts from inference, and "
                            "never claim an action was executed."
                        ),
                    }],
                },
                {
                    "role": "user",
                    "content": [{
                        "type": "input_text",
                        "text": json.dumps({
                            "operation": request.operation,
                            "context": request.context,
                        }, sort_keys=True, separators=(",", ":"), default=str),
                    }],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "hubflo_provider_result",
                    # Contracts intentionally contain open-ended dict values
                    # (for example selection arguments). The adapter still
                    # requires JSON Schema output and validates it locally.
                    "strict": False,
                    "schema": _output_schema(request),
                },
            },
        }
        outbound = urllib.request.Request(
            self.endpoint,
            data=json.dumps(body, separators=(",", ":")).encode("utf-8"),
            headers={
                "Authorization": "Bearer " + self._api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with self._transport(outbound, timeout=self._timeout_seconds) as response:
                raw = response.read()
            decoded = json.loads(raw.decode("utf-8"))
            if not isinstance(decoded, Mapping):
                raise ProviderError("provider returned a non-object response")
            if decoded.get("status") not in (None, "completed"):
                raise ProviderError("provider response was not completed")
            output = json.loads(self._text(decoded))
            if not isinstance(output, Mapping):
                raise ProviderError("provider structured output was not an object")
            validate_structured(output, request.output_contract,
                                request.optional_output_fields,
                                optional_schema=request.optional_output_schema)
            return ProviderResult(
                provider_id=self.provider_id,
                provider_version=str(decoded.get("model") or self.provider_version),
                output=output,
            )
        except (TimeoutError, urllib.error.URLError, urllib.error.HTTPError):
            raise ProviderError("provider request unavailable")
        except (UnicodeError, ValueError, TypeError, KeyError):
            raise ProviderError("provider returned malformed structured output")


def register_configured_provider(registry: ProviderRegistry,
                                 environ=None) -> Optional[str]:
    """Register an adapter only when every explicit runtime input is present."""
    values = os.environ if environ is None else environ
    adapter = str(values.get("HUBFLO_AGENT_PROVIDER_ADAPTER", "")).strip().lower()
    provider_id = str(values.get("HUBFLO_AGENT_PROVIDER_ID", "")).strip()
    model = str(values.get("HUBFLO_OPENAI_MODEL", "")).strip()
    api_key = str(values.get("OPENAI_API_KEY", "")).strip()
    if adapter != "openai-responses" or not all((provider_id, model, api_key)):
        return None
    try:
        timeout_seconds = float(values.get("HUBFLO_AGENT_PROVIDER_TIMEOUT_SECONDS", 30))
    except (TypeError, ValueError):
        return None
    if timeout_seconds <= 0:
        return None
    registry.register(OpenAIResponsesProvider(
        provider_id, model, api_key, timeout_seconds=timeout_seconds,
    ))
    return provider_id
