"""Generic Core ↔ Industry Module contract."""

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class IndustryRequest:
    """Industry-neutral request passed across the Core ↔ Industry boundary."""

    capability: str
    text: str = ""
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IndustryResult:
    """Industry-neutral structured result returned to Core/application orchestration."""

    handled: bool = False
    classification: str = ""
    entities: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


class IndustryModule(Protocol):
    """Minimum contract implemented by an industry module."""

    name: str

    def interpret(self, request: IndustryRequest) -> IndustryResult:
        """Return industry-specific interpretation without executing feature lifecycles."""
        ...
