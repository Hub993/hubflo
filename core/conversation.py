"""Industry-neutral Core Conversation orchestration."""

import re
from dataclasses import dataclass, field
from typing import Any

from core.industry import IndustryModule, IndustryRequest, IndustryResult


@dataclass(frozen=True)
class ConversationRequest:
    """Industry-neutral request for Core-native conversation interpretation."""

    capability: str
    text: str = ""
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConversationResult:
    """Structured result from Core-native conversation interpretation."""

    handled: bool = False
    intent: str = ""
    action: str = ""
    object_type: str = ""
    entities: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class CoreConversation:
    """Delegate structured interpretation through an injected Industry Module."""

    def __init__(self, industry: IndustryModule):
        self._industry = industry

    def interpret(self, request: IndustryRequest) -> IndustryResult:
        """Return the structured Industry Module interpretation unchanged."""
        return self._industry.interpret(request)

    def interpret_core(self, request: ConversationRequest) -> ConversationResult:
        """Interpret supported industry-neutral Core conversation candidates."""
        if request.capability != "core_recognition":
            return ConversationResult()

        candidate = str(
            request.context.get("candidate") or ""
        ).strip().lower()

        if candidate != "pm_reminder_creation":
            return ConversationResult()

        text = (request.text or "").lower().strip()
        if not text:
            return ConversationResult()

        handled = bool(
            re.search(r"\bremind\s+me\b", text)
            or re.search(r"\bset\s+(?:a\s+)?reminder\b", text)
        )

        if not handled:
            return ConversationResult()

        return ConversationResult(
            handled=True,
            intent="reminder",
            action="create",
            object_type="reminder",
        )
