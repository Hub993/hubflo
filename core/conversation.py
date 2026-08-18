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
        candidate = str(
            request.context.get("candidate") or ""
        ).strip().lower()

        if request.capability == "shared_datetime":
            if candidate != "time_of_day":
                return ConversationResult()

            text = (request.text or "").lower()

            if re.search(r"\bat\s+noon\b", text):
                hour, minute = 12, 0
            elif re.search(r"\bat\s+midnight\b", text):
                hour, minute = 0, 0
            else:
                match = re.search(
                    r"\bat\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b",
                    text,
                )
                if not match:
                    return ConversationResult()

                hour = int(match.group(1))
                minute = int(match.group(2) or 0)
                meridiem = match.group(3)

                if minute > 59:
                    return ConversationResult()

                if meridiem:
                    if hour < 1 or hour > 12:
                        return ConversationResult()
                    if meridiem == "am":
                        hour = 0 if hour == 12 else hour
                    else:
                        hour = 12 if hour == 12 else hour + 12
                elif hour > 23:
                    return ConversationResult()

            return ConversationResult(
                handled=True,
                object_type="time",
                metadata={"hour": hour, "minute": minute},
            )

        if request.capability != "core_recognition":
            return ConversationResult()

        if candidate == "pm_reminder_lifecycle":
            text = (request.text or "").lower().strip()
            if not text:
                return ConversationResult()

            # Reminder creation takes precedence over lifecycle recognition.
            if re.match(
                r"^(?:please\s+)?(?:remind\s+me|set\s+(?:a\s+)?reminder)\b",
                text,
            ):
                return ConversationResult()

            action = ""

            if text in ("ok", "okay", "ack", "acknowledge"):
                action = "acknowledge"
            elif re.match(
                r"^(?:please\s+)?(?:ok|okay|ack|acknowledge)\b.*\breminder\b",
                text,
            ):
                action = "acknowledge"
            elif re.match(
                r"^(?:please\s+)?(?:snooze|postpone)\b.*\breminder\b",
                text,
            ):
                action = "snooze"
            elif re.match(
                r"^(?:please\s+)?(?:redirect|reassign)\b.*\breminder\b",
                text,
            ):
                action = "redirect"
            elif re.match(
                r"^(?:please\s+)?cancel\b.*\breminder\b",
                text,
            ):
                action = "cancel"

            if not action:
                return ConversationResult()

            return ConversationResult(
                handled=True,
                intent="reminder",
                action=action,
                object_type="reminder",
            )

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
