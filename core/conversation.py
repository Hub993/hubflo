"""Industry-neutral Core Conversation orchestration."""

import datetime as dt
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
            if candidate == "calendar_date":
                reference_date_text = str(
                    request.context.get("reference_date") or ""
                ).strip()
                try:
                    reference_date = dt.date.fromisoformat(reference_date_text)
                except (TypeError, ValueError):
                    return ConversationResult()

                text = (request.text or "").lower()

                iso_match = re.search(
                    r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b",
                    text,
                )
                if iso_match:
                    try:
                        target_date = dt.date(
                            int(iso_match.group(1)),
                            int(iso_match.group(2)),
                            int(iso_match.group(3)),
                        )
                    except ValueError:
                        return ConversationResult(
                            handled=True,
                            object_type="date",
                            metadata={"valid": False},
                        )

                    return ConversationResult(
                        handled=True,
                        object_type="date",
                        metadata={
                            "year": target_date.year,
                            "month": target_date.month,
                            "day": target_date.day,
                            "valid": True,
                        },
                    )

                numeric_match = re.search(
                    r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b",
                    text,
                )
                if numeric_match:
                    month = int(numeric_match.group(1))
                    day = int(numeric_match.group(2))
                    year_text = numeric_match.group(3)
                    year = (
                        int(year_text)
                        if year_text
                        else reference_date.year
                    )
                    if year < 100:
                        year += 2000
                    try:
                        target_date = dt.date(year, month, day)
                    except ValueError:
                        return ConversationResult(
                            handled=True,
                            object_type="date",
                            metadata={"valid": False},
                        )
                    if not year_text and target_date < reference_date:
                        target_date = dt.date(year + 1, month, day)

                    return ConversationResult(
                        handled=True,
                        object_type="date",
                        metadata={
                            "year": target_date.year,
                            "month": target_date.month,
                            "day": target_date.day,
                            "valid": True,
                        },
                    )

                month_numbers = {
                    "january": 1,
                    "february": 2,
                    "march": 3,
                    "april": 4,
                    "may": 5,
                    "june": 6,
                    "july": 7,
                    "august": 8,
                    "september": 9,
                    "october": 10,
                    "november": 11,
                    "december": 12,
                }
                month_names = "|".join(month_numbers)
                month_match = re.search(
                    rf"\b({month_names})\s+(\d{{1,2}})"
                    rf"(?:st|nd|rd|th)?(?:,?\s+(\d{{4}}))?\b",
                    text,
                )
                if month_match:
                    month = month_numbers[month_match.group(1)]
                    day = int(month_match.group(2))
                    year_text = month_match.group(3)
                    year = (
                        int(year_text)
                        if year_text
                        else reference_date.year
                    )
                    try:
                        target_date = dt.date(year, month, day)
                    except ValueError:
                        return ConversationResult(
                            handled=True,
                            object_type="date",
                            metadata={"valid": False},
                        )
                    if not year_text and target_date < reference_date:
                        target_date = dt.date(year + 1, month, day)

                    return ConversationResult(
                        handled=True,
                        object_type="date",
                        metadata={
                            "year": target_date.year,
                            "month": target_date.month,
                            "day": target_date.day,
                            "valid": True,
                        },
                    )

                if re.search(r"\btomorrow\b", text):
                    target_date = reference_date + dt.timedelta(days=1)
                elif re.search(r"\btoday\b", text):
                    target_date = reference_date
                else:
                    target_date = None

                if target_date is None:
                    weekday_names = (
                        "monday|tuesday|wednesday|thursday|friday|"
                        "saturday|sunday"
                    )
                    weekday_match = re.search(
                        rf"\b({weekday_names})\b",
                        text,
                    )
                    if weekday_match:
                        weekday = (
                            "monday",
                            "tuesday",
                            "wednesday",
                            "thursday",
                            "friday",
                            "saturday",
                            "sunday",
                        ).index(weekday_match.group(1))
                        days_ahead = (
                            weekday - reference_date.weekday()
                        ) % 7
                        target_date = reference_date + dt.timedelta(
                            days=days_ahead
                        )

                if target_date is None:
                    return ConversationResult()

                return ConversationResult(
                    handled=True,
                    object_type="date",
                    metadata={
                        "year": target_date.year,
                        "month": target_date.month,
                        "day": target_date.day,
                        "valid": True,
                    },
                )

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
