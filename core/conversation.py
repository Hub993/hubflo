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

        if request.capability == "routing_arbitration":
            if candidate not in (
                "await_vs_normal_route",
                "pending_state_vs_normal_route",
            ):
                return ConversationResult()

            deterministic_recognition = (
                request.context.get("deterministic_recognition") is True
            )
            pending_reply_valid = (
                request.context.get("pending_reply_valid") is True
            )
            pending_reply_invalid = (
                request.context.get("pending_reply_invalid") is True
            )

            if deterministic_recognition and pending_reply_valid:
                action = "pending_await"
            elif deterministic_recognition:
                action = "normal_route"
            elif pending_reply_valid and pending_reply_invalid:
                action = "pending_await"
            elif pending_reply_valid:
                action = "resume_pending"
            else:
                action = "pending_await"

            return ConversationResult(
                handled=True,
                action=action,
                object_type="routing",
                metadata={
                    "bypass_pending_await": action == "normal_route",
                    "resume_pending": action == "resume_pending",
                    "preserve_pending": action != "resume_pending",
                },
            )

        if request.capability == "record_resolution":
            if candidate != "text_reference":
                return ConversationResult()

            records = request.context.get("records")
            if not isinstance(records, (list, tuple)):
                return ConversationResult()

            stop_words = {
                "a", "an", "and", "are", "as", "at", "be", "because",
                "been", "being", "by", "for", "from", "had", "has",
                "have", "in", "is", "it", "of", "on", "or", "the",
                "to", "was", "were", "with",
            }

            def normalized_tokens(value: Any) -> list[str]:
                tokens = re.findall(
                    r"[a-z0-9]+",
                    str(value or "").lower().replace("-", " "),
                )
                return [
                    token
                    for token in tokens
                    if token not in stop_words and not token.isdigit()
                ]

            def contains_sequence(
                haystack: list[str],
                needle_tokens: list[str],
            ) -> bool:
                if not needle_tokens or len(needle_tokens) > len(haystack):
                    return False
                width = len(needle_tokens)
                return any(
                    haystack[index:index + width] == needle_tokens
                    for index in range(len(haystack) - width + 1)
                )

            def longest_shared_sequence(
                left: list[str],
                right: list[str],
            ) -> int:
                longest = 0
                for left_index in range(len(left)):
                    for right_index in range(len(right)):
                        length = 0
                        while (
                            left_index + length < len(left)
                            and right_index + length < len(right)
                            and left[left_index + length]
                            == right[right_index + length]
                        ):
                            length += 1
                        longest = max(longest, length)
                return longest

            request_text = str(request.text or "")
            text_tokens = normalized_tokens(request_text)
            if not text_tokens:
                if (
                    request.context.get("resolve_single_unqualified") is True
                ):
                    valid_records = [
                        record
                        for record in records
                        if isinstance(record, dict)
                        and record.get("id") is not None
                        and (
                            record.get("label")
                            or record.get("labels")
                        )
                    ]
                    if len(valid_records) == 1:
                        resolved = valid_records[0]
                        return ConversationResult(
                            handled=True,
                            object_type="record",
                            entities={"record_id": resolved.get("id")},
                            metadata={
                                "resolution": "resolved",
                                "match_count": 1,
                                "unqualified_single": True,
                            },
                        )

                return ConversationResult(
                    handled=True,
                    object_type="record",
                    metadata={"resolution": "not_found", "matches": []},
                )

            leading_reference_match = re.match(
                r"^\s*(.+?)\s+"
                r"\b(?:is|are|was|were|has|have|had)\b",
                request_text,
                flags=re.IGNORECASE,
            )
            reference_tokens = (
                normalized_tokens(leading_reference_match.group(1))
                if leading_reference_match
                else []
            )

            clear_matches = []
            for record in records:
                if not isinstance(record, dict):
                    continue

                record_id = record.get("id")
                labels = record.get("labels")
                if labels is None:
                    labels = [record.get("label")]
                elif isinstance(labels, str):
                    labels = [labels]
                elif not isinstance(labels, (list, tuple)):
                    continue

                clear_match = False
                for label in labels:
                    label_tokens = normalized_tokens(label)
                    if not label_tokens:
                        continue

                    if reference_tokens:
                        clear_match = contains_sequence(
                            label_tokens,
                            reference_tokens,
                        )
                    else:
                        shared_length = longest_shared_sequence(
                            text_tokens,
                            label_tokens,
                        )
                        clear_match = (
                            shared_length >= 2
                            or (
                                shared_length == 1
                                and len(text_tokens) == 1
                            )
                        )

                    if clear_match:
                        break

                if clear_match:
                    clear_matches.append(
                        {
                            "id": record_id,
                            "label": str(record.get("label") or ""),
                        }
                    )

            if not clear_matches:
                return ConversationResult(
                    handled=True,
                    object_type="record",
                    metadata={"resolution": "not_found", "matches": []},
                )

            if len(clear_matches) == 1:
                resolved = clear_matches[0]
                return ConversationResult(
                    handled=True,
                    object_type="record",
                    entities={"record_id": resolved["id"]},
                    metadata={
                        "resolution": "resolved",
                        "match_count": 1,
                    },
                )

            return ConversationResult(
                handled=True,
                object_type="record",
                metadata={
                    "resolution": "ambiguous",
                    "matches": clear_matches,
                },
            )

        if request.capability == "recurrence":
            if candidate != "schedule_recurrence":
                return ConversationResult()

            text = (request.text or "").lower().strip()
            recurrence_rule = "none"
            recurrence_interval = 1
            recurrence_seconds = None
            recurrence_weekday = None

            every_n_match = re.search(
                r"\bevery\s+(\d+)\s+"
                r"(minutes?|hours?|days?|weeks?|months?)\b",
                text,
            )
            if every_n_match:
                recurrence_interval = max(1, int(every_n_match.group(1)))
                unit = every_n_match.group(2)
                if unit.startswith("minute"):
                    recurrence_rule = "interval"
                    recurrence_seconds = recurrence_interval * 60
                elif unit.startswith("hour"):
                    recurrence_rule = "interval"
                    recurrence_seconds = recurrence_interval * 3600
                elif unit.startswith("day"):
                    recurrence_rule = "daily"
                elif unit.startswith("week"):
                    recurrence_rule = "weekly"
                else:
                    recurrence_rule = "monthly"

            weekday_recurrence_match = re.search(
                r"\bevery\s+"
                r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
                text,
            )
            if weekday_recurrence_match:
                recurrence_rule = "weekly"
                recurrence_weekday = weekday_recurrence_match.group(1)

            if re.search(r"\bevery\s+weekday\b", text):
                recurrence_rule = "weekdays"
            elif re.search(r"\b(?:every\s+day|daily)\b", text):
                recurrence_rule = "daily"
            elif re.search(r"\b(?:every\s+week|weekly)\b", text):
                recurrence_rule = "weekly"
            elif re.search(r"\b(?:every\s+month|monthly)\b", text):
                recurrence_rule = "monthly"
            elif re.search(r"\b(?:every\s+hour|hourly)\b", text):
                recurrence_rule = "hourly"
                recurrence_seconds = 3600

            if recurrence_rule == "none":
                return ConversationResult()

            return ConversationResult(
                handled=True,
                object_type="recurrence",
                metadata={
                    "recurrence_rule": recurrence_rule,
                    "recurrence_interval": recurrence_interval,
                    "recurrence_seconds": recurrence_seconds,
                    "weekday": recurrence_weekday,
                },
            )

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

                def calendar_date_metadata(
                    match,
                    target_date=None,
                    valid: bool = True,
                ) -> dict[str, Any]:
                    match_start, match_end = match.span()
                    on_prefix = re.search(
                        r"\bon\s+$",
                        text[:match_start],
                    )
                    if on_prefix:
                        match_start = on_prefix.start()

                    metadata = {
                        "valid": valid,
                        "match_start": match_start,
                        "match_end": match_end,
                    }
                    if target_date is not None:
                        metadata.update(
                            {
                                "year": target_date.year,
                                "month": target_date.month,
                                "day": target_date.day,
                            }
                        )
                    return metadata

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
                            metadata=calendar_date_metadata(
                                iso_match,
                                valid=False,
                            ),
                        )

                    return ConversationResult(
                        handled=True,
                        object_type="date",
                        metadata=calendar_date_metadata(
                            iso_match,
                            target_date=target_date,
                        ),
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
                            metadata=calendar_date_metadata(
                                numeric_match,
                                valid=False,
                            ),
                        )
                    if not year_text and target_date < reference_date:
                        target_date = dt.date(year + 1, month, day)

                    return ConversationResult(
                        handled=True,
                        object_type="date",
                        metadata=calendar_date_metadata(
                            numeric_match,
                            target_date=target_date,
                        ),
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
                month_date_year_separator = str(
                    request.context.get("month_date_year_separator") or ""
                ).strip().lower()
                if month_date_year_separator == "optional":
                    month_pattern = (
                        rf"\b({month_names})\s+(\d{{1,2}})"
                        rf"(?:st|nd|rd|th)?"
                        rf"(?:,\s*|\s+)?(\d{{4}})?\b"
                    )
                else:
                    month_pattern = (
                        rf"\b({month_names})\s+(\d{{1,2}})"
                        rf"(?:st|nd|rd|th)?(?:,?\s+(\d{{4}}))?\b"
                    )
                month_match = re.search(
                    month_pattern,
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
                            metadata=calendar_date_metadata(
                                month_match,
                                valid=False,
                            ),
                        )
                    if not year_text and target_date < reference_date:
                        target_date = dt.date(year + 1, month, day)

                    return ConversationResult(
                        handled=True,
                        object_type="date",
                        metadata=calendar_date_metadata(
                            month_match,
                            target_date=target_date,
                        ),
                    )

                relative_date_selection = str(
                    request.context.get("relative_date_selection") or ""
                ).strip().lower()
                if relative_date_selection == "first_textual":
                    date_match = re.search(
                        r"\b(today|tomorrow)\b",
                        text,
                    )
                    if date_match and date_match.group(1) == "today":
                        target_date = reference_date
                    elif date_match:
                        target_date = reference_date + dt.timedelta(days=1)
                    else:
                        target_date = None
                else:
                    date_match = re.search(r"\btomorrow\b", text)
                    if date_match:
                        target_date = reference_date + dt.timedelta(days=1)
                    else:
                        date_match = re.search(r"\btoday\b", text)
                        target_date = reference_date if date_match else None

                if target_date is None:
                    weekday_names = (
                        "monday|tuesday|wednesday|thursday|friday|"
                        "saturday|sunday"
                    )
                    date_match = re.search(
                        rf"\b({weekday_names})\b",
                        text,
                    )
                    if date_match:
                        weekday = (
                            "monday",
                            "tuesday",
                            "wednesday",
                            "thursday",
                            "friday",
                            "saturday",
                            "sunday",
                        ).index(date_match.group(1))
                        days_ahead = (
                            weekday - reference_date.weekday()
                        ) % 7
                        target_date = reference_date + dt.timedelta(
                            days=days_ahead
                        )

                if target_date is None or date_match is None:
                    return ConversationResult()

                return ConversationResult(
                    handled=True,
                    object_type="date",
                    metadata=calendar_date_metadata(
                        date_match,
                        target_date=target_date,
                    ),
                )

            if candidate == "relative_duration":
                text = (request.text or "").lower()
                match = re.search(
                    r"\bin\s+(\d+)\s+(minutes?|hours?|days?|weeks?)\b",
                    text,
                )
                if not match:
                    return ConversationResult()

                amount = int(match.group(1))
                unit_text = match.group(2)
                if unit_text.startswith("minute"):
                    unit = "minute"
                elif unit_text.startswith("hour"):
                    unit = "hour"
                elif unit_text.startswith("day"):
                    unit = "day"
                else:
                    unit = "week"

                return ConversationResult(
                    handled=True,
                    object_type="duration",
                    metadata={
                        "amount": amount,
                        "unit": unit,
                        "valid": amount > 0,
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
