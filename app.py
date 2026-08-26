# app_v6_1.py — Hubflo V6.1 working
# ---------------------------------------------------------------
# Rebuilt from v5 base with all verified post-V5 improvements:
# - Order-step checklist
# - Task subtype detection (assigned/self)
# - Daily digest scaffolds (6 AM subs, 6 PM PMs)
# - Change-order cost/time impact fields
# - Stock / material tracking
# ---------------------------------------------------------------

import os, json, logging, datetime as dt, requests, hashlib
from typing import Optional
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, Response

from core.conversation import ConversationRequest, CoreConversation
from core.industry import IndustryRequest
from industries.construction import ConstructionIndustryModule
from storage_v6_1 import (
    init_db, create_task, get_tasks, get_summary,
    mark_done, approve_task, reject_task, set_order_state,
    revoke_last, subcontractor_accuracy,
    create_meeting, start_meeting, close_meeting,
    create_stock_item, adjust_stock, get_stock_report,
    record_change_order,
    add_task_to_group, get_group_children, edit_task_text,
    get_all_change_orders, create_call_reminder,
    create_inspection, log_delay,
    # >>> FEATURE_3_REMINDER_IMPORTS_START — REMINDER FRAMEWORK V6.1 <<<
    PMReminder, create_pm_reminder, claim_due_pm_reminders,
    complete_pm_reminder_delivery, fail_pm_reminder_delivery,
    acknowledge_pm_reminder, snooze_pm_reminder,
    redirect_pm_reminder, cancel_pm_reminder,
    # >>> FEATURE_3_REMINDER_IMPORTS_END <<<
)

from storage_v6_1 import Task

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("hubflo")

_CONSTRUCTION_INDUSTRY = ConstructionIndustryModule()
_CORE_CONVERSATION = CoreConversation(_CONSTRUCTION_INDUSTRY)

# ---------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------
ADMIN_TOKEN = os.environ.get("HUBFLO_ADMIN_TOKEN", "").strip()
D360_KEY = (
    os.environ.get("DIALOG360_API_KEY")
    or os.environ.get("Dialog360_API_Key")
    or os.environ.get("D360_KEY")
    or os.environ.get("D360_key")
    or ""
).strip()
DEFAULT_PHONE_ID = os.environ.get("BOUND_NUMBER", "").strip()
WHATSAPP_BASE = os.environ.get("D360_SEND_URL","").strip()

ORDER_LIFECYCLE_STATES = [
    "quoted","pending_approval","approved",
    "cancelled","invoiced","enacted"
]

_PHASE_DIGEST_TOGGLE = {}

# ---------------------------------------------------------------------
# Boot DB
# ---------------------------------------------------------------------
init_db()


# ============================================================
# HUBFLO INTEGRITY PATCH — CANONICAL HEARTBEAT (v6 unified)
# ============================================================
from sqlalchemy import text
from storage_v6_1 import (
    SessionLocal, hygiene_pin, hygiene_guard, SystemState
)

@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    """Canonical heartbeat — DB check + hygiene tether."""
    try:
        with SessionLocal() as s:
            s.execute(text("SELECT 1"))
        db_state = "ok"
    except Exception as e:
        db_state = f"fail:{str(e)[:80]}"

    # record hygiene pin and check staleness
    hygiene_pin()
    ok, note = hygiene_guard()

    return jsonify({
        "db": db_state,
        "hygiene_ok": ok,
        "note": note,
        "utc": dt.datetime.utcnow().isoformat() + "Z"
    }), 200

@app.route("/integrity/status", methods=["GET"])
def integrity_status():
    """Report redmode + hygiene info for external tether."""
    with SessionLocal() as s:
        ss = s.query(SystemState).first()
        return jsonify({
            "redmode": bool(ss.redmode) if ss else None,
            "redmode_reason": ss.redmode_reason if ss else None,
            "hygiene_last_utc": ss.hygiene_last_utc if ss else None
        }), 200
# ============================================================

# >>> PATCH_CLASSIFIER_V6_1_START — NATURAL LANGUAGE REBUILD (V6.1-REV2) <<<

import re

def classify_message(text: str) -> dict:
    """
    Natural-language classifier restored to V6.1-REV2 behaviour.
    No hashtags, no rigid keywords, free-flow chat only.
    Returns:
        { "tag": "...", "subtype": "...", "order_state": "..." }
    """

    global SENDER_GLOBAL
    t = (text or "").lower().strip()

    # -----------------------------
    # EXPLICIT "NOT AN ORDER" / UPDATE GUARD
    # -----------------------------
    # e.g. "This is just an update not an order"
    if "not an order" in t or "just an update" in t:
        if t.startswith("i will") or t.startswith("i'm going to"):
            return {"tag": "task", "subtype": "self", "order_state": None}
        return {"tag": "task", "subtype": "assigned", "order_state": None}

    # -----------------------------
    # CHANGE ORDER (requires an existing open order)
    # -----------------------------
    if (
        "change the order" in t
        or "change that order" in t
        or "change order" in t
        or "change it to" in t
        or "change it" in t
    ):
        open_order = None
        try:
            from storage_v6_1 import SessionLocal, Task
            with SessionLocal() as s:
                open_order = (
                    s.query(Task)
                    .filter(
                        Task.sender == SENDER_GLOBAL,
                        Task.status == "open",
                        Task.tag == "order"
                    )
                    .order_by(Task.id.desc())
                    .first()
                )
        except Exception:
            open_order = None

        if open_order:
            return {
                "tag": "change",
                "subtype": "assigned",
                "order_state": "change_requested"
            }
        else:
            # No existing order → treat as a normal task
            return {
                "tag": "task",
                "subtype": "assigned",
                "order_state": None
            }

    # -----------------------------
    # APPROVE / REJECT (for an order)
    # -----------------------------
    if "approve" in t:
        return {"tag": "task", "subtype": "assigned", "order_state": "approve"}

    if "reject" in t:
        return {"tag": "task", "subtype": "assigned", "order_state": "reject"}

    # -----------------------------
    # ORDER DETECTION (free-language)
    # -----------------------------
    order_phrases = [
        r"\bget me\b",
        r"\bgrab\b",
        r"\border\b",
        r"\bwe need\b",
        r"\bbring\b",
        r"\bdrop\b",
        r"\bdeliver\b",
        r"\bsupplier\b",
        r"\bquantity\b",
        r"\bdelivery\b",
        r"\bdrop location\b",
    ]
    if any(re.search(p, t) for p in order_phrases):
        return {
            "tag": "order",
            "subtype": "assigned",
            "order_state": "requested",
        }

    # -----------------------------
    # URGENT
    # -----------------------------
    if "urgent" in t or "asap" in t:
        return {"tag": "urgent", "subtype": "assigned", "order_state": None}

    # -----------------------------
    # DEFAULT = TASK
    # Self-tasks when "I will / I'm going to"
    # -----------------------------
    if t.startswith("i will") or t.startswith("i'm going to"):
        return {"tag": "task", "subtype": "self", "order_state": None}

    return {"tag": "task", "subtype": "assigned", "order_state": None}

# >>> PATCH_CLASSIFIER_V6_1_END <<<

# >>> PATCH_1_INSPECTION_CLASSIFIER_START — INSPECTOR SCHEDULING V6.1 <<<

_INSPECTION_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

_INSPECTION_MONTHS = {
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


def classify_inspection(text: str) -> bool:
    result = _CORE_CONVERSATION.interpret(
        IndustryRequest(
            capability="domain_recognition",
            text=text or "",
            context={"candidate": "inspection"},
        )
    )

    return bool(
        result.handled
        and result.classification == "inspection"
    )


def _inspection_datetime_from_date(
    date_value: dt.date,
) -> dt.datetime:
    """
    Store the requested inspection calendar date at midnight.

    This is the requested inspection date, not the time at which
    the WhatsApp message was received.
    """
    return dt.datetime.combine(
        date_value,
        dt.time.min,
    )


def _next_inspection_weekday(
    weekday_name: str,
    today: dt.date,
) -> dt.date:
    """
    Resolve the next occurrence of a named weekday.

    If the requested weekday is today, today's date is used.
    """
    target_weekday = _INSPECTION_WEEKDAYS[weekday_name]
    days_ahead = (
        target_weekday - today.weekday()
    ) % 7

    return today + dt.timedelta(days=days_ahead)


def _inspection_reference_date(
    timezone_name: str,
) -> dt.date:
    """
    Return the current calendar date in the sender's timezone.

    America/New_York is used when the supplied timezone is missing
    or invalid.
    """
    safe_timezone_name = (
        timezone_name or "America/New_York"
    )

    try:
        sender_timezone = ZoneInfo(safe_timezone_name)
    except Exception:
        sender_timezone = ZoneInfo(
            "America/New_York"
        )

    return dt.datetime.now(
        sender_timezone
    ).date()


def parse_inspection_request(
    text: str,
    today: Optional[dt.date] = None,
    timezone_name: str = "America/New_York",
    date_order: Optional[str] = "month_first",
) -> Optional[dict]:
    """
    Parse inspection scheduling messages such as:

      Schedule inspection for slab on Friday
      Book inspection for framing tomorrow
      Schedule inspection for electrical rough-in on 08/14/2026
      Schedule inspection for plumbing on August 14
      Schedule inspection for final on 2026-08-14

    Returns:

      {
          "phase": "slab",
          "required_date": datetime(...)
      }

    Returns None when either the inspection phase or the requested
    date cannot be parsed.
    """
    raw = (text or "").strip()
    if not raw:
        return None

    working = raw.lower().strip()

    if today is None:
        today = _inspection_reference_date(
            timezone_name
        )

    command_match = re.match(
        r"^\s*(?:schedule|book)\s+"
        r"(?:an?\s+)?inspection\s+for\s+",
        working,
        flags=re.IGNORECASE,
    )

    if not command_match:
        return None

    body = working[
        command_match.end():
    ].strip()

    if not body:
        return None

    requested_date = None
    date_span = None

    try:
        calendar_date_result = _CORE_CONVERSATION.interpret_core(
            ConversationRequest(
                capability="shared_datetime",
                text=body,
                context={
                    "candidate": "calendar_date",
                    "reference_date": today.isoformat(),
                    "date_order": date_order,
                    "relative_date_selection": "first_textual",
                    "month_date_year_separator": "optional",
                },
            )
        )
    except ValueError:
        return None

    if calendar_date_result.handled:
        metadata = calendar_date_result.metadata
        if not metadata.get("valid"):
            return None

        try:
            requested_date = dt.date(
                int(metadata["year"]),
                int(metadata["month"]),
                int(metadata["day"]),
            )
            date_span = (
                int(metadata["match_start"]),
                int(metadata["match_end"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    if (
        requested_date is None
        or date_span is None
    ):
        return None

    # Everything before the detected date expression
    # is treated as the inspection phase.
    phase = body[
        :date_span[0]
    ].strip()

    phase = re.sub(
        r"\s+\bon\s*$",
        "",
        phase,
    ).strip()

    phase = phase.rstrip(" ,.-")

    if not phase:
        return None

    return {
        "phase": phase,
        "required_date": (
            _inspection_datetime_from_date(
                requested_date
            )
        ),
    }

# >>> PATCH_1_INSPECTION_CLASSIFIER_END <<<

# >>> PATCH_2_DELAY_CLASSIFIER_START — CRITICAL-PATH DELAY TRACKING V6.1 <<<

def classify_delay(text: str) -> bool:
    result = _CORE_CONVERSATION.interpret(
        IndustryRequest(
            capability="domain_recognition",
            text=text or "",
            context={"candidate": "critical_path_delay"},
        )
    )

    return bool(
        result.handled
        and result.classification == "critical_path_delay"
    )

def _resolve_delay_task_reference(
    text: str,
    project_code: Optional[str],
    client_id: Optional[int] = None,
) -> dict:
    project = (
        str(project_code).strip()
        if project_code is not None
        else ""
    )
    if not project:
        return {"status": "project_missing"}

    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .filter(
                Task.project_code == project,
                Task.status == "open",
            )
            .order_by(Task.id.desc())
            .all()
        )
        if client_id is not None:
            rows = [
                row for row in rows
                if int(row.client_id or 1) == int(client_id)
            ]

    records = []
    for row in rows:
        label = (row.text or "").strip()
        if not label or label.lower().startswith("[await:"):
            continue
        records.append(
            {
                "id": row.id,
                "label": label,
                "labels": [label],
            }
        )

    result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="record_resolution",
            text=text or "",
            context={
                "candidate": "text_reference",
                "records": records,
            },
        )
    )

    resolution = str(
        result.metadata.get("resolution") or ""
    ).strip().lower()

    if resolution == "resolved":
        try:
            return {
                "status": "resolved",
                "task_id": int(result.entities["record_id"]),
            }
        except (KeyError, TypeError, ValueError):
            return {"status": "not_found"}

    if resolution == "ambiguous":
        matches = result.metadata.get("matches")
        return {
            "status": "ambiguous",
            "matches": matches if isinstance(matches, list) else [],
        }

    return {"status": "not_found"}

# >>> PATCH_2_DELAY_CLASSIFIER_END <<<


# >>> FEATURE_3_REMINDER_CLASSIFIER_START — REMINDER FRAMEWORK COMPLETION V6.1 <<<

_REMINDER_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

_REMINDER_MONTHS = {
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


def classify_pm_reminder(text: str) -> bool:
    result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="core_recognition",
            text=text or "",
            context={"candidate": "pm_reminder_creation"},
        )
    )

    return bool(result.handled)


def classify_pm_reminder_lifecycle(text: str) -> Optional[str]:
    result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="core_recognition",
            text=text or "",
            context={"candidate": "pm_reminder_lifecycle"},
        )
    )

    return result.action if result.handled else None


def _pm_reminder_id_from_text(text: str) -> Optional[int]:
    t = (text or "").lower()
    match = re.search(r"\breminder\s*#?\s*(\d+)\b", t)
    if not match:
        match = re.search(
            r"^(?:ok|okay|ack|acknowledge)\s*#?\s*(\d+)\b",
            t.strip(),
        )
    return int(match.group(1)) if match else None


def _pm_reminder_clock(text: str) -> Optional[dt.time]:
    result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="shared_datetime",
            text=text or "",
            context={"candidate": "time_of_day"},
        )
    )

    if not result.handled:
        return None

    return dt.time(
        result.metadata["hour"],
        result.metadata["minute"],
    )


def _pm_reminder_next_weekday(
    start_date: dt.date,
    weekday: int,
    include_today: bool = True,
) -> dt.date:
    days_ahead = (weekday - start_date.weekday()) % 7
    if days_ahead == 0 and not include_today:
        days_ahead = 7
    return start_date + dt.timedelta(days=days_ahead)


def _pm_reminder_add_months(
    value: dt.datetime,
    months: int,
    anchor_day: Optional[int] = None,
) -> dt.datetime:
    import calendar

    month_index = value.year * 12 + value.month - 1 + months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(
        anchor_day or value.day,
        calendar.monthrange(year, month)[1],
    )
    return value.replace(year=year, month=month, day=day)


def _pm_reminder_to_utc_naive(local_value: dt.datetime) -> dt.datetime:
    return local_value.astimezone(dt.timezone.utc).replace(tzinfo=None)


def parse_pm_reminder_request(
    text: str,
    timezone_name: str = "America/New_York",
    now_local: Optional[dt.datetime] = None,
    date_order: Optional[str] = "month_first",
) -> Optional[dict]:
    """Parse initial schedule metadata without altering the stored text."""
    raw = text or ""
    t = raw.lower().strip()
    if not t:
        return None

    try:
        owner_tz = ZoneInfo(timezone_name or "America/New_York")
    except Exception:
        owner_tz = ZoneInfo("America/New_York")
        timezone_name = "America/New_York"

    if now_local is None:
        now_local = dt.datetime.now(owner_tz)
    elif now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=owner_tz)
    else:
        now_local = now_local.astimezone(owner_tz)

    recurrence_rule = "none"
    recurrence_interval = 1
    recurrence_seconds = None
    recurrence_anchor_day = None
    recurrence_weekday = None

    recurrence_result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="recurrence",
            text=t,
            context={"candidate": "schedule_recurrence"},
        )
    )
    if recurrence_result.handled:
        recurrence_metadata = recurrence_result.metadata
        recurrence_rule = recurrence_metadata["recurrence_rule"]
        recurrence_interval = recurrence_metadata["recurrence_interval"]
        recurrence_seconds = recurrence_metadata["recurrence_seconds"]
        recurrence_weekday = recurrence_metadata.get("weekday")

    relative_duration_result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="shared_datetime",
            text=t,
            context={"candidate": "relative_duration"},
        )
    )
    if relative_duration_result.handled and recurrence_rule == "none":
        metadata = relative_duration_result.metadata
        if not metadata.get("valid"):
            return None

        amount = int(metadata["amount"])
        unit = metadata["unit"]
        if unit == "minute":
            next_local = now_local + dt.timedelta(minutes=amount)
        elif unit == "hour":
            next_local = now_local + dt.timedelta(hours=amount)
        elif unit == "day":
            next_local = now_local + dt.timedelta(days=amount)
        else:
            next_local = now_local + dt.timedelta(weeks=amount)

        return {
            "next_run": _pm_reminder_to_utc_naive(next_local),
            "rule": "once",
            "recurring": False,
            "recurrence_rule": "none",
            "recurrence_interval": 1,
            "recurrence_seconds": None,
            "recurrence_anchor_day": None,
            "timezone": timezone_name,
        }

    clock_value = _pm_reminder_clock(t)

    # Fixed intervals without an explicit clock begin after one interval.
    if recurrence_rule in ("interval", "hourly") and clock_value is None:
        seconds = recurrence_seconds or (3600 * recurrence_interval)
        next_local = now_local + dt.timedelta(seconds=seconds)
        return {
            "next_run": _pm_reminder_to_utc_naive(next_local),
            "rule": recurrence_rule,
            "recurring": True,
            "recurrence_rule": recurrence_rule,
            "recurrence_interval": recurrence_interval,
            "recurrence_seconds": seconds,
            "recurrence_anchor_day": None,
            "timezone": timezone_name,
        }

    target_date = None
    calendar_date_result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="shared_datetime",
            text=t,
            context={
                "candidate": "calendar_date",
                "reference_date": now_local.date().isoformat(),
                "date_order": date_order,
            },
        )
    )

    if calendar_date_result.handled:
        if not calendar_date_result.metadata.get("valid"):
            return None

        target_date = dt.date(
            calendar_date_result.metadata["year"],
            calendar_date_result.metadata["month"],
            calendar_date_result.metadata["day"],
        )

    monthly_day_match = re.search(
        r"\bon\s+(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)\b",
        t,
    )
    if recurrence_rule == "monthly" and monthly_day_match:
        recurrence_anchor_day = int(monthly_day_match.group(1))

    if clock_value is None:
        if recurrence_rule == "daily":
            next_local = now_local + dt.timedelta(days=recurrence_interval)
        elif recurrence_rule == "weekly":
            if recurrence_weekday:
                weekday = _REMINDER_WEEKDAYS[recurrence_weekday]
                next_date = _pm_reminder_next_weekday(
                    now_local.date(),
                    weekday,
                    include_today=False,
                )
                next_local = dt.datetime.combine(
                    next_date,
                    now_local.timetz().replace(tzinfo=None),
                    tzinfo=owner_tz,
                )
            else:
                next_local = now_local + dt.timedelta(weeks=recurrence_interval)
        elif recurrence_rule == "weekdays":
            next_date = now_local.date()
            while True:
                next_date += dt.timedelta(days=1)
                if next_date.weekday() < 5:
                    break
            next_local = dt.datetime.combine(
                next_date,
                now_local.timetz().replace(tzinfo=None),
                tzinfo=owner_tz,
            )
        elif recurrence_rule == "monthly":
            recurrence_anchor_day = recurrence_anchor_day or now_local.day
            next_local = _pm_reminder_add_months(
                now_local,
                recurrence_interval,
                recurrence_anchor_day,
            )
        else:
            return None
    else:
        if target_date is None:
            target_date = now_local.date()

        if recurrence_rule == "weekly" and recurrence_weekday:
            weekday = _REMINDER_WEEKDAYS[recurrence_weekday]
            target_date = _pm_reminder_next_weekday(
                now_local.date(),
                weekday,
                include_today=True,
            )

        if recurrence_rule == "weekdays":
            target_date = now_local.date()
            if target_date.weekday() >= 5:
                while target_date.weekday() >= 5:
                    target_date += dt.timedelta(days=1)

        if recurrence_rule == "monthly":
            recurrence_anchor_day = recurrence_anchor_day or target_date.day
            try:
                target_date = target_date.replace(day=recurrence_anchor_day)
            except ValueError:
                next_local = _pm_reminder_add_months(
                    dt.datetime.combine(
                        target_date.replace(day=1),
                        clock_value,
                        tzinfo=owner_tz,
                    ),
                    0,
                    recurrence_anchor_day,
                )
                target_date = next_local.date()

        next_local = dt.datetime.combine(
            target_date,
            clock_value,
            tzinfo=owner_tz,
        )

        if next_local <= now_local:
            if recurrence_rule == "daily":
                next_local += dt.timedelta(days=recurrence_interval)
            elif recurrence_rule == "weekly":
                next_local += dt.timedelta(weeks=recurrence_interval)
            elif recurrence_rule == "weekdays":
                while True:
                    next_local += dt.timedelta(days=1)
                    if next_local.weekday() < 5:
                        break
            elif recurrence_rule == "monthly":
                next_local = _pm_reminder_add_months(
                    next_local,
                    recurrence_interval,
                    recurrence_anchor_day,
                )
            elif recurrence_rule in ("interval", "hourly"):
                seconds = recurrence_seconds or 3600
                next_local += dt.timedelta(seconds=seconds)
            elif target_date == now_local.date():
                next_local += dt.timedelta(days=1)
            else:
                return None

    recurring = recurrence_rule != "none"
    return {
        "next_run": _pm_reminder_to_utc_naive(next_local),
        "rule": recurrence_rule if recurring else "once",
        "recurring": recurring,
        "recurrence_rule": recurrence_rule,
        "recurrence_interval": recurrence_interval,
        "recurrence_seconds": recurrence_seconds,
        "recurrence_anchor_day": recurrence_anchor_day,
        "timezone": timezone_name,
    }


def ambiguous_calendar_date_options(
    text: str,
    reference_date: dt.date,
) -> list[dict]:
    """Return the bounded month-first/day-first meanings for numeric ambiguity."""
    result = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="shared_datetime",
            text=text or "",
            context={
                "candidate": "calendar_date",
                "reference_date": reference_date.isoformat(),
            },
        )
    )
    metadata = result.metadata if result.handled else {}
    if not metadata.get("ambiguous"):
        return []
    first = int(metadata["first"])
    second = int(metadata["second"])
    year = int(metadata["year"])
    if year < 100:
        year += 2000
    options = []
    for date_order, month, day in (
        ("month_first", first, second),
        ("day_first", second, first),
    ):
        try:
            value = dt.date(year, month, day)
        except ValueError:
            continue
        if value < reference_date and not metadata.get("year_supplied"):
            value = value.replace(year=year + 1)
        options.append({
            "id": value.isoformat(),
            "label": value.strftime("%B %d, %Y"),
            "date_order": date_order,
        })
    return options


def format_configured_datetime(
    value_utc: dt.datetime,
    timezone_name: str,
    date_display: str = "month_first",
    time_format: str = "12h",
) -> str:
    """Display a canonical naive-UTC value using sender configuration."""
    try:
        owner_tz = ZoneInfo(timezone_name or "America/New_York")
    except Exception:
        owner_tz = ZoneInfo("America/New_York")
    aware_utc = value_utc
    if aware_utc.tzinfo is None:
        aware_utc = aware_utc.replace(tzinfo=dt.timezone.utc)
    local_value = aware_utc.astimezone(owner_tz)
    date_pattern = "%d/%m/%Y" if date_display == "day_first" else "%m/%d/%Y"
    time_pattern = "%H:%M" if time_format == "24h" else "%I:%M %p"
    return local_value.strftime(f"{date_pattern} {time_pattern}")


_NATURAL_ORDER_REQUIRED_FIELDS = (
    "item",
    "quantity",
    "supplier",
    "delivery_date",
    "drop_location",
)


def parse_natural_order(
    text: str,
    project_code: Optional[str] = None,
) -> Optional[dict]:
    """Extract natural order meaning without performing business mutation."""
    raw = str(text or "").strip()
    command = re.match(
        r"^(?:please\s+)?(?:order|get\s+me|arrange\s+delivery\s+of)\s+",
        raw,
        flags=re.IGNORECASE,
    )
    if not command:
        return None
    body = raw[command.end():].strip()
    if project_code:
        body = re.sub(
            rf"\s+for\s+(?:project\s+)?{re.escape(str(project_code))}\b",
            " ",
            body,
            flags=re.IGNORECASE,
        ).strip()

    fields = {
        "item": None,
        "quantity": None,
        "supplier": None,
        "delivery_date": None,
        "drop_location": None,
    }
    drop_match = re.search(
        r"\bto\s+(?:the\s+)?(.+?)\s*$",
        body,
        flags=re.IGNORECASE,
    )
    if drop_match:
        fields["drop_location"] = drop_match.group(1).strip(" .,")
        body = body[:drop_match.start()].strip()

    delivery_match = re.search(
        r"\bfor\s+delivery\s+(?:on\s+)?(.+?)\s*$",
        body,
        flags=re.IGNORECASE,
    )
    if delivery_match:
        fields["delivery_date"] = delivery_match.group(1).strip(" .,")
        body = body[:delivery_match.start()].strip()

    supplier_match = re.search(
        r"\bfrom\s+(.+?)\s*$",
        body,
        flags=re.IGNORECASE,
    )
    if supplier_match:
        fields["supplier"] = supplier_match.group(1).strip(" .,")
        body = body[:supplier_match.start()].strip()

    quantity_match = re.match(
        r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+)?(?:\s+of)?\s+(.+)$",
        body,
        flags=re.IGNORECASE,
    )
    if quantity_match:
        number = quantity_match.group(1)
        unit = quantity_match.group(2)
        fields["quantity"] = f"{number} {unit}".strip() if unit else number
        fields["item"] = quantity_match.group(3).strip(" .,")
    elif body:
        fields["item"] = body.strip(" .,")

    missing = [
        field for field in _NATURAL_ORDER_REQUIRED_FIELDS
        if not fields.get(field)
    ]
    return {
        "intent": "order",
        "action": "create",
        "fields": fields,
        "missing_fields": missing,
    }


def parse_natural_meeting(
    text: str,
    timezone_name: str = "America/New_York",
    date_order: Optional[str] = "month_first",
    now_local: Optional[dt.datetime] = None,
) -> Optional[dict]:
    """Return structured Meeting meaning for the existing Meeting handler."""
    recognized = _CORE_CONVERSATION.interpret_core(
        ConversationRequest(
            capability="core_recognition",
            text=text or "",
            context={"candidate": "meeting_creation"},
        )
    )
    if not recognized.handled:
        return None
    parsed_schedule = parse_pm_reminder_request(
        f"Remind me {text}",
        timezone_name=timezone_name,
        now_local=now_local,
        date_order=date_order,
    )
    if not parsed_schedule:
        return None
    title_body = re.sub(
        r"^(?:please\s+)?(?:schedule|book|set\s+up|arrange|create)\s+",
        "",
        str(text or ""),
        flags=re.IGNORECASE,
    )
    title_body = re.split(
        r"\b(?:today|tomorrow|on\s+\d|on\s+(?:monday|tuesday|wednesday|"
        r"thursday|friday|saturday|sunday)|at\s+)\b",
        title_body,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip(" .,")
    return {
        "intent": "meeting",
        "action": "create",
        "title": title_body or "Site Meeting",
        "scheduled_for": parsed_schedule["next_run"],
        "timezone": parsed_schedule["timezone"],
    }


def interpret_supported_message(
    text: str,
    project_code: Optional[str] = None,
) -> dict:
    """Converged non-mutating routing and structured meaning for Stage 2."""
    raw = str(text or "").strip()
    lower = raw.lower()
    entities = {}
    context_patterns = {
        "project": r"\bproject\s+([a-z0-9_-]+)",
        "phase": r"\bphase\s+([a-z0-9][a-z0-9 _-]*?)(?=\s+(?:in|on|for|at)\b|$)",
        "zone": r"\bzone\s+([a-z0-9_-]+)",
        "trade": r"\btrade\s+([a-z0-9][a-z0-9 _-]*?)(?=\s+(?:in|on|for|at)\b|$)",
    }
    for name, pattern in context_patterns.items():
        match = re.search(pattern, lower, flags=re.IGNORECASE)
        if match:
            entities[name] = match.group(1).strip()
    if project_code and "project" not in entities:
        entities["project"] = str(project_code)

    lifecycle = classify_pm_reminder_lifecycle(raw)
    if lifecycle:
        return {"route": "reminder", "action": lifecycle, "entities": entities}
    if classify_pm_reminder(raw):
        return {"route": "reminder", "action": "create", "entities": entities}

    meeting = _CORE_CONVERSATION.interpret_core(ConversationRequest(
        capability="core_recognition",
        text=raw,
        context={"candidate": "meeting_creation"},
    ))
    if meeting.handled:
        return {"route": "meeting", "action": "create", "entities": entities}

    if classify_inspection(raw):
        return {"route": "inspection", "action": "create", "entities": entities}
    if classify_delay(raw):
        return {"route": "delay", "action": "create", "entities": entities}

    order = parse_natural_order(raw, project_code)
    if order:
        return {**order, "route": "order", "entities": {**entities, **order["fields"]}}

    approval = re.match(
        r"^(approve|reject)\s+(?:change\s+)?order\s*#?\s*(\d+)\s*$",
        lower,
    )
    if approval:
        return {
            "route": "approval",
            "action": approval.group(1),
            "entities": {**entities, "task_id": int(approval.group(2))},
        }

    if re.match(r"^(?:record|log|confirm)\s+(?:a\s+)?delivery\b", lower):
        return {"route": "delivery", "action": "create", "entities": entities}
    if re.match(r"^(?:pin|pinned)\s+note\b", lower):
        return {"route": "pinned_note", "action": "create", "entities": entities}
    if re.match(r"^(?:add\s+|create\s+|write\s+)?note\b", lower):
        return {"route": "note", "action": "create", "entities": entities}
    if re.match(r"^(?:show|find|search|list)\b", lower):
        return {"route": "search", "action": "read", "entities": entities}
    if re.match(r"^(?:what(?:'s| is)\s+)?(?:the\s+)?status\b", lower):
        return {"route": "status", "action": "read", "entities": entities}
    if "stock" in lower and re.search(
        r"\b(?:add|receive|received|remove|use|used|deduct|adjust)\b", lower
    ):
        return {"route": "stock", "action": "adjust", "entities": entities}

    assigned = re.match(r"^assign\s+(.+?)\s+to\s+(.+)$", raw, flags=re.IGNORECASE)
    if assigned:
        entities.update({
            "recipient_reference": assigned.group(1).strip(),
            "task_text": assigned.group(2).strip(),
        })
        return {"route": "task", "action": "create", "subtype": "assigned", "entities": entities}
    if re.match(r"^(?:create|add)\s+(?:a\s+)?task\s+for\s+me\b", lower):
        return {"route": "task", "action": "create", "subtype": "self", "entities": entities}
    if re.match(r"^urgent\s*:", lower) or re.search(r"\basap\b", lower):
        return {"route": "task", "action": "create", "subtype": "urgent", "entities": entities}
    if re.match(r"^(?:create|add)\s+(?:a\s+)?task\b", lower):
        return {"route": "task", "action": "create", "subtype": "assigned", "entities": entities}

    return {"route": "ordinary_fallback", "action": "create", "entities": entities}


def parse_pm_reminder_snooze_until(
    text: str,
    timezone_name: str,
) -> Optional[dt.datetime]:
    t = (text or "").lower()
    duration_match = re.search(
        r"\b(\d+)\s+(minutes?|hours?|days?|weeks?)\b",
        t,
    )
    if duration_match:
        amount = int(duration_match.group(1))
        unit = duration_match.group(2)
        if amount <= 0:
            return None
        now_utc = dt.datetime.utcnow()
        if unit.startswith("minute"):
            return now_utc + dt.timedelta(minutes=amount)
        if unit.startswith("hour"):
            return now_utc + dt.timedelta(hours=amount)
        if unit.startswith("day"):
            return now_utc + dt.timedelta(days=amount)
        return now_utc + dt.timedelta(weeks=amount)

    until_match = re.search(r"\b(?:until|to)\s+(.+)$", text or "", re.I)
    if until_match:
        parsed = parse_pm_reminder_request(
            f"remind me {until_match.group(1)}",
            timezone_name=timezone_name,
        )
        if parsed:
            return parsed["next_run"]

    return None

# >>> FEATURE_3_REMINDER_CLASSIFIER_END <<<


# ---------------------------------------------------------------------
# WhatsApp send utility
# ---------------------------------------------------------------------
def send_whatsapp_text(phone_id:str,to:str,body:str)->tuple[bool,dict]:
    if not (D360_KEY and phone_id and to and body):
        log.warning("send_whatsapp_text skipped (missing key/to/body)")
        return False,{}
    headers={"D360-API-KEY":D360_KEY,"Content-Type":"application/json"}
    payload={"to":to,"type":"text","text":{"body":body}}
    try:
        r=requests.post(WHATSAPP_BASE,headers=headers,json=payload,timeout=10)
        data=r.json() if r.text else {}
        return (200<=r.status_code<300),data
    except Exception as e:
        log.exception("D360 send error: %s",e)
        return False,{"error":str(e)}

# === ADD NEAR TOP, BELOW send_whatsapp_text ===
import json

def send_order_checklist(phone_id: str, to: str, task_id: int):
    headers = {"D360-API-KEY": D360_KEY, "Content-Type": "application/json"}
    payload = {
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "Order logged. Confirm next detail:"},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": f"order_item:{task_id}", "title": "Item"}},
                    {"type": "reply", "reply": {"id": f"order_quantity:{task_id}", "title": "Quantity"}},
                    {"type": "reply", "reply": {"id": f"order_supplier:{task_id}", "title": "Supplier"}},
                    {"type": "reply", "reply": {"id": f"order_delivery_date:{task_id}", "title": "Delivery Date"}},
                    {"type": "reply", "reply": {"id": f"order_drop_location:{task_id}", "title": "Drop Location"}},
                ]
            }
        }
    }
    try:
        r = requests.post(WHATSAPP_BASE, headers=headers, json=payload, timeout=10)
        return (200 <= r.status_code < 300)
    except:
        return False


# === MODIFY IN /webhook, inside loop after create_task(...) and before return ===
        row = create_task(
            sender=sender,
            text=text or "",
            tag=tag,
            project_code=None,
            subcontractor_name=None,
            order_state=order_state,
            attachment=attachment,
            subtype=subtype
        )

        # send checklist for orders
        if tag == "order":
            send_order_checklist(phone_id, sender, row["id"])
            return ("", 200)

        # existing auto-replies remain unchanged below

# ---------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------
@app.route("/",methods=["GET"])
def health():
    return "HubFlo V6 service running",200

# ---------------------------------------------------------------------
# WEBHOOK — W2 REBUILD (BLOCK 1)
# Header, JSON extraction, metadata, imports
# ---------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def webhook():
    # -------- RAW INBOUND PAYLOAD --------
    raw = request.get_json(silent=True) or {}

    # Defensive extraction: no crashes on partial payloads
    try:
        entry = (raw.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        msgs = value.get("messages") or []
        contacts = value.get("contacts") or []
        phone_id = (value.get("metadata") or {}).get("phone_number_id") or DEFAULT_PHONE_ID
    except Exception:
        msgs, contacts, phone_id = [], [], DEFAULT_PHONE_ID

    # -------- SENDER EXTRACTION --------
    sender = None
    if contacts:
        sender = contacts[0].get("wa_id") or sender

    # -------- IMPORT STORAGE LAYERS --------
    from storage import (
        SessionLocal as DBSession,
        User,
        Task,
        PMProjectMap,
        get_user_role,
        get_pms_for_project,
    )

    from storage_v6_1 import (
        create_task,
        adjust_stock,
        create_stock_item,
        save_pending_conversation_state,
        get_pending_conversation_state,
        claim_conversation_state_continuation,
        advance_conversation_state_continuation,
        resolve_conversation_state,
        touch_conversation_state_activity,
        retire_conversation_state,
    )

    # -----------------------------------------------------------------
    # MU12 — GENERIC PERSISTENT CONVERSATION-STATE ORCHESTRATION
    # -----------------------------------------------------------------
    def _conversation_scope(sender_wa: str) -> tuple[int, Optional[str]]:
        with DBSession() as s:
            sender_user = (
                s.query(User)
                .filter(User.wa_id == sender_wa)
                .first()
            )

            if not sender_user:
                return 1, None

            try:
                client_id = int(sender_user.client_id or 1)
            except (TypeError, ValueError):
                client_id = 1

            project_code = sender_user.project_code
            if project_code is not None:
                project_code = str(project_code).strip() or None
            return client_id, project_code

    def _conversation_client_id(sender_wa: str) -> int:
        return _conversation_scope(sender_wa)[0]

    def _get_active_conversation_state(sender_wa: str) -> Optional[dict]:
        client_id, project_code = _conversation_scope(sender_wa)
        state = get_pending_conversation_state(
            sender_wa,
            client_id,
            project_code,
        )
        if state is None and project_code is not None:
            state = get_pending_conversation_state(
                sender_wa,
                client_id,
                None,
            )
        return state

    def _conversation_lifecycle_action(raw_text: str) -> Optional[str]:
        result = _CORE_CONVERSATION.interpret_core(
            ConversationRequest(
                capability="conversation_lifecycle",
                text=raw_text or "",
                context={"candidate": "control_intent"},
            )
        )
        return result.action if result.handled else None

    def _retire_active_conversation_state(
        state: dict,
        action: str,
    ) -> bool:
        result = retire_conversation_state(
            state["id"],
            state["sender"],
            state["client_id"],
            state.get("project_code"),
            action,
        )
        return result.get("status_result") == "retired"

    def _await_expected_field(await_text: str) -> Optional[str]:
        match = re.match(
            r"^\s*\[await:([^\]]+)\]",
            await_text or "",
            flags=re.IGNORECASE,
        )
        return match.group(1).strip().lower() if match else None

    def _save_await_conversation_state(
        task_id: int,
        task_sender: str,
        project_code: Optional[str],
        await_text: str,
        original_request: str,
        structured_context: Optional[dict] = None,
        candidate_metadata: Optional[dict] = None,
    ) -> Optional[dict]:
        expected_field = _await_expected_field(await_text)
        if not expected_field:
            return None

        client_id = _conversation_client_id(task_sender)
        continuation_key = f"business-state:{int(task_id)}"
        existing = get_pending_conversation_state(
            task_sender,
            client_id,
            project_code,
            continuation_key=continuation_key,
        )

        if existing:
            original_request = existing["original_request"]
            if structured_context is None:
                structured_context = existing.get("structured_context") or {}
            if candidate_metadata is None:
                candidate_metadata = existing.get("candidate_metadata") or {}
            continuation = existing.get("continuation") or {}
        else:
            structured_context = structured_context or {
                "source_record_id": int(task_id),
            }
            candidate_metadata = candidate_metadata or {}
            continuation = {
                "source_record_id": int(task_id),
                "source_record_type": "pending_business_state",
            }

        state = save_pending_conversation_state(
            {
                "client_id": client_id,
                "sender": task_sender,
                "project_code": project_code,
                "state_kind": "await",
                "expected_field": expected_field,
                "original_request": original_request or "",
                "structured_context": structured_context,
                "candidate_metadata": candidate_metadata,
                "continuation": continuation,
                "continuation_key": continuation_key,
            }
        )
        return state if state.get("active") else None

    def _get_await_conversation_state(awaiting) -> Optional[dict]:
        return get_pending_conversation_state(
            awaiting.sender,
            _conversation_client_id(awaiting.sender),
            awaiting.project_code,
            continuation_key=f"business-state:{int(awaiting.id)}",
        )

    def _ensure_await_conversation_state(awaiting) -> Optional[dict]:
        existing = _get_await_conversation_state(awaiting)
        if existing:
            return existing

        body = (awaiting.text or "").split("\n", 1)
        fallback_original = body[1] if len(body) == 2 else awaiting.text or ""
        return _save_await_conversation_state(
            awaiting.id,
            awaiting.sender,
            awaiting.project_code,
            awaiting.text or "",
            fallback_original,
        )

    def _sync_await_conversation_state_after_resolver(
        pending_state: Optional[dict],
        awaiting,
        prior_text: str,
        prior_status: str,
    ) -> None:
        if not pending_state:
            return

        if (awaiting.text or "") == prior_text and awaiting.status == prior_status:
            return

        expected_field = _await_expected_field(awaiting.text or "")
        if awaiting.status == "open" and expected_field:
            advance_conversation_state_continuation(
                pending_state["id"],
                pending_state["sender"],
                pending_state["client_id"],
                pending_state.get("project_code"),
                expected_field,
            )
            return

        resolve_conversation_state(
            pending_state["id"],
            pending_state["sender"],
            pending_state["client_id"],
            pending_state.get("project_code"),
        )

    def _run_await_resolver(
        resolver,
        awaiting,
        raw_txt: str,
        sender_wa: str,
        session,
        pending_state: Optional[dict],
    ) -> bool:
        if not pending_state:
            return False

        claimed_state = claim_conversation_state_continuation(
            pending_state["id"],
            pending_state["sender"],
            pending_state["client_id"],
            pending_state.get("project_code"),
        )
        if claimed_state.get("status_result") != "claimed":
            return False

        prior_text = awaiting.text or ""
        prior_status = awaiting.status
        resolver(awaiting, raw_txt, sender_wa, session)
        _sync_await_conversation_state_after_resolver(
            claimed_state,
            awaiting,
            prior_text,
            prior_status,
        )
        return True

    # -----------------------------------------------------------------
    # BLOCK 1 ENDS HERE — READY FOR BLOCK 2 (SEARCH ENGINE)
    # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # SEARCH ENGINE — W2 CLEAN REBUILD
    # -----------------------------------------------------------------

    def is_search_request(text: str) -> bool:
        """Lightweight trigger for search commands."""
        t = (text or "").lower()
        phrases = [
            "search ",
            "search for",
            "find all",
            "find ",
            "list all",
            "show all",
            "show me all",
            "give me all",
            "overrun jobs",
            "overrun work",
            "overdue jobs",
            "late jobs",
        ]
        return any(p in t for p in phrases)

    def run_search(sender_wa: str, text: str):
        """Role-aware, scoped search with PM escalation for subs outside scope."""
        t = (text or "").lower()

        with DBSession() as s:
            # USER VALIDATION
            u = (
                s.query(User)
                .filter(User.wa_id == sender_wa, User.active == True)
                .first()
            )
            if not u:
                send_whatsapp_text(
                    phone_id,
                    sender_wa,
                    "Search is not available — your number is not linked."
                )
                return

            role = (u.role or "").lower().strip()
            q = s.query(Task).filter(
                Task.client_id == int(u.client_id or 1)
            )

            # ------------------------------------------------------------
            # ROLE SCOPING
            # ------------------------------------------------------------

            if role == "sub":
                # Subs only see their own tasks
                q = q.filter(Task.sender == sender_wa)

            elif role == "pm":
                # PMs = tasks across mapped projects
                proj_rows = (
                    s.query(PMProjectMap.project_code)
                    .filter(
                        PMProjectMap.pm_user_id == u.id,
                        PMProjectMap.client_id == int(u.client_id or 1),
                    )
                    .all()
                )
                projects = [r.project_code for r in proj_rows]
                if not projects:
                    send_whatsapp_text(phone_id, sender_wa, "No projects mapped to you yet.")
                    return

                q = q.filter(Task.project_code.in_(projects))

            else:
                # Directors / Admin roles → same project mapping logic
                proj_rows = (
                    s.query(PMProjectMap.project_code)
                    .filter(
                        PMProjectMap.pm_user_id == u.id,
                        PMProjectMap.client_id == int(u.client_id or 1),
                    )
                    .all()
                )
                projects = [r.project_code for r in proj_rows]
                if not projects:
                    send_whatsapp_text(
                        phone_id,
                        sender_wa,
                        "Search is not enabled for your role yet."
                    )
                    return

                q = q.filter(Task.project_code.in_(projects))

            # ------------------------------------------------------------
            # SUB CONTRACTOR-SPECIFIC SCOPING
            # ------------------------------------------------------------
            target_sub = None
            if " for " in t:
                subs = (
                    s.query(Task.subcontractor_name)
                    .filter(
                        Task.client_id == int(u.client_id or 1),
                        Task.subcontractor_name != None,
                    )
                    .distinct()
                    .all()
                )
                for row in subs:
                    name = (row.subcontractor_name or "").strip()
                    if name and name.lower() in t:
                        target_sub = name
                        break

            if role == "sub" and target_sub:
                own = (u.subcontractor_name or "").strip().lower()
                if own and target_sub.lower() != own:
                    # Escalate to PMs of the sub's project
                    if u.project_code:
                        pm_rows = (
                            s.query(User)
                            .join(PMProjectMap, PMProjectMap.pm_user_id == User.id)
                            .filter(
                                User.client_id == int(u.client_id or 1),
                                PMProjectMap.client_id == int(u.client_id or 1),
                                PMProjectMap.project_code == u.project_code,
                                User.role == "pm",
                                User.active == True,
                            )
                            .all()
                        )
                        for pm in pm_rows:
                            send_whatsapp_text(
                                phone_id,
                                pm.wa_id,
                                f"⚠ Search escalation from {u.name or u.wa_id}: '{text}'"
                            )

                    send_whatsapp_text(
                        phone_id,
                        sender_wa,
                        "That search is outside your scope — PM has been notified."
                    )
                    return

            # ------------------------------------------------------------
            # OVERRUN FILTERS
            # ------------------------------------------------------------
            if any(k in t for k in ("overrun", "over run", "overdue", "late")):
                q = q.filter(Task.overrun_days > 0)

            # ------------------------------------------------------------
            # TRADE HINT FILTERS
            # ------------------------------------------------------------
            if "paint" in t or "painting" in t:
                q = q.filter(Task.text.ilike("%paint%"))

            if "plumb" in t:
                q = q.filter(Task.subcontractor_name.ilike("%plumb%"))

            if "elect" in t or "electric" in t:
                q = q.filter(Task.subcontractor_name.ilike("%elect%"))

            # ------------------------------------------------------------
            # KEYWORD TAIL EXTRACTION
            # ------------------------------------------------------------
            keywords = []
            for token in ["for", "on", "about"]:
                if f"{token} " in t:
                    tail = t.split(token, 1)[1]
                    for w in tail.split():
                        w = w.strip(",. ")
                        if len(w) >= 4:
                            keywords.append(w)
                    break

            if keywords:
                q = q.filter(Task.text.ilike(f"%{keywords[0]}%"))

            # ------------------------------------------------------------
            # EXECUTE QUERY
            # ------------------------------------------------------------
            rows = q.order_by(Task.id.desc()).limit(25).all()

            if not rows:
                send_whatsapp_text(
                    phone_id,
                    sender_wa,
                    "No matching tasks found."
                )
                return

            # ------------------------------------------------------------
            # FORMAT RESULTS
            # ------------------------------------------------------------
            lines = ["🔎 Search results:"]
            for tsk in rows:
                meta_bits = []
                if tsk.project_code:
                    meta_bits.append(tsk.project_code)
                if tsk.subcontractor_name:
                    meta_bits.append(tsk.subcontractor_name)

                meta = " | ".join(meta_bits)
                snippet = (tsk.text or "").strip()
                if len(snippet) > 80:
                    snippet = snippet[:77] + "..."

                lines.append(f"- ({tsk.id}) {meta} {snippet}".strip())

            send_whatsapp_text(phone_id, sender_wa, "\n".join(lines))

    # -----------------------------------------------------------------
    # END OF BLOCK 2 — NEXT: STOCK SYSTEM (BLOCK 3)
    # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # STOCK ENGINE — W2 CLEAN REBUILD
    # -----------------------------------------------------------------

    def is_new_stock_item_request(text: str) -> bool:
        t = (text or "").lower()
        return "add new stock item" in t

    def parse_new_stock_item(text: str) -> str:
        t = (text or "").lower()
        if ":" in t:
            return t.split("add new stock item", 1)[1].split(":", 1)[1].strip()
        return t.split("add new stock item", 1)[1].strip()

    def parse_stock_command(text: str):
        """Detect 'add/remove X units of Y to/from stock' patterns."""
        t = (text or "").lower()
        if "stock" not in t:
            return None

        # Possible verbs
        verbs_add = ["add", "added", "received", "put", "delivered", "stocked"]
        verbs_remove = ["take", "took", "use", "used", "deduct", "remove", "issue", "pull"]

        kind = None
        for v in verbs_add:
            if f"{v} " in t:
                kind = "add"
                break
        if not kind:
            for v in verbs_remove:
                if f"{v} " in t:
                    kind = "remove"
                    break

        if not kind:
            return None

        # Regex: qty + optional unit + material + direction to/from stock
        m = re.search(
            r"(\d+)\s*([a-zA-Z]+)?\s*(?:of\s+)?(.+?)\s+(?:to|into|in to|in|from|out of)\s+stock",
            t
        )

        if not m:
            # Not enough info → ask for clarification
            return {
                "kind": kind,
                "material": t,
                "qty": None,
                "unit": None,
                "needs_prompt": True,
            }

        qty = int(m.group(1))
        unit = m.group(2)
        material = (m.group(3) or "").strip()

        needs_prompt = False
        if not unit or unit.lower() in ("of", "to", "into", "from", "out", "in"):
            unit = None
            needs_prompt = True

        return {
            "kind": kind,
            "material": material,
            "qty": qty,
            "unit": unit,
            "needs_prompt": needs_prompt,
        }

    # -----------------------------------------------------------------
    # STOCK AWAIT-CHAINS (RESOLUTION)
    # -----------------------------------------------------------------

    # These are executed inside the main message loop (Block 5),
    # but declared here for clarity and structure.

    def resolve_await_stock_unit(awaiting, raw_txt, sender, s):
        """[await:stock_unit] → unit chosen."""
        meta_str = awaiting.text.split("\n", 1)[0].split(" ", 1)[-1]
        meta = {}
        for chunk in meta_str.split(";"):
            if "=" in chunk:
                k, v = chunk.split("=", 1)
                meta[k.strip()] = v.strip()

        kind = meta.get("kind", "add")
        qty = meta.get("qty")
        material = meta.get("material", "stock item")

        try:
            qty_val = int(qty)
        except Exception:
            qty_val = None

        unit = raw_txt.strip().lower()

        # Missing quantity → note only
        if not qty_val:
            awaiting.text = f"STOCK NOTE: {kind} {unit} {material} (qty missing)"
            awaiting.status = "done"
            awaiting.last_updated = dt.datetime.utcnow()
            s.commit()
            send_whatsapp_text(
                phone_id,
                sender,
                "Noted — quantity missing, stock not adjusted."
            )
            return

        # Apply delta
        delta = qty_val if kind == "add" else -qty_val
        adjust_stock({
            "material": material,
            "unit": unit,
            "delta": delta,
            "actor": sender,
            "source": "whatsapp",
        })

        awaiting.text = f"STOCK {kind}: {qty_val} {unit} {material}"
        awaiting.status = "done"
        awaiting.last_updated = dt.datetime.utcnow()
        s.commit()

        send_whatsapp_text(
            phone_id,
            sender,
            f"Stock updated: {delta:+} {unit} of {material}."
        )

    def resolve_await_new_stock_unit(awaiting, raw_txt, sender, s):
        """[await:new_stock_unit] → choose unit."""
        material = (
            awaiting.text.split("material=", 1)[1].strip()
            if "material=" in awaiting.text
            else "stock item"
        )
        unit = raw_txt.strip().lower()
        awaiting.text = f"[await:new_stock_qty] material={material};unit={unit}"
        s.commit()
        send_whatsapp_text(phone_id, sender, "What opening quantity?")

    # -----------------------------------------------------------------
    # PATCH: STOCK QTY GUARD — resolve_await_new_stock_qty
    # Marker: [await:new_stock_qty]
    # Scope: guard non-numeric input, preserve state, stop retry cascade
    # -----------------------------------------------------------------

    def validate_new_stock_qty_reply(raw_txt: str) -> dict:
        """Expose the existing guarded quantity rule without mutation."""
        raw = (raw_txt or "").strip()
        if not raw.isdigit():
            return {"valid": False, "reason": "whole_number"}

        qty_val = int(raw)
        if qty_val <= 0:
            return {"valid": False, "reason": "positive"}

        return {"valid": True, "value": qty_val}

    def resolve_await_new_stock_qty(awaiting, raw_txt, sender, s):
        """[await:new_stock_qty] → choose quantity, create item (guarded)."""

        meta_str = awaiting.text.split(" ", 1)[-1]
        meta = {}
        for chunk in meta_str.split(";"):
            if "=" in chunk:
                k, v = chunk.split("=", 1)
                meta[k.strip()] = v.strip()

        material = meta.get("material", "stock item")
        unit = meta.get("unit", "units")

        validation = validate_new_stock_qty_reply(raw_txt)

        # HARD GUARD — only accept whole-number input
        if not validation["valid"]:
            if validation["reason"] == "whole_number":
                message = "Send a whole number for the quantity."
            else:
                message = "Quantity must be greater than zero."
            send_whatsapp_text(
                phone_id,
                sender,
                message
            )
            return

        qty_val = int(validation["value"])

        create_stock_item({
            "name": material,
            "unit": unit,
            "opening_qty": qty_val,
            "actor": sender,
            "source": "whatsapp",
        })

        awaiting.text = f"NEW STOCK ITEM: {material} ({qty_val} {unit})"
        awaiting.status = "done"
        awaiting.last_updated = dt.datetime.utcnow()
        s.commit()

        send_whatsapp_text(
            phone_id,
            sender,
            f"New stock item created: {material} ({qty_val} {unit})."
        )

    # -----------------------------------------------------------------
    # END PATCH — resolve_await_new_stock_qty
    # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # END OF BLOCK 3 — NEXT: AWAIT-CHAIN FOR ORDERS (BLOCK 4)
    # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # ORDER AWAIT-CHAIN ENGINE — W2 CLEAN REBUILD
    # -----------------------------------------------------------------

    def resolve_await_item(awaiting, raw_txt, sender, s):
        """[await:item] → move to quantity"""
        awaiting.text = "[await:quantity]\n" f"Item: {raw_txt.strip()}"
        s.commit()
        send_whatsapp_text(phone_id, sender, "Quantity?")

    def resolve_await_quantity(awaiting, raw_txt, sender, s):
        """[await:quantity] → move to supplier"""
        body = awaiting.text.split("\n", 1)[1] if "\n" in (awaiting.text or "") else ""
        awaiting.text = "[await:supplier]\n" f"{body}\nQuantity: {raw_txt.strip()}".strip()
        s.commit()
        send_whatsapp_text(phone_id, sender, "Supplier?")

    def resolve_await_supplier(awaiting, raw_txt, sender, s):
        """[await:supplier] → move to delivery_date"""
        fields = extract_order_fields(awaiting)
        awaiting.text = (
            "[await:delivery_date]\n"
            f"Item: {fields.get('Item','')}\n"
            f"Quantity: {fields.get('Quantity','')}\n"
            f"Supplier: {raw_txt.strip()}"
        )
        s.commit()
        send_whatsapp_text(phone_id, sender, "Delivery date?")

    def resolve_await_delivery_date(awaiting, raw_txt, sender, s):
        """[await:delivery_date] → move to drop_location"""
        fields = extract_order_fields(awaiting)
        awaiting.text = (
            "[await:drop_location]\n"
            f"Item: {fields.get('Item','')}\n"
            f"Quantity: {fields.get('Quantity','')}\n"
            f"Supplier: {fields.get('Supplier','')}\n"
            f"Delivery Date: {raw_txt.strip()}"
        )
        s.commit()
        send_whatsapp_text(phone_id, sender, "Drop location on site?")

    def resolve_await_drop_location(awaiting, raw_txt, sender, s):
        """[await:drop_location] → finalize + pending_approval"""
        fields = extract_order_fields(awaiting)
        awaiting.text = (
            f"Item: {fields.get('Item','')}\n"
            f"Quantity: {fields.get('Quantity','')}\n"
            f"Supplier: {fields.get('Supplier','')}\n"
            f"Delivery Date: {fields.get('Delivery Date','')}\n"
            f"Drop Location: {raw_txt.strip()}"
        )
        awaiting.status = "pending_approval"
        awaiting.last_updated = dt.datetime.utcnow()
        s.commit()

        send_whatsapp_text(
            phone_id,
            sender,
            "✅ Order details captured. Awaiting PM approval."
        )

    # -----------------------------------------------------------------
    # Utility: extract order fields from awaiting.text
    # -----------------------------------------------------------------

    def extract_order_fields(task):
        """Extract Item/Quantity/Supplier/Delivery Date from task.text."""
        lines = [
            l.strip()
            for l in task.text.splitlines()
            if not l.lower().startswith("[await:")
        ]
        out = {}
        for l in lines:
            if ":" in l:
                k, v = l.split(":", 1)
                out[k.strip()] = v.strip()
        return out

    def _natural_order_task_text(
        fields: dict,
        expected_field: Optional[str] = None,
    ) -> str:
        labels = {
            "item": "Item",
            "quantity": "Quantity",
            "supplier": "Supplier",
            "delivery_date": "Delivery Date",
            "drop_location": "Drop Location",
        }
        lines = []
        if expected_field:
            lines.append(f"[await:{expected_field}]")
        for field in (
            "item", "quantity", "supplier", "delivery_date", "drop_location"
        ):
            if fields.get(field):
                lines.append(f"{labels[field]}: {fields[field]}")
        return "\n".join(lines)

    def _natural_order_prompt(field: str) -> str:
        return {
            "item": "What item should be ordered?",
            "quantity": "What quantity is required?",
            "supplier": "Who should we source this from?",
            "delivery_date": "What delivery date is required?",
            "drop_location": "Where should it be delivered on site?",
        }.get(field, "What information is missing?")

    def _natural_order_followup_value(
        expected_field: str,
        raw_text: str,
    ) -> Optional[str]:
        value = str(raw_text or "").strip()
        if not value or not re.search(r"[a-z0-9]", value, re.IGNORECASE):
            return None
        if expected_field == "quantity":
            quantity = re.match(r"^(\d+(?:\.\d+)?)(?:\s+.+)?$", value)
            if not quantity or float(quantity.group(1)) <= 0:
                return None
        return value

    def _run_natural_order_continuation(
        awaiting,
        raw_txt: str,
        pending_state: Optional[dict],
        session,
    ) -> bool:
        if not pending_state:
            return False
        context = dict(pending_state.get("structured_context") or {})
        if context.get("kind") != "natural_order":
            return False
        claimed = claim_conversation_state_continuation(
            pending_state["id"], pending_state["sender"],
            pending_state["client_id"], pending_state.get("project_code"),
        )
        if claimed.get("status_result") != "claimed":
            return True
        expected_field = str(claimed.get("expected_field") or "").strip()
        fields = dict((claimed.get("structured_context") or {}).get("order_fields") or {})
        current_client_id, current_project = _conversation_scope(
            claimed["sender"]
        )
        task_project = str(awaiting.project_code or "").strip() or None
        state_project = str(claimed.get("project_code") or "").strip() or None
        if (
            int(awaiting.client_id or 1) != int(claimed["client_id"])
            or int(current_client_id) != int(claimed["client_id"])
            or task_project != state_project
            or current_project != state_project
        ):
            advance_conversation_state_continuation(
                claimed["id"], claimed["sender"], claimed["client_id"],
                claimed.get("project_code"), expected_field,
                structured_context=claimed.get("structured_context") or {},
            )
            send_whatsapp_text(
                phone_id,
                claimed["sender"],
                "That pending order is no longer available in your scope.",
            )
            return True
        value = _natural_order_followup_value(expected_field, raw_txt)
        if expected_field not in _NATURAL_ORDER_REQUIRED_FIELDS or value is None:
            advance_conversation_state_continuation(
                claimed["id"], claimed["sender"], claimed["client_id"],
                claimed.get("project_code"), expected_field,
                structured_context=claimed.get("structured_context") or {},
            )
            send_whatsapp_text(
                phone_id, claimed["sender"], _natural_order_prompt(expected_field)
            )
            return True
        fields[expected_field] = value
        missing = [
            field for field in _NATURAL_ORDER_REQUIRED_FIELDS
            if not fields.get(field)
        ]
        next_context = dict(claimed.get("structured_context") or {})
        next_context["order_fields"] = fields
        next_context["missing_fields"] = missing
        if missing:
            next_field = missing[0]
            awaiting.text = _natural_order_task_text(fields, next_field)
            awaiting.last_updated = dt.datetime.utcnow()
            session.commit()
            advance_conversation_state_continuation(
                claimed["id"], claimed["sender"], claimed["client_id"],
                claimed.get("project_code"), next_field,
                structured_context=next_context,
            )
            send_whatsapp_text(
                phone_id, claimed["sender"], _natural_order_prompt(next_field)
            )
            return True
        awaiting.text = _natural_order_task_text(fields)
        awaiting.status = "pending_approval"
        awaiting.last_updated = dt.datetime.utcnow()
        session.commit()
        resolve_conversation_state(
            claimed["id"], claimed["sender"], claimed["client_id"],
            claimed.get("project_code"),
        )
        send_whatsapp_text(
            phone_id,
            claimed["sender"],
            "✅ Order details captured. Awaiting PM approval.",
        )
        return True

    # -----------------------------------------------------------------
    # END OF BLOCK 4 — NEXT: ORDER BUTTON ENGINE (BLOCK 5)
    # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # ORDER BUTTON ENGINE — W2 CLEAN REBUILD
    # -----------------------------------------------------------------

    if msgs:
        m = msgs[0]
        mtype = m.get("type")

        if mtype == "interactive":
            br = (m.get("interactive") or {}).get("button_reply") or {}
            bid = br.get("id", "") or ""

            from storage_v6_1 import SessionLocal as S2, Task as T2

            def _mark(tid, flag, prompt):
                """Rewrite first line of task.text to the next [await:*] stage."""
                with S2() as s:
                    t = s.get(T2, tid)
                    if t:
                        # Remove prior await tags
                        body = (
                            t.text.split("\n", 1)[1]
                            if "\n" in (t.text or "")
                            else t.text or ""
                        )
                        t.text = f"[await:{flag}]\n{body}"
                        s.commit()
                        _save_await_conversation_state(
                            t.id,
                            t.sender,
                            t.project_code,
                            t.text or "",
                            body,
                        )
                send_whatsapp_text(phone_id, sender, prompt)
                return ("", 200)

            # ---------------------------------------------------------
            # ORDER BUTTONS (ID MATCHING)
            # ---------------------------------------------------------

            if bid.startswith("order_item:"):
                tid = int(bid.split(":", 1)[1])
                with S2() as s:
                    t = s.get(T2, tid)
                    if t:
                        original_request = t.text or ""
                        t.text = f"[await:item]\n{original_request}"
                        s.commit()
                        _save_await_conversation_state(
                            t.id,
                            t.sender,
                            t.project_code,
                            t.text or "",
                            original_request,
                        )
                send_whatsapp_text(phone_id, sender, "Great — what item should we order?")
                return ("", 200)

            if bid.startswith("order_quantity:"):
                return _mark(
                    int(bid.split(":", 1)[1]),
                    "quantity",
                    "Okay — what quantity do we need?"
                )

            if bid.startswith("order_supplier:"):
                return _mark(
                    int(bid.split(":", 1)[1]),
                    "supplier",
                    "Got it — who should we source this from?"
                )

            if bid.startswith("order_delivery_date:"):
                return _mark(
                    int(bid.split(":", 1)[1]),
                    "delivery_date",
                    "When must this be delivered?"
                )

            if bid.startswith("order_drop_location:"):
                return _mark(
                    int(bid.split(":", 1)[1]),
                    "drop_location",
                    "Where should this be dropped on site?"
                )

    # -----------------------------------------------------------------
    # END OF BLOCK 5 — NEXT: MAIN MESSAGE LOOP (BLOCK 6)
    # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # MU11 — NON-MUTATING NORMAL-ROUTE RECOGNITION EVIDENCE
    # -----------------------------------------------------------------
    def has_deterministic_normal_route_recognition(raw_text: str) -> bool:
        t = raw_text or ""
        lower = t.lower()

        # Preserve the existing await-bypass compatibility path exactly.
        if any(w in lower for w in (
            "approve",
            "reject",
            "change the order",
            "change that order",
            "change order",
            "change it",
            "change it to",
        )):
            return True

        natural_order = parse_natural_order(t)
        if natural_order and not natural_order.get("missing_fields"):
            return True

        # Probe only existing authoritative recognition. These helpers do not
        # execute handlers or mutate the pending await.
        if is_new_stock_item_request(t):
            return True
        if parse_stock_command(t):
            return True
        if is_search_request(t):
            return True
        if classify_inspection(t):
            return True
        if classify_delay(t):
            return True
        meeting_recognition = _CORE_CONVERSATION.interpret_core(
            ConversationRequest(
                capability="core_recognition",
                text=t,
                context={"candidate": "meeting_creation"},
            )
        )
        if meeting_recognition.handled:
            return True

        return False

    # -----------------------------------------------------------------
    # MU13 — SHARED RESOLUTION + PERSISTENT CLARIFICATION ORCHESTRATION
    # -----------------------------------------------------------------
    def _mu13_sender_authorization(sender_wa: str) -> Optional[dict]:
        client_id, sender_project = _conversation_scope(sender_wa)
        with DBSession() as s:
            sender_user = (
                s.query(User)
                .filter(
                    User.wa_id == sender_wa,
                    User.active == True,
                )
                .first()
            )
            if not sender_user:
                return None

            projects = set()
            if sender_project:
                projects.add(sender_project)

            mapped_rows = (
                s.query(PMProjectMap.project_code)
                .filter(
                    PMProjectMap.pm_user_id == sender_user.id,
                    PMProjectMap.client_id == client_id,
                )
                .all()
            )
            for row in mapped_rows:
                project_code = row.project_code
                if project_code is not None:
                    project_code = str(project_code).strip() or None
                if project_code:
                    projects.add(project_code)

            return {
                "client_id": client_id,
                "sender_project_code": sender_project,
                "project_codes": sorted(projects),
            }

    def _mu13_project_records(authorization: Optional[dict]) -> list[dict]:
        if not authorization:
            return []
        records = []
        for project_code in authorization.get("project_codes") or []:
            code = str(project_code or "").strip()
            if not code:
                continue
            records.append(
                {
                    "id": code,
                    "label": code,
                    "labels": [code, f"project {code}"],
                }
            )
        return records

    def _mu13_authorized_reminder_records(
        sender_wa: str,
        action: str,
        authorization: Optional[dict] = None,
    ) -> list[dict]:
        authorization = authorization or _mu13_sender_authorization(sender_wa)
        if not authorization:
            return []

        allowed_projects = set(authorization.get("project_codes") or [])
        records = []
        with DBSession() as s:
            rows = (
                s.query(PMReminder)
                .filter(
                    (PMReminder.pm_wa == sender_wa)
                    | (PMReminder.recipient_wa == sender_wa)
                )
                .order_by(PMReminder.id.desc())
                .all()
            )

            for reminder in rows:
                if action == "acknowledge":
                    eligible = (
                        reminder.delivered_at is not None
                        and reminder.status != "cancelled"
                    )
                elif action == "cancel":
                    eligible = bool(reminder.active) and reminder.status == "active"
                elif action in ("snooze", "redirect"):
                    eligible = (
                        (bool(reminder.active) and reminder.status == "active")
                        or (
                            reminder.delivered_at is not None
                            and reminder.status != "cancelled"
                        )
                    )
                else:
                    eligible = False

                if not eligible:
                    continue

                project_code = reminder.project_code
                if project_code is not None:
                    project_code = str(project_code).strip() or None
                if project_code and project_code not in allowed_projects:
                    continue

                raw_label = (reminder.text or "").strip()
                display_label = raw_label or "Reminder"
                labels = [display_label]

                records.append(
                    {
                        "id": reminder.id,
                        "label": display_label,
                        "labels": labels,
                        "project_code": project_code,
                    }
                )

        return records

    def _mu13_authorized_person_records(
        sender_wa: str,
        authorization: Optional[dict] = None,
    ) -> list[dict]:
        authorization = authorization or _mu13_sender_authorization(sender_wa)
        if not authorization:
            return []

        client_id = int(authorization["client_id"])
        allowed_projects = set(authorization.get("project_codes") or [])

        with DBSession() as s:
            mapping_rows = (
                s.query(PMProjectMap.pm_user_id, PMProjectMap.project_code)
                .filter(PMProjectMap.client_id == client_id)
                .all()
            )
            mapped_projects: dict[int, set[str]] = {}
            for user_id, project_code in mapping_rows:
                code = str(project_code or "").strip()
                if code:
                    mapped_projects.setdefault(int(user_id), set()).add(code)

            users = (
                s.query(User)
                .filter(
                    User.client_id == client_id,
                    User.active == True,
                )
                .order_by(User.id.asc())
                .all()
            )

            records = []
            for user in users:
                projects = set(mapped_projects.get(int(user.id), set()))
                user_project = str(user.project_code or "").strip()
                if user_project:
                    projects.add(user_project)
                if user.wa_id == sender_wa:
                    projects.update(allowed_projects)

                if allowed_projects:
                    if user.wa_id != sender_wa and not (projects & allowed_projects):
                        continue
                elif user.wa_id != sender_wa:
                    continue

                labels = []
                for value in (
                    user.name,
                    user.subcontractor_name,
                    user.wa_id,
                ):
                    label = str(value or "").strip()
                    if label and label not in labels:
                        labels.append(label)

                if not labels:
                    continue

                records.append(
                    {
                        "id": user.wa_id,
                        "label": labels[0],
                        "labels": labels,
                        "project_codes": sorted(projects),
                    }
                )

        return records

    def _mu13_person_records_for_project(
        records: list[dict],
        project_code: Optional[str],
    ) -> list[dict]:
        project = str(project_code or "").strip()
        if not project:
            return list(records)
        return [
            record
            for record in records
            if project in set(record.get("project_codes") or [])
        ]

    def _mu13_intersect_persisted_records(
        persisted_records: list[dict],
        current_records: list[dict],
    ) -> list[dict]:
        current_by_id = {
            str(record.get("id")): record
            for record in current_records
            if record.get("id") is not None
        }
        available = []
        for persisted in persisted_records:
            if persisted.get("id") is None:
                continue
            current = current_by_id.get(str(persisted.get("id")))
            if not current:
                continue

            candidate = dict(persisted)

            if "project_code" in persisted or "project_code" in current:
                persisted_project = str(
                    persisted.get("project_code") or ""
                ).strip()
                current_project = str(
                    current.get("project_code") or ""
                ).strip()
                if persisted_project and not current_project:
                    continue
                candidate["project_code"] = current_project or None

            if "project_codes" in persisted or "project_codes" in current:
                persisted_projects = {
                    str(value).strip()
                    for value in (persisted.get("project_codes") or [])
                    if str(value).strip()
                }
                current_projects = {
                    str(value).strip()
                    for value in (current.get("project_codes") or [])
                    if str(value).strip()
                }
                shared_projects = persisted_projects & current_projects
                if persisted_projects and not shared_projects:
                    continue
                candidate["project_codes"] = sorted(shared_projects)

            available.append(candidate)

        return available

    def _mu13_authorization_within_persisted_scope(
        state: dict,
        candidate_metadata: dict,
        current_authorization: Optional[dict],
    ) -> Optional[dict]:
        if not current_authorization:
            return None

        persisted_scope = candidate_metadata.get("authorization_scope") or {}
        if str(persisted_scope.get("sender") or "") != str(state["sender"]):
            return None
        try:
            persisted_client_id = int(persisted_scope["client_id"])
            current_client_id = int(current_authorization["client_id"])
            state_client_id = int(state["client_id"])
        except (KeyError, TypeError, ValueError):
            return None
        if not (
            persisted_client_id == current_client_id == state_client_id
        ):
            return None

        persisted_projects = {
            str(value).strip()
            for value in (persisted_scope.get("project_codes") or [])
            if str(value).strip()
        }
        current_projects = {
            str(value).strip()
            for value in (current_authorization.get("project_codes") or [])
            if str(value).strip()
        }
        narrowed = dict(current_authorization)
        narrowed["project_codes"] = sorted(
            persisted_projects & current_projects
        )

        sender_project = str(
            current_authorization.get("sender_project_code") or ""
        ).strip()
        narrowed["sender_project_code"] = (
            sender_project
            if sender_project in persisted_projects
            else None
        )
        return narrowed

    def _mu13_resolve_records(
        query_text: str,
        records: list[dict],
        allow_single_unqualified: bool = False,
    ) -> dict:
        result = _CORE_CONVERSATION.interpret_core(
            ConversationRequest(
                capability="record_resolution",
                text=query_text or "",
                context={
                    "candidate": "text_reference",
                    "records": records,
                    "resolve_single_unqualified": allow_single_unqualified,
                },
            )
        )
        resolution = str(
            result.metadata.get("resolution") or "not_found"
        ).strip().lower()
        if resolution == "resolved":
            return {
                "status": "resolved",
                "record_id": result.entities.get("record_id"),
            }
        if resolution == "ambiguous":
            matches = result.metadata.get("matches")
            return {
                "status": "ambiguous",
                "matches": matches if isinstance(matches, list) else [],
            }
        return {"status": "not_found", "matches": []}

    def _mu13_resolve_person(query_text: str, records: list[dict]) -> dict:
        target = str(query_text or "").strip()
        digits = re.sub(r"\D", "", target)
        if len(digits) >= 7:
            exact = [
                record
                for record in records
                if re.sub(r"\D", "", str(record.get("id") or "")) == digits
            ]
            if len(exact) == 1:
                return {"status": "resolved", "record_id": exact[0]["id"]}
            if len(exact) > 1:
                return {"status": "ambiguous", "matches": exact}
            return {"status": "not_found", "matches": []}

        return _mu13_resolve_records(target, records)

    def _mu13_reminder_reference_text(
        raw_text: str,
        action: str,
        strip_command: bool = True,
    ) -> str:
        working = str(raw_text or "").strip()
        if action == "redirect":
            target_match = re.search(
                r"\bto\s+(.+)$",
                working,
                flags=re.IGNORECASE,
            )
            if target_match:
                working = working[:target_match.start()]

        if strip_command:
            # Recognition already happened through the authoritative lifecycle
            # classifier; remove only its leading command token for reference
            # matching rather than re-recognizing lifecycle language here.
            working = re.sub(
                r"^\s*(?:please\s+)?[^\s]+\b",
                "",
                working,
                count=1,
                flags=re.IGNORECASE,
            )

        working = re.sub(
            r"\breminders?\s*#?\s*\d*\b",
            " ",
            working,
            flags=re.IGNORECASE,
        )

        if action == "snooze":
            working = re.sub(
                r"\bfor\s+\d+\s+(?:minutes?|hours?|days?|weeks?)\b.*$",
                " ",
                working,
                flags=re.IGNORECASE,
            )
            working = re.sub(
                r"\buntil\s+.+$",
                " ",
                working,
                flags=re.IGNORECASE,
            )

        return " ".join(working.strip(" .,:;-_").split())

    def _mu13_strip_project_reference(
        reference_text: str,
        project_code: Optional[str],
    ) -> str:
        working = str(reference_text or "")
        project = str(project_code or "").strip()
        if project:
            working = re.sub(
                rf"\bproject\s+{re.escape(project)}\b",
                " ",
                working,
                flags=re.IGNORECASE,
            )
            working = re.sub(
                rf"\b{re.escape(project)}\b",
                " ",
                working,
                flags=re.IGNORECASE,
            )
        working = re.sub(r"\babout\b", " ", working, flags=re.IGNORECASE)
        return " ".join(working.strip(" .,:;-_").split())

    def _mu13_redirect_target_text(raw_text: str) -> str:
        target_match = re.search(
            r"\bto\s+(.+)$",
            raw_text or "",
            flags=re.IGNORECASE,
        )
        return (
            target_match.group(1).strip(" .")
            if target_match
            else ""
        )

    def _mu13_sender_timezone(sender_wa: str) -> str:
        return _mu15_sender_datetime_configuration(sender_wa)["timezone"]

    def _mu15_sender_datetime_configuration(sender_wa: str) -> dict:
        with DBSession() as s:
            sender_user = (
                s.query(User)
                .filter(
                    User.wa_id == sender_wa,
                    User.active == True,
                )
                .first()
            )
            if sender_user:
                date_order = str(sender_user.date_order or "").strip().lower()
                time_format = str(sender_user.time_format or "").strip().lower()
                date_display = str(sender_user.date_display or "").strip().lower()
                return {
                    "timezone": sender_user.timezone or "America/New_York",
                    "date_order": (
                        date_order
                        if date_order in ("month_first", "day_first")
                        else None
                    ),
                    "time_format": time_format if time_format in ("12h", "24h") else "12h",
                    "date_display": (
                        date_display
                        if date_display in ("month_first", "day_first")
                        else "month_first"
                    ),
                }
        return {
            "timezone": "America/New_York",
            "date_order": "month_first",
            "time_format": "12h",
            "date_display": "month_first",
        }

    def _mu15_execute_reminder_creation(
        sender_wa: str,
        raw_text: str,
        parsed_reminder: dict,
    ) -> bool:
        user_info = get_user_role(sender_wa) or {}
        payload = {
            "pm_wa": sender_wa,
            "recipient_wa": sender_wa,
            "project_code": user_info.get("project_code"),
            "text": raw_text,
            "rule": parsed_reminder["rule"],
            "timezone": parsed_reminder["timezone"],
            "next_run": parsed_reminder["next_run"],
            "recurring": parsed_reminder["recurring"],
            "recurrence_rule": parsed_reminder["recurrence_rule"],
            "recurrence_interval": parsed_reminder["recurrence_interval"],
            "recurrence_seconds": parsed_reminder["recurrence_seconds"],
            "recurrence_anchor_day": parsed_reminder["recurrence_anchor_day"],
        }
        result = create_pm_reminder(payload)
        if result.get("status") != "ok":
            send_whatsapp_text(
                phone_id, sender_wa, "The reminder could not be created."
            )
            return False
        config = _mu15_sender_datetime_configuration(sender_wa)
        displayed = format_configured_datetime(
            result["next_run"],
            config["timezone"],
            config["date_display"],
            config["time_format"],
        )
        recurrence_note = (
            f" Recurs {result['recurrence_rule']}."
            if result.get("recurring")
            else ""
        )
        send_whatsapp_text(
            phone_id,
            sender_wa,
            f"Reminder #{result['id']} scheduled for {displayed}.{recurrence_note}",
        )
        return True

    def _mu15_persist_datetime_clarification(
        message: dict,
        sender_wa: str,
        raw_text: str,
        config: dict,
        options: list[dict],
        match_span: tuple[int, int],
    ) -> dict:
        client_id, project_code = _conversation_scope(sender_wa)
        identity = str(message.get("id") or message.get("timestamp") or raw_text)
        key = "reminder-datetime:" + hashlib.sha256(
            f"{sender_wa}|{identity}".encode("utf-8")
        ).hexdigest()
        return save_pending_conversation_state({
            "client_id": client_id,
            "sender": sender_wa,
            "project_code": project_code,
            "state_kind": "clarification",
            "expected_field": "calendar_date",
            "original_request": raw_text,
            "structured_context": {
                "timezone": config["timezone"],
                "match_start": match_span[0],
                "match_end": match_span[1],
            },
            "candidate_metadata": {"date_candidates": options},
            "continuation": {"kind": "reminder_datetime"},
            "continuation_key": key,
        })

    def _mu15_send_date_choices(sender_wa: str, options: list[dict]) -> None:
        lines = ["That date is ambiguous. Reply with the intended date:"]
        lines.extend(f"- {option['label']}" for option in options)
        send_whatsapp_text(phone_id, sender_wa, "\n".join(lines))

    def _mu15_existing_datetime_clarification(
        state: dict,
        raw_text: str,
    ) -> bool:
        continuation = state.get("continuation") or {}
        if continuation.get("kind") != "reminder_datetime":
            return False
        if has_deterministic_normal_route_recognition(raw_text):
            return False
        touched = touch_conversation_state_activity(
            state["id"], state["sender"], state["client_id"],
            state.get("project_code"),
        )
        if touched.get("status_result") != "touched":
            return True
        options = (state.get("candidate_metadata") or {}).get("date_candidates") or []
        normalized = str(raw_text or "").strip().lower()
        selected = next(
            (
                option for option in options
                if normalized == option.get("date_order", "").replace("_", " ")
            ),
            None,
        )
        if selected is None:
            reference_date = _inspection_reference_date(
                (state.get("structured_context") or {}).get("timezone")
            )
            result = _CORE_CONVERSATION.interpret_core(
                ConversationRequest(
                    capability="shared_datetime",
                    text=raw_text or "",
                    context={
                        "candidate": "calendar_date",
                        "reference_date": reference_date.isoformat(),
                    },
                )
            )
            if result.handled and result.metadata.get("valid"):
                selected_id = dt.date(
                    result.metadata["year"],
                    result.metadata["month"],
                    result.metadata["day"],
                ).isoformat()
                selected = next(
                    (option for option in options if option.get("id") == selected_id),
                    None,
                )
        if selected is None:
            _mu15_send_date_choices(state["sender"], options)
            return True

        claimed = claim_conversation_state_continuation(
            state["id"], state["sender"], state["client_id"],
            state.get("project_code"),
        )
        if claimed.get("status_result") != "claimed":
            return True
        context = claimed.get("structured_context") or {}
        original = claimed.get("original_request") or ""
        try:
            start = int(context["match_start"])
            end = int(context["match_end"])
        except (KeyError, TypeError, ValueError):
            advance_conversation_state_continuation(
                claimed["id"], claimed["sender"], claimed["client_id"],
                claimed.get("project_code"), claimed.get("expected_field"),
            )
            return True
        completed_text = original[:start] + selected["id"] + original[end:]
        config = _mu15_sender_datetime_configuration(claimed["sender"])
        parsed = parse_pm_reminder_request(
            completed_text,
            timezone_name=config["timezone"],
            date_order=config["date_order"],
        )
        if parsed and _mu15_execute_reminder_creation(
            claimed["sender"], original, parsed,
        ):
            resolve_conversation_state(
                claimed["id"], claimed["sender"], claimed["client_id"],
                claimed.get("project_code"),
            )
        else:
            advance_conversation_state_continuation(
                claimed["id"], claimed["sender"], claimed["client_id"],
                claimed.get("project_code"), claimed.get("expected_field"),
            )
        return True

    def _mu13_message_continuation_key(
        message: dict,
        sender_wa: str,
        action: str,
        raw_text: str,
    ) -> str:
        identity = str(message.get("id") or message.get("timestamp") or "").strip()
        if not identity:
            identity = (
                f"{sender_wa}|{action}|{raw_text}|"
                f"{dt.datetime.utcnow().isoformat()}"
            )
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        return f"reminder-lifecycle:{digest}"

    def _mu13_state_payload(
        sender_wa: str,
        authorization: dict,
        state_kind: str,
        expected_field: Optional[str],
        original_request: str,
        structured_context: dict,
        candidate_metadata: dict,
        continuation_key: str,
        continuation: dict,
    ) -> dict:
        _, scope_project = _conversation_scope(sender_wa)
        return {
            "client_id": int(authorization["client_id"]),
            "sender": sender_wa,
            "project_code": scope_project,
            "state_kind": state_kind,
            "expected_field": expected_field,
            "original_request": original_request or "",
            "structured_context": structured_context,
            "candidate_metadata": candidate_metadata,
            "continuation": continuation,
            "continuation_key": continuation_key,
        }

    def _mu13_refresh_state(
        state: dict,
        expected_field: Optional[str],
        structured_context: dict,
    ) -> dict:
        authorization_scope = (
            state.get("candidate_metadata", {}).get("authorization_scope") or {}
        )
        payload = {
            "client_id": state["client_id"],
            "sender": state["sender"],
            "project_code": state.get("project_code"),
            "state_kind": state["state_kind"],
            "expected_field": expected_field,
            "original_request": state.get("original_request") or "",
            "structured_context": structured_context,
            "candidate_metadata": state.get("candidate_metadata") or {},
            "continuation": state.get("continuation") or {},
            "continuation_key": state["continuation_key"],
        }
        if authorization_scope and int(
            authorization_scope.get("client_id") or state["client_id"]
        ) != int(state["client_id"]):
            return {"status": "error", "code": "scope_conflict"}
        return save_pending_conversation_state(payload)

    def _mu13_candidate_by_id(records: list[dict], record_id) -> Optional[dict]:
        target = str(record_id)
        return next(
            (
                record
                for record in records
                if record.get("id") is not None
                and str(record.get("id")) == target
            ),
            None,
        )

    def _mu13_send_reminder_choices(
        sender_wa: str,
        records: list[dict],
        no_match: bool = False,
    ) -> None:
        if not records:
            send_whatsapp_text(
                phone_id,
                sender_wa,
                "No currently authorized reminder remains for that clarification.",
            )
            return
        heading = (
            "I couldn't match that reminder. Reply with part of its subject:"
            if no_match
            else "More than one reminder matches. Reply with part of its subject:"
        )
        lines = [heading]
        for record in records[:10]:
            label = str(record.get("label") or "Reminder").strip()
            project_code = str(record.get("project_code") or "").strip()
            if len(label) > 90:
                label = label[:87] + "..."
            suffix = f" [{project_code}]" if project_code else ""
            lines.append(f"- #{record['id']}: {label}{suffix}")
        send_whatsapp_text(phone_id, sender_wa, "\n".join(lines))

    def _mu13_send_person_choices(
        sender_wa: str,
        records: list[dict],
        no_match: bool = False,
    ) -> None:
        if not records:
            send_whatsapp_text(
                phone_id,
                sender_wa,
                "No linked active user is currently authorized for that reminder.",
            )
            return
        heading = (
            "I couldn't match that person. Reply with the person's name:"
            if no_match
            else "More than one person matches. Reply with the person's name:"
        )
        lines = [heading]
        for record in records[:10]:
            lines.append(f"- {record.get('label') or record.get('id')}")
        send_whatsapp_text(phone_id, sender_wa, "\n".join(lines))

    def _mu13_send_action_not_found(sender_wa: str, action: str) -> None:
        messages = {
            "acknowledge": "No delivered reminder was found to acknowledge.",
            "snooze": "No active reminder was found to snooze.",
            "redirect": "No active reminder was found to reassign.",
            "cancel": "No active reminder was found to cancel.",
        }
        send_whatsapp_text(
            phone_id,
            sender_wa,
            messages.get(action, "No matching reminder was found."),
        )

    def _mu13_execute_reminder_action(
        sender_wa: str,
        action: str,
        structured_context: dict,
    ) -> bool:
        try:
            reminder_id = int(structured_context["reminder_id"])
        except (KeyError, TypeError, ValueError):
            return False

        if action == "acknowledge":
            result = acknowledge_pm_reminder(
                sender_wa,
                reminder_id=reminder_id,
            )
            if result.get("status") == "not_found":
                _mu13_send_action_not_found(sender_wa, action)
                return False
            send_whatsapp_text(
                phone_id,
                sender_wa,
                f"Reminder #{result['id']} acknowledged.",
            )
            return True

        if action == "snooze":
            until_text = str(structured_context.get("snooze_until") or "").strip()
            try:
                until_utc = dt.datetime.fromisoformat(until_text)
            except (TypeError, ValueError):
                return False
            result = snooze_pm_reminder(
                sender_wa,
                until_utc,
                reminder_id=reminder_id,
            )
            if result.get("status") == "not_found":
                _mu13_send_action_not_found(sender_wa, action)
                return False
            if result.get("status") == "error":
                send_whatsapp_text(
                    phone_id,
                    sender_wa,
                    "The reminder could not be snoozed to that time.",
                )
                return False
            send_whatsapp_text(
                phone_id,
                sender_wa,
                f"Reminder #{result['id']} postponed.",
            )
            return True

        if action == "redirect":
            recipient_wa = str(
                structured_context.get("recipient_wa") or ""
            ).strip()
            if not recipient_wa:
                return False
            result = redirect_pm_reminder(
                sender_wa,
                recipient_wa,
                reminder_id=reminder_id,
            )
            if result.get("status") == "not_found":
                _mu13_send_action_not_found(sender_wa, action)
                return False
            send_whatsapp_text(
                phone_id,
                sender_wa,
                (
                    f"Reminder #{result['id']} reassigned "
                    f"to {result['recipient_wa']}."
                ),
            )
            return True

        if action == "cancel":
            result = cancel_pm_reminder(
                sender_wa,
                reminder_id=reminder_id,
            )
            if result.get("status") == "not_found":
                _mu13_send_action_not_found(sender_wa, action)
                return False
            send_whatsapp_text(
                phone_id,
                sender_wa,
                f"Reminder #{result['id']} cancelled.",
            )
            return True

        return False

    def _mu13_revalidate_selected_continuation(
        state: dict,
        action: str,
        structured_context: dict,
        candidate_metadata: dict,
    ) -> bool:
        authorization = _mu13_authorization_within_persisted_scope(
            state,
            candidate_metadata,
            _mu13_sender_authorization(state["sender"]),
        )
        if not authorization:
            return False

        persisted_reminders = candidate_metadata.get("reminder_candidates") or []
        current_reminders = _mu13_authorized_reminder_records(
            state["sender"],
            action,
            authorization,
        )
        current_reminders = _mu13_intersect_persisted_records(
            persisted_reminders,
            current_reminders,
        )
        reminder = _mu13_candidate_by_id(
            current_reminders,
            structured_context.get("reminder_id"),
        )
        if not reminder:
            return False

        selected_project = str(
            structured_context.get("selected_project_code") or ""
        ).strip()
        if selected_project and reminder.get("project_code") != selected_project:
            return False

        if action == "redirect":
            persisted_people = candidate_metadata.get("person_candidates") or []
            current_people = _mu13_authorized_person_records(
                state["sender"],
                authorization,
            )
            current_people = _mu13_intersect_persisted_records(
                persisted_people,
                current_people,
            )
            current_people = _mu13_person_records_for_project(
                current_people,
                reminder.get("project_code"),
            )
            recipient = _mu13_candidate_by_id(
                current_people,
                structured_context.get("recipient_wa"),
            )
            if not recipient:
                return False

        return True

    def _mu13_release_claim(state: dict) -> None:
        if state.get("state_kind") == "clarification":
            advance_conversation_state_continuation(
                state["id"],
                state["sender"],
                state["client_id"],
                state.get("project_code"),
                state.get("expected_field"),
            )
        else:
            resolve_conversation_state(
                state["id"],
                state["sender"],
                state["client_id"],
                state.get("project_code"),
            )

    def _mu13_claim_revalidate_execute(
        state: dict,
        action: str,
        structured_context: dict,
    ) -> bool:
        claimed = claim_conversation_state_continuation(
            state["id"],
            state["sender"],
            state["client_id"],
            state.get("project_code"),
        )
        if claimed.get("status_result") != "claimed":
            return True

        candidate_metadata = claimed.get("candidate_metadata") or {}
        if not _mu13_revalidate_selected_continuation(
            claimed,
            action,
            structured_context,
            candidate_metadata,
        ):
            _mu13_release_claim(claimed)
            send_whatsapp_text(
                phone_id,
                claimed["sender"],
                "That selection is no longer authorized for this clarification.",
            )
            return True

        succeeded = _mu13_execute_reminder_action(
            claimed["sender"],
            action,
            structured_context,
        )
        if succeeded:
            resolve_conversation_state(
                claimed["id"],
                claimed["sender"],
                claimed["client_id"],
                claimed.get("project_code"),
            )
        else:
            _mu13_release_claim(claimed)
        return True

    def _mu13_persist_reminder_state(
        sender_wa: str,
        authorization: dict,
        message: dict,
        raw_text: str,
        action: str,
        state_kind: str,
        expected_field: Optional[str],
        structured_context: dict,
        reminder_candidates: list[dict],
        person_candidates: list[dict],
        project_candidates: list[dict],
    ) -> dict:
        continuation_key = _mu13_message_continuation_key(
            message,
            sender_wa,
            action,
            raw_text,
        )
        candidate_metadata = {
            "reminder_candidates": reminder_candidates,
            "person_candidates": person_candidates,
            "project_candidates": project_candidates,
            "authorization_scope": {
                "sender": sender_wa,
                "client_id": int(authorization["client_id"]),
                "project_codes": list(authorization.get("project_codes") or []),
            },
        }
        continuation = {
            "kind": "reminder_lifecycle",
            "action": action,
        }
        return save_pending_conversation_state(
            _mu13_state_payload(
                sender_wa,
                authorization,
                state_kind,
                expected_field,
                raw_text,
                structured_context,
                candidate_metadata,
                continuation_key,
                continuation,
            )
        )

    def _mu13_initial_reminder_action(
        message: dict,
        sender_wa: str,
        raw_text: str,
        action: str,
        reminder_id: Optional[int],
    ) -> bool:
        structured_context = {"action": action}

        if action == "snooze":
            until_utc = parse_pm_reminder_snooze_until(
                raw_text,
                _mu13_sender_timezone(sender_wa),
            )
            if until_utc is None:
                send_whatsapp_text(
                    phone_id,
                    sender_wa,
                    (
                        "Include how long to snooze the reminder. "
                        "For example: Snooze reminder 12 for 30 minutes."
                    ),
                )
                return True
            structured_context["snooze_until"] = until_utc.isoformat()

        if action == "redirect":
            structured_context["recipient_target"] = (
                _mu13_redirect_target_text(raw_text)
            )

        authorization = _mu13_sender_authorization(sender_wa)
        if not authorization:
            _mu13_send_action_not_found(sender_wa, action)
            return True

        project_candidates = _mu13_project_records(authorization)
        reminder_candidates = _mu13_authorized_reminder_records(
            sender_wa,
            action,
            authorization,
        )
        person_candidates = (
            _mu13_authorized_person_records(sender_wa, authorization)
            if action == "redirect"
            else []
        )

        if reminder_id is not None:
            selected = _mu13_candidate_by_id(reminder_candidates, reminder_id)
            if not selected:
                _mu13_send_action_not_found(sender_wa, action)
                return True

            structured_context["reminder_id"] = int(reminder_id)
            structured_context["selected_project_code"] = (
                selected.get("project_code")
            )

            if action == "redirect":
                scoped_people = _mu13_person_records_for_project(
                    person_candidates,
                    selected.get("project_code"),
                )
                target_resolution = _mu13_resolve_person(
                    structured_context.get("recipient_target") or "",
                    scoped_people,
                )
                if target_resolution["status"] != "resolved":
                    if not scoped_people:
                        send_whatsapp_text(
                            phone_id,
                            sender_wa,
                            (
                                "Name a linked active user or WhatsApp number. "
                                "For example: Reassign reminder 12 to John Smith."
                            ),
                        )
                        return True
                    state = _mu13_persist_reminder_state(
                        sender_wa,
                        authorization,
                        message,
                        raw_text,
                        action,
                        "clarification",
                        "recipient_reference",
                        structured_context,
                        [selected],
                        scoped_people,
                        project_candidates,
                    )
                    if state.get("status_result") == "inactive":
                        return True
                    _mu13_send_person_choices(
                        sender_wa,
                        scoped_people,
                        no_match=(target_resolution["status"] == "not_found"),
                    )
                    return True
                structured_context["recipient_wa"] = target_resolution["record_id"]

            return _mu13_execute_reminder_action(
                sender_wa,
                action,
                structured_context,
            )

        selected_project = None
        project_resolution = _mu13_resolve_records(
            raw_text,
            project_candidates,
        )
        if project_resolution["status"] == "resolved":
            selected_project = str(project_resolution["record_id"])
            structured_context["selected_project_code"] = selected_project
            reminder_candidates = [
                record
                for record in reminder_candidates
                if record.get("project_code") == selected_project
            ]
            if action == "redirect":
                person_candidates = _mu13_person_records_for_project(
                    person_candidates,
                    selected_project,
                )

        if not reminder_candidates:
            if (
                action == "acknowledge"
                and "reminder" not in raw_text.lower()
                and reminder_id is None
            ):
                return False
            _mu13_send_action_not_found(sender_wa, action)
            return True

        reference_text = _mu13_strip_project_reference(
            _mu13_reminder_reference_text(raw_text, action),
            selected_project,
        )
        reminder_resolution = _mu13_resolve_records(
            reference_text,
            reminder_candidates,
            allow_single_unqualified=True,
        )

        if reminder_resolution["status"] != "resolved":
            state = _mu13_persist_reminder_state(
                sender_wa,
                authorization,
                message,
                raw_text,
                action,
                "clarification",
                "reminder_record",
                structured_context,
                reminder_candidates,
                person_candidates,
                project_candidates,
            )
            if state.get("status_result") == "inactive":
                return True
            _mu13_send_reminder_choices(
                sender_wa,
                reminder_candidates,
                no_match=(reminder_resolution["status"] == "not_found"),
            )
            return True

        selected = _mu13_candidate_by_id(
            reminder_candidates,
            reminder_resolution["record_id"],
        )
        if not selected:
            return True
        structured_context["reminder_id"] = int(selected["id"])
        structured_context["selected_project_code"] = selected.get("project_code")

        if action == "redirect":
            scoped_people = _mu13_person_records_for_project(
                person_candidates,
                selected.get("project_code"),
            )
            target_resolution = _mu13_resolve_person(
                structured_context.get("recipient_target") or "",
                scoped_people,
            )
            if target_resolution["status"] != "resolved":
                if not scoped_people:
                    send_whatsapp_text(
                        phone_id,
                        sender_wa,
                        (
                            "Name a linked active user or WhatsApp number. "
                            "For example: Reassign reminder 12 to John Smith."
                        ),
                    )
                    return True
                state = _mu13_persist_reminder_state(
                    sender_wa,
                    authorization,
                    message,
                    raw_text,
                    action,
                    "clarification",
                    "recipient_reference",
                    structured_context,
                    [selected],
                    scoped_people,
                    project_candidates,
                )
                if state.get("status_result") == "inactive":
                    return True
                _mu13_send_person_choices(
                    sender_wa,
                    scoped_people,
                    no_match=(target_resolution["status"] == "not_found"),
                )
                return True
            structured_context["recipient_wa"] = target_resolution["record_id"]
            person_candidates = scoped_people

        state = _mu13_persist_reminder_state(
            sender_wa,
            authorization,
            message,
            raw_text,
            action,
            "continuation",
            "ready",
            structured_context,
            [selected],
            person_candidates,
            project_candidates,
        )
        if state.get("status_result") == "inactive":
            return True
        return _mu13_claim_revalidate_execute(
            state,
            action,
            structured_context,
        )

    def _mu13_existing_reminder_clarification(
        state: dict,
        raw_text: str,
    ) -> bool:
        continuation = state.get("continuation") or {}
        if continuation.get("kind") != "reminder_lifecycle":
            return False

        if has_deterministic_normal_route_recognition(raw_text):
            return False

        touch_conversation_state_activity(
            state["id"],
            state["sender"],
            state["client_id"],
            state.get("project_code"),
        )

        action = str(continuation.get("action") or "").strip()
        if action not in ("acknowledge", "snooze", "redirect", "cancel"):
            return True

        candidate_metadata = state.get("candidate_metadata") or {}
        authorization = _mu13_authorization_within_persisted_scope(
            state,
            candidate_metadata,
            _mu13_sender_authorization(state["sender"]),
        )
        if not authorization:
            return True

        persisted_reminders = candidate_metadata.get("reminder_candidates") or []
        current_reminders = _mu13_authorized_reminder_records(
            state["sender"],
            action,
            authorization,
        )
        available_reminders = _mu13_intersect_persisted_records(
            persisted_reminders,
            current_reminders,
        )
        structured_context = dict(state.get("structured_context") or {})
        expected_field = str(state.get("expected_field") or "").strip()

        if expected_field == "reminder_record":
            persisted_projects = candidate_metadata.get("project_candidates") or []
            current_projects = _mu13_project_records(authorization)
            available_projects = _mu13_intersect_persisted_records(
                persisted_projects,
                current_projects,
            )
            followup_project = _mu13_resolve_records(
                raw_text,
                available_projects,
            )
            selected_project = None
            if followup_project["status"] == "resolved":
                selected_project = str(followup_project["record_id"])
                available_reminders = [
                    record
                    for record in available_reminders
                    if record.get("project_code") == selected_project
                ]

            followup_action = classify_pm_reminder_lifecycle(raw_text)
            reference_text = _mu13_strip_project_reference(
                _mu13_reminder_reference_text(
                    raw_text,
                    action,
                    strip_command=(followup_action == action),
                ),
                selected_project,
            )
            resolution = _mu13_resolve_records(
                reference_text,
                available_reminders,
                allow_single_unqualified=bool(selected_project),
            )
            if resolution["status"] != "resolved":
                _mu13_send_reminder_choices(
                    state["sender"],
                    available_reminders,
                    no_match=(resolution["status"] == "not_found"),
                )
                return True

            selected = _mu13_candidate_by_id(
                available_reminders,
                resolution["record_id"],
            )
            if not selected:
                return True
            structured_context["reminder_id"] = int(selected["id"])
            structured_context["selected_project_code"] = selected.get("project_code")

            if action == "redirect":
                persisted_people = candidate_metadata.get("person_candidates") or []
                current_people = _mu13_authorized_person_records(
                    state["sender"],
                    authorization,
                )
                available_people = _mu13_intersect_persisted_records(
                    persisted_people,
                    current_people,
                )
                available_people = _mu13_person_records_for_project(
                    available_people,
                    selected.get("project_code"),
                )
                target_resolution = _mu13_resolve_person(
                    structured_context.get("recipient_target") or "",
                    available_people,
                )
                if target_resolution["status"] != "resolved":
                    updated = _mu13_refresh_state(
                        state,
                        "recipient_reference",
                        structured_context,
                    )
                    if updated.get("status_result") == "inactive":
                        return True
                    _mu13_send_person_choices(
                        state["sender"],
                        available_people,
                        no_match=(target_resolution["status"] == "not_found"),
                    )
                    return True
                structured_context["recipient_wa"] = target_resolution["record_id"]

            updated = _mu13_refresh_state(
                state,
                expected_field,
                structured_context,
            )
            if updated.get("status_result") == "inactive":
                return True
            return _mu13_claim_revalidate_execute(
                updated,
                action,
                structured_context,
            )

        if expected_field == "recipient_reference" and action == "redirect":
            selected = _mu13_candidate_by_id(
                available_reminders,
                structured_context.get("reminder_id"),
            )
            if not selected:
                _mu13_send_reminder_choices(
                    state["sender"],
                    available_reminders,
                    no_match=True,
                )
                return True

            persisted_people = candidate_metadata.get("person_candidates") or []
            current_people = _mu13_authorized_person_records(
                state["sender"],
                authorization,
            )
            available_people = _mu13_intersect_persisted_records(
                persisted_people,
                current_people,
            )
            available_people = _mu13_person_records_for_project(
                available_people,
                selected.get("project_code"),
            )
            resolution = _mu13_resolve_person(raw_text, available_people)
            if resolution["status"] != "resolved":
                _mu13_send_person_choices(
                    state["sender"],
                    available_people,
                    no_match=(resolution["status"] == "not_found"),
                )
                return True

            structured_context["recipient_wa"] = resolution["record_id"]
            updated = _mu13_refresh_state(
                state,
                expected_field,
                structured_context,
            )
            if updated.get("status_result") == "inactive":
                return True
            return _mu13_claim_revalidate_execute(
                updated,
                action,
                structured_context,
            )

        return True

    # -----------------------------------------------------------------
    # MAIN MESSAGE LOOP — W2 CLEAN REBUILD
    # -----------------------------------------------------------------

    for m in msgs:
        sender = m.get("from") or sender
        mtype = m.get("type")
        text = None
        attachment = None

        # -------------------------------------------------------------
        # MEDIA HANDLING
        # -------------------------------------------------------------
        if mtype == "text":
            text = (m.get("text") or {}).get("body")

        elif mtype in ("image", "document", "audio", "video"):
            meta = m.get(mtype, {}) or {}
            mid = meta.get("id")
            attachment = {
                "url": f"whatsapp_media://{mtype}/{mid}" if mid else None,
                "mime": meta.get("mime_type"),
                "name": meta.get("filename"),
            }
            text = meta.get("caption")

        # >>> FEATURE_3_REMINDER_WEBHOOK_START — LIFECYCLE + CREATION V6.1 <<<

        if text:
            active_conversation_state = _get_active_conversation_state(sender)
            lifecycle_action = _conversation_lifecycle_action(text)
            if active_conversation_state and lifecycle_action:
                if _retire_active_conversation_state(
                    active_conversation_state,
                    lifecycle_action,
                ):
                    lifecycle_messages = {
                        "cancelled": "Conversation cancelled.",
                        "restarted": "Started over. Send your new request.",
                        "abandoned": "Okay, I won't continue that request.",
                    }
                    send_whatsapp_text(
                        phone_id,
                        sender,
                        lifecycle_messages[lifecycle_action],
                    )
                return ("", 200)
            if (
                active_conversation_state
                and active_conversation_state.get("state_kind") == "clarification"
                and _mu15_existing_datetime_clarification(
                    active_conversation_state,
                    text,
                )
            ):
                return ("", 200)
            if (
                active_conversation_state
                and active_conversation_state.get("state_kind") == "clarification"
            ):
                if _mu13_existing_reminder_clarification(
                    active_conversation_state,
                    text,
                ):
                    return ("", 200)

            reminder_action = classify_pm_reminder_lifecycle(text)
            reminder_id = _pm_reminder_id_from_text(text)

            if reminder_action:
                if _mu13_initial_reminder_action(
                    m,
                    sender,
                    text,
                    reminder_action,
                    reminder_id,
                ):
                    return ("", 200)

            if classify_pm_reminder(text):
                datetime_config = _mu15_sender_datetime_configuration(sender)
                sender_timezone = datetime_config["timezone"]
                reference_date = _inspection_reference_date(sender_timezone)
                date_options = (
                    ambiguous_calendar_date_options(text, reference_date)
                    if datetime_config["date_order"] is None
                    else []
                )
                if date_options:
                    ambiguous_result = _CORE_CONVERSATION.interpret_core(
                        ConversationRequest(
                            capability="shared_datetime",
                            text=text,
                            context={
                                "candidate": "calendar_date",
                                "reference_date": reference_date.isoformat(),
                            },
                        )
                    )
                    metadata = ambiguous_result.metadata
                    state = _mu15_persist_datetime_clarification(
                        m,
                        sender,
                        text,
                        datetime_config,
                        date_options,
                        (int(metadata["match_start"]), int(metadata["match_end"])),
                    )
                    if state.get("active"):
                        _mu15_send_date_choices(sender, date_options)
                    return ("", 200)

                parsed_reminder = parse_pm_reminder_request(
                    text,
                    timezone_name=sender_timezone,
                    date_order=datetime_config["date_order"],
                )
                if not parsed_reminder:
                    send_whatsapp_text(
                        phone_id,
                        sender,
                        (
                            "Include when the reminder should run. "
                            "For example: Remind me tomorrow at 9 AM "
                            "to call the inspector."
                        ),
                    )
                    return ("", 200)

                _mu15_execute_reminder_creation(sender, text, parsed_reminder)
                return ("", 200)

        # >>> FEATURE_3_REMINDER_WEBHOOK_END <<<


        # -------------------------------------------------------------
        # AUTO-FIX FOR PRIOR BAD TASKS (PRESERVED FROM FRIDAY)
        # -------------------------------------------------------------
        with DBSession() as s:
            bad = (
                s.query(Task)
                .filter(Task.id == 97, Task.status == "open")
                .first()
            )
            if bad:
                bad.status = "done"
                bad.text = f"[autoclosed:{dt.datetime.utcnow().isoformat()}]"
                bad.last_updated = dt.datetime.utcnow()
                s.commit()

        # -------------------------------------------------------------
        # CHECK FOR PERSISTENT CONVERSATION STATE / LEGACY AWAIT
        # -------------------------------------------------------------
        if text:
            deterministic_recognition = (
                has_deterministic_normal_route_recognition(text)
            )
            pending_state = _get_active_conversation_state(sender)

            with DBSession() as s:
                awaiting = None

                # Persistent generic state is discovered first. Existing await
                # Tasks remain compatibility/business-state records only.
                if pending_state and pending_state.get("state_kind") == "await":
                    continuation = pending_state.get("continuation") or {}
                    try:
                        source_record_id = int(
                            continuation.get("source_record_id")
                        )
                    except (TypeError, ValueError):
                        source_record_id = None

                    if source_record_id is not None:
                        candidate_await = (
                            s.query(Task)
                            .filter(
                                Task.id == source_record_id,
                                Task.sender == sender,
                                Task.client_id == pending_state["client_id"],
                                Task.status == "open",
                                Task.text.ilike("[await:%]%"),
                            )
                            .first()
                        )
                        if candidate_await:
                            state_project = pending_state.get("project_code")
                            task_project = candidate_await.project_code
                            if task_project is not None:
                                task_project = str(task_project).strip() or None
                            if task_project == state_project:
                                awaiting = candidate_await

                # Backward-compatible discovery for pre-MU12 await records.
                if pending_state is None:
                    awaiting = (
                        s.query(Task)
                        .filter(
                            Task.sender == sender,
                            Task.status == "open",
                            Task.text.ilike("[await:%]%"),
                        )
                        .order_by(Task.id.desc())
                        .first()
                    )

                    if awaiting and not deterministic_recognition:
                        current_client_id, current_project = _conversation_scope(
                            sender
                        )
                        await_project = awaiting.project_code
                        if await_project is not None:
                            await_project = str(await_project).strip() or None
                        if (
                            int(awaiting.client_id or 1) == int(current_client_id)
                            and (
                                await_project is None
                                or await_project == current_project
                            )
                        ):
                            pending_state = _ensure_await_conversation_state(
                                awaiting
                            )

                pending_reply_valid = False
                pending_reply_invalid = False
                if pending_state and awaiting:
                    continuation = pending_state.get("continuation") or {}
                    try:
                        continuation_matches = (
                            int(continuation.get("source_record_id"))
                            == int(awaiting.id)
                        )
                    except (TypeError, ValueError):
                        continuation_matches = False

                    if (
                        continuation_matches
                        and pending_state.get("expected_field")
                        == "new_stock_qty"
                    ):
                        validation = validate_new_stock_qty_reply(text)
                        pending_reply_valid = bool(validation["valid"])
                        pending_reply_invalid = not pending_reply_valid

                arbitration = None
                if pending_state or awaiting:
                    arbitration = _CORE_CONVERSATION.interpret_core(
                        ConversationRequest(
                            capability="routing_arbitration",
                            context={
                                "candidate": (
                                    "await_vs_normal_route"
                                    if awaiting
                                    else "pending_state_vs_normal_route"
                                ),
                                "deterministic_recognition": (
                                    deterministic_recognition
                                ),
                                "pending_reply_valid": pending_reply_valid,
                                "pending_reply_invalid": pending_reply_invalid,
                            },
                        )
                    )

                bypass_pending = bool(
                    arbitration
                    and arbitration.handled
                    and arbitration.action == "normal_route"
                )

                if pending_state and not bypass_pending:
                    touched_state = touch_conversation_state_activity(
                        pending_state["id"],
                        pending_state["sender"],
                        pending_state["client_id"],
                        pending_state.get("project_code"),
                    )
                    if touched_state.get("status_result") == "touched":
                        pending_state = touched_state

                # A generic pending state that is not backed by a legacy await
                # is preserved until a later consumer supplies authoritative
                # continuation evidence. Deterministic unrelated commands retain
                # MU11 normal-route bypass behavior.
                if pending_state and not awaiting and not bypass_pending:
                    return ("", 200)

                if awaiting and not bypass_pending:
                    raw_txt = (text or "").strip()
                    await_lower = (awaiting.text or "").lower()

                    if _run_natural_order_continuation(
                        awaiting, raw_txt, pending_state, s,
                    ):
                        return ("", 200)

                    # ------------------------------
                    # ORDER AWAIT CHAINS
                    # ------------------------------
                    if await_lower.startswith("[await:item]"):
                        _run_await_resolver(
                            resolve_await_item, awaiting, raw_txt, sender, s,
                            pending_state,
                        )
                        return ("", 200)

                    if await_lower.startswith("[await:quantity]"):
                        _run_await_resolver(
                            resolve_await_quantity, awaiting, raw_txt, sender, s,
                            pending_state,
                        )
                        return ("", 200)

                    if await_lower.startswith("[await:supplier]"):
                        _run_await_resolver(
                            resolve_await_supplier, awaiting, raw_txt, sender, s,
                            pending_state,
                        )
                        return ("", 200)

                    if await_lower.startswith("[await:delivery_date]"):
                        _run_await_resolver(
                            resolve_await_delivery_date, awaiting, raw_txt, sender, s,
                            pending_state,
                        )
                        return ("", 200)

                    if await_lower.startswith("[await:drop_location]"):
                        _run_await_resolver(
                            resolve_await_drop_location, awaiting, raw_txt, sender, s,
                            pending_state,
                        )
                        return ("", 200)

                    # ------------------------------
                    # STOCK AWAIT CHAINS
                    # ------------------------------
                    if await_lower.startswith("[await:stock_unit]"):
                        _run_await_resolver(
                            resolve_await_stock_unit, awaiting, raw_txt, sender, s,
                            pending_state,
                        )
                        return ("", 200)

                    if await_lower.startswith("[await:new_stock_unit]"):
                        _run_await_resolver(
                            resolve_await_new_stock_unit, awaiting, raw_txt, sender, s,
                            pending_state,
                        )
                        return ("", 200)

                    if await_lower.startswith("[await:new_stock_qty]"):
                        if pending_reply_invalid:
                            resolve_await_new_stock_qty(
                                awaiting, raw_txt, sender, s
                            )
                        else:
                            _run_await_resolver(
                                resolve_await_new_stock_qty, awaiting, raw_txt,
                                sender, s, pending_state,
                            )
                        return ("", 200)

        # -------------------------------------------------------------
        # NEW STOCK ITEM REQUEST
        # -------------------------------------------------------------
        if text and is_new_stock_item_request(text):
            material = parse_new_stock_item(text)
            pending_row = create_task(
                sender=sender,
                text=f"[await:new_stock_unit] material={material}",
                tag="stock",
                project_code=None,
                subcontractor_name=None,
                order_state=None,
                attachment=None,
                subtype="assigned",
            )
            _save_await_conversation_state(
                pending_row["id"],
                sender,
                pending_row.get("project_code"),
                pending_row.get("text") or "",
                text or "",
            )
            send_whatsapp_text(
                phone_id,
                sender,
                f"Adding new stock item '{material}'. What unit? (bags, pallets, drums, crates, etc.)"
            )
            return ("", 200)

        # -------------------------------------------------------------
        # DIRECT STOCK COMMANDS
        # -------------------------------------------------------------
        stock_cmd = parse_stock_command(text) if text else None
        if stock_cmd:
            if stock_cmd.get("needs_prompt") or not stock_cmd.get("unit"):
                # Ask user for missing unit
                meta = (
                    f"kind={stock_cmd['kind']};"
                    f"qty={stock_cmd.get('qty')};"
                    f"material={stock_cmd['material']}"
                )
                pending_row = create_task(
                    sender=sender,
                    text=f"[await:stock_unit] {meta}",
                    tag="stock",
                    project_code=None,
                    subcontractor_name=None,
                    order_state=None,
                    attachment=None,
                    subtype="assigned",
                )
                _save_await_conversation_state(
                    pending_row["id"],
                    sender,
                    pending_row.get("project_code"),
                    pending_row.get("text") or "",
                    text or "",
                )
                send_whatsapp_text(
                    phone_id,
                    sender,
                    "Which unit? (bags / pallets / drums / buckets / crates / other)"
                )
                return ("", 200)

            # Unit + qty present → adjust stock
            try:
                qty_val = int(stock_cmd.get("qty") or 0)
            except Exception:
                qty_val = 0

            delta = qty_val if stock_cmd["kind"] == "add" else -qty_val

            adjust_stock({
                "material": stock_cmd["material"],
                "unit": stock_cmd["unit"],
                "delta": delta,
                "actor": sender,
                "source": "whatsapp",
            })

            send_whatsapp_text(
                phone_id,
                sender,
                f"Stock updated: {delta:+} {stock_cmd['unit']} of {stock_cmd['material']}."
            )
            return ("", 200)

        # -------------------------------------------------------------
        # SEARCH ENGINE
        # -------------------------------------------------------------
        if text and is_search_request(text):
            run_search(sender, text)
            return ("", 200)

        # -------------------------------------------------------------
        # NATURAL MEETING ROUTING
        # -------------------------------------------------------------
        if text:
            datetime_config = _mu15_sender_datetime_configuration(sender)
            meeting_meaning = parse_natural_meeting(
                text,
                timezone_name=datetime_config["timezone"],
                date_order=datetime_config["date_order"],
            )
            if meeting_meaning:
                user_info = get_user_role(sender) or {}
                result = create_meeting(
                    meeting_meaning["title"],
                    user_info.get("project_code"),
                    user_info.get("subcontractor_name"),
                    None,
                    meeting_meaning["scheduled_for"],
                    [],
                    sender,
                )
                send_whatsapp_text(
                    phone_id,
                    sender,
                    f"Meeting #{result['id']} scheduled.",
                )
                return ("", 200)

        # >>> PATCH_1_INSPECTION_WEBHOOK_START — INSPECTOR SCHEDULING V6.1 <<<

        if text and classify_inspection(text):
            datetime_config = _mu15_sender_datetime_configuration(sender)
            sender_timezone = datetime_config["timezone"]

            parsed_inspection = (
                parse_inspection_request(
                    text,
                    timezone_name=sender_timezone,
                    date_order=datetime_config["date_order"],
                )
            )

            if not parsed_inspection:
                send_whatsapp_text(
                    phone_id,
                    sender,
                    (
                        "Please include the inspection "
                        "phase and date. For example: "
                        "Schedule inspection for slab "
                        "on Friday."
                    ),
                )
                return ("", 200)

            user_info = (
                get_user_role(sender) or {}
            )

            project_code = user_info.get(
                "project_code"
            )

            payload = {
                "client_id": int(user_info.get("client_id") or 1),
                "project_code": project_code,
                "phase": (
                    parsed_inspection["phase"]
                ),
                "required_date": (
                    parsed_inspection[
                        "required_date"
                    ]
                ),
                "inspector": None,
                "notes": text,
            }

            row = create_inspection(payload)

            requested_date_text = (
                parsed_inspection[
                    "required_date"
                ].strftime(
                    "%A, %B %d, %Y"
                )
            )

            send_whatsapp_text(
                phone_id,
                sender,
                (
                    f"Inspection logged "
                    f"(#{row['id']}): "
                    f"{parsed_inspection['phase']} "
                    f"on {requested_date_text}."
                ),
            )

            return ("", 200)

        # >>> PATCH_1_INSPECTION_WEBHOOK_END <<<

        # >>> PATCH_2_DELAY_WEBHOOK_START — CRITICAL-PATH DELAY TRACKING V6.1 <<<

        if text and classify_delay(text):
            user_info = get_user_role(sender) or {}
            project_code = user_info.get("project_code")

            task_match = re.search(
                r"\btask\s+#?(\d+)\b",
                text.lower(),
            )

            if task_match:
                task_id = int(task_match.group(1))
            else:
                task_resolution = _resolve_delay_task_reference(
                    text,
                    project_code,
                    user_info.get("client_id"),
                )
                resolution_status = task_resolution.get("status")

                if resolution_status == "project_missing":
                    send_whatsapp_text(
                        phone_id,
                        sender,
                        (
                            "Your WhatsApp number is not mapped "
                            "to a project, so I cannot identify "
                            "the delayed task or phase."
                        ),
                    )
                    return ("", 200)

                if resolution_status == "ambiguous":
                    matches = task_resolution.get("matches") or []
                    choices = []
                    for match in matches[:3]:
                        try:
                            match_id = int(match.get("id"))
                        except (AttributeError, TypeError, ValueError):
                            continue
                        label = str(match.get("label") or "").strip()
                        if len(label) > 60:
                            label = label[:57].rstrip() + "..."
                        choices.append(
                            f"task {match_id}: {label}"
                            if label
                            else f"task {match_id}"
                        )

                    detail = (
                        " " + "; ".join(choices) + "."
                        if choices
                        else ""
                    )
                    send_whatsapp_text(
                        phone_id,
                        sender,
                        (
                            "I found more than one matching task "
                            "in your project."
                            f"{detail} Please resend the delay "
                            "with the specific task or phase name."
                        ),
                    )
                    return ("", 200)

                if resolution_status != "resolved":
                    send_whatsapp_text(
                        phone_id,
                        sender,
                        (
                            "I could not match that delay to a task "
                            "in your project. Please resend it with "
                            "the task or phase name."
                        ),
                    )
                    return ("", 200)

                task_id = int(task_resolution["task_id"])

            days_match = re.search(
                r"\b(?:by\s+)?"
                r"(-?\d+(?:\.\d+)?)\s+days?\b",
                text.lower(),
            )

            if not days_match:
                send_whatsapp_text(
                    phone_id,
                    sender,
                    (
                        "Please include a positive delay duration. "
                        "For example: Delay task 101 "
                        "by 3 days due to rain."
                    ),
                )
                return ("", 200)

            delay_days = float(
                days_match.group(1)
            )

            if delay_days <= 0:
                send_whatsapp_text(
                    phone_id,
                    sender,
                    (
                        "Delay duration must be greater "
                        "than zero days."
                    ),
                )
                return ("", 200)

            payload = {
                "task_id": task_id,
                "project_code": project_code,
                "reporter": sender,
                "days": delay_days,
                "reason": text,
            }

            result = log_delay(payload)

            if result.get("status") != "ok":
                error_code = result.get("code")

                if error_code == "sender_project_missing":
                    message = (
                        "Your WhatsApp number is not mapped "
                        "to a project, so the delay cannot "
                        "be logged."
                    )

                elif error_code == "task_project_missing":
                    message = (
                        f"Task {task_id} is not mapped to "
                        "a project, so the delay cannot "
                        "be logged."
                    )

                elif error_code == "task_not_found":
                    message = (
                        f"Task {task_id} was not found. "
                        "Please check the task number."
                    )

                elif error_code == "project_mismatch":
                    message = (
                        f"Task {task_id} is not part of "
                        "your mapped project."
                    )

                elif error_code == "invalid_task_id":
                    message = (
                        "Please provide a valid task number."
                    )

                elif error_code == "invalid_delay_days":
                    message = (
                        "Please provide a delay duration "
                        "greater than zero days."
                    )

                else:
                    message = (
                        "The delay could not be logged. "
                        "Please check the task number, "
                        "project mapping, and delay duration."
                    )

                send_whatsapp_text(
                    phone_id,
                    sender,
                    message,
                )
                return ("", 200)

            send_whatsapp_text(
                phone_id,
                sender,
                (
                    f"Delay logged (#{result['id']}): "
                    f"task {result['task_id']} delayed "
                    f"by {result['days']:g} days."
                ),
            )

            return ("", 200)

        # >>> PATCH_2_DELAY_WEBHOOK_END <<<


        # -------------------------------------------------------------
        # FALLBACK → classifier + task creation
        # -------------------------------------------------------------
        global SENDER_GLOBAL
        SENDER_GLOBAL = sender

        cls = classify_message(text or "")
        tag = cls.get("tag")
        subtype = cls.get("subtype")
        order_state = cls.get("order_state")

        user_info = get_user_role(sender) or {}
        project_code = user_info.get("project_code")
        subcontractor_name = user_info.get("subcontractor_name")
        structured_route = interpret_supported_message(text or "", project_code)

        if structured_route["route"] == "status":
            with DBSession() as s:
                query = s.query(Task).filter(
                    Task.client_id == int(user_info.get("client_id") or 1)
                )
                if project_code:
                    query = query.filter(Task.project_code == project_code)
                rows = query.all()
            counts = {}
            for row in rows:
                counts[row.status] = counts.get(row.status, 0) + 1
            summary = ", ".join(
                f"{name}: {count}" for name, count in sorted(counts.items())
            ) or "no task records"
            send_whatsapp_text(phone_id, sender, f"Project status — {summary}.")
            return ("", 200)

        if structured_route["route"] == "approval":
            task_id = structured_route["entities"]["task_id"]
            with DBSession() as s:
                authorized_task = s.get(Task, task_id)
                if (
                    authorized_task
                    and (
                        int(authorized_task.client_id or 1)
                        != int(user_info.get("client_id") or 1)
                        or (
                            project_code
                            and authorized_task.project_code != project_code
                        )
                    )
                ):
                    authorized_task = None
            if not authorized_task:
                send_whatsapp_text(
                    phone_id, sender, "That order is not available in your scope."
                )
                return ("", 200)
            if structured_route["action"] == "approve":
                approve_task(task_id, actor=sender)
            else:
                reject_task(task_id, actor=sender)
            send_whatsapp_text(
                phone_id,
                sender,
                f"Order {task_id} {structured_route['action']}d.",
            )
            return ("", 200)

        assignee_wa = None
        if (
            structured_route["route"] == "task"
            and structured_route.get("subtype") == "assigned"
            and structured_route["entities"].get("recipient_reference")
        ):
            reference = structured_route["entities"]["recipient_reference"]
            with DBSession() as s:
                people = s.query(User).filter(
                    User.client_id == int(user_info.get("client_id") or 1),
                    User.active == True,
                ).all()
            records = [
                {
                    "id": person.wa_id,
                    "label": person.name or person.wa_id,
                    "labels": [person.name or "", person.wa_id or ""],
                }
                for person in people
                if not project_code or not person.project_code
                or person.project_code == project_code
            ]
            resolution = _CORE_CONVERSATION.interpret_core(
                ConversationRequest(
                    capability="record_resolution",
                    text=reference,
                    context={
                        "candidate": "text_reference",
                        "records": records,
                    },
                )
            )
            if resolution.metadata.get("resolution") != "resolved":
                send_whatsapp_text(
                    phone_id,
                    sender,
                    "Name one uniquely authorized person for that task.",
                )
                return ("", 200)
            assignee_wa = str(resolution.entities["record_id"])

        route_overrides = {
            "task": (
                "urgent" if structured_route.get("subtype") == "urgent" else "task",
                structured_route.get("subtype") or "assigned",
            ),
            "note": ("note", "note"),
            "pinned_note": ("note", "pinned"),
            "delivery": ("delivery", "assigned"),
        }
        if structured_route["route"] in route_overrides:
            tag, subtype = route_overrides[structured_route["route"]]
            order_state = None

        new_row = create_task(
            sender=sender,
            text=text or "",
            tag=tag,
            project_code=project_code,
            subcontractor_name=subcontractor_name,
            order_state=order_state,
            attachment=attachment,
            subtype=subtype,
            assignee_wa=assignee_wa,
        )

        # -------------------------------------------------------------
        # ORDER CHECKLIST (IF APPLICABLE)
        # -------------------------------------------------------------
        if tag == "order":
            if os.environ.get("ENABLE_BUTTONS") == "1":
                try:
                    send_order_checklist(phone_id, sender, new_row["id"])
                except Exception:
                    pass
                return ("", 200)

            natural_order = parse_natural_order(text or "", project_code)

            # No buttons → preserve supplied fields and ask only for missing.
            with DBSession() as s:
                t = s.get(Task, new_row["id"])
                if t and natural_order:
                    fields = natural_order["fields"]
                    missing = natural_order["missing_fields"]
                    if not missing:
                        t.text = _natural_order_task_text(fields)
                        t.status = "pending_approval"
                        t.last_updated = dt.datetime.utcnow()
                        s.commit()
                        send_whatsapp_text(
                            phone_id,
                            sender,
                            "✅ Order details captured. Awaiting PM approval.",
                        )
                        return ("", 200)
                    expected_field = missing[0]
                    original_request = t.text or ""
                    t.text = _natural_order_task_text(fields, expected_field)
                    s.commit()
                    _save_await_conversation_state(
                        t.id,
                        t.sender,
                        t.project_code,
                        t.text or "",
                        original_request,
                        structured_context={
                            "source_record_id": t.id,
                            "classification": cls,
                            "kind": "natural_order",
                            "order_fields": fields,
                            "missing_fields": missing,
                        },
                    )
                    send_whatsapp_text(
                        phone_id, sender, _natural_order_prompt(expected_field)
                    )
                    return ("", 200)
                if t and not (t.text or "").lower().startswith("[await:item]"):
                    original_request = t.text or ""
                    t.text = f"[await:item]\n{original_request}"
                    s.commit()
                    _save_await_conversation_state(
                        t.id,
                        t.sender,
                        t.project_code,
                        t.text or "",
                        original_request,
                        structured_context={
                            "source_record_id": t.id,
                            "classification": cls,
                        },
                    )
            send_whatsapp_text(phone_id, sender, "Item?")
            return ("", 200)

    # -----------------------------------------------------------------
    # END OF BLOCK 6 — NEXT: BLOCK 7 (FINAL RETURN)
    # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # BLOCK 7 — FINAL RETURN
    # -----------------------------------------------------------------
    return ("", 200)

# ---------------------------------------------------------------------
# END OF W2 WEBHOOK
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
# Admin views — dual output (HTML + JSON)
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Admin guard
# ---------------------------------------------------------------------
def _auth_fail(): return Response("Unauthorized",401)
def _check_admin():
    token=request.args.get("token","")
    return not ADMIN_TOKEN or token==ADMIN_TOKEN

@app.route("/admin/summary",methods=["GET"])
def api_summary():
    if not _check_admin(): return _auth_fail()
    return jsonify(get_summary())


# >>> FEATURE_3D_REMINDER_ADMIN_VISIBILITY_START — READ-ONLY V6.1 <<<

def _admin_pm_reminder_datetime(value):
    """Serialize reminder timestamps without mutating reminder state."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        rendered = value.isoformat()
        return rendered if value.tzinfo is not None else rendered + "Z"
    return str(value)


def _admin_pm_reminder_dict(reminder: PMReminder) -> dict:
    """Read-only admin representation of one reminder."""
    return {
        "id": reminder.id,
        "pm_wa": reminder.pm_wa,
        "recipient_wa": reminder.recipient_wa or reminder.pm_wa,
        "project_code": reminder.project_code,
        "text": reminder.text,
        "rule": reminder.rule,
        "timezone": reminder.timezone or "America/New_York",
        "next_run": _admin_pm_reminder_datetime(reminder.next_run),
        "recurring": bool(reminder.recurring),
        "recurrence_rule": reminder.recurrence_rule or "none",
        "recurrence_interval": reminder.recurrence_interval or 1,
        "recurrence_seconds": reminder.recurrence_seconds,
        "recurrence_anchor_day": reminder.recurrence_anchor_day,
        "status": reminder.status,
        "active": bool(reminder.active),
        "claimed_at": _admin_pm_reminder_datetime(reminder.claimed_at),
        "retry_after": _admin_pm_reminder_datetime(reminder.retry_after),
        "delivered_at": _admin_pm_reminder_datetime(reminder.delivered_at),
        "acknowledged_at": _admin_pm_reminder_datetime(
            reminder.acknowledged_at
        ),
        "snoozed_at": _admin_pm_reminder_datetime(reminder.snoozed_at),
        "redirected_at": _admin_pm_reminder_datetime(
            reminder.redirected_at
        ),
        "cancelled_at": _admin_pm_reminder_datetime(reminder.cancelled_at),
        "completed_at": _admin_pm_reminder_datetime(reminder.completed_at),
        "failed_at": _admin_pm_reminder_datetime(reminder.failed_at),
        "delivery_count": reminder.delivery_count or 0,
        "failure_count": reminder.failure_count or 0,
        "last_error": reminder.last_error,
        "created_at": _admin_pm_reminder_datetime(reminder.created_at),
        "updated_at": _admin_pm_reminder_datetime(reminder.updated_at),
    }


def _admin_pm_reminder_rows() -> list[dict]:
    """Return all reminders newest first without changing lifecycle state."""
    with SessionLocal() as s:
        reminders = (
            s.query(PMReminder)
            .order_by(PMReminder.id.desc())
            .all()
        )
        return [
            _admin_pm_reminder_dict(reminder)
            for reminder in reminders
        ]


@app.route("/admin/reminders.json", methods=["GET"])
def admin_reminders_json():
    if not _check_admin():
        return _auth_fail()
    return jsonify(_admin_pm_reminder_rows()), 200


@app.route("/admin/reminders", methods=["GET"])
def admin_reminders():
    if not _check_admin():
        return _auth_fail()

    rows = _admin_pm_reminder_rows()

    def h(value):
        return (
            str(value if value is not None else "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    table_rows = []
    for row in rows:
        recurrence = (
            row["recurrence_rule"]
            if row["recurring"]
            else "one-time"
        )
        table_rows.append(
            "<tr>"
            f"<td>{row['id']}</td>"
            f"<td>{h(row['pm_wa'])}</td>"
            f"<td>{h(row['recipient_wa'])}</td>"
            f"<td>{h(row['project_code'])}</td>"
            f"<td>{h(row['status'])}</td>"
            f"<td>{'yes' if row['active'] else 'no'}</td>"
            f"<td>{h(row['next_run'])}</td>"
            f"<td>{h(row['timezone'])}</td>"
            f"<td>{h(row['rule'])}</td>"
            f"<td>{h(recurrence)}</td>"
            f"<td>{row['recurrence_interval']}</td>"
            f"<td>{h(row['recurrence_seconds'])}</td>"
            f"<td>{h(row['recurrence_anchor_day'])}</td>"
            f"<td>{h(row['claimed_at'])}</td>"
            f"<td>{h(row['delivered_at'])}</td>"
            f"<td>{h(row['acknowledged_at'])}</td>"
            f"<td>{h(row['snoozed_at'])}</td>"
            f"<td>{h(row['redirected_at'])}</td>"
            f"<td>{h(row['cancelled_at'])}</td>"
            f"<td>{h(row['completed_at'])}</td>"
            f"<td>{h(row['failed_at'])}</td>"
            f"<td>{h(row['retry_after'])}</td>"
            f"<td>{row['delivery_count']}</td>"
            f"<td>{row['failure_count']}</td>"
            f"<td>{h(row['last_error'])}</td>"
            f"<td>{h(row['created_at'])}</td>"
            f"<td>{h(row['updated_at'])}</td>"
            f"<td class=\"reminder-text\">{h(row['text'])}</td>"
            "</tr>"
        )

    body = "".join(table_rows) or (
        '<tr><td colspan="28">No reminders found.</td></tr>'
    )

    html = (
        "<!doctype html><html><head>"
        "<meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<title>HUBFLO Reminders</title>"
        "<style>"
        "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;"
        "margin:20px;color:#222}"
        ".table-wrap{overflow-x:auto}"
        "table{border-collapse:collapse;width:100%;font-size:13px}"
        "th,td{border:1px solid #ccc;padding:6px 8px;text-align:left;"
        "vertical-align:top;white-space:nowrap}"
        "th{background:#f3f3f3;position:sticky;top:0}"
        ".reminder-text{white-space:pre-wrap;min-width:320px}"
        "</style></head><body>"
        f"<h1>Reminder Admin Visibility</h1><p>Total: {len(rows)}</p>"
        "<div class=\"table-wrap\"><table><thead><tr>"
        "<th>ID</th><th>Owner</th><th>Recipient</th><th>Project</th>"
        "<th>Status</th><th>Active</th><th>Next Run (UTC)</th>"
        "<th>Timezone</th><th>Rule</th><th>Recurrence</th>"
        "<th>Recurrence Interval</th><th>Recurrence Seconds</th>"
        "<th>Recurrence Anchor Day</th><th>Claimed At</th>"
        "<th>Delivered</th><th>Acknowledged</th><th>Snoozed At</th>"
        "<th>Redirected At</th><th>Cancelled At</th><th>Completed At</th>"
        "<th>Failed At</th><th>Retry After</th><th>Deliveries</th>"
        "<th>Failures</th><th>Last Error</th><th>Created At</th>"
        "<th>Updated At</th><th>Original Text</th>"
        f"</tr></thead><tbody>{body}</tbody></table></div>"
        "</body></html>"
    )
    return Response(html, status=200, mimetype="text/html")

# >>> FEATURE_3D_REMINDER_ADMIN_VISIBILITY_END <<<

@app.route("/admin/view", methods=["GET"])
def admin_view():
    if not _check_admin(): return _auth_fail()
    rows = get_tasks(limit=200)

    def h(s):
        return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    th = (
        "<tr><th>ID</th><th>Time</th><th>Sender</th><th>Client</th><th>Tag</th>"
        "<th>Status</th><th>Order State</th>"
        "<th>Cost ($)</th><th>Time Impact (days)</th><th>Approval Req</th>"
        "<th>Text</th></tr>"
    )
    trs = []
    for r in rows:
        # NEW: derive client-display (safe)
        client_display = r.get('project_code') or ""
        trs.append(
            f"<tr>"
            f"<td>{r['id']}</td>"
            f"<td>{h(r['ts'])}</td>"
            f"<td>{h(r.get('sender') or '')}</td>"
            f"<td>{h(client_display)}</td>"
            f"<td>{h(r.get('tag') or '')}</td>"
            f"<td>{h(r.get('status') or '')}</td>"
            f"<td>{h(r.get('order_state') or '')}</td>"
            f"<td>{h(str(r.get('cost') or ''))}</td>"
            f"<td>{h(str(r.get('time_impact_days') or ''))}</td>"
            f"<td>{'✅' if r.get('approval_required') else ''}</td>"
            f"<td>{h(r['text'])}</td>"
            f"</tr>"
        )

    body = f"""
    <html><head><title>HubFlo Admin</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}}
      table{{border-collapse:collapse;width:100%}}
      th,td{{border:1px solid #ddd;padding:6px;font-size:13px}}
      th{{background:#f2f2f2;text-align:left}}
    </style></head><body>
    <h2>HubFlo Admin (HTML)</h2>
    <table>{th}{''.join(trs)}</table>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")

@app.get("/admin/json")
def admin_json():
    token = request.args.get("token", "")
    if token != ADMIN_TOKEN:
        return jsonify({"error": "unauthorized"}), 403

    from storage import get_summary
    return jsonify(get_summary())

@app.route("/admin/view.json")
def admin_view_json():
    token = request.args.get("token")
    if token != ADMIN_TOKEN:
        return jsonify([])

    limit = int(request.args.get("limit", 50))

    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .order_by(Task.id.desc())
            .limit(limit)
            .all()
        )

    out = []
    for r in rows:
        out.append({
            "id": r.id,
            "ts": r.ts,
            "sender": r.sender,
            "text": r.text,
            "tag": r.tag,
            "subtype": r.subtype,
            "order_state": r.order_state,
            "cost": r.cost,
            "time_impact_days": r.time_impact_days,
            "approval_required": r.approval_required,
            "status": r.status,
            "project_code": r.project_code,
            "subcontractor_name": r.subcontractor_name,
            "approved_at": r.approved_at,
            "rejected_at": r.rejected_at,
            "completed_at": r.completed_at,
            "started_at": r.started_at,
            "due_date": r.due_date,
            "overrun_days": r.overrun_days,
            "is_rework": r.is_rework,
            "attachment": {
                "name": r.attachment_name,
                "mime": r.attachment_mime,
                "url": r.attachment_url,
            } if r.attachment_url else None,
            "attachment_url": r.attachment_url,
            "last_updated": r.last_updated,
        })

    return jsonify(out)

# >>> PATCH_1_INSPECTION_ADMIN_START — READ-ONLY TEST VIEW V6.1 <<<

@app.route("/admin/inspections", methods=["GET"])
def admin_inspections_view():
    if not _check_admin():
        return _auth_fail()

    from storage_v6_1 import Inspection

    with SessionLocal() as s:
        rows = (
            s.query(Inspection)
            .order_by(Inspection.id.desc())
            .all()
        )

    def h(value):
        return (
            str(value or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def display_datetime(value):
        return value.isoformat(sep=" ") if value else ""

    headings = (
        "<tr>"
        "<th>ID</th>"
        "<th>Project Code</th>"
        "<th>Phase</th>"
        "<th>Required Date</th>"
        "<th>Actual Date</th>"
        "<th>Inspector</th>"
        "<th>Notes</th>"
        "<th>Created At</th>"
        "</tr>"
    )

    table_rows = []

    for row in rows:
        table_rows.append(
            "<tr>"
            f"<td>{row.id}</td>"
            f"<td>{h(row.project_code)}</td>"
            f"<td>{h(row.phase)}</td>"
            f"<td>{h(display_datetime(row.required_date))}</td>"
            f"<td>{h(display_datetime(row.actual_date))}</td>"
            f"<td>{h(row.inspector)}</td>"
            f"<td>{h(row.notes)}</td>"
            f"<td>{h(display_datetime(row.created_at))}</td>"
            "</tr>"
        )

    body = f"""
    <html>
    <head>
        <title>HubFlo Inspection Test View</title>
        <style>
            body{{
                font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
            }}
            table{{
                border-collapse:collapse;
                width:100%;
            }}
            th,td{{
                border:1px solid #ddd;
                padding:6px;
                font-size:13px;
                vertical-align:top;
            }}
            th{{
                background:#f2f2f2;
                text-align:left;
            }}
        </style>
    </head>
    <body>
        <h2>HubFlo Inspections — Test View</h2>
        <p>Read-only verification endpoint.</p>
        <table>
            {headings}
            {''.join(table_rows)}
        </table>
    </body>
    </html>
    """

    return Response(
        body,
        200,
        mimetype="text/html",
    )


@app.route("/admin/inspections.json", methods=["GET"])
def admin_inspections_json():
    if not _check_admin():
        return _auth_fail()

    from storage_v6_1 import Inspection

    with SessionLocal() as s:
        rows = (
            s.query(Inspection)
            .order_by(Inspection.id.desc())
            .all()
        )

        output = []

        for row in rows:
            output.append({
                "id": row.id,
                "project_code": row.project_code,
                "phase": row.phase,
                "required_date": (
                    row.required_date.isoformat()
                    if row.required_date
                    else None
                ),
                "actual_date": (
                    row.actual_date.isoformat()
                    if row.actual_date
                    else None
                ),
                "inspector": row.inspector,
                "notes": row.notes,
                "created_at": (
                    row.created_at.isoformat()
                    if row.created_at
                    else None
                ),
            })

    return jsonify(output), 200

# >>> PATCH_1_INSPECTION_ADMIN_END <<<

# >>> PATCH_2_DELAY_ADMIN_START — READ-ONLY TEST VIEW V6.1 <<<

@app.route("/admin/delays", methods=["GET"])
def admin_delays_view():
    if not _check_admin():
        return _auth_fail()

    from storage_v6_1 import DelayLog

    with SessionLocal() as s:
        rows = (
            s.query(DelayLog)
            .order_by(DelayLog.id.desc())
            .all()
        )

    def h(value):
        return (
            str(value or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def display_datetime(value):
        return value.isoformat(sep=" ") if value else ""

    headings = (
        "<tr>"
        "<th>ID</th>"
        "<th>Task ID</th>"
        "<th>Project Code</th>"
        "<th>Reporter</th>"
        "<th>Days</th>"
        "<th>Reason</th>"
        "<th>Created At</th>"
        "</tr>"
    )

    table_rows = []

    for row in rows:
        table_rows.append(
            "<tr>"
            f"<td>{row.id}</td>"
            f"<td>{h(row.task_id)}</td>"
            f"<td>{h(row.project_code)}</td>"
            f"<td>{h(row.reporter)}</td>"
            f"<td>{h(row.days)}</td>"
            f"<td>{h(row.reason)}</td>"
            f"<td>{h(display_datetime(row.created_at))}</td>"
            "</tr>"
        )

    body = f"""
    <html>
    <head>
        <title>HubFlo Delay Test View</title>
        <style>
            body{{
                font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
            }}
            table{{
                border-collapse:collapse;
                width:100%;
            }}
            th,td{{
                border:1px solid #ddd;
                padding:6px;
                font-size:13px;
                vertical-align:top;
            }}
            th{{
                background:#f2f2f2;
                text-align:left;
            }}
        </style>
    </head>
    <body>
        <h2>HubFlo Critical-Path Delays — Test View</h2>
        <p>Read-only verification endpoint.</p>
        <table>
            {headings}
            {''.join(table_rows)}
        </table>
    </body>
    </html>
    """

    return Response(
        body,
        200,
        mimetype="text/html",
    )


@app.route("/admin/delays.json", methods=["GET"])
def admin_delays_json():
    if not _check_admin():
        return _auth_fail()

    from storage_v6_1 import DelayLog

    with SessionLocal() as s:
        rows = (
            s.query(DelayLog)
            .order_by(DelayLog.id.desc())
            .all()
        )

        output = []

        for row in rows:
            output.append({
                "id": row.id,
                "task_id": row.task_id,
                "project_code": row.project_code,
                "reporter": row.reporter,
                "days": row.days,
                "reason": row.reason,
                "created_at": (
                    row.created_at.isoformat()
                    if row.created_at
                    else None
                ),
            })

    return jsonify(output), 200

# >>> PATCH_2_DELAY_ADMIN_END <<<

# >>> PATCH_11_APP_START — SUPPLIER DIRECTORY <<<

@app.route("/admin/supplier/create", methods=["POST"])
def admin_supplier_create():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    from storage_v6_1 import supplier_create
    result = supplier_create(data)
    return jsonify(result)

@app.route("/admin/suppliers", methods=["GET"])
def admin_supplier_list():
    if not _check_admin():
        return _auth_fail()

    from storage_v6_1 import supplier_list
    result = supplier_list()
    return jsonify(result)

# >>> PATCH_11_APP_END <<<

# ---------------------------------------------------------------------
# Admin action routes (parity with v5)
# ---------------------------------------------------------------------

# >>> PATCH_14_APP_START — CRITICAL FLAGS IN DIGESTS <<<

def _task_is_critical_for_digest(t: dict) -> bool:
    """
    Mirrors storage.is_task_critical but operates on the
    already-serialized task dictionaries passed into digest builders.
    """
    cost = t.get("cost")
    time_impact = t.get("time_impact_days")
    approval = t.get("approval_required")

    if cost and cost >= 1000:
        return True
    if time_impact and time_impact >= 3:
        return True
    if approval:
        return True
    return False

# >>> PATCH_14_APP_END <<<

# >>> PATCH_3_APP_START — INLINE TASK TEXT EDIT <<<

@app.route("/admin/task/edit", methods=["POST"])
def admin_task_edit():
    token = request.args.get("token", "").strip()
    if token != ADMIN_TOKEN:
        return {"error": "unauthorized"}, 401

    data = request.get_json(force=True, silent=True) or {}
    tid = data.get("task_id")
    new_text = data.get("new_text")
    actor = data.get("actor")

    if not tid or not new_text:
        return {"error": "missing fields"}, 400

    from storage_v6_1 import edit_task_text
    result = edit_task_text(tid, new_text, actor)

    return jsonify(result)

# >>> PATCH_3_APP_END <<<

@app.route("/admin/task/find", methods=["GET"])
def admin_task_find():
    if not _check_admin():
        return _auth_fail()

    tid = request.args.get("id", "").strip()
    if not tid.isdigit():
        return jsonify({"error": "invalid id"}), 400

    with SessionLocal() as s:
        t = s.get(Task, int(tid))
        if not t:
            return jsonify({"error": "not found"}), 404

        return jsonify({
            "id": t.id,
            "sender": t.sender,
            "text": t.text,
            "tag": t.tag,
            "status": t.status,
            "project_code": t.project_code,
            "subcontractor_name": t.subcontractor_name,
            "ts": t.ts.isoformat() if t.ts else None,
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        }), 200

@app.route("/admin/task/recent", methods=["GET"])
def admin_task_recent():
    if not _check_admin():
        return _auth_fail()

    limit = request.args.get("limit", "20").strip()
    if not limit.isdigit():
        limit = "20"

    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .order_by(Task.id.desc())
            .limit(int(limit))
            .all()
        )

        out = []
        for t in rows:
            out.append({
                "id": t.id,
                "sender": t.sender,
                "text": t.text,
                "tag": t.tag,
                "status": t.status,
                "project_code": t.project_code,
                "subcontractor_name": t.subcontractor_name,
                "ts": t.ts.isoformat() if t.ts else None,
            })

    return jsonify({"tasks": out, "count": len(out)}), 200

# >>> PATCH_19_APP_START — SIMPLE TASK SEARCH (DEBUG SAFE) <<<

@app.route("/admin/task/search", methods=["GET"])
def admin_task_search():
    if not _check_admin():
        return _auth_fail()

    q = (request.args.get("q") or "").strip().lower()
    if not q:
        return jsonify({"error": "missing q"}), 400

    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .filter(Task.text.ilike(f"%{q}%"))
            .order_by(Task.id.desc())
            .limit(50)
            .all()
        )

    out = []
    for t in rows:
        out.append({
            "id": t.id,
            "sender": t.sender,
            "text": t.text,
            "tag": t.tag,
            "status": t.status,
            "project_code": t.project_code,
            "subcontractor_name": t.subcontractor_name,
            "ts": t.ts.isoformat() if t.ts else None,
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        })

    return jsonify({"count": len(out), "results": out}), 200

# >>> PATCH_19_APP_END <<<

# >>> PATCH_20_APP_START — RAW TASK DEBUG DUMP (ADMIN ONLY) <<<

@app.route("/admin/task/raw", methods=["GET"])
def admin_task_raw():
    if not _check_admin():
        return _auth_fail()

    tid = request.args.get("id", "").strip()
    if not tid.isdigit():
        return jsonify({"error": "invalid id"}), 400

    with SessionLocal() as s:
        t = s.get(Task, int(tid))
        if not t:
            return jsonify({"error": "not found"}), 404

        # Serialize *every* field, raw
        return jsonify({
            "id": t.id,
            "sender": t.sender,
            "text": t.text,
            "tag": t.tag,
            "subtype": t.subtype,
            "status": t.status,
            "order_state": t.order_state,
            "project_code": t.project_code,
            "subcontractor_name": t.subcontractor_name,
            "ts": t.ts.isoformat() if t.ts else None,
            "started_at": t.started_at.isoformat() if t.started_at else None,
            "completed_at": t.completed_at.isoformat() if t.completed_at else None,
            "approved_at": t.approved_at.isoformat() if t.approved_at else None,
            "rejected_at": t.rejected_at.isoformat() if t.rejected_at else None,
            "due_date": t.due_date.isoformat() if t.due_date else None,
            "overrun_days": t.overrun_days,
            "is_rework": t.is_rework,
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
            "attachment_name": t.attachment_name,
            "attachment_mime": t.attachment_mime,
            "attachment_url": t.attachment_url,
            "last_updated": t.last_updated.isoformat() if t.last_updated else None
        }), 200

# >>> PATCH_20_APP_END <<<

@app.route("/admin/task_group/add", methods=["POST"])
def admin_task_group_add():
    token = request.args.get("token", "").strip()
    if token != ADMIN_TOKEN:
        return {"error": "unauthorized"}, 401

    data = request.get_json(force=True, silent=True) or {}
    parent_id = data.get("parent_id")
    child_id = data.get("child_id")
    actor = data.get("actor", "admin")

    if not parent_id or not child_id:
        return {"error": "missing fields"}, 400

    from storage_v6_1 import add_task_to_group
    result = add_task_to_group(int(parent_id), int(child_id), actor)
    return jsonify(result)

@app.route("/admin/task_group/children", methods=["GET"])
def admin_task_group_children():
    token = request.args.get("token", "").strip()
    if token != ADMIN_TOKEN:
        return {"error": "unauthorized"}, 401

    parent_id = request.args.get("parent_id")
    if not parent_id:
        return {"error": "missing parent_id"}, 400

    from storage_v6_1 import get_group_children
    kids = get_group_children(int(parent_id))
    return jsonify({"parent_id": int(parent_id), "children": kids})

@app.route("/admin/approve", methods=["POST"])
def api_approve():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    note = data.get("note")

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    result = approve_task(int(tid), actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    # Optional note for audit (future use)
    if note:
        log_audit("admin", "approve_note", "task", int(tid), details=note)

    return jsonify(result), 200

@app.route("/admin/reject", methods=["POST"])
def api_reject():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    rework = data.get("rework", True)

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    result = reject_task(int(tid), rework=bool(rework), actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    return jsonify(result), 200

@app.route("/admin/revoke", methods=["POST"])
def api_revoke():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    note = data.get("note")

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    result = revoke_last(int(tid), actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    # Optional note for audit
    if note:
        log_audit("admin", "revoke_note", "task", int(tid), details=note)

    return jsonify(result), 200

# === CALL-ACTION TEMPLATES (ADMIN ONLY) ================================
@app.route("/admin/call/templates", methods=["GET"])
def admin_call_templates():
    if not _check_admin():
        return _auth_fail()

    templates = [
        {
            "id": "call_supplier",
            "label": "Call supplier",
            "description": "Use for chasing materials, deliveries or clarifications with suppliers."
        },
        {
            "id": "call_pm",
            "label": "Call PM",
            "description": "Use for coordination calls between subcontractor and project manager."
        },
        {
            "id": "call_owner",
            "label": "Call owner",
            "description": "Use for high-level issues requiring owner or director attention."
        },
    ]
    return jsonify({"status": "ok", "templates": templates}), 200
# ======================================================================

@app.route("/admin/order_state", methods=["POST"])
def api_order_state():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    state = (data.get("state") or "").strip().lower()

    allowed = ["quoted","pending_approval","approved","cancelled","invoiced","enacted"]

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    if state not in allowed:
        return jsonify({"error": "invalid state", "allowed": allowed}), 400

    result = set_order_state(int(tid), state, actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    return jsonify(result), 200

@app.route("/admin/accuracy", methods=["GET"])
def api_accuracy():
    if not _check_admin(): return _auth_fail()
    name = request.args.get("subcontractor", "")
    if not name:
        return jsonify({"error": "missing subcontractor"}), 400
    return jsonify(subcontractor_accuracy(name))

@app.route("/admin/meeting/create", methods=["POST"])
def api_meeting_create():
    if not _check_admin(): return _auth_fail()
    title = request.args.get("title", "Site Meeting")
    project_code = request.args.get("project") or None
    subcontractor_name = request.args.get("subcontractor") or None
    site_name = request.args.get("site") or None
    scheduled_for = request.args.get("when") or None
    task_ids = request.args.get("tasks") or ""
    if scheduled_for:
        try:
            scheduled_for = dt.datetime.fromisoformat(scheduled_for)
        except Exception:
            scheduled_for = None
    ids = []
    for t in (task_ids.split(",") if task_ids else []):
        t = t.strip()
        if t.isdigit(): ids.append(int(t))
    return jsonify(create_meeting(
        title=title, project_code=project_code, subcontractor_name=subcontractor_name,
        site_name=site_name, scheduled_for=scheduled_for, task_ids=ids, created_by="admin"
    ))

@app.route("/admin/meeting/start", methods=["POST"])
def api_meeting_start():
    if not _check_admin(): return _auth_fail()
    mid = int(request.args.get("id", "0"))
    return jsonify(start_meeting(mid, actor="admin") or {"error": "not found"})

@app.route("/admin/meeting/close", methods=["POST"])
def api_meeting_close():
    if not _check_admin(): return _auth_fail()
    mid = int(request.args.get("id", "0"))
    return jsonify(close_meeting(mid, actor="admin") or {"error": "not found"})

# ---------------------------------------------------------------------
# Take-On Import: Users / Roles / Hierarchy
# ---------------------------------------------------------------------
from storage import SessionLocal, User

@app.route("/admin/import_takeon_users", methods=["POST"])
def api_import_takeon_users():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True)
    if not isinstance(data, list):
        return jsonify({"error": "expected list of user rows"}), 400

    # Data format expected:
    # [
    #   {
    #     "wa_id": "27821234567",
    #     "name": "John Doe",
    #     "role": "sub",
    #     "subcontractor_name": "BrickBuild Co",
    #     "project_code": "PRJ001"
    #   },
    #   ...
    # ]

    inserted = 0
    with SessionLocal() as s:
        # clear existing
        s.query(User).delete()

        for row in data:
            u = User(
                wa_id=str(row.get("wa_id", "")).strip(),
                name=(row.get("name") or "").strip(),
                role=(row.get("role") or "").strip().lower(),
                subcontractor_name=(row.get("subcontractor_name") or "").strip() or None,
                project_code=(row.get("project_code") or "").strip() or None,
                phone=str(row.get("wa_id", "")).strip(),  # store same for now
                active=True,
            )
            s.add(u)
            inserted += 1

        s.commit()

    return jsonify({"status": "ok", "imported": inserted}), 200

# ---------------------------------------------------------------------
# Change Orders & Stock endpoints (new)
# ---------------------------------------------------------------------
@app.route("/admin/change_order",methods=["POST"])
def api_change_order():
    if not _check_admin(): return _auth_fail()
    data=request.get_json(force=True)
    return jsonify(record_change_order(data))

# >>> PATCH_8_APP_START — INLINE CHANGE-ORDER EDIT (AUDIT SAFE) <<<

@app.route("/admin/change_order/edit", methods=["POST"])
def api_change_order_edit():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("task_id")
    fields = data.get("fields") or {}

    if not tid:
        return jsonify({"error": "missing task_id"}), 400

    from storage import SessionLocal, Task, log_audit

    editable = {"cost", "time_impact_days", "approval_required"}

    with SessionLocal() as s:
        t = s.get(Task, int(tid))
        if not t:
            return jsonify({"error": "task not found"}), 404

        before = {
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        }

        # apply safe edits
        for k, v in fields.items():
            if k not in editable:
                continue
            if k == "approval_required":
                setattr(t, k, bool(v))
            else:
                try:
                    setattr(t, k, float(v) if v is not None else None)
                except:
                    pass

        s.commit(); s.refresh(t)

        after = {
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        }

        details = json.dumps({"before": before, "after": after}, default=str)
        log_audit("admin", "change_order_edit", "task", t.id, details=details)

        return jsonify({
            "status": "ok",
            "task_id": t.id,
            "before": before,
            "after": after
        }), 200

# >>> PATCH_8_APP_END <<<

@app.route("/admin/stock/create",methods=["POST"])
def api_stock_create():
    if not _check_admin(): return _auth_fail()
    data=request.get_json(force=True)
    return jsonify(create_stock_item(data))

@app.route("/admin/stock/adjust",methods=["POST"])
def api_stock_adjust():
    if not _check_admin(): return _auth_fail()
    data=request.get_json(force=True)
    return jsonify(adjust_stock(data))

@app.route("/admin/stock/report",methods=["GET"])
def api_stock_report():
    if not _check_admin(): return _auth_fail()
    return jsonify(get_stock_report())

# === PM ↔ PROJECT ASSIGNMENT (ADMIN) =================================
@app.route("/admin/assign_pm", methods=["POST"])
def admin_assign_pm():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True, silent=True) or {}
    pm_wa = data.get("pm_wa", "").strip()
    project_code = data.get("project_code", "").strip()

    if not pm_wa or not project_code:
        return jsonify({"error": "missing pm_wa or project_code"}), 400

    from storage import SessionLocal, User, PMProjectMap

    with SessionLocal() as s:
        pm = (
            s.query(User)
            .filter(User.wa_id == pm_wa, User.active == True)
            .first()
        )
        if not pm or pm.role != "pm":
            return jsonify({"error": "not a valid pm"}), 400

        existing = (
            s.query(PMProjectMap)
            .filter(PMProjectMap.pm_user_id == pm.id,
                    PMProjectMap.project_code == project_code)
            .first()
        )
        if not existing:
            m = PMProjectMap(pm_user_id=pm.id, project_code=project_code, primary_pm=True)
            s.add(m)
            s.commit()

        return jsonify({"status": "ok", "pm": pm_wa, "project_code": project_code}), 200

# === DIGEST SCAFFOLDS (sandbox only) =================================
@app.route("/admin/digest/pm", methods=["GET"])
def admin_digest_pm():
    if not _check_admin(): return _auth_fail()

    pm_wa = request.args.get("pm") or ""
    if not pm_wa:
        return jsonify({"error": "missing pm"}), 400

    from storage import SessionLocal, User, PMProjectMap, Task

    with SessionLocal() as s:
        pm = s.query(User).filter(User.wa_id == pm_wa, User.active == True).first()
        if not pm or pm.role != "pm":
            return jsonify({"error": "not a pm"}), 400

        proj_rows = (
            s.query(PMProjectMap.project_code)
            .filter(PMProjectMap.pm_user_id == pm.id)
            .all()
        )
        projects = [r.project_code for r in proj_rows]

        tasks = (
            s.query(Task)
            .filter(Task.project_code.in_(projects), Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        lines = [f"📋 Daily PM Digest for {pm.name}"]
        for t in tasks:
            label = f"[{t.tag.upper()}]" if t.tag else ""
            cost = f" | 💲{t.cost}" if t.cost is not None else ""
            time_imp = f" | ⏱{t.time_impact_days}d" if t.time_impact_days is not None else ""
            approval = " | ✅Approval" if t.approval_required else ""
            lines.append(f"- ({t.id}) {label} {t.text}{cost}{time_imp}{approval}")

        return jsonify({
            "preview_text": "\n".join(lines),
            "total_open": len(tasks),
            "projects": projects
        }), 200

@app.route("/admin/digest/pm/send", methods=["POST"])
def admin_digest_pm_send():
    if not _check_admin(): 
        return _auth_fail()

    pm_wa = request.args.get("pm") or ""
    if not pm_wa:
        return jsonify({"error": "missing pm"}), 400

    from storage import SessionLocal, User, PMProjectMap, Task

    with SessionLocal() as s:
        pm = s.query(User).filter(User.wa_id == pm_wa, User.active == True).first()
        if not pm or pm.role != "pm":
            return jsonify({"error": "not a pm"}), 400

        proj_rows = (
            s.query(PMProjectMap.project_code)
            .filter(PMProjectMap.pm_user_id == pm.id)
            .all()
        )
        projects = [r.project_code for r in proj_rows]

        tasks = (
            s.query(Task)
            .filter(Task.project_code.in_(projects), Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        if not tasks:
            return jsonify({"status": "no-open-tasks", "sent_to": pm_wa}), 200

        lines = [f"📋 Daily PM Digest for {pm.name}"]
        for t in tasks:
            label = f"[{t.tag.upper()}]" if t.tag else ""
            extra = []
            if t.cost: extra.append(f"${t.cost:.2f}")
            if t.time_impact_days: extra.append(f"{t.time_impact_days} d")
            if t.approval_required: extra.append("⚠ Approval")
            note = f" ({', '.join(extra)})" if extra else ""
            lines.append(f"- ({t.id}) {label} {t.text}{note}")
        message = "\n".join(lines)

        # Sandbox-safe send
        log.info(f"DAILY_PM_DIGEST_SEND_SANDBOX → {pm_wa}: {message}")

        return jsonify({"status": "ok", "sent_to": pm_wa}), 200

@app.route("/admin/digest/sub", methods=["GET"])
def admin_digest_sub():
    if not _check_admin(): 
        return _auth_fail()

    sub_wa = request.args.get("sender") or ""
    if not sub_wa:
        return jsonify({"error": "missing sender"}), 400

    from storage import SessionLocal, User, Task

    with SessionLocal() as s:
        sub = (
            s.query(User)
            .filter(User.wa_id == sub_wa, User.active == True)
            .first()
        )
        if not sub or sub.role != "sub":
            return jsonify({"error": "not a subcontractor"}), 400

        tasks = (
            s.query(Task)
            .filter(Task.sender == sub_wa)
            .order_by(Task.id.desc())
            .limit(200)
            .all()
        )

        resp = []
        for t in tasks:
            resp.append({
                "id": t.id,
                "project": t.project_code,
                "tag": t.tag,
                "subtype": t.subtype,
                "text": t.text,
                "status": t.status,
                "cost": t.cost,
                "time_impact_days": t.time_impact_days,
                "approval_required": t.approval_required,
                "ts": t.ts.isoformat() if t.ts else None
            })

        return jsonify({"sub": sub.name, "tasks": resp}), 200


@app.route("/admin/digest/sub/preview", methods=["GET"])
def admin_digest_sub_preview():
    if not _check_admin(): return _auth_fail()

    sub_wa = request.args.get("sender") or ""
    if not sub_wa:
        return jsonify({"error": "missing sender"}), 400

    from storage import SessionLocal, User, Task

    with SessionLocal() as s:
        sub = s.query(User).filter(User.wa_id == sub_wa, User.active == True).first()
        if not sub or sub.role != "sub":
            return jsonify({"error": "not a subcontractor"}), 400

        tasks = (
            s.query(Task)
            .filter(Task.sender == sub_wa, Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        lines = [f"📋 Daily Tasks for {sub.name} ({sub.subcontractor_name or 'No Company'})"]
        for t in tasks:
            extra = []
            if t.cost: extra.append(f"${t.cost:.2f}")
            if t.time_impact_days: extra.append(f"{t.time_impact_days} d")
            if t.approval_required: extra.append("⚠ Approval")
            note = f" ({', '.join(extra)})" if extra else ""
            lines.append(f"- ({t.id}) {t.text}{note}")

        return jsonify({
            "preview_text": "\n".join(lines),
            "total_open": len(tasks)
        }), 200

@app.route("/admin/digest/sub/send", methods=["POST"])
def admin_digest_sub_send():
    if not _check_admin(): 
        return _auth_fail()

    sub_wa = request.args.get("sender") or ""
    if not sub_wa:
        return jsonify({"error": "missing sender"}), 400

    from storage import SessionLocal, User, Task
    with SessionLocal() as s:
        sub = s.query(User).filter(User.wa_id == sub_wa, User.active == True).first()
        if not sub or sub.role != "sub":
            return jsonify({"error": "not a subcontractor"}), 400

        tasks = (
            s.query(Task)
            .filter(Task.sender == sub_wa, Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        lines = [f"📋 Daily Tasks for {sub.name} ({sub.subcontractor_name or 'No Company'})"]
        for t in tasks:
            extra = []
            if t.cost: extra.append(f"${t.cost:.2f}")
            if t.time_impact_days: extra.append(f"{t.time_impact_days} d")
            if t.approval_required: extra.append("⚠ Approval")
            note = f" ({', '.join(extra)})" if extra else ""
            lines.append(f"- ({t.id}) {t.text}{note}")

        message = "\n".join(lines)

    # No real send (sandbox). Just log/acknowledge success.
    log.info(f"DAILY_DIGEST_SEND_SANDBOX → {sub_wa}: {message}")
    return jsonify({"status": "ok", "sent_to": sub_wa}), 200

import threading
import time
import pytz
from datetime import datetime
from storage import SessionLocal, User, Task

def daily_digest_scheduler():
    while True:
        now_utc = datetime.utcnow()

        with SessionLocal() as s:
            subs = s.query(User).filter(User.role == "sub", User.active == True).all()

            for sub in subs:
                tzname = sub.timezone or "America/New_York"
                try:
                    tz = pytz.timezone(tzname)
                except:
                    tz = pytz.timezone("America/New_York")

                local_now = now_utc.replace(tzinfo=pytz.utc).astimezone(tz)

                # Only fire at exactly 06:00 local, minutes only (safe in 1-min cycle)
                if local_now.hour == 6 and local_now.minute == 0:

                    # fetch open tasks
                    tasks = (
                        s.query(Task)
                        .filter(Task.sender == sub.wa_id, Task.status == "open")
                        .order_by(Task.id.asc())
                        .all()
                    )

                    # If no open tasks → send nothing (silent skip)
                    if not tasks:
                        continue

                    # Build message
                    lines = [f"📋 Daily Tasks for {sub.name} ({sub.subcontractor_name or 'No Company'})"]
                    for t in tasks:
                        lines.append(f"- ({t.id}) {t.text}")
                    message = "\n".join(lines)

                    # Sandbox-safe "send"
                    log.info(f"DAILY_DIGEST_AUTO_SEND → {sub.wa_id}: {message}")

        time.sleep(60)


# start scheduler thread (daemon)
threading.Thread(target=daily_digest_scheduler, daemon=True).start()

def daily_pm_digest_scheduler():
    while True:
        now_utc = datetime.utcnow()

        with SessionLocal() as s:
            pms = s.query(User).filter(User.role == "pm", User.active == True).all()

            for pm in pms:
                tzname = pm.timezone or "America/New_York"
                try:
                    tz = pytz.timezone(tzname)
                except:
                    tz = pytz.timezone("America/New_York")

                local_now = now_utc.replace(tzinfo=pytz.utc).astimezone(tz)

                # Trigger at exactly 18:00 local
                if local_now.hour == 18 and local_now.minute == 0:
                    # sandbox-safe auto send
                    # one-per-day guard
                    state_key = f"pm_digest_{pm.wa_id}_{local_now.strftime('%Y-%m-%d')}"
                    if os.environ.get(state_key) == "sent":
                        continue
                    os.environ[state_key] = "sent"
                    log.info(f"DAILY_PM_DIGEST_AUTO_SEND → {pm.wa_id}")
        time.sleep(60)

threading.Thread(target=daily_pm_digest_scheduler, daemon=True).start()


# >>> FEATURE_3_REMINDER_SCHEDULER_START — AUTOMATIC DELIVERY V6.1 <<<

def run_due_pm_reminders_once(
    now_utc: Optional[dt.datetime] = None,
    limit: int = 50,
) -> dict:
    """Claim, deliver, and finalize one due-reminder batch."""
    now_utc = now_utc or dt.datetime.utcnow()
    claimed = claim_due_pm_reminders(
        now_utc=now_utc,
        limit=limit,
    )

    delivered = 0
    failed = 0

    for reminder in claimed:
        reminder_id = reminder["id"]
        claim_token = reminder["claim_token"]
        recipient_wa = reminder.get("recipient_wa") or reminder.get("pm_wa")

        try:
            # The stored reminder text is delivered without rewriting.
            ok, response_data = send_whatsapp_text(
                DEFAULT_PHONE_ID,
                recipient_wa,
                reminder.get("text") or "",
            )
        except Exception as exc:
            ok = False
            response_data = {"error": str(exc)}

        if ok:
            completed = complete_pm_reminder_delivery(
                reminder_id,
                claim_token,
                delivered_at=dt.datetime.utcnow(),
            )
            if completed.get("status") == "error":
                log.error(
                    "REMINDER_COMPLETE_FAILED id=%s result=%s",
                    reminder_id,
                    completed,
                )
                failed += 1
            else:
                delivered += 1
        else:
            fail_pm_reminder_delivery(
                reminder_id,
                claim_token,
                error=json.dumps(response_data or {}, default=str),
                failed_at=dt.datetime.utcnow(),
            )
            failed += 1

    return {
        "status": "ok",
        "claimed": len(claimed),
        "delivered": delivered,
        "failed": failed,
    }


def pm_reminder_scheduler():
    while True:
        try:
            result = run_due_pm_reminders_once()
            if result["claimed"]:
                log.info("PM_REMINDER_TICK %s", result)
        except Exception:
            log.exception("PM reminder scheduler tick failed")
        time.sleep(30)


threading.Thread(
    target=pm_reminder_scheduler,
    daemon=True,
    name="hubflo-pm-reminders",
).start()


@app.route("/admin/reminders/tick", methods=["POST"])
def admin_reminders_tick():
    if not _check_admin():
        return _auth_fail()
    return jsonify(run_due_pm_reminders_once()), 200

# >>> FEATURE_3_REMINDER_SCHEDULER_END <<<


# ============================================================
# FUTURE VOICE CHANNEL SUPPORT (TWILIO VOICE STUBS)
# ============================================================

@app.route("/voice/inbound", methods=["POST"])
def voice_inbound_stub():
    """
    Stub for future Twilio Voice inbound-call webhook.
    No action performed; logs minimal metadata only.
    """
    payload = request.get_json(silent=True) or {}
    log.info(f"VOICE_INBOUND_STUB: {json.dumps(payload)[:400]}")
    return jsonify({"status": "stub-ok", "direction": "inbound"}), 200


@app.route("/voice/status", methods=["POST"])
def voice_status_stub():
    """
    Stub for future Twilio Voice call-status events:
    ringing, in-progress, completed, failed.
    No action performed; no DB writes yet.
    """
    payload = request.get_json(silent=True) or {}
    log.info(f"VOICE_STATUS_STUB: {json.dumps(payload)[:400]}")
    return jsonify({"status": "stub-ok"}), 200


@app.route("/voice/completed", methods=["POST"])
def voice_completed_stub():
    """
    Stub for future Twilio 'call completed' events.
    Will later write to CallLog.
    Currently does nothing except log.
    """
    payload = request.get_json(silent=True) or {}
    log.info(f"VOICE_COMPLETED_STUB: {json.dumps(payload)[:400]}")
    return jsonify({"status": "stub-ok", "saved": False}), 200

# ============================================================
# MULTI-PHASE DIGEST (TOGGLE SUPPORT)
# ============================================================

@app.route("/admin/digest/pm/phase_toggle", methods=["POST"])
def admin_digest_pm_phase_toggle():
    """
    Toggle per-phase digest mode for a given project.
    Future: stored in DB (currently ephemeral, memory only).
    """
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    project = (data.get("project_code") or "").strip()
    enable = bool(data.get("enable"))

    if not project:
        return jsonify({"error": "missing project_code"}), 400

    # In v6.1 this is temporary in-memory toggle
    _PHASE_DIGEST_TOGGLE[project] = enable

    return jsonify({
        "status": "ok",
        "project": project,
        "enabled": enable
    }), 200


@app.route("/admin/digest/pm/phase_status", methods=["GET"])
def admin_digest_pm_phase_status():
    """
    Inspect the current toggle value for a project.
    """
    if not _check_admin():
        return _auth_fail()

    project = (request.args.get("project_code") or "").strip()
    if not project:
        return jsonify({"error": "missing project_code"}), 400

    val = _PHASE_DIGEST_TOGGLE.get(project, False)
    return jsonify({
        "status": "ok",
        "project": project,
        "enabled": val
    }), 200

# ============================================================
# MANUAL SCHEDULER TRIGGER (SLC18 — DRY RUN)
# ============================================================
@app.route("/admin/digest/pm/tick", methods=["POST"])
def admin_digest_pm_tick():
    if not _check_admin(): return _auth_fail()
    log.info("SLC18: MANUAL_PM_DIGEST_TICK")
    return admin_digest_pm_send()

@app.route("/admin/digest/sub/tick", methods=["POST"])
def admin_digest_sub_tick():
    if not _check_admin(): return _auth_fail()
    log.info("SLC18: MANUAL_SUB_DIGEST_TICK")
    # resolve subcontractor WA ID for manual trigger
    sub_wa = request.args.get("sender") or request.args.get("sub") or ""
    if not sub_wa:
        return jsonify({"error": "missing sender"}), 400
    return admin_digest_sub_send()

# ---------------------------------------------------------------------
# Admin Reporting — Aggregated Summary (Phase 2)
# ---------------------------------------------------------------------
@app.route("/admin/report/summary", methods=["GET"])
def admin_report_summary():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func

    with SessionLocal() as s:
        total_tasks = s.query(func.count(Task.id)).scalar() or 0
        open_tasks = s.query(func.count(Task.id)).filter(Task.status == "open").scalar() or 0
        approved = s.query(func.count(Task.id)).filter(Task.status == "approved").scalar() or 0
        rejected = s.query(func.count(Task.id)).filter(Task.status == "rejected").scalar() or 0
        done = s.query(func.count(Task.id)).filter(Task.status == "done").scalar() or 0

        total_cost = s.query(func.sum(Task.cost)).scalar() or 0.0
        total_time_impact = s.query(func.sum(Task.time_impact_days)).scalar() or 0.0

        with_cost = s.query(func.count(Task.id)).filter(Task.cost != None).scalar() or 0
        with_time = s.query(func.count(Task.id)).filter(Task.time_impact_days != None).scalar() or 0

    return jsonify({
        "summary": {
            "total_tasks": total_tasks,
            "open": open_tasks,
            "approved": approved,
            "rejected": rejected,
            "done": done
        },
        "change_orders": {
            "total_cost": round(total_cost, 2),
            "total_time_impact_days": float(total_time_impact),
            "count_with_cost": with_cost,
            "count_with_time_impact": with_time
        },
        "status": "aggregated-ok"
    }), 200

# === ADMIN REPORT DASHBOARD (HTML VIEW) ============================
@app.route("/admin/report/view", methods=["GET"])
def admin_report_view():
    if not _check_admin():
        return _auth_fail()

    # Fetch JSON data from the same summary route
    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_summary", token=request.args.get("token"))
    ).get_json(force=True)

    ch = summary.get("change_orders", {})
    s = summary.get("summary", {})

    body = f"""
    <html><head><title>HubFlo Report Dashboard</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      h2{{margin-top:0}}
      table{{border-collapse:collapse;width:60%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Summary Dashboard</h2>

      <table>
        <tr><th colspan=2>Task Summary</th></tr>
        <tr><td>Total Tasks</td><td>{s.get('total_tasks',0)}</td></tr>
        <tr><td>Open</td><td>{s.get('open',0)}</td></tr>
        <tr><td>Approved</td><td>{s.get('approved',0)}</td></tr>
        <tr><td>Done</td><td>{s.get('done',0)}</td></tr>
        <tr><td>Rejected</td><td>{s.get('rejected',0)}</td></tr>
      </table>

      <table>
        <tr><th colspan=2>Change Orders</th></tr>
        <tr><td>Count w/ Cost</td><td>{ch.get('count_with_cost',0)}</td></tr>
        <tr><td>Count w/ Time Impact</td><td>{ch.get('count_with_time_impact',0)}</td></tr>
        <tr><td>Total Cost ($)</td><td>{ch.get('total_cost',0.0)}</td></tr>
        <tr><td>Total Time Impact (days)</td><td>{ch.get('total_time_impact_days',0.0)}</td></tr>
      </table>

      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# ---------------------------------------------------------------------
# Admin Reporting — Subcontractor Performance (Phase 4)
# ---------------------------------------------------------------------
@app.route("/admin/report/performance", methods=["GET"])
def admin_report_performance():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func, case

    with SessionLocal() as s:
        rows = (
            s.query(
                Task.subcontractor_name,
                func.count(Task.id).label("total"),
                func.sum(case((Task.status == "done", 1), else_=0)).label("done"),
                func.sum(case((Task.status == "approved", 1), else_=0)).label("approved"),
                func.sum(case((Task.status == "rejected", 1), else_=0)).label("rejected"),
                func.sum(case((Task.is_rework.is_(True), 1), else_=0)).label("reworks"),
                func.sum(case(((Task.overrun_days > 0), 1), else_=0)).label("overruns"),
            )
            .group_by(Task.subcontractor_name)
            .order_by(Task.subcontractor_name.asc())
            .all()
        )

        result = []
        for r in rows:
            name = r.subcontractor_name or "(unassigned)"
            total = r.total or 0
            on_time = (r.done or 0) - (r.overruns or 0)
            pct = 0 if total == 0 else round(100.0 * on_time / total, 1)
            result.append({
                "subcontractor": name,
                "total": total,
                "done": r.done or 0,
                "approved": r.approved or 0,
                "rejected": r.rejected or 0,
                "reworks": r.reworks or 0,
                "overruns": r.overruns or 0,
                "accuracy_pct": pct,
            })

    return jsonify({"status": "ok", "performance": result}), 200


# === ADMIN PERFORMANCE DASHBOARD (HTML VIEW) ============================
@app.route("/admin/report/performance/view", methods=["GET"])
def admin_report_performance_view():
    if not _check_admin():
        return _auth_fail()

    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_performance", token=request.args.get("token"))
    ).get_json(force=True)

    rows = summary.get("performance", [])
    body_rows = "".join(
        f"<tr><td>{r['subcontractor']}</td>"
        f"<td>{r['total']}</td>"
        f"<td>{r['done']}</td>"
        f"<td>{r['approved']}</td>"
        f"<td>{r['rejected']}</td>"
        f"<td>{r['reworks']}</td>"
        f"<td>{r['overruns']}</td>"
        f"<td>{r['accuracy_pct']}%</td></tr>"
        for r in rows
    )

    body = f"""
    <html><head><title>HubFlo Performance Report</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      table{{border-collapse:collapse;width:90%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Subcontractor Performance</h2>
      <table>
        <tr>
          <th>Subcontractor</th><th>Total</th><th>Done</th><th>Approved</th>
          <th>Rejected</th><th>Reworks</th><th>Overruns</th><th>Accuracy %</th>
        </tr>
        {body_rows or "<tr><td colspan=8>No data</td></tr>"}
      </table>
      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# ---------------------------------------------------------------------
# Admin Reporting — Per-Project Summary (Phase 5)
# ---------------------------------------------------------------------
@app.route("/admin/report/project", methods=["GET"])
def admin_report_project():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func, case

    with SessionLocal() as s:
        rows = (
            s.query(
                Task.project_code,
                func.count(Task.id).label("total"),
                func.sum(func.coalesce(Task.cost, 0)).label("total_cost"),
                func.sum(func.coalesce(Task.time_impact_days, 0)).label("total_time_impact_days"),
                func.sum(case((Task.status == "open", 1), else_=0)).label("open"),
                func.sum(case((Task.status == "approved", 1), else_=0)).label("approved"),
                func.sum(case((Task.status == "done", 1), else_=0)).label("done"),
                func.sum(case((Task.status == "rejected", 1), else_=0)).label("rejected"),
            )
            .group_by(Task.project_code)
            .order_by(Task.project_code.asc())
            .all()
        )

        result = []
        for r in rows:
            result.append({
                "project_code": r.project_code or "(unassigned)",
                "total_tasks": r.total or 0,
                "open": r.open or 0,
                "approved": r.approved or 0,
                "done": r.done or 0,
                "rejected": r.rejected or 0,
                "total_cost": round(float(r.total_cost or 0), 2),
                "total_time_impact_days": float(r.total_time_impact_days or 0),
            })

    return jsonify({"status": "ok", "projects": result}), 200


# === ADMIN PROJECT SUMMARY DASHBOARD (HTML VIEW) =====================
@app.route("/admin/report/project/view", methods=["GET"])
def admin_report_project_view():
    if not _check_admin():
        return _auth_fail()

    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_project", token=request.args.get("token"))
    ).get_json(force=True)

    rows = summary.get("projects", [])

    body_rows = ""
    for r in rows:
        body_rows += (
            f"<tr><td>{r['project_code']}</td>"
            f"<td>{r['total_tasks']}</td>"
            f"<td>{r['open']}</td>"
            f"<td>{r['approved']}</td>"
            f"<td>{r['done']}</td>"
            f"<td>{r['rejected']}</td>"
            f"<td>{r['total_cost']}</td>"
            f"<td>{r['total_time_impact_days']}</td></tr>"
        )

    body = f"""
    <html><head><title>HubFlo Project Summary</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      table{{border-collapse:collapse;width:80%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Per-Project Summary</h2>
      <table>
        <tr><th>Project</th><th>Total</th><th>Open</th><th>Approved</th>
            <th>Done</th><th>Rejected</th><th>Total Cost ($)</th><th>Time Impact (days)</th></tr>
        {body_rows if body_rows else "<tr><td colspan=8>No data</td></tr>"}
      </table>
      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# ---------------------------------------------------------------------
# Admin Reporting — Global Overview (Phase 6)
# ---------------------------------------------------------------------
@app.route("/admin/report/overview", methods=["GET"])
def admin_report_overview():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func

    with SessionLocal() as s:
        total_tasks = s.query(func.count(Task.id)).scalar() or 0
        open_tasks = s.query(func.count(Task.id)).filter(Task.status == "open").scalar() or 0
        approved = s.query(func.count(Task.id)).filter(Task.status == "approved").scalar() or 0
        rejected = s.query(func.count(Task.id)).filter(Task.status == "rejected").scalar() or 0
        done = s.query(func.count(Task.id)).filter(Task.status == "done").scalar() or 0

        total_cost = s.query(func.sum(Task.cost)).scalar() or 0.0
        total_time = s.query(func.sum(Task.time_impact_days)).scalar() or 0.0

        total_subs = s.query(func.count(func.distinct(Task.subcontractor_name))).scalar() or 0
        total_projects = s.query(func.count(func.distinct(Task.project_code))).scalar() or 0

    return jsonify({
        "summary": {
            "total_tasks": total_tasks,
            "open": open_tasks,
            "approved": approved,
            "done": done,
            "rejected": rejected,
        },
        "totals": {
            "projects": total_projects,
            "subcontractors": total_subs,
            "total_cost": round(total_cost, 2),
            "total_time_impact_days": float(total_time),
        },
        "status": "ok"
    }), 200

@app.route("/admin/test_seed", methods=["GET"])
def admin_test_seed():
    """
    One-off test data seeder for sandbox.
    Hit:
      /admin/test_seed?token=YOUR_ADMIN_TOKEN
    and it will insert a few example projects, subs and tasks.
    """
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, User  # reuse existing storage binding

    created_users = 0
    created_tasks = 0

    with SessionLocal() as s:
        # --- Ensure a PM linked to YOUR number ---------------------------
        pm_wa = "13522098414"  # your sandbox WA
        pm = (
            s.query(User)
            .filter(User.wa_id == pm_wa, User.active == True)
            .first()
        )
        if not pm:
            pm = User(
                wa_id=pm_wa,
                name="Nev (PM)",
                role="pm",
                subcontractor_name=None,
                project_code=None,
                phone=pm_wa,
                active=True,
            )
            s.add(pm)
            created_users += 1

        # --- Ensure a few subs -------------------------------------------
        def get_or_create_sub(wa_id, name, company, project_code):
            nonlocal created_users
            u = (
                s.query(User)
                .filter(User.wa_id == wa_id, User.active == True)
                .first()
            )
            if not u:
                u = User(
                    wa_id=wa_id,
                    name=name,
                    role="sub",
                    subcontractor_name=company,
                    project_code=project_code,
                    phone=wa_id,
                    active=True,
                )
                s.add(u)
                created_users += 1
            return u

        sub_paint = get_or_create_sub(
            "278200000001", "Alex Painter", "BrightCo Painting", "OCALA-01"
        )
        sub_plumb = get_or_create_sub(
            "278200000002", "Sam Plumber", "XCX Plumbing", "OCALA-01"
        )
        sub_misc = get_or_create_sub(
            "278200000003", "Mike Builder", "General Build Co", "OCALA-02"
        )

        # --- Create example tasks ----------------------------------------
        from storage_v6_1 import Task  # use same Task model as rest of app
        import datetime as dt

        now = dt.datetime.utcnow()

        tasks = []

        # Painting jobs on different sites
        tasks.append(Task(
            sender=sub_paint.wa_id,
            text="Paint all interior walls in units 1–4",
            tag="task",
            status="open",
            project_code="OCALA-01",
            subcontractor_name=sub_paint.subcontractor_name,
            ts=now,
        ))
        tasks.append(Task(
            sender=sub_paint.wa_id,
            text="Repaint exterior of block B (north elevation)",
            tag="task",
            status="open",
            project_code="OCALA-02",
            subcontractor_name=sub_paint.subcontractor_name,
            ts=now,
        ))

        # Plumbing jobs, including an overrun
        tasks.append(Task(
            sender=sub_plumb.wa_id,
            text="Fix leaking pipe in unit 3 bathroom",
            tag="task",
            status="open",
            project_code="OCALA-01",
            subcontractor_name=sub_plumb.subcontractor_name,
            ts=now,
            overrun_days=0.0,
        ))
        tasks.append(Task(
            sender=sub_plumb.wa_id,
            text="Replace main water line for block A (overrun)",
            tag="task",
            status="open",
            project_code="OCALA-01",
            subcontractor_name=sub_plumb.subcontractor_name,
            ts=now,
            overrun_days=4.0,   # treated as overrun
        ))

        # A general urgent task
        tasks.append(Task(
            sender=sub_misc.wa_id,
            text="Urgent: secure loose roof sheeting over unit 5",
            tag="urgent",
            status="open",
            project_code="OCALA-02",
            subcontractor_name=sub_misc.subcontractor_name,
            ts=now,
        ))

        for t in tasks:
            s.add(t)
        created_tasks = len(tasks)

        s.commit()

    return jsonify({
        "status": "ok",
        "created_users": created_users,
        "created_tasks": created_tasks
    }), 200

# === ADMIN OVERVIEW DASHBOARD (HTML VIEW) ============================
@app.route("/admin/report/overview/view", methods=["GET"])
def admin_report_overview_view():
    if not _check_admin():
        return _auth_fail()

    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_overview", token=request.args.get("token"))
    ).get_json(force=True)

    s = summary.get("summary", {})
    t = summary.get("totals", {})

    body = f"""
    <html><head><title>HubFlo Global Overview</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      table{{border-collapse:collapse;width:50%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Global Overview</h2>
      <table>
        <tr><th colspan=2>Task Totals</th></tr>
        <tr><td>Total Tasks</td><td>{s.get('total_tasks',0)}</td></tr>
        <tr><td>Open</td><td>{s.get('open',0)}</td></tr>
        <tr><td>Approved</td><td>{s.get('approved',0)}</td></tr>
        <tr><td>Done</td><td>{s.get('done',0)}</td></tr>
        <tr><td>Rejected</td><td>{s.get('rejected',0)}</td></tr>
      </table>

      <table>
        <tr><th colspan=2>Totals</th></tr>
        <tr><td>Projects</td><td>{t.get('projects',0)}</td></tr>
        <tr><td>Subcontractors</td><td>{t.get('subcontractors',0)}</td></tr>
        <tr><td>Total Cost ($)</td><td>{t.get('total_cost',0.0)}</td></tr>
        <tr><td>Total Time Impact (days)</td><td>{t.get('total_time_impact_days',0.0)}</td></tr>
      </table>

      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# >>> PATCH_1_APP_START — CALL LOG ENDPOINT <<<

from storage import log_call

@app.route("/admin/voice/log", methods=["POST"])
def admin_voice_log():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}

    direction = (data.get("direction") or "").strip().lower()   # inbound | outbound
    from_wa   = (data.get("from") or "").strip()
    to_wa     = (data.get("to") or "").strip()
    duration  = data.get("duration_seconds")
    notes     = data.get("notes")

    if direction not in ("inbound", "outbound"):
        return jsonify({"error": "direction must be inbound|outbound"}), 400

    if not from_wa or not to_wa:
        return jsonify({"error": "missing from or to"}), 400

    try:
        duration = int(duration) if duration is not None else None
    except:
        return jsonify({"error": "invalid duration_seconds"}), 400

    rec = log_call(
        direction=direction,
        from_wa=from_wa,
        to_wa=to_wa,
        duration_seconds=duration,
        notes=notes,
    )

    return jsonify({"status": "ok", "call": rec}), 200

# >>> PATCH_1_APP_END <<<

# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

if __name__=="__main__":
    port=int(os.environ.get("PORT","10000"))
    app.run(host="0.0.0.0",port=port,debug=False)
