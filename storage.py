# storage.py — HUBFLO V6.1 working
# Derived from verified v5 base + reinforced tethered safeguards
# ---------------------------------------------------------------------
import os
import json
import datetime as dt
from typing import Optional, Iterable

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, Boolean, Float,
    UniqueConstraint,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import inspect, text

# ---------------------------------------------------------------------
# DB bootstrap
# ---------------------------------------------------------------------
def _normalize_db_url(url: str) -> str:
    if not url:
        return "sqlite:///hubflo.db"
    # Use psycopg (v3) driver explicitly
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url

DATABASE_URL = _normalize_db_url(os.environ.get("DATABASE_URL", "").strip())
ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, future=True)
Base = declarative_base()

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------

# >>> PATCH_4_STORAGE_START — MULTI-TENANCY CLIENT FIELD <<<

# Every persisted object belongs to a client_id to ensure isolation of
# all data when multiple clients share one WhatsApp number.
# Default client_id = 1 until multi-client onboarding UI is added.

DEFAULT_CLIENT_ID = 1

def current_client_id() -> int:
    # Placeholder: returns DEFAULT_CLIENT_ID for now.
    # Future toggle will override this.
    return DEFAULT_CLIENT_ID

# >>> PATCH_4_STORAGE_END <<<

# --- NEW: People & Role Model (Hierarchy Lookup) ----------------------
class User(Base):
    __tablename__ = "users"

    client_id = Column(Integer, default=DEFAULT_CLIENT_ID, index=True)
    id = Column(Integer, primary_key=True)
    wa_id = Column(String(64), unique=True, index=True)  # WhatsApp ID
    name = Column(String(128))
    role = Column(String(32))  # sub | pm | ops | director | owner
    subcontractor_name = Column(String(128), nullable=True)
    project_code = Column(String(128), nullable=True)

    phone = Column(String(64), nullable=True)
    active = Column(Boolean, default=True)

    timezone = Column(String(64), default="America/New_York")  # default timezone
    date_order = Column(String(16), default="month_first")
    time_format = Column(String(8), default="12h")
    date_display = Column(String(16), default="month_first")

    created_at = Column(DateTime, default=dt.datetime.utcnow)
    updated_at = Column(DateTime, default=dt.datetime.utcnow,
                        onupdate=dt.datetime.utcnow)

# >>> PATCH_5_STORAGE_START — CLIENT DISPLAY NAME <<<

# Per-client WhatsApp display name
class ClientWAIdentity(Base):
    __tablename__ = "client_wa_identity"

    id = Column(Integer, primary_key=True)
    client_id = Column(String(64), index=True, nullable=False)
    display_name_for_whatsapp = Column(String(128), nullable=False)

# lookup helper
def get_client_display_name(client_id: str) -> Optional[str]:
    with SessionLocal() as s:
        row = (
            s.query(ClientWAIdentity)
            .filter(ClientWAIdentity.client_id == client_id)
            .first()
        )
        return row.display_name_for_whatsapp if row else None

# setter helper
def set_client_display_name(client_id: str, name: str) -> dict:
    with SessionLocal() as s:
        row = (
            s.query(ClientWAIdentity)
            .filter(ClientWAIdentity.client_id == client_id)
            .first()
        )
        if not row:
            row = ClientWAIdentity(
                client_id=client_id,
                display_name_for_whatsapp=name.strip()
            )
            s.add(row)
        else:
            row.display_name_for_whatsapp = name.strip()

        s.commit()
        s.refresh(row)
        return {
            "client_id": row.client_id,
            "display_name_for_whatsapp": row.display_name_for_whatsapp
        }

# >>> PATCH_5_STORAGE_END <<<


# >>> PATCH_1_INSPECTION_STORAGE_START — INSPECTOR SCHEDULING V6.1 <<<

class Inspection(Base):
    __tablename__ = "inspections"

    id = Column(Integer, primary_key=True)
    project_code = Column(String, index=True)
    phase = Column(String)
    required_date = Column(DateTime)
    actual_date = Column(DateTime)
    inspector = Column(String)
    notes = Column(Text)
    created_at = Column(DateTime, default=dt.datetime.utcnow)


def create_inspection(payload: dict) -> dict:
    with SessionLocal() as s:
        ins = Inspection(
            project_code=payload.get("project_code"),
            phase=payload.get("phase"),
            required_date=payload.get("required_date"),
            inspector=payload.get("inspector"),
            notes=payload.get("notes"),
        )
        s.add(ins)
        s.commit()
        s.refresh(ins)

        return {"id": ins.id}

# >>> PATCH_1_INSPECTION_STORAGE_END <<<

# >>> PATCH_2_DELAY_STORAGE_START — CRITICAL-PATH DELAY TRACKING V6.1 <<<

class DelayLog(Base):
    __tablename__ = "delay_logs"

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, index=True)
    project_code = Column(String, index=True)
    reporter = Column(String)
    days = Column(Float)
    reason = Column(Text)
    created_at = Column(DateTime, default=dt.datetime.utcnow)


def log_delay(payload: dict) -> dict:
    """
    Validate and create a critical-path delay record.

    Required:
      - sender mapped to a project
      - valid existing task_id
      - task mapped to a project
      - task project exactly matches sender project
      - positive delay duration
    """
    task_id = payload.get("task_id")
    project_code = payload.get("project_code")
    reporter = payload.get("reporter")
    reason = payload.get("reason")

    sender_project_code = (
        str(project_code).strip()
        if project_code is not None
        else ""
    )

    if not sender_project_code:
        return {
            "status": "error",
            "code": "sender_project_missing",
            "message": (
                "The sender is not mapped to a project."
            ),
        }

    try:
        task_id = int(task_id)
    except (TypeError, ValueError):
        return {
            "status": "error",
            "code": "invalid_task_id",
            "message": "A valid task number is required.",
        }

    try:
        delay_days = float(payload.get("days"))
    except (TypeError, ValueError):
        return {
            "status": "error",
            "code": "invalid_delay_days",
            "message": "A valid delay duration is required.",
        }

    if delay_days <= 0:
        return {
            "status": "error",
            "code": "invalid_delay_days",
            "message": (
                "Delay duration must be greater than zero."
            ),
        }

    with SessionLocal() as s:
        task = s.get(Task, task_id)

        if not task:
            return {
                "status": "error",
                "code": "task_not_found",
                "message": f"Task {task_id} was not found.",
            }

        task_project_code = (
            str(task.project_code).strip()
            if task.project_code is not None
            else ""
        )

        if not task_project_code:
            return {
                "status": "error",
                "code": "task_project_missing",
                "message": (
                    f"Task {task_id} is not mapped "
                    f"to a project."
                ),
            }

        if task_project_code != sender_project_code:
            return {
                "status": "error",
                "code": "project_mismatch",
                "message": (
                    f"Task {task_id} belongs to project "
                    f"{task_project_code}, not "
                    f"{sender_project_code}."
                ),
            }

        delay = DelayLog(
            task_id=task.id,
            project_code=task_project_code,
            reporter=reporter,
            days=delay_days,
            reason=reason,
        )

        s.add(delay)
        s.commit()
        s.refresh(delay)

        return {
            "status": "ok",
            "id": delay.id,
            "task_id": delay.task_id,
            "project_code": delay.project_code,
            "days": delay.days,
        }

# >>> PATCH_2_DELAY_STORAGE_END <<<

# >>> FEATURE_3_REMINDER_STORAGE_START — REMINDER FRAMEWORK COMPLETION V6.1 <<<

class PMReminder(Base):
    __tablename__ = "pm_reminders"

    id = Column(Integer, primary_key=True)

    # The owner never changes. recipient_wa may change when redirected.
    pm_wa = Column(String(64), nullable=False, index=True)
    recipient_wa = Column(String(64), nullable=True, index=True)
    project_code = Column(String(128), nullable=True, index=True)

    # Preserve the inbound reminder text exactly; rule is scheduling metadata.
    text = Column(Text, nullable=False)
    rule = Column(String(128), nullable=True)
    timezone = Column(String(64), default="America/New_York")

    # next_run is stored as naive UTC for compatibility with existing storage.
    next_run = Column(DateTime, nullable=False, index=True)
    recurring = Column(Boolean, default=False, index=True)
    recurrence_rule = Column(String(32), default="none")
    recurrence_interval = Column(Integer, default=1)
    recurrence_seconds = Column(Integer, nullable=True)
    recurrence_anchor_day = Column(Integer, nullable=True)

    status = Column(String(24), default="active", index=True)
    active = Column(Boolean, default=True, index=True)

    # Database-backed delivery claim prevents concurrent scheduler execution.
    claimed_at = Column(DateTime, nullable=True, index=True)
    claim_token = Column(String(64), nullable=True, index=True)
    retry_after = Column(DateTime, nullable=True, index=True)

    delivered_at = Column(DateTime, nullable=True)
    acknowledged_at = Column(DateTime, nullable=True)
    snoozed_at = Column(DateTime, nullable=True)
    redirected_at = Column(DateTime, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    failed_at = Column(DateTime, nullable=True)

    delivery_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime, default=dt.datetime.utcnow)
    updated_at = Column(
        DateTime,
        default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow,
    )


def _normalize_pm_reminder_utc(value) -> Optional[dt.datetime]:
    if value is None:
        return None

    if isinstance(value, str):
        try:
            value = dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None

    if not isinstance(value, dt.datetime):
        return None

    if value.tzinfo is not None:
        value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)

    return value


def _as_pm_reminder_dict(reminder: PMReminder) -> dict:
    return {
        "id": reminder.id,
        "pm_wa": reminder.pm_wa,
        "recipient_wa": reminder.recipient_wa or reminder.pm_wa,
        "project_code": reminder.project_code,
        "text": reminder.text,
        "rule": reminder.rule,
        "timezone": reminder.timezone or "America/New_York",
        "next_run": reminder.next_run,
        "recurring": bool(reminder.recurring),
        "recurrence_rule": reminder.recurrence_rule or "none",
        "recurrence_interval": reminder.recurrence_interval or 1,
        "recurrence_seconds": reminder.recurrence_seconds,
        "recurrence_anchor_day": reminder.recurrence_anchor_day,
        "status": reminder.status,
        "active": bool(reminder.active),
        "claim_token": reminder.claim_token,
        "retry_after": reminder.retry_after,
        "delivered_at": reminder.delivered_at,
        "acknowledged_at": reminder.acknowledged_at,
        "cancelled_at": reminder.cancelled_at,
        "completed_at": reminder.completed_at,
        "delivery_count": reminder.delivery_count or 0,
        "failure_count": reminder.failure_count or 0,
        "last_error": reminder.last_error,
        "created_at": reminder.created_at,
        "updated_at": reminder.updated_at,
    }


def create_pm_reminder(payload: dict) -> dict:
    """Create a scheduled reminder while preserving its text exactly."""
    payload = payload or {}
    owner_wa = str(payload.get("pm_wa") or payload.get("owner_wa") or "").strip()
    if not owner_wa:
        return {"status": "error", "code": "owner_required"}

    next_run = _normalize_pm_reminder_utc(payload.get("next_run"))
    if next_run is None:
        return {"status": "error", "code": "next_run_required"}

    original_text = payload.get("text")
    if original_text is None:
        original_text = payload.get("reminder_text")
    if original_text is None:
        original_text = payload.get("rule")
    if original_text is None:
        original_text = ""
    if not isinstance(original_text, str):
        original_text = str(original_text)

    with SessionLocal() as s:
        timezone_name = payload.get("timezone")
        if not timezone_name:
            owner = s.query(User).filter(User.wa_id == owner_wa).first()
            timezone_name = (
                owner.timezone
                if owner and owner.timezone
                else "America/New_York"
            )

        recurrence_rule = str(
            payload.get("recurrence_rule") or "none"
        ).strip().lower()
        recurrence_interval = payload.get("recurrence_interval") or 1
        try:
            recurrence_interval = max(1, int(recurrence_interval))
        except (TypeError, ValueError):
            recurrence_interval = 1

        recurrence_seconds = payload.get("recurrence_seconds")
        try:
            recurrence_seconds = (
                int(recurrence_seconds)
                if recurrence_seconds is not None
                else None
            )
        except (TypeError, ValueError):
            recurrence_seconds = None

        recurring = bool(payload.get("recurring")) or (
            recurrence_rule not in ("", "none", "once")
        ) or bool(recurrence_seconds)

        anchor_day = payload.get("recurrence_anchor_day")
        try:
            anchor_day = int(anchor_day) if anchor_day is not None else None
        except (TypeError, ValueError):
            anchor_day = None

        reminder = PMReminder(
            pm_wa=owner_wa,
            recipient_wa=(
                str(payload.get("recipient_wa") or owner_wa).strip()
                or owner_wa
            ),
            project_code=payload.get("project_code"),
            text=original_text,
            rule=payload.get("rule") or (recurrence_rule if recurring else "once"),
            timezone=str(timezone_name or "America/New_York"),
            next_run=next_run,
            recurring=recurring,
            recurrence_rule=recurrence_rule if recurring else "none",
            recurrence_interval=recurrence_interval,
            recurrence_seconds=recurrence_seconds,
            recurrence_anchor_day=anchor_day,
            status="active",
            active=True,
        )
        s.add(reminder)
        s.commit()
        s.refresh(reminder)
        result = _as_pm_reminder_dict(reminder)

    log_audit(
        owner_wa,
        "reminder_create",
        "pm_reminder",
        result["id"],
        details=f"next_run={result['next_run']}",
    )
    result["status"] = "ok"
    return result


def _pm_reminder_recurrence_kind(reminder: PMReminder) -> str:
    kind = (reminder.recurrence_rule or "").strip().lower()
    if kind not in ("", "none", "once"):
        return kind

    rule_text = (reminder.rule or "").lower()
    if "weekday" in rule_text:
        return "weekdays"
    if "month" in rule_text:
        return "monthly"
    if "week" in rule_text:
        return "weekly"
    if "day" in rule_text or "daily" in rule_text:
        return "daily"
    if "hour" in rule_text or "hourly" in rule_text:
        return "hourly"
    if reminder.recurrence_seconds:
        return "interval"
    return "none"


def _pm_reminder_is_recurring(reminder: PMReminder) -> bool:
    return bool(reminder.recurring) or (
        _pm_reminder_recurrence_kind(reminder) != "none"
    )


def _add_pm_reminder_months(
    local_value: dt.datetime,
    months: int,
    anchor_day: int,
) -> dt.datetime:
    import calendar

    month_index = (local_value.year * 12 + local_value.month - 1) + months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(anchor_day, calendar.monthrange(year, month)[1])
    return local_value.replace(year=year, month=month, day=day)


def _advance_pm_reminder_next_run(
    reminder: PMReminder,
    after_utc: dt.datetime,
) -> dt.datetime:
    """Advance recurrence in the owner's timezone and return naive UTC."""
    from zoneinfo import ZoneInfo

    after_utc = _normalize_pm_reminder_utc(after_utc) or dt.datetime.utcnow()
    current_utc = reminder.next_run or after_utc
    kind = _pm_reminder_recurrence_kind(reminder)
    interval = max(1, int(reminder.recurrence_interval or 1))

    if kind in ("interval", "hourly"):
        seconds = reminder.recurrence_seconds
        if not seconds:
            seconds = 3600 * interval
        seconds = max(60, int(seconds))
        elapsed = max(0.0, (after_utc - current_utc).total_seconds())
        steps = max(1, int(elapsed // seconds) + 1)
        return current_utc + dt.timedelta(seconds=steps * seconds)

    try:
        owner_tz = ZoneInfo(reminder.timezone or "America/New_York")
    except Exception:
        owner_tz = ZoneInfo("America/New_York")

    current_local = current_utc.replace(
        tzinfo=dt.timezone.utc
    ).astimezone(owner_tz)
    candidate_local = current_local
    anchor_day = reminder.recurrence_anchor_day or current_local.day

    for _ in range(3700):
        if kind == "daily":
            candidate_local = candidate_local + dt.timedelta(days=interval)
        elif kind == "weekly":
            candidate_local = candidate_local + dt.timedelta(weeks=interval)
        elif kind == "monthly":
            candidate_local = _add_pm_reminder_months(
                candidate_local,
                interval,
                anchor_day,
            )
        elif kind == "weekdays":
            business_days = interval
            while business_days > 0:
                candidate_local = candidate_local + dt.timedelta(days=1)
                if candidate_local.weekday() < 5:
                    business_days -= 1
        else:
            candidate_local = candidate_local + dt.timedelta(days=1)

        candidate_utc = candidate_local.astimezone(
            dt.timezone.utc
        ).replace(tzinfo=None)
        if candidate_utc > after_utc:
            return candidate_utc

    # Defensive fallback; normal recurrence paths return inside the loop.
    return after_utc + dt.timedelta(days=1)


def claim_due_pm_reminders(
    now_utc: Optional[dt.datetime] = None,
    limit: int = 50,
    lease_seconds: int = 300,
) -> list[dict]:
    """Atomically claim due reminders so only one worker can deliver each."""
    import uuid
    from sqlalchemy import or_

    now_utc = _normalize_pm_reminder_utc(now_utc) or dt.datetime.utcnow()
    stale_before = now_utc - dt.timedelta(seconds=max(60, lease_seconds))

    with SessionLocal() as s:
        stale_rows = (
            s.query(PMReminder)
            .filter(
                PMReminder.active == True,
                PMReminder.status == "delivering",
                PMReminder.claimed_at != None,
                PMReminder.claimed_at <= stale_before,
            )
            .all()
        )
        for stale in stale_rows:
            stale.status = "active"
            stale.claimed_at = None
            stale.claim_token = None
            stale.retry_after = now_utc + dt.timedelta(minutes=5)
            stale.failed_at = now_utc
            stale.failure_count = (stale.failure_count or 0) + 1
            stale.last_error = "delivery claim expired before completion"

        s.flush()

        candidate_ids = [
            row[0]
            for row in (
                s.query(PMReminder.id)
                .filter(
                    PMReminder.active == True,
                    PMReminder.status == "active",
                    PMReminder.next_run <= now_utc,
                    or_(
                        PMReminder.retry_after == None,
                        PMReminder.retry_after <= now_utc,
                    ),
                )
                .order_by(PMReminder.next_run.asc(), PMReminder.id.asc())
                .limit(max(1, int(limit)))
                .all()
            )
        ]

        claimed = []
        for reminder_id in candidate_ids:
            token = uuid.uuid4().hex
            updated = (
                s.query(PMReminder)
                .filter(
                    PMReminder.id == reminder_id,
                    PMReminder.active == True,
                    PMReminder.status == "active",
                    PMReminder.next_run <= now_utc,
                    or_(
                        PMReminder.retry_after == None,
                        PMReminder.retry_after <= now_utc,
                    ),
                )
                .update(
                    {
                        PMReminder.status: "delivering",
                        PMReminder.claimed_at: now_utc,
                        PMReminder.claim_token: token,
                    },
                    synchronize_session=False,
                )
            )
            if updated == 1:
                s.flush()
                reminder = s.get(PMReminder, reminder_id)
                claimed.append(_as_pm_reminder_dict(reminder))

        s.commit()
        return claimed


def complete_pm_reminder_delivery(
    reminder_id: int,
    claim_token: str,
    delivered_at: Optional[dt.datetime] = None,
) -> dict:
    delivered_at = (
        _normalize_pm_reminder_utc(delivered_at)
        or dt.datetime.utcnow()
    )

    with SessionLocal() as s:
        reminder = (
            s.query(PMReminder)
            .filter(
                PMReminder.id == int(reminder_id),
                PMReminder.status == "delivering",
                PMReminder.claim_token == claim_token,
            )
            .first()
        )
        if not reminder:
            return {"status": "error", "code": "claim_not_found"}

        reminder.delivered_at = delivered_at
        reminder.delivery_count = (reminder.delivery_count or 0) + 1
        reminder.failure_count = 0
        reminder.failed_at = None
        reminder.last_error = None
        reminder.retry_after = None
        reminder.claimed_at = None
        reminder.claim_token = None
        reminder.acknowledged_at = None

        if _pm_reminder_is_recurring(reminder):
            reminder.recurring = True
            reminder.next_run = _advance_pm_reminder_next_run(
                reminder,
                delivered_at,
            )
            reminder.status = "active"
            reminder.active = True
            reminder.completed_at = None
        else:
            reminder.status = "completed"
            reminder.active = False
            reminder.completed_at = delivered_at

        s.commit()
        s.refresh(reminder)
        result = _as_pm_reminder_dict(reminder)

    log_audit(
        reminder.pm_wa,
        "reminder_delivered",
        "pm_reminder",
        int(reminder_id),
        details=f"recipient={result['recipient_wa']}",
    )
    result["status_result"] = "ok"
    return result


def fail_pm_reminder_delivery(
    reminder_id: int,
    claim_token: str,
    error: str,
    failed_at: Optional[dt.datetime] = None,
) -> dict:
    failed_at = _normalize_pm_reminder_utc(failed_at) or dt.datetime.utcnow()

    with SessionLocal() as s:
        reminder = (
            s.query(PMReminder)
            .filter(
                PMReminder.id == int(reminder_id),
                PMReminder.status == "delivering",
                PMReminder.claim_token == claim_token,
            )
            .first()
        )
        if not reminder:
            return {"status": "error", "code": "claim_not_found"}

        reminder.failure_count = (reminder.failure_count or 0) + 1
        backoff_minutes = min(
            60,
            5 * (2 ** min(reminder.failure_count - 1, 4)),
        )
        reminder.failed_at = failed_at
        reminder.last_error = str(error or "delivery failed")[:2000]
        reminder.retry_after = failed_at + dt.timedelta(
            minutes=backoff_minutes
        )
        reminder.status = "active"
        reminder.active = True
        reminder.claimed_at = None
        reminder.claim_token = None

        s.commit()
        s.refresh(reminder)
        result = _as_pm_reminder_dict(reminder)

    log_audit(
        reminder.pm_wa,
        "reminder_delivery_failed",
        "pm_reminder",
        int(reminder_id),
        details=result["last_error"],
    )
    result["status_result"] = "retry_scheduled"
    return result


def _pm_reminder_for_actor(
    session,
    actor_wa: str,
    reminder_id: Optional[int] = None,
    active_only: bool = False,
    delivered_only: bool = False,
) -> Optional[PMReminder]:
    q = session.query(PMReminder).filter(
        (PMReminder.pm_wa == actor_wa)
        | (PMReminder.recipient_wa == actor_wa)
    )

    if reminder_id is not None:
        try:
            q = q.filter(PMReminder.id == int(reminder_id))
        except (TypeError, ValueError):
            return None

    if active_only:
        q = q.filter(
            PMReminder.active == True,
            PMReminder.status == "active",
        )

    if delivered_only:
        q = q.filter(
            PMReminder.delivered_at != None,
            PMReminder.status != "cancelled",
        )
        return q.order_by(
            PMReminder.delivered_at.desc(),
            PMReminder.id.desc(),
        ).first()

    return q.order_by(PMReminder.id.desc()).first()


def acknowledge_pm_reminder(
    actor_wa: str,
    reminder_id: Optional[int] = None,
) -> dict:
    now_utc = dt.datetime.utcnow()
    with SessionLocal() as s:
        reminder = _pm_reminder_for_actor(
            s,
            actor_wa,
            reminder_id=reminder_id,
            delivered_only=True,
        )
        if not reminder:
            return {"status": "not_found"}

        reminder.acknowledged_at = now_utc
        s.commit()
        s.refresh(reminder)
        result = _as_pm_reminder_dict(reminder)

    log_audit(actor_wa, "reminder_acknowledge", "pm_reminder", result["id"])
    result["status_result"] = "acknowledged"
    return result


def snooze_pm_reminder(
    actor_wa: str,
    until_utc: dt.datetime,
    reminder_id: Optional[int] = None,
) -> dict:
    until_utc = _normalize_pm_reminder_utc(until_utc)
    if until_utc is None or until_utc <= dt.datetime.utcnow():
        return {"status": "error", "code": "invalid_snooze_time"}

    with SessionLocal() as s:
        reminder = _pm_reminder_for_actor(
            s,
            actor_wa,
            reminder_id=reminder_id,
            active_only=True,
        )
        if not reminder:
            # A successfully delivered one-time reminder is completed
            # automatically, but may still be reopened by Snooze/Postpone.
            reminder = _pm_reminder_for_actor(
                s,
                actor_wa,
                reminder_id=reminder_id,
                delivered_only=True,
            )
        if not reminder:
            return {"status": "not_found"}

        reminder.next_run = until_utc
        reminder.retry_after = None
        reminder.snoozed_at = dt.datetime.utcnow()
        reminder.acknowledged_at = None
        reminder.claimed_at = None
        reminder.claim_token = None
        reminder.status = "active"
        reminder.active = True
        reminder.completed_at = None
        reminder.cancelled_at = None
        s.commit()
        s.refresh(reminder)
        result = _as_pm_reminder_dict(reminder)

    log_audit(
        actor_wa,
        "reminder_snooze",
        "pm_reminder",
        result["id"],
        details=f"next_run={result['next_run']}",
    )
    result["status_result"] = "snoozed"
    return result


def redirect_pm_reminder(
    actor_wa: str,
    recipient_wa: str,
    reminder_id: Optional[int] = None,
) -> dict:
    recipient_wa = str(recipient_wa or "").strip()
    if not recipient_wa:
        return {"status": "error", "code": "recipient_required"}

    with SessionLocal() as s:
        reminder = _pm_reminder_for_actor(
            s,
            actor_wa,
            reminder_id=reminder_id,
            active_only=True,
        )
        reopened_completed_one_time = False
        if not reminder:
            # A successfully delivered one-time reminder is completed
            # automatically, but may still be reopened by Redirect/Reassign.
            reminder = _pm_reminder_for_actor(
                s,
                actor_wa,
                reminder_id=reminder_id,
                delivered_only=True,
            )
            reopened_completed_one_time = bool(
                reminder
                and reminder.status == "completed"
                and not reminder.active
                and not _pm_reminder_is_recurring(reminder)
            )
        if not reminder:
            return {"status": "not_found"}

        prior_recipient = reminder.recipient_wa or reminder.pm_wa
        reminder.recipient_wa = recipient_wa
        reminder.redirected_at = dt.datetime.utcnow()

        if reopened_completed_one_time:
            reminder.next_run = dt.datetime.utcnow()
            reminder.status = "active"
            reminder.active = True
            reminder.completed_at = None
            reminder.claimed_at = None
            reminder.claim_token = None
            reminder.retry_after = None
            reminder.failed_at = None
            reminder.failure_count = 0
            reminder.last_error = None

        s.commit()
        s.refresh(reminder)
        result = _as_pm_reminder_dict(reminder)

    log_audit(
        actor_wa,
        "reminder_redirect",
        "pm_reminder",
        result["id"],
        details=f"from={prior_recipient};to={recipient_wa}",
    )
    result["status_result"] = "redirected"
    return result


def cancel_pm_reminder(
    actor_wa: str,
    reminder_id: Optional[int] = None,
) -> dict:
    now_utc = dt.datetime.utcnow()
    with SessionLocal() as s:
        reminder = _pm_reminder_for_actor(
            s,
            actor_wa,
            reminder_id=reminder_id,
            active_only=True,
        )
        if not reminder:
            return {"status": "not_found"}

        reminder.status = "cancelled"
        reminder.active = False
        reminder.cancelled_at = now_utc
        reminder.claimed_at = None
        reminder.claim_token = None
        reminder.retry_after = None
        s.commit()
        s.refresh(reminder)
        result = _as_pm_reminder_dict(reminder)

    log_audit(actor_wa, "reminder_cancel", "pm_reminder", result["id"])
    result["status_result"] = "cancelled"
    return result

# >>> FEATURE_3_REMINDER_STORAGE_END <<<

class Task(Base):
    __tablename__ = "tasks"

    client_id = Column(Integer, default=DEFAULT_CLIENT_ID, index=True)
    id = Column(Integer, primary_key=True)
    sender = Column(String(64), index=True)
    text = Column(Text)
    tag = Column(String(32), index=True)
    ts = Column(DateTime, default=dt.datetime.utcnow, index=True)

    status = Column(String(24), default="open", index=True)
    due_date = Column(DateTime)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    approved_at = Column(DateTime)
    rejected_at = Column(DateTime)

    is_rework = Column(Boolean, default=False)
    overrun_days = Column(Float, default=0.0)

    subcontractor_name = Column(String(128))
    project_code = Column(String(128), index=True)

    pm_wa_id = Column(String(64), nullable=True, index=True)

    attachment_url = Column(Text)
    attachment_mime = Column(String(128))
    attachment_name = Column(String(256))

    order_state = Column(String(32))
    subtype = Column(String(24))

    # === NEW FIELDS (CHANGE-ORDER STRUCTURE) ===
    cost = Column(Float, nullable=True)
    time_impact_days = Column(Float, nullable=True)
    approval_required = Column(Boolean, default=False)

    last_updated = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow)

# >>> PATCH_10_STORAGE_START — TASK GROUPING <<<

class TaskGroup(Base):
    __tablename__ = "task_groups"

    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, index=True)       # FK-like reference → Task.id
    child_id = Column(Integer, index=True)        # FK-like reference → Task.id
    created_at = Column(DateTime, default=dt.datetime.utcnow)

def add_task_to_group(parent_id: int, child_id: int, actor: Optional[str] = None) -> dict:
    with SessionLocal() as s:
        g = TaskGroup(parent_id=parent_id, child_id=child_id)
        s.add(g)
        s.commit()
        s.refresh(g)
        log_audit(actor, "task_group_add", "task_group", g.id,
                  details=f"parent={parent_id}, child={child_id}")
        return {"status": "ok", "group_id": g.id}

def get_group_children(parent_id: int) -> list[int]:
    with SessionLocal() as s:
        rows = s.query(TaskGroup).filter(TaskGroup.parent_id == parent_id).all()
        return [r.child_id for r in rows]

# >>> PATCH_10_STORAGE_END <<<

class Meeting(Base):
    __tablename__ = "meetings"

    client_id = Column(Integer, default=DEFAULT_CLIENT_ID, index=True)
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    project_code = Column(String(128), index=True)
    subcontractor_name = Column(String(128))
    site_name = Column(String(200))
    scheduled_for = Column(DateTime, index=True)
    started_at = Column(DateTime)
    closed_at = Column(DateTime)
    created_by = Column(String(64))
    status = Column(String(24), default="scheduled", index=True)
    task_ids = Column(Text)  # comma-separated ids

# >>> PATCH_1_STORAGE_START — CALL LOG MODEL <<<

class CallLog(Base):
    __tablename__ = "call_logs"

    client_id = Column(Integer, default=DEFAULT_CLIENT_ID, index=True)
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=dt.datetime.utcnow, index=True)

    direction = Column(String(16))         # inbound | outbound
    from_wa = Column(String(64), index=True)
    to_wa = Column(String(64), index=True)

    duration_seconds = Column(Integer, nullable=True)
    notes = Column(Text, nullable=True)

    created_at = Column(DateTime, default=dt.datetime.utcnow)

# Helper: record call metadata
def log_call(direction: str,
             from_wa: str,
             to_wa: str,
             duration_seconds: Optional[int],
             notes: Optional[str]) -> dict:
    with SessionLocal() as s:
        c = CallLog(
            direction=direction,
            from_wa=from_wa,
            to_wa=to_wa,
            duration_seconds=duration_seconds,
            notes=notes,
        )
        s.add(c)
        s.commit()
        s.refresh(c)
        return {
            "id": c.id,
            "ts": c.ts.isoformat() if c.ts else None,
            "direction": c.direction,
            "from": c.from_wa,
            "to": c.to_wa,
            "duration_seconds": c.duration_seconds,
            "notes": c.notes,
        }

# >>> PATCH_1_STORAGE_END <<<

# --- PM ↔ Project Assignment ----------------------------------------
class PMProjectMap(Base):
    __tablename__ = "pm_project_map"

    client_id = Column(Integer, default=DEFAULT_CLIENT_ID, index=True)
    id = Column(Integer, primary_key=True)
    pm_user_id = Column(Integer, index=True)      # FK → User.id (not enforced here)
    project_code = Column(String(128), index=True)
    primary_pm = Column(Boolean, default=True)

    created_at = Column(DateTime, default=dt.datetime.utcnow)
    updated_at = Column(DateTime, default=dt.datetime.utcnow,
                        onupdate=dt.datetime.utcnow)

class Audit(Base):
    __tablename__ = "audits"

    client_id = Column(Integer, default=DEFAULT_CLIENT_ID, index=True)
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=dt.datetime.utcnow, index=True)
    actor = Column(String(64))
    action = Column(String(64))
    ref_type = Column(String(32))
    ref_id = Column(Integer)
    details = Column(Text)

# ---------------------------------------------------------------------
# System integrity model (heartbeat state)
# ---------------------------------------------------------------------
class SystemState(Base):
    __tablename__ = "system_state"

    id = Column(Integer, primary_key=True)
    hygiene_last_utc = Column(String(40), nullable=True)
    redmode = Column(Boolean, default=False)
    redmode_reason = Column(String(200), nullable=True)


# >>> MU12_CONVERSATION_STATE_STORAGE_START <<<

class ConversationState(Base):
    """Persistent industry-neutral conversation continuation state."""

    __tablename__ = "conversation_states"
    __table_args__ = (
        UniqueConstraint(
            "client_id",
            "sender",
            "continuation_key",
            name="uq_conversation_state_continuation",
        ),
    )

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, nullable=False, index=True)
    sender = Column(String(64), nullable=False, index=True)
    project_code = Column(String(128), nullable=True, index=True)
    state_kind = Column(String(32), nullable=False, index=True)
    expected_field = Column(String(128), nullable=True)
    original_request = Column(Text, nullable=False, default="")
    structured_context_json = Column("structured_context", Text, nullable=True)
    candidate_metadata_json = Column("candidate_metadata", Text, nullable=True)
    continuation_json = Column("continuation", Text, nullable=True)
    continuation_key = Column(String(256), nullable=False, index=True)
    status = Column(String(24), nullable=False, default="active", index=True)
    active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow)
    last_activity_at = Column(DateTime, default=dt.datetime.utcnow, index=True)
    updated_at = Column(
        DateTime,
        default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow,
    )
    resolved_at = Column(DateTime, nullable=True)
    retired_at = Column(DateTime, nullable=True)
    retirement_reason = Column(String(32), nullable=True)


CONVERSATION_STATE_EXPIRY = dt.timedelta(hours=24)


def _conversation_state_json(value) -> str:
    return json.dumps(
        value if value is not None else {},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _conversation_state_value(value: Optional[str]):
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed


def _as_conversation_state_dict(state: ConversationState) -> dict:
    return {
        "id": state.id,
        "client_id": state.client_id,
        "sender": state.sender,
        "project_code": state.project_code,
        "state_kind": state.state_kind,
        "expected_field": state.expected_field,
        "original_request": state.original_request or "",
        "structured_context": _conversation_state_value(
            state.structured_context_json
        ),
        "candidate_metadata": _conversation_state_value(
            state.candidate_metadata_json
        ),
        "continuation": _conversation_state_value(state.continuation_json),
        "continuation_key": state.continuation_key,
        "status": state.status,
        "active": bool(state.active),
        "created_at": state.created_at,
        "last_activity_at": state.last_activity_at or state.created_at,
        "updated_at": state.updated_at,
        "resolved_at": state.resolved_at,
        "retired_at": state.retired_at,
        "retirement_reason": state.retirement_reason,
    }


def save_pending_conversation_state(payload: dict) -> dict:
    """Create or idempotently refresh one generic active continuation."""
    payload = payload or {}
    sender = str(payload.get("sender") or "").strip()
    state_kind = str(payload.get("state_kind") or "").strip()
    continuation_key = str(payload.get("continuation_key") or "").strip()
    if not sender or not state_kind or not continuation_key:
        return {"status": "error", "code": "required_state_identity_missing"}

    try:
        client_id = int(payload.get("client_id") or current_client_id())
    except (TypeError, ValueError):
        return {"status": "error", "code": "invalid_client_id"}

    project_code = payload.get("project_code")
    if project_code is not None:
        project_code = str(project_code).strip() or None

    expected_field = payload.get("expected_field")
    if expected_field is not None:
        expected_field = str(expected_field).strip() or None

    original_request = payload.get("original_request")
    if original_request is None:
        original_request = ""
    elif not isinstance(original_request, str):
        original_request = str(original_request)

    structured_context_supplied = "structured_context" in payload
    candidate_metadata_supplied = "candidate_metadata" in payload
    continuation_supplied = "continuation" in payload

    structured_context_json = _conversation_state_json(
        payload.get("structured_context")
    )
    candidate_metadata_json = _conversation_state_json(
        payload.get("candidate_metadata")
    )
    continuation_json = _conversation_state_json(payload.get("continuation"))

    with SessionLocal() as s:
        state = (
            s.query(ConversationState)
            .filter(
                ConversationState.client_id == client_id,
                ConversationState.sender == sender,
                ConversationState.continuation_key == continuation_key,
            )
            .first()
        )

        if state is None:
            state = ConversationState(
                client_id=client_id,
                sender=sender,
                project_code=project_code,
                state_kind=state_kind,
                expected_field=expected_field,
                original_request=original_request,
                structured_context_json=structured_context_json,
                candidate_metadata_json=candidate_metadata_json,
                continuation_json=continuation_json,
                continuation_key=continuation_key,
                status="active",
                active=True,
                last_activity_at=dt.datetime.utcnow(),
            )
            s.add(state)
            s.commit()
            s.refresh(state)
            result = _as_conversation_state_dict(state)
            result["status_result"] = "created"
            return result

        if not state.active or state.status != "active":
            result = _as_conversation_state_dict(state)
            result["status_result"] = "inactive"
            return result

        if state.project_code != project_code or state.state_kind != state_kind:
            result = _as_conversation_state_dict(state)
            result["status_result"] = "scope_conflict"
            return result

        changes = {"expected_field": expected_field}
        if structured_context_supplied:
            changes["structured_context_json"] = structured_context_json
        if candidate_metadata_supplied:
            changes["candidate_metadata_json"] = candidate_metadata_json
        if continuation_supplied:
            changes["continuation_json"] = continuation_json
        if not state.original_request and original_request:
            changes["original_request"] = original_request

        changed = False
        for field_name, value in changes.items():
            if getattr(state, field_name) != value:
                setattr(state, field_name, value)
                changed = True

        if changed:
            state.updated_at = dt.datetime.utcnow()
            s.commit()
            s.refresh(state)

        result = _as_conversation_state_dict(state)
        result["status_result"] = "updated" if changed else "unchanged"
        return result


def get_pending_conversation_state(
    sender: str,
    client_id: int,
    project_code: Optional[str],
    continuation_key: Optional[str] = None,
    now_utc: Optional[dt.datetime] = None,
) -> Optional[dict]:
    """Retrieve one unexpired state inside exact sender/client/project scope."""
    sender = str(sender or "").strip()
    if not sender:
        return None
    try:
        client_id = int(client_id)
    except (TypeError, ValueError):
        return None

    normalized_project = (
        str(project_code).strip() if project_code is not None else None
    )
    if normalized_project == "":
        normalized_project = None

    with SessionLocal() as s:
        q = s.query(ConversationState).filter(
            ConversationState.client_id == client_id,
            ConversationState.sender == sender,
            ConversationState.active == True,
            ConversationState.status.in_(("active", "continuing")),
        )
        if normalized_project is None:
            q = q.filter(ConversationState.project_code == None)
        else:
            q = q.filter(ConversationState.project_code == normalized_project)
        if continuation_key:
            q = q.filter(
                ConversationState.continuation_key == str(continuation_key)
            )

        state = q.order_by(ConversationState.id.desc()).first()
        if state is None:
            return None

        lifecycle_now = now_utc or dt.datetime.utcnow()
        last_activity = state.last_activity_at or state.created_at
        if last_activity is None:
            last_activity = state.updated_at or lifecycle_now
        if lifecycle_now >= last_activity + CONVERSATION_STATE_EXPIRY:
            updated = (
                s.query(ConversationState)
                .filter(
                    ConversationState.id == state.id,
                    ConversationState.active == True,
                    ConversationState.status.in_(("active", "continuing")),
                )
                .update(
                    {
                        ConversationState.status: "expired",
                        ConversationState.active: False,
                        ConversationState.retired_at: lifecycle_now,
                        ConversationState.retirement_reason: "expired",
                        ConversationState.updated_at: lifecycle_now,
                    },
                    synchronize_session=False,
                )
            )
            if updated:
                s.commit()
            else:
                s.rollback()
            return None
        return _as_conversation_state_dict(state)


def claim_conversation_state_continuation(
    state_id: int,
    sender: str,
    client_id: int,
    project_code: Optional[str],
    now_utc: Optional[dt.datetime] = None,
) -> dict:
    """Atomically claim one active continuation before orchestration resumes it."""
    try:
        state_id = int(state_id)
        client_id = int(client_id)
    except (TypeError, ValueError):
        return {"status": "error", "code": "invalid_state_identity"}

    sender = str(sender or "").strip()
    normalized_project = (
        str(project_code).strip() if project_code is not None else None
    )
    if normalized_project == "":
        normalized_project = None

    with SessionLocal() as s:
        q = s.query(ConversationState).filter(
            ConversationState.id == state_id,
            ConversationState.client_id == client_id,
            ConversationState.sender == sender,
            ConversationState.active == True,
            ConversationState.status == "active",
        )
        if normalized_project is None:
            q = q.filter(ConversationState.project_code == None)
        else:
            q = q.filter(ConversationState.project_code == normalized_project)

        now_utc = now_utc or dt.datetime.utcnow()
        expiry_cutoff = now_utc - CONVERSATION_STATE_EXPIRY
        q = q.filter(ConversationState.last_activity_at > expiry_cutoff)
        updated = q.update(
            {
                ConversationState.status: "continuing",
                ConversationState.last_activity_at: now_utc,
                ConversationState.updated_at: now_utc,
            },
            synchronize_session=False,
        )
        if updated != 1:
            s.rollback()
            return {"status": "not_found"}

        s.commit()
        state = s.get(ConversationState, state_id)
        result = _as_conversation_state_dict(state)
        result["status_result"] = "claimed"
        return result


def advance_conversation_state_continuation(
    state_id: int,
    sender: str,
    client_id: int,
    project_code: Optional[str],
    expected_field: Optional[str],
    now_utc: Optional[dt.datetime] = None,
    structured_context: Optional[dict] = None,
) -> dict:
    """Return a claimed continuation to active state for its next field."""
    try:
        state_id = int(state_id)
        client_id = int(client_id)
    except (TypeError, ValueError):
        return {"status": "error", "code": "invalid_state_identity"}

    sender = str(sender or "").strip()
    normalized_project = (
        str(project_code).strip() if project_code is not None else None
    )
    if normalized_project == "":
        normalized_project = None

    if expected_field is not None:
        expected_field = str(expected_field).strip() or None

    with SessionLocal() as s:
        q = s.query(ConversationState).filter(
            ConversationState.id == state_id,
            ConversationState.client_id == client_id,
            ConversationState.sender == sender,
            ConversationState.active == True,
            ConversationState.status == "continuing",
        )
        if normalized_project is None:
            q = q.filter(ConversationState.project_code == None)
        else:
            q = q.filter(ConversationState.project_code == normalized_project)

        now_utc = now_utc or dt.datetime.utcnow()
        updates = {
            ConversationState.status: "active",
            ConversationState.expected_field: expected_field,
            ConversationState.updated_at: now_utc,
        }
        if structured_context is not None:
            updates[ConversationState.structured_context_json] = (
                _conversation_state_json(structured_context)
            )
        updated = q.update(
            updates,
            synchronize_session=False,
        )
        if updated != 1:
            s.rollback()
            return {"status": "not_found"}

        s.commit()
        state = s.get(ConversationState, state_id)
        result = _as_conversation_state_dict(state)
        result["status_result"] = "advanced"
        return result


def resolve_conversation_state(
    state_id: int,
    sender: str,
    client_id: int,
    project_code: Optional[str],
    now_utc: Optional[dt.datetime] = None,
) -> dict:
    """Resolve one active state once, within exact identity/scope bounds."""
    try:
        state_id = int(state_id)
        client_id = int(client_id)
    except (TypeError, ValueError):
        return {"status": "error", "code": "invalid_state_identity"}

    sender = str(sender or "").strip()
    normalized_project = (
        str(project_code).strip() if project_code is not None else None
    )
    if normalized_project == "":
        normalized_project = None

    with SessionLocal() as s:
        q = s.query(ConversationState).filter(
            ConversationState.id == state_id,
            ConversationState.client_id == client_id,
            ConversationState.sender == sender,
            ConversationState.active == True,
            ConversationState.status.in_(("active", "continuing")),
        )
        if normalized_project is None:
            q = q.filter(ConversationState.project_code == None)
        else:
            q = q.filter(ConversationState.project_code == normalized_project)

        now_utc = now_utc or dt.datetime.utcnow()
        updated = q.update(
            {
                ConversationState.status: "resolved",
                ConversationState.active: False,
                ConversationState.resolved_at: now_utc,
                ConversationState.updated_at: now_utc,
            },
            synchronize_session=False,
        )
        if updated != 1:
            s.rollback()
            return {"status": "not_found"}
        s.commit()
        state = s.get(ConversationState, state_id)
        result = _as_conversation_state_dict(state)
        result["status_result"] = "resolved"
        return result


def touch_conversation_state_activity(
    state_id: int,
    sender: str,
    client_id: int,
    project_code: Optional[str],
    now_utc: Optional[dt.datetime] = None,
) -> dict:
    """Record an inbound actually processed as a continuation attempt."""
    try:
        state_id = int(state_id)
        client_id = int(client_id)
    except (TypeError, ValueError):
        return {"status": "error", "code": "invalid_state_identity"}
    sender = str(sender or "").strip()
    normalized_project = (
        str(project_code).strip() if project_code is not None else None
    )
    if normalized_project == "":
        normalized_project = None
    lifecycle_now = now_utc or dt.datetime.utcnow()
    expiry_cutoff = lifecycle_now - CONVERSATION_STATE_EXPIRY
    with SessionLocal() as s:
        q = s.query(ConversationState).filter(
            ConversationState.id == state_id,
            ConversationState.client_id == client_id,
            ConversationState.sender == sender,
            ConversationState.active == True,
            ConversationState.status == "active",
            ConversationState.last_activity_at > expiry_cutoff,
        )
        if normalized_project is None:
            q = q.filter(ConversationState.project_code == None)
        else:
            q = q.filter(ConversationState.project_code == normalized_project)
        updated = q.update(
            {
                ConversationState.last_activity_at: lifecycle_now,
                ConversationState.updated_at: lifecycle_now,
            },
            synchronize_session=False,
        )
        if updated != 1:
            s.rollback()
            return {"status": "not_found"}
        s.commit()
        state = s.get(ConversationState, state_id)
        result = _as_conversation_state_dict(state)
        result["status_result"] = "touched"
        return result


def retire_conversation_state(
    state_id: int,
    sender: str,
    client_id: int,
    project_code: Optional[str],
    reason: str,
    now_utc: Optional[dt.datetime] = None,
) -> dict:
    """Atomically retire active conversation state without business mutation."""
    allowed_reasons = {"cancelled", "restarted", "abandoned", "expired"}
    reason = str(reason or "").strip().lower()
    if reason not in allowed_reasons:
        return {"status": "error", "code": "invalid_retirement_reason"}
    try:
        state_id = int(state_id)
        client_id = int(client_id)
    except (TypeError, ValueError):
        return {"status": "error", "code": "invalid_state_identity"}
    sender = str(sender or "").strip()
    normalized_project = (
        str(project_code).strip() if project_code is not None else None
    )
    if normalized_project == "":
        normalized_project = None
    lifecycle_now = now_utc or dt.datetime.utcnow()
    with SessionLocal() as s:
        q = s.query(ConversationState).filter(
            ConversationState.id == state_id,
            ConversationState.client_id == client_id,
            ConversationState.sender == sender,
            ConversationState.active == True,
            ConversationState.status.in_(("active", "continuing")),
        )
        if normalized_project is None:
            q = q.filter(ConversationState.project_code == None)
        else:
            q = q.filter(ConversationState.project_code == normalized_project)
        updated = q.update(
            {
                ConversationState.status: reason,
                ConversationState.active: False,
                ConversationState.retired_at: lifecycle_now,
                ConversationState.retirement_reason: reason,
                ConversationState.updated_at: lifecycle_now,
            },
            synchronize_session=False,
        )
        if updated != 1:
            s.rollback()
            return {"status": "not_found"}
        s.commit()
        state = s.get(ConversationState, state_id)
        result = _as_conversation_state_dict(state)
        result["status_result"] = "retired"
        return result

# >>> MU12_CONVERSATION_STATE_STORAGE_END <<<

# --- HOTFIX: ensure system_state table matches model ---
from sqlalchemy import inspect, text
def _repair_system_state():
    insp = inspect(ENGINE)
    cols = [c['name'] for c in insp.get_columns("system_state")]
    if "client_id" in cols:
        with ENGINE.connect() as conn:
            conn.execute(text("ALTER TABLE system_state DROP COLUMN client_id"))

# --- HOTFIX: ensure tasks table matches model ---
def _repair_tasks():
    insp = inspect(ENGINE)
    cols = [c['name'] for c in insp.get_columns("tasks")]
    if "client_id" in cols:
        with ENGINE.connect() as conn:
            conn.execute(text("ALTER TABLE tasks DROP COLUMN client_id"))

def _repair_pm_project_map():
    """Ensure existing pm_project_map tables contain client_id + expected index."""
    insp = inspect(ENGINE)

    if "pm_project_map" not in insp.get_table_names():
        return

    existing_columns = {
        column["name"]
        for column in insp.get_columns("pm_project_map")
    }
    existing_indexes = {
        index.get("name")
        for index in insp.get_indexes("pm_project_map")
    }

    with ENGINE.begin() as conn:
        if "client_id" not in existing_columns:
            conn.execute(text(
                "ALTER TABLE pm_project_map "
                "ADD COLUMN client_id INTEGER DEFAULT 1"
            ))

        conn.execute(text(
            "UPDATE pm_project_map "
            "SET client_id = 1 "
            "WHERE client_id IS NULL"
        ))

        if "ix_pm_project_map_client_id" not in existing_indexes:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_pm_project_map_client_id "
                "ON pm_project_map (client_id)"
            ))


def _repair_users_datetime_configuration():
    """Add MU15 sender date/time configuration without breaking US defaults."""
    insp = inspect(ENGINE)
    if "users" not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns("users")}
    additions = {
        "date_order": "VARCHAR(16)",
        "time_format": "VARCHAR(8)",
        "date_display": "VARCHAR(16)",
    }
    with ENGINE.begin() as conn:
        for column_name, column_type in additions.items():
            if column_name not in existing:
                conn.execute(text(
                    f"ALTER TABLE users ADD COLUMN {column_name} {column_type}"
                ))
        conn.execute(text(
            "UPDATE users SET "
            "date_order = COALESCE(NULLIF(date_order, ''), 'month_first'), "
            "time_format = COALESCE(NULLIF(time_format, ''), '12h'), "
            "date_display = COALESCE(NULLIF(date_display, ''), 'month_first')"
        ))

# >>> FEATURE_3_REMINDER_SCHEMA_REPAIR_START — BACKWARD COMPATIBILITY V6.1 <<<

def _repair_pm_reminders():
    """Add lifecycle columns when an earlier reminder table already exists."""
    insp = inspect(ENGINE)
    if "pm_reminders" not in insp.get_table_names():
        return

    existing = {c["name"] for c in insp.get_columns("pm_reminders")}
    additions = {
        "recipient_wa": "VARCHAR(64)",
        "project_code": "VARCHAR(128)",
        "text": "TEXT",
        "rule": "VARCHAR(128)",
        "timezone": "VARCHAR(64)",
        "next_run": "TIMESTAMP",
        "recurring": "BOOLEAN",
        "recurrence_rule": "VARCHAR(32)",
        "recurrence_interval": "INTEGER",
        "recurrence_seconds": "INTEGER",
        "recurrence_anchor_day": "INTEGER",
        "status": "VARCHAR(24)",
        "active": "BOOLEAN",
        "claimed_at": "TIMESTAMP",
        "claim_token": "VARCHAR(64)",
        "retry_after": "TIMESTAMP",
        "delivered_at": "TIMESTAMP",
        "acknowledged_at": "TIMESTAMP",
        "snoozed_at": "TIMESTAMP",
        "redirected_at": "TIMESTAMP",
        "cancelled_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "failed_at": "TIMESTAMP",
        "delivery_count": "INTEGER",
        "failure_count": "INTEGER",
        "last_error": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    }

    true_literal = "TRUE" if ENGINE.dialect.name == "postgresql" else "1"
    false_literal = "FALSE" if ENGINE.dialect.name == "postgresql" else "0"

    with ENGINE.begin() as conn:
        for column_name, column_type in additions.items():
            if column_name not in existing:
                conn.execute(text(
                    f"ALTER TABLE pm_reminders "
                    f"ADD COLUMN {column_name} {column_type}"
                ))

        conn.execute(text(
            "UPDATE pm_reminders "
            "SET recipient_wa = COALESCE(recipient_wa, pm_wa), "
            "text = COALESCE(text, rule, ''), "
            "timezone = COALESCE(NULLIF(timezone, ''), "
            "(SELECT timezone FROM users "
            "WHERE users.wa_id = pm_reminders.pm_wa LIMIT 1), "
            "'America/New_York'), "
            "recurring = COALESCE(recurring, " + false_literal + "), "
            "recurrence_rule = COALESCE(recurrence_rule, 'none'), "
            "recurrence_interval = COALESCE(recurrence_interval, 1), "
            "status = COALESCE(status, 'active'), "
            "active = COALESCE(active, " + true_literal + "), "
            "delivery_count = COALESCE(delivery_count, 0), "
            "failure_count = COALESCE(failure_count, 0), "
            "created_at = COALESCE(created_at, CURRENT_TIMESTAMP), "
            "updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)"
        ))

# >>> FEATURE_3_REMINDER_SCHEMA_REPAIR_END <<<


def _repair_conversation_states():
    """Add MU14 lifecycle columns to an existing conversation-state table."""
    insp = inspect(ENGINE)
    if "conversation_states" not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns("conversation_states")}
    additions = {
        "last_activity_at": "TIMESTAMP",
        "retired_at": "TIMESTAMP",
        "retirement_reason": "VARCHAR(32)",
    }
    with ENGINE.begin() as conn:
        for column_name, column_type in additions.items():
            if column_name not in existing:
                conn.execute(text(
                    f"ALTER TABLE conversation_states "
                    f"ADD COLUMN {column_name} {column_type}"
                ))
        conn.execute(text(
            "UPDATE conversation_states SET last_activity_at = "
            "COALESCE(last_activity_at, updated_at, created_at, CURRENT_TIMESTAMP)"
        ))


# ---------------------------------------------------------------------
# Hygiene helpers (used by /heartbeat and tether checks)
# ---------------------------------------------------------------------
def hygiene_pin():
    """Record current UTC timestamp for heartbeat tether."""
    with SessionLocal() as s:
        ss = s.query(SystemState).first()
        if not ss:
            ss = SystemState()
            s.add(ss)
        ss.hygiene_last_utc = dt.datetime.utcnow().isoformat() + "Z"
        s.commit()

def hygiene_guard(threshold_seconds=120) -> tuple[bool, str]:
    """Return (ok, note) based on how stale the last heartbeat is."""
    with SessionLocal() as s:
        ss = s.query(SystemState).first()
        if not ss or not ss.hygiene_last_utc:
            return False, "no-hygiene-record"
        try:
            last = dt.datetime.fromisoformat(ss.hygiene_last_utc.replace("Z",""))
        except Exception:
            return False, "bad-hygiene-timestamp"
        gap = (dt.datetime.utcnow() - last).total_seconds()
        return (gap <= threshold_seconds), f"gap={int(gap)}s"

def init_db():
    Base.metadata.create_all(ENGINE)

    # ---- DB REPAIR PATCHES ----
    try:
        _repair_system_state()
    except Exception:
        pass

    try:
        _repair_tasks()
    except Exception:
        pass

    try:
        _repair_pm_project_map()
    except Exception:
        pass

    try:
        _repair_users_datetime_configuration()
    except Exception:
        pass

    # >>> FEATURE_3_REMINDER_INIT_START — SCHEMA REPAIR V6.1 <<<
    try:
        _repair_pm_reminders()
    except Exception:
        pass
    # >>> FEATURE_3_REMINDER_INIT_END <<<

    try:
        _repair_conversation_states()
    except Exception:
        pass

    return True

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _ts(x: Optional[dt.datetime]) -> Optional[str]:
    return x.strftime("%Y-%m-%d %H:%M:%S") if x else None

def _as_task_dict(t: Task) -> dict:
    return {
        "id": t.id,
        "sender": t.sender,
        "text": t.text,
        "tag": t.tag,
        "ts": _ts(t.ts),
        "status": t.status,
        "due_date": _ts(t.due_date),
        "started_at": _ts(t.started_at),
        "completed_at": _ts(t.completed_at),
        "approved_at": _ts(t.approved_at),
        "rejected_at": _ts(t.rejected_at),
        "is_rework": t.is_rework,
        "overrun_days": t.overrun_days,
        "subcontractor_name": t.subcontractor_name,
        "project_code": t.project_code,
        "order_state": t.order_state,
        "subtype": t.subtype,
        "cost": t.cost,
        "time_impact_days": t.time_impact_days,
        "approval_required": t.approval_required,
        "attachment": {
            "name": t.attachment_name,
            "mime": t.attachment_mime,
            "url": t.attachment_url,
        } if t.attachment_url else None,
        "last_updated": _ts(t.last_updated),
    }

def _as_meeting_dict(m: Meeting) -> dict:
    return {
        "id": m.id,
        "title": m.title,
        "project_code": m.project_code,
        "subcontractor_name": m.subcontractor_name,
        "site_name": m.site_name,
        "scheduled_for": _ts(m.scheduled_for),
        "started_at": _ts(m.started_at),
        "closed_at": _ts(m.closed_at),
        "created_by": m.created_by,
        "status": m.status,
        "task_ids": m.task_ids or "",
    }

def log_audit(actor: Optional[str], action: str, ref_type: str, ref_id: int, details: Optional[str] = None):
    with SessionLocal() as s:
        s.add(Audit(actor=actor, action=action, ref_type=ref_type, ref_id=ref_id, details=details))
        s.commit()

# ---------------------------------------------------------------------
# Lookup Helpers (People / Hierarchy)
# ---------------------------------------------------------------------
def get_user_role(wa_id: str) -> Optional[dict]:
    with SessionLocal() as s:
        u = s.query(User).filter(User.wa_id == wa_id).first()
        if not u:
            return None
        return {
            "wa_id": u.wa_id,
            "client_id": u.client_id,
            "name": u.name,
            "role": u.role,
            "subcontractor_name": u.subcontractor_name,
            "project_code": u.project_code,
            "phone": u.phone,
            "active": u.active,
        }

def get_pms_for_project(project_code: str) -> list[dict]:
    if not project_code:
        return []
    with SessionLocal() as s:
        rows = (
            s.query(PMProjectMap, User)
            .join(User, PMProjectMap.pm_user_id == User.id)
            .filter(PMProjectMap.project_code == project_code, User.active == True)
            .order_by(PMProjectMap.primary_pm.desc(), User.name.asc())
            .all()
        )
        result = []
        for m, u in rows:
            result.append({
                "wa_id": u.wa_id,
                "name": u.name,
                "role": u.role,
                "primary": m.primary_pm
            })
        return result

# ---------------------------------------------------------------------
# Core CRUD
# ---------------------------------------------------------------------
# >>> PATCH_4_STORAGE_QUERY_FILTERS_START — CLIENT FILTER <<<
def _apply_client_filter(q):
    return q.filter_by(client_id=current_client_id())
# >>> PATCH_4_STORAGE_QUERY_FILTERS_END <<<

# >>> PATCH_3_STORAGE_START — INLINE TASK EDIT (AUDIT SAFE) <<<

def edit_task_text(task_id: int,
                   new_text: str,
                   actor: Optional[str] = None) -> dict:
    """
    Inline PM-safe text edit.
    Preserves old→new pairs via Audit table.
    """
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return {"error": "task not found"}

        old_text = t.text or ""
        t.text = new_text or ""
        t.last_updated = dt.datetime.utcnow()
        s.commit(); s.refresh(t)

        details = f"old='{old_text}' → new='{new_text}'"
        log_audit(actor, "task_edit_text", "task", t.id, details=details)

        return _as_task_dict(t)

# >>> PATCH_3_STORAGE_END <<<

def create_task(sender: str, text: str, tag: Optional[str] = None,
                attachment: Optional[dict] = None,
                subcontractor_name: Optional[str] = None,
                project_code: Optional[str] = None,
                due_date: Optional[dt.datetime] = None,
                order_state: Optional[str] = None,
                subtype: Optional[str] = None) -> dict:
    with SessionLocal() as s:
        t = Task(
            sender=sender, text=text or "", tag=tag,
            subcontractor_name=subcontractor_name, project_code=project_code,
            due_date=due_date, order_state=order_state, subtype=subtype
        )
        if attachment:
            t.attachment_name = attachment.get("name")
            t.attachment_mime = attachment.get("mime")
            t.attachment_url  = attachment.get("url")
        s.add(t)
        s.commit(); s.refresh(t)
        log_audit(sender, "create", "task", t.id, details=text or "")
        return _as_task_dict(t)

def get_tasks(limit: int = 200, client_id: Optional[str] = None):
    with SessionLocal() as s:
        # Apply client isolation FIRST
        qry = _apply_client_filter(s.query(Task)).order_by(Task.id.desc())

        rows = qry.limit(limit).all()
        out = []
        for r in rows:
            out.append({
                "id": r.id,
                "ts": r.ts.isoformat() if r.ts else None,
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
                "attachment_name": r.attachment_name,
                "attachment_mime": r.attachment_mime,
                "attachment_url": r.attachment_url,
                "last_updated": r.last_updated,
            })
        return out

def get_summary():
    with SessionLocal() as s:
        qry = _apply_client_filter(s.query(Task)).order_by(Task.id.desc())

        rows = qry.limit(200).all()

        out = []
        for r in rows:
            out.append({
                "id": r.id,
                "ts": r.ts.isoformat() if r.ts else None,
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
                "approved_at": r.approved_at.isoformat() if r.approved_at else None,
                "rejected_at": r.rejected_at.isoformat() if r.rejected_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "due_date": r.due_date.isoformat() if r.due_date else None,
                "overrun_days": r.overrun_days,
                "is_rework": r.is_rework,
                "attachment_url": r.attachment_url,
                "last_updated": r.last_updated.isoformat() if r.last_updated else None,
            })
        return out

def mark_done(task_id: int, actor: Optional[str] = None):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t: return None
        t.status = "done"
        t.completed_at = dt.datetime.utcnow()
        if t.due_date:
            delta = (t.completed_at.date() - t.due_date.date()).days
            t.overrun_days = float(max(0, delta))
        s.commit(); s.refresh(t)
        log_audit(actor, "mark_done", "task", t.id)
        return _as_task_dict(t)

def approve_task(task_id: int, actor: Optional[str] = None):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t: return None
        t.status = "approved"
        t.approved_at = dt.datetime.utcnow()
        s.commit(); s.refresh(t)
        log_audit(actor, "approve", "task", t.id)
        return _as_task_dict(t)

def reject_task(task_id: int, rework: bool = True, actor: Optional[str] = None):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t: return None
        t.status = "rejected"
        t.is_rework = bool(rework)
        t.rejected_at = dt.datetime.utcnow()
        s.commit(); s.refresh(t)
        log_audit(actor, "reject", "task", t.id, details=f"rework={rework}")
        return _as_task_dict(t)

def set_order_state(task_id: int, state: str, actor: Optional[str] = None):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t: return None
        t.order_state = state
        s.commit(); s.refresh(t)
        log_audit(actor, "order_state", "task", t.id, details=state)
        return _as_task_dict(t)

def revoke_last(task_id: int, actor: Optional[str] = None):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t: return None
        if t.status in ("approved", "rejected", "done"):
            t.status = "open"
            t.approved_at = None
            t.rejected_at = None
            t.completed_at = None
            s.commit(); s.refresh(t)
            log_audit(actor, "revoke", "task", t.id)
        return _as_task_dict(t)

# ---------------------------------------------------------------------
# Accuracy scoring
# ---------------------------------------------------------------------
def subcontractor_accuracy(subcontractor_name: str):
    with SessionLocal() as s:
        rows: Iterable[Task] = s.query(Task).filter(Task.subcontractor_name == subcontractor_name).all()
        total = len(rows)
        on_time = 0; overruns = 0; reworks = 0
        for t in rows:
            if t.status in ("approved", "done"):
                if (t.overrun_days or 0) > 0:
                    overruns += 1
                else:
                    on_time += 1
            if t.is_rework:
                reworks += 1
        pct = int(0 if total == 0 else round(100.0 * on_time / total))
        return {
            "subcontractor": subcontractor_name,
            "total": total,
            "on_time": on_time,
            "overruns": overruns,
            "reworks": reworks,
            "accuracy_pct": pct,
        }

# ---------------------------------------------------------------------
# Meetings (Phase-1)
# ---------------------------------------------------------------------
def create_meeting(title: str, project_code: Optional[str],
                   subcontractor_name: Optional[str],
                   site_name: Optional[str],
                   scheduled_for: Optional[dt.datetime],
                   task_ids: Optional[list[int]],
                   created_by: Optional[str]) -> dict:
    with SessionLocal() as s:
        m = Meeting(
            title=title or "Site Meeting",
            project_code=project_code,
            subcontractor_name=subcontractor_name,
            site_name=site_name,
            scheduled_for=scheduled_for,
            task_ids=",".join(str(i) for i in (task_ids or [])),
            created_by=created_by,
            status="scheduled"
        )
        s.add(m); s.commit(); s.refresh(m)
        log_audit(created_by, "meeting_create", "meeting", m.id, details=m.title)
        return _as_meeting_dict(m)

def start_meeting(meeting_id: int, actor: Optional[str] = None):
    with SessionLocal() as s:
        m = s.get(Meeting, meeting_id)
        if not m: return None
        m.status = "active"
        m.started_at = dt.datetime.utcnow()
        s.commit(); s.refresh(m)
        log_audit(actor, "meeting_start", "meeting", m.id)
        return _as_meeting_dict(m)

def close_meeting(meeting_id: int, actor: Optional[str] = None):
    with SessionLocal() as s:
        m = s.get(Meeting, meeting_id)
        if not m: return None
        m.status = "closed"
        m.closed_at = dt.datetime.utcnow()
        s.commit(); s.refresh(m)
        log_audit(actor, "meeting_close", "meeting", m.id)
        return _as_meeting_dict(m)

# ---------------------------------------------------------------------
# Change Orders & Stock (placeholders for V6 live test)
# ---------------------------------------------------------------------
def record_change_order(data: dict):
    with SessionLocal() as s:
        tid = data.get("task_id")
        cost = data.get("cost")
        time_impact = data.get("time_impact_days")
        approval = data.get("approval_required")

        t = s.get(Task, tid)
        if not t:
            return {"error": "task not found"}

        t.cost = float(cost) if cost is not None else None
        t.time_impact_days = float(time_impact) if time_impact is not None else None
        t.approval_required = bool(approval)
        s.commit(); s.refresh(t)

        log_audit(data.get("actor"), "change_order_update", "task", t.id)
        return _as_task_dict(t)

def get_phase_digest_toggle() -> dict:
    """Returns empty toggle placeholder for future multi-phase digests."""
    return {}

# >>> PATCH_13_STORAGE_START — ADVANCED CHANGE ORDER VIEW SUPPORT <<<

def get_all_change_orders() -> list[dict]:
    """
    Returns every task where cost or time_impact_days is set,
    for use in advanced admin reporting.
    """
    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .filter(
                (Task.cost != None) |
                (Task.time_impact_days != None)
            )
            .order_by(Task.id.desc())
            .all()
        )

        out = []
        for r in rows:
            out.append({
                "id": r.id,
                "sender": r.sender,
                "project_code": r.project_code,
                "subcontractor_name": r.subcontractor_name,
                "text": r.text,
                "cost": r.cost,
                "time_impact_days": r.time_impact_days,
                "approval_required": r.approval_required,
                "status": r.status,
                "ts": r.ts.isoformat() if r.ts else None
            })
        return out

# >>> PATCH_13_STORAGE_END <<<

# >>> PATCH_2_STORAGE_START — CALL REMINDER HELPER <<<

def create_call_reminder(sender: str,
                         raw_text: str,
                         target: str) -> dict:
    """
    Creates a reminder task:
    'remind me to call <target>'
    """
    note = f"CALL REMINDER → Call {target}"
    return create_task(
        sender=sender,
        text=note,
        tag="task",
        subtype="assigned"
    )

# >>> PATCH_2_STORAGE_END <<<

# >>> PATCH_14_STORAGE_START — CRITICAL FLAGS <<<

def is_task_critical(t: Task) -> bool:
    """
    A task is 'critical' if it has:
    • cost >= 1000, OR
    • time_impact_days >= 3, OR
    • approval_required == True
    """
    if t.cost and t.cost >= 1000:
        return True
    if t.time_impact_days and t.time_impact_days >= 3:
        return True
    if t.approval_required:
        return True
    return False

# >>> PATCH_14_STORAGE_END <<<

# >>> PATCH_15_STORAGE_START — STOCK TRACKING & CONSUMPTION <<<

class StockItem(Base):
    __tablename__ = "stock_items"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, index=True)
    project_code = Column(String(128), index=True, nullable=True)
    supplier_name = Column(String(200), nullable=True)
    unit = Column(String(32), nullable=True)  # bags, lengths, etc.

    # Running balance
    current_qty = Column(Float, default=0.0)

    # How many days of cover we prefer to keep (for suggestions)
    min_days_cover = Column(Float, nullable=True)  # e.g. 7 days

    created_at = Column(DateTime, default=dt.datetime.utcnow)
    updated_at = Column(
        DateTime,
        default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow,
    )


class StockMovement(Base):
    __tablename__ = "stock_movements"

    id = Column(Integer, primary_key=True)
    stock_item_id = Column(Integer, index=True)      # FK-like → StockItem.id
    ts = Column(DateTime, default=dt.datetime.utcnow, index=True)

    # Positive = received, Negative = consumed
    qty_change = Column(Float, nullable=False)

    # Optional link back to the originating task/order
    related_task_id = Column(Integer, nullable=True)


def _get_or_create_stock_item(
    s,
    name: str,
    project_code: Optional[str] = None,
    supplier_name: Optional[str] = None,
    unit: Optional[str] = None,
) -> StockItem:
    name_norm = (name or "").strip()
    if not name_norm:
        raise ValueError("stock name required")

    q = s.query(StockItem).filter(StockItem.name == name_norm)
    if project_code:
        q = q.filter(StockItem.project_code == project_code)

    item = q.first()
    if not item:
        item = StockItem(
            name=name_norm,
            project_code=project_code,
            supplier_name=supplier_name,
            unit=unit,
            current_qty=0.0,
        )
        s.add(item)
        s.flush()  # ensure item.id is available

    # Update optional descriptors if provided
    if supplier_name:
        item.supplier_name = supplier_name
    if unit:
        item.unit = unit

    return item


def create_stock_item(data: dict) -> dict:
    """
    Create or upsert a stock item (no quantity change yet).
    data keys:
      - name (required)
      - project_code (optional)
      - supplier_name (optional)
      - unit (optional)
      - min_days_cover (optional, float)
    """
    with SessionLocal() as s:
        name = data.get("name", "")
        project_code = data.get("project_code")
        supplier_name = data.get("supplier_name")
        unit = data.get("unit")
        min_days_cover = data.get("min_days_cover")

        item = _get_or_create_stock_item(
            s,
            name=name,
            project_code=project_code,
            supplier_name=supplier_name,
            unit=unit,
        )

        if min_days_cover is not None:
            try:
                item.min_days_cover = float(min_days_cover)
            except (TypeError, ValueError):
                item.min_days_cover = None

        s.commit()
        s.refresh(item)

        return {
            "status": "ok",
            "id": item.id,
            "name": item.name,
            "project_code": item.project_code,
            "supplier_name": item.supplier_name,
            "unit": item.unit,
            "current_qty": item.current_qty,
            "min_days_cover": item.min_days_cover,
        }


def adjust_stock(data: dict) -> dict:
    """
    Adjust stock and record a movement.

    data keys:
      - name (required)
      - delta (required, positive=in, negative=out)
      - project_code (optional)
      - supplier_name (optional)
      - unit (optional)
      - related_task_id (optional, int)
    """
    with SessionLocal() as s:
        name = data.get("name", "")
        delta_raw = data.get("delta")

        try:
            delta = float(delta_raw)
        except (TypeError, ValueError):
            return {"status": "error", "message": "invalid or missing delta"}

        project_code = data.get("project_code")
        supplier_name = data.get("supplier_name")
        unit = data.get("unit")
        related_task_id = data.get("related_task_id")

        try:
            item = _get_or_create_stock_item(
                s,
                name=name,
                project_code=project_code,
                supplier_name=supplier_name,
                unit=unit,
            )
        except ValueError as e:
            return {"status": "error", "message": str(e)}

        # Update running balance
        item.current_qty = (item.current_qty or 0.0) + delta

        mov = StockMovement(
            stock_item_id=item.id,
            qty_change=delta,
            related_task_id=related_task_id,
        )
        s.add(mov)

        s.commit()
        s.refresh(item)

        return {
            "status": "ok",
            "item_id": item.id,
            "name": item.name,
            "project_code": item.project_code,
            "current_qty": item.current_qty,
        }


def _stock_usage_metrics(
    s,
    item: StockItem,
    window_days: int = 30,
) -> dict:
    """
    Compute simple usage metrics for a single item over the last N days.
    We only look at negative movements (consumption).
    """
    now = dt.datetime.utcnow()
    since = now - dt.timedelta(days=window_days)

    moves = (
        s.query(StockMovement)
        .filter(
            StockMovement.stock_item_id == item.id,
            StockMovement.ts >= since,
            StockMovement.qty_change < 0,  # consumption only
        )
        .all()
    )

    total_used = sum(-m.qty_change for m in moves)  # make positive
    avg_daily_use = 0.0
    if window_days > 0 and total_used > 0:
        avg_daily_use = total_used / float(window_days)

    days_cover = None
    if avg_daily_use > 0:
        days_cover = (item.current_qty or 0.0) / avg_daily_use

    min_cover = item.min_days_cover or 7.0  # default 7 days if unset
    reorder_suggested = False
    if days_cover is not None and days_cover < min_cover:
        reorder_suggested = True

    return {
        "avg_daily_use": avg_daily_use,
        "days_cover": days_cover,
        "min_days_cover": min_cover,
        "reorder_suggested": reorder_suggested,
    }


def get_stock_report(project_code: Optional[str] = None) -> dict:
    """
    Returns a report of all stock items and basic consumption/suggestion
    metrics. If project_code is provided, only items for that project
    are included.
    """
    with SessionLocal() as s:
        q = s.query(StockItem)
        if project_code:
            q = q.filter(StockItem.project_code == project_code)

        items = q.order_by(StockItem.name.asc()).all()
        rows = []

        for item in items:
            metrics = _stock_usage_metrics(s, item)
            msg = None
            if metrics["reorder_suggested"]:
                msg = (
                    f"Low cover: approx {metrics['days_cover']:.1f} days "
                    f"remaining (target {metrics['min_days_cover']:.1f} days)."
                )

            rows.append(
                {
                    "id": item.id,
                    "name": item.name,
                    "project_code": item.project_code,
                    "supplier_name": item.supplier_name,
                    "unit": item.unit,
                    "current_qty": item.current_qty,
                    "avg_daily_use": metrics["avg_daily_use"],
                    "days_cover": metrics["days_cover"],
                    "min_days_cover": metrics["min_days_cover"],
                    "reorder_suggested": metrics["reorder_suggested"],
                    "message": msg,
                }
            )

        return {"status": "ok", "items": rows}

# >>> PATCH_15_STORAGE_END <<<

# >>> PATCH_11_STORAGE_START — SUPPLIER DIRECTORY <<<

class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), unique=True, nullable=False, index=True)
    phone = Column(String(64), nullable=True)
    email = Column(String(200), nullable=True)
    notes = Column(Text, nullable=True)

    created_at = Column(DateTime, default=dt.datetime.utcnow)
    updated_at = Column(
        DateTime,
        default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow
    )

def supplier_create(data: dict) -> dict:
    with SessionLocal() as s:
        sup = Supplier(
            name=data.get("name", ""),
            phone=data.get("phone"),
            email=data.get("email"),
            notes=data.get("notes"),
        )
        s.add(sup)
        s.commit()
        s.refresh(sup)
        return {"status": "ok", "id": sup.id}

def supplier_list() -> list[dict]:
    with SessionLocal() as s:
        rows = s.query(Supplier).order_by(Supplier.name.asc()).all()
        return [
            {
                "id": r.id,
                "name": r.name,
                "phone": r.phone,
                "email": r.email,
                "notes": r.notes,
            }
            for r in rows
        ]

# >>> PATCH_11_STORAGE_END <<<
