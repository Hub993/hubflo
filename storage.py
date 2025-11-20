# storage_v6_1.py — HUBFLO V6.1 working
# Derived from verified v5 base + reinforced tethered safeguards
# ---------------------------------------------------------------------
import os
import datetime as dt
from typing import Optional, Iterable

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, Boolean, Float
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

    _repair_system_state()
    _repair_tasks()

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
        qry = _apply_client_filter(s.query(Task), client_id).order_by(Task.id.desc())

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
        qry = _apply_client_filter(
            s.query(Task)
        ).order_by(Task.id.desc())

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

def create_stock_item(data: dict):
    """Temporary stub; replace with full implementation later."""
    return {"status": "ok", "message": "Stock item created", "data": data}

def adjust_stock(data: dict):
    """Temporary stub; replace with full implementation later."""
    return {"status": "ok", "message": "Stock adjusted", "data": data}

def get_stock_report():
    """Temporary stub; replace with full implementation later."""
    return {"status": "ok", "report": []}

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

# ---------------------------------------------------------------------
# Project → PM lookup (stub; returns all PMs matching project_code)
# ---------------------------------------------------------------------
def get_pms_for_project(project_code: str):
    with SessionLocal() as s:
        return [
            {
                "wa_id": u.wa_id,
                "name": u.name,
                "role": u.role,
                "primary": True  # default until multi-PM assignment
            }
            for u in s.query(User).filter(
                User.project_code == project_code,
                User.role == "pm",
                User.active == True
            ).all()
        ]
