# storage_v6.py — HUBFLO Unified Storage Layer (Post-v5/v6 rebuild)
# Derived from verified v5 base + reinforced tethered safeguards
# ---------------------------------------------------------------------
import os
import datetime as dt
from typing import Optional, Iterable

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, Boolean, Float
)
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------------------------------------------------------------------
# DB bootstrap
# ---------------------------------------------------------------------
def _normalize_db_url(url: str) -> str:
    if not url:
        return "sqlite:///hubflo.db"
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

DATABASE_URL = _normalize_db_url(os.environ.get("DATABASE_URL", "").strip())
ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, future=True)
Base = declarative_base()

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------
class Task(Base):
    __tablename__ = "tasks"

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

    attachment_url = Column(Text)
    attachment_mime = Column(String(128))
    attachment_name = Column(String(256))

    order_state = Column(String(32))  # quoted|pending_approval|approved|cancelled|invoiced|enacted
    subtype = Column(String(24))      # assigned|self (added in v6)

    last_updated = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow)

class Meeting(Base):
    __tablename__ = "meetings"

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

class Audit(Base):
    __tablename__ = "audits"

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
# Core CRUD
# ---------------------------------------------------------------------
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

def get_tasks(tag: Optional[str] = None, q: Optional[str] = None,
              sender: Optional[str] = None, limit: int = 100):
    with SessionLocal() as s:
        qry = s.query(Task).order_by(Task.id.desc())
        if tag:
            if tag.lower() == "none":
                qry = qry.filter((Task.tag.is_(None)) | (Task.tag == ""))
            else:
                qry = qry.filter(Task.tag == tag)
        if sender:
            qry = qry.filter(Task.sender == sender)
        if q:
            like = f"%{q}%"
            qry = qry.filter(Task.text.ilike(like))
        rows = qry.limit(limit).all()
        return [_as_task_dict(t) for t in rows]

def get_summary(limit_latest: int = 50):
    with SessionLocal() as s:
        rows = s.query(Task).order_by(Task.id.desc()).limit(limit_latest).all()
        counts = {}
        for t in rows:
            key = (t.tag or "none")
            counts[key] = counts.get(key, 0) + 1
        latest = [_as_task_dict(t) for t in rows[:10]]
        return {"counts_by_tag": counts, "latest": latest}

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
    with Session_local() as s:
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
    """Temporary stub; replace with full implementation later."""
    return {"status": "ok", "message": "Change order recorded", "data": data}

def create_stock_item(data: dict):
    """Temporary stub; replace with full implementation later."""
    return {"status": "ok", "message": "Stock item created", "data": data}

def adjust_stock(data: dict):
    """Temporary stub; replace with full implementation later."""
    return {"status": "ok", "message": "Stock adjusted", "data": data}

def get_stock_report():
    """Temporary stub; replace with full implementation later."""
    return {"status": "ok", "report": []}