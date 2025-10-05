# storage.py  — HubFlo v4
# Minimal, safe, single-file persistence using SQLAlchemy.
# Notes:
# - Supports sqlite (default) or Postgres via DATABASE_URL.
# - Adds fields for lifecycle, overrun/rework, project_code, subcontractor_name.
# - Adds a lightweight Meeting table for on-phone meeting runs.

import os
import datetime as dt
from typing import Optional, Dict, Any, List

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, Boolean, Float
)
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------------------------------------------------------------------------
# DB bootstrap
# ---------------------------------------------------------------------------

def _normalize_db_url(url: str) -> str:
    if not url:
        return "sqlite:///hubflo.db"
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ENGINE = create_engine(_normalize_db_url(DATABASE_URL), pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, future=True)
Base = declarative_base()

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True)

    # core
    sender = Column(String(64), index=True, nullable=True)   # wa_id or phone
    text = Column(Text, nullable=True)                       # raw user text
    tag = Column(String(32), index=True, nullable=True)      # order|change|task|urgent|none
    ts = Column(DateTime, default=dt.datetime.utcnow, index=True)

    # lifecycle / status
    status = Column(String(24), default="open", index=True)  # open|in_progress|done|approved|rejected
    due_date = Column(DateTime, nullable=True, index=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    rejected_at = Column(DateTime, nullable=True)

    # flags
    is_rework = Column(Boolean, default=False)
    overrun_days = Column(Float, default=0.0)

    # attachments (flat for now)
    attachment_url = Column(Text, nullable=True)
    attachment_mime = Column(String(128), nullable=True)
    attachment_name = Column(String(256), nullable=True)

    # routing / roles
    subcontractor_name = Column(String(128), index=True, nullable=True)
    project_code = Column(String(128), index=True, nullable=True)

class Meeting(Base):
    __tablename__ = "meetings"

    id = Column(Integer, primary_key=True)
    title = Column(String(256), nullable=True)         # e.g. "JJ Electrical / Site Harms"
    project_code = Column(String(128), index=True, nullable=True)
    participant = Column(String(128), index=True, nullable=True)   # subcontractor or group
    scheduled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    closed_at = Column(DateTime, nullable=True)

def init_db():
    Base.metadata.create_all(ENGINE)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _as_task_dict(t: Task) -> Dict[str, Any]:
    fmt = lambda d: d.strftime("%Y-%m-%d %H:%M:%S") if d else None
    return {
        "id": t.id,
        "sender": t.sender,
        "text": t.text,
        "tag": t.tag,
        "ts": fmt(t.ts),
        "status": t.status,
        "due_date": fmt(t.due_date),
        "started_at": fmt(t.started_at),
        "completed_at": fmt(t.completed_at),
        "approved_at": fmt(t.approved_at),
        "rejected_at": fmt(t.rejected_at),
        "is_rework": t.is_rework,
        "overrun_days": t.overrun_days,
        "attachment": {
            "name": t.attachment_name,
            "mime": t.attachment_mime,
            "url": t.attachment_url,
        } if t.attachment_url else None,
        "subcontractor_name": t.subcontractor_name,
        "project_code": t.project_code,
    }

def _as_meeting_dict(m: Meeting) -> Dict[str, Any]:
    fmt = lambda d: d.strftime("%Y-%m-%d %H:%M:%S") if d else None
    return {
        "id": m.id,
        "title": m.title,
        "project_code": m.project_code,
        "participant": m.participant,
        "scheduled_at": fmt(m.scheduled_at),
        "created_at": fmt(m.created_at),
        "started_at": fmt(m.started_at),
        "closed_at": fmt(m.closed_at),
    }

# ---------------------------------------------------------------------------
# CRUD: tasks
# ---------------------------------------------------------------------------

def create_task(
    sender: str,
    text: str,
    tag: Optional[str] = None,
    attachment: Optional[Dict[str, Any]] = None,
    subcontractor_name: Optional[str] = None,
    project_code: Optional[str] = None,
    due_date: Optional[dt.datetime] = None
) -> Dict[str, Any]:
    with SessionLocal() as s:
        t = Task(
            sender=sender,
            text=text or "",
            tag=tag or None,
            subcontractor_name=subcontractor_name,
            project_code=project_code,
            due_date=due_date
        )
        if attachment:
            t.attachment_name = attachment.get("name")
            t.attachment_mime = attachment.get("mime")
            t.attachment_url  = attachment.get("url")
        s.add(t)
        s.commit()
        s.refresh(t)
        return _as_task_dict(t)

def get_tasks(
    tag: Optional[str] = None,
    q: Optional[str] = None,
    sender: Optional[str] = None,
    project_code: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    with SessionLocal() as s:
        qry = s.query(Task).order_by(Task.id.desc())
        if tag:
            if tag.lower() == "none":
                qry = qry.filter((Task.tag.is_(None)) | (Task.tag == ""))
            else:
                qry = qry.filter(Task.tag == tag)
        if sender:
            qry = qry.filter(Task.sender == sender)
        if project_code:
            qry = qry.filter(Task.project_code == project_code)
        if q:
            like = f"%{q}%"
            qry = qry.filter(Task.text.ilike(like))
        rows = qry.limit(limit).all()
        return [_as_task_dict(t) for t in rows]

def get_summary():
    with SessionLocal() as s:
        rows = s.query(Task).order_by(Task.id.desc()).limit(100).all()
        counts = {}
        for t in rows:
            key = (t.tag or "none")
            counts[key] = counts.get(key, 0) + 1
        latest = [_as_task_dict(t) for t in rows[:10]]
        return {"counts_by_tag": counts, "latest": latest}

def set_due(task_id: int, due: dt.datetime) -> Optional[Dict[str, Any]]:
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return None
        t.due_date = due
        s.commit()
        s.refresh(t)
        return _as_task_dict(t)

def set_started(task_id: int, ts: dt.datetime) -> Optional[Dict[str, Any]]:
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return None
        t.started_at = ts
        t.status = "in_progress"
        s.commit()
        s.refresh(t)
        return _as_task_dict(t)

def mark_done(task_id: int):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return None
        t.status = "done"
        t.completed_at = dt.datetime.utcnow()
        if t.due_date and t.completed_at:
            delta = (t.completed_at.date() - t.due_date.date()).days
            t.overrun_days = float(max(0, delta))
        s.commit()
        s.refresh(t)
        return _as_task_dict(t)

def approve_task(task_id: int):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return None
        t.status = "approved"
        t.approved_at = dt.datetime.utcnow()
        s.commit()
        s.refresh(t)
        return _as_task_dict(t)

def reject_task(task_id: int, rework: bool = True):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return None
        t.status = "rejected"
        t.is_rework = bool(rework)
        t.rejected_at = dt.datetime.utcnow()
        s.commit()
        s.refresh(t)
        return _as_task_dict(t)

# ---------------------------------------------------------------------------
# Accuracy scoring
# ---------------------------------------------------------------------------

def subcontractor_accuracy(subcontractor_name: str):
    with SessionLocal() as s:
        rows = s.query(Task).filter(Task.subcontractor_name == subcontractor_name).all()
        total = len(rows)
        on_time = 0
        overruns = 0
        reworks = 0
        for t in rows:
            if t.status in ("approved", "done"):
                if (t.overrun_days or 0) > 0:
                    overruns += 1
                else:
                    on_time += 1
            if t.is_rework:
                reworks += 1
        pct = 0 if total == 0 else round(100.0 * on_time / total)
        return {
            "subcontractor": subcontractor_name,
            "total": total,
            "on_time": on_time,
            "overruns": overruns,
            "reworks": reworks,
            "accuracy_pct": int(pct),
        }

# ---------------------------------------------------------------------------
# Meetings
# ---------------------------------------------------------------------------

def create_meeting(title: str, participant: Optional[str], project_code: Optional[str],
                   scheduled_at: Optional[dt.datetime]) -> Dict[str, Any]:
    with SessionLocal() as s:
        m = Meeting(
            title=title,
            participant=participant,
            project_code=project_code,
            scheduled_at=scheduled_at,
        )
        s.add(m)
        s.commit()
        s.refresh(m)
        return _as_meeting_dict(m)

def get_meetings(limit: int = 50) -> List[Dict[str, Any]]:
    with SessionLocal() as s:
        rows = s.query(Meeting).order_by(Meeting.id.desc()).limit(limit).all()
        return [_as_meeting_dict(m) for m in rows]

def set_meeting_started(meeting_id: int) -> Optional[Dict[str, Any]]:
    with SessionLocal() as s:
        m = s.get(Meeting, meeting_id)
        if not m:
            return None
        m.started_at = dt.datetime.utcnow()
        s.commit()
        s.refresh(m)
        return _as_meeting_dict(m)

def set_meeting_closed(meeting_id: int) -> Optional[Dict[str, Any]]:
    with SessionLocal() as s:
        m = s.get(Meeting, meeting_id)
        if not m:
            return None
        m.closed_at = dt.datetime.utcnow()
        s.commit()
        s.refresh(m)
        return _as_meeting_dict(m)

# ---------------------------------------------------------------------------
# Escalations & digests
# ---------------------------------------------------------------------------

def _progress_ratio(now: dt.datetime, t: Task) -> float:
    """Return fraction of lead consumed [0..1+] based on started_at..due_date, else ts..due_date."""
    if not t.due_date:
        return 0.0
    start = t.started_at or t.ts or (now - dt.timedelta(days=1))
    total = (t.due_date - start).total_seconds()
    if total <= 0:
        return 1.1  # force overdue path
    elapsed = (now - start).total_seconds()
    return max(0.0, elapsed / total)

def run_escalations(now: Optional[dt.datetime] = None) -> Dict[str, Any]:
    """
    Computes who should be nudged based on 80/90% rules and overdue.
    Returns a dry-run structure (the API layer can 'send' via WA).
    """
    now = now or dt.datetime.utcnow()
    out = {"at": now.isoformat(), "nudge_80": [], "nudge_90": [], "overdue": []}
    with SessionLocal() as s:
        rows = s.query(Task).all()
        for t in rows:
            if not t.due_date:
                continue
            r = _progress_ratio(now, t)
            if r >= 1.0 and t.status not in ("approved", "done"):
                out["overdue"].append(_as_task_dict(t))
            elif r >= 0.9:
                out["nudge_90"].append(_as_task_dict(t))
            elif r >= 0.8:
                out["nudge_80"].append(_as_task_dict(t))
    return out

def digest_preview(when: str = "morning") -> Dict[str, Any]:
    """
    Preview payload for the 6am/6pm digests (no sending here).
    """
    now = dt.datetime.utcnow()
    nudges = run_escalations(now)
    upcoming = []
    overdue = nudges["overdue"]
    with SessionLocal() as s:
        soon = now + dt.timedelta(days=2)
        rows = s.query(Task).filter(Task.due_date != None).all()  # noqa: E711
        for t in rows:
            if t.due_date and now <= t.due_date <= soon and t.status not in ("approved", "done"):
                upcoming.append(_as_task_dict(t))
    return {
        "when": when,
        "generated_at": now.isoformat(),
        "upcoming_48h": upcoming,
        "nudge_80": nudges["nudge_80"],
        "nudge_90": nudges["nudge_90"],
        "overdue": overdue,
    }

# ---------------------------------------------------------------------------
# Storage status
# ---------------------------------------------------------------------------

def storage_status() -> Dict[str, Any]:
    """
    Rough storage telemetry for admin. For sqlite we report file size; for Postgres we show URL class only.
    Uses thresholds 60/80/90% against MAX_DB_MB (env).
    """
    max_mb = float(os.environ.get("MAX_DB_MB", "200"))  # default 200MB if unspecified
    url = _normalize_db_url(os.environ.get("DATABASE_URL", "").strip())
    kind = "postgres" if url.startswith("postgresql://") else "sqlite"
    used_mb = None
    if kind == "sqlite":
        path = url.replace("sqlite:///", "", 1)
        if os.path.exists(path):
            used_mb = os.path.getsize(path) / (1024 * 1024)
        else:
            used_mb = 0.0
    percent = None if used_mb is None else round(100.0 * used_mb / max_mb, 1)
    level = None
    if percent is not None:
        level = 60 if percent >= 90 else 40 if percent >= 80 else 20 if percent >= 60 else 0
    return {
        "driver": kind,
        "max_mb": max_mb,
        "used_mb": used_mb,
        "percent": percent,
        "alert_level": level,  # 0/20/40/60 meaning none/60/80/90
    }