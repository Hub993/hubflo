# storage.py
import os
import datetime as dt
from urllib.parse import urlparse

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, Boolean, Float
)
from sqlalchemy.orm import sessionmaker, declarative_base

# --- DB bootstrap ------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

def _normalize_db_url(url: str) -> str:
    """
    Accepts sqlite or postgres URLs. Render usually provides DATABASE_URL.
    """
    if not url:
        # Ephemeral local sqlite fallback (OK for free-tier tests)
        return "sqlite:///hubflo.db"
    # Render might give postgres://; SQLAlchemy wants postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

ENGINE = create_engine(_normalize_db_url(DATABASE_URL), pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, future=True)
Base = declarative_base()

# --- Models -----------------------------------------------------------------

class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True)

    # core
    sender = Column(String(64), index=True, nullable=True)   # wa_id or phone
    text = Column(Text, nullable=True)                       # raw user text
    tag = Column(String(32), index=True, nullable=True)      # order/change/task/urgent/none
    ts = Column(DateTime, default=dt.datetime.utcnow, index=True)

    # lifecycle / status
    status = Column(String(24), default="open", index=True)  # open|in_progress|done|approved|rejected
    due_date = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    rejected_at = Column(DateTime, nullable=True)

    # flags
    is_rework = Column(Boolean, default=False)
    overrun_days = Column(Float, default=0.0)

    # attachments (store first; can expand to child table later)
    attachment_url = Column(Text, nullable=True)
    attachment_mime = Column(String(128), nullable=True)
    attachment_name = Column(String(256), nullable=True)

    # routing / roles (simple placeholders)
    subcontractor_name = Column(String(128), nullable=True)
    project_code = Column(String(128), index=True, nullable=True)

def init_db():
    Base.metadata.create_all(ENGINE)

# --- CRUD helpers ------------------------------------------------------------

def _as_dict(t: Task) -> dict:
    return {
        "id": t.id,
        "sender": t.sender,
        "text": t.text,
        "tag": t.tag,
        "ts": t.ts.strftime("%Y-%m-%d %H:%M:%S") if t.ts else None,
        "status": t.status,
        "due_date": t.due_date.strftime("%Y-%m-%d %H:%M:%S") if t.due_date else None,
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

def create_task(sender: str, text: str, tag: str = None,
                attachment: dict | None = None,
                subcontractor_name: str | None = None,
                project_code: str | None = None,
                due_date: dt.datetime | None = None) -> dict:
    with SessionLocal() as s:
        t = Task(
            sender=sender,
            text=text,
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
        return _as_dict(t)

def get_tasks(tag: str | None = None, q: str | None = None,
              sender: str | None = None, limit: int = 100):
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
        return [_as_dict(t) for t in rows]

def get_summary():
    with SessionLocal() as s:
        rows = s.query(Task).order_by(Task.id.desc()).limit(50).all()
        counts = {}
        for t in rows:
            key = (t.tag or "none")
            counts[key] = counts.get(key, 0) + 1
        latest = [_as_dict(t) for t in rows[:10]]
        return {"counts_by_tag": counts, "latest": latest}

# status transitions

def mark_done(task_id: int):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return None
        t.status = "done"
        t.completed_at = dt.datetime.utcnow()
        # compute overrun if due_date existed
        if t.due_date and t.completed_at:
            delta = (t.completed_at.date() - t.due_date.date()).days
            t.overrun_days = float(max(0, delta))
        s.commit()
        s.refresh(t)
        return _as_dict(t)

def approve_task(task_id: int):
    with SessionLocal() as s:
        t = s.get(Task, task_id)
        if not t:
            return None
        t.status = "approved"
        t.approved_at = dt.datetime.utcnow()
        s.commit()
        s.refresh(t)
        return _as_dict(t)

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
        return _as_dict(t)

# accuracy scoring for a subcontractor (simple baseline)

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
        pct = 0.0 if total == 0 else round(100.0 * on_time / total)
        return {
            "subcontractor": subcontractor_name,
            "total": total,
            "on_time": on_time,
            "overruns": overruns,
            "reworks": reworks,
            "accuracy_pct": int(pct),
        }