# storage.py
import os
import datetime as dt
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, Boolean, Float, func
)
from sqlalchemy.orm import sessionmaker, declarative_base

# --- DB bootstrap ------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

def _normalize_db_url(url: str) -> str:
    if not url:
        return "sqlite:///hubflo.db"
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
    sender = Column(String(64), index=True, nullable=True)
    text = Column(Text, nullable=True)
    tag = Column(String(32), index=True, nullable=True)
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

    # attachments (metadata only for now)
    attachment_url = Column(Text, nullable=True)
    attachment_mime = Column(String(128), nullable=True)
    attachment_name = Column(String(256), nullable=True)

    # routing / roles
    subcontractor_name = Column(String(128), index=True, nullable=True)
    project_code = Column(String(128), index=True, nullable=True)

def init_db():
    Base.metadata.create_all(ENGINE)

# --- Helpers ----------------------------------------------------------------

def _as_dt_str(d: dt.datetime | None) -> str | None:
    return d.strftime("%Y-%m-%d %H:%M:%S") if d else None

def _as_dict(t: Task) -> dict:
    return {
        "id": t.id,
        "sender": t.sender,
        "text": t.text,
        "tag": t.tag,
        "ts": _as_dt_str(t.ts),
        "status": t.status,
        "due_date": _as_dt_str(t.due_date),
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

# --- CRUD -------------------------------------------------------------------

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

def _apply_search(qry, q: str):
    """
    Case-insensitive, multi-term search across several fields.
    Works on SQLite and Postgres.
    """
    terms = [t.strip() for t in (q or "").split() if t.strip()]
    if not terms:
        return qry
    fields = [
        func.coalesce(func.lower(Task.text), ""),
        func.coalesce(func.lower(Task.tag), ""),
        func.coalesce(func.lower(Task.sender), ""),
        func.coalesce(func.lower(Task.subcontractor_name), ""),
        func.coalesce(func.lower(Task.project_code), ""),
    ]
    for term in terms:
        like = f"%{term.lower()}%"
        # each term must match at least one field
        qry = qry.filter(
            (fields[0].like(like)) |
            (fields[1].like(like)) |
            (fields[2].like(like)) |
            (fields[3].like(like)) |
            (fields[4].like(like))
        )
    return qry

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
            qry = _apply_search(qry, q)
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

# --- Status transitions ------------------------------------------------------

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

# --- Accuracy scoring --------------------------------------------------------

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