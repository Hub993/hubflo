import os, datetime
from sqlalchemy import create_engine, text

DB_URL = os.environ["DATABASE_URL"]
engine = create_engine(DB_URL, pool_pre_ping=True)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
 id SERIAL PRIMARY KEY,
 alias TEXT UNIQUE NOT NULL,
 name TEXT,
 wa_id TEXT UNIQUE,
 role TEXT,
 active BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS tasks (
 task_id INTEGER PRIMARY KEY,
 project TEXT, trade TEXT, assignee_alias TEXT,
 due_date DATE, status TEXT, eta TEXT,
 notes TEXT, evidence_urls TEXT, change_orders TEXT,
 last_update_ts TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS events (
 id SERIAL PRIMARY KEY,
 ts TIMESTAMPTZ DEFAULT NOW(),
 task_id INTEGER,
 kind TEXT,
 payload JSONB
);
"""

def init_db():
 with engine.begin() as cx:
   cx.exec_driver_sql(SCHEMA_SQL)

def now_iso():
 return datetime.datetime.utcnow().isoformat()

def upsert_task(task):
 sql = """
 INSERT INTO tasks (task_id,project,trade,assignee_alias,due_date,status,eta,notes,evidence_urls,change_orders,last_update_ts)
 VALUES (:task_id,:project,:trade,:assignee_alias,:due_date,:status,:eta,:notes,:evidence_urls,:change_orders,:last_update_ts)
 ON CONFLICT (task_id) DO UPDATE SET
   project=EXCLUDED.project, trade=EXCLUDED.trade, assignee_alias=EXCLUDED.assignee_alias,
   due_date=EXCLUDED.due_date, status=EXCLUDED.status, eta=EXCLUDED.eta,
   notes=COALESCE(tasks.notes,'') || CASE WHEN EXCLUDED.notes IS NULL OR EXCLUDED.notes='' THEN '' ELSE ' | '||EXCLUDED.notes END,
   evidence_urls=COALESCE(tasks.evidence_urls,'') || CASE WHEN EXCLUDED.evidence_urls IS NULL OR EXCLUDED.evidence_urls='' THEN '' ELSE ' | '||EXCLUDED.evidence_urls END,
   change_orders=COALESCE(tasks.change_orders,'') || CASE WHEN EXCLUDED.change_orders IS NULL OR EXCLUDED.change_orders='' THEN '' ELSE ' | '||EXCLUDED.change_orders END,
   last_update_ts=EXCLUDED.last_update_ts;
 """
 with engine.begin() as cx:
   cx.execute(text(sql), task)

def append_event(task_id, kind, payload):
 with engine.begin() as cx:
   cx.execute(text("INSERT INTO events (task_id, kind, payload) VALUES (:t,:k,:p)"),
              {"t": task_id, "k": kind, "p": payload})

def get_tasks(ids):
 q = text("SELECT task_id, status, eta, notes, evidence_urls, change_orders FROM tasks WHERE task_id = ANY(:ids)")
 with engine.begin() as cx:
   return [dict(r) for r in cx.execute(q, {"ids": ids}).mappings()]
