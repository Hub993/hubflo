# storage.py
import os, sqlite3, threading

_DB_LOCK = threading.Lock()
_DB_PATH = os.environ.get("DATABASE_URL", "hubflo.db")

def _conn():
    return sqlite3.connect(_DB_PATH, check_same_thread=False)

def _column_exists(cx, table, col):
    try:
        cols = cx.execute(f"PRAGMA table_info({table})").fetchall()
        names = {c[1] for c in cols}
        return col in names
    except Exception:
        return False

def init_db():
    with _DB_LOCK, _conn() as cx:
        # Base table
        cx.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            text   TEXT,
            ts     DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        # Ensure 'tag' column exists (SQLite doesn't add cols on CREATE IF NOT EXISTS)
        if not _column_exists(cx, "tasks", "tag"):
            try:
                cx.execute("ALTER TABLE tasks ADD COLUMN tag TEXT")
            except Exception:
                pass
        cx.commit()

def create_task(task):
    """
    task: dict with keys 'sender', 'text', optional 'tag'
    """
    with _DB_LOCK, _conn() as cx:
        cur = cx.execute(
            "INSERT INTO tasks (sender, text, tag) VALUES (?, ?, ?)",
            (task.get("sender"), task.get("text"), task.get("tag"))
        )
        cx.commit()
        return cur.lastrowid

def get_tasks(ids=None):
    q = "SELECT id, sender, text, tag, ts FROM tasks"
    params = ()
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        q += f" WHERE id IN ({placeholders})"
        params = tuple(ids)
    q += " ORDER BY id DESC LIMIT 100"
    with _DB_LOCK, _conn() as cx:
        rows = cx.execute(q, params).fetchall()
    return [
        {"id": r[0], "sender": r[1], "text": r[2], "tag": r[3], "ts": r[4]}
        for r in rows
    ]