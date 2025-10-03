# storage.py
import os, sqlite3, threading

_DB_LOCK = threading.Lock()
# If DATABASE_URL is a simple filename, we use SQLite at that path.
# If it's unset, default to a local file "hubflo.db".
_DB_PATH = os.environ.get("DATABASE_URL", "hubflo.db")

def _conn():
    return sqlite3.connect(_DB_PATH, check_same_thread=False)

def init_db():
    with _DB_LOCK, _conn() as cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            text   TEXT,
            ts     DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cx.commit()

def create_task(task):
    """task: dict with keys 'sender', 'text'"""
    with _DB_LOCK, _conn() as cx:
        cur = cx.execute(
            "INSERT INTO tasks (sender, text) VALUES (?, ?)",
            (task.get("sender"), task.get("text"))
        )
        cx.commit()
        return cur.lastrowid

def get_tasks(ids=None):
    q = "SELECT id, sender, text, ts FROM tasks"
    params = ()
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        q += f" WHERE id IN ({placeholders})"
        params = tuple(ids)
    q += " ORDER BY id DESC LIMIT 100"
    with _DB_LOCK, _conn() as cx:
        rows = cx.execute(q, params).fetchall()
    return [
        {"id": r[0], "sender": r[1], "text": r[2], "ts": r[3]}
        for r in rows
    ]