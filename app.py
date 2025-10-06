# v5 app.py
import os
import json
import logging
import datetime as dt
from typing import Optional

from flask import Flask, request, jsonify, Response
import requests

from storage import (
    init_db, create_task, get_tasks, get_summary,
    mark_done, approve_task, reject_task, set_order_state, revoke_last,
    subcontractor_accuracy,
    create_meeting, start_meeting, close_meeting
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Env config
# -----------------------------------------------------------------------------
ADMIN_TOKEN = os.environ.get("HUBFLO_ADMIN_TOKEN", "").strip()

D360_KEY = (
    os.environ.get("DIALOG360_API_KEY")
    or os.environ.get("Dialog360_API_Key")
    or os.environ.get("D360_KEY")
    or os.environ.get("D360_key")
    or ""
).strip()

DEFAULT_PHONE_ID = os.environ.get("BOUND_NUMBER", "").strip()
WHATSAPP_BASE = "https://waba.360dialog.io/v1/messages"

MAX_ATTACHMENT_MB = 16
ALLOWED_MIME_PREFIXES = ("image/", "application/pdf", "application/vnd.", "text/")

ORDER_LIFECYCLE_STATES = ["quoted", "pending_approval", "approved", "cancelled", "invoiced", "enacted"]

# -----------------------------------------------------------------------------
# Boot DB
# -----------------------------------------------------------------------------
init_db()

# -----------------------------------------------------------------------------
# Tagging / classification
# -----------------------------------------------------------------------------
ORDER_PREFIXES = ("order", "purchase", "procure", "buy")
CHANGE_PREFIXES = ("change", "variation", "revise", "amend", "adjust")
TASK_PREFIXES  = ("task", "todo", "to-do", "install", "fix", "inspect", "lay", "build", "schedule")

HASHTAG_MAP = {
    "#order": "order",
    "#change": "change",
    "#task": "task",
    "#urgent": "urgent",
}

def classify_tag(text: str) -> Optional[str]:
    if not text:
        return None
    t = text.strip().lower()

    # hashtag override
    for h, tag in HASHTAG_MAP.items():
        if h in t:
            return tag

    # keyword prefix
    for p in ORDER_PREFIXES:
        if t.startswith(p + " "): return "order"
    for p in CHANGE_PREFIXES:
        if t.startswith(p + " "): return "change"
    for p in TASK_PREFIXES:
        if t.startswith(p + " "): return "task"

    # quantity + material heuristic
    if any(u in t for u in ["m ", "meter", "metre", "roll", "cable", "conduit"]) and any(ch.isdigit() for ch in t):
        return "order"

    # urgent words
    if any(w in t for w in ["urgent", "asap", "immediately"]):
        return "urgent"

    return None

# -----------------------------------------------------------------------------
# WhatsApp send utility (will 401 in sandbox – OK)
# -----------------------------------------------------------------------------
def send_whatsapp_text(phone_id: str, to: str, body: str) -> tuple[bool, dict]:
    if not (D360_KEY and phone_id and to and body):
        log.warning("send_whatsapp_text skipped (missing key/to/body)")
        return False, {}
    headers = {"D360-API-KEY": D360_KEY, "Content-Type": "application/json"}
    payload = {"to": to, "type": "text", "text": {"body": body}}
    try:
        r = requests.post(WHATSAPP_BASE, headers=headers, json=payload, timeout=10)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        log.info("D360_SEND status_code=%s body=%s", r.status_code, json.dumps(data))
        return (200 <= r.status_code < 300), data
    except Exception as e:
        log.exception("D360 send error: %s", e)
        return False, {"error": str(e)}

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def health():
    return "HubFlo service running", 200

# -----------------------------------------------------------------------------
# Webhook (Meta/360dialog inbound)
# -----------------------------------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    log.info("Inbound webhook hit")
    raw = request.get_json(silent=True) or {}
    log.info("RAW_PAYLOAD=%s", json.dumps(raw))

    try:
        entry = (raw.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        msgs = value.get("messages") or []
        contacts = value.get("contacts") or []
        phone_id = (value.get("metadata") or {}).get("phone_number_id") or DEFAULT_PHONE_ID
    except Exception:
        msgs, contacts, phone_id = [], [], DEFAULT_PHONE_ID

    sender = None
    if contacts:
        sender = contacts[0].get("wa_id") or sender

    created_ids = []
    for m in msgs:
        sender = m.get("from") or sender
        mtype = m.get("type")
        text = None
        attachment = None

        if mtype == "text":
            text = (m.get("text") or {}).get("body")
        elif mtype in ("image", "document", "audio", "video"):
            # Only capture metadata placeholder (download requires Graph API)
            mid = m.get(mtype, {}).get("id")
            url = f"whatsapp_media://{mtype}/{mid}" if mid else None
            mime = m.get(mtype, {}).get("mime_type")
            name = m.get(mtype, {}).get("filename")
            attachment = {"url": url, "mime": mime, "name": name}
            text = (m.get(mtype, {}) or {}).get("caption")

        tag = classify_tag(text or "")
        log.info("MSG_BODY=%s SENDER=%s PHONE_ID=%s", text, sender, phone_id)
        if tag:
            log.info("TAG=%s", tag)

        # Order lifecycle hint (#quoted, #approved, etc.)
        order_state = None
        if tag == "order" and text:
            t_low = text.lower()
            for state in ORDER_LIFECYCLE_STATES:
                if f"#{state}" in t_low:
                    order_state = state
                    break

        row = create_task(sender=sender, text=text or "", tag=tag, attachment=attachment, order_state=order_state)
        created_ids.append(row["id"])
        log.info("TASK_CREATED id=%s", row["id"])

        # Soft auto-reply (won’t deliver in sandbox, but we log attempt)
        reply = None
        if tag == "order":
            reply = "Order noted. We’ll track and keep you updated."
        elif tag == "change":
            reply = "Change request logged. Awaiting quotes/approval steps."
        elif tag == "task":
            reply = "Task created. We’ll remind before start and near due."
        if reply:
            ok, _ = send_whatsapp_text(phone_id=phone_id, to=sender, body=reply)
            log.info("AUTO_REPLY status=%s", ok)

    return ("", 200)

# -----------------------------------------------------------------------------
# Admin guard
# -----------------------------------------------------------------------------
def _auth_fail():
    return Response("Unauthorized", 401)

def _check_admin():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return False
    return True

# -----------------------------------------------------------------------------
# Admin: HTML view & JSON APIs
# -----------------------------------------------------------------------------
@app.route("/admin/view", methods=["GET"])
def admin_view():
    if not _check_admin(): return _auth_fail()
    tag = request.args.get("tag") or None
    q = request.args.get("q") or None
    sender = request.args.get("sender") or None
    rows = get_tasks(tag=tag, q=q, sender=sender, limit=200)

    def h(s):  # tiny escape
        return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    filters = f"""<div style="margin:8px 0;">
      Filters: tag={h(tag) or '*'} | q={h(q) or '*'} | sender={h(sender) or '*'}
    </div>"""
    th = (
        "<tr><th>ID</th><th>Time</th><th>Sender</th><th>Tag</th>"
        "<th>Status</th><th>Order State</th><th>Text</th></tr>"
    )
    trs = []
    for r in rows:
        trs.append(
            f"<tr><td>{r['id']}</td><td>{r['ts']}</td><td>{h(r['sender'])}</td>"
            f"<td>{h(r.get('tag') or '')}</td>"
            f"<td>{h(r.get('status') or '')}</td>"
            f"<td>{h(r.get('order_state') or '')}</td>"
            f"<td>{h(r['text'])}</td></tr>"
        )

    body = f"""
    <html><head><title>HubFlo Admin</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}}
      table{{border-collapse:collapse;width:100%}}
      th,td{{border:1px solid #ddd;padding:6px;font-size:13px}}
      th{{background:#f2f2f2;text-align:left}}
    </style></head><body>
    <h2>HubFlo Admin</h2>
    {filters}
    <table>{th}{''.join(trs)}</table>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")

@app.route("/admin/tasks", methods=["GET"])
def api_tasks():
    if not _check_admin(): return _auth_fail()
    tag = request.args.get("tag") or None
    q = request.args.get("q") or None
    sender = request.args.get("sender") or None
    limit = int(request.args.get("limit", "200"))
    return jsonify(get_tasks(tag=tag, q=q, sender=sender, limit=limit))

@app.route("/admin/summary", methods=["GET"])
def api_summary():
    if not _check_admin(): return _auth_fail()
    return jsonify(get_summary())

# status transitions
@app.route("/admin/mark_done", methods=["POST"])
def api_mark_done():
    if not _check_admin(): return _auth_fail()
    tid = int(request.args.get("id", "0"))
    return jsonify(mark_done(tid, actor="admin") or {"error": "not found"})

@app.route("/admin/approve", methods=["POST"])
def api_approve():
    if not _check_admin(): return _auth_fail()
    tid = int(request.args.get("id", "0"))
    return jsonify(approve_task(tid, actor="admin") or {"error": "not found"})

@app.route("/admin/reject", methods=["POST"])
def api_reject():
    if not _check_admin(): return _auth_fail()
    tid = int(request.args.get("id", "0"))
    rework = request.args.get("rework", "1") != "0"
    return jsonify(reject_task(tid, rework=rework, actor="admin") or {"error": "not found"})

@app.route("/admin/revoke", methods=["POST"])
def api_revoke():
    if not _check_admin(): return _auth_fail()
    tid = int(request.args.get("id", "0"))
    return jsonify(revoke_last(tid, actor="admin") or {"error": "not found"})

@app.route("/admin/order_state", methods=["POST"])
def api_order_state():
    if not _check_admin(): return _auth_fail()
    tid = int(request.args.get("id", "0"))
    state = request.args.get("state", "").strip().lower()
    if state not in ORDER_LIFECYCLE_STATES:
        return jsonify({"error": "invalid state", "allowed": ORDER_LIFECYCLE_STATES}), 400
    return jsonify(set_order_state(tid, state, actor="admin") or {"error": "not found"})

@app.route("/admin/accuracy", methods=["GET"])
def api_accuracy():
    if not _check_admin(): return _auth_fail()
    name = request.args.get("subcontractor", "")
    if not name:
        return jsonify({"error": "missing subcontractor"}), 400
    return jsonify(subcontractor_accuracy(name))

# meetings (phase-1)
@app.route("/admin/meeting/create", methods=["POST"])
def api_meeting_create():
    if not _check_admin(): return _auth_fail()
    title = request.args.get("title", "Site Meeting")
    project_code = request.args.get("project") or None
    subcontractor_name = request.args.get("subcontractor") or None
    site_name = request.args.get("site") or None
    scheduled_for = request.args.get("when") or None
    task_ids = request.args.get("tasks") or ""        # "1,2,3"
    if scheduled_for:
        try:
            scheduled_for = dt.datetime.fromisoformat(scheduled_for)
        except Exception:
            scheduled_for = None
    ids = []
    for t in (task_ids.split(",") if task_ids else []):
        t = t.strip()
        if t.isdigit(): ids.append(int(t))
    m = create_meeting(
        title=title, project_code=project_code, subcontractor_name=subcontractor_name,
        site_name=site_name, scheduled_for=scheduled_for, task_ids=ids, created_by="admin"
    )
    return jsonify(m)

@app.route("/admin/meeting/start", methods=["POST"])
def api_meeting_start():
    if not _check_admin(): return _auth_fail()
    mid = int(request.args.get("id", "0"))
    m = start_meeting(mid, actor="admin")
    return jsonify(m or {"error": "not found"})

@app.route("/admin/meeting/close", methods=["POST"])
def api_meeting_close():
    if not _check_admin(): return _auth_fail()
    mid = int(request.args.get("id", "0"))
    m = close_meeting(mid, actor="admin")
    return jsonify(m or {"error": "not found"})

# -----------------------------------------------------------------------------
# Run (Render launches via `python app.py`)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)