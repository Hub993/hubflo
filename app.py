# app.py — HubFlo v4
# Includes admin checklist endpoints for testing:
#  - /admin/set_due?id=&due=YYYY-MM-DD[THH:MM:SS]
#  - /admin/set_started?id=&ts=YYYY-MM-DDTHH:MM:SS
#  - /admin/run_escalations
#  - /admin/meeting_start?meeting_id=
#  - /admin/meeting_close?meeting_id=
#  - /admin/meetings
#  - /admin/digest_preview?when=morning|evening
#  - /admin/storage_status
# View supports &project_code= and &q= search; summary unchanged.
#
# Outbound WA replies are attempted via 360dialog if D360 key present (sandbox will 401).

import os
import json
import logging
import datetime as dt
from typing import Tuple, Optional

from flask import Flask, request, jsonify, Response
import requests

from storage import (
    init_db, create_task, get_tasks, get_summary,
    mark_done, approve_task, reject_task, subcontractor_accuracy,
    set_due, set_started,
    create_meeting, get_meetings, set_meeting_started, set_meeting_closed,
    run_escalations, digest_preview, storage_status
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# ---------------------------------------------------------------------------
# Env/config
# ---------------------------------------------------------------------------

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

# Boot DB
init_db()

# ---------------------------------------------------------------------------
# Classifiers
# ---------------------------------------------------------------------------

ORDER_PREFIXES = ("order", "purchase", "procure", "buy")
CHANGE_PREFIXES = ("change", "variation", "revise", "amend", "adjust")
TASK_PREFIXES  = ("task", "todo", "to-do", "install", "fix", "inspect", "lay", "build", "schedule", "meet")

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

    # explicit prefixes
    for p in ORDER_PREFIXES:
        if t.startswith(p + " "):
            return "order"
    for p in CHANGE_PREFIXES:
        if t.startswith(p + " "):
            return "change"
    for p in TASK_PREFIXES:
        if t.startswith(p + " "):
            return "task"

    # hybrid heuristic: quantity + material
    if any(u in t for u in ["m ", "meter", "metre", "roll", "cable", "conduit"]) and any(ch.isdigit() for ch in t):
        return "order"

    # urgency
    if any(w in t for w in ["urgent", "asap", "immediately"]):
        return "urgent"

    return None

def looks_like_meeting(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    return t.startswith("meeting ") or t.startswith("meeting with ")

# ---------------------------------------------------------------------------
# WA send (best-effort; sandbox yields 401)
# ---------------------------------------------------------------------------

def send_whatsapp_text(phone_id: str, to: str, body: str) -> Tuple[bool, dict]:
    if not (D360_KEY and phone_id and to and body):
        log.warning("send_whatsapp_text skipped (missing key/to/body)")
        return False, {}
    headers = {
        "D360-API-KEY": D360_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "to": to,
        "type": "text",
        "text": {"body": body},
    }
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

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    return "HubFlo service running", 200

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
        project_code = None
        subcontractor_name = None

        if mtype == "text":
            text = (m.get("text") or {}).get("body")
        elif mtype in ("image", "document", "audio", "video"):
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

        # Meeting capture (lightweight)
        if looks_like_meeting(text or ""):
            title = (text or "").strip()
            meeting = create_meeting(title=title, participant=None, project_code=None, scheduled_at=None)
            log.info("MEETING_CREATED id=%s", meeting["id"])

        row = create_task(
            sender=sender, text=text or "", tag=tag,
            attachment=attachment, subcontractor_name=subcontractor_name,
            project_code=project_code
        )
        created_ids.append(row["id"])
        log.info("TASK_CREATED id=%s", row["id"])

        # Best-effort auto-replies (will 401 in sandbox)
        reply = None
        if tag == "order":
            reply = "Order noted. We’ll track and keep you updated."
        elif tag == "change":
            reply = "Change request logged. Awaiting quotes/approval steps."
        elif tag == "task":
            reply = "Task created. We’ll remind before start and near due."
        elif tag == "urgent":
            reply = "Urgent item logged. We’ll escalate immediately."

        if reply:
            ok, _ = send_whatsapp_text(phone_id=phone_id, to=sender, body=reply)
            log.info("AUTO_REPLY status=%s", ok)

    return ("", 200)

# ---------------------------------------------------------------------------
# Admin + API
# ---------------------------------------------------------------------------

def _auth_fail():
    return Response("Unauthorized", 401)

def _need_token():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return None
    return token

@app.route("/admin/view", methods=["GET"])
def admin_view():
    if _need_token() is None:
        return _auth_fail()

    tag = request.args.get("tag") or None
    q = request.args.get("q") or None
    sender = request.args.get("sender") or None
    project_code = request.args.get("project_code") or None
    rows = get_tasks(tag=tag, q=q, sender=sender, project_code=project_code, limit=200)

    def h(s):
        return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    filters = f"""<div style="margin:8px 0;">
      Filters: tag={h(tag) or '*'} | q={h(q) or '*'} | sender={h(sender) or '*'} | project_code={h(project_code) or '*'}
    </div>"""
    th = "<tr><th>ID</th><th>Time</th><th>Sender</th><th>Tag</th><th>Status</th><th>Due</th><th>Text</th></tr>"
    trs = []
    for r in rows:
        trs.append(
            f"<tr><td>{r['id']}</td><td>{r['ts']}</td><td>{h(r['sender'])}</td>"
            f"<td>{h(r.get('tag') or '')}</td><td>{h(r.get('status') or '')}</td>"
            f"<td>{h(r.get('due_date') or '')}</td><td>{h(r['text'])}</td></tr>"
        )
    body = f"""
    <html><head><title>HubFlo Admin</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}}
      table{{border-collapse:collapse;width:100%}}
      th,td{{border:1px solid #ddd;padding:6px;font-size:13px}}
      th{{background:#f2f2f2;text-align:left}}
      .note{{margin:6px 0;color:#444}}
    </style></head><body>
    <h2>HubFlo Admin</h2>
    {filters}
    <div class="note">Try: &amp;tag=order, &amp;q=conduit, &amp;project_code=P-100</div>
    <table>{th}{''.join(trs)}</table>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")

@app.route("/admin/tasks", methods=["GET"])
def api_tasks():
    if _need_token() is None:
        return _auth_fail()
    tag = request.args.get("tag") or None
    q = request.args.get("q") or None
    sender = request.args.get("sender") or None
    project_code = request.args.get("project_code") or None
    limit = int(request.args.get("limit", "100"))
    return jsonify(get_tasks(tag=tag, q=q, sender=sender, project_code=project_code, limit=limit))

@app.route("/admin/summary", methods=["GET"])
def api_summary():
    if _need_token() is None:
        return _auth_fail()
    return jsonify(get_summary())

@app.route("/admin/mark_done", methods=["POST"])
def api_mark_done():
    if _need_token() is None:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    return jsonify(mark_done(tid) or {"error": "not found"})

@app.route("/admin/approve", methods=["POST"])
def api_approve():
    if _need_token() is None:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    return jsonify(approve_task(tid) or {"error": "not found"})

@app.route("/admin/reject", methods=["POST"])
def api_reject():
    if _need_token() is None:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    rework = request.args.get("rework", "1") != "0"
    return jsonify(reject_task(tid, rework=rework) or {"error": "not found"})

# ---- v4: dates & progress ---------------------------------------------------

def _parse_dt(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    # Accept YYYY-MM-DD or full ISO
    try:
        if "T" in value:
            return dt.datetime.fromisoformat(value)
        return dt.datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        return None

@app.route("/admin/set_due", methods=["POST", "GET"])
def api_set_due():
    if _need_token() is None:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    due = _parse_dt(request.args.get("due", ""))
    if not due:
        return jsonify({"error": "bad due format"}), 400
    return jsonify(set_due(tid, due) or {"error": "not found"})

@app.route("/admin/set_started", methods=["POST", "GET"])
def api_set_started():
    if _need_token() is None:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    ts = _parse_dt(request.args.get("ts", ""))
    ts = ts or dt.datetime.utcnow()
    return jsonify(set_started(tid, ts) or {"error": "not found"})

@app.route("/admin/run_escalations", methods=["GET"])
def api_run_escalations():
    if _need_token() is None:
        return _auth_fail()
    data = run_escalations()
    # In sandbox we only preview; in prod we'd iterate and send WA messages.
    return jsonify(data)

# ---- v4: meetings -----------------------------------------------------------

@app.route("/admin/meetings", methods=["GET"])
def api_meetings():
    if _need_token() is None:
        return _auth_fail()
    return jsonify(get_meetings())

@app.route("/admin/meeting_start", methods=["POST", "GET"])
def api_meeting_start():
    if _need_token() is None:
        return _auth_fail()
    mid = request.args.get("meeting_id", "")
    if mid.lower() == "latest":
        ms = get_meetings(limit=1)
        if not ms:
            return jsonify({"error": "no meetings"}), 404
        mid = ms[0]["id"]
    else:
        mid = int(mid or "0")
    return jsonify(set_meeting_started(int(mid)) or {"error": "not found"})

@app.route("/admin/meeting_close", methods=["POST", "GET"])
def api_meeting_close():
    if _need_token() is None:
        return _auth_fail()
    mid = request.args.get("meeting_id", "")
    if mid.lower() == "latest":
        ms = get_meetings(limit=1)
        if not ms:
            return jsonify({"error": "no meetings"}), 404
        mid = ms[0]["id"]
    else:
        mid = int(mid or "0")
    return jsonify(set_meeting_closed(int(mid)) or {"error": "not found"})

# ---- v4: digests & storage telemetry ---------------------------------------

@app.route("/admin/digest_preview", methods=["GET"])
def api_digest_preview():
    if _need_token() is None:
        return _auth_fail()
    when = (request.args.get("when") or "morning").lower()
    return jsonify(digest_preview(when=when))

@app.route("/admin/storage_status", methods=["GET"])
def api_storage_status():
    if _need_token() is None:
        return _auth_fail()
    return jsonify(storage_status())

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)