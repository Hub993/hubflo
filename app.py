# app.py
import os
import json
import logging
import datetime as dt
from flask import Flask, request, jsonify, Response
import requests

from storage import (
    init_db, create_task, get_tasks, get_summary,
    mark_done, approve_task, reject_task, subcontractor_accuracy
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# --- Env config --------------------------------------------------------------

ADMIN_TOKEN = os.environ.get("HUBFLO_ADMIN_TOKEN", "").strip()

# 360dialog API token: support both names to avoid future drift
D360_KEY = (
    os.environ.get("DIALOG360_API_KEY")
    or os.environ.get("Dialog360_API_Key")
    or os.environ.get("D360_KEY")
    or os.environ.get("D360_key")
    or ""
).strip()

# For local dev you can set a fixed phone id, but we prefer using payload's phone_id.
DEFAULT_PHONE_ID = os.environ.get("BOUND_NUMBER", "").strip()

WHATSAPP_BASE = "https://waba.360dialog.io/v1/messages"  # 360dialog messages endpoint

# attachment constraints (mirror WhatsApp practical limits)
MAX_ATTACHMENT_MB = 16
ALLOWED_MIME_PREFIXES = ("image/", "application/pdf", "application/vnd.", "text/")

# --- Boot DB -----------------------------------------------------------------
init_db()

# --- Utilities ---------------------------------------------------------------

ORDER_PREFIXES = ("order", "purchase", "procure", "buy")
CHANGE_PREFIXES = ("change", "variation", "revise", "amend", "adjust")
TASK_PREFIXES  = ("task", "todo", "to-do", "install", "fix", "inspect", "lay", "build", "schedule")

HASHTAG_MAP = {
    "#order": "order",
    "#change": "change",
    "#task": "task",
    "#urgent": "urgent",
}

ORDER_LIFECYCLE_STATES = ["quoted", "pending_approval", "approved", "cancelled", "invoiced", "enacted"]

def classify_tag(text: str) -> str | None:
    if not text:
        return None
    t = text.strip().lower()

    # 1) hashtag override
    for h, tag in HASHTAG_MAP.items():
        if h in t:
            return tag

    # 2) keyword prefix at start
    for p in ORDER_PREFIXES:
        if t.startswith(p + " "):
            return "order"
    for p in CHANGE_PREFIXES:
        if t.startswith(p + " "):
            return "change"
    for p in TASK_PREFIXES:
        if t.startswith(p + " "):
            return "task"

    # 3) soft heuristic: contains quantity + material might be an order
    if any(u in t for u in ["m ", "meter", "metre", "roll", "cable", "conduit"]) and any(ch.isdigit() for ch in t):
        return "order"

    # 4) urgent words
    if any(w in t for w in ["urgent", "asap", "immediately"]):
        return "urgent"

    # 5) fallback none
    return None

def within_size_limit(size_bytes: int) -> bool:
    return size_bytes <= MAX_ATTACHMENT_MB * 1024 * 1024

def send_whatsapp_text(phone_id: str, to: str, body: str) -> tuple[bool, dict]:
    """
    360dialog send. In sandbox we expect 401 until a real number/token is used.
    """
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
        # Optional: "messaging_product": "whatsapp" — 360dialog infers
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

# --- Webhook -----------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    return "HubFlo service running", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    log.info("Inbound webhook hit")
    raw = request.get_json(silent=True) or {}
    log.info("RAW_PAYLOAD=%s", json.dumps(raw))

    # WhatsApp payload (Meta/360dialog) structure
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

    # Iterate messages (usually one)
    created_ids = []
    for m in msgs:
        sender = m.get("from") or sender
        mtype = m.get("type")
        text = None
        attachment = None

        if mtype == "text":
            text = (m.get("text") or {}).get("body")
        elif mtype in ("image", "document", "audio", "video"):
            # Store metadata only; actual media fetch requires Graph API download.
            mime = None
            name = None
            mid = m.get(mtype, {}).get("id")
            # Compose a placeholder URL we can later exchange via Graph API if authorized
            url = f"whatsapp_media://{mtype}/{mid}" if mid else None
            mime = m.get(mtype, {}).get("mime_type")
            name = m.get(mtype, {}).get("filename")
            # Size not in webhook; enforce at download time later. For now we accept metadata.
            attachment = {"url": url, "mime": mime, "name": name}
            # Some media include captions
            text = (m.get(mtype, {}) or {}).get("caption")

        tag = classify_tag(text or "")
        log.info("MSG_BODY=%s SENDER=%s PHONE_ID=%s", text, sender, phone_id)
        if tag:
            log.info("TAG=%s", tag)

        # Create task row
        row = create_task(sender=sender, text=text or "", tag=tag, attachment=attachment)
        created_ids.append(row["id"])
        log.info("TASK_CREATED id=%s", row["id"])

        # Auto-reply (gated by sandbox; safe to attempt)
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

# --- Admin & API -------------------------------------------------------------

def _auth_fail():
    return Response("Unauthorized", 401)

@app.route("/admin/view", methods=["GET"])
def admin_view():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return _auth_fail()

    tag = request.args.get("tag") or None
    q = request.args.get("q") or None
    sender = request.args.get("sender") or None
    rows = get_tasks(tag=tag, q=q, sender=sender, limit=100)

    # Simple HTML table for quick checks
    def h(s):  # tiny escape
        return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    filters = f"""<div style="margin:8px 0;">
      Filters: tag={h(tag) or '*'} | q={h(q) or '*'} | sender={h(sender) or '*'}
    </div>"""
    th = "<tr><th>ID</th><th>Time</th><th>Sender</th><th>Tag</th><th>Text</th></tr>"
    trs = []
    for r in rows:
        trs.append(
            f"<tr><td>{r['id']}</td><td>{r['ts']}</td><td>{h(r['sender'])}</td>"
            f"<td>{h(r.get('tag') or '')}</td><td>{h(r['text'])}</td></tr>"
        )
    body = f"""
    <html><head><title>HubFlo Admin</title>
    <style>
      body{{font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif;}}
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
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return _auth_fail()
    tag = request.args.get("tag") or None
    q = request.args.get("q") or None
    sender = request.args.get("sender") or None
    limit = int(request.args.get("limit", "100"))
    return jsonify(get_tasks(tag=tag, q=q, sender=sender, limit=limit))

@app.route("/admin/summary", methods=["GET"])
def api_summary():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return _auth_fail()
    return jsonify(get_summary())

@app.route("/admin/mark_done", methods=["POST"])
def api_mark_done():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    return jsonify(mark_done(tid) or {"error": "not found"})

@app.route("/admin/approve", methods=["POST"])
def api_approve():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    return jsonify(approve_task(tid) or {"error": "not found"})

@app.route("/admin/reject", methods=["POST"])
def api_reject():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return _auth_fail()
    tid = int(request.args.get("id", "0"))
    rework = request.args.get("rework", "1") != "0"
    return jsonify(reject_task(tid, rework=rework) or {"error": "not found"})

@app.route("/admin/accuracy", methods=["GET"])
def api_accuracy():
    token = request.args.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return _auth_fail()
    name = request.args.get("subcontractor", "")
    if not name:
        return jsonify({"error": "missing subcontractor"}), 400
    return jsonify(subcontractor_accuracy(name))

# --- Run (Render launches via `python app.py`) -------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)