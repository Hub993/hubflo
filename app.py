import os, json, logging
from flask import Flask, request, jsonify, abort
from storage import init_db, create_task, get_tasks
import requests
from collections import Counter

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# --- Environment (outbound still gated in sandbox) ---
META_TOKEN = os.getenv("META_TOKEN", "")
D360_KEY = os.getenv("D360_KEY", "")
D360_SEND_URL = os.getenv("D360_SEND_URL", "https://waba.360dialog.io/v1/messages")
ADMIN_TOKEN = os.getenv("HUBFLO_ADMIN_TOKEN", "")

# DB ready
init_db()

# ---------- Helpers ----------

def _check_admin():
    token = request.args.get("token")
    if token != ADMIN_TOKEN:
        abort(401)

def classify_text_strict(txt: str):
    """
    Strict-but-friendly tagging:
    - Prefixes accepted with or without colon: 'Order:' or 'Order ', etc.
    - Hashtag overrides anywhere: #order, #change, #urgent, #task
    - NO quantity/material heuristics (e.g., '20m conduit' stays untagged) to avoid false positives.
    """
    t = (txt or "").lower().strip()

    # Hashtag overrides (take precedence)
    if "#order" in t:
        return "order"
    if "#change" in t:
        return "change"
    if "#urgent" in t:
        return "urgent"
    if "#task" in t:
        return "task"

    # Accept prefixes with or without ':' (e.g., "order 25m...", "order: 25m...")
    def starts_with(word):
        return t.startswith(f"{word}:") or t.startswith(f"{word} ")

    # Orders (explicit only)
    if starts_with("order") or starts_with("po") or starts_with("purchase"):
        return "order"

    # Changes / variations (explicit)
    if starts_with("change") or t.startswith("variation:") or " variation:" in t or t.startswith("revise scope:") or t.startswith("amend:"):
        return "change"

    # Urgency (explicit cue words at start)
    if starts_with("urgent") or t.startswith("asap") or t.startswith("immediately") or t.startswith("now!"):
        return "urgent"

    # Tasks (explicit cues)
    if starts_with("task") or starts_with("install") or starts_with("schedule"):
        return "task"

    return None

def extract_text_sender_phoneid(p):
    # 360dialog direct format
    msgs = p.get("messages")
    if msgs:
        m0 = msgs[0]
        sender = m0.get("from")
        t = m0.get("type")
        if t == "text":
            txt = (m0.get("text") or {}).get("body")
        elif "button" in m0:
            txt = (m0.get("button") or {}).get("text")
        elif t == "interactive":
            inter = m0.get("interactive") or {}
            txt = ((inter.get("button_reply") or {}).get("title")
                   or (inter.get("list_reply") or {}).get("title"))
        else:
            txt = None
        return (txt, sender, None)

    # Meta relay format (what your logs show)
    try:
        entry = (p.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        msgs = value.get("messages") or []
        metadata = value.get("metadata") or {}
        phone_id = metadata.get("phone_number_id")
        if msgs:
            m0 = msgs[0]
            sender = m0.get("from")
            t = m0.get("type")
            if t == "text":
                txt = (m0.get("text") or {}).get("body")
            elif "button" in m0:
                txt = (m0.get("button") or {}).get("text")
            elif t == "interactive":
                inter = m0.get("interactive") or {}
                txt = ((inter.get("button_reply") or {}).get("title")
                       or (inter.get("list_reply") or {}).get("title"))
            else:
                txt = None
            return (txt, sender, phone_id)
    except Exception:
        pass

    return (None, None, None)

def send_whatsapp_text(to, body, phone_id=None):
    # Meta Cloud (will require META_TOKEN; optional)
    if META_TOKEN and phone_id:
        try:
            url = f"https://graph.facebook.com/v20.0/{phone_id}/messages"
            headers = {"Authorization": f"Bearer {META_TOKEN}", "Content-Type": "application/json"}
            data = {"messaging_product": "whatsapp", "to": to, "text": {"body": body}}
            r = requests.post(url, headers=headers, json=data, timeout=15)
            app.logger.info(f"META_SEND status_code={r.status_code} body={r.text}")
            return 200 <= r.status_code < 300
        except Exception as e:
            app.logger.error(f"META_SEND error={e}")

    # 360dialog (sandbox likely 401; fine for now)
    if D360_KEY:
        try:
            headers = {"D360-API-KEY": D360_KEY, "Content-Type": "application/json"}
            data = {"to": to, "type": "text", "text": {"body": body}}
            r = requests.post(D360_SEND_URL, headers=headers, json=data, timeout=15)
            app.logger.info(f"D360_SEND status_code={r.status_code} body={r.text}")
            return 200 <= r.status_code < 300
        except Exception as e:
            app.logger.error(f"D360_SEND error={e}")

    app.logger.warning("AUTO_REPLY skipped (no valid provider creds: META_TOKEN+phone_id or D360_KEY)")
    return False

# ---------- Routes ----------

@app.route("/")
def index():
    return "HUBFLO service running", 200

@app.route("/webhook", methods=["POST"])
@app.route("/whatsapp/webhook", methods=["POST"])
def webhook():
    app.logger.info("Inbound webhook hit")
    payload = request.get_json(force=True, silent=True) or {}
    app.logger.info("RAW_PAYLOAD=" + json.dumps(payload)[:3000])

    text, sender, phone_id = extract_text_sender_phoneid(payload)
    app.logger.info(f"MSG_BODY={text} SENDER={sender} PHONE_ID={phone_id}")

    tag = classify_text_strict(text) if text else None
    if tag:
        app.logger.info(f"TAG={tag}")

    task_id = None
    if text and sender:
        try:
            task_id = create_task({"text": text, "sender": sender, "tag": tag})
            app.logger.info(f"TASK_CREATED id={task_id}")
        except Exception as e:
            app.logger.error(f"TASK_CREATE_ERROR: {e}")

        # Optional auto-reply (sandbox 401 is expected)
        reply_body = f"✅ Saved (tag={tag or 'none'}): {text}"
        sent_ok = send_whatsapp_text(sender, reply_body, phone_id=phone_id)
        app.logger.info(f"AUTO_REPLY status={sent_ok}")

    return jsonify(ok=True, task_id=task_id, tag=tag), 200

# ----- Admin (read-only) -----

@app.route("/admin/debug", methods=["GET"])
def admin_debug():
    _check_admin()
    return jsonify(get_tasks()), 200

@app.route("/admin/search", methods=["GET"])
def admin_search():
    _check_admin()
    tag = (request.args.get("tag") or "").strip().lower()
    q = (request.args.get("q") or "").strip().lower()
    rows = get_tasks()
    if tag:
        rows = [r for r in rows if (r.get("tag") or "") == tag]
    if q:
        rows = [r for r in rows if q in (r.get("text") or "").lower()]
    return jsonify(rows), 200

@app.route("/admin/summary", methods=["GET"])
def admin_summary():
    _check_admin()
    rows = get_tasks()
    counts = Counter((r.get("tag") or "none") for r in rows)
    latest = rows[:10]
    return jsonify({"counts_by_tag": counts, "latest": latest}), 200

# ---------- Main ----------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)