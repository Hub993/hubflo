import os, json, logging, requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# --- optional storage imports (DB off by default) ---
storage_create_task = None
try:
    from storage import init_db, get_tasks, create_task as storage_create_task
except Exception:
    from storage import init_db, get_tasks

load_dotenv()

# --- feature toggles (flip in Render → Environment) ---
FEATURE_REPLY  = os.getenv("FEATURE_REPLY",  "1") == "1"  # echo back to sender
FEATURE_DB     = os.getenv("FEATURE_DB",     "0") == "1"  # save task when create_task exists
FEATURE_RULES  = os.getenv("FEATURE_RULES",  "0") == "1"  # simple rules (e.g., "urgent")

# --- WhatsApp send config ---
D360_KEY      = os.getenv("D360_KEY", "")
D360_SEND_URL = os.getenv("D360_SEND_URL", "https://waba.360dialog.io/v1/messages")

app = Flask(__name__)
init_db()

# logging visible in Render
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

@app.route("/")
def index():
    return "HUBFLO service running", 200

# ---------- helpers ----------
def extract_text_and_sender(payload):
    """Return (text, sender_waid). Supports 360dialog direct and Meta relay shapes."""
    # 360dialog direct
    msgs = payload.get("messages")
    if msgs:
        m0 = msgs[0]
        sender = m0.get("from")
        t = m0.get("type")
        if t == "text":
            return ((m0.get("text") or {}).get("body"), sender)
        if t == "button":
            return ((m0.get("button") or {}).get("text"), sender)
        if t == "interactive":
            inter = m0.get("interactive") or {}
            txt = ((inter.get("button_reply") or {}).get("title")
                   or (inter.get("list_reply") or {}).get("title"))
            return (txt, sender)
    # Meta relay shape
    try:
        entry = (payload.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        msgs = value.get("messages") or []
        if msgs:
            m0 = msgs[0]
            sender = m0.get("from")
            t = m0.get("type")
            if t == "text":
                return ((m0.get("text") or {}).get("body"), sender)
            if "button" in m0:
                return ((m0.get("button") or {}).get("text"), sender)
            if t == "interactive":
                inter = m0.get("interactive") or {}
                txt = ((inter.get("button_reply") or {}).get("title")
                       or (inter.get("list_reply") or {}).get("title"))
                return (txt, sender)
    except Exception:
        pass
    return (None, None)

def send_whatsapp_text(to_waid, body):
    if not (D360_KEY and to_waid and body):
        app.logger.warning("send_whatsapp_text skipped (missing key/to/body)")
        return False
    headers = {"D360-API-KEY": D360_KEY, "Content-Type": "application/json"}
    payload = {"to": str(to_waid), "type": "text", "text": {"body": body}}
    try:
        r = requests.post(D360_SEND_URL, headers=headers, json=payload, timeout=12)
        app.logger.info(f"WHATSAPP_SEND status={r.status_code} body={r.text[:400]}")
        return 200 <= r.status_code < 300
    except Exception as e:
        app.logger.error(f"WHATSAPP_SEND error: {e}")
        return False

def apply_rules(text, sender):
    """Very simple rule: if 'urgent' in text, send alert."""
    fired = []
    if not (FEATURE_RULES and text):
        return fired
    if "urgent" in text.lower():
        alert = f"⚠️ URGENT detected: {text}"
        send_whatsapp_text(sender, alert)
        fired.append("urgent")
    return fired

# ---------- webhook ----------
@app.route("/webhook", methods=["POST"])
@app.route("/whatsapp/webhook", methods=["POST"])  # alias
def webhook():
    app.logger.info("Inbound webhook hit")
    payload = request.get_json(force=True, silent=True) or {}
    app.logger.info("RAW_PAYLOAD=" + json.dumps(payload)[:3000])

    msg_body, sender = extract_text_and_sender(payload)
    app.logger.info(f"MSG_BODY={msg_body} SENDER={sender}")

    if not msg_body:
        return jsonify(ok=True, matched=False, reason="no_text"), 200

    # DB save (only if toggled on and create_task exists)
    task_id, db_status = None, "skipped"
    if FEATURE_DB and storage_create_task:
        try:
            task_id = storage_create_task({"text": msg_body, "sender": sender})
            db_status = "saved"
        except Exception as e:
            db_status = f"error:{e}"

    # Rules
    rules_fired = apply_rules(msg_body, sender)

    # Confirmation reply
    reply = f"✅ Received: {msg_body}" if task_id is None else f"✅ Task #{task_id} created"
    echo_ok = False
    if FEATURE_REPLY and sender:
        echo_ok = send_whatsapp_text(sender, reply)

    return jsonify(ok=True, echo_sent=bool(echo_ok), task_id=task_id,
                   db=db_status, rules=rules_fired), 200

# ---------- admin ----------
@app.route("/admin/debug", methods=["GET"])
def admin_debug():
    token = request.args.get("token")
    if token != os.getenv("HUBFLO_ADMIN_TOKEN"):
        return "Unauthorized", 401
    return jsonify(get_tasks()), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)