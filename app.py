import os, json, logging, requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Meta Cloud API token (Render → Environment must have META_TOKEN set)
META_TOKEN = os.getenv("META_TOKEN", "")

@app.route("/")
def index():
    return "HUBFLO service running", 200

@app.route("/webhook", methods=["POST"])
@app.route("/whatsapp/webhook", methods=["POST"])
def webhook():
    app.logger.info("Inbound webhook hit")
    payload = request.get_json(force=True, silent=True) or {}
    app.logger.info("RAW_PAYLOAD=" + json.dumps(payload)[:3000])

    text, sender, phone_id = extract_meta(payload)
    app.logger.info(f"MSG_BODY={text} SENDER={sender} PHONE_ID={phone_id}")

    if text and sender and phone_id and META_TOKEN:
        ok = send_meta_reply(phone_id, sender, f"✅ Received: {text}")
        app.logger.info(f"WHATSAPP_SEND status={ok}")
    else:
        if not META_TOKEN:
            app.logger.warning("Missing META_TOKEN; cannot send reply")
    return jsonify({"status": "ok"}), 200

def extract_meta(p):
    """Return (text, sender_waid, phone_number_id) from Meta relay payload."""
    try:
        entry = (p.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        phone_id = (value.get("metadata") or {}).get("phone_number_id")
        msgs = value.get("messages") or []
        if msgs:
            m0 = msgs[0]
            sender = m0.get("from")
            t = m0.get("type")
            if t == "text":
                return ((m0.get("text") or {}).get("body"), sender, phone_id)
            if "button" in m0:
                return ((m0.get("button") or {}).get("text"), sender, phone_id)
            if t == "interactive":
                inter = m0.get("interactive") or {}
                txt = ((inter.get("button_reply") or {}).get("title")
                       or (inter.get("list_reply") or {}).get("title"))
                return (txt, sender, phone_id)
    except Exception:
        pass
    return (None, None, None)

def send_meta_reply(phone_id, to_waid, body):
    """Send via Meta Graph: POST https://graph.facebook.com/v20.0/{phone_id}/messages"""
    url = f"https://graph.facebook.com/v20.0/{phone_id}/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {META_TOKEN}",
    }
    data = {
        "messaging_product": "whatsapp",
        "to": str(to_waid),
        "type": "text",
        "text": {"body": body},
    }
    try:
        r = requests.post(url, headers=headers, json=data, timeout=12)
        app.logger.info(f"META_SEND status_code={r.status_code} body={r.text[:400]}")
        return 200 <= r.status_code < 300
    except Exception as e:
        app.logger.error(f"META_SEND error: {e}")
        return False

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)