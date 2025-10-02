import os, json, logging, requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# Logging visible on Render
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# WhatsApp send config (uses env)
D360_KEY = os.getenv("D360_KEY", "")
D360_SEND_URL = os.getenv("D360_SEND_URL", "https://waba-v2.360dialog.io/messages")

@app.route("/")
def index():
    return "HUBFLO service running", 200

@app.route("/webhook", methods=["POST"])
@app.route("/whatsapp/webhook", methods=["POST"])
def webhook():
    app.logger.info("Inbound webhook hit")
    payload = request.get_json(force=True, silent=True) or {}
    app.logger.info("RAW_PAYLOAD=" + json.dumps(payload)[:3000])

    msg_body, sender = extract_text_and_sender(payload)
    app.logger.info(f"MSG_BODY={msg_body} SENDER={sender}")

    # If we have text and a sender, echo a confirmation back
    if msg_body and sender:
        sent = send_whatsapp_text(sender, f"✅ Received: {msg_body}")
        app.logger.info(f"WHATSAPP_SEND status={sent}")

    return jsonify({"status": "ok"}), 200

def extract_text_and_sender(p):
    # 1) 360dialog direct
    msgs = p.get("messages")
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

    # 2) Meta relay (your current payload)
    try:
        entry = (p.get("entry") or [])[0]
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
    """Send a WhatsApp text via 360dialog."""
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)