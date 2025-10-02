import os, json, logging, requests
from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

D360_KEY = os.getenv("D360_KEY", "")
D360_SEND_URL = os.getenv("D360_SEND_URL", "https://waba.360dialog.io/v1/messages")

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

    if msg_body and sender:
        ok = send_whatsapp_text(sender, f"✅ Received: {msg_body}")
        app.logger.info(f"WHATSAPP_SEND status={ok}")
    return jsonify({"status":"ok"}), 200

def extract_text_and_sender(p):
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
    if not (D360_KEY and to_waid and body):
        app.logger.warning("send_whatsapp_text skipped (missing key/to/body)")
        return False
    payload = {"to": str(to_waid), "type": "text", "text": {"body": body}}
    # Try both header schemes; some accounts require Authorization: Bearer
    headers_primary = {
        "Content-Type": "application/json",
        "D360-API-KEY": D360_KEY
    }
    headers_bearer = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {D360_KEY}"
    }
    try:
        r = requests.post(D360_SEND_URL, headers=headers_primary, json=payload, timeout=12)
        app.logger.info(f"WHATSAPP_SEND status_code={r.status_code} body={r.text[:400]}")
        if 200 <= r.status_code < 300:
            return True
        # fallback try with Bearer
        r2 = requests.post(D360_SEND_URL, headers=headers_bearer, json=payload, timeout=12)
        app.logger.info(f"WHATSAPP_SEND (bearer) status_code={r2.status_code} body={r2.text[:400]}")
        return 200 <= r2.status_code < 300
    except Exception as e:
        app.logger.error(f"WHATSAPP_SEND error: {e}")
        return False

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)