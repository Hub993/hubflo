import os, json, logging
from flask import Flask, request, jsonify

app = Flask(__name__)

# ✅ show INFO logs on Render
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

@app.route("/")
def index():
    return "HUBFLO service running", 200

# ✅ webhook + alias
@app.route("/webhook", methods=["POST"])
@app.route("/whatsapp/webhook", methods=["POST"])
def webhook():
    app.logger.info("Inbound webhook hit")
    payload = request.get_json(force=True, silent=True) or {}
    app.logger.info("RAW_PAYLOAD=" + json.dumps(payload)[:3000])

    # Minimal extractor (360dialog direct)
    msg_body, sender = None, None
    msgs = payload.get("messages") or []
    if msgs:
        m0 = msgs[0]
        sender = m0.get("from")
        t = m0.get("type")
        if t == "text":
            msg_body = (m0.get("text") or {}).get("body")
        elif "button" in m0:
            msg_body = (m0.get("button") or {}).get("text")
        elif t == "interactive":
            inter = m0.get("interactive") or {}
            msg_body = ((inter.get("button_reply") or {}).get("title")
                        or (inter.get("list_reply") or {}).get("title"))

    app.logger.info(f"MSG_BODY={msg_body} SENDER={sender}")
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)