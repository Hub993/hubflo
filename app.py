import os
import json
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# Env variables
D360_KEY = os.getenv("D360_KEY", "")
SEND_URL = os.getenv("D360_SEND_URL", "https://waba-v2.360dialog.io/messages")

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        payload = request.get_json(force=True)
        app.logger.info("Inbound webhook hit")
        app.logger.info(f"RAW_PAYLOAD= {json.dumps(payload)[:2000]}")

        # Try extracting message body
        msg_body = None
        sender = None
        if payload and "messages" in payload:
            messages = payload.get("messages", [])
            if messages and "text" in messages[0]:
                msg_body = messages[0]["text"].get("body")
                sender = messages[0].get("from")

        app.logger.info(f"MSG_BODY= {msg_body} SENDER= {sender}")

        # Send a reply back if we have both
        if msg_body and sender:
            headers = {
                "Content-Type": "application/json",
                "D360-API-KEY": D360_KEY
            }
            data = {
                "to": sender,
                "type": "text",
                "text": {"body": f"✅ Received: {msg_body}"}
            }
            r = requests.post(SEND_URL, headers=headers, json=data)
            app.logger.info(f"WHATSAPP_SEND status={r.status_code} body={r.text}")

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        app.logger.error(f"Webhook error: {str(e)}")
        return jsonify({"error": "bad request"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)