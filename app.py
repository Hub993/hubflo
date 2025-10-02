import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import logging

from storage import init_db, get_tasks
from parse import parse_text

load_dotenv()

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# Initialize the database
init_db()

@app.route("/", methods=["GET"])
def home():
    return "Hubflo service is running", 200

@app.route("/webhook", methods=["POST"])
@app.route("/whatsapp/webhook", methods=["POST"])  # alias for 360dialog
def webhook():
    app.logger.info("Inbound webhook hit")
    payload = request.get_json(force=True, silent=True)

    if not payload:
        app.logger.warning("No JSON payload received")
        return jsonify({"status": "no payload"}), 400

    # Extract message body safely
    msg_body = None
    try:
        if "messages" in payload and len(payload["messages"]) > 0:
            msg = payload["messages"][0]
            if msg.get("type") == "text":
                msg_body = msg["text"]["body"]
            elif msg.get("type") == "button":
                msg_body = msg["button"]["text"]
            elif msg.get("type") == "interactive":
                msg_body = msg["interactive"]["button_reply"]["title"]
    except Exception as e:
        app.logger.error(f"Error parsing payload: {e}")

    app.logger.info(f"MSG_BODY={msg_body}")

    # TODO: future steps → save to DB, confirmation reply, rules
    return jsonify({"status": "received"}), 200

@app.route("/debug/tasks", methods=["GET"])
def debug_tasks():
    tasks = get_tasks()
    return jsonify(tasks), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)