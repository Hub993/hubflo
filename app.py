import os
from flask import Flask, request, jsonify, abort
from dotenv import load_dotenv
load_dotenv()
from storage import init_db, get_tasks
from parse import parse_text, apply_action
import logging

ADMIN_TOKEN = os.environ.get("HUBFLO_ADMIN_TOKEN","")

app = Flask(__name__)
init_db()

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

def check_auth():
 # Accept either header OR ?token=
 token = request.headers.get("Authorization", "")
 if token.lower().startswith("bearer "):
   token = token[7:]
 else:
   token = request.args.get("token", "")
 if not ADMIN_TOKEN or token != ADMIN_TOKEN:
   abort(401)

@app.route("/health")
def health():
 return jsonify(ok=True)

@app.route("/debug/tasks")
def debug_tasks():
 check_auth()
 ids = request.args.get("ids","")
 id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
 return jsonify(get_tasks(id_list))

@app.route("/webhook", methods=["POST"])
def webhook():
 app.logger.info("Inbound webhook hit")
 print("Inbound webhook hit")
 data = request.get_json(silent=True) or {}
 try:
   msg = data["entry"][0]["changes"][0]["value"]["messages"][0]
 except Exception:
   return jsonify(ok=True, ignored=True)
 text = (msg.get("text",{}) or {}).get("body","").strip()
 action = parse_text(text) if text else None
 if not action:
   return jsonify(ok=True, matched=False)
 ok = apply_action(action)
 return jsonify(ok=ok, action=action)

@app.route("/whatsapp/webhook", methods=["POST"])
def whatsapp_webhook():
   return webhook()

# Stubs: will flesh out after templates
@app.route("/daily", methods=["POST"])
def daily():
 check_auth()
 return jsonify(ok=True, sent=1)

@app.route("/send", methods=["POST"])
def send():
 check_auth()
 return jsonify(ok=True)

@app.route("/nudge", methods=["POST"])
def nudge():
 check_auth()
 return jsonify(ok=True)

if __name__ == "__main__":
 app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))