import os, json, logging
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from storage import init_db, get_tasks

load_dotenv()

app = Flask(__name__)
init_db()

# ✅ show our INFO logs in Render
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

    def extract_text(p):
        # 360dialog direct
        msgs = p.get("messages")
        if msgs:
            m0 = msgs[0]
            t = m0.get("type")
            if t == "text":
                return (m0.get("text") or {}).get("body")
            if t == "button":
                return (m0.get("button") or {}).get("text")
            if t == "interactive":
                inter = m0.get("interactive") or {}
                return ((inter.get("button_reply") or {}).get("title")
                        or (inter.get("list_reply") or {}).get("title"))
        # Meta relay
        try:
            entry = (p.get("entry") or [])[0]
            changes = (entry.get("changes") or [])[0]
            value = changes.get("value") or {}
            msgs = value.get("messages") or []
            if msgs:
                m0 = msgs[0]
                t = m0.get("type")
                if t == "text":
                    return (m0.get("text") or {}).get("body")
                if "button" in m0:
                    return (m0.get("button") or {}).get("text")
                if t == "interactive":
                    inter = m0.get("interactive") or {}
                    return ((inter.get("button_reply") or {}).get("title")
                            or (inter.get("list_reply") or {}).get("title"))
        except Exception:
            pass
        return None

    msg_body = extract_text(payload)
    app.logger.info(f"MSG_BODY={msg_body}")
    print(f"MSG_BODY_PRINT={msg_body}")  # extra safety

    return jsonify({"status": "ok"}), 200

@app.route("/admin/debug", methods=["GET"])
def admin_debug():
    token = request.args.get("token")
    if token != os.getenv("HUBFLO_ADMIN_TOKEN"):
        return "Unauthorized", 401
    return jsonify(get_tasks()), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)