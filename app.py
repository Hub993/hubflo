import os, json, logging
from flask import Flask, request, jsonify
from storage import init_db, create_task, get_tasks

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Ensure DB table exists on boot
init_db()

@app.route("/")
def index():
    return "HUBFLO service running", 200

@app.route("/webhook", methods=["POST"])
@app.route("/whatsapp/webhook", methods=["POST"])
def webhook():
    app.logger.info("Inbound webhook hit")
    payload = request.get_json(force=True, silent=True) or {}
    app.logger.info("RAW_PAYLOAD=" + json.dumps(payload)[:3000])

    text, sender = extract_text_and_sender(payload)
    app.logger.info(f"MSG_BODY={text} SENDER={sender}")

    task_id = None
    if text and sender:
        try:
            task_id = create_task({"text": text, "sender": sender})
            app.logger.info(f"TASK_CREATED id={task_id}")
        except Exception as e:
            app.logger.error(f"TASK_CREATE_ERROR: {e}")

    return jsonify(ok=True, task_id=task_id), 200

@app.route("/admin/debug", methods=["GET"])
def admin_debug():
    token = request.args.get("token")
    if token != os.getenv("HUBFLO_ADMIN_TOKEN"):
        return "Unauthorized", 401
    return jsonify(get_tasks()), 200

def extract_text_and_sender(p):
    # 360dialog direct format
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

    # Meta relay format (what we’ve been seeing)
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)