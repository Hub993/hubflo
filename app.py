import os
import json
import logging
import urllib.parse
import requests
from flask import Flask, request, jsonify, abort, Response
from dotenv import load_dotenv

# --- env + logging -----------------------------------------------------------
load_dotenv()

ADMIN_TOKEN = os.environ.get("HUBFLO_ADMIN_TOKEN", "")
BOUND_NUMBER = os.environ.get("BOUND_NUMBER", "")  # your WhatsApp sender (waba)
D360_KEY     = os.environ.get("D360_KEY", "")      # 360dialog Sandbox/Prod key (x-headers)
D360_BEARER  = os.environ.get("DIALOG360_API_KEY", "")  # if you later use Bearer flow

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# --- storage ---------------------------------------------------------------
# storage.py must provide: init_db(), create_task(dict), get_tasks([ids]|None)
from storage import init_db, create_task, get_tasks
init_db()

# --- helpers ---------------------------------------------------------------
def check_auth():
    # Token via header OR querystring ?token=
    token = request.headers.get("Authorization", "")
    if token.lower().startswith("bearer "):
        token = token[7:]
    else:
        token = request.args.get("token", "")
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        abort(401)

def classify_tag(text: str) -> str:
    """
    Hybrid classifier:
    - explicit hashtags: #order #task #change #urgent (takes priority)
    - prefix cues: startswith('order', 'change', 'urgent', 'task')
    - else: blank (ungrouped/general)
    """
    if not text:
        return None
    low = text.lower().strip()

    # hashtag override
    if "#order" in low:  return "order"
    if "#task" in low:   return "task"
    if "#change" in low: return "change"
    if "#urgent" in low: return "urgent"

    # soft prefix cues
    if low.startswith("order"):   return "order"
    if low.startswith("change"):  return "change"
    if low.startswith("urgent"):  return "urgent"
    if low.startswith("task"):    return "task"

    # no tag
    return None

def send_whatsapp_text(to_wa_id: str, body: str) -> dict:
    """
    Attempt to send via 360dialog.
    Note: In sandbox you will likely see 401 'Invalid api key'—that’s expected until we go production.
    """
    result = {"ok": False, "status_code": None, "body": None}

    # Endpoint used by 360dialog
    url = "https://waba.360dialog.io/v1/messages"

    payload = {
        "to": to_wa_id,
        "type": "text",
        "text": {"body": body}
    }

    # Try x-API-KEY header style (common for D360)
    if D360_KEY:
        headers = {
            "Content-Type": "application/json",
            "D360-API-KEY": D360_KEY
        }
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
            result["status_code"] = r.status_code
            result["body"] = r.text
            app.logger.info(f"D360_SEND status_code={r.status_code} body={r.text}")
            if r.ok:
                result["ok"] = True
                return result
        except Exception as e:
            app.logger.error(f"D360_SEND error: {e}")

    # Try Bearer fallback if provided
    if D360_BEARER:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {D360_BEARER}",
        }
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
            result["status_code"] = r.status_code
            result["body"] = r.text
            app.logger.info(f"D360_SEND (bearer) status_code={r.status_code} body={r.text}")
            if r.ok:
                result["ok"] = True
                return result
        except Exception as e:
            app.logger.error(f"D360_SEND bearer error: {e}")

    # If we got here, either keys missing or 401 etc.
    if not D360_KEY and not D360_BEARER:
        app.logger.warning("send_whatsapp_text skipped (missing D360 key)")
    return result

# --- routes ----------------------------------------------------------------
@app.route("/")
def index():
    return Response("HUBFLO service running", mimetype="text/plain")

@app.route("/health")
def health():
    return jsonify(ok=True)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    app.logger.info("Inbound webhook hit")
    try:
        app.logger.info(f"RAW_PAYLOAD={json.dumps(data)}")
    except Exception:
        pass

    # Extract message and sender (Meta/WhatsApp webhook structure)
    msg_body = None
    sender = None
    try:
        value = data["entry"][0]["changes"][0]["value"]
        msg = value["messages"][0]
        msg_body = (msg.get("text") or {}).get("body")
        sender = msg.get("from")
    except Exception:
        # could be other callback types
        return jsonify(ok=True, ignored=True), 200

    app.logger.info(f"MSG_BODY={msg_body} SENDER={sender}")

    # Tag + store
    tag = classify_tag(msg_body or "")
    row_id = create_task({"sender": sender, "tag": tag, "text": msg_body})
    app.logger.info(f"TASK_CREATED id={row_id}")

    # --- Basic Reply Rule (always on for now) ------------------------------
    # Even in sandbox (401), we want to see the attempt + logs for end-to-end
    reply_text = "✅ HUBFLO received your message."
    if tag:
        reply_text += f" (tag='{tag}')"
    send_res = send_whatsapp_text(sender, reply_text)
    app.logger.info(f"AUTO_REPLY status={send_res.get('ok', False)}")

    return jsonify(ok=True), 200

@app.route("/whatsapp/webhook", methods=["POST"])
def whatsapp_webhook():
    return webhook()

# --- Admin: JSON & HTML views ---------------------------------------------
@app.route("/admin/debug")
def admin_debug():
    check_auth()
    ids = request.args.get("ids", "")
    id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    return jsonify(get_tasks(id_list if id_list else None))

@app.route("/admin/summary")
def admin_summary():
    check_auth()
    # counts by tag + latest 10 items
    rows = get_tasks(None)
    counts = {}
    for r in rows:
        t = r.get("tag") or "none"
        counts[t] = counts.get(t, 0) + 1
    latest = rows[:10]
    return jsonify({"counts_by_tag": counts, "latest": latest})

@app.route("/admin/search")
def admin_search():
    check_auth()
    tag = request.args.get("tag")
    q = request.args.get("q")
    sender = request.args.get("sender")

    rows = get_tasks(None)
    def ok(r):
        if tag and (r.get("tag") or "") != tag:
            return False
        if sender and (r.get("sender") or "") != sender:
            return False
        if q:
            needle = q.lower()
            hay = f"{r.get('text','')} {r.get('tag','')} {r.get('sender','')}".lower()
            if needle not in hay:
                return False
        return True

    out = [r for r in rows if ok(r)]
    return jsonify(out)

@app.route("/admin/view")
def admin_view():
    check_auth()
    tag = request.args.get("tag", "*")
    q = request.args.get("q", "*")
    sender = request.args.get("sender", "*")
    rows = get_tasks(None)

    def match(r):
        if tag != "*" and (r.get("tag") or "") != tag:
            return False
        if sender != "*" and (r.get("sender") or "") != sender:
            return False
        if q != "*":
            needle = q.lower()
            hay = f"{r.get('text','')} {r.get('tag','')} {r.get('sender','')}".lower()
            if needle not in hay:
                return False
        return True

    filtered = [r for r in rows if match(r)]
    html_rows = "\n".join(
        f"<tr><td>{r['id']}</td><td>{r['ts']}</td><td>{r.get('sender','')}</td>"
        f"<td>{r.get('tag','')}</td><td>{r.get('text','')}</td></tr>"
        for r in filtered
    )
    html = f"""
    <html>
    <head>
      <title>HUBFLO Admin</title>
      <style>
        body {{ font-family: -apple-system, system-ui, Helvetica, Arial, sans-serif; padding: 14px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; font-size: 14px; }}
        th {{ background: #f7f7f7; text-align: left; }}
        .filters {{ margin-bottom: 10px; font-size: 14px; }}
        code {{ background:#f3f3f3; padding:2px 6px; border-radius:4px; }}
      </style>
    </head>
    <body>
      <h2>HUBFLO Admin</h2>
      <div class="filters">
        Filters:
        tag=<code>{tag}</code> &nbsp; q=<code>{q}</code> &nbsp; sender=<code>{sender}</code>
      </div>
      <table>
        <thead>
          <tr><th>ID</th><th>Time</th><th>Sender</th><th>Tag</th><th>Text</th></tr>
        </thead>
        <tbody>
          {html_rows if html_rows else "<tr><td colspan='5'>(no rows)</td></tr>"}
        </tbody>
      </table>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")

# --- main ------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)