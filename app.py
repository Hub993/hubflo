# app.py — Hubflo (full replacement)
import os, json, logging
from datetime import datetime
from urllib.parse import quote
from flask import Flask, request, jsonify, Response
from storage import init_db, create_task, get_tasks  # storage API you already have

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

ADMIN_TOKEN = os.getenv("HUBFLO_ADMIN_TOKEN", "").strip()

# ---------- helpers ----------
def classify_tag(text: str) -> str | None:
    if not text:
        return None
    t = text.strip()
    low = t.lower()
    # explicit hashtags anywhere
    if "#order" in low:  return "order"
    if "#task" in low:   return "task"
    if "#change" in low: return "change"
    if "urgent" in low or low.startswith("!!"):
        return "urgent"
    # prefix formats
    for p, tag in (("order:", "order"), ("change:", "change"), ("task:", "task")):
        if low.startswith(p): return tag
    # “Order 10m …” (no colon)
    if low.startswith("order "):  return "order"
    if low.startswith("change "): return "change"
    if low.startswith("task "):   return "task"
    return None

def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def require_admin_token():
    token = request.args.get("token","").strip()
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        return Response("Unauthorized", status=401)
    return None

# ---------- routes ----------
@app.route("/", methods=["GET"])
def health():
    return "Hubflo service running"

@app.route("/webhook", methods=["POST"])
def webhook():
    log.info("Inbound webhook hit")
    try:
        payload = request.get_json(force=True, silent=True)
        log.info("RAW_PAYLOAD=%s", json.dumps(payload, ensure_ascii=False))
    except Exception:
        payload = None
        log.info("RAW_PAYLOAD=None")

    msg_body = None
    sender = None
    phone_id = None
    try:
        entry = payload.get("entry",[{}])[0]
        changes = entry.get("changes",[{}])[0]
        value = changes.get("value",{})
        phone_id = value.get("metadata",{}).get("phone_number_id")
        msgs = value.get("messages",[{}])
        m = msgs[0]
        sender = m.get("from")
        if m.get("type") == "text":
            msg_body = m.get("text",{}).get("body")
    except Exception:
        pass

    log.info("MSG_BODY=%s SENDER=%s PHONE_ID=%s", msg_body, sender, phone_id)

    # create task regardless of tagging (you’ve been doing this)
    tag = classify_tag(msg_body or "")
    if tag:
        log.info("TAG=%s", tag)

    try:
        tid = create_task(sender=sender, text=msg_body or "", tag=tag)
        log.info("TASK_CREATED id=%s", tid)
    except Exception as e:
        log.exception("TASK_CREATE_FAILED: %s", e)

    # reply path is still gated by your 360 key; we keep behavior unchanged
    return ("", 200)

@app.route("/admin/view", methods=["GET"])
def admin_view():
    unauthorized = require_admin_token()
    if unauthorized: return unauthorized

    tag = request.args.get("tag","").strip().lower() or None
    q   = request.args.get("q","").strip() or None
    sender = request.args.get("sender","").strip() or None
    limit = int(request.args.get("limit","200") or "200")
    # pull rows (try with q support; fall back if storage doesn’t accept it)
    try:
        rows = get_tasks(tag=tag, q=q, sender=sender, limit=limit, order="desc")
    except TypeError:
        rows = get_tasks(tag=tag, limit=limit, order="desc")
        if q:
            qlow = q.lower()
            rows = [r for r in rows if qlow in (r.get("text","").lower()
                                                or r.get("tag","") or ""
                                               ) or qlow in (r.get("sender","") or "").lower()]
        if sender:
            sl = sender.lower()
            rows = [r for r in rows if sl in (r.get("sender","") or "").lower()]

    # HTML
    def fmt_row(r):
        return f"<tr><td>{r.get('id')}</td><td>{html_escape(r.get('ts',''))}</td>" \
               f"<td>{html_escape(r.get('sender',''))}</td>" \
               f"<td>{html_escape(r.get('tag') or '')}</td>" \
               f"<td>{html_escape(r.get('text',''))}</td></tr>"

    base = "/admin/view?token=" + quote(ADMIN_TOKEN)
    current = base \
        + (f"&tag={quote(tag)}" if tag else "") \
        + (f"&q={quote(q)}" if q else "") \
        + (f"&sender={quote(sender)}" if sender else "")

    html = f"""
<!doctype html>
<html><head>
<meta charset="utf-8"/>
<title>Hubflo Admin</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 18px; }}
 h2 {{ margin: 0 0 8px; }}
 .controls {{ margin: 10px 0 16px; display: flex; gap: 8px; flex-wrap: wrap; }}
 input[type=text] {{ padding:6px 8px; border:1px solid #ccc; border-radius:8px; min-width:180px; }}
 select {{ padding:6px 8px; border:1px solid #ccc; border-radius:8px; }}
 button {{ padding:6px 10px; border:1px solid #888; background:#f7f7f7; border-radius:8px; cursor:pointer; }}
 table {{ border-collapse: collapse; width: 100%; }}
 th, td {{ border: 1px solid #e2e2e2; padding: 8px 10px; text-align: left; }}
 th {{ background: #fafafa; }}
 .muted {{ color:#666; font-size:12px; }}
</style>
</head><body>
<h2>Hubflo Admin</h2>
<div class="muted">Filters: tag, free-text (q), sender. Showing newest first.</div>

<form class="controls" method="get" action="/admin/view">
  <input type="hidden" name="token" value="{html_escape(ADMIN_TOKEN)}"/>
  <label>Tag:
    <select name="tag" onchange="this.form.submit()">
      <option value="" {"selected" if not tag else ""}>*</option>
      <option value="order" {"selected" if tag=="order" else ""}>order</option>
      <option value="change" {"selected" if tag=="change" else ""}>change</option>
      <option value="task" {"selected" if tag=="task" else ""}>task</option>
      <option value="urgent" {"selected" if tag=="urgent" else ""}>urgent</option>
    </select>
  </label>
  <label>q:
    <input type="text" name="q" value="{html_escape(q or '')}" placeholder="search text / sender"/>
  </label>
  <label>sender:
    <input type="text" name="sender" value="{html_escape(sender or '')}" placeholder="phone or part"/>
  </label>
  <label>limit:
    <input type="text" name="limit" value="{limit}" style="width:70px"/>
  </label>
  <button type="submit">Apply</button>
  <a href="{base}"><button type="button">Clear</button></a>
</form>

<table>
  <thead><tr><th>ID</th><th>Time</th><th>Sender</th><th>Tag</th><th>Text</th></tr></thead>
  <tbody>
    {''.join(fmt_row(r) for r in rows) if rows else '<tr><td colspan="5" class="muted">No rows</td></tr>'}
  </tbody>
</table>

<div class="muted" style="margin-top:10px">
  Current URL: {html_escape(current)}
</div>
</body></html>
"""
    return Response(html, mimetype="text/html")

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","10000")))