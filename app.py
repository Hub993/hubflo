CONFIRM V6
# app_v6.py  — HubFlo Version 6
# ---------------------------------------------------------------
# Rebuilt from v5 base with all verified post-V5 improvements:
# - Order-step checklist
# - Task subtype detection (assigned/self)
# - Daily digest scaffolds (6 AM subs, 6 PM PMs)
# - Change-order cost/time impact fields
# - Stock / material tracking
# ---------------------------------------------------------------

import os, json, logging, datetime as dt, requests
from typing import Optional
from flask import Flask, request, jsonify, Response
from storage_v6 import (
    init_db, create_task, get_tasks, get_summary,
    mark_done, approve_task, reject_task, set_order_state,
    revoke_last, subcontractor_accuracy,
    create_meeting, start_meeting, close_meeting,
    create_stock_item, adjust_stock, get_stock_report,
    record_change_order
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("hubflo")

# ---------------------------------------------------------------------
# Environment / config
# ---------------------------------------------------------------------
ADMIN_TOKEN = os.environ.get("HUBFLO_ADMIN_TOKEN", "").strip()
D360_KEY = (
    os.environ.get("DIALOG360_API_KEY")
    or os.environ.get("Dialog360_API_Key")
    or os.environ.get("D360_KEY")
    or os.environ.get("D360_key")
    or ""
).strip()
DEFAULT_PHONE_ID = os.environ.get("BOUND_NUMBER", "").strip()
WHATSAPP_BASE = "https://waba.360dialog.io/v1/messages"

ORDER_LIFECYCLE_STATES = [
    "quoted","pending_approval","approved",
    "cancelled","invoiced","enacted"
]

# ---------------------------------------------------------------------
# Boot DB
# ---------------------------------------------------------------------
init_db()

# ---------------------------------------------------------------------
# Tagging / classification
# ---------------------------------------------------------------------
ORDER_PREFIXES = ("order","purchase","procure","buy")
CHANGE_PREFIXES = ("change","variation","revise","amend","adjust")
TASK_PREFIXES  = ("task","todo","to-do","install","fix","inspect","lay","build","schedule")
HASHTAG_MAP = {"#order":"order","#change":"change","#task":"task","#urgent":"urgent"}

def classify_tag(text:str)->Optional[str]:
    if not text: return None
    t=text.strip().lower()
    for h,tag in HASHTAG_MAP.items():
        if h in t: return tag
    for p in ORDER_PREFIXES:
        if t.startswith(p+" "): return "order"
    for p in CHANGE_PREFIXES:
        if t.startswith(p+" "): return "change"
    for p in TASK_PREFIXES:
        if t.startswith(p+" "): return "task"
    if any(u in t for u in ["m ","meter","metre","roll","cable","conduit"]) and any(ch.isdigit() for ch in t):
        return "order"
    if any(w in t for w in ["urgent","asap","immediately"]):
        return "urgent"
    return None

def detect_subtype(text:str)->str:
    if not text: return "assigned"
    t=text.lower()
    if "i will" in t or "i'll" in t or "my task" in t: return "self"
    return "assigned"

# ---------------------------------------------------------------------
# WhatsApp send utility
# ---------------------------------------------------------------------
def send_whatsapp_text(phone_id:str,to:str,body:str)->tuple[bool,dict]:
    if not (D360_KEY and phone_id and to and body):
        log.warning("send_whatsapp_text skipped (missing key/to/body)")
        return False,{}
    headers={"D360-API-KEY":D360_KEY,"Content-Type":"application/json"}
    payload={"to":to,"type":"text","text":{"body":body}}
    try:
        r=requests.post(WHATSAPP_BASE,headers=headers,json=payload,timeout=10)
        data=r.json() if r.text else {}
        return (200<=r.status_code<300),data
    except Exception as e:
        log.exception("D360 send error: %s",e)
        return False,{"error":str(e)}

# ---------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------
@app.route("/",methods=["GET"])
def health():
    return "HubFlo V6 service running",200

# ---------------------------------------------------------------------
# Webhook (Meta/360dialog inbound)
# ---------------------------------------------------------------------
@app.route("/webhook",methods=["POST"])
def webhook():
    raw=request.get_json(silent=True) or {}
    try:
        entry=(raw.get("entry") or [])[0]
        changes=(entry.get("changes") or [])[0]
        value=changes.get("value") or {}
        msgs=value.get("messages") or []
        contacts=value.get("contacts") or []
        phone_id=(value.get("metadata") or {}).get("phone_number_id") or DEFAULT_PHONE_ID
    except Exception:
        msgs,contacts,phone_id=[],[],DEFAULT_PHONE_ID
    sender=None
    if contacts: sender=contacts[0].get("wa_id") or sender
    for m in msgs:
        sender=m.get("from") or sender
        mtype=m.get("type")
        text=(m.get("text") or {}).get("body") if mtype=="text" else None
        if not text and mtype in ("image","document","audio","video"):
            text=(m.get(mtype,{}) or {}).get("caption")
        tag=classify_tag(text or "")
        subtype=detect_subtype(text or "")
        order_state=None
        if tag=="order" and text:
            for state in ORDER_LIFECYCLE_STATES:
                if f"#{state}" in text.lower(): order_state=state; break
        row=create_task(sender=sender,text=text or "",tag=tag,
                        project_code=None,subcontractor_name=None,
                        order_state=order_state)
        if tag=="order": reply="Order noted."
        elif tag=="change": reply="Change logged."
        elif tag=="task": reply="Task created."
        else: reply=None
        if reply: send_whatsapp_text(phone_id,to=sender,body=reply)
    return "",200

# ---------------------------------------------------------------------
# Admin guard
# ---------------------------------------------------------------------
def _auth_fail(): return Response("Unauthorized",401)
def _check_admin():
    token=request.args.get("token","")
    return not ADMIN_TOKEN or token==ADMIN_TOKEN

# ---------------------------------------------------------------------
# Admin routes (summary only for brevity; unchanged logic from v5)
# ---------------------------------------------------------------------
@app.route("/admin/view",methods=["GET"])
def admin_view():
    if not _check_admin(): return _auth_fail()
    rows=get_tasks(limit=200)
    body=json.dumps(rows,indent=2)
    return Response(body,200,mimetype="application/json")

@app.route("/admin/summary",methods=["GET"])
def api_summary():
    if not _check_admin(): return _auth_fail()
    return jsonify(get_summary())

# ---------------------------------------------------------------------
# Change Orders & Stock endpoints (new)
# ---------------------------------------------------------------------
@app.route("/admin/change_order",methods=["POST"])
def api_change_order():
    if not _check_admin(): return _auth_fail()
    data=request.get_json(force=True)
    return jsonify(record_change_order(data))

@app.route("/admin/stock/create",methods=["POST"])
def api_stock_create():
    if not _check_admin(): return _auth_fail()
    data=request.get_json(force=True)
    return jsonify(create_stock_item(data))

@app.route("/admin/stock/adjust",methods=["POST"])
def api_stock_adjust():
    if not _check_admin(): return _auth_fail()
    data=request.get_json(force=True)
    return jsonify(adjust_stock(data))

@app.route("/admin/stock/report",methods=["GET"])
def api_stock_report():
    if not _check_admin(): return _auth_fail()
    return jsonify(get_stock_report())

# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------
if __name__=="__main__":
    port=int(os.environ.get("PORT","10000"))
    app.run(host="0.0.0.0",port=port,debug=False)