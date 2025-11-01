# app_v6.py - Hubflo Version 6
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
from storage import (
    init_db, create_task, get_tasks, get_summary,
    mark_done, approve_task, reject_task, set_order_state,
    revoke_last, subcontractor_accuracy,
    create_meeting, start_meeting, close_meeting,
    create_stock_item, adjust_stock, get_stock_report,
    record_change_order
)

from storage import Task

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


# ============================================================
# HUBFLO INTEGRITY PATCH — CANONICAL HEARTBEAT (v6 unified)
# ============================================================
from sqlalchemy import text
from storage import SessionLocal, hygiene_pin, hygiene_guard, SystemState

@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    """Canonical heartbeat — DB check + hygiene tether."""
    try:
        with SessionLocal() as s:
            s.execute(text("SELECT 1"))
        db_state = "ok"
    except Exception as e:
        db_state = f"fail:{str(e)[:80]}"

    # record hygiene pin and check staleness
    hygiene_pin()
    ok, note = hygiene_guard()

    return jsonify({
        "db": db_state,
        "hygiene_ok": ok,
        "note": note,
        "utc": dt.datetime.utcnow().isoformat() + "Z"
    }), 200

@app.route("/integrity/status", methods=["GET"])
def integrity_status():
    """Report redmode + hygiene info for external tether."""
    with SessionLocal() as s:
        ss = s.query(SystemState).first()
        return jsonify({
            "redmode": bool(ss.redmode) if ss else None,
            "redmode_reason": ss.redmode_reason if ss else None,
            "hygiene_last_utc": ss.hygiene_last_utc if ss else None
        }), 200
# ============================================================

# ---------------------------------------------------------------------
# Tagging / classification
# ---------------------------------------------------------------------
ORDER_PREFIXES = ("order","purchase","procure","buy")

# CHANGE verbs = scope modifications
CHANGE_PREFIXES = (
    "change","variation","revise","amend","adjust",
    "extend","enlarge","widen","lengthen","raise","lower",
    "shift","relocate","move","reposition",
    "replace","remove","strip","demolish","knock","add","reduce","modify","alter"
)

# TASK verbs = performing work / execution actions
TASK_PREFIXES = (
    "task","todo","to-do",
    "install","fix","inspect","lay","build","schedule","plumb","level","square","miter",
    "build up","strip","rip","float",

    # expanded construction verbs
    "frame","hang","set","mount","fit","align","adjust","brace","support",
    "cut","trim","finish","sand","prime","paint","seal",
    "dig","excavate","trench","backfill","compact",
    "pour","tie","rebar","form","grout","cure",
    "weld","grind","fabricate","assemble",
    "bolt","anchor","fasten","secure",
    "clean","prep","clear","demo","remove"
)

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

# === ADD NEAR TOP, BELOW send_whatsapp_text ===
import json

def send_order_checklist(phone_id: str, to: str, task_id: int):
    headers = {"D360-API-KEY": D360_KEY, "Content-Type": "application/json"}
    payload = {
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "Order logged. Confirm next detail:"},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": f"order_item:{task_id}", "title": "Item"}},
                    {"type": "reply", "reply": {"id": f"order_quantity:{task_id}", "title": "Quantity"}},
                    {"type": "reply", "reply": {"id": f"order_supplier:{task_id}", "title": "Supplier"}},
                    {"type": "reply", "reply": {"id": f"order_delivery_date:{task_id}", "title": "Delivery Date"}},
                    {"type": "reply", "reply": {"id": f"order_drop_location:{task_id}", "title": "Drop Location"}},
                ]
            }
        }
    }
    try:
        r = requests.post(WHATSAPP_BASE, headers=headers, json=payload, timeout=10)
        return (200 <= r.status_code < 300)
    except:
        return False


# === MODIFY IN /webhook, inside loop after create_task(...) and before return ===
        row = create_task(
            sender=sender,
            text=text or "",
            tag=tag,
            project_code=None,
            subcontractor_name=None,
            order_state=order_state,
            attachment=attachment,
            subtype=subtype
        )

        # send checklist for orders
        if tag == "order":
            send_order_checklist(phone_id, sender, row["id"])
            return ("", 200)

        # existing auto-replies remain unchanged below

# ---------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------
@app.route("/",methods=["GET"])
def health():
    return "HubFlo V6 service running",200

# ---------------------------------------------------------------------
# Webhook (Meta/360dialog inbound)
# ---------------------------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    raw = request.get_json(silent=True) or {}
    try:
        entry = (raw.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        msgs = value.get("messages") or []
        contacts = value.get("contacts") or []
        phone_id = (value.get("metadata") or {}).get("phone_number_id") or DEFAULT_PHONE_ID
    except Exception:
        msgs, contacts, phone_id = [], [], DEFAULT_PHONE_ID

    sender = None
    if contacts:
        sender = contacts[0].get("wa_id") or sender

    for m in msgs:
        sender = m.get("from") or sender
        mtype = m.get("type")
        text = None
        attachment = None

        # === INTERACTIVE BUTTON HANDLING ======================================
        if mtype == "interactive":
            br = (m.get("interactive") or {}).get("button_reply") or {}
            bid = br.get("id", "") or ""

            if bid.startswith("order_item:"):
                tid = int(bid.split(":")[1])
                with SessionLocal() as s:
                    t = s.get(Task, tid)
                    if t:
                        t.text = f"[await:item] {t.text or ''}"
                        s.commit()
                send_whatsapp_text(phone_id, sender, "Great — what item should we order?")
                return ("", 200)

            if bid.startswith("order_quantity:"):
                tid = int(bid.split(":")[1])
                with SessionLocal() as s:
                    t = s.get(Task, tid)
                    if t:
                        t.text = f"[await:quantity] {t.text or ''}"
                        s.commit()
                send_whatsapp_text(phone_id, sender, "Okay — what quantity do we need?")
                return ("", 200)

            if bid.startswith("order_supplier:"):
                tid = int(bid.split(":")[1])
                with SessionLocal() as s:
                    t = s.get(Task, tid)
                    if t:
                        t.text = f"[await:supplier] {t.text or ''}"
                        s.commit()
                send_whatsapp_text(phone_id, sender, "Got it — who should we source this from?")
                return ("", 200)

            if bid.startswith("order_delivery_date:"):
                tid = int(bid.split(":")[1])
                with SessionLocal() as s:
                    t = s.get(Task, tid)
                    if t:
                        t.text = f"[await:delivery_date] {t.text or ''}"
                        s.commit()
                send_whatsapp_text(phone_id, sender, "When must this be delivered?")
                return ("", 200)

            if bid.startswith("order_drop_location:"):
                tid = int(bid.split(":")[1])
                with SessionLocal() as s:
                    t = s.get(Task, tid)
                    if t:
                        t.text = f"[await:drop_location] {t.text or ''}"
                        s.commit()
                send_whatsapp_text(phone_id, sender, "Where should this be dropped on site?")
                return ("", 200)

        # === MEDIA & TEXT =====================================================
        if mtype == "text":
            text = (m.get("text") or {}).get("body")

        elif mtype in ("image", "document", "audio", "video"):
            meta = m.get(mtype, {}) or {}
            mid = meta.get("id")
            url = f"whatsapp_media://{mtype}/{mid}" if mid else None
            mime = meta.get("mime_type")
            name = meta.get("filename")
            attachment = {"url": url, "mime": mime, "name": name}
            text = meta.get("caption")

        # === AWAIT FOLLOW-UP CAPTURE ==========================================
        if text:
            with SessionLocal() as s:
                awaiting = (
                    s.query(Task)
                    .filter(
                        Task.sender == sender,
                        Task.status == "open",
                        Task.tag == "order",
                        Task.text.ilike("[await:%]%")
                    )
                    .order_by(Task.id.desc())
                    .first()
                )

                if awaiting:
                    lower = awaiting.text.lower()

                    # normalize body (remove await prefix)
                    body = awaiting.text
                    if body.startswith("[await:"):
                        body = body.split("]",1)[1].strip()

                    lines = [l.strip() for l in body.splitlines() if l.strip()]
                    fields = {}
                    for l in lines:
                        if ":" in l:
                            k,v = l.split(":",1)
                            fields[k.strip()] = v.strip()

                    item     = fields.get("Item")
                    qty      = fields.get("Quantity")
                    supplier = fields.get("Supplier")
                    ddate    = fields.get("Delivery Date")
                    drop     = fields.get("Drop Location")

                    # ITEM
                    if lower.startswith("[await:item]"):
                        awaiting.text = f"[await:quantity] Item: {text}"
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Quantity?")
                        return ("", 200)

                    # QUANTITY
                    if lower.startswith("[await:quantity]"):
                        awaiting.text = f"[await:supplier] Item: {item or ''}\nQuantity: {text}"
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Supplier?")
                        return ("", 200)

                    # SUPPLIER
                    if lower.startswith("[await:supplier]"):
                        awaiting.text = (
                            f"[await:delivery_date] Item: {item or ''}\n"
                            f"Quantity: {qty or ''}\n"
                            f"Supplier: {text}"
                        )
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Delivery date?")
                        return ("", 200)

                    # DELIVERY DATE
                    if lower.startswith("[await:delivery_date]"):
                        awaiting.text = (
                            f"[await:drop_location] Item: {item or ''}\n"
                            f"Quantity: {qty or ''}\n"
                            f"Supplier: {supplier or ''}\n"
                            f"Delivery Date: {text}"
                        )
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Drop location on site?")
                        return ("", 200)

                    # DROP LOCATION
                    if lower.startswith("[await:drop_location]"):
                        awaiting.text = (
                            f"Item: {item or ''}\n"
                            f"Quantity: {qty or ''}\n"
                            f"Supplier: {supplier or ''}\n"
                            f"Delivery Date: {ddate or ''}\n"
                            f"Drop Location: {text}"
                        )
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "✅ Order details recorded.")
                        return ("", 200)

        # === CLASSIFICATION ====================================================
        tag = classify_tag(text or "")
        subtype = detect_subtype(text or "")

        # Self-tasks should still behave as tasks
        if subtype == "self" and tag is None:
            tag = "task"

        # detect order lifecycle state (if any hash present)
        order_state = None
        if tag == "order" and text:
            for state in ORDER_LIFECYCLE_STATES:
                if f"#{state}" in text.lower():
                    order_state = state
                    break

        # lookup sender identity (role / subcontractor / project)
        from storage import get_user_role
        user = get_user_role(sender) or {}

        # PM routing lookup (project-based)
        from storage import get_pms_for_project
        pms = []
        proj = user.get("project_code") or None
        if proj:
            pms = get_pms_for_project(proj) or []

        # create task (always)
        row = create_task(
            sender=sender,
            text=text or "",
            tag=tag,
            project_code=user.get("project_code") or None,
            subcontractor_name=user.get("subcontractor_name") or None,
            order_state=order_state,
            attachment=attachment,
            subtype=subtype
        )

        # routing context available:
        # pms = list of {"wa_id","name","role","primary"}
        # stored now for digest + escalation layers (no outbound sends at this stage)

        # === SANDBOX ORDER FALLBACK (no interactive buttons) ===================
        if tag == "order" and "[await:" not in (text or "").lower():
            # immediately mark task to begin follow-up sequence
            with SessionLocal() as s:
                t = s.get(Task, row["id"])
                if t and not (t.text or "").lower().startswith("[await:item]"):
                    t.text = f"[await:item] {t.text}"
                    s.commit()
            # prompt next detail
            send_whatsapp_text(phone_id, sender, "Item?")
            return ("", 200)

        # === NON-ORDER ROUTES ==================================================
        if tag == "change":
            send_whatsapp_text(phone_id, sender, "Change logged.")
        elif tag == "task":
            send_whatsapp_text(phone_id, sender, "Task created.")

    return ("", 200)

# ---------------------------------------------------------------------
# Admin views — dual output (HTML + JSON)
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Admin guard
# ---------------------------------------------------------------------
def _auth_fail(): return Response("Unauthorized",401)
def _check_admin():
    token=request.args.get("token","")
    return not ADMIN_TOKEN or token==ADMIN_TOKEN

@app.route("/admin/summary",methods=["GET"])
def api_summary():
    if not _check_admin(): return _auth_fail()
    return jsonify(get_summary())

@app.route("/admin/view", methods=["GET"])
def admin_view():
    if not _check_admin(): return _auth_fail()
    rows = get_tasks(limit=200)

    def h(s):
        return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    th = (
        "<tr><th>ID</th><th>Time</th><th>Sender</th><th>Tag</th>"
        "<th>Status</th><th>Order State</th><th>Text</th></tr>"
    )
    trs = []
    for r in rows:
        trs.append(
            f"<tr>"
            f"<td>{r['id']}</td>"
            f"<td>{h(r['ts'])}</td>"
            f"<td>{h(r.get('sender') or '')}</td>"
            f"<td>{h(r.get('tag') or '')}</td>"
            f"<td>{h(r.get('status') or '')}</td>"
            f"<td>{h(r.get('order_state') or '')}</td>"
            f"<td>{h(r['text'])}</td>"
            f"</tr>"
        )

    body = f"""
    <html><head><title>HubFlo Admin</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}}
      table{{border-collapse:collapse;width:100%}}
      th,td{{border:1px solid #ddd;padding:6px;font-size:13px}}
      th{{background:#f2f2f2;text-align:left}}
    </style></head><body>
    <h2>HubFlo Admin (HTML)</h2>
    <table>{th}{''.join(trs)}</table>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")

@app.route("/admin/view.json", methods=["GET"])
def admin_view_json():
    if not _check_admin(): return _auth_fail()
    tag = request.args.get("tag") or None
    q = request.args.get("q") or None
    sender = request.args.get("sender") or None
    limit = int(request.args.get("limit", "200"))
    return jsonify(get_tasks(tag=tag, q=q, sender=sender, limit=limit))

# ---------------------------------------------------------------------
# Admin action routes (parity with v5)
# ---------------------------------------------------------------------

@app.route("/admin/approve", methods=["POST"])
def api_approve():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    note = data.get("note")

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    result = approve_task(int(tid), actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    # Optional note for audit (future use)
    if note:
        log_audit("admin", "approve_note", "task", int(tid), details=note)

    return jsonify(result), 200

@app.route("/admin/reject", methods=["POST"])
def api_reject():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    rework = data.get("rework", True)

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    result = reject_task(int(tid), rework=bool(rework), actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    return jsonify(result), 200

@app.route("/admin/revoke", methods=["POST"])
def api_revoke():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    note = data.get("note")

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    result = revoke_last(int(tid), actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    # Optional note for audit
    if note:
        log_audit("admin", "revoke_note", "task", int(tid), details=note)

    return jsonify(result), 200


@app.route("/admin/order_state", methods=["POST"])
def api_order_state():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("id")
    state = (data.get("state") or "").strip().lower()

    allowed = ["quoted","pending_approval","approved","cancelled","invoiced","enacted"]

    if tid is None:
        return jsonify({"error": "missing id"}), 400

    if state not in allowed:
        return jsonify({"error": "invalid state", "allowed": allowed}), 400

    result = set_order_state(int(tid), state, actor="admin")

    if not result:
        return jsonify({"error": "not found"}), 404

    return jsonify(result), 200

@app.route("/admin/accuracy", methods=["GET"])
def api_accuracy():
    if not _check_admin(): return _auth_fail()
    name = request.args.get("subcontractor", "")
    if not name:
        return jsonify({"error": "missing subcontractor"}), 400
    return jsonify(subcontractor_accuracy(name))

@app.route("/admin/meeting/create", methods=["POST"])
def api_meeting_create():
    if not _check_admin(): return _auth_fail()
    title = request.args.get("title", "Site Meeting")
    project_code = request.args.get("project") or None
    subcontractor_name = request.args.get("subcontractor") or None
    site_name = request.args.get("site") or None
    scheduled_for = request.args.get("when") or None
    task_ids = request.args.get("tasks") or ""
    if scheduled_for:
        try:
            scheduled_for = dt.datetime.fromisoformat(scheduled_for)
        except Exception:
            scheduled_for = None
    ids = []
    for t in (task_ids.split(",") if task_ids else []):
        t = t.strip()
        if t.isdigit(): ids.append(int(t))
    return jsonify(create_meeting(
        title=title, project_code=project_code, subcontractor_name=subcontractor_name,
        site_name=site_name, scheduled_for=scheduled_for, task_ids=ids, created_by="admin"
    ))

@app.route("/admin/meeting/start", methods=["POST"])
def api_meeting_start():
    if not _check_admin(): return _auth_fail()
    mid = int(request.args.get("id", "0"))
    return jsonify(start_meeting(mid, actor="admin") or {"error": "not found"})

@app.route("/admin/meeting/close", methods=["POST"])
def api_meeting_close():
    if not _check_admin(): return _auth_fail()
    mid = int(request.args.get("id", "0"))
    return jsonify(close_meeting(mid, actor="admin") or {"error": "not found"})

# ---------------------------------------------------------------------
# Take-On Import: Users / Roles / Hierarchy
# ---------------------------------------------------------------------
from storage import SessionLocal, User

@app.route("/admin/import_takeon_users", methods=["POST"])
def api_import_takeon_users():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True)
    if not isinstance(data, list):
        return jsonify({"error": "expected list of user rows"}), 400

    # Data format expected:
    # [
    #   {
    #     "wa_id": "27821234567",
    #     "name": "John Doe",
    #     "role": "sub",
    #     "subcontractor_name": "BrickBuild Co",
    #     "project_code": "PRJ001"
    #   },
    #   ...
    # ]

    inserted = 0
    with SessionLocal() as s:
        # clear existing
        s.query(User).delete()

        for row in data:
            u = User(
                wa_id=str(row.get("wa_id", "")).strip(),
                name=(row.get("name") or "").strip(),
                role=(row.get("role") or "").strip().lower(),
                subcontractor_name=(row.get("subcontractor_name") or "").strip() or None,
                project_code=(row.get("project_code") or "").strip() or None,
                phone=str(row.get("wa_id", "")).strip(),  # store same for now
                active=True,
            )
            s.add(u)
            inserted += 1

        s.commit()

    return jsonify({"status": "ok", "imported": inserted}), 200

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

# === DIGEST SCAFFOLDS (sandbox only) =================================
@app.route("/admin/digest/pm", methods=["GET"])
def admin_digest_pm():
    if not _check_admin(): return _auth_fail()

    pm_wa = request.args.get("pm") or ""
    if not pm_wa:
        return jsonify({"error": "missing pm"}), 400

    from storage import SessionLocal, User, PMProjectMap, Task

    with SessionLocal() as s:
        pm = s.query(User).filter(User.wa_id == pm_wa, User.active == True).first()
        if not pm or pm.role != "pm":
            return jsonify({"error": "not a pm"}), 400

        proj_rows = (
            s.query(PMProjectMap.project_code)
            .filter(PMProjectMap.pm_user_id == pm.id)
            .all()
        )
        projects = [r.project_code for r in proj_rows]
        if not projects:
            return jsonify({"pm": pm.name, "tasks": []}), 200

        tasks = (
            s.query(Task)
            .filter(Task.project_code.in_(projects))
            .order_by(Task.id.desc())
            .limit(200)
            .all()
        )

        resp = []
        for t in tasks:
            resp.append({
                "id": t.id,
                "project": t.project_code,
                "tag": t.tag,
                "text": t.text,
                "status": t.status,
                "ts": t.ts.isoformat() if t.ts else None
            })

        return jsonify({"pm": pm.name, "projects": projects, "tasks": resp}), 200

@app.route("/admin/digest/sub", methods=["GET"])
def admin_digest_sub():
    if not _check_admin(): return _auth_fail()

    sub_wa = request.args.get("sender") or ""
    if not sub_wa:
        return jsonify({"error": "missing sender"}), 400

    from storage import SessionLocal, User, Task

    with SessionLocal() as s:
        sub = s.query(User).filter(User.wa_id == sub_wa, User.active == True).first()
        if not sub or sub.role != "sub":
            return jsonify({"error": "not a subcontractor"}), 400

        tasks = (
            s.query(Task)
            .filter(Task.sender == sub_wa)
            .order_by(Task.id.desc())
            .limit(200)
            .all()
        )

        resp = []
        for t in tasks:
            resp.append({
                "id": t.id,
                "project": t.project_code,
                "tag": t.tag,
                "text": t.text,
                "status": t.status,
                "ts": t.ts.isoformat() if t.ts else None
            })

        return jsonify({"sub": sub.name, "tasks": resp}), 200

# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

if __name__=="__main__":
    port=int(os.environ.get("PORT","10000"))
    app.run(host="0.0.0.0",port=port,debug=False)

