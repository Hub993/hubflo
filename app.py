# app_v6_1.py — Hubflo V6.1 working
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

from storage_v6_1 import (
    init_db, create_task, get_tasks, get_summary,
    mark_done, approve_task, reject_task, set_order_state,
    revoke_last, subcontractor_accuracy,
    create_meeting, start_meeting, close_meeting,
    create_stock_item, adjust_stock, get_stock_report,
    record_change_order,
    add_task_to_group, get_group_children, edit_task_text,
    get_all_change_orders, create_call_reminder
)

from storage_v6_1 import Task

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

_PHASE_DIGEST_TOGGLE = {}

# ---------------------------------------------------------------------
# Boot DB
# ---------------------------------------------------------------------
init_db()


# ============================================================
# HUBFLO INTEGRITY PATCH — CANONICAL HEARTBEAT (v6 unified)
# ============================================================
from sqlalchemy import text
from storage_v6_1 import (
    SessionLocal, hygiene_pin, hygiene_guard, SystemState
)

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

# >>> PATCH_CLASSIFIER_V6_1_START — NATURAL LANGUAGE REBUILD (V6.1) <<<

import re

def classify_message(text: str) -> dict:
    """
    Natural-language classifier restored to V6 behaviour.
    No hashtags, no rigid keywords, free-flow chat only.
    Returns:
        { "tag": "...", "subtype": "...", "order_state": "..." }
    """

    global SENDER_GLOBAL
    t = (text or "").lower().strip()

    # -----------------------------
    # CHANGE ORDER (requires an existing open order)
    # -----------------------------
    if (
        "change the order" in t
        or "change that order" in t
        or "change order" in t
        or "change it to" in t
        or "change it" in t
    ):
        open_order = None
        try:
            from storage_v6_1 import SessionLocal, Task
            with SessionLocal() as s:
                open_order = (
                    s.query(Task)
                    .filter(
                        Task.sender == SENDER_GLOBAL,
                        Task.status == "open",
                        Task.tag == "order"
                    )
                    .order_by(Task.id.desc())
                    .first()
                )
        except Exception:
            open_order = None

        if open_order:
            return {
                "tag": "change",
                "subtype": "assigned",
                "order_state": "change_requested"
            }
        else:
            # No existing order → treat as a normal task
            return {
                "tag": "task",
                "subtype": "assigned",
                "order_state": None
            }

    # -----------------------------
    # APPROVE / REJECT (for an order)
    # -----------------------------
    if "approve" in t:
        return {"tag": "task", "subtype": "assigned", "order_state": "approve"}

    if "reject" in t:
        return {"tag": "task", "subtype": "assigned", "order_state": "reject"}

    # -----------------------------
    # ORDER DETECTION (free-language)
    # -----------------------------
    order_phrases = [
        r"\bget me\b",
        r"\bgrab\b",
        r"\border\b",
        r"\bwe need\b",
        r"\bbring\b",
        r"\bdrop\b",
        r"\bdeliver\b",
        r"\bsupplier\b",
        r"\bquantity\b",
        r"\bdelivery\b",
        r"\bdrop location\b",
    ]
    if any(re.search(p, t) for p in order_phrases):
        return {
            "tag": "order",
            "subtype": "assigned",
            "order_state": "requested",
        }

    # -----------------------------
    # URGENT
    # -----------------------------
    if "urgent" in t or "asap" in t:
        return {"tag": "urgent", "subtype": "assigned", "order_state": None}

    # -----------------------------
    # DEFAULT = TASK
    # Self-tasks when "I will / I'm going to"
    # -----------------------------
    if t.startswith("i will") or t.startswith("i'm going to"):
        return {"tag": "task", "subtype": "self", "order_state": None}

    return {"tag": "task", "subtype": "assigned", "order_state": None}

# >>> PATCH_CLASSIFIER_V6_1_END <<<

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
        log.info("WEBHOOK_INBOUND: %s", json.dumps(raw)[:500])
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
        if text and not any(w in text.lower() for w in (
            "approve", "reject",
            "change the order", "change that order",
            "change order", "change it", "change it to"
        )):
            with SessionLocal() as s:
                awaiting = (
                    s.query(Task)
                    .filter(
                        Task.sender == sender,
                        Task.status == "open",
                        Task.tag == "order",
                        Task.text.like("[await:%]%")
                    )
                    .order_by(Task.id.desc())
                    .first()
                )

                if awaiting:
                    raw_txt = text.strip()
                    await_lower = awaiting.text.lower()

                    # --- parse existing structured fields -----------------------
                    body = awaiting.text.split("]", 1)[1].strip()
                    lines = [l.strip() for l in body.splitlines() if l.strip()]
                    fields = {}
                    for l in lines:
                        if ":" in l:
                            k, v = l.split(":", 1)
                            fields[k.strip()] = v.strip()

                    item     = fields.get("Item", "")
                    qty      = fields.get("Quantity", "")
                    supplier = fields.get("Supplier", "")
                    ddate    = fields.get("Delivery Date", "")
                    drop_loc = fields.get("Drop Location", "")

                    # --- ITEM ---------------------------------------------------
                    if await_lower.startswith("[await:item]"):
                        awaiting.text = (
                            "[await:quantity]\n"
                            f"Item: {raw_txt}"
                        )
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Quantity?")
                        return ("", 200)

                    # --- QUANTITY -----------------------------------------------
                    if await_lower.startswith("[await:quantity]"):
                        awaiting.text = (
                            "[await:supplier]\n"
                            f"Item: {item}\n"
                            f"Quantity: {raw_txt}"
                        )
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Supplier?")
                        return ("", 200)

                    # --- SUPPLIER -----------------------------------------------
                    if await_lower.startswith("[await:supplier]"):
                        awaiting.text = (
                            "[await:delivery_date]\n"
                            f"Item: {item}\n"
                            f"Quantity: {qty}\n"
                            f"Supplier: {raw_txt}"
                        )
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Delivery date?")
                        return ("", 200)

                    # --- DELIVERY DATE ------------------------------------------
                    if await_lower.startswith("[await:delivery_date]"):
                        awaiting.text = (
                            "[await:drop_location]\n"
                            f"Item: {item}\n"
                            f"Quantity: {qty}\n"
                            f"Supplier: {supplier}\n"
                            f"Delivery Date: {raw_txt}"
                        )
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "Drop location on site?")
                        return ("", 200)

                    # --- DROP LOCATION ------------------------------------------
                    if await_lower.startswith("[await:drop_location]"):
                        awaiting.text = (
                            f"Item: {item}\n"
                            f"Quantity: {qty}\n"
                            f"Supplier: {supplier}\n"
                            f"Delivery Date: {ddate}\n"
                            f"Drop Location: {raw_txt}"
                        )
                        awaiting.status = "pending_approval"
                        awaiting.last_updated = dt.datetime.utcnow()
                        s.commit()
                        send_whatsapp_text(phone_id, sender, "✅ Order details captured. Awaiting PM approval.")
                        return ("", 200)

        # === END AWAIT FOLLOW-UP ==============================================

        # === CLASSIFICATION (V6.1 unified) =====================================

        # PATCH: bind sender globally for classifier patches
        global SENDER_GLOBAL
        SENDER_GLOBAL = sender

        cls = classify_message(text or "")
        tag = cls.get("tag")
        subtype = cls.get("subtype")
        order_state = cls.get("order_state")

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

        # send interactive buttons when explicitly enabled (prod only)
        if tag == "order" and os.environ.get("ENABLE_BUTTONS") == "1":
            try:
                send_order_checklist(phone_id, sender, row["id"])
            except Exception:
                pass
            return ("", 200)

        # Disable sandbox fallback whenever an await chain already exists
        with SessionLocal() as s:
            active_await = (
                s.query(Task)
                 .filter(
                     Task.sender == sender,
                     Task.status == "open",
                     Task.tag == "order",
                     Task.text.like("[await:%]%")
                 )
                 .first()
            )

        if active_await:
            # Skip sandbox fallback entirely
            pass
        else:
            # sandbox fallback stays available
            SANDBOX_OK = True

        # === SANDBOX ORDER FALLBACK (no interactive buttons) ===================
        if tag == "order" and "[await:" not in (text or "").lower():
            with SessionLocal() as s:
                t = s.get(Task, row["id"])
                if t and not (t.text or "").lower().startswith("[await:item]"):
                    t.text = f"[await:item] {t.text}"
                    s.commit()
            send_whatsapp_text(phone_id, sender, "Item?")
            return ("", 200)

    # --- FALLBACK RETURN (ensures webhook always returns 200) ---
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
        "<tr><th>ID</th><th>Time</th><th>Sender</th><th>Client</th><th>Tag</th>"
        "<th>Status</th><th>Order State</th>"
        "<th>Cost ($)</th><th>Time Impact (days)</th><th>Approval Req</th>"
        "<th>Text</th></tr>"
    )
    trs = []
    for r in rows:
        # NEW: derive client-display (safe)
        client_display = r.get('project_code') or ""
        trs.append(
            f"<tr>"
            f"<td>{r['id']}</td>"
            f"<td>{h(r['ts'])}</td>"
            f"<td>{h(r.get('sender') or '')}</td>"
            f"<td>{h(client_display)}</td>"
            f"<td>{h(r.get('tag') or '')}</td>"
            f"<td>{h(r.get('status') or '')}</td>"
            f"<td>{h(r.get('order_state') or '')}</td>"
            f"<td>{h(str(r.get('cost') or ''))}</td>"
            f"<td>{h(str(r.get('time_impact_days') or ''))}</td>"
            f"<td>{'✅' if r.get('approval_required') else ''}</td>"
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

@app.get("/admin/json")
def admin_json():
    token = request.args.get("token", "")
    if token != ADMIN_TOKEN:
        return jsonify({"error": "unauthorized"}), 403

    from storage import get_summary
    return jsonify(get_summary())

@app.route("/admin/view.json")
def admin_view_json():
    token = request.args.get("token")
    if token != ADMIN_TOKEN:
        return jsonify([])

    limit = int(request.args.get("limit", 50))

    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .order_by(Task.id.desc())
            .limit(limit)
            .all()
        )

    out = []
    for r in rows:
        out.append({
            "id": r.id,
            "ts": r.ts,
            "sender": r.sender,
            "text": r.text,
            "tag": r.tag,
            "subtype": r.subtype,
            "order_state": r.order_state,
            "cost": r.cost,
            "time_impact_days": r.time_impact_days,
            "approval_required": r.approval_required,
            "status": r.status,
            "project_code": r.project_code,
            "subcontractor_name": r.subcontractor_name,
            "approved_at": r.approved_at,
            "rejected_at": r.rejected_at,
            "completed_at": r.completed_at,
            "started_at": r.started_at,
            "due_date": r.due_date,
            "overrun_days": r.overrun_days,
            "is_rework": r.is_rework,
            "attachment": {
                "name": r.attachment_name,
                "mime": r.attachment_mime,
                "url": r.attachment_url,
            } if r.attachment_url else None,
            "attachment_url": r.attachment_url,
            "last_updated": r.last_updated,
        })

    return jsonify(out)

# >>> PATCH_11_APP_START — SUPPLIER DIRECTORY <<<

@app.route("/admin/supplier/create", methods=["POST"])
def admin_supplier_create():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    from storage_v6_1 import supplier_create
    result = supplier_create(data)
    return jsonify(result)

@app.route("/admin/suppliers", methods=["GET"])
def admin_supplier_list():
    if not _check_admin():
        return _auth_fail()

    from storage_v6_1 import supplier_list
    result = supplier_list()
    return jsonify(result)

# >>> PATCH_11_APP_END <<<

# ---------------------------------------------------------------------
# Admin action routes (parity with v5)
# ---------------------------------------------------------------------

# >>> PATCH_14_APP_START — CRITICAL FLAGS IN DIGESTS <<<

def _task_is_critical_for_digest(t: dict) -> bool:
    """
    Mirrors storage.is_task_critical but operates on the
    already-serialized task dictionaries passed into digest builders.
    """
    cost = t.get("cost")
    time_impact = t.get("time_impact_days")
    approval = t.get("approval_required")

    if cost and cost >= 1000:
        return True
    if time_impact and time_impact >= 3:
        return True
    if approval:
        return True
    return False

# >>> PATCH_14_APP_END <<<

# >>> PATCH_3_APP_START — INLINE TASK TEXT EDIT <<<

@app.route("/admin/task/edit", methods=["POST"])
def admin_task_edit():
    token = request.args.get("token", "").strip()
    if token != ADMIN_TOKEN:
        return {"error": "unauthorized"}, 401

    data = request.get_json(force=True, silent=True) or {}
    tid = data.get("task_id")
    new_text = data.get("new_text")
    actor = data.get("actor")

    if not tid or not new_text:
        return {"error": "missing fields"}, 400

    from storage_v6_1 import edit_task_text
    result = edit_task_text(tid, new_text, actor)

    return jsonify(result)

# >>> PATCH_3_APP_END <<<

@app.route("/admin/task/find", methods=["GET"])
def admin_task_find():
    if not _check_admin():
        return _auth_fail()

    tid = request.args.get("id", "").strip()
    if not tid.isdigit():
        return jsonify({"error": "invalid id"}), 400

    with SessionLocal() as s:
        t = s.get(Task, int(tid))
        if not t:
            return jsonify({"error": "not found"}), 404

        return jsonify({
            "id": t.id,
            "sender": t.sender,
            "text": t.text,
            "tag": t.tag,
            "status": t.status,
            "project_code": t.project_code,
            "subcontractor_name": t.subcontractor_name,
            "ts": t.ts.isoformat() if t.ts else None,
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        }), 200

@app.route("/admin/task/recent", methods=["GET"])
def admin_task_recent():
    if not _check_admin():
        return _auth_fail()

    limit = request.args.get("limit", "20").strip()
    if not limit.isdigit():
        limit = "20"

    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .order_by(Task.id.desc())
            .limit(int(limit))
            .all()
        )

        out = []
        for t in rows:
            out.append({
                "id": t.id,
                "sender": t.sender,
                "text": t.text,
                "tag": t.tag,
                "status": t.status,
                "project_code": t.project_code,
                "subcontractor_name": t.subcontractor_name,
                "ts": t.ts.isoformat() if t.ts else None,
            })

    return jsonify({"tasks": out, "count": len(out)}), 200

# >>> PATCH_19_APP_START — SIMPLE TASK SEARCH (DEBUG SAFE) <<<

@app.route("/admin/task/search", methods=["GET"])
def admin_task_search():
    if not _check_admin():
        return _auth_fail()

    q = (request.args.get("q") or "").strip().lower()
    if not q:
        return jsonify({"error": "missing q"}), 400

    with SessionLocal() as s:
        rows = (
            s.query(Task)
            .filter(Task.text.ilike(f"%{q}%"))
            .order_by(Task.id.desc())
            .limit(50)
            .all()
        )

    out = []
    for t in rows:
        out.append({
            "id": t.id,
            "sender": t.sender,
            "text": t.text,
            "tag": t.tag,
            "status": t.status,
            "project_code": t.project_code,
            "subcontractor_name": t.subcontractor_name,
            "ts": t.ts.isoformat() if t.ts else None,
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        })

    return jsonify({"count": len(out), "results": out}), 200

# >>> PATCH_19_APP_END <<<

# >>> PATCH_20_APP_START — RAW TASK DEBUG DUMP (ADMIN ONLY) <<<

@app.route("/admin/task/raw", methods=["GET"])
def admin_task_raw():
    if not _check_admin():
        return _auth_fail()

    tid = request.args.get("id", "").strip()
    if not tid.isdigit():
        return jsonify({"error": "invalid id"}), 400

    with SessionLocal() as s:
        t = s.get(Task, int(tid))
        if not t:
            return jsonify({"error": "not found"}), 404

        # Serialize *every* field, raw
        return jsonify({
            "id": t.id,
            "sender": t.sender,
            "text": t.text,
            "tag": t.tag,
            "subtype": t.subtype,
            "status": t.status,
            "order_state": t.order_state,
            "project_code": t.project_code,
            "subcontractor_name": t.subcontractor_name,
            "ts": t.ts.isoformat() if t.ts else None,
            "started_at": t.started_at.isoformat() if t.started_at else None,
            "completed_at": t.completed_at.isoformat() if t.completed_at else None,
            "approved_at": t.approved_at.isoformat() if t.approved_at else None,
            "rejected_at": t.rejected_at.isoformat() if t.rejected_at else None,
            "due_date": t.due_date.isoformat() if t.due_date else None,
            "overrun_days": t.overrun_days,
            "is_rework": t.is_rework,
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
            "attachment_name": t.attachment_name,
            "attachment_mime": t.attachment_mime,
            "attachment_url": t.attachment_url,
            "last_updated": t.last_updated.isoformat() if t.last_updated else None
        }), 200

# >>> PATCH_20_APP_END <<<

@app.route("/admin/task_group/add", methods=["POST"])
def admin_task_group_add():
    token = request.args.get("token", "").strip()
    if token != ADMIN_TOKEN:
        return {"error": "unauthorized"}, 401

    data = request.get_json(force=True, silent=True) or {}
    parent_id = data.get("parent_id")
    child_id = data.get("child_id")
    actor = data.get("actor", "admin")

    if not parent_id or not child_id:
        return {"error": "missing fields"}, 400

    from storage_v6_1 import add_task_to_group
    result = add_task_to_group(int(parent_id), int(child_id), actor)
    return jsonify(result)

@app.route("/admin/task_group/children", methods=["GET"])
def admin_task_group_children():
    token = request.args.get("token", "").strip()
    if token != ADMIN_TOKEN:
        return {"error": "unauthorized"}, 401

    parent_id = request.args.get("parent_id")
    if not parent_id:
        return {"error": "missing parent_id"}, 400

    from storage_v6_1 import get_group_children
    kids = get_group_children(int(parent_id))
    return jsonify({"parent_id": int(parent_id), "children": kids})

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

# === CALL-ACTION TEMPLATES (ADMIN ONLY) ================================
@app.route("/admin/call/templates", methods=["GET"])
def admin_call_templates():
    if not _check_admin():
        return _auth_fail()

    templates = [
        {
            "id": "call_supplier",
            "label": "Call supplier",
            "description": "Use for chasing materials, deliveries or clarifications with suppliers."
        },
        {
            "id": "call_pm",
            "label": "Call PM",
            "description": "Use for coordination calls between subcontractor and project manager."
        },
        {
            "id": "call_owner",
            "label": "Call owner",
            "description": "Use for high-level issues requiring owner or director attention."
        },
    ]
    return jsonify({"status": "ok", "templates": templates}), 200
# ======================================================================

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

# >>> PATCH_8_APP_START — INLINE CHANGE-ORDER EDIT (AUDIT SAFE) <<<

@app.route("/admin/change_order/edit", methods=["POST"])
def api_change_order_edit():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    tid = data.get("task_id")
    fields = data.get("fields") or {}

    if not tid:
        return jsonify({"error": "missing task_id"}), 400

    from storage import SessionLocal, Task, log_audit

    editable = {"cost", "time_impact_days", "approval_required"}

    with SessionLocal() as s:
        t = s.get(Task, int(tid))
        if not t:
            return jsonify({"error": "task not found"}), 404

        before = {
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        }

        # apply safe edits
        for k, v in fields.items():
            if k not in editable:
                continue
            if k == "approval_required":
                setattr(t, k, bool(v))
            else:
                try:
                    setattr(t, k, float(v) if v is not None else None)
                except:
                    pass

        s.commit(); s.refresh(t)

        after = {
            "cost": t.cost,
            "time_impact_days": t.time_impact_days,
            "approval_required": t.approval_required,
        }

        details = json.dumps({"before": before, "after": after}, default=str)
        log_audit("admin", "change_order_edit", "task", t.id, details=details)

        return jsonify({
            "status": "ok",
            "task_id": t.id,
            "before": before,
            "after": after
        }), 200

# >>> PATCH_8_APP_END <<<

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

# === PM ↔ PROJECT ASSIGNMENT (ADMIN) =================================
@app.route("/admin/assign_pm", methods=["POST"])
def admin_assign_pm():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True, silent=True) or {}
    pm_wa = data.get("pm_wa", "").strip()
    project_code = data.get("project_code", "").strip()

    if not pm_wa or not project_code:
        return jsonify({"error": "missing pm_wa or project_code"}), 400

    from storage import SessionLocal, User, PMProjectMap

    with SessionLocal() as s:
        pm = (
            s.query(User)
            .filter(User.wa_id == pm_wa, User.active == True)
            .first()
        )
        if not pm or pm.role != "pm":
            return jsonify({"error": "not a valid pm"}), 400

        existing = (
            s.query(PMProjectMap)
            .filter(PMProjectMap.pm_user_id == pm.id,
                    PMProjectMap.project_code == project_code)
            .first()
        )
        if not existing:
            m = PMProjectMap(pm_user_id=pm.id, project_code=project_code, primary_pm=True)
            s.add(m)
            s.commit()

        return jsonify({"status": "ok", "pm": pm_wa, "project_code": project_code}), 200

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

        tasks = (
            s.query(Task)
            .filter(Task.project_code.in_(projects), Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        lines = [f"📋 Daily PM Digest for {pm.name}"]
        for t in tasks:
            label = f"[{t.tag.upper()}]" if t.tag else ""
            cost = f" | 💲{t.cost}" if t.cost is not None else ""
            time_imp = f" | ⏱{t.time_impact_days}d" if t.time_impact_days is not None else ""
            approval = " | ✅Approval" if t.approval_required else ""
            lines.append(f"- ({t.id}) {label} {t.text}{cost}{time_imp}{approval}")

        return jsonify({
            "preview_text": "\n".join(lines),
            "total_open": len(tasks),
            "projects": projects
        }), 200

@app.route("/admin/digest/pm/send", methods=["POST"])
def admin_digest_pm_send():
    if not _check_admin(): 
        return _auth_fail()

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

        tasks = (
            s.query(Task)
            .filter(Task.project_code.in_(projects), Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        if not tasks:
            return jsonify({"status": "no-open-tasks", "sent_to": pm_wa}), 200

        lines = [f"📋 Daily PM Digest for {pm.name}"]
        for t in tasks:
            label = f"[{t.tag.upper()}]" if t.tag else ""
            extra = []
            if t.cost: extra.append(f"${t.cost:.2f}")
            if t.time_impact_days: extra.append(f"{t.time_impact_days} d")
            if t.approval_required: extra.append("⚠ Approval")
            note = f" ({', '.join(extra)})" if extra else ""
            lines.append(f"- ({t.id}) {label} {t.text}{note}")
        message = "\n".join(lines)

        # Sandbox-safe send
        log.info(f"DAILY_PM_DIGEST_SEND_SANDBOX → {pm_wa}: {message}")

        return jsonify({"status": "ok", "sent_to": pm_wa}), 200

@app.route("/admin/digest/sub", methods=["GET"])
def admin_digest_sub():
    if not _check_admin(): 
        return _auth_fail()

    sub_wa = request.args.get("sender") or ""
    if not sub_wa:
        return jsonify({"error": "missing sender"}), 400

    from storage import SessionLocal, User, Task

    with SessionLocal() as s:
        sub = (
            s.query(User)
            .filter(User.wa_id == sub_wa, User.active == True)
            .first()
        )
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
                "subtype": t.subtype,
                "text": t.text,
                "status": t.status,
                "cost": t.cost,
                "time_impact_days": t.time_impact_days,
                "approval_required": t.approval_required,
                "ts": t.ts.isoformat() if t.ts else None
            })

        return jsonify({"sub": sub.name, "tasks": resp}), 200


@app.route("/admin/digest/sub/preview", methods=["GET"])
def admin_digest_sub_preview():
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
            .filter(Task.sender == sub_wa, Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        lines = [f"📋 Daily Tasks for {sub.name} ({sub.subcontractor_name or 'No Company'})"]
        for t in tasks:
            extra = []
            if t.cost: extra.append(f"${t.cost:.2f}")
            if t.time_impact_days: extra.append(f"{t.time_impact_days} d")
            if t.approval_required: extra.append("⚠ Approval")
            note = f" ({', '.join(extra)})" if extra else ""
            lines.append(f"- ({t.id}) {t.text}{note}")

        return jsonify({
            "preview_text": "\n".join(lines),
            "total_open": len(tasks)
        }), 200

@app.route("/admin/digest/sub/send", methods=["POST"])
def admin_digest_sub_send():
    if not _check_admin(): 
        return _auth_fail()

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
            .filter(Task.sender == sub_wa, Task.status == "open")
            .order_by(Task.id.asc())
            .all()
        )

        lines = [f"📋 Daily Tasks for {sub.name} ({sub.subcontractor_name or 'No Company'})"]
        for t in tasks:
            extra = []
            if t.cost: extra.append(f"${t.cost:.2f}")
            if t.time_impact_days: extra.append(f"{t.time_impact_days} d")
            if t.approval_required: extra.append("⚠ Approval")
            note = f" ({', '.join(extra)})" if extra else ""
            lines.append(f"- ({t.id}) {t.text}{note}")

        message = "\n".join(lines)

    # No real send (sandbox). Just log/acknowledge success.
    log.info(f"DAILY_DIGEST_SEND_SANDBOX → {sub_wa}: {message}")
    return jsonify({"status": "ok", "sent_to": sub_wa}), 200

import threading
import time
import pytz
from datetime import datetime
from storage import SessionLocal, User, Task

def daily_digest_scheduler():
    while True:
        now_utc = datetime.utcnow()

        with SessionLocal() as s:
            subs = s.query(User).filter(User.role == "sub", User.active == True).all()

            for sub in subs:
                tzname = sub.timezone or "America/New_York"
                try:
                    tz = pytz.timezone(tzname)
                except:
                    tz = pytz.timezone("America/New_York")

                local_now = now_utc.replace(tzinfo=pytz.utc).astimezone(tz)

                # Only fire at exactly 06:00 local, minutes only (safe in 1-min cycle)
                if local_now.hour == 6 and local_now.minute == 0:

                    # fetch open tasks
                    tasks = (
                        s.query(Task)
                        .filter(Task.sender == sub.wa_id, Task.status == "open")
                        .order_by(Task.id.asc())
                        .all()
                    )

                    # If no open tasks → send nothing (silent skip)
                    if not tasks:
                        continue

                    # Build message
                    lines = [f"📋 Daily Tasks for {sub.name} ({sub.subcontractor_name or 'No Company'})"]
                    for t in tasks:
                        lines.append(f"- ({t.id}) {t.text}")
                    message = "\n".join(lines)

                    # Sandbox-safe "send"
                    log.info(f"DAILY_DIGEST_AUTO_SEND → {sub.wa_id}: {message}")

        time.sleep(60)


# start scheduler thread (daemon)
threading.Thread(target=daily_digest_scheduler, daemon=True).start()

def daily_pm_digest_scheduler():
    while True:
        now_utc = datetime.utcnow()

        with SessionLocal() as s:
            pms = s.query(User).filter(User.role == "pm", User.active == True).all()

            for pm in pms:
                tzname = pm.timezone or "America/New_York"
                try:
                    tz = pytz.timezone(tzname)
                except:
                    tz = pytz.timezone("America/New_York")

                local_now = now_utc.replace(tzinfo=pytz.utc).astimezone(tz)

                # Trigger at exactly 18:00 local
                if local_now.hour == 18 and local_now.minute == 0:
                    # sandbox-safe auto send
                    # one-per-day guard
                    state_key = f"pm_digest_{pm.wa_id}_{local_now.strftime('%Y-%m-%d')}"
                    if os.environ.get(state_key) == "sent":
                        continue
                    os.environ[state_key] = "sent"
                    log.info(f"DAILY_PM_DIGEST_AUTO_SEND → {pm.wa_id}")
        time.sleep(60)

threading.Thread(target=daily_pm_digest_scheduler, daemon=True).start()


# ============================================================
# FUTURE VOICE CHANNEL SUPPORT (TWILIO VOICE STUBS)
# ============================================================

@app.route("/voice/inbound", methods=["POST"])
def voice_inbound_stub():
    """
    Stub for future Twilio Voice inbound-call webhook.
    No action performed; logs minimal metadata only.
    """
    payload = request.get_json(silent=True) or {}
    log.info(f"VOICE_INBOUND_STUB: {json.dumps(payload)[:400]}")
    return jsonify({"status": "stub-ok", "direction": "inbound"}), 200


@app.route("/voice/status", methods=["POST"])
def voice_status_stub():
    """
    Stub for future Twilio Voice call-status events:
    ringing, in-progress, completed, failed.
    No action performed; no DB writes yet.
    """
    payload = request.get_json(silent=True) or {}
    log.info(f"VOICE_STATUS_STUB: {json.dumps(payload)[:400]}")
    return jsonify({"status": "stub-ok"}), 200


@app.route("/voice/completed", methods=["POST"])
def voice_completed_stub():
    """
    Stub for future Twilio 'call completed' events.
    Will later write to CallLog.
    Currently does nothing except log.
    """
    payload = request.get_json(silent=True) or {}
    log.info(f"VOICE_COMPLETED_STUB: {json.dumps(payload)[:400]}")
    return jsonify({"status": "stub-ok", "saved": False}), 200

# ============================================================
# MULTI-PHASE DIGEST (TOGGLE SUPPORT)
# ============================================================

@app.route("/admin/digest/pm/phase_toggle", methods=["POST"])
def admin_digest_pm_phase_toggle():
    """
    Toggle per-phase digest mode for a given project.
    Future: stored in DB (currently ephemeral, memory only).
    """
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}
    project = (data.get("project_code") or "").strip()
    enable = bool(data.get("enable"))

    if not project:
        return jsonify({"error": "missing project_code"}), 400

    # In v6.1 this is temporary in-memory toggle
    _PHASE_DIGEST_TOGGLE[project] = enable

    return jsonify({
        "status": "ok",
        "project": project,
        "enabled": enable
    }), 200


@app.route("/admin/digest/pm/phase_status", methods=["GET"])
def admin_digest_pm_phase_status():
    """
    Inspect the current toggle value for a project.
    """
    if not _check_admin():
        return _auth_fail()

    project = (request.args.get("project_code") or "").strip()
    if not project:
        return jsonify({"error": "missing project_code"}), 400

    val = _PHASE_DIGEST_TOGGLE.get(project, False)
    return jsonify({
        "status": "ok",
        "project": project,
        "enabled": val
    }), 200

# ============================================================
# MANUAL SCHEDULER TRIGGER (SLC18 — DRY RUN)
# ============================================================
@app.route("/admin/digest/pm/tick", methods=["POST"])
def admin_digest_pm_tick():
    if not _check_admin(): return _auth_fail()
    log.info("SLC18: MANUAL_PM_DIGEST_TICK")
    return admin_digest_pm_send()

@app.route("/admin/digest/sub/tick", methods=["POST"])
def admin_digest_sub_tick():
    if not _check_admin(): return _auth_fail()
    log.info("SLC18: MANUAL_SUB_DIGEST_TICK")
    # resolve subcontractor WA ID for manual trigger
    sub_wa = request.args.get("sender") or request.args.get("sub") or ""
    if not sub_wa:
        return jsonify({"error": "missing sender"}), 400
    return admin_digest_sub_send()

# ---------------------------------------------------------------------
# Admin Reporting — Aggregated Summary (Phase 2)
# ---------------------------------------------------------------------
@app.route("/admin/report/summary", methods=["GET"])
def admin_report_summary():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func

    with SessionLocal() as s:
        total_tasks = s.query(func.count(Task.id)).scalar() or 0
        open_tasks = s.query(func.count(Task.id)).filter(Task.status == "open").scalar() or 0
        approved = s.query(func.count(Task.id)).filter(Task.status == "approved").scalar() or 0
        rejected = s.query(func.count(Task.id)).filter(Task.status == "rejected").scalar() or 0
        done = s.query(func.count(Task.id)).filter(Task.status == "done").scalar() or 0

        total_cost = s.query(func.sum(Task.cost)).scalar() or 0.0
        total_time_impact = s.query(func.sum(Task.time_impact_days)).scalar() or 0.0

        with_cost = s.query(func.count(Task.id)).filter(Task.cost != None).scalar() or 0
        with_time = s.query(func.count(Task.id)).filter(Task.time_impact_days != None).scalar() or 0

    return jsonify({
        "summary": {
            "total_tasks": total_tasks,
            "open": open_tasks,
            "approved": approved,
            "rejected": rejected,
            "done": done
        },
        "change_orders": {
            "total_cost": round(total_cost, 2),
            "total_time_impact_days": float(total_time_impact),
            "count_with_cost": with_cost,
            "count_with_time_impact": with_time
        },
        "status": "aggregated-ok"
    }), 200

# === ADMIN REPORT DASHBOARD (HTML VIEW) ============================
@app.route("/admin/report/view", methods=["GET"])
def admin_report_view():
    if not _check_admin():
        return _auth_fail()

    # Fetch JSON data from the same summary route
    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_summary", token=request.args.get("token"))
    ).get_json(force=True)

    ch = summary.get("change_orders", {})
    s = summary.get("summary", {})

    body = f"""
    <html><head><title>HubFlo Report Dashboard</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      h2{{margin-top:0}}
      table{{border-collapse:collapse;width:60%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Summary Dashboard</h2>

      <table>
        <tr><th colspan=2>Task Summary</th></tr>
        <tr><td>Total Tasks</td><td>{s.get('total_tasks',0)}</td></tr>
        <tr><td>Open</td><td>{s.get('open',0)}</td></tr>
        <tr><td>Approved</td><td>{s.get('approved',0)}</td></tr>
        <tr><td>Done</td><td>{s.get('done',0)}</td></tr>
        <tr><td>Rejected</td><td>{s.get('rejected',0)}</td></tr>
      </table>

      <table>
        <tr><th colspan=2>Change Orders</th></tr>
        <tr><td>Count w/ Cost</td><td>{ch.get('count_with_cost',0)}</td></tr>
        <tr><td>Count w/ Time Impact</td><td>{ch.get('count_with_time_impact',0)}</td></tr>
        <tr><td>Total Cost ($)</td><td>{ch.get('total_cost',0.0)}</td></tr>
        <tr><td>Total Time Impact (days)</td><td>{ch.get('total_time_impact_days',0.0)}</td></tr>
      </table>

      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# ---------------------------------------------------------------------
# Admin Reporting — Subcontractor Performance (Phase 4)
# ---------------------------------------------------------------------
@app.route("/admin/report/performance", methods=["GET"])
def admin_report_performance():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func, case

    with SessionLocal() as s:
        rows = (
            s.query(
                Task.subcontractor_name,
                func.count(Task.id).label("total"),
                func.sum(case((Task.status == "done", 1), else_=0)).label("done"),
                func.sum(case((Task.status == "approved", 1), else_=0)).label("approved"),
                func.sum(case((Task.status == "rejected", 1), else_=0)).label("rejected"),
                func.sum(case((Task.is_rework.is_(True), 1), else_=0)).label("reworks"),
                func.sum(case(((Task.overrun_days > 0), 1), else_=0)).label("overruns"),
            )
            .group_by(Task.subcontractor_name)
            .order_by(Task.subcontractor_name.asc())
            .all()
        )

        result = []
        for r in rows:
            name = r.subcontractor_name or "(unassigned)"
            total = r.total or 0
            on_time = (r.done or 0) - (r.overruns or 0)
            pct = 0 if total == 0 else round(100.0 * on_time / total, 1)
            result.append({
                "subcontractor": name,
                "total": total,
                "done": r.done or 0,
                "approved": r.approved or 0,
                "rejected": r.rejected or 0,
                "reworks": r.reworks or 0,
                "overruns": r.overruns or 0,
                "accuracy_pct": pct,
            })

    return jsonify({"status": "ok", "performance": result}), 200


# === ADMIN PERFORMANCE DASHBOARD (HTML VIEW) ============================
@app.route("/admin/report/performance/view", methods=["GET"])
def admin_report_performance_view():
    if not _check_admin():
        return _auth_fail()

    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_performance", token=request.args.get("token"))
    ).get_json(force=True)

    rows = summary.get("performance", [])
    body_rows = "".join(
        f"<tr><td>{r['subcontractor']}</td>"
        f"<td>{r['total']}</td>"
        f"<td>{r['done']}</td>"
        f"<td>{r['approved']}</td>"
        f"<td>{r['rejected']}</td>"
        f"<td>{r['reworks']}</td>"
        f"<td>{r['overruns']}</td>"
        f"<td>{r['accuracy_pct']}%</td></tr>"
        for r in rows
    )

    body = f"""
    <html><head><title>HubFlo Performance Report</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      table{{border-collapse:collapse;width:90%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Subcontractor Performance</h2>
      <table>
        <tr>
          <th>Subcontractor</th><th>Total</th><th>Done</th><th>Approved</th>
          <th>Rejected</th><th>Reworks</th><th>Overruns</th><th>Accuracy %</th>
        </tr>
        {body_rows or "<tr><td colspan=8>No data</td></tr>"}
      </table>
      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# ---------------------------------------------------------------------
# Admin Reporting — Per-Project Summary (Phase 5)
# ---------------------------------------------------------------------
@app.route("/admin/report/project", methods=["GET"])
def admin_report_project():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func, case

    with SessionLocal() as s:
        rows = (
            s.query(
                Task.project_code,
                func.count(Task.id).label("total"),
                func.sum(func.coalesce(Task.cost, 0)).label("total_cost"),
                func.sum(func.coalesce(Task.time_impact_days, 0)).label("total_time_impact_days"),
                func.sum(case((Task.status == "open", 1), else_=0)).label("open"),
                func.sum(case((Task.status == "approved", 1), else_=0)).label("approved"),
                func.sum(case((Task.status == "done", 1), else_=0)).label("done"),
                func.sum(case((Task.status == "rejected", 1), else_=0)).label("rejected"),
            )
            .group_by(Task.project_code)
            .order_by(Task.project_code.asc())
            .all()
        )

        result = []
        for r in rows:
            result.append({
                "project_code": r.project_code or "(unassigned)",
                "total_tasks": r.total or 0,
                "open": r.open or 0,
                "approved": r.approved or 0,
                "done": r.done or 0,
                "rejected": r.rejected or 0,
                "total_cost": round(float(r.total_cost or 0), 2),
                "total_time_impact_days": float(r.total_time_impact_days or 0),
            })

    return jsonify({"status": "ok", "projects": result}), 200


# === ADMIN PROJECT SUMMARY DASHBOARD (HTML VIEW) =====================
@app.route("/admin/report/project/view", methods=["GET"])
def admin_report_project_view():
    if not _check_admin():
        return _auth_fail()

    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_project", token=request.args.get("token"))
    ).get_json(force=True)

    rows = summary.get("projects", [])

    body_rows = ""
    for r in rows:
        body_rows += (
            f"<tr><td>{r['project_code']}</td>"
            f"<td>{r['total_tasks']}</td>"
            f"<td>{r['open']}</td>"
            f"<td>{r['approved']}</td>"
            f"<td>{r['done']}</td>"
            f"<td>{r['rejected']}</td>"
            f"<td>{r['total_cost']}</td>"
            f"<td>{r['total_time_impact_days']}</td></tr>"
        )

    body = f"""
    <html><head><title>HubFlo Project Summary</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      table{{border-collapse:collapse;width:80%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Per-Project Summary</h2>
      <table>
        <tr><th>Project</th><th>Total</th><th>Open</th><th>Approved</th>
            <th>Done</th><th>Rejected</th><th>Total Cost ($)</th><th>Time Impact (days)</th></tr>
        {body_rows if body_rows else "<tr><td colspan=8>No data</td></tr>"}
      </table>
      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# ---------------------------------------------------------------------
# Admin Reporting — Global Overview (Phase 6)
# ---------------------------------------------------------------------
@app.route("/admin/report/overview", methods=["GET"])
def admin_report_overview():
    if not _check_admin():
        return _auth_fail()

    from storage import SessionLocal, Task
    from sqlalchemy import func

    with SessionLocal() as s:
        total_tasks = s.query(func.count(Task.id)).scalar() or 0
        open_tasks = s.query(func.count(Task.id)).filter(Task.status == "open").scalar() or 0
        approved = s.query(func.count(Task.id)).filter(Task.status == "approved").scalar() or 0
        rejected = s.query(func.count(Task.id)).filter(Task.status == "rejected").scalar() or 0
        done = s.query(func.count(Task.id)).filter(Task.status == "done").scalar() or 0

        total_cost = s.query(func.sum(Task.cost)).scalar() or 0.0
        total_time = s.query(func.sum(Task.time_impact_days)).scalar() or 0.0

        total_subs = s.query(func.count(func.distinct(Task.subcontractor_name))).scalar() or 0
        total_projects = s.query(func.count(func.distinct(Task.project_code))).scalar() or 0

    return jsonify({
        "summary": {
            "total_tasks": total_tasks,
            "open": open_tasks,
            "approved": approved,
            "done": done,
            "rejected": rejected,
        },
        "totals": {
            "projects": total_projects,
            "subcontractors": total_subs,
            "total_cost": round(total_cost, 2),
            "total_time_impact_days": float(total_time),
        },
        "status": "ok"
    }), 200


# === ADMIN OVERVIEW DASHBOARD (HTML VIEW) ============================
@app.route("/admin/report/overview/view", methods=["GET"])
def admin_report_overview_view():
    if not _check_admin():
        return _auth_fail()

    from flask import url_for
    summary = app.test_client().get(
        url_for("admin_report_overview", token=request.args.get("token"))
    ).get_json(force=True)

    s = summary.get("summary", {})
    t = summary.get("totals", {})

    body = f"""
    <html><head><title>HubFlo Global Overview</title>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;}}
      table{{border-collapse:collapse;width:50%;margin-top:10px}}
      th,td{{border:1px solid #ccc;padding:6px 10px;font-size:14px;text-align:left}}
      th{{background:#f4f4f4}}
    </style></head><body>
      <h2>HubFlo Global Overview</h2>
      <table>
        <tr><th colspan=2>Task Totals</th></tr>
        <tr><td>Total Tasks</td><td>{s.get('total_tasks',0)}</td></tr>
        <tr><td>Open</td><td>{s.get('open',0)}</td></tr>
        <tr><td>Approved</td><td>{s.get('approved',0)}</td></tr>
        <tr><td>Done</td><td>{s.get('done',0)}</td></tr>
        <tr><td>Rejected</td><td>{s.get('rejected',0)}</td></tr>
      </table>

      <table>
        <tr><th colspan=2>Totals</th></tr>
        <tr><td>Projects</td><td>{t.get('projects',0)}</td></tr>
        <tr><td>Subcontractors</td><td>{t.get('subcontractors',0)}</td></tr>
        <tr><td>Total Cost ($)</td><td>{t.get('total_cost',0.0)}</td></tr>
        <tr><td>Total Time Impact (days)</td><td>{t.get('total_time_impact_days',0.0)}</td></tr>
      </table>

      <p style="margin-top:20px;color:#666;font-size:13px">
        Status: {summary.get('status')}<br>
        Token used: {request.args.get('token','')}
      </p>
    </body></html>
    """
    return Response(body, 200, mimetype="text/html")
# ================================================================

# >>> PATCH_1_APP_START — CALL LOG ENDPOINT <<<

from storage import log_call

@app.route("/admin/voice/log", methods=["POST"])
def admin_voice_log():
    if not _check_admin():
        return _auth_fail()

    data = request.get_json(force=True) or {}

    direction = (data.get("direction") or "").strip().lower()   # inbound | outbound
    from_wa   = (data.get("from") or "").strip()
    to_wa     = (data.get("to") or "").strip()
    duration  = data.get("duration_seconds")
    notes     = data.get("notes")

    if direction not in ("inbound", "outbound"):
        return jsonify({"error": "direction must be inbound|outbound"}), 400

    if not from_wa or not to_wa:
        return jsonify({"error": "missing from or to"}), 400

    try:
        duration = int(duration) if duration is not None else None
    except:
        return jsonify({"error": "invalid duration_seconds"}), 400

    rec = log_call(
        direction=direction,
        from_wa=from_wa,
        to_wa=to_wa,
        duration_seconds=duration,
        notes=notes,
    )

    return jsonify({"status": "ok", "call": rec}), 200

# >>> PATCH_1_APP_END <<<

# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

if __name__=="__main__":
    port=int(os.environ.get("PORT","10000"))
    app.run(host="0.0.0.0",port=port,debug=False)

