import re
from storage import upsert_task, now_iso, append_event

def parse_text(msg: str):
 # Sandbox codes
 m = re.match(r"^D(\d+)$", msg, re.I)
 if m: return {"task_id": int(m.group(1)), "status": "done"}
 m = re.match(r"^DL(\d+)\s+(\d+)([dh])$", msg, re.I)
 if m: return {"task_id": int(m.group(1)), "status": "delayed", "notes": f"Delay {m.group(2)}{m.group(3)}"}
 m = re.match(r"^CO(\d+)\s+(.+)$", msg, re.I)
 if m: return {"task_id": int(m.group(1)), "change_orders": m.group(2)}
 m = re.match(r"^ETA(\d+)\s+(\d{1,2}:\d{2})$", msg, re.I)
 if m: return {"task_id": int(m.group(1)), "eta": m.group(2)}
 m = re.match(r"^N(\d+)\s+(.+)$", msg, re.I)
 if m: return {"task_id": int(m.group(1)), "notes": m.group(2)}

 # Production-style natural language (when we have wa_id routing)
 m = re.match(r"^done\b", msg, re.I)
 if m: return {"_natural": True, "status": "done"}
 m = re.match(r"^delay\s+(\d+)\s*(days?|d|hours?|h)\b", msg, re.I)
 if m: return {"_natural": True, "status": "delayed", "notes": f"Delay {m.group(1)}{m.group(2)[0].lower()}"}
 m = re.match(r"^change\s*order\b[:\s]*(.+)$", msg, re.I)
 if m: return {"_natural": True, "change_orders": m.group(1)}
 m = re.match(r"^eta\s+(\d{1,2}:\d{2})$", msg, re.I)
 if m: return {"_natural": True, "eta": m.group(1)}
 return None

def apply_action(action: dict, fallback_task_id=None):
 task_id = action.get("task_id") or fallback_task_id
 if not task_id:
   return False
 row = {
   "task_id": task_id,
   "project": None, "trade": None, "assignee_alias": None,
   "due_date": None, "status": action.get("status"),
   "eta": action.get("eta"), "notes": action.get("notes"),
   "evidence_urls": None, "change_orders": action.get("change_orders"),
   "last_update_ts": now_iso()
 }
 upsert_task(row)
 append_event(task_id, "update", action)
 return True
