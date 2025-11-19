# storage_v6_1.py — thin v6.1 facade over core storage
# ----------------------------------------------------
# Purpose:
# - Re-export the stable v5/v6 storage layer under a fixed name
#   so app_v6_1.py can import everything from here only.

from storage import (
    # Core SQLAlchemy plumbing
    engine,
    SessionLocal,
    Base,

    # Models
    Task,
    SystemState,
    # Meeting model is used by meeting helpers
    Meeting,
    # User / routing / mapping models
    User,
    PMProjectMap,

    # Core task API (v5 base + v6 extensions)
    init_db,
    create_task,
    get_tasks,
    get_summary,
    mark_done,
    approve_task,
    reject_task,
    set_order_state,
    revoke_last,
    subcontractor_accuracy,

    # Meetings
    create_meeting,
    start_meeting,
    close_meeting,

    # Stock / materials
    create_stock_item,
    adjust_stock,
    get_stock_report,

    # Change orders
    record_change_order,
    get_all_change_orders,

    # Task grouping / editing
    add_task_to_group,
    get_group_children,
    edit_task_text,

    # Call reminders
    create_call_reminder,

    # Supplier directory
    supplier_create,
    supplier_list,

    # Hygiene / system state
    hygiene_pin,
    hygiene_guard,

    # User/PM/project routing + audit logging
    get_user_role,
    get_pms_for_project,
    log_call,
    log_audit,
)