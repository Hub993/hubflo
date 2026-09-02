# storage_v6_1.py — thin v6.1 facade over core storage
# ----------------------------------------------------
# Purpose:
# - Re-export the stable v5/v6 storage layer under a fixed name
#   so app_v6_1.py can import everything from here only.

from storage import (
    # Core SQLAlchemy plumbing
    SessionLocal,
    Base,

    # Models
    Task,
    Inspection,
    DelayLog,
    # >>> FEATURE_3_REMINDER_FACADE_MODEL_START — V6.1 <<<
    PMReminder,
    # >>> FEATURE_3_REMINDER_FACADE_MODEL_END <<<
    ConversationState,
    SystemState,
    # Meeting model is used by meeting helpers
    Meeting,
    # User / routing / mapping models
    User,
    SenderMembership,
    CurrentContextSelection,
    MultiContextInboundClaim,
    PMProjectMap,

    # Core task API (v5 base + v6 extensions)
    init_db,
    migrate_inspection_accountability,
    create_task,
    get_tasks,
    get_summary,
    get_personal_responsibilities,
    mark_done,
    approve_task,
    reject_task,
    set_order_state,
    revoke_last,
    subcontractor_accuracy,

    # Inspector scheduling
    create_inspection,

    # Critical-path delay tracking
    log_delay,

    # >>> FEATURE_3_REMINDER_FACADE_API_START — V6.1 <<<
    create_pm_reminder,
    claim_due_pm_reminders,
    complete_pm_reminder_delivery,
    fail_pm_reminder_delivery,
    acknowledge_pm_reminder,
    snooze_pm_reminder,
    redirect_pm_reminder,
    cancel_pm_reminder,
    # >>> FEATURE_3_REMINDER_FACADE_API_END <<<

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

    # Generic persistent conversation state
    save_pending_conversation_state,
    get_pending_conversation_state,
    claim_conversation_state_continuation,
    advance_conversation_state_continuation,
    resolve_conversation_state,
    touch_conversation_state_activity,
    retire_conversation_state,

    # Hygiene / system state
    hygiene_pin,
    hygiene_guard,

    # User/PM/project routing + audit logging
    get_user_role,
    get_pms_for_project,
    log_call,
    log_audit,
    resolve_sender_context,
    set_effective_membership,
    clear_effective_membership,
    commit_context_selection,
    claim_multi_context_inbound,
    release_multi_context_inbound,
    complete_multi_context_inbound,
)
