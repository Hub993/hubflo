"""Read-only, scope-bound Stage 2 operational evidence for Agent reasoning."""

import datetime as dt
from typing import Any, Dict, Iterable, List, Mapping, Optional

import storage

from .contracts import Principal, ProtectedItem, SecurityError


def _value(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return value


def _record(row, fields: Iterable[str]) -> Dict[str, Any]:
    return {field: _value(getattr(row, field, None)) for field in fields}


class OperationalEvidenceAssembler:
    """Read authoritative Stage 2 rows without mutating or retaining them."""

    _TASK_FIELDS = (
        "id", "client_id", "project_code", "sender", "text", "tag", "subtype",
        "status", "due_date", "started_at", "completed_at", "approved_at",
        "rejected_at", "is_rework", "overrun_days", "subcontractor_name",
        "pm_wa_id", "order_state", "cost", "time_impact_days",
        "approval_required", "ts", "last_updated",
    )
    _INSPECTION_FIELDS = (
        "id", "client_id", "project_code", "phase", "required_date",
        "actual_date", "inspector", "accountable_wa", "notes", "created_at",
    )
    _MEETING_FIELDS = (
        "id", "client_id", "project_code", "title", "subcontractor_name",
        "site_name", "scheduled_for", "started_at", "closed_at", "created_by",
        "status", "task_ids",
    )
    _STOCK_FIELDS = (
        "id", "client_id", "project_code", "name", "supplier_name", "unit",
        "current_qty", "min_days_cover", "created_at", "updated_at",
    )
    _DELAY_FIELDS = (
        "id", "client_id", "task_id", "project_code", "reporter", "days",
        "reason", "created_at",
    )

    def __init__(self, session_factory=None, now=None):
        self._session_factory = session_factory or storage.SessionLocal
        self._now = now or dt.datetime.utcnow

    @staticmethod
    def _scope_query(query, model, client_id: int, project_code: Optional[str]):
        query = query.filter(model.client_id == client_id)
        if project_code is not None:
            query = query.filter(model.project_code == project_code)
        return query

    @staticmethod
    def _item(table: str, record_id: int, client_id: int,
              project_code: Optional[str], value: Mapping[str, Any]) -> ProtectedItem:
        reference = "hubflo:%s:%s" % (table, record_id)
        return ProtectedItem(
            reference=reference,
            value=dict(value),
            security_domain="SD3",
            client_id=client_id,
            project_code=project_code,
            classification="authoritative-operational-%s" % table,
            confidentiality="restricted",
            permitted_uses=("reason",),
            provider_eligible=True,
            authoritative=True,
            retention_max_seconds=0,
            provenance={
                "source_ref": "hubflo://%s/%s" % (table, record_id),
                "table": table,
                "record_id": record_id,
            },
        )

    @staticmethod
    def _current_membership(session, principal: Principal,
                            membership: Mapping[str, Any]):
        try:
            membership_id = int(membership["id"])
            user_id = int(membership["user_id"])
        except (KeyError, TypeError, ValueError):
            raise SecurityError("current sender membership is required")
        row = session.query(storage.SenderMembership).filter(
            storage.SenderMembership.id == membership_id,
            storage.SenderMembership.user_id == user_id,
            storage.SenderMembership.active == True,
        ).one_or_none()
        expected_principal = "user:%s" % user_id
        if row is None or principal.principal_id != expected_principal:
            raise SecurityError("sender membership is stale or unauthorized")
        if row.context_kind != "client" or row.client_id is None:
            raise SecurityError("client operational context is required")
        if int(row.client_id) != principal.scope.client_id:
            raise SecurityError("sender membership client scope changed")
        if (row.project_code or None) != (principal.scope.project_code or None):
            raise SecurityError("sender membership project scope changed")
        return row

    def assemble(self, principal: Principal,
                 membership: Mapping[str, Any]) -> tuple:
        """Return detached structured records for the current exact client scope."""
        if principal.scope.client_id is None:
            return ()
        client_id = int(principal.scope.client_id)
        project_code = principal.scope.project_code or None
        items: List[ProtectedItem] = []
        now = self._now()

        with self._session_factory() as session:
            self._current_membership(session, principal, membership)

            tasks = self._scope_query(
                session.query(storage.Task), storage.Task, client_id, project_code
            ).order_by(storage.Task.id).all()
            scoped_tasks = {row.id: row for row in tasks}
            for row in tasks:
                value = _record(row, self._TASK_FIELDS)
                terminal = str(row.status or "").lower() in (
                    "completed", "closed", "cancelled", "rejected"
                ) or row.completed_at is not None
                value["derived_due_state"] = (
                    "none" if row.due_date is None else
                    "completed" if terminal else
                    "overdue" if row.due_date < now else
                    "due"
                )
                value["derived_started"] = row.started_at is not None
                value["derived_completed"] = terminal
                value["derived_attention_indicators"] = [
                    name for name, present in (
                        ("overdue", value["derived_due_state"] == "overdue"),
                        ("overrun", bool(row.overrun_days and row.overrun_days > 0)),
                        ("rework", bool(row.is_rework)),
                        ("rejected", row.rejected_at is not None),
                    ) if present
                ]
                items.append(self._item(
                    storage.Task.__tablename__, row.id, client_id,
                    row.project_code or None, value,
                ))

            inspections = self._scope_query(
                session.query(storage.Inspection), storage.Inspection,
                client_id, project_code,
            ).order_by(storage.Inspection.id).all()
            for row in inspections:
                value = _record(row, self._INSPECTION_FIELDS)
                value["derived_outstanding"] = row.actual_date is None
                value["derived_overdue"] = bool(
                    row.actual_date is None and row.required_date and row.required_date < now
                )
                items.append(self._item(
                    storage.Inspection.__tablename__, row.id, client_id,
                    row.project_code or None, value,
                ))

            meetings = self._scope_query(
                session.query(storage.Meeting), storage.Meeting,
                client_id, project_code,
            ).order_by(storage.Meeting.id).all()
            for row in meetings:
                items.append(self._item(
                    storage.Meeting.__tablename__, row.id, client_id,
                    row.project_code or None, _record(row, self._MEETING_FIELDS),
                ))

            stocks = self._scope_query(
                session.query(storage.StockItem), storage.StockItem,
                client_id, project_code,
            ).order_by(storage.StockItem.id).all()
            scoped_stock = {row.id: row for row in stocks}
            for row in stocks:
                items.append(self._item(
                    storage.StockItem.__tablename__, row.id, client_id,
                    row.project_code or None, _record(row, self._STOCK_FIELDS),
                ))

            delays = self._scope_query(
                session.query(storage.DelayLog), storage.DelayLog,
                client_id, project_code,
            ).order_by(storage.DelayLog.id).all()
            for row in delays:
                if row.task_id not in scoped_tasks:
                    continue
                items.append(self._item(
                    storage.DelayLog.__tablename__, row.id, client_id,
                    row.project_code or None, _record(row, self._DELAY_FIELDS),
                ))

            task_ids = tuple(scoped_tasks)
            groups = (
                session.query(storage.TaskGroup).filter(
                    storage.TaskGroup.parent_id.in_(task_ids),
                    storage.TaskGroup.child_id.in_(task_ids),
                ).order_by(storage.TaskGroup.id).all()
                if task_ids else []
            )
            for row in groups:
                parent = scoped_tasks.get(row.parent_id)
                child = scoped_tasks.get(row.child_id)
                relationship_project = (
                    parent.project_code
                    if parent.project_code == child.project_code
                    else None
                )
                items.append(self._item(
                    storage.TaskGroup.__tablename__, row.id, client_id,
                    relationship_project or None,
                    {"id": row.id, "parent_task_id": row.parent_id,
                     "child_task_id": row.child_id,
                     "created_at": _value(row.created_at)},
                ))

            movements = session.query(storage.StockMovement).filter(
                storage.StockMovement.stock_item_id.in_(list(scoped_stock) or [-1])
            ).order_by(storage.StockMovement.id).all()
            for row in movements:
                items.append(self._item(
                    storage.StockMovement.__tablename__, row.id, client_id,
                    scoped_stock[row.stock_item_id].project_code or None,
                    _record(row, ("id", "stock_item_id", "ts", "qty_change",
                                  "related_task_id")),
                ))

        return tuple(items)
