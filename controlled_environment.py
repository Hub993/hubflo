"""Deterministic, isolated Hub/Florida/SA acceptance fixtures.

This module is deliberately a fixture loader, not a second authority layer.
Identity and context resolution use storage.User, SenderMembership and
CurrentContextSelection; Agent state uses AgentRepository.
"""

import datetime as dt

import storage
from agent_layer.contracts import Principal, Scope
from agent_layer.models import (
    AgentAuthorityGrant, AgentAuthorityValue, AgentCapability,
    AgentConfiguration, AgentEntitlement, AgentPrincipalAuthority,
)
from agent_layer.persistence import AgentRepository, scope_key
from agent_layer.runtime import CAPABILITY_CATALOG


FIXTURE_TAG = "HUBFLO-RTW7-CONTROLLED-1.0"
CHANNEL = "controlled-whatsapp"
SENDERS = {"Neville": "controlled:neville", "Dolan": "controlled:dolan", "Paul": "controlled:paul"}
PROJECTS = ("Site 1", "Site 2", "Site 3")
SA_PROJECTS = ("Branch 1", "Branch 2")


def _now(days=0):
    return dt.datetime.utcnow() + dt.timedelta(days=days)


def _client_ids():
    with storage.SessionLocal() as s:
        rows = s.query(storage.ClientWAIdentity).filter(
            storage.ClientWAIdentity.display_name_for_whatsapp.in_(
                (f"{FIXTURE_TAG}: Florida", f"{FIXTURE_TAG}: SA")
            )
        ).all()
        found = {row.display_name_for_whatsapp.rsplit(": ", 1)[-1]: int(row.client_id)
                 for row in rows}
        if len(found) == 2:
            return found
        used = set()
        # ClientWAIdentity is the fixture's durable reservation.  Scan every
        # accepted client-scoped table only when allocating a missing slot so
        # a partial table scan cannot collide with an unrelated record.
        for table in storage.Base.metadata.sorted_tables:
            if "client_id" not in table.c:
                continue
            for value, in s.execute(table.select().with_only_columns(table.c.client_id)):
                if value is None:
                    continue
                try:
                    used.add(int(value))
                except (TypeError, ValueError):
                    continue
        candidate = max(used or {0}) + 1
        for label in ("Florida", "SA"):
            if label not in found:
                while candidate in used:
                    candidate += 1
                found[label] = candidate
                used.add(candidate)
                candidate += 1
        for label, client_id in found.items():
            if not s.query(storage.ClientWAIdentity).filter_by(
                    client_id=str(client_id),
                    display_name_for_whatsapp=f"{FIXTURE_TAG}: {label}").first():
                s.add(storage.ClientWAIdentity(
                    client_id=str(client_id),
                    display_name_for_whatsapp=f"{FIXTURE_TAG}: {label}"))
        s.commit()
        return found


def _actor(name):
    return SENDERS[name]


def _membership(user, kind, client_id, label, role, project=None):
    return storage.SenderMembership(
        user_id=user.id, context_kind=kind, client_id=client_id,
        context_label=label, role=role, project_code=project,
        authority_basis=FIXTURE_TAG, created_by=FIXTURE_TAG, updated_by=FIXTURE_TAG,
    )


def _user(s, name):
    wa_id = SENDERS[name]
    row = s.query(storage.User).filter_by(wa_id=wa_id).one_or_none()
    if row is None:
        row = storage.User(wa_id=wa_id, name=name, role="manager", active=True)
        s.add(row); s.flush()
    return row


def reset_controlled_fixture():
    """Remove only rows owned by this fixture namespace."""
    ids = _client_ids()
    client_ids = set(ids.values())
    project_codes = set(PROJECTS) | set(SA_PROJECTS)
    with storage.SessionLocal() as s:
        users = s.query(storage.User).filter(storage.User.wa_id.in_(tuple(SENDERS.values()))).all()
        user_ids = {u.id for u in users}
        task_ids = {row.id for row in s.query(storage.Task).filter(
            storage.Task.client_id.in_(client_ids), storage.Task.project_code.in_(project_codes)).all()}
        if task_ids:
            s.query(storage.DelayLog).filter(storage.DelayLog.task_id.in_(task_ids)).delete(synchronize_session=False)
            s.query(storage.TaskGroup).filter(
                storage.TaskGroup.parent_id.in_(task_ids) | storage.TaskGroup.child_id.in_(task_ids)
            ).delete(synchronize_session=False)
        for model in (storage.Inspection, storage.Meeting, storage.StockItem, storage.PMProjectMap,
                      storage.Task, storage.Audit):
            s.query(model).filter(model.client_id.in_(client_ids)).delete(synchronize_session=False)
        if user_ids:
            s.query(storage.CurrentContextSelection).filter(storage.CurrentContextSelection.user_id.in_(user_ids)).delete(synchronize_session=False)
            s.query(storage.MultiContextInboundClaim).filter(storage.MultiContextInboundClaim.sender.in_(tuple(SENDERS.values()))).delete(synchronize_session=False)
            s.query(storage.SenderMembership).filter(storage.SenderMembership.user_id.in_(user_ids)).delete(synchronize_session=False)
            s.query(storage.User).filter(storage.User.id.in_(user_ids)).delete(synchronize_session=False)
        s.commit()
    _reset_agent_rows(client_ids)


def _reset_agent_rows(client_ids):
    with storage.SessionLocal() as s:
        scopes = {"platform"} | {scope_key(cid) for cid in client_ids}
        s.query(AgentAuthorityGrant).filter(
            AgentAuthorityGrant.scope_key.in_(scopes),
            AgentAuthorityGrant.authority_basis == FIXTURE_TAG).delete(synchronize_session=False)
        s.query(AgentAuthorityValue).filter(
            AgentAuthorityValue.scope_key.in_(scopes),
            AgentAuthorityValue.authority_instrument == FIXTURE_TAG).delete(synchronize_session=False)
        s.query(AgentPrincipalAuthority).filter(
            AgentPrincipalAuthority.scope_key.in_(scopes),
            AgentPrincipalAuthority.authority_basis == FIXTURE_TAG).delete(synchronize_session=False)
        s.query(AgentEntitlement).filter(
            AgentEntitlement.scope_key.in_(scopes),
            AgentEntitlement.authority_basis == FIXTURE_TAG).delete(synchronize_session=False)
        s.query(AgentConfiguration).filter(
            AgentConfiguration.scope_key.in_(scopes),
            AgentConfiguration.proposer == FIXTURE_TAG).delete(synchronize_session=False)
        s.commit()


def _install_agent_state(client_ids, user_ids):
    repo = AgentRepository()
    runtime_caps = {"manager_pa.assist", "help.discover"}
    for cap in CAPABILITY_CATALOG:
        if cap.capability_id in runtime_caps:
            repo.upsert_capability(cap)
            with storage.SessionLocal() as s:
                row = s.get(AgentCapability, cap.capability_id)
                row.enabled = True; row.healthy = True
                s.commit()
    service = "fixture:rtw7-loader"
    authority = {"principal_class": "service", "independence_group": "fixture-loader",
                 "permissions": ["capability.control", "authority.delegate", "entitlement.configure"],
                 "capabilities": sorted(runtime_caps), "risk_classes": ["R1"],
                 "max_autonomy": 2, "actions": ["invoke"], "information_permissions": ["read"],
                 "domains": ["SD3", "SD4"], "confidentiality": ["restricted", "internal"],
                 "industry_keys": []}
    principals = {service: authority}
    scopes = {"platform": None, "client:%s" % client_ids["Florida"]: client_ids["Florida"],
              "client:%s" % client_ids["SA"]: client_ids["SA"]}
    for key, cid in scopes.items():
        value_principals = dict(principals)
        for name, uid in user_ids.items():
            if key == "platform" and name != "Neville":
                continue
            if key != "platform" and ((name == "Dolan" and cid != client_ids["Florida"]) or
                                       (name == "Paul" and cid != client_ids["SA"])):
                continue
            value_principals[f"user:{uid}"] = {
                "principal_class": "user", "independence_group": f"controlled-{name}",
                "permissions": [], "capabilities": ["help.discover"] if key == "platform" else ["manager_pa.assist"],
                "risk_classes": ["R1"], "max_autonomy": 1, "actions": ["invoke"],
                "information_permissions": ["read"], "domains": ["SD3", "SD4"],
                "confidentiality": ["restricted", "internal"], "industry_keys": [],
            }
        repo.install_authority_value("AB-AUTH-001", key, {"principals": value_principals},
                                     FIXTURE_TAG, FIXTURE_TAG, _now(),
                                     proof_ref="fixture://%s/authority" % FIXTURE_TAG)
        proposal = repo.propose_configuration(
            "controlled.fixture", {"controlled_fixture": True}, FIXTURE_TAG,
            f"{FIXTURE_TAG}:{key}:configuration", client_id=cid,
            reason="controlled acceptance scope marker")
        repo.commit_configuration(
            "controlled.fixture", proposal["version"], None, FIXTURE_TAG,
            FIXTURE_TAG, client_id=cid)
        cap = "help.discover" if key == "platform" else "manager_pa.assist"
        repo.grant_authority(f"{FIXTURE_TAG}:loader:{key}", service, "service", cap,
                             FIXTURE_TAG, client_id=cid, max_autonomy=2,
                             information_permissions=("read",), action_permissions=("invoke",),
                             allowed_domains=("SD3", "SD4"), granting_actor=Principal(service, "service", Scope(cid)),
                             autonomy_limits={})
        for name, uid in user_ids.items():
            if (key == "platform" and name != "Neville") or (key != "platform" and
                    ((name == "Dolan" and cid != client_ids["Florida"]) or (name == "Paul" and cid != client_ids["SA"]))):
                continue
            cap = "help.discover" if key == "platform" else "manager_pa.assist"
            repo.grant_authority(f"{FIXTURE_TAG}:{name}:{key}", f"user:{uid}", "user", cap,
                                 FIXTURE_TAG, client_id=cid, max_autonomy=1,
                                 information_permissions=("read",), action_permissions=("invoke",),
                                 allowed_domains=("SD3", "SD4"), granting_actor=Principal(service, "service", Scope(cid)),
                                 autonomy_limits={})
            if key != "platform":
                repo.assign_entitlement(f"{FIXTURE_TAG}:{name}:assist", cid, "capability", cap,
                                        {"enabled": True}, 1, FIXTURE_TAG, f"user:{uid}",
                                        granting_actor=Principal(service, "service", Scope(cid)))


def _seed_operations(client_ids):
    florida = client_ids["Florida"]; sa = client_ids["SA"]
    for actor, cid, projects in ((_actor("Dolan"), florida, PROJECTS), (_actor("Paul"), sa, SA_PROJECTS)):
        for project in projects:
            membership = {"sender": actor, "context_kind": "client", "client_id": cid,
                          "project_code": project, "id": 0, "user_id": 0}
            with storage.SessionLocal() as s:
                user_id = s.query(storage.User.id).filter_by(wa_id=actor).one()[0]
                membership["user_id"] = user_id
                membership["id"] = s.query(storage.SenderMembership.id).filter_by(
                    user_id=user_id, client_id=cid).first()[0]
            token = storage.set_effective_membership(membership)
            try:
                if cid == florida:
                    healthy = storage.create_task(actor, f"{project} future work", "task", project_code=project, due_date=_now(3), assignee_wa=actor)
                    due = storage.create_task(actor, f"{project} due work", "task", project_code=project, due_date=_now(0), assignee_wa=actor)
                    overdue = [storage.create_task(actor, f"{project} overdue {days} day work", "urgent", project_code=project, due_date=_now(-days), subcontractor_name="Concrete1", assignee_wa=actor) for days in (1, 2, 3, 5, 6)]
                    acknowledged = storage.create_task(actor, f"{project} acknowledged work", "task", project_code=project, due_date=_now(1), assignee_wa=actor)
                    escalation = storage.create_task(actor, f"{project} escalation evidence", "urgent", project_code=project, due_date=_now(-2), assignee_wa=actor)
                    change = storage.create_task(actor, f"{project} approved change", "change", project_code=project, due_date=_now(2), assignee_wa=actor)
                    completed = storage.create_task(actor, f"{project} completed performance", "task", project_code=project, status="completed", completed_at=_now(-1), subcontractor_name="Electrical1", assignee_wa=actor)
                    rework = storage.create_task(actor, f"{project} rework work", "task", project_code=project, assignee_wa=actor)
                    order = storage.create_task(actor, f"{project} order", "order", project_code=project, subtype="order", order_state="ordered", assignee_wa=actor)
                    delivery = storage.create_task(actor, f"{project} delivered order", "delivery", project_code=project, subtype="order", order_state="delivered", assignee_wa=actor)
                    roster = "Ops1 PM1 PM2 Site Manager1 Site Manager2 Site Manager3 Plumbing1 HVAC1 Drywall1 Rebar1 Concrete Supply1 Materials1 Equipment1"
                else:
                    overdue_customer = storage.create_task(actor, f"{project} overdue customer commitment", "urgent", project_code=project, due_date=_now(-2), subtype="customer_commitment", assignee_wa=actor)
                    overdue_workshop = storage.create_task(actor, f"{project} overdue workshop commitment", "urgent", project_code=project, due_date=_now(-4), subtype="workshop_commitment", assignee_wa=actor)
                    overdue_supplier = storage.create_task(actor, f"{project} overdue supplier commitment", "urgent", project_code=project, due_date=_now(-6), subtype="supplier_commitment", subcontractor_name="Parts Supplier1", assignee_wa=actor)
                    change = storage.create_task(actor, f"{project} approved commitment", "change", project_code=project, due_date=_now(2), assignee_wa=actor)
                    branch_issue = storage.create_task(actor, f"{project} branch issue", "urgent", project_code=project, assignee_wa=actor)
                    completed = storage.create_task(actor, f"{project} completed activity", "task", project_code=project, status="completed", completed_at=_now(-1), assignee_wa=actor)
                    rework = overdue_workshop
                    order = storage.create_task(actor, f"{project} supplier history", "task", project_code=project, status="completed", completed_at=_now(-2), subtype="supplier_performance", subcontractor_name="Parts Supplier1", assignee_wa=actor)
                    delivery = storage.create_task(actor, f"{project} healthy future activity", "task", project_code=project, due_date=_now(5), subtype="future_activity", assignee_wa=actor)
                    healthy = delivery; due = overdue_customer; overdue = [overdue_customer, overdue_workshop, overdue_supplier]; escalation = overdue_workshop
                    roster = "Branch 1 Branch 2 Manager1 Workshop Manager1 Parts Manager1 Sales Manager1 Parts Supplier1 Tyre Supplier1 Equipment Supplier1 Vehicle1 Vehicle2 Vehicle3"
                storage.create_task(actor, f"{project} controlled roster {roster}", "task", project_code=project, assignee_wa=actor)
                with storage.SessionLocal() as s:
                    s.query(storage.Task).filter_by(id=rework["id"]).update({"is_rework": True, "started_at": _now(-1)})
                    s.query(storage.Task).filter_by(id=acknowledged["id"] if cid == florida else completed["id"]).update({"started_at": _now(-1)})
                    s.query(storage.Task).filter_by(id=escalation["id"]).update({"overrun_days": 2})
                    s.add(storage.Inspection(client_id=cid, project_code=project, phase="operational", required_date=_now(-2), inspector=("Site Manager1" if cid == florida else "Manager1"), notes="inspection evidence"))
                    s.add(storage.Meeting(client_id=cid, title=f"{project} actions", project_code=project, site_name=project, scheduled_for=_now(1), created_by=actor, status="scheduled"))
                    supplier_names = (("Concrete Supply1", "Materials1", "Equipment1") if cid == florida else
                                      ("Parts Supplier1", "Tyre Supplier1", "Equipment Supplier1"))
                    if cid == florida:
                        s.add(storage.StockItem(client_id=cid, name="Rebar1", project_code=project, supplier_name="Materials1", unit="lengths", current_qty=40))
                    for supplier_name in supplier_names:
                        s.add(storage.StockItem(client_id=cid, name=f"{supplier_name} stock", project_code=project,
                                                supplier_name=supplier_name, unit="units", current_qty=10))
                    s.commit()
                if cid == florida:
                    storage.record_change_order({"task_id": change["id"], "cost": 1500, "time_impact_days": 4, "approval_required": True, "actor": actor})
                    with storage.SessionLocal() as s:
                        s.query(storage.Task).filter_by(id=change["id"]).update({"approved_at": _now(-1)})
                        s.commit()
                    storage.add_task_to_group(overdue[0]["id"], rework["id"], actor)
                    storage.log_delay({"task_id": escalation["id"], "project_code": project, "reporter": actor, "days": 2, "reason": "escalation evidence"})
                    storage.log_delay({"task_id": escalation["id"], "project_code": project, "reporter": actor, "days": 3, "reason": "repeat delay evidence"})
                else:
                    storage.record_change_order({"task_id": change["id"], "cost": 500, "time_impact_days": 1, "approval_required": True, "actor": actor})
                    with storage.SessionLocal() as s:
                        s.query(storage.Task).filter_by(id=change["id"]).update({"approved_at": _now(-1)})
                        s.commit()
                    storage.log_delay({"task_id": escalation["id"], "project_code": project, "reporter": actor, "days": 2, "reason": "recurring delay evidence"})
                    storage.log_delay({"task_id": escalation["id"], "project_code": project, "reporter": actor, "days": 3, "reason": "recurring delay evidence"})
            finally:
                storage._EFFECTIVE_MEMBERSHIP.reset(token)


def setup_controlled_fixture():
    storage.init_db()
    reset_controlled_fixture()
    ids = _client_ids()
    with storage.SessionLocal() as s:
        florida = _user(s, "Dolan"); paul = _user(s, "Paul"); neville = _user(s, "Neville")
        s.add_all([storage.SenderMembership(user_id=neville.id, context_kind="platform", client_id=None, context_label="Hub", role="manager", authority_basis=FIXTURE_TAG, created_by=FIXTURE_TAG, updated_by=FIXTURE_TAG),
                   storage.SenderMembership(user_id=neville.id, context_kind="client", client_id=ids["Florida"], context_label="Florida", role="manager", authority_basis=FIXTURE_TAG, created_by=FIXTURE_TAG, updated_by=FIXTURE_TAG),
                   storage.SenderMembership(user_id=neville.id, context_kind="client", client_id=ids["SA"], context_label="SA", role="manager", authority_basis=FIXTURE_TAG, created_by=FIXTURE_TAG, updated_by=FIXTURE_TAG),
                   storage.SenderMembership(user_id=florida.id, context_kind="client", client_id=ids["Florida"], context_label="Florida", role="manager", authority_basis=FIXTURE_TAG, created_by=FIXTURE_TAG, updated_by=FIXTURE_TAG),
                   storage.SenderMembership(user_id=paul.id, context_kind="client", client_id=ids["SA"], context_label="SA", role="manager", authority_basis=FIXTURE_TAG, created_by=FIXTURE_TAG, updated_by=FIXTURE_TAG)])
        s.commit()
    storage.set_client_display_name(str(ids["Florida"]), f"{FIXTURE_TAG}: Florida")
    storage.set_client_display_name(str(ids["SA"]), f"{FIXTURE_TAG}: SA")
    with storage.SessionLocal() as s:
        user_ids = {name: s.query(storage.User).filter_by(wa_id=wa).one().id for name, wa in SENDERS.items()}
    _install_agent_state(ids, user_ids)
    _seed_operations(ids)
    return {"client_ids": ids, "user_ids": user_ids, "senders": dict(SENDERS)}
