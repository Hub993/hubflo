# HUBFLO Agent Layer 2.0 cumulative candidate — RTW7 handoff

This is an implementation/evidence handoff, not an independent acceptance
decision. It does not issue the completion phrase reserved to RTW7.

## Authority, source and lineage

- Authoritative implementation parent: `54f26bdf2d66a91bd12b92dd56e7c59b1061004f`.
- Accepted Stage 2 authority: `8bc773baba23930d7ad181d066b74cd8cc0c2601`.
- `git merge-base --is-ancestor` result before implementation: exit `0`.
- The correction began by moving the rejected candidate branch pointer back to
  the authoritative parent. The pre-existing untracked rejected-candidate
  patch was preserved as evidence and is not part of this candidate.
- Locked contracts: Annex B 1.1 and Security/Data/IP Policy 1.0 at the
  authoritative parent.
- Candidate commit: the commit containing this document; its direct parent is
  the authoritative implementation parent above.
- Stage 2 source files changed: none.

## Implemented design and source inventory

- `agent_layer/contracts.py`: principal/scope, protected-information,
  invocation, capability, provider, verifier and outcome-inspector contracts.
- `agent_layer/models.py`: additive HUBFLO-owned durable Agent Layer schema.
- `agent_layer/persistence.py`: transactional configuration, authority,
  entitlement, provider, objective/work/execution, approval, verification,
  learning, intelligence, distribution, retention, scoped-index, audit and
  security-event persistence.
- `agent_layer/security.py`: deterministic scope/domain/purpose/provider
  context assembly, secret rejection/redaction and recomposition constraint
  derivation.
- `agent_layer/providers.py`: provider abstraction, structured-output
  validation and ordered fallback.
- `agent_layer/runtime.py`: 31 registered capability contracts, effective-state
  introspection, deterministic invocation gate, approval/autonomy/entitlement,
  claim/replay/uncertain-outcome handling, post-claim revalidation, independent
  verification, safe provider fallback, configurable Take-on/learning/autonomy
  gates, distribution decisions, no-agent behavior, fan-in and restore gate.
- `agent_layer/stage2.py`: injected adapter to existing authoritative handlers;
  it owns no business recognition, authorization, lifecycle or mutation.
- `tests/test_agent_layer_2.py`: controlled T-D/T-I/T-S/T-F/T-X/T-R evidence.
- `scripts/run_agent_layer_acceptance.sh`: isolated reproducible acceptance and
  compile runner.
- `evidence/agent_layer/ACCEPTANCE_MATRIX.md`: pre-implementation test oracle.

Agent Layer is not imported by Stage 2 and is not required for ordinary
deterministic operation. Constructing `AgentRuntime` creates/updates only its
own registry/schema. All capabilities begin disabled and no authority grant,
provider approval, recipient, entitlement, autonomy promotion or Take-on value
is synthesized.

## Schema and migration inventory

The candidate adds 19 SQLAlchemy tables through idempotent `create_all` on
Agent Layer startup:

`al_authority_values`, `al_principal_authorities`, `al_configurations`, `al_capabilities`,
`al_authority_grants`, `al_entitlements`, `al_provider_policies`,
`al_objectives`, `al_work_units`, `al_executions`, `al_approvals`,
`al_verifications`, `al_learning`, `al_intelligence`, `al_distributions`,
`al_retention_states`, `al_context_artifacts`, `al_audit_events`, and
`al_security_events`.

No Stage 2 table or column is altered. There is no data backfill. Existing
deployments receive only additive tables when an authorized Agent Layer runtime
is first constructed. Rollback is application-level disable/revocation; the
candidate does not claim irreversible Stage 2 rollback.

## RTW7 Correction 2 material-defect corrections

1. Every one of the 31 catalog entries now persists non-empty, exhaustive
   input/output schemas plus explicit semantic constraints and the full
   capability-owned execution contract. Provider output contracts are taken
   only from that catalog. All reasoning families execute through governed
   invocation with family-specific structured results; malformed input and
   structurally valid but authority-creating semantic output fail closed.
   Every state capability also executes its real repository/evaluation path.
2. Distribution decisions no longer accept authority booleans. They resolve a
   current scoped recipient principal, information classification/novelty,
   effective distribution configuration, recipient permission, entitlement
   when configured, channel and revocation state. Each decision is persisted
   under its scoped idempotency key, and delivery must bind to that matching
   authoritative decision.
3. Individual adaptation independently requires current AB-AUTH-018 and
   SDIP-AV-005; enabling `learning.persist` cannot bypass either. Learning and
   intelligence supersession, withdrawal, refinement, work dependencies and
   distribution references are validated against compound scope and logical
   identity before mutation.
4. Every governed invocation (including replay/resume) resolves current exact-
   scope AB-AUTH-001 principal authority in addition to its grant. Approval is
   revalidated at use, consequential paths revalidate after claim, and
   revoked/narrowed principals stop. Authority versions use the maximum
   historical version, preserving revoked history while replacements advance
   monotonically.
5. Grants represent Levels 1–5, explicit autonomy limits and shadow
   eligibility. S3 Level 3 uses approval; explicitly authorized Level 4/5 may
   proceed only inside configured limits. Shadow execution durably records
   would-have-done evidence without invoking the consequential handler.
   Effective disables cover global layer, client/project/Flo configuration,
   agent role, capability, action and risk class.

## RTW7 Correction 3 material-defect corrections

1. Optional capability inputs now carry capability-owned types and are rejected
   before persistence, provider use or handler execution; nullable optional
   values are explicit. Provider optional outputs use the same owned contract.
2. Learning scope is checked against the invoking principal's current scope;
   client/project requests cannot promote themselves to platform or arbitrary
   industry scope. Generalization remains an explicit authority-gated path.
3. Supervisory eligibility independently checks exact client/project scope,
   security domain, confidentiality, provenance/classification and current
   information authority; `information.supervise` alone grants no access.
4. Shadow execution records the real provider or side-effect-free
   capability-specific proposed result and durable comparison evidence. No
   consequential handler, delivery or state mutation runs in shadow mode.
5. Non-empty invocation evidence references are included in request identity,
   so changed material evidence requires a new approval while exact replay
   remains idempotent.
6. Configuration revocation is available through the governed commit path and
   repository API with exact scope and current actor authority. Revocation is
   immediately excluded from effective configuration while history remains;
   replacement/rollback uses a new governed version.

## RTW7 Correction 4 material-defect corrections

1. Entitlement assignment optional fields have capability-owned string/dict/int
   types; valid assignments execute and malformed values fail closed.
2. Client/project learning remains client-scoped. Industry/platform movement
   requires the explicit authority-gated promotion path, which preserves origin
   scope; unresolved generalized authority remains fail-closed.
3. Supervisory access requires explicit matching confidentiality authority in
   addition to scope, domain, provenance and information-access controls.
4. Shadow distribution invokes the same distribution decision function as the
   normal path, suppressing only durable distribution mutation while retaining
   the actual proposed decision and evidence.

## RTW7 Correction 5 material-defect corrections

1. Entitlement operations enforce subtype-specific required fields before
   approval progression or execution; assign and revoke contracts are distinct.
2. Learning promotion requires current exact-scope actor authority for the
   promotion action in addition to policy values; revoked actors fail closed.
3. Every protected-information decision requires explicit matching
   confidentiality authority, including awareness and internal use.
4. Shadow execution dispatches through shared capability-owned builtin planning
   with mutation suppression; no parallel `_shadow_preview` semantics remain.

## RTW7 Correction 6 material-defect corrections

1. Learning promotion is submitted through governed `learning.persist`, with
   source and platform/target actor authority, target-scope checks, durable
   claim/idempotency replay, and origin/target provenance.
2. Every shadow-capable builtin accepts the common shadow execution contract;
   configuration commit and entitlement mutation paths produce validated
   proposals without commit, while consequential external handlers remain
   uncalled.

## RTW7 Correction 7 material-defect corrections

1. Learning promotion now requires independently established target authority
   and explicit target-industry membership; caller scope cannot widen it.
   Source/target authority is revalidated through the governed invocation.
2. Configuration and objective shadow paths share state-sensitive precondition
   checks with normal execution; nonexistent or stale state cannot yield a
   fabricated successful shadow result.

## RTW7 Correction 8 material-defect corrections

1. The stale promotion fixture now installs explicit authoritative target
   industry membership; missing membership remains denied while origin
   provenance is preserved.
2. Configuration shadow selects the real operation-specific commit or revoke
   planner and suppresses only mutation.
3. Consequential capabilities declare either shared shadow support or tested
   contractual non-applicability; guardian remediation no longer crashes in
   authorized shadow mode.

## RTW7 Correction 9 material-defect corrections

1. Governed `learning.persist` promotion now enforces both current promotion
   policy families inside the governed gate; direct invocation cannot bypass
   policy and replay remains idempotent.
2. `security.contain` explicitly declares shadow non-applicability because
   containment requires authoritative event mutation; shadow returns a governed
   non-live result and never fabricates `CONTAINED`.

The earlier Correction 1 controls remain present:

1. Every catalog entry persists a complete capability-owned contract.
   Governed invocation uses a capability-owned deterministic executor,
   provider path, or injected authoritative adapter; an absent path fails
   closed. Configuration proposal/commit, learning, intelligence,
   entitlement evaluation, distribution decision, objective, graph, help,
   continuity and security observation are exercised through that path. The
   former generic eligible/success response is removed.
2. Durable logical identities use tenant/client/project compound scope for
   configurations, grants, entitlements, objectives, work, executions,
   approvals, learning, intelligence, distributions, retention, context
   artifacts and security events. Same objective/delivery/intelligence/
   learning identifiers across clients/projects are tested independently;
   scoped withdrawal cannot affect a peer scope.
3. Provider exposure requires a classified payload item matching the entire
   invocation payload. All exposed items are checked for domain, provenance,
   confidentiality, use, eligibility, training restrictions, retention,
   access controls, region, audit/attribution, deletion/withdrawal and
   distribution constraints. Output schemas come only from the capability
   contract. Zero-data policies and raw/unclassified payloads receive no data;
   fallback must preserve or tighten every constraint and satisfy actual data.
4. A signed/provenance-linked AB-AUTH-001 projection establishes principals,
   classes, permissions, capability/risk ceilings and independence groups.
   Capability control, delegation, entitlement/provider configuration,
   approval, verification, grant revocation and containment require a current
   authorized actor. Delegation cannot exceed the delegator, and verifier
   independence is authority-group based rather than ID inequality.

## Capability/enablement matrix

| Family | Implemented machinery | Candidate default / unresolved gate |
|---|---|---|
| AL-CP-001 | scoped supervision, introspection, delegation grant model | disabled; AB-AUTH-001 |
| AL-FLO-001 | independently scoped Industry and Client reasoning capabilities | disabled; AB-AUTH-001/019 and provider policy |
| AL-TAKEON-001 | industry/client/evolution proposals and common config provenance | proposal capability disabled; commit returns authority required until AB-AUTH-017 |
| AL-CFG-001 | proposal/effective versions, merge/introspection, commit, rollback/revocation, recomposition | disabled until configuration authority/approval/verifier assigned |
| AL-PA-001 | scoped provider assistance and effective-function discovery | disabled; entitlement, role and AB-AUTH-018 required for persistence |
| AL-PLAT-001 | diagnosis and separately governed R4 remediation contracts | disabled; remediation requires approval/verifier/authority |
| AL-CAP-001 | evidence-grounded provider-neutral assessment | disabled; provider, budgets and thresholds unset |
| AL-CHAN-001 | channel-neutral S3 delivery contract and exactly-once state | disabled; channels/recipients/approval unset |
| AL-OBJ-001 | durable versioned objectives and current-scope resume | disabled for invocation; storage machinery available to authorized service |
| AL-CRIT-001 | derived dependency/critical-path reasoning and persistence | disabled; provider/acceptance thresholds unset |
| AL-CONS-001 | structured consequence/intervention recommendation | disabled; no intervention authority implied |
| AL-PERF-001 | evidence-scoped performance reasoning/outcome linkage | disabled; thresholds unset |
| AL-LEARN-001 | platform/industry/client/individual records, lifecycle, withdrawal | disabled; retention/promotion values unset; user persistence fail-closed |
| AL-INTEL-001 | durable provenance, refinement/supersession, validation and scope | disabled; retention basis required per record |
| AL-DIST-001 | awareness/use/supervision/distribution/recipient separation and idempotent delivery state | disabled; novel routine distribution and recipients unset |
| AL-ENT-001 | independent dimensions, deterministic evaluation and versioned mutation | disabled; packages/prices/assignments unset |
| AL-AUTO-001 | Levels 1–5, explicit bounded limits, approval, real no-side-effect shadow evidence, role/action/risk/global/capability controls, degradation/kill and self-promotion denial | disabled; default ceiling never exceeds Level 2 without current grant and authority |
| AL-TOOL-001 | structured governed invocation and injected authoritative Stage 2 handler | disabled; no handler or grant inferred |
| AL-GRAPH-001 | durable DAG, dependency claims, checkpoints, local failure and governed fan-out/fan-in | disabled for production; representative controlled behavior tested |
| AL-VERIFY-001 | structured PASS/FAIL/INCONCLUSIVE independent verifier | disabled; required classes unset beyond R4 invariant |
| AL-PROV-001 | strict provider policy/contract/attribution and equivalent fallback | disabled; no live provider allowlist |
| AL-NOAI-001 | deterministic security/config/entitlement/continuity gates | deterministic gate available; judgment capabilities pending/stopped |
| AL-MARKET-001 | source/freshness/hash validation and no-action result | disabled; source/routing policy unset |
| AL-HELP-001 | actual effective-state function discovery | disabled until user grant; deterministic when enabled |
| AL-SEC-001 | immutable events, suppressor denial and governed containment | observation/containment capabilities disabled pending mappings |

The scalable-composition claim is included for the durable in-process topology
contract: two independently claimed branches, structured artifacts, durable
checkpoints and a governed fan-in that refuses missing/failed/wrong-scope
branches. No claim is made here about an external distributed queue or a live
multi-node deployment.

## Authority/configuration values

Implementation relies only on:

- the two locked repository contracts and their permanent invariants;
- authoritative source and accepted Stage 2 commit identifiers above;
- the policy-fixed fail-closed defaults: no protected provider use, no
  cross-client generalization, no persistent individual adaptation, no routine
  novel-intelligence distribution, no runtime SD1 authority, no automated
  destructive Stage 2 deletion, and no unconfigured autonomy above Level 2.

All `TEST-*` values in the test module are controlled fixtures and are never
installed by runtime production code. AB-AUTH-001 through AB-AUTH-019 and
SDIP-AV-001 through SDIP-AV-010 remain unset in candidate production state.
Dependent behavior is disabled or returns `AUTHORITY VALUE REQUIRED`, `DENIED`,
`HOLD/QUARANTINE`, `PENDING`, `ESCALATE`, or `STOP`.

## Test commands and observed results

1. Pristine-parent Stage 2 baseline after installing declared dependencies:
   `HUBFLO_TEST_PYTHON=.venv/bin/python scripts/run_stage2_regression.sh` —
   106 tests, OK, plus original compile command.
2. Agent Layer acceptance:
   `HUBFLO_TEST_PYTHON=.venv/bin/python scripts/run_agent_layer_acceptance.sh` —
   72 tests, OK, plus Agent Layer compile.
3. Cumulative discovery/regression:
   isolated `python -m unittest discover -v tests` —
   178 tests, OK on the final cumulative run.
4. Accepted Stage 2-only explicit suite — 106 tests, OK.
5. Startup integrity with isolated SQLite: imported `app`, constructed
   `AgentRuntime`, and observed all 31 capability contracts — OK.
6. `git diff --check` — clean before final commit.

The local Python runtime emits a pre-existing urllib3/LibreSSL compatibility
warning; it did not fail imports or tests. No live provider or external channel
was contacted. Test databases and bytecode caches were isolated in temporary
directories.

## Annex B integrated evidence

| Suite | Classes | Controlled evidence/result |
|---|---|---|
| B-SUITE-001 | T-D/T-I/T-X/T-R | tenant/project context, same-key objective/distribution/learning/intelligence/withdrawal, provider, cache/index/embedding, handoff/fan-in and audit negatives green |
| B-SUITE-002 | T-D/T-I/T-F/T-R | one mutation across four competing invocations; duplicate replay; authoritative uncertain completion; unknown outcome escalation; restart persistence green |
| B-SUITE-003 | T-D/T-I/T-X/T-F/T-R | proposal/effective distinction, governed proposal/R4 commit, deterministic merge/provenance, stale conflict, authorized delegation/revocation, monotonic revoke/replacement authority versions and safe recomposition green |
| B-SUITE-004 | T-D/T-I/T-S/T-X/T-F/T-R | four scopes; independent AB-AUTH-018/SDIP-AV-005 individual gate; client/user privacy; provider-independent records; same-ID cross-scope refinement/supersession denial and Stage 2 truth separation green |
| B-SUITE-005 | T-D/T-I/T-X/T-F/T-R | caller assertions rejected; current recipient/configuration/entitlement/channel/classification resolution; decision-to-delivery binding; novel hold and withdrawal restriction green |
| B-SUITE-006 | T-D/T-I/T-S/T-F/T-X | Levels 1–5 representation; bounded L4 execution/outside-limit denial; L3 approval; no-mutation shadow evidence; role/action/risk/global/capability disables; current principal narrowing/revocation green |
| B-SUITE-007 | T-D/T-I/T-F/T-X/T-R | classified whole-payload enforcement, capability-owned output, zero-data denial, provider success, invalid output, timeout, constraint-preserving fallback, deterministic no-agent continuity and durable state green |
| B-SUITE-008 | T-D/T-I/T-F/T-X | direct durable objective, dependency wait, two-branch fan-out, checkpoints, scoped fan-in, local failure and no duplicate completion green |
| B-SUITE-009 | T-D/T-I/T-X | principal/scope/config/capability/provider/approval/verification/claim/outcome/control audit fields and tenant visibility green |
| B-SUITE-010 | T-D/T-X/T-F | R4 authorized approval and authority-group-independent verifier required; arbitrary actors and INCONCLUSIVE stop; self-approval denied |
| B-SUITE-011 | T-D/T-I/T-X | SD1–SD4 controls, SD2 rejection, opaque secret references, derived/source separation and recomposition intersection green |
| B-SUITE-012 | T-R | accepted Stage 2 explicit 106/106 and cumulative suite green; no Stage 2 source changed |

## SDIP-T01–SDIP-T18 evidence

| Test | Classes | Controlled result |
|---|---|---|
| SDIP-T01 | T-I/T-X | wrong-client and same-logical-key objective/distribution/withdrawal/context/provider/cache/index/embedding/learning/intelligence/audit access denied |
| SDIP-T02 | T-D/T-X | same-key cross-project learning and wrong-project context/objective/fan-in denied; matching scope succeeds |
| SDIP-T03 | T-D/T-X | secrets rejected from context, durable state, cache/embedding/index; audit redacts; opaque reference permitted |
| SDIP-T04 | T-D/T-X/T-R | runtime grants have explicit domains; no SD1 default; Stage 2 adapter has no source/session access |
| SDIP-T05 | T-I/T-F/T-X | unapproved provider, raw payload and zero-permitted-data provider receive no context; fully classified structured path and tightening fallback green |
| SDIP-T06 | T-D/T-X | all provider-policy dimensions represented; training-permitted mode, unknown classification and retention/purpose/domain/access/region incompatibility fail closed |
| SDIP-T07 | T-D/T-I/T-X/T-R | client/user learning isolated and derived intelligence cannot mutate Stage 2 truth |
| SDIP-T08 | T-X | SDIP-AV-004 absent and cross-client promotion/retrieval denied by locked default |
| SDIP-T09 | T-D/T-X | novel recipient distribution held; unestablished/cross-client supervisor denied; caller assertions create no visibility |
| SDIP-T10 | T-D/T-I/T-X | awareness and entitlement independently insufficient; distribution requires current recipient permission, configuration, channel and entitlement; delivery binds to persisted decision |
| SDIP-T11 | T-I/T-F/T-X | principal narrowing/revocation, delegation/grant/provider/capability/config revocation and revocation-between-claim-and-handler stop progression; replacement versions are monotonic |
| SDIP-T12 | T-D/T-I/T-X | empty retention basis rejected; withdrawal marks restricted and removes future eligible retrieval |
| SDIP-T13 | T-D/T-X | restore manifest retains scope, excludes authority/control resurrection and demands current revalidation |
| SDIP-T14 | T-D/T-X | fixtures are explicit `TEST-*`; test broader visibility has no production authority/config write |
| SDIP-T15 | T-I/T-F/T-X | tenant event preserved, implicated/arbitrary principal cannot suppress or contain, independently authorized containment succeeds |
| SDIP-T16 | T-D/T-F/T-R | deterministic security/scope/provider/distribution/config gates remain active; ordinary Stage 2 106/106 |
| SDIP-T17 | T-D/T-I/T-X | composed client/project/domain/autonomy/provider/distribution constraints intersect; cross-client composition denied |
| SDIP-T18 | T-R | no Stage 2 source/schema modification; accepted Stage 2 106/106 green |

## Replay/concurrency/claim/uncertain-outcome evidence

- Unique idempotency keys plus atomic insert/claim produce one consequential
  handler call under four competing threads.
- Same-key/same-request replay returns the existing outcome; same-key/different
  request returns conflict.
- Work claims have one winner; completed checkpoints cannot be reclaimed.
- Consequential exceptions become `outcome_uncertain`. A retry first calls the
  capability outcome inspector. Established completion is reused; an unknown
  result escalates without a second consequence; established non-completion
  requires a new governed intent.
- Current capability, health, exact-scope AB-AUTH-001 principal authority,
  configuration, grant, scope, autonomy,
  entitlement and approval are evaluated before claim and again immediately
  before a consequential handler. The active-revocation test executes no
  handler after the grant is revoked.

## Pending controlled/live evidence

No T-L evidence was fabricated. Annex B Section 62 live cases 1–25 remain
pending controlled deployment to the extent their capability is later included
in production scope. They require actual authenticated principals, authority
register values, effective client/Flo/project topology, approved providers,
real operational evidence/metrics, enabled channels/recipients, configured
entitlements and/or a deployed fault/security scenario. Section 62 case 26 is
represented by the real accepted Stage 2 regression, not relabeled as a live
Agent Layer deployment.

The following remain deliberately not enabled by production defaults: production Take-on commit,
individual persistent adaptation, cross-client learning/generalization,
novel-to-established promotion, proactive/routine distribution, commercial
  packages/billing terms, Levels 4–5 production autonomy assignments (the
  bounded generic machinery is implemented), live provider routing,
live channel delivery, Guardian remediation, automated destructive retention,
source/deployment access and external incident notification. Each depends on a
listed authority/security/legal/commercial value.

## Known failures, exclusions and confirmation

- Known internal test failures at handoff: none.
- External distributed queue/multi-node topology: not claimed; the accepted
  scalable-composition claim is the durable governed topology contract tested
  in-process and portable to a later worker adapter.
- Real backup media/RPO/RTO: technology and values not implemented or claimed;
  restore-security semantics are implemented/tested.
- Encryption/transport are delegated to the approved deployment/database/
  provider platform; no deployment platform was in this repository and no live
  encryption claim is made.
- No implementation-owned binding authority, policy, security, commercial,
  retention, legal, topology, provider, recipient, threshold or Take-on value
  was invented.
