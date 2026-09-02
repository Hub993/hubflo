# Agent Layer 2.0 internal acceptance matrix

Authority basis: `HUBFLO_AGENT_LAYER_2.0_ANNEX_B_1.1.md`,
`HUBFLO_SECURITY_DATA_IP_PROTECTION_POLICY_1.0.md`, authoritative source HEAD
`54f26bdf2d66a91bd12b92dd56e7c59b1061004f`, and accepted Stage 2
`8bc773baba23930d7ad181d066b74cd8cc0c2601`.

This matrix was created before Agent Layer source implementation. It is the
internal test oracle, not an acceptance decision. T-L cases remain controlled
deployment evidence and cannot be fabricated locally.

## Capability families

| Contract | Implementation claim | Required internal evidence | Live/deferred evidence |
|---|---|---|---|
| AL-CP-001 | Configurable Hub Manager control/supervision and bounded delegation | T-D/T-I/T-X/T-F/T-R | Authorized supervisory T-L |
| AL-FLO-001 | Generic Industry Flo and private Client Flo scopes | T-D/T-I/T-X/T-R; Core dependency inspection | Scoped production T-L |
| AL-TAKEON-001 | Proposal/evolution machinery implemented; detailed Take-on activation disabled | proposal/effective separation, invalid/partial/cross-client/self-elevation negatives | AB-AUTH-017 and authorized T-L |
| AL-CFG-001 | Versioned proposal, commit, introspection, rollback/revocation and constraint composition | T-D/T-I/T-X/T-F/T-R, duplicate and stale races | Authorized config T-L |
| AL-PA-001 | Scoped help/advice/proposal role; persistent personal adaptation disabled | enabled-function grounding and cross-scope/provider failure negatives | AB-AUTH-018 and authorized T-L |
| AL-PLAT-001 | Guardian observation, diagnosis, containment recommendation and governed handoff | layered-fault, protected-data and outage tests | Controlled live fault T-L |
| AL-CAP-001 | Evidence-linked capacity/architecture findings | deterministic provenance and no migration authority | Current-metrics T-L |
| AL-CHAN-001 | Channel-neutral structured invocation and delivery state | identity/scope, channel failure and duplicate delivery | Enabled-channel T-L |
| AL-OBJ-001 | Durable versioned objectives with cancellation and resume revalidation | persistence/restart/unauthorized resume | Multi-turn T-L |
| AL-CRIT-001 | Derived dependency/critical-path findings separated from truth | evidence/inference/isolation/outcome comparison | Operational-evidence T-L |
| AL-CONS-001 | Consequence/intervention recommendation with authority ceiling | structured recommendation and no direct mutation | Authorized evidence T-L |
| AL-PERF-001 | Scoped performance evidence/prediction/outcome intelligence | source age/scope and client-isolation tests | Current-evidence T-L |
| AL-LEARN-001 | Four durable scopes, outcome linkage, lifecycle and promotion gate | T-D/T-I/T-S/T-X/T-F/T-R | Authorized learning T-L |
| AL-INTEL-001 | Versioned persistent derived intelligence and provenance | create/retrieve/refine/supersede/isolation/provider outage | Authorized derived T-L |
| AL-DIST-001 | Separate awareness/use/supervision/distribution/recipient decisions resolved from authoritative state | caller-assertion denial, current recipient/configuration/entitlement/channel resolution, novel hold, scoped persisted decision and delivery binding | Configured delivery T-L |
| AL-ENT-001 | Generic independently evaluated entitlement dimensions | independent dimensions, revocation, authorization non-escalation | Feature-controlled T-L or exclusion |
| AL-AUTO-001 | Capability/scope Levels 1–5, bounded autonomy, approvals, real shadow execution, degrade and disables | Levels 2/3/4, exact-limit/outside-limit, no-side-effect shadow, self-promotion denial, global/client/role/capability/action/risk controls | Authorized bounded-action T-L |
| AL-TOOL-001 | Registered structured governed capability invocation | disabled/unhealthy/unauthorized/incompatible denial; S3 exactly once | Authorized S3 T-L if enabled |
| AL-GRAPH-001 | Durable DAG plus representative governed fan-out/fan-in | dependency/checkpoint/resume/local failure/join/no widening | Distributed-topology T-L if deployed |
| AL-VERIFY-001 | Deterministic/independent/human structured verification | PASS/FAIL/INCONCLUSIVE and unavailable-stop tests | Required live verifier case |
| AL-PROV-001 | Provider protocol, policy gate, validation and safe fallback | substitute/failure/incompatible data-use/retention tests | Authorized provider T-L |
| AL-NOAI-001 | Deterministic gates/state remain available; judgment work pauses | provider-off continuity and safe recovery tests | Controlled outage T-L |
| AL-MARKET-001 | Attributable/freshness-aware external-evidence records | authority-injection/migration denial | Enabled external evidence T-L |
| AL-HELP-001 | Registry/effective-state grounded function discovery | unauthorized/disabled capability omission | Manager PA T-L |
| AL-SEC-001 | Immutable security events and deterministic containment controls | access/tenant/secret/provider/tool negatives and suppressor denial | Controlled security-event T-L |

## Cross-capability suites

| Suite | Internal oracle |
|---|---|
| B-SUITE-001 | Retrieval, context, provider, cache, learning, intelligence, handoff, distribution and audit stay tenant/project scoped. |
| B-SUITE-002 | Normal, duplicate, replay, competing claim, uncertain result and restart produce at most one consequence. |
| B-SUITE-003 | Proposed/committed versions, monotonic authority replacement history, effective constraints, delegation, revocation, entitlement and recomposition are deterministic and explainable. |
| B-SUITE-004 | Four learning scopes, independent individual-persistence authorities, terminology, outcome linkage, scoped refinement/supersession and provider substitution never create authority or leakage. |
| B-SUITE-005 | Awareness, supervisory use, distribution, recipient rights and entitlement vary independently and caller assertions never establish them. |
| B-SUITE-006 | Levels 1–5, bounded Level 4, approval, no-side-effect shadow, degradation and global/client/role/capability/action/risk disable gates fail closed and deny self-promotion. |
| B-SUITE-007 | Invalid output, timeout, eligible fallback, no fallback and no-agent paths preserve durable state and limits. |
| B-SUITE-008 | Direct and DAG work, representative two-branch fan-out/fan-in, checkpoint/restart and localized failure preserve scope and exactly-once behavior. |
| B-SUITE-009 | Material decisions retain principal, scope, versions, evidence, provider, approval, claim, outcome and control state without private chain-of-thought. |
| B-SUITE-010 | R4/S4 requires separately authorized approval and independent verification; self-approval is denied. |
| B-SUITE-011 | SD1 source, SD2 secrets, SD3 runtime data and SD4 intelligence remain separate under context assembly and recomposition. |
| B-SUITE-012 | Full accepted Stage 2 suite stays green; Agent Layer is optional and owns no business recognition or handler. |

## Security/Data/IP policy

| Test | Internal oracle |
|---|---|
| SDIP-T01 | Cross-client context/retrieval/cache/provider/learning/intelligence/output is denied. |
| SDIP-T02 | Cross-project context is denied unless explicit broader scope exists. |
| SDIP-T03 | Secret values are rejected from reasoning, learning, embeddings and ordinary audit; opaque references remain usable. |
| SDIP-T04 | Runtime roles have no implicit source/repository/deployment permission. |
| SDIP-T05 | Unapproved/incompatible providers receive no protected context; fallback cannot widen scope. |
| SDIP-T06 | Provider training/use/retention incompatibility fails closed. |
| SDIP-T07 | Client/user learning isolation and derived/truth separation hold. |
| SDIP-T08 | Cross-client proprietary generalization is disabled by locked default. |
| SDIP-T09 | Novelty creates no visibility; unauthorized review is held/quarantined. |
| SDIP-T10 | Awareness/entitlement never creates recipient rights; channel gate fails closed. |
| SDIP-T11 | Principal/provider/capability/distribution/learning revocation blocks future and active progression. |
| SDIP-T12 | Optional persistence without retention basis is denied; withdrawal restricts future use and dependent intelligence. |
| SDIP-T13 | Restore preserves scope and revalidates current authority; revoked rights are not restored. |
| SDIP-T14 | Test visibility and data remain separately marked and confer no production rights. |
| SDIP-T15 | Suspected incidents are contained/evidenced/escalated and cannot be suppressed by the implicated principal. |
| SDIP-T16 | Security/scope/provider/distribution/secret gates remain deterministic without agents. |
| SDIP-T17 | Agent/tool/provider recomposition derives and preserves all effective constraints. |
| SDIP-T18 | Stage 2 authorization, lifecycle, persistence, routing and exactly-once regression remain unchanged. |

## Deferred authority oracle

AB-AUTH-001 through AB-AUTH-019 and SDIP-AV-001 through SDIP-AV-010 are
represented as versioned authority values. No implementation default supplies
them except the policy-fixed deny states: autonomy at most Level 2 when not
explicitly authorized, no protected provider use, no cross-client
generalization/reuse, no persistent individual adaptation, no routine novel
intelligence distribution, no runtime SD1 access, and no automated destructive
Stage 2 deletion. Any dependent enabled request returns `AUTHORITY_VALUE_REQUIRED`,
`DENIED`, `HOLD`, `PENDING`, `ESCALATE`, or `STOP` as its contract requires.
