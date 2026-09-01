**1. Annex B final authority artifact**

# HUBFLO AGENT LAYER 2.0 — ANNEX B ACCEPTANCE CONTRACT 1.1

**Document:** Annex B — Agent-Layer Acceptance Contract
**Version:** 1.1
**Status:** **FINAL / AUTHORITY-LOCKED AGENT LAYER ACCEPTANCE CONTRACT — IMPLEMENTATION NOT AUTHORIZED**
**Scope:** Future HUBFLO Runtime Agent Layer only
**Implementation authority:** None
**Stage 2 effect:** None
**Version-lock basis:** `PASS — ANNEX B 1.1-RC3 CONTRACT ACCEPTED FOR NEXT AUTHORITY GATE`
**Remaining implementation gates:** launch-scope Security, Data & IP Protection Policy version lock; required launch-scope authority/configuration values; canonical Take-on authority instrument where applicable.

Annex B 1.1 consolidates Annex B 1.0 with the approved O3 architecture. It preserves Stage 2 as immutable operational authority and incorporates the contract properties established through the O3 reconciliation history.

## 1. Purpose

Annex B defines the architectural, behavioral, authority, security, persistence and acceptance contract for HUBFLO Agent Layer 2.0.

It exists so a later authorized engineering agent can implement the Agent Layer without inventing:

- operational authority;
- tenancy;
- security boundaries;
- autonomy;
- configuration semantics;
- learning scope;
- intelligence ownership/use;
- delegation;
- distribution;
- entitlement;
- provider authority;
- approval;
- acceptance rules.

It defines required outcomes and properties, not unnecessary implementation mechanics.

This contract does **not** authorize implementation, deployment, Stage 2 modification or self-acceptance.

## 2. Controlling authority and preservation

Controlling authority remains HES1.0, CE2.0, closed Stage 2, Package 1.2 and the applicable O3/RTW7 authority.

Stage 2 is closed and immutable at:

`8bc773baba23930d7ad181d066b74cd8cc0c2601`

Agent Layer MUST compose over Stage 2 rather than recreate it.

Stage 2 remains authoritative for:

- operational state;
- accepted business lifecycles;
- authorization and client/project scope;
- persistence;
- exactly-once/replay protections;
- routing and structured operational interpretation;
- authoritative handlers;
- business validation;
- consequential operational mutation;
- authoritative operational outcomes.

Agent Layer owns flexible intelligence topology above that bedrock: reasoning, learning, supervision, composition, configuration, information use, distribution and governed progression toward action.

The accepted CE2.0 dependency direction remains explicit: **Industry Module → Core**. Core MAY use the generic Industry contract, but Core MUST NOT depend on a concrete Industry Module. Industry-specific interpretation may enrich Core/Agent reasoning only through that preserved contract boundary; Agent Layer MUST NOT invert or bypass it.

External autonomous engineering agents and HUBFLO Runtime Agents remain separate systems.

## 3. Normative language

**MUST / MUST NOT / REQUIRED / SHALL / SHALL NOT** are binding.

**SHOULD / SHOULD NOT** are strong defaults and require recorded rationale for departure.

**MAY** is optional within the stated authority boundary.

**AUTHORITY VALUE REQUIRED** identifies a value intentionally not fixed by architecture. Safe configuration machinery MAY be implemented around it, but an implementation agent MUST NOT choose the binding value.

## 4. Foundational product rules

**B-FND-001 — Deterministic operational bedrock**

Routine deterministic HUBFLO work MUST remain deterministic where technically possible.

Agent Layer MUST invoke accepted operational capabilities rather than replace their logic.

**B-FND-002 — Agents supervise complexity**

Agent reasoning is appropriate for judgment, diagnosis, prediction, prioritization, adaptation, coordination, optimization, learning, exception management and long-running objectives.

The architecture MUST NOT route every ordinary event through probabilistic reasoning merely because an Agent Layer exists.

**B-FND-003 — HUBFLO owns truth and durable intelligence**

HUBFLO owns authoritative operational state, objectives, permissions, configuration, execution state, learning records, derived intelligence, audit and outcomes.

Provider/model session memory, generated prose, embeddings or transient provider state MUST NOT become authoritative merely because a model produced or retained them.

**B-FND-004 — No parallel business architecture**

Agents MUST NOT create:

- a second business-recognition authority;
- a second authorization system;
- parallel lifecycle authority;
- a parallel authoritative business-object store;
- hidden approval paths;
- arbitrary DB mutation outside governed capabilities.

**B-FND-005 — Capability and authority are separate**

Reasoning ability does not authorize action.

The universal ceiling is:

**available capability + configured authority + current policy/security constraints**

Learning, confidence, model capability or successful prior execution MUST NOT self-create additional authority.

**B-FND-006 — Platform sovereignty**

HUBFLO retains platform sovereignty by default.

Authority, topology, capability exposure, information use, delegation, autonomy, distribution and entitlement MAY be configured, expanded, restricted or revoked only through governed HUBFLO mechanisms.

Delegation does not surrender platform sovereignty.

**B-FND-007 — Configuration principle**

Permanent invariants and generic enforcement machinery are contractual.

Evolving relationships, partitions, topology, authority, intelligence use, learning use, distribution, entitlement and commercial controls are configurable unless a permanent invariant explicitly forbids configuration.

The governing principle is:

**hard-code generic machinery and permanent invariants; configure evolving relationships, partitions, topology, authority, distribution, learning use, entitlements and commercial controls.**

## 5. Architectural planes and runtime roles

### 5.1 Control Plane

Owns:

- policy;
- effective configuration;
- permission;
- approval;
- autonomy;
- governance;
- kill/disable controls;
- entitlement enforcement;
- audit visibility;
- health/performance governance;
- supervisory configuration.

The Control Plane determines whether a reasoned proposal is eligible to progress toward execution.

### 5.2 Intelligence Plane

May:

- reason;
- diagnose;
- predict;
- combine evidence;
- learn;
- develop/refine derived intelligence;
- supervise objectives;
- recommend;
- plan;
- select candidate governed capabilities.

Intelligence does not itself constitute execution authority.

### 5.3 Execution Plane

Contains governed deterministic capabilities, including Stage 2 handlers and later separately authorized platform/integration capabilities.

Consequential execution MUST reach an authoritative capability through deterministic policy enforcement.

### 5.4 Runtime roles

**Hub Manager**

Owner/control-facing orchestration, evidence aggregation, management by exception, bounded delegation, approvals, supervisory control and governed capability invocation.

It is not an unrestricted superagent.

**Industry Flo**

Provides industry-scoped terminology, language, domain interpretation, sequencing/dependency knowledge, approved industry intelligence and governed learned vocabulary.

Industry Flo MUST be capable of learning terminology and expressions actually used within its governed industry scope without converting learned terminology into business authority.

**Client / Company Flo**

Represents one client's authorized operational context, policy, objectives, terminology, configuration, permissions and private learned intelligence.

It remains tenant-confidential.

**Manager PA**

A governed practical assistant associated with an authorized user/manager.

It MAY:

- assist with permitted day-to-day work;
- explain relevant operational information;
- teach the user how to use functionality actually enabled for them;
- prepare recommendations/drafts;
- use permitted individual-user working preferences.

It MUST inherit applicable user/client/project access rather than become a privileged super-role.

Learned user preferences MUST NOT become authorization, business fact, approval or authoritative operational state.

Persistent personal adaptation requires the applicable learning/retention policy.

**Take-on / Client Evolution**

Provides agent-assisted discovery, proposal, explanation and governed configuration for initial Take-on and subsequent evolution.

**Platform Guardian**

Supports:

**observe → diagnose → report/escalate → recommend repair → governed engineering/remediation handoff**

Guardian MAY later invoke bounded remediation capabilities only where those capabilities and authority are separately configured.

No Guardian role implies source-write, infrastructure-change, deployment or security-control authority.

Higher-impact remediation requires correspondingly stronger deterministic security, approval, verification and audit.

**Capacity / Architecture Intelligence**

Reasons about platform load, capacity, resilience, cost and future technical requirements.

**Market / Technology Intelligence**

Monitors attributable external technical/provider/security/regulatory developments and recommends action.

**Specialists / distributed agents**

MAY be used when justified by isolation, permissions, workload scaling, independent verification, specialization, parallelism or resumability.

The simplest topology satisfying the contract SHOULD be preferred, but no permanent “single orchestrator” topology is mandated.

## 6. Principal, authority, hierarchy and scope model

**B-AUTH-001 — Principal resolution**

Every consequential action MUST resolve to an authoritative principal or authorized service context.

Supported authority concepts MUST include:

- platform authority;
- client authority;
- user authority;
- service/integration authority;
- delegated runtime-agent authority.

Agent principals originate no new authority.

**B-AUTH-002 — Current authority**

Consequential execution MUST evaluate current authoritative permission/configuration.

Historic or cached authority metadata MUST NOT override revocation, narrowing, kill switches or changed scope.

**B-AUTH-003 — Configurable scope dimensions**

Effective access/authority MUST be capable of being expressed across applicable combinations of:

- platform;
- industry/Flo;
- client;
- project;
- business object;
- function;
- capability;
- action/risk class;
- integration/channel;
- time/delegation condition;
- entitlement.

Not all dimensions must be used for every capability.

**B-AUTH-004 — Non-escalation**

No agent may:

- create authority;
- self-assign role;
- widen its client/project access;
- promote its autonomy;
- alter approval rules to authorize itself;
- grant another agent unauthorized authority;
- convert learning/entitlement into permission.

**B-AUTH-005 — Capability-to-action progression**

An agent may progress only as far as current capability and configured authority allow.

Supported progression MAY include:

1. Observe
2. Analyze / Recommend
3. Request Approval
4. Execute Within Defined Limits
5. Full Permitted Autonomy

A capability need not support every level.

**B-AUTH-006 — Recovered client hierarchy constraints**

The recovered baseline hierarchy is:

`SUB → PM → OPS → DIRECTOR → OWNER`

OPS is optional where configured.

Hierarchy rank does **not** automatically grant blanket visibility or lower-role action authority.

Project visibility is explicit allocation/authorization.

A General Manager/equivalent MAY be configured for all projects or a subset.

Temporary delegation/substitution MUST remain within existing authority and be scope-bounded, time/revocation-bounded and auditable.

Where a configured hierarchy level is absent/unavailable, escalation proceeds to the next configured higher level unless governing Take-on configuration supplies another authorized fallback.

Hierarchy, permission and responsibility configuration changes MUST pass through governed HUBFLO configuration.

**B-AUTH-007 — Undefined role/capability matrix**

Exact organizational-role → Agent-Layer-capability permissions remain authority/configuration values except where explicitly governed above or by higher authority.

Unassigned consequential permissions fail closed.

## 7. Tenant, project and information isolation

**B-ISO-001 — Client confidentiality**

Client A proprietary data MUST NOT silently enter Client B:

- prompt/context;
- retrieval;
- caches;
- embeddings;
- traces/logs;
- provider sessions;
- tests;
- private learning;
- intelligence products.

An explicitly approved aggregate/generalization path is the only permitted exception.

**B-ISO-002 — Project scope**

Project-specific capabilities receive only authorized project context.

Cross-project reasoning MAY occur only where the effective capability and principal are authorized for the broader scope.

**B-ISO-003 — Evidence-scoped context**

Material context SHOULD retain sufficient metadata to establish:

- source/provenance;
- client/project or broader governed scope;
- evidence class;
- freshness;
- relevant authorization attributes;
- derived versus authoritative status.

**B-ISO-004 — Persistent continuation ceiling**

Persisted candidate/objective context MUST NOT silently broaden its authority/candidate universe on continuation.

Current authority MAY narrow previously eligible information.

**B-ISO-005 — Provider boundary**

A provider mechanism that cannot satisfy required confidentiality/isolation MUST NOT receive the protected context.

**B-ISO-006 — Testing**

Isolation tests MUST cover retrieval, providers, caches, learning, derived intelligence, handoffs, configuration and distribution.

## 8. Security-domain separation

The architecture MUST explicitly distinguish:

1. **Source / engineering assets**
   - source code;
   - repositories;
   - build/release controls;
   - engineering artifacts;
   - deployment controls.
2. **Credentials / secrets**
   - provider credentials;
   - runtime secrets;
   - integration credentials;
   - repository/deployment credentials;
   - authentication secrets.
3. **Runtime / client operational data**
   - authoritative client/project records;
   - communications;
   - operational evidence;
   - client documents/information.
4. **Learned / derived intelligence**
   - learned patterns;
   - findings;
   - predictions;
   - benchmarks;
   - inferred relationships;
   - derived intelligence products.

Access to one domain does not imply access to another.

Credentials/secrets MUST NOT become model context or learned state.

Source/engineering access MUST NOT be inferred from Guardian/runtime-agent visibility.

Client operational data MUST NOT enter engineering/provider contexts except through separately authorized scoped handling.

Derived intelligence retains its own provenance, ownership/scope, distribution and retention controls.

The applicable **HUBFLO Security, Data & IP Protection Policy** MUST be version-locked as a mandatory **pre-implementation gate for the entire Agent Layer launch scope**. No launch-scope Agent Layer implementation may begin while that applicable policy is missing or unversioned. This gate does not authorize an implementer to invent unresolved policy values.

## 9. Configuration, policy and effective-state contract

**B-CFG-001 — First-class configuration capability**

Configuration/policy is a first-class Agent Layer capability, not bespoke logic embedded separately inside each Flo.

Generic machinery MUST be capable of expressing, where applicable:

- agents/Flos;
- capabilities;
- composition/supervisory topology;
- information/intelligence sources;
- integrations;
- client/industry partitions;
- delegation;
- authority/autonomy;
- learning scope/use;
- recipients/distribution;
- reasoning intensity;
- frequency;
- usage/rate controls;
- entitlement;
- commercial controls.

Domain-specific extensions MAY exist where genuinely required.

One universal schema is not mandated. Common enforcement semantics are.

**B-CFG-002 — Proposed versus effective configuration**

Proposed configuration and effective committed configuration MUST remain distinct.

Model suggestion is never authoritative configuration.

Configuration becomes effective only through the applicable governed deterministic capability.

**B-CFG-003 — Effective-state introspection**

HUBFLO MUST be capable of determining/explaining, for material behavior:

- applicable configuration;
- actual effective configuration;
- relevant capability availability;
- information scope;
- authority/autonomy ceiling;
- entitlement;
- dependencies/policy constraints;
- why a material output/action was permitted, blocked or routed as it was.

**B-CFG-004 — Provenance and versioning**

Material configuration/policy/composition changes MUST preserve sufficient durable history to establish:

- before state;
- requested change;
- proposer/initiator;
- approving authority where required;
- reason/evidence where material;
- version/effective time;
- resulting effective state.

**B-CFG-005 — Revocation / rollback**

Configuration and delegation MUST be revocable.

Rollback MUST be supported where technically and semantically valid.

Already-completed irreversible operational actions MUST NOT be represented as rolled back merely because configuration was reverted.

**B-CFG-006 — Safe recomposition**

Creating a new composition MUST NOT require security/authority protection to be manually rebuilt for every topology.

The platform MUST derive and enforce the effective constraints applicable to that composition.

Constraint propagation is not assumed to be identical for every dimension; the platform must determine which current security, information, authority, entitlement and mutation constraints apply.

**B-CFG-007 — Platform sovereignty**

Configuration MAY delegate, restrict, expose, withdraw, throttle, combine, recompose or entitle behavior only within platform-governed limits.

Configuration cannot disable permanent safety invariants.

O3 expressly establishes configuration and safe recomposition as structural architecture rather than per-Flo special cases.

## 10. Capability / tool contract

Every executable or decision-support capability available to runtime agents MUST exist in HUBFLO-controlled capability governance.

Each material capability version MUST make determinable:

- capability identity/version;
- purpose;
- eligible agent/principal classes;
- input/output contract;
- applicable information scope;
- required permission/configuration;
- side-effect/risk classification;
- authoritative execution capability/handler where applicable;
- preconditions;
- idempotency/replay requirements;
- **capability-specific concurrency/claim behavior, including the exact required result for concurrent or competing attempts and whether a claim/ownership concept is applicable;**
- approval requirements;
- audit requirements;
- failure/retry/fallback behavior;
- **capability-specific uncertain-outcome/retry behavior, including the authoritative evidence that MUST be checked before any consequential retry and the exact required result when prior outcome cannot be established;**
- **capability-specific no-agent/degraded behavior, including whether the capability continues deterministically, degrades to a defined non-consequential mode, remains pending/escalates, or stops;**
- verification requirements;
- kill/disable state;
- health/status;
- regression dependencies.

A capability for which concurrency/claim, uncertain-outcome/retry, or no-agent/degraded behavior is not applicable MUST state that explicitly and the reason MUST be testable from the capability contract. Silence is not a valid contract.

**B-TOOL-X001 — Governed invocation**

Disabled, unhealthy, unauthorized, incompatible or unavailable consequential capabilities MUST NOT execute.

**B-TOOL-X002 — Structured consequential invocation**

Consequential invocation MUST be represented as governed structured data rather than reconstructed from free-form prose at execution time.

**B-TOOL-X003 — Deterministic execution gate**

Agent selection does not authorize execution.

The deterministic gate MUST evaluate applicable:

- principal;
- information/execution scope;
- capability;
- configured authority/autonomy;
- risk/side-effect class;
- approvals;
- kill/disable state;
- current health;
- idempotency/replay state;
- entitlement where material.

**B-TOOL-X004 — Authoritative mutation**

Consequential business mutation MUST pass through the applicable authoritative HUBFLO capability.

**B-TOOL-X005 — Capability-specific concurrency / claim and exactly-once contract**

Each material capability MUST define its required behavior when the same or competing work can arrive concurrently, be replayed, or race with continuation/recovery.

The contract MUST state, as applicable:

- whether concurrent processing is permitted, serialized, rejected, coalesced or otherwise governed at the behavioral level;
- whether a claim/ownership step is required before consequential progression;
- what current authority/state MUST be revalidated after claim/selection and before consequential execution;
- what the losing, duplicate or already-completed attempt MUST return/do;
- the exact expected authoritative result for each material concurrency/replay case.

For consequential mutation, retry/replay/concurrency MUST NOT produce duplicate consequential mutation. If a capability does not require a claim/ownership concept, that non-applicability MUST be explicit and MUST NOT weaken its replay/idempotency guarantee.

This requirement defines observable contract behavior only; it does not prescribe transaction, lock, queue, graph, class, API or storage mechanics.

**B-TOOL-X006 — Capability-specific uncertain-outcome / retry contract**

Each material capability MUST define behavior for any invocation whose transport, provider, process or execution result is uncertain.

Before repeating a consequential action, HUBFLO MUST inspect the capability's defined authoritative outcome/idempotency evidence. The contract MUST define the exact expected result for at least these applicable states:

- prior authoritative completion is established → return/use that outcome and do not repeat the consequence;
- prior non-completion is established and retry remains permitted → retry only under the capability's governed replay/concurrency contract;
- prior outcome cannot be established safely → do not speculate or repeat the consequence; preserve state and STOP/ESCALATE or remain pending according to that capability's declared failure policy.

A model/provider assertion, transport success/failure or missing reply is not by itself authoritative proof of operational outcome.

**B-TOOL-X007 — Capability-specific no-agent / degraded contract**

Each material capability MUST declare the exact behavior when required agent/model reasoning is unavailable or degraded.

The contract MUST specify whether the capability:

- continues through an existing deterministic path;
- degrades to a defined observe/analyze/non-consequential mode;
- remains pending and escalates;
- or stops fail-closed.

No-agent/degraded behavior MUST NOT fabricate judgment, broaden information access, increase authority/autonomy, bypass approval/verification, or alter Stage 2 operational authority. Recovery MUST revalidate current authority, configuration and material state before consequential progression.

**B-TOOL-X008 — Substitution safety**

Fallback or substitution MUST NOT silently change scope, semantics, authority, side-effect class or data visibility.

## 11. Side-effect classes

Each executable capability/action MUST carry a governing side-effect classification:

- **S0 Observe** — read/observe only.
- **S1 Analyze** — derived analysis/recommendation; no authoritative business mutation.
- **S2 Internal reversible mutation** — governed reversible internal state.
- **S3 Operational/external action** — consequential operational mutation or external communication within approved authority.
- **S4 Restricted high-risk action** — legal/commercial/security/destructive/infrastructure action requiring exceptional governance.

An agent cannot downgrade the applicable class.

## 12. Risk classes

Risk classes remain:

- **R0** negligible/read-only;
- **R1** low/reversible;
- **R2** material operational;
- **R3** high consequence;
- **R4** restricted/legal/security.

Binding monetary, schedule, payment, deletion or other materiality thresholds remain authority-owned.

Ambiguous high-impact action MUST fail toward the safer/higher-risk treatment.

## 13. Autonomy model

Autonomy is capability-specific, scope-specific and policy-governed.

**Level 1 — Observe**
Monitor permitted information.

**Level 2 — Analyze / Recommend**
Diagnose, predict, prioritize, explain or recommend without consequential execution.

**Level 3 — Request Approval**
Prepare a bounded structured proposed action for an authorized approver.

**Level 4 — Execute Within Defined Limits**
Execute within explicitly configured limits without case-by-case approval.

**Level 5 — Full Permitted Autonomy**
Execute throughout the full explicitly permitted envelope. “Full” never means unrestricted.

**B-AUTO-X001 — Separability**

Reasoning depth, proactivity, distribution and execution authority are independently governable.

**B-AUTO-X002 — Promotion**

Promotion requires policy and measured evidence; model capability alone is insufficient.

**B-AUTO-X003 — Degradation**

Health, policy, performance or security deterioration MUST be capable of reducing autonomy.

**B-AUTO-X004 — Shadow mode**

Consequential promotion MUST support shadow/replay evidence where required.

**B-AUTO-X005 — Unresolved default**

Until explicitly authorized, a new capability MUST NOT execute above Level 2.

This is fail-closed configuration, not a permanent product ceiling.

**B-AUTO-X006 — R4**

R4 actions remain approval/verification gated unless later explicit authority defines a narrower exception.

## 14. Approval, escalation, override and disable control

Approvals MUST be durable and bind sufficiently to:

- principal;
- action/capability;
- scope;
- material parameters;
- evidence;
- risk;
- decision;
- expiry/revalidation;
- execution result.

Material change invalidates approval unless the approval explicitly covered the bounded change.

Escalation MUST preserve evidence, scope, attempted action and required authority.

Escalating to a more senior recipient does not automatically expand the initiating agent's access or authority.

Authorized principals MUST be able to lower/disable Agent-Layer behavior at least at:

- global Agent Layer;
- client/Flo;
- agent role/type;
- capability;
- action/risk class.

Where implemented, project/provider/integration-level controls follow the same fail-closed principle.

Configuration revocation affects future eligibility immediately after authoritative propagation; it does not silently undo completed authoritative operations.

## 15. Durable objective state

Long-running objectives are HUBFLO-owned durable state.

The architecture MUST support sufficient objective state for:

- identity/version;
- owner/principal;
- scope;
- desired outcome;
- success criteria;
- deadline/no-deadline;
- dependencies;
- evidence;
- status/risk;
- recommendations/interventions;
- approvals;
- completed work;
- next eligible work;
- confidence metadata where relevant;
- outcomes/history;
- responsible capability/version;
- cancellation/closure.

Generated narrative is not objective-state authority.

Resumption MUST revalidate current information access, execution authority, dependencies, entitlement where relevant and current operational state.

Objective cancellation does not automatically cancel underlying Stage 2 business objects.

## 16. Durable execution and scalable composition

Agent execution MUST scale from simple direct work to more complex topology without replacing fundamental contracts.

The architecture MUST support, where justified:

- bounded work units;
- durable execution state;
- explicit dependencies;
- hierarchical execution;
- graph execution;
- recursive/specialist decomposition;
- fan-out/fan-in;
- checkpoints;
- resume;
- local retry/recovery;
- structured artifacts/references;
- independent verification;
- workload-appropriate reasoning/model cost;
- independently scalable client/Flo workloads.

Complex topology is optional, not mandatory.

Each bounded unit MUST have enough definition to determine input, output/completion and failure behavior.

Cycles MUST be bounded by attempts, budget, deadline or other deterministic completion/stop criteria.

Consequential checkpointing MUST prevent re-execution of completed non-idempotent work.

A large transcript MUST NOT be the sole durable execution state.

This preserves O3's requirement that simple execution can evolve into graph/hierarchical/specialist/distributed work without changing underlying authority/isolation contracts.


**B-COMP-X001 — Scalable-composition acceptance claim**

If the accepted launch scope claims fan-out/fan-in or scalable distributed composition, acceptance MUST include at least one representative case with:

- one governed parent objective/work item;
- two or more independently bounded fan-out work units with explicit scope, authority ceiling, inputs and completion/failure state;
- independent execution/evaluation of those work units without shared authority creation;
- structured returned artifacts/results;
- a governed fan-in that evaluates branch completion, scope, confidentiality/security constraints, authority, failures and result compatibility before producing the combined result;
- no duplicate consequential side effect across branches, retry or join;
- durable route/checkpoint evidence sufficient to resume safely.

The governed fan-in MUST NOT:

- treat a failed/incomplete required branch as successful completion;
- widen client/project/information scope because another branch had broader access;
- convert branch awareness into recipient or execution authority;
- discard required failure/verification evidence merely to obtain convergence.

**Exact acceptance result:** the representative fan-out completes or fails independently per branch contract; the fan-in produces a combined result only from eligible completed inputs under current policy and preserves any required failure/escalation state. No authority, tenant/confidentiality boundary or consequential exactly-once guarantee is widened by composition.

If fan-out/fan-in scalable composition is not implemented for the launch scope, the acceptance manifest MUST explicitly exclude that claim. It MUST NOT be counted as accepted capability by architectural possibility alone.

## 17. Cross-agent transition and delegation contract

Where work crosses agent/capability boundaries, the transition MUST preserve enough structured context to determine:

- source/target;
- objective/work identity;
- requesting/owning principal;
- requested work;
- relevant context/evidence references;
- applicable information constraints;
- delegated execution ceiling where applicable;
- permitted capabilities/tools;
- risk/side-effect ceiling;
- completion/failure state;
- budgets/deadlines where relevant;
- returned artifacts/results.

**B-XAG-001 — No authority creation**

Delegation cannot create authority that no authorized principal/configuration possesses.

**B-XAG-002 — Information access is not identical to action authority**

The receiving agent's permitted awareness/context and its execution authority MUST each be evaluated under their applicable current policy.

A cross-agent transition MUST NOT assume every information, distribution and execution scope shares identical topology.

**B-XAG-003 — Recipient access is separate**

An internal/supervisory agent's awareness does not grant downstream recipients access to the same information.

**B-XAG-004 — Structured artifacts**

Stable structured facts/artifacts/references SHOULD be preferred over uncontrolled transcript propagation.

**B-XAG-005 — Completion integrity**

Narrative or silence does not constitute reliable completion evidence.

## 18. Provider/model neutrality

Providers/models remain replaceable reasoning engines.

HUBFLO-owned operational state, configuration, objectives, learning, derived intelligence and execution history MUST survive provider substitution.

Provider output driving governed behavior MUST satisfy the required structured contract before becoming eligible for downstream use.

Substitution MUST preserve:

- permitted data classification;
- information scope;
- authority ceiling;
- side-effect semantics;
- output compatibility;
- audit attribution.

Provider failure MUST NOT corrupt durable work state.

Binding provider allowlists, retention restrictions, routing weights and budgets remain policy/authority values.

## 19. Deterministic/no-agent fallback

If model services are unavailable:

- ordinary deterministic HUBFLO behavior SHOULD continue where technically possible;
- judgment-dependent work MUST explicitly degrade, stop, remain pending or escalate;
- HUBFLO MUST NOT fabricate a judgment;
- fallback cannot increase permission or autonomy;
- resumed work MUST revalidate current state/configuration.

## 20. Failure, degradation, retry and recovery

Supported failure outcomes include:

- RETRY;
- FALLBACK;
- SKIP;
- REPAIR;
- ESCALATE;
- STOP.

Failure SHOULD remain local to the smallest reasonable capability/work boundary.

Retries MUST be bounded.

Uncertain consequential outcomes MUST be inspected against authoritative idempotency/outcome state before retry.

Degraded health MUST be explicit.

Recovery MUST resume from HUBFLO-owned durable state, not assumed model memory.

Disaster continuity/RPO/RTO values remain separately governed.

## 21. Audit, provenance and evidence contract

Material agent behavior MUST be auditable without storing private chain-of-thought.

Sufficient audit evidence MAY include:

- principal;
- scope;
- objective/work;
- configuration/policy version;
- capability/tool;
- provider/model/version where used;
- evidence references;
- structured decision/result;
- autonomy;
- approval;
- delegation;
- idempotency/claim;
- authoritative handler/outcome;
- retry/fallback;
- distribution;
- entitlement basis;
- override/disable state;
- timestamps.

Audit history MUST be protected from ordinary agent modification.

## 22. Evidence and confidence

Evidence classes remain:

- authoritative HUBFLO state;
- external primary/authoritative evidence;
- observation/telemetry;
- historical HUBFLO outcomes;
- approved aggregate evidence;
- benchmarks;
- model inference;
- user assertion where verification matters.

Confidence is not evidence.

Known/inferred dependency states MUST remain distinguishable.

Benchmarks MUST NOT be represented as client fact.

Thresholds used to unlock autonomy remain authority-owned.

## 23. Learning, retention and generalization

Learning is first-class HUBFLO-owned state.

The architecture MUST support four separate learning scopes:

1. **platform**
2. **industry**
3. **client**
4. **individual user**

These are structural scopes, not a predefined promotion hierarchy.

**B-LEARN-X001 — Learning record**

Learning-relevant evidence SHOULD preserve sufficient linkage between:

- observation/evidence;
- prediction/finding;
- provider/model/version where applicable;
- confidence;
- recommendation/action;
- approval;
- actual outcome;
- error/variance;
- human override;
- capability/configuration version.

**B-LEARN-X002 — Client privacy**

Client-specific learning remains private unless an explicit governed aggregation/generalization path applies.

**B-LEARN-X003 — Cross-client path**

Any cross-client aggregation requires the approved data/security policy and MUST NOT silently transfer proprietary client content.

**B-LEARN-X004 — Provider independence**

Learned state survives provider/model substitution.

**B-LEARN-X005 — Human override**

Override is a learning signal, not automatic evidence that the original reasoning was wrong.

**B-LEARN-X006 — Industry language**

Industry Flo MAY learn vocabulary, terminology, abbreviations and expressions actually used within its governed industry/sector scope.

Learned terminology may improve recognition, explanation and reasoning.

It MUST NOT create authorization or override deterministic business truth.

**B-LEARN-X007 — Industry/sector/Flo boundaries**

The architecture MUST support explicit governed industry and, where configured, sector/subsector boundaries.

It MUST NOT infer cross-sector or cross-industry reuse merely from linguistic similarity.

Actual taxonomies, memberships and permitted reuse are configuration/policy values.

**B-LEARN-X008 — Individual-user adaptation**

Permitted user preferences may influence assistance and presentation only within the applicable user/client scope.

Individual learning cannot silently propagate into client/industry/platform learning.

**B-LEARN-X009 — Promotion/generalization**

Learning may move between scopes only through an explicit governed mechanism.

The architecture MUST support promotion/generalization but MUST NOT hard-code presently unresolved promotion policy.

**B-LEARN-X010 — Lifecycle**

Learning MUST be capable of correction, validation, refinement, decay/retirement where appropriate, withdrawal, supersession and replacement without destroying provenance/history required for governance.

The exact retention, de-identification, withdrawal and promotion values remain separately governed. The four scopes and Industry Flo learning are preserved O3 requirements.

## 24. Persistent derived intelligence

Derived intelligence is a first-class HUBFLO-owned asset separate from authoritative operational truth.

The architecture MUST be capable of durably representing, where material:

- learned patterns;
- findings;
- predictions;
- evidence/provenance;
- confidence;
- validation;
- outcomes;
- benchmarks;
- anomalies;
- dependency intelligence;
- critical-path intelligence;
- consequence/risk intelligence;
- client/sector/industry intelligence;
- intelligence products;
- confirmed/rejected/superseded/refined state;
- ownership/scope;
- awareness/use state;
- distribution state.

**B-INTEL-X001 — Operational truth separation**

Derived intelligence MAY reason over authoritative Stage 2 records.

It MUST NOT silently rewrite Stage 2 facts, lifecycle state or dependency truth.

Promotion of an inferred relationship into authoritative operational state requires a separate governed capability.

**B-INTEL-X002 — Provenance**

Material intelligence MUST retain enough provenance/evidence to distinguish:

- observed fact;
- historical outcome;
- external evidence;
- benchmark;
- inference;
- later validation.

**B-INTEL-X003 — Lifecycle**

Intelligence MUST support refinement/supersession without erasing the provenance of prior material conclusions.

**B-INTEL-X004 — Validation/outcome**

Where actual outcomes later become known, the architecture SHOULD support comparison against prior predictions/findings.

**B-INTEL-X005 — Provider portability**

Derived intelligence cannot exist only inside provider memory/session state.

O3 explicitly requires persistent findings, predictions, provenance, validation, benchmarks, anomalies and refinement state independent of model sessions.

## 25. Awareness, supervisory use and distribution

The architecture MUST distinguish:

1. intelligence exists;
2. an agent may be aware of it;
3. it may influence authorized internal reasoning;
4. HUBFLO supervisory/control functions may inspect it;
5. it may be distributable;
6. a particular recipient is entitled/authorized to receive it.

These are not interchangeable.

**B-DIST-X001 — Awareness does not equal distribution**

An agent being permitted to reason over information does not confer recipient access.

**B-DIST-X002 — Supervisory protection**

Platform technical, security, engineering and internal intelligence remains independently protected from ordinary client-facing distribution.

**B-DIST-X003 — Novel intelligence**

Default treatment:

**novel / unclassified intelligence → eligible HUBFLO supervisory review/visibility first, subject to independently authorized access**

Novelty creates **no access authority**. The fact that intelligence is new, unusual, unclassified or potentially valuable MUST NOT by itself make it visible to any supervisor, agent, client, recipient or engineering function.

Any supervisory visibility MUST still satisfy the applicable current:

- tenant/client/project and information-scope authorization;
- confidentiality restrictions;
- security-domain separation;
- Security, Data & IP Protection Policy;
- source/provenance restrictions;
- entitlement/distribution constraints where applicable.

If no authorized supervisory principal/function may inspect the intelligence, HUBFLO MUST retain or quarantine it only as permitted by applicable policy and MUST NOT broaden visibility to obtain review.

Novel intelligence MUST NOT automatically become client-distributed merely because an agent generated it.

**B-DIST-X004 — Established intelligence**

Once an intelligence/output type and its use/distribution have been deliberately governed, it follows effective configuration without repeated owner approval for every routine occurrence.

**B-DIST-X005 — Test visibility**

Controlled development/testing configurations MAY expose wider behavior/findings for evaluation without establishing production distribution rights.

**B-DIST-X006 — No fixed recipient catalogue**

The architecture MUST allow recipients and output/distribution relationships to be configured rather than permanently hard-coded to today's role catalogue.

These separations and the novel-intelligence default are explicit O3 architecture.

## 26. Security observability

Security observability is distinct from ordinary platform-health monitoring.

It SHOULD detect applicable:

- unusual access;
- privilege/autonomy change;
- credential failure;
- tenant-boundary violations;
- abnormal agent/tool behavior;
- suspicious data access;
- exfiltration/leakage indicators;
- provider anomalies;
- policy-bypass attempts;
- repeated authorization failure.

Automatic containment is permitted only through a separately governed capability and authority.

Security events cannot be suppressed by the runtime agent implicated in them.

Cross-client security correlation must comply with Security/Data/IP policy.

## 27. High-risk legal/commercial/infrastructure actions

R4/S4 includes, unless a later authority instrument explicitly classifies a bounded subtype otherwise:

- contractual commitments;
- major cost/date commitments;
- payments;
- destructive deletion;
- security/permission/autonomy changes;
- provider/DB/major infrastructure migration;
- external publication;
- legally material communication.

Such execution requires the applicable approval and independent-verification controls.

Approval MUST bind material parameters.

Reversal limitations MUST be known where material.

## 28. Independent verification

Risk-appropriate consequential work MUST NOT rely solely on the same context acting as generator, verifier, approver and publisher.

Verification may be:

- deterministic;
- independent model/agent context;
- human;
- combined.

Required verification produces structured:

- PASS;
- FAIL;
- INCONCLUSIVE.

INCONCLUSIVE cannot be promoted to PASS by the executing agent.

R4/S4 requires independent verification unless higher authority later provides a governed exception.

## 29. Performance, reasoning cost and load governance

Capabilities SHOULD measure applicable:

- accuracy;
- intervention effectiveness;
- false positives/negatives;
- overrides;
- failure;
- latency;
- cost;
- recovery;
- security/isolation events.

Reasoning is a governed resource.

The architecture MUST support independently configurable:

- reasoning depth/intensity;
- budgets;
- rate/frequency;
- prioritization;
- cheaper deterministic paths;
- higher-cost reasoning where justified;
- overload degradation.

Under overload, deterministic and safety/consequential workflows SHOULD be protected ahead of optional intelligence work.

Exact budgets/limits remain configuration values.

## 30. Entitlement and commercial carveability

Entitlement is distinct from execution authorization.

The architecture MUST support independent governed entitlement, where required, across:

- users/seats;
- projects;
- agent/Flo access;
- capability/functionality;
- source/data access;
- learned/derived intelligence;
- predictive intelligence;
- reports/outputs;
- recipients;
- delivery frequency;
- reasoning depth/intensity;
- automation/autonomy availability;
- premium intelligence/services.

Entitlement does not override tenant/security/authorization controls.

A user commercially entitled to a capability is still limited by applicable information and execution authority.

Payment/billing state MAY influence effective entitlement through governed deterministic platform capability.

Billing/payment itself remains deterministic platform functionality, not probabilistic model mutation.

The architecture MUST permit commercial packaging to evolve without hard-coding current tiers or prices.

It MUST be possible conceptually to support:

**discover value → preserve findings → refine intelligence → package → entitlement → delivery → billing → measure usefulness → evolve**

without making that sequence a single mandatory workflow.

Commercial package levels remain unresolved and MUST NOT be invented by implementation.

## 31. Simulation, replay and shadow evidence

Before consequential autonomy promotion, the applicable system MUST support suitable combinations of:

- deterministic simulation;
- historical replay;
- shadow operation;
- outcome comparison.

Simulation cannot be represented as live evidence.

Replay MUST NOT trigger uncontrolled real side effects.

Shadow decisions MUST retain sufficient attribution to compare proposed behavior with outcomes.

## 32. Acceptance evidence classes

- **T-D** — deterministic/unit/property/contract.
- **T-I** — controlled integration using real application components.
- **T-S** — simulation/replay/shadow.
- **T-F** — failure/recovery/degradation.
- **T-X** — isolation/security/negative authority.
- **T-L** — real authorized live runtime path.
- **T-R** — regression against accepted existing behavior.

Every acceptance artifact MUST identify its evidence class.

Transport/provider success alone is not authoritative outcome evidence.

Security, authority, isolation, configuration and kill-switch acceptance MUST include negative cases.

## 33. PASS / FAIL semantics

**PASS — CAPABILITY ACCEPTED**

Requires:

- all mandatory contract properties for claimed capability/scope;
- all required evidence classes;
- no unresolved authority value affecting enabled claimed behavior;
- regression green;
- authoritative outcome evidence where required.

**FAIL — CAPABILITY NOT ACCEPTED**

Includes:

- unauthorized action;
- leakage;
- direct unauthorized mutation;
- duplicated consequence;
- absent required audit;
- unbounded retry;
- provider fallback broadening authority/data scope;
- failed disable/revocation;
- invented authority value;
- false evidence classification.

**INCOMPLETE — AUTHORITY VALUE REQUIRED**

Used where architecture/implementation may be structurally correct but an authority value required for the claimed enabled behavior is not approved.

## 34. Versioning and change control

Contract, capability, configuration, policy, entitlement and relevant intelligence versions MUST be attributable where material.

Material changes affecting authority, side effect, security, scope, autonomy, distribution or acceptance require versioning and regression review.

Authority thresholds/policies MUST remain distinct from model prompt/version.

Backward compatibility with accepted Stage 2 is mandatory unless separately reopened and governed.

---

# PART II — CAPABILITY-FAMILY CONTRACTS

Every capability below inherits Part I.

## 35. AL-CP-001 — Hub Manager / Control Plane

**Purpose:** management by exception, owner/control orchestration, supervisory visibility and bounded delegation.

**May:** aggregate, explain, recommend, request approval, delegate bounded work and invoke authorized capabilities.

**Must not:** act as unrestricted superagent; suppress evidence; bypass policy; create authority.

**Acceptance:**

- **T-D:** out-of-scope requests denied.
- **T-I:** aggregate multiple structured sources without merging their authority/state.
- **T-X:** cross-client access denied.
- **T-F:** disabling consequential Hub Manager behavior works fail-closed.
- **T-L:** authorized evidence-grounded supervisory query/control case.
- **T-R:** deterministic HUBFLO remains usable without Hub Manager.

**Authority/configuration later:** actual principal/capability mappings.

## 36. AL-FLO-001 — Industry / Client Flo separation

**Purpose:** preserve industry reasoning separately from private client identity/state/learning while supporting configured industry/sector boundaries.

Industry Flo MAY provide governed industry vocabulary, terminology, relationships, domain interpretation and learned industry intelligence.

Client Flo remains client-private.

**Rules:**

- no silent cross-client proprietary context;
- no Client A learning as Client B fact;
- industry/sector membership/reuse is explicit governed configuration;
- learned language is not business authority;
- Core remains independent of individual industry implementation.

**Acceptance:**

- **T-D:** mismatched Flo/client scope rejected.
- **T-I:** shared Industry Flo can support isolated clients without merging private state.
- **T-X:** retrieval/cache/provider/learning/handoff leakage tests.
- **T-L:** correctly scoped client/Flo reasoning.
- **T-R:** Core/Industry dependency remains intact.

## 37. AL-TAKEON-001 — Industry Take-on / Client Take-on / Ongoing Evolution

**Purpose:** establish and evolve governed configuration.

Three distinct logical phases MUST be supported:

1. **Industry Take-on**
   - establish governed industry/Flo scope;
   - industry/sector mapping where applicable;
   - vocabulary/domain-knowledge sources;
   - permitted industry-learning context.
2. **Client Take-on**
   - establish initial governed client configuration, including applicable users/roles, projects, suppliers, terminology, policies, channels, responsibility/escalation, permissions, entitlements and learning consent.
3. **Ongoing Client Evolution**
   - modify governed client configuration after go-live using the same underlying configuration authority and provenance model.

Take-on MAY use natural-language assistance to discover desired outcomes, identify configuration needs, propose configuration and explain consequences.

Uncommitted proposal is not authoritative state.

Consequential configuration change MUST pass through an authorized deterministic configuration capability.

Recovered authority additionally requires:

- baseline `SUB → PM → OPS → DIRECTOR → OWNER`, with OPS optional where configured;
- no rank-derived blanket visibility or lower-role authority;
- project visibility by explicit allocation/authorization;
- configured delegation/substitution only within existing authority;
- governed hierarchy/permission changes;
- Take-on/entitlement control over user/seat creation;
- Take-on-defined project-creation authority;
- project closure as lifecycle closure rather than deletion;
- company-level supplier/trade masters may remain distinct from project assignments.

**Canonical authority-artifact gate:** no complete standalone Take-on authority instrument has yet been recovered. Before implementing behavior dependent on detailed unrecovered Take-on authority, the authoritative artifact MUST be recovered and version-verified; if none exists, the minimum required authority instrument must be created and locked rather than reconstructed by implementation.

**Acceptance:**

- **T-D:** proposed/effective configuration distinct.
- **T-I:** authorized change changes subsequent effective state.
- **T-X:** no self-elevation, role/scope widening or wrong-client import.
- **T-F:** partial/invalid change does not corrupt existing configuration.
- **T-L:** authorized configuration evolution with before/after audit.
- **T-R:** Stage 2 semantics remain unchanged.

## 38. AL-CFG-001 — Configuration / Policy / Effective State

**Purpose:** provide the common governed configuration machinery required by Agent Layer, including effective-state calculation, policy/authority evaluation, provenance, delegation/revocation, safe recomposition and explanation.

**Preconditions:**

- an identified configuration subject/scope and current effective configuration exist or a valid creation scope is authorized;
- the proposing/committing principal or governed service is authenticated and authorized for the affected configuration dimensions;
- applicable tenant/client/project, security, Data/IP, entitlement and permanent-invariant constraints are available;
- any authority-owned value required for the proposed setting is version-locked or the affected behavior remains disabled/fail-closed.

**Authoritative inputs:**

- proposed configuration change or effective-state query;
- current committed configuration/version and provenance;
- applicable principal, delegation and authority records;
- client/Flo/project/agent/capability scope;
- applicable policy, security, entitlement, autonomy, distribution and learning constraints;
- dependency/composition relationships required to calculate effective state.

**Permission / boundary:**

- a principal may configure only dimensions and scope independently authorized to that principal;
- delegation cannot create authority absent from the delegating/effective authority chain;
- recomposition MUST NOT bypass tenant isolation, Security/Data/IP policy, permanent invariants, entitlement ceilings or action/autonomy ceilings;
- configuration MUST NOT redefine or replace Stage 2 operational authority.

**Expected structured output / state:**

For an effective-state query: a structured effective configuration with contributing configuration/policy versions and sufficient explanation/provenance to identify why each material constraint applies.

For a proposed change: one of **VALIDATED-PROPOSAL**, **COMMITTED**, **REJECTED**, **CONFLICT/REVALIDATION REQUIRED** or **AUTHORITY VALUE REQUIRED**, with affected scope, applicable constraints and resulting version/state where committed.

A committed change MUST create a versioned authoritative configuration state and preserve prior provenance/history required for audit and rollback/review.

**Side effects / authoritative capability:**

- read/effective-state evaluation is S0/S1;
- committed configuration change is a governed configuration mutation through the registered authoritative configuration capability;
- any underlying security/privilege/autonomy/high-risk effect retains the higher side-effect/risk class required by Parts I/Section 27; configuration MUST NOT downgrade it;
- no arbitrary direct DB mutation is permitted.

**Replay / idempotency / concurrency:**

- replay of the same already-committed change MUST NOT duplicate grants, objects, delegations or policy effects;
- a stale proposal MUST be revalidated against current effective state before commitment;
- concurrent changes MUST NOT silently compose where their combination is conflicting, ambiguous, security-relevant or authority-widening;
- revocation/deactivation MUST affect future eligibility and active consequential work MUST revalidate before further execution.

**Audit evidence:** proposer/requester, authorizing principal, affected scope, before/after version, provenance, effective constraints, decision, authority basis, timestamp and resulting state.

**Failure / fallback:** invalid, unauthorized, stale-conflicting, unresolvable or policy-incomplete changes MUST fail without altering effective committed configuration. Missing authority-owned values produce **AUTHORITY VALUE REQUIRED** or disabled/fail-closed behavior, not an invented value.

**No-agent behavior:** committed configuration and deterministic effective-state/policy evaluation MUST remain available without model reasoning. Agent-assisted discovery/explanation may be unavailable, but effective authority and enforcement MUST NOT disappear or widen.

**Acceptance / exact expected results:**

- **T-D:** representative configuration compositions calculate the expected deterministic effective constraints and provenance.
- **T-I:** an authorized versioned change becomes the subsequent effective state; repeating the same committed change creates no duplicate grant/object/effect.
- **T-X:** attempted recomposition that would bypass tenant, security, Data/IP, entitlement or execution ceilings is **REJECTED** with no effective-state widening.
- **T-F:** stale/concurrent conflicting change returns **CONFLICT/REVALIDATION REQUIRED** or fails closed; existing committed state remains coherent. Revocation blocks future eligibility and causes active consequential work to revalidate.
- **T-L:** one authorized live effective-configuration change is visible through introspection and subsequent Agent-Layer behavior with before/after audit.
- **T-R:** Stage 2 operational semantics and authority remain unchanged.

**Authority/configuration later:** actual client/Flo topology, delegation, role matrices, autonomy, recipient and entitlement values.

## 39. AL-PA-001 — Manager PA

**Purpose:** provide practical, teaching and contextual assistance to an authorized user/manager without creating a privileged super-role.

**Preconditions:**

- authenticated authorized user/manager context;
- PA capability is enabled/entitled where required;
- current user/client/project access and enabled-capability state can be resolved;
- any persistent individual-user adaptation is permitted by the applicable learning/retention policy.

**Authoritative inputs:**

- the user's request;
- current authorized operational/context state;
- enabled capability/function registry;
- current configuration/policy;
- permitted user-preference/adaptation context;
- evidence references needed to explain current operational state or function availability.

**Permission / boundary:**

- PA information visibility is no broader than the user's independently authorized/configured scope and applicable confidentiality/security/Data-IP constraints;
- preference/adaptation is not authority, approval or business fact;
- PA MUST NOT expose another user's/client's private adaptation or operational context;
- PA MAY prepare a consequential proposal, but execution requires the separately governed capability and its current authority/autonomy/approval gate.

**Expected structured output / state:**

One or more of: evidence-grounded explanation, contextual help, organized/prioritized authorized work view, recommendation, draft, or structured proposal for a separately governed capability.

Any persisted individual-user preference/adaptation state MUST be explicitly governed, scoped to the permitted user/client context, provenance-linked and distinct from operational truth/authority.

**Side effects / authoritative capability:**

- ordinary help/explanation/recommendation/drafting is S0/S1;
- optional persistence of permitted personal adaptation is governed internal state under the applicable learning/configuration capability;
- consequential operational/external action is not a PA side effect merely because the PA proposed it and MUST execute only through the separate authoritative capability.

**Replay / idempotency / concurrency:**

- repeated advisory/help requests do not mutate operational state;
- replay of any separately invoked consequential capability inherits that capability's idempotency/exactly-once rules;
- before a consequential proposal progresses, current user authority, enabled capability and material operational state MUST be revalidated if they may have changed during the PA interaction.

**Audit evidence:** user/principal, client/project scope, PA/capability version, evidence/configuration basis, any governed preference use/update, proposed tool/action and actual separately governed execution result where invoked.

**Failure / fallback:** on unavailable reasoning, ambiguous/unauthorized scope, unavailable functionality or prohibited preference data, PA MUST fail closed for protected information/action and MUST NOT invent system capability or authority.

**No-agent behavior:** PA assistance may be unavailable, but ordinary Stage 2/deterministic HUBFLO operation and deterministic function/permission enforcement MUST continue where applicable.

**Acceptance / exact expected results:**

- **T-D:** help/recommendation output lists or uses only actually enabled and authorized capabilities/state.
- **T-I:** PA explains or assists with current authoritative state/configuration and any structured action proposal remains separate from execution authority.
- **T-X:** cross-user/client/project request and prohibited individual-learning access are denied; no protected data is disclosed.
- **T-F:** PA/provider failure causes unavailable/degraded assistance only; operational state and user authority do not widen or corrupt.
- **T-L:** an authorized practical assistance/help interaction is grounded in real enabled state and evidence; if it proposes an action, that action does not execute without its separate gate.
- **T-R:** PA remains optional to ordinary Stage 2 operations.

**Authority/configuration later:** launch-role access and individual-user retention/adaptation policy.

## 40. AL-PLAT-001 — Platform Guardian

**Purpose:** observe and diagnose platform/runtime health and support governed remediation progression.

**Inputs may include:** telemetry, logs, metrics, health, queues/jobs, providers, application/runtime, DB state, handler outcomes and delivery evidence.

**Outputs:** observation, diagnosis, evidence, confidence, affected scope, escalation and repair/remediation recommendation.

Default architecture supports:

**observe → diagnose → report/escalate → recommend repair → governed handoff**

Bounded remediation MAY later be registered and authorized.

Source modification, deployment, infrastructure/security mutation or other high-impact action is not implied by Guardian status.

**Acceptance:**

- **T-D:** representative faults localize correctly.
- **T-I:** failures across multiple layers distinguished.
- **T-F:** degraded/recovery behavior observable.
- **T-X:** diagnostics do not disclose unauthorized client data or engineering secrets.
- **T-L:** controlled live fault/degradation case.
- **T-R:** Guardian outage does not stop ordinary deterministic HUBFLO.

**Authority/configuration later:** notification/remediation thresholds and high-impact approval rules.

## 41. AL-CAP-001 — Capacity / Architecture Intelligence

Preserves Annex B 1.0.

May forecast and recommend capacity/resilience/cost/architecture changes.

Major infrastructure/provider/DB changes remain separately governed.

Acceptance requires deterministic calculation/provenance, historical comparison where relevant, live current metrics and prohibition of unapproved migration.

## 42. AL-CHAN-001 — Channel independence

Channels remain adapters to shared governed capabilities.

WhatsApp, email, SMS, voice, web/app and future integrations MUST NOT create duplicated authoritative business systems.

Identity resolves to authoritative principal/scope before consequential action.

Channel failure cannot corrupt underlying objective/business state.

Enabled channels and delivery policies remain configured values.

## 43. AL-OBJ-001 — Durable objective state

Implements Section 15.

Acceptance MUST prove:

- deterministic objective transitions;
- persistence across turns/restarts;
- authority revalidation;
- unauthorized resume denial;
- live multi-turn durable state;
- cancellation does not silently cancel business objects.

## 44. AL-CRIT-001 — Critical-path / dependency intelligence

**Purpose:** reason over Stage 2 operational evidence to identify dependencies, blockers, delay patterns and critical-path risk.

Must distinguish known operational facts from inference.

Inferred dependencies do not become Stage 2 truth merely because a model inferred them.

Persistent learned/refined critical-path intelligence MAY evolve under Sections 23–24.

Acceptance requires evidence-linked reasoning, replay/outcome comparison where appropriate, isolation and continued Stage 2 authority.

## 45. AL-CONS-001 — Consequence / intervention reasoning

Produces evidence-linked consequence, time horizon/severity where supported, confidence/inference state, candidate intervention, required authority and expected benefit/risk.

Intervention selection is constrained by registered capability and effective authority.

Analysis itself does not mutate business state.

## 46. AL-PERF-001 — Turnaround / performance intelligence

Uses evidence hierarchy and preserves source scope/age.

Reliable client-specific evidence may outweigh generic benchmark.

Client-private performance is not another client's private fact.

Historical prediction/outcome comparison SHOULD refine future performance intelligence.

## 47. AL-LEARN-001 — Outcome learning

Implements Section 23.

Acceptance MUST prove:

- durable prediction/action/outcome/version linkage;
- provider independence;
- correct learning scope;
- no silent client leakage;
- history remains usable after provider substitution;
- learning does not mutate business truth or authority.

## 48. AL-INTEL-001 — Persistent derived intelligence

**Purpose:** persist, govern, validate, refine, supersede and reuse HUBFLO-derived intelligence independently of transient provider sessions while keeping it distinct from authoritative Stage 2 operational state.

**Preconditions:**

- authorized source evidence/context exists for the deriving capability;
- applicable tenant/client/project/industry/platform learning and intelligence-use scope is resolved;
- applicable Security, Data & IP Protection Policy and retention/generalization rules permit the derivation/persistence;
- the intelligence type/status can be represented with provenance and derived-versus-authoritative distinction.

**Authoritative inputs:**

- scoped evidence/source references;
- prior related intelligence where authorized;
- prediction/finding/inference inputs;
- validation/outcome evidence when available;
- provider/model/capability/configuration versions where material;
- current intelligence-use, learning, security, entitlement and retention constraints.

**Permission / boundary:**

- permission to access source evidence does not automatically grant permission to distribute the derived intelligence;
- permission to hold/use derived intelligence does not grant authority over Stage 2 operational state;
- intelligence MUST retain client/project/industry/platform and confidentiality/Data-IP scope sufficient to prevent unauthorized reuse/generalization;
- cross-client use requires the separately governed aggregation/generalization path.

**Expected structured output / state:**

A durable intelligence record or versioned refinement containing, where applicable: intelligence identity/type; scope/ownership; source/provenance references; derived status; finding/prediction; confidence/inference classification; validation status; created/updated/version metadata; relation to prior intelligence; and **current**, **refined**, **superseded**, **invalidated** or equivalent governed lifecycle status.

Later authorized reasoning MUST be able to retrieve the current eligible intelligence together with source lineage and supersession/refinement history.

**Side effects / authoritative capability:**

- reasoning is S1;
- persistence/refinement/supersession of derived intelligence is governed internal state through the registered intelligence/learning persistence capability;
- it MUST NOT directly mutate authoritative Stage 2 business state;
- any later operational intervention remains a separate governed capability.

**Replay / idempotency / concurrency:**

- replay of the same derivation event/evidence MUST NOT create conflicting authoritative duplicates or erase lineage;
- a retry may resolve to the existing eligible record/version or a provenance-linked refinement according to deterministic identity/version rules;
- concurrent validation/refinement MUST preserve prior versions/evidence and MUST NOT silently lose a supersession/invalidating outcome;
- stale intelligence MUST be revalidated where current state materially affects eligibility/use.

**Audit evidence:** intelligence ID/version, source/evidence lineage, scope, deriving capability/provider/model versions where used, validation/outcome evidence, refinement/supersession relation, policy/configuration basis, retention/generalization state and actor/principal where applicable.

**Failure / fallback:** invalid structure, prohibited scope, missing required provenance or policy restriction prevents persistence/use. Provider failure MUST NOT destroy existing durable intelligence. Uncertain validation MUST remain unresolved/qualified rather than silently treated as established fact.

**No-agent behavior:** already persisted intelligence remains HUBFLO-owned and retrievable/useable only where deterministic policy permits. Model-dependent new derivation/refinement may pause or degrade explicitly; retention, revocation, isolation and authoritative Stage 2 operations continue deterministically.

**Acceptance / exact expected results:**

- **T-D:** created intelligence preserves required provenance, scope, derived status and version/lifecycle semantics.
- **T-I:** later authorized reasoning retrieves prior durable intelligence with source lineage; Stage 2 operational records remain unchanged by intelligence persistence.
- **T-S:** historical outcome/validation can refine, supersede or invalidate earlier intelligence while preserving the earlier record and lineage.
- **T-X:** private intelligence is denied outside its authorized tenant/client/project/Data-IP boundary; cross-client reuse cannot occur without the governed path.
- **T-F:** provider replacement/outage leaves existing intelligence intact and attributable; interrupted derivation cannot corrupt prior current state.
- **T-L:** live authorized operational evidence produces one persisted derived finding that is subsequently retrievable under the correct scope with provenance.
- **T-R:** derived intelligence remains distinct from authoritative Stage 2 operational state and cannot become business truth solely by model assertion.

## 49. AL-DIST-001 — Awareness / supervisory visibility / distribution

**Purpose:** independently govern whether intelligence may exist, be known to an agent, influence authorized internal reasoning, be visible to an authorized supervisory function, be distributable, and be received by a particular recipient.

**Preconditions:**

- referenced intelligence/evidence exists and its scope/classification/provenance are known enough for a distribution decision;
- current effective distribution/configuration policy is available;
- proposed agent/supervisor/recipient identity and client/project/Flo scope are resolved;
- applicable Security, Data & IP Protection Policy, confidentiality and entitlement constraints are in force.

**Authoritative inputs:**

- intelligence/evidence reference and current lifecycle/classification status;
- origin tenant/client/project/industry/platform scope;
- proposed awareness/use/supervisory/distribution action;
- target agent/supervisor/recipient and channel/output type where applicable;
- current authorization, confidentiality, security/Data-IP, entitlement, distribution and recipient configuration;
- novelty/established-governance status.

**Permission / boundary:**

- novelty creates **no access authority**;
- supervisory visibility requires independently authorized information access and remains subject to tenant/client/project, confidentiality, security-domain and Security/Data-IP constraints;
- agent awareness does not grant recipient access;
- internal reasoning permission does not grant distribution permission;
- entitlement cannot override security/authorization;
- recipient rights are evaluated independently for the specific intelligence/output and current scope.

**Expected structured output / state:**

A governed decision such as **ALLOW-AWARENESS**, **ALLOW-INTERNAL-USE**, **SUPERVISORY-REVIEW-ELIGIBLE**, **ALLOW-DISTRIBUTION**, **DENY**, or **HOLD/QUARANTINE**, with intelligence reference, target, scope, policy/configuration basis, recipient entitlement/authorization state and delivery state where distribution occurs.

For novel/unclassified intelligence, the default is eligibility for authorized HUBFLO supervisory review first; if no authorized supervisor may access it, the result MUST be **HOLD/QUARANTINE** or equivalent permitted non-distribution state, not widened access.

**Side effects / authoritative capability:**

- awareness/use eligibility evaluation is S0/S1;
- changing governed distribution state/configuration is a governed internal mutation;
- actual external/recipient delivery is an S3 action through an authorized channel/distribution capability and inherits that capability's delivery/idempotency rules;
- no distribution decision alters Stage 2 operational truth.

**Replay / idempotency / concurrency:**

- repeated evaluation without changed state does not create additional authority;
- replay of a governed delivery intent MUST NOT duplicate delivery where the distribution capability defines exactly-once/idempotent delivery semantics;
- current recipient authorization/entitlement/distribution policy MUST be revalidated before delivery;
- concurrent revocation and delivery eligibility MUST fail toward denial/hold unless the delivery was already authoritatively completed under valid prior state.

**Audit evidence:** intelligence reference/version, origin scope, target agent/supervisor/recipient, policy/configuration/entitlement versions, allow/deny/hold basis, novelty/established status, channel/delivery reference, result and revocation state where relevant.

**Failure / fallback:** missing/ambiguous recipient authority, classification, confidentiality or policy MUST deny/hold rather than distribute. Distribution/provider/channel failure MUST preserve the intelligence and delivery state without treating transport success/failure as an operational truth change.

**No-agent behavior:** deterministic distribution of already established intelligence MAY continue only where its configured distribution decision requires no model judgment and all current permissions remain valid. Novel/unclassified intelligence requiring supervisory classification/review MUST remain held or review-eligible; it MUST NOT auto-distribute because reasoning is unavailable.

**Acceptance / exact expected results:**

- **T-D:** awareness, internal use, supervisory visibility, distribution and recipient eligibility are independently evaluated and can produce different results for the same intelligence.
- **T-I:** established governed intelligence follows current effective distribution configuration and records the correct recipient/delivery decision.
- **T-X:** (a) an agent allowed to know/use intelligence cannot leak it to an unauthorized recipient; (b) a novel finding does not become visible to a supervisor lacking independent tenant/confidentiality/security/Data-IP access; (c) entitlement alone cannot override denial.
- **T-F:** recipient/distribution revocation blocks future delivery and forces current eligibility revalidation without erasing required history; provider/channel failure does not broaden recipients.
- **T-L:** where an authorized output channel and recipient exist, one governed intelligence delivery follows current recipient configuration and produces auditable delivery state.
- **T-R:** distribution logic does not alter Stage 2 authority or operational state.

**Authority/configuration later:** actual recipients, proactive/requested rules, frequencies and material novel→established governance criteria.

## 50. AL-ENT-001 — Entitlement / commercial controls

**Purpose:** provide configurable product/commercial carveability across capability, information/intelligence products, output, recipient, frequency, reasoning, agent and automation dimensions without changing underlying operational/security authority.

**Preconditions:**

- an entitlement subject (client/Flo/user/product scope) and requested feature/use are identified;
- current versioned entitlement configuration is available;
- applicable operational authorization, tenant/security/Data-IP and capability health/policy remain independently evaluable;
- any commercial/legal value required for an actual package/limit is authority-defined rather than inferred by implementation.

**Authoritative inputs:**

- subject/client/Flo/user scope;
- requested capability/intelligence/output/recipient/frequency/reasoning/agent/automation dimension;
- current entitlement assignment/package/limits where defined;
- current authorization/security/Data-IP/distribution constraints;
- current configuration version and revocation state.

**Permission / boundary:**

- entitlement is eligibility/commercial availability, not operational permission;
- an entitlement grant MUST NOT create client/project data access, role authority, execution permission, autonomy or distribution rights absent their separate governing authority;
- entitlement evaluation MUST remain tenant-scoped and cannot expose another client's commercial configuration except where independently authorized;
- commercial configuration MUST NOT redefine Stage 2 operational semantics.

**Expected structured output / state:**

For evaluation: **ENTITLED**, **NOT-ENTITLED**, **LIMIT/CONDITION NOT MET**, **REVALIDATION REQUIRED** or **AUTHORITY VALUE REQUIRED**, with dimension, subject/scope, version and governing entitlement basis.

For an authorized entitlement change: a versioned assignment/revocation/limit state with before/after provenance sufficient for subsequent deterministic eligibility evaluation.

**Side effects / authoritative capability:**

- entitlement checks are S0;
- assignment/revocation/configuration is a governed internal configuration/commercial-control mutation through the registered entitlement/configuration capability;
- any feature subsequently exercised retains its own independent side-effect/risk/approval class;
- entitlement MUST NOT be used as a shortcut around capability/authorization gates.

**Replay / idempotency / concurrency:**

- repeating the same entitlement assignment/revocation MUST NOT duplicate seats/grants/limits or create broader eligibility;
- stale entitlement state MUST be revalidated before consequential use;
- concurrent grant/revocation changes MUST not silently widen eligibility; ambiguous current state fails toward **NOT-ENTITLED/REVALIDATION REQUIRED**;
- revocation MUST force future capability/intelligence eligibility revalidation and active work MUST revalidate before further gated use where entitlement is material.

**Audit evidence:** entitlement subject/scope, dimension, before/after state/version, proposer/authorizer where applicable, commercial/configuration basis, decision, revocation and downstream eligibility reference where material.

**Failure / fallback:** missing, invalid, ambiguous or authority-incomplete entitlement data MUST fail closed for the gated commercial capability/product. It MUST NOT grant operational authority as a fallback.

**No-agent behavior:** entitlement evaluation and enforcement MUST be deterministic and available without model reasoning. AI/provider failure MUST NOT grant, remove or reinterpret entitlement by itself.

**Acceptance / exact expected results:**

- **T-D:** at least two entitlement dimensions vary independently and produce the expected deterministic eligibility decisions.
- **T-I:** authorized entitlement change alters capability/intelligence availability while underlying security, role and operational authorization remain unchanged.
- **T-X:** an entitled subject lacking operational/data/distribution authorization remains denied that protected action/information; cross-client entitlement inspection is blocked.
- **T-F:** revocation causes subsequent use to return **NOT-ENTITLED/REVALIDATION REQUIRED** and active gated work revalidates safely; ambiguous concurrent state never widens eligibility.
- **T-L:** where a launch-scope entitlement-controlled feature is enabled, one authorized live entitlement change or evaluation is reflected in subsequent availability with audit evidence. If no launch-scope feature is entitlement-controlled, this T-L case is explicitly excluded rather than falsely counted.
- **T-R:** Stage 2 remains operationally authoritative and commercially neutral.

**Commercial/legal later:** packages, prices, billing terms, premium-product definitions and derived-IP commercial rights.

## 51. AL-AUTO-001 — Autonomy Policy

Implements Sections 12–14.

Acceptance includes:

- Level 2/3/4 distinction;
- shadow mode;
- degradation;
- self-promotion denial;
- kill/disable behavior;
- governed live approval/bounded-action case when authorized.

## 52. AL-TOOL-001 — Governed capability contracts

Implements Sections 10–12.

Any arbitrary Agent-Layer DB mutation outside an authorized handler/capability is automatic FAIL.

## 53. AL-GRAPH-001 — Durable/scalable execution

Implements Sections 15–17.

The implementation MAY use simple, graph, hierarchical, specialist or distributed execution as source-aware design warrants.

Acceptance MUST prove bounded execution, dependency handling, checkpoint/resume, failure localization, structured handoff and authority safety.

No specific graph technology or representation is contractual.

## 54. AL-VERIFY-001 — Independent verification

Implements Section 28.

Mandatory verifier unavailability follows the applicable STOP/ESCALATE rule rather than silent bypass.

## 55. AL-PROV-001 — Provider neutrality / fallback

Implements Section 18.

Acceptance MUST demonstrate provider abstraction, structured validation, safe substitution/failure, isolation and retention of HUBFLO-owned operational/learning/intelligence state.

## 56. AL-NOAI-001 — No-agent deterministic fallback

Implements Section 19.

Making an AI provider mandatory for ordinary accepted deterministic operation without separate product authority is FAIL.

## 57. AL-MARKET-001 — Market / Technology Intelligence

External evidence MUST be attributable/freshness-aware where material.

External content cannot grant tool authority or alter policy.

Architecture/provider recommendations do not themselves authorize migration.

## 58. AL-HELP-001 — Contextual help / function discovery

Help MUST use actual enabled state, current policy and current scope.

It cannot disclose protected state or grant permission.

Manager PA MAY consume this capability.

## 59. AL-SEC-001 — Security Observability

Implements Sections 8 and 26.

Acceptance requires privilege/cross-tenant/suspicious-tool negative testing, failure visibility, controlled live security finding and separation from ordinary platform-health diagnosis.

---

# PART III — INTEGRATED ACCEPTANCE

## 60. Required cross-capability suites

**B-SUITE-001 — Tenant / authority / information isolation**

- multiple clients/projects;
- denied role/capability;
- retrieval/tool/provider/cache/learning/intelligence/handoff/distribution negatives;
- audit visibility isolation.

**Evidence:** T-D, T-I, T-X, T-R.

**B-SUITE-002 — Exactly-once consequential execution**

- normal execution;
- duplicate;
- replay;
- concurrent claim;
- uncertain transport;
- restart.

At most one authoritative mutation.

**Evidence:** T-D, T-I, T-F, T-R and appropriate T-L.

**B-SUITE-003 — Configuration / effective-state / recomposition**

- proposed versus committed state;
- effective-config calculation;
- policy versioning;
- delegation;
- revocation;
- topology change;
- entitlement change;
- no safety/authority bypass under recomposition;
- explanation/introspection.

**Evidence:** T-D, T-I, T-X, T-F, T-R and applicable T-L.

**B-SUITE-004 — Learning and persistent intelligence**

- four learning scopes;
- client privacy;
- Industry Flo language learning;
- prediction/outcome linkage;
- validation/refinement/supersession;
- provider substitution;
- no promotion-created authority.

**Evidence:** T-D, T-I, T-S, T-X, T-F, T-R and applicable T-L.

**B-SUITE-005 — Awareness / distribution / entitlement**

- agent aware but recipient denied;
- internal supervisory access separate from client distribution;
- novel-intelligence supervisory default;
- established configured distribution;
- entitlement removal;
- capability entitlement versus information entitlement.

**Evidence:** T-D, T-I, T-X, T-F, T-R and applicable T-L.

**B-SUITE-006 — Autonomy / approval / disable**

- Level 2 recommendation;
- Level 3 approval;
- authorized Level 4 if in scope;
- out-of-bounds denial;
- degradation;
- capability/client/global disable;
- attempted self-promotion.

**Evidence:** T-D, T-I, T-S, T-F, T-X and applicable T-L.

**B-SUITE-007 — Provider / no-agent continuity**

- provider success;
- invalid structured result;
- timeout;
- permitted fallback;
- no fallback;
- deterministic continuity;
- judgment-required unavailable behavior;
- provider substitution without state/learning/intelligence loss.

**B-SUITE-008 — Durable objective / scalable execution**

- simple path;
- multi-step dependency path;
- representative independent fan-out + governed fan-in satisfying **B-COMP-X001** where scalable composition/fan-out-fan-in is claimed; otherwise the acceptance manifest MUST explicitly exclude fan-out/fan-in scalable composition from accepted scope;
- checkpoint/restart;
- local failure/recovery;
- authority revalidation;
- structured artifact handoff;
- no duplicate side effect.

**B-SUITE-009 — Evidence / audit integrity**
Must prove material behavior records sufficient principal, scope, configuration, capability, provider, evidence, approval, claim, outcome, distribution/entitlement and override state.

**B-SUITE-010 — High-risk governance**
R4/S4 cannot execute without required authority/verification and cannot self-approve.

**B-SUITE-011 — Security domains**
Must demonstrate:

- runtime agents cannot obtain engineering/source authority merely through runtime access;
- credentials do not enter model/learning context;
- learned intelligence cannot silently recover prohibited raw client/source/secret material;
- domain permissions remain least-privilege;
- recomposition does not collapse domains.

**B-SUITE-012 — Stage 2 preservation**
Must prove Agent Layer does not:

- replace Shared Conversation Engine;
- bypass authentication/authorization;
- duplicate business handlers;
- change Stage 2 lifecycle semantics;
- bypass exactly-once safeguards;
- require agents for ordinary deterministic operation.

The full accepted Stage 2 regression suite remains mandatory regression evidence.

## 61. Live operational acceptance doctrine

Live claims require real authorized runtime use of the material component being accepted.

A live case records:

- case ID;
- principal/scope;
- relevant configuration/policy version;
- runtime capability/agent version;
- provider/model/version where applicable;
- input/event;
- evidence/audit;
- authoritative or governed derived outcome;
- expected/actual result;
- PASS/FAIL;
- cleanup/reversal where relevant.

Transport success alone is insufficient.

Unsafe destructive/legal/high-risk actions need not be created artificially for testing where controlled negative/verification evidence can safely prove the gate.

## 62. Minimum live acceptance coverage

Final Agent Layer completion scope MUST include appropriate live or explicitly governed controlled evidence for enabled claimed capabilities, including at minimum where in claimed production scope:

1. Hub Manager evidence-grounded supervisory query.
2. Multi-client isolation.
3. Effective configuration/Take-on evolution.
4. Configuration introspection.
5. Manager PA/help interaction.
6. Guardian operational diagnosis.
7. Current capacity assessment.
8. Shared channel/capability path.
9. Durable multi-turn objective.
10. Critical-path/dependency intelligence.
11. Consequence/intervention reasoning.
12. Performance intelligence.
13. Learning outcome capture.
14. Persisted derived intelligence reused later.
15. Novel-intelligence supervisory treatment.
16. Governed established intelligence distribution where enabled.
17. Entitlement enforcement.
18. Autonomy/approval case.
19. S3 governed tool execution if enabled.
20. Durable checkpoint/resume.
21. Independent verifier case where required.
22. Provider attribution plus controlled failure/fallback.
23. Deterministic no-agent continuity.
24. Market/Technology Intelligence where enabled.
25. Security-observability event.
26. Full Stage 2 regression.

A capability deliberately excluded from launch scope cannot be counted as accepted production functionality merely because code exists.

## 63. Deferred authority / policy register

Each binding deferred value requires:

- approving authority;
- version;
- effective date;
- scope;
- value;
- dependent capability;
- acceptance/regression linkage.

Deferred families include:

**AB-AUTH-001 — Role/capability matrix**
Exact organizational/principal capability permissions.

**AB-AUTH-002 — Initial autonomy**
Initial levels and capabilities eligible for later Levels 4–5.

**AB-AUTH-003 — Promotion/degradation thresholds**
Reliability/performance/override/failure thresholds.

**AB-AUTH-004 — Materiality thresholds**
Monetary/date/payment/destructive/high-impact classifications.

**AB-AUTH-005 — Approval/escalation timing**
Where not already governed by authoritative business rules.

**AB-AUTH-006 — Reasoning/retry budgets**

**AB-AUTH-007 — Provider policy**

**AB-AUTH-008 — Learning retention/generalization**

- retention;
- de-identification;
- opt-in/withdrawal;
- cohort rules;
- promotion/generalization.

**AB-AUTH-009 — Prediction/diagnostic acceptance thresholds**

**AB-AUTH-010 — Platform SLO/recovery values**

**AB-AUTH-011 — Security policy values**

**AB-AUTH-012 — Market-intelligence policy**

**AB-AUTH-013 — Objective/execution/intelligence retention**

**AB-AUTH-014 — Additional mandatory verification classes**

**AB-AUTH-015 — Distribution policy**
Actual proactive/requested outputs, recipients, frequencies and material novel→established governance criteria.

**AB-AUTH-016 — Entitlement/commercial configuration**
Actual packages, limits, entitlement assignments and billing rules.

**AB-AUTH-017 — Take-on authority instrument**
Canonical detailed Take-on/hierarchy/configuration authority not already recovered.

**AB-AUTH-018 — Individual-user adaptation**
Persistent personal preference/learning retention, use, correction and promotion rules.

**AB-AUTH-019 — Industry/sector/Flo mapping**
Actual industry/sector/subsector configuration and permitted cross-scope intelligence reuse.

An implementation agent MAY implement safe configurable/fail-closed support. It MUST NOT choose the binding value.

## 64. Security/Data/IP policy gate

The applicable **HUBFLO Security, Data & IP Protection Policy MUST be version-locked before any Agent Layer launch-scope implementation begins.**

This is a mandatory pre-implementation gate for the launch scope as a whole, not only for individual capabilities that appear to depend on an unresolved policy value.

The version-locked policy must govern at minimum:

- source/IP exposure;
- runtime-model data minimization;
- secrets/credentials;
- provider retention/training restrictions;
- tenant isolation;
- learned/derived-state boundaries;
- cross-scope aggregation;
- production access;
- security observability;
- incident response;
- backup/recovery;
- provider substitution equivalence;
- applicable retention/deletion/withdrawal;
- rights in source versus derived intelligence where legally/commercially material.

**Pre-implementation expected result:** before the first authorized implementation change for the Agent Layer launch scope, the implementation/acceptance record identifies the applicable Security, Data & IP Protection Policy version and that version is locked by HUBFLO authority. If it is absent, unversioned or not applicable to the claimed launch scope, the result is **STOP — PRE-IMPLEMENTATION SECURITY/DATA/IP POLICY GATE NOT SATISFIED**.

An implementation agent MUST NOT supply, infer or relax missing binding policy values in order to pass this gate.

## 65. Implementation-readiness rule

A capability or coherent implementation boundary is ready only when:

- its required architectural/behavioral contract exists;
- required authority/tool boundaries exist;
- applicable security invariants are defined;
- binding values necessary to exercise claimed behavior are supplied, or the behavior can remain explicitly disabled/fail-closed;
- acceptance can be tested without inventing an oracle.

## 66. Agent Layer completion gate

Required sequence:

**Stage 2 complete → Annex B final version lock → applicable Security, Data & IP Protection Policy version-lock + remaining launch-scope authority gates → authorized implementation → internally green deterministic/integration/security/failure/shadow evidence → independent RTW review → controlled deployment → required live evidence → independent final acceptance**

Only the independent acceptance authority may issue:

**PASS — AGENT LAYER 2.0 COMPLETE**

No implementing engineering agent may issue its own final PASS.

## 67. Explicit implementation prohibitions

A future implementation agent MUST NOT:

1. reopen or alter accepted Stage 2 to simplify Agent Layer;
2. replace authoritative handlers with model logic;
3. create parallel operational truth;
4. invent role, permission, autonomy, tenancy, security, retention, commercial or legal values;
5. create a second Core business-recognition architecture;
6. treat provider/model memory as HUBFLO durable state;
7. mutate authoritative operational state outside governed capability paths;
8. self-expand privilege/autonomy;
9. collapse client confidentiality through prompts, retrieval, caches, embeddings, traces, learning or intelligence;
10. allow configuration/recomposition to bypass permanent security invariants;
11. treat confidence as evidence;
12. treat benchmark/inference as client fact;
13. retry consequential work without outcome/idempotency inspection;
14. let provider substitution broaden scope/authority;
15. create unbounded runtime reasoning loops;
16. bypass required approval, verification or disable controls;
17. relabel simulated evidence as live;
18. make ordinary deterministic HUBFLO operation unnecessarily agent-dependent;
19. hard-code today's client/Flo/recipient/commercial topology as permanent architecture;
20. infer recipient rights from agent awareness;
21. infer execution authority from learning, entitlement or model quality;
22. self-approve or self-deploy.

## 68. Acceptance manifest

A candidate later submitted for Agent-Layer acceptance MUST identify:

- Annex B version;
- repository/build identifier;
- enabled capability families;
- effective configuration/policy version;
- capability registry/export;
- provider/model versions;
- authority register version;
- security-policy version and pre-implementation gate evidence;
- scalable-composition/fan-out-fan-in claim or explicit exclusion;
- schema/storage migrations;
- configuration migrations;
- test/evidence results by class;
- live cases;
- open failures/waivers;
- disabled/deferred capabilities;
- Stage 2 regression;
- security/isolation evidence;
- intelligence/learning isolation evidence;
- verifier evidence;
- deployment environment;
- rollback/recovery plan;
- independent final decision.

No disabled or omitted capability may silently count toward completion.

## 69. Contract completeness statement

Annex B 1.1 is the final authority-locked architectural and behavioral acceptance contract required by the approved O3 Agent Layer architecture.

It deliberately leaves technical realization to later source-aware engineering and leaves unresolved policy, authority/configuration, security, legal and commercial values explicitly deferred where architecture does not fix them.

It creates **no implementation authority**.

It is the final authority-locked Annex B 1.1 contract, but it creates **no Agent Layer implementation authority**. Implementation remains blocked pending the launch-scope Security, Data & IP Protection Policy version lock, required launch-scope authority/configuration values, and the canonical Take-on authority instrument where applicable.

---

**2. Reconciliation history**


**RC3 correction history from immediate parent 1.1-RC2:**

- **Section 10 — general capability/tool contract:** every material capability must now define its capability-specific concurrency/claim behavior and exact competing-attempt result; uncertain-outcome/retry handling, including authoritative outcome/idempotency inspection before any consequential retry; and explicit no-agent/degraded behavior, including the exact fallback, pending, escalation or stop result applicable when agent reasoning or a required dependency is unavailable. These obligations remain contract-level and implementation-neutral; they do not prescribe transaction, locking, queue, graph, orchestration, class, schema or API mechanics.

**Preserved RC2 correction history from authoring parent 1.1-RC1:**

- CE2.0 dependency wording is now explicit: **Industry Module → Core; Core may use the generic Industry contract but MUST NOT depend on a concrete Industry Module.**
- `AL-CFG-001`, `AL-PA-001`, `AL-INTEL-001`, `AL-DIST-001` and `AL-ENT-001` now carry acceptance-grade preconditions, authoritative inputs, permissions/boundaries, expected output/state, side effects/authoritative capability, replay/idempotency/concurrency, audit, failure/fallback, no-agent behavior and exact expected results.
- novel-intelligence supervisory visibility now creates no access authority and remains subordinate to tenant, confidentiality, security and Security/Data/IP constraints.
- scalable composition claims now require representative independent fan-out plus governed fan-in acceptance; otherwise fan-out/fan-in must be explicitly excluded from accepted scope.
- the applicable Security, Data & IP Protection Policy version-lock is now a mandatory pre-implementation gate for the Agent Layer launch scope.

- **Stage 2 composition made explicit:** Annex B now states directly that Agent Layer intelligence composes over accepted Stage 2 operational authority rather than merely coexisting with authoritative handlers.
- **First-class configuration capability added:** `AL-CFG-001` plus `B-CFG-*` makes configuration/effective-state calculation, provenance, introspection, delegation/revocation and safe recomposition contractual. This implements O3's requirement that configuration be structural rather than bespoke Flo logic.
- **Platform sovereignty clarified:** configurable delegation/recomposition never removes HUBFLO's ultimate deterministic control.
- **Manager PA formalized:** `AL-PA-001` adds practical assistance, teaching and bounded user adaptation without privileged-role escalation, as preserved by O3 continuity.
- **Industry Flo expanded:** explicit industry-language learning plus configurable industry/sector/Flo boundaries; no cross-scope learning inferred by implementation.
- **Take-on expanded:** `AL-TAKEON-001` now distinguishes Industry Take-on, Client Take-on and Ongoing Client Evolution; the existing recovered hierarchy constraints are preserved; unrecovered canonical Take-on authority is explicitly gated rather than reconstructed.
- **Security domains added:** source/engineering assets, credentials/secrets, runtime/client data and learned/derived intelligence now have explicit separate trust domains.
- **Learning model expanded:** platform / industry / client / individual-user scopes are explicit; promotion/generalization remains governed rather than prematurely fixed.
- **Persistent intelligence added:** `AL-INTEL-001` makes findings, predictions, provenance, validation, anomalies, critical-path/dependency intelligence and supersession/refinement durable HUBFLO-owned state.
- **Awareness/distribution separated:** `AL-DIST-001` separates intelligence existence, agent awareness, internal use, supervisory visibility, distribution and recipient access.
- **Novel-intelligence rule added:** new/unclassified intelligence defaults to HUBFLO supervisory visibility; deliberately established intelligence follows configured use/distribution.
- **Cross-agent contract reframed:** authority non-escalation remains fixed, but information visibility, internal reasoning scope and execution authority are no longer assumed to have identical topology.
- **Guardian ceiling corrected:** recommendation-only is no longer treated as a permanent architectural ceiling; bounded future remediation is architecturally allowed only through separately governed capability/authority.
- **Scalable execution generalized:** Annex B retains durable graph semantics while expressly allowing simple, hierarchical, recursive, specialist and distributed compositions without making a particular graph shape mandatory.
- **Entitlement/commercial carveability added:** `AL-ENT-001` separates capability, data, intelligence, output, recipient, frequency, reasoning, agent and automation entitlement from operational permission. Current packages/pricing remain unchosen.
- **Integrated acceptance expanded:** new suites test configuration/recomposition, persistent intelligence, distribution/entitlement and security-domain separation.
- **Completeness/status corrected:** Annex B 1.0's implementation-final completeness claim is superseded for reconciliation purposes. `1.1-RC2` is expressly a candidate requiring independent RTW7 review, as required by the current O3 instruction.

**3. Deferred decisions / gates**

- **SECURITY POLICY GATE:** formal Security, Data & IP Protection Policy, including source/IP exposure, provider handling, secrets, retention/deletion, learned-state handling, incident response and security equivalence.
- **AUTHORITY / CONFIGURATION — LATER:** exact role→capability permission matrix.
- **AUTHORITY / CONFIGURATION — LATER:** initial autonomy configuration and capability-specific promotion/degradation values.
- **AUTHORITY / CONFIGURATION — LATER:** exact client/Flo/project/agent topology, delegation assignments and recipient mappings.
- **AUTHORITY / CONFIGURATION — LATER:** canonical detailed Take-on authority instrument where no previously authoritative artifact can be recovered.
- **AUTHORITY / CONFIGURATION — LATER:** exact industry/sector/Flo memberships and permitted reuse relationships.
- **POLICY / PRODUCT — LATER:** individual-user learning retention/use/promotion and broader learning generalization rules.
- **POLICY / PRODUCT — LATER:** exact novel→established intelligence governance criteria and proactive/requested output policy.
- **AUTHORITY / CONFIGURATION — LATER:** provider allowlists, routing, budgets, reasoning intensity/frequency and SLO/recovery values.
- **AUTHORITY / CONFIGURATION — LATER:** prediction/diagnostic/intervention thresholds required for higher autonomy.
- **COMMERCIAL / LEGAL — LATER:** packages, prices, premium intelligence definitions, payment terms and commercial allocation of capability/intelligence.
- **COMMERCIAL / LEGAL — LATER:** client/HUBFLO rights, retention and permitted commercial reuse of source versus derived intelligence where applicable.
- **CODEX IMPLEMENTATION DECISION:** concrete persistence model, schemas, classes/modules, APIs, queues, graph representation, orchestrator, caches/indexes, concurrency/transaction design, provider adapters and deployment topology.

None of these is an architectural defect merely because its eventual value is not yet selected. O3 specifically directs the contract to defer such values rather than invent them.

**4. Implementation-freedom statement**

**Contractually fixed:**

- Stage 2 operational sovereignty and Core/Industry separation;
- no parallel authoritative business architecture;
- platform sovereignty and deterministic execution gating;
- configuration/policy as first-class generic machinery;
- safe effective-constraint evaluation under recomposition;
- tenant/security-domain separation and least privilege;
- capability + configured authority as execution ceiling;
- durable HUBFLO-owned objectives, execution state, learning and derived intelligence;
- four learning scopes;
- provenance/refinement/supersession requirements;
- awareness/use/supervision/distribution/recipient separation;
- novel-intelligence supervisory default;
- governed delegation/revocation;
- provider neutrality;
- scalable simple/graph/hierarchical/specialist/distributed execution capability;
- entitlement/commercial carveability;
- independent verification/evidence/acceptance.

**Left to later source-aware Codex design:**

- classes/modules;
- database tables/columns/schema structure;
- APIs/routes;
- queues/jobs technology;
- concrete graph representation;
- orchestration engine;
- persistence/index/cache strategy;
- transaction/locking/concurrency mechanics;
- provider-adapter design;
- background scheduling;
- deployment topology;
- internal code boundaries;
- performance optimizations.

Those implementation freedoms are expressly preserved by the reconciliation instruction.

**5. Conflict report**

**NONE**

No blocking conflict was found between the approved O3 architecture and HES1.0/CE2.0/closed Stage 2.

During the RC1 → RC2 → RC3 reconciliation, Annex B 1.0's statement that it was already implementation-complete was superseded by the later controlling O3 instruction establishing Annex B 1.0 as the reconciliation base pending independent contract review. RC3 subsequently received `PASS — ANNEX B 1.1-RC3 CONTRACT ACCEPTED FOR NEXT AUTHORITY GATE`, and Annex B 1.1 is now version-locked as the final authority artifact. This remains a lineage/status reconciliation, not an architectural conflict, and does not authorize Agent Layer implementation.

**ANNEX B 1.1 — FINAL AUTHORITY ARTIFACT — VERSION LOCKED — AGENT LAYER IMPLEMENTATION NOT AUTHORIZED**