# HUBFLO SECURITY, DATA & IP PROTECTION POLICY 1.0

**Document:** HUBFLO Security, Data & IP Protection Policy  
**Version:** 1.0  
**Status:** **FINAL / AUTHORITY-LOCKED SECURITY, DATA & IP PROTECTION POLICY — AGENT LAYER IMPLEMENTATION NOT AUTHORIZED**  
**Scope:** HUBFLO Agent Layer launch scope  
**Controlling authority:** HES1.0; CE2.0; closed Stage 2; Package 1.2; `HUBFLO_AGENT_LAYER_2.0_ANNEX_B_1.1.md`  
**Stage 2 effect:** None  
**Implementation authority:** None  
**Version-lock basis:** `PASS — SECURITY, DATA & IP PROTECTION POLICY 1.0-RC1 ACCEPTED FOR NEXT AUTHORITY GATE`  
**Substantive change from accepted RC1:** None  
**Remaining implementation gates:** required launch-scope authority/configuration values for enabled behavior; canonical Take-on authority instrument where applicable; any deferred security value required by the first claimed implementation scope.  
**Next gate:** First Agent Layer implementation-scope readiness gate

## 1. Purpose and authority

This policy defines the minimum binding security, data-handling and IP-protection rules that apply to the HUBFLO Agent Layer launch scope.

It exists so a later authorized implementation agent can implement Agent Layer infrastructure without inventing security, confidentiality, provider-use, retention, learning, distribution, source-access or IP-handling authority.

This policy must be interpreted consistently with Annex B 1.1. It does not reopen Stage 2, alter Stage 2 operational authority, authorize Agent Layer implementation, or select implementation technology.

If this policy conflicts with HES1.0, CE2.0, closed Stage 2 or Annex B 1.1, the higher authority controls and implementation stops pending reconciliation.

## 2. Normative rules

**MUST / MUST NOT / REQUIRED / SHALL / SHALL NOT** are binding.

**SHOULD / SHOULD NOT** are strong defaults and require recorded rationale for departure.

**MAY** is optional within the governing security and authority boundary.

**AUTHORITY VALUE REQUIRED** means the platform may implement configurable/fail-closed support, but no implementation agent may choose the binding value.

The default for missing security, confidentiality, scope, provider, retention, learning, distribution or source-access authority is **fail closed**.

## 3. Permanent security invariants

The Agent Layer MUST preserve all of the following:

1. Access to one security domain does not create access to another.
2. Tenant/client/project scope is enforced before protected context is assembled or exposed.
3. Novelty creates no access authority.
4. Agent awareness does not create recipient/distribution authority.
5. Entitlement does not override security or authorization.
6. Runtime-agent visibility does not imply source, repository, build, deployment or engineering authority.
7. Credentials and secrets never become model context, retrieval context, embeddings, learned state, generated durable intelligence or ordinary logs.
8. Learning, confidence, provider capability or prior success never creates authority.
9. Provider substitution cannot broaden data exposure, scope, retention, authority or permitted use.
10. Configuration/recomposition cannot bypass permanent security or confidentiality invariants.
11. Client-specific data and private intelligence cannot silently cross client boundaries.
12. Agent Layer security controls cannot weaken Stage 2 authentication, authorization, scoping, persistence or exactly-once protections.
13. Security enforcement must continue deterministically when model/agent reasoning is unavailable.
14. Protected information is exposed only to the minimum independently authorized principal, capability, provider and recipient required for the permitted purpose.
15. Unknown or insufficiently classified protected information is treated as restricted until classification is resolved.

## 4. Security domains

HUBFLO distinguishes four independent security domains.

### SD1 — Source / engineering assets

Includes:
- source code;
- repositories;
- branches;
- build/release controls;
- engineering artifacts;
- deployment controls;
- infrastructure configuration;
- architecture/security engineering material.

Runtime-agent access to operational systems does not imply SD1 access.

### SD2 — Credentials / secrets

Includes:
- provider credentials;
- runtime secrets;
- integration credentials;
- API keys/tokens;
- repository/deployment credentials;
- database credentials;
- authentication secrets;
- signing/encryption key material.

SD2 values are never normal reasoning content.

### SD3 — Runtime / client operational data

Includes:
- client/project records;
- communications;
- operational evidence;
- documents;
- attachments;
- user/account context;
- business facts;
- authoritative Stage 2 state.

SD3 remains governed by tenant/client/project authorization and confidentiality.

### SD4 — Learned / derived intelligence

Includes:
- learned patterns;
- findings;
- predictions;
- benchmarks;
- inferred relationships;
- critical-path/dependency intelligence;
- learned terminology;
- user adaptation;
- intelligence products.

SD4 remains distinct from authoritative Stage 2 truth and retains provenance, source scope, confidentiality, retention, use and distribution controls.

No composition may collapse SD1–SD4 into a single undifferentiated trust domain.

## 5. Required information-control attributes

Material protected information made available to Agent Layer processing MUST be capable of carrying or resolving the applicable:

- security domain;
- tenant/client;
- project or broader authorized scope;
- source/provenance;
- authoritative versus derived status;
- confidentiality restriction;
- permitted use;
- learning eligibility;
- provider eligibility;
- distribution/recipient eligibility;
- retention/withdrawal state;
- current authority/configuration basis.

Not every record must persist every attribute directly. HUBFLO may derive effective controls from authoritative configuration and relationships.

Missing metadata MUST NOT be interpreted as broader permission.

## 6. Identity, current authority and least privilege

Protected information may be accessed or acted upon only under an authenticated principal or separately authorized service context.

Current authority MUST be evaluated at the material access/action boundary.

Cached, historic or persisted authority metadata cannot override:
- revocation;
- narrowed project/client scope;
- disabled capability;
- provider disablement;
- changed confidentiality;
- changed distribution rights;
- kill/disable controls.

Delegation may narrow or transmit existing authority only. It cannot manufacture new authority.

Every material Agent Layer component SHOULD operate with the least access required for its current function.

## 7. Tenant, client and project isolation

Client A proprietary information MUST NOT silently enter Client B:
- prompts/context;
- retrieval;
- model/provider sessions;
- caches;
- embeddings/indexes;
- logs/traces;
- tests;
- learned state;
- derived intelligence;
- handoffs;
- distribution outputs.

Project-specific processing receives only authorized project context.

Cross-project processing is permitted only when the effective principal/capability is independently authorized for the broader scope.

A tenant/client/project boundary MUST be enforced before protected context is exposed to a provider or downstream capability, not merely filtered from final output.

Persisted objectives, work items and continuations MUST revalidate current scope and cannot silently widen the candidate/context universe.

## 8. Source and engineering IP protection

Runtime agents MUST NOT receive source/repository/build/deployment authority merely because they diagnose platform behavior.

Source or engineering assets MAY be exposed only to an explicitly authorized engineering principal/tool for an approved engineering purpose and scope.

External engineering agents:
- receive only the repositories/files required by the authorized engineering task;
- do not gain runtime/client-data authority merely through source access;
- do not gain deployment/security-control authority unless separately authorized;
- must not treat prior rejected candidates or historical patches as current source authority;
- must not persist HUBFLO source into unrelated provider memory, learning or reusable public context.

Client operational data MUST NOT be copied into source repositories, issue trackers, code comments, engineering prompts or engineering artifacts unless a separately authorized, scoped handling path permits it.

Where production evidence is needed for engineering diagnosis, the minimum necessary evidence must be used and secrets/client content must be removed or minimized where possible.

HUBFLO source, proprietary engineering material and security architecture remain protected from ordinary client-facing/runtime distribution.

## 9. Credentials and secrets

Secrets MUST NOT be:
- inserted into model prompts;
- exposed through retrieval;
- placed in embeddings/vector indexes;
- stored in learned/derived intelligence;
- copied into ordinary logs/traces;
- transmitted through uncontrolled agent handoffs;
- exposed to recipients;
- used as examples/test fixtures.

Capabilities requiring credentials SHOULD use secret references/handles or controlled execution boundaries rather than exposing secret values to reasoning components.

If a secret is detected in model/retrieval/logging context:
1. the affected path MUST stop or quarantine before further propagation where technically possible;
2. the secret MUST be treated as potentially compromised;
3. relevant evidence MUST be preserved without further reproducing the secret;
4. the affected credential MUST be rotated/revoked when required by the governing security authority;
5. incident handling in Section 20 applies.

Runtime/model unavailability cannot cause secrets to be exposed as a fallback.

## 10. Runtime-model data minimization

Protected context supplied to probabilistic reasoning MUST be limited to information reasonably necessary for the permitted objective.

Context assembly MUST:
- resolve current tenant/client/project scope first;
- exclude secrets;
- exclude unrelated client/project information;
- exclude engineering/source assets unless the active principal/tool has explicit SD1 authority;
- prefer references/structured facts over uncontrolled full-record/transcript propagation where sufficient;
- retain enough provenance for material derived conclusions.

A model's larger context capacity is not justification for broader data exposure.

Provider/model outputs are untrusted derived content until they satisfy the applicable structured contract and policy gates.

## 11. Provider/model handling

No model/provider is approved merely because it is technically available.

Before a provider receives protected information, HUBFLO MUST determine that the applicable provider configuration/terms satisfy the required:
- tenant/confidentiality treatment;
- permitted-use restrictions;
- training/model-improvement restrictions;
- retention controls;
- access/security controls;
- region/location restrictions where later required;
- audit/attribution needs;
- deletion/withdrawal support where material;
- security equivalence required by this policy.

Protected HUBFLO/client information MUST NOT be used by a provider to train or improve general/shared provider models unless a later explicit HUBFLO authority and applicable client/legal authority permit that specific use.

Provider-side retention MUST be minimized to the shortest approved period/configuration compatible with the permitted function. An implementation agent may not select a broader-retention mode for convenience.

If a provider cannot satisfy the applicable confidentiality, retention, training/use or isolation requirements, protected context MUST NOT be sent to that provider.

Provider/model session memory is never authoritative HUBFLO state.

Provider outputs, usage logs and identifiers sufficient for attribution MAY be recorded within HUBFLO-owned audit controls subject to this policy.

### Provider substitution

A substitute provider may be used only if the new path is no less restrictive for:
- protected data classes;
- tenant/project scope;
- permitted provider use;
- retention;
- confidentiality;
- authority;
- side-effect semantics;
- structured output;
- audit attribution.

If equivalence cannot be established, the affected function MUST stop, remain pending, or use a separately approved deterministic/non-agent path.

## 12. Logs, traces, caches, embeddings and indexes

Logs/traces MUST be sufficient for operational/security/audit needs without becoming uncontrolled copies of protected content.

They MUST NOT contain secrets.

Where protected content is stored in logs, traces, caches, embeddings or indexes:
- the same tenant/client/project confidentiality boundary applies;
- access must be least privilege;
- retrieval must preserve scope;
- retention must be governed;
- revocation/withdrawal/deletion state must be respected;
- cross-client retrieval must be prevented.

Caches MUST NOT cause data to survive beyond the authorization or retention state that made the data eligible.

Embeddings/indexes are not de-identified merely because content is transformed.

A vector/semantic index MUST be treated as protected if its contents or outputs can reveal protected information.

Debugging or observability features cannot be enabled in a way that silently broadens protected-data exposure.

## 13. Learning and derived intelligence

Learning is HUBFLO-owned governed state across separate:
- platform;
- industry;
- client;
- individual-user scopes.

Client-specific learning remains client-private unless a separately governed aggregation/generalization path applies.

Individual-user learning remains within its permitted user/client scope and cannot silently propagate upward or sideways.

Derived intelligence:
- remains distinct from authoritative Stage 2 truth;
- retains sufficient provenance/evidence;
- retains applicable scope/confidentiality restrictions;
- cannot create authority;
- cannot silently expose its protected source content to a wider audience;
- must support correction/refinement/supersession/withdrawal without destroying required governance history.

Access to source evidence does not automatically grant distribution rights in derived intelligence.

Access to derived intelligence does not grant access to its raw source material unless independently authorized.

### Novel intelligence

Novel/unclassified intelligence is eligible for HUBFLO supervisory review only where an independently authorized supervisory principal/function may access it.

Novelty itself creates no access authority.

Where no authorized supervisory access exists, the intelligence MUST remain held/quarantined or be discarded according to applicable policy; visibility may not be broadened merely to obtain review.

Novel intelligence cannot become routine client or cross-client distribution until its use/distribution is governed.

## 14. Cross-client aggregation and generalization

Default launch policy is:

**cross-client proprietary reuse/generalization is disabled unless an explicit governed aggregation/generalization authority is in force.**

A later approved path MUST define:
- eligible source scopes;
- permitted purpose;
- de-identification/aggregation requirements;
- minimum evidence/cohort controls where applicable;
- prohibited proprietary/raw content;
- provenance;
- resulting intelligence scope;
- retention;
- withdrawal/correction effect;
- permitted recipients/use.

No implementation agent may infer permission to generalize because two clients use similar language, industries, workflows or providers.

Until such authority is version-locked, client-specific data and learning remain private to that client.

## 15. Awareness, distribution and recipients

The following are separate policy decisions:
1. intelligence exists;
2. an agent may be aware of it;
3. it may influence authorized internal reasoning;
4. a supervisory function may inspect it;
5. it may be distributable;
6. a particular recipient may receive it.

Authorization for one does not imply authorization for another.

Before material distribution, HUBFLO MUST evaluate the current:
- recipient identity;
- tenant/client/project scope;
- confidentiality;
- data/intelligence classification;
- distribution configuration;
- entitlement where applicable;
- channel/integration eligibility.

A recipient's commercial entitlement cannot override confidentiality or authorization.

A channel cannot create new recipient rights.

Prohibited or uncertain distribution fails closed.

## 16. Development, test and engineering data

Synthetic or purpose-built test data SHOULD be used where it can prove the required behavior.

Production/client data MAY be used in controlled testing or diagnosis only where:
- the use is necessary;
- the test/engineering principal is independently authorized;
- scope is minimized;
- secrets are excluded;
- isolation is preserved;
- evidence is auditable;
- the use complies with applicable retention and provider rules.

Development/test configurations may intentionally expose broader test findings to authorized test principals, but this creates no production distribution right.

Test fixtures, snapshots, logs and derived artifacts MUST NOT leak one client's protected information into another client's test context.

Production data MUST NOT be copied into external engineering or model contexts solely for convenience.

## 17. Retention, deletion and withdrawal

Retention must be purpose-limited and governed by data/intelligence class and applicable authority.

Protected data MUST NOT be retained indefinitely solely because storage is available or future usefulness is speculative.

Existing Stage 2 authoritative data retention is not changed by this policy.

For new Agent Layer state:
- a retention basis/state must be determinable;
- optional new persistent collection with no authorized retention basis must remain disabled;
- transient provider/model context must use the minimum approved provider retention mode;
- learned/derived state must remain subject to withdrawal, supersession and permitted deletion/retirement.

### Withdrawal

Where a user/client/policy withdrawal is effective:
- future optional use, learning, promotion and distribution covered by the withdrawal MUST stop;
- affected active work MUST revalidate eligibility;
- stored state must be marked for the required delete/quarantine/restriction treatment;
- derived intelligence materially dependent on withdrawn source must be re-evaluated for continued eligibility;
- prior audit/provenance may retain the minimum non-content evidence required for governance where permitted.

### Deletion

Destructive deletion of authoritative or legally/commercially material records remains a high-risk governed action under Annex B unless a later authority defines a bounded automated retention-expiry subtype.

Until such authority exists, an implementation agent MUST NOT invent an automated destructive-deletion policy for authoritative Stage 2 business records.

Deletion or retirement of Agent Layer derived/optional state MUST preserve required non-sensitive provenance/audit linkage where technically and legally permitted.

Exact retention periods remain authority/configuration values unless already fixed by another controlling instrument.

## 18. Backup and recovery

Backups inherit the confidentiality/security classification of the protected data they contain.

Backup access MUST be independently controlled and must not create a second informal data-access path.

Restore operations MUST:
- preserve tenant/client/project isolation;
- restore current security configuration or revalidate it before resumed consequential processing;
- not silently reactivate revoked capabilities/principals/providers;
- not treat stale permissions as current;
- preserve audit/evidence needed to establish restored state.

Deleted/withdrawn material that remains temporarily inside immutable backup media MUST NOT be returned to ordinary operational use after the deletion/withdrawal becomes effective, except where a separately authorized restore/legal/security process requires it.

Exact backup retention and RPO/RTO values remain separately governed.

## 19. Security observability

Security observability is separate from ordinary platform-health monitoring.

The platform SHOULD detect applicable:
- unusual or denied access;
- tenant-boundary violations;
- unexpected cross-scope retrieval;
- privilege/autonomy changes;
- credential failures/exposure;
- suspicious provider behavior;
- abnormal tool/capability use;
- exfiltration/leakage indicators;
- policy-bypass attempts;
- repeated authorization failure;
- unexpected source/engineering access;
- anomalous distribution.

Security-event evidence MUST be protected from ordinary runtime-agent modification or suppression.

The runtime agent implicated in an event cannot be the sole verifier or suppressor of that event.

Cross-client security correlation may use only information permitted by this policy and any later approved aggregation/security authority.

## 20. Incident containment and response

On a credible suspected security, tenant-isolation, secret-exposure, provider-misuse or unauthorized-distribution event, HUBFLO MUST be capable of:

1. identifying the affected security domain, client/project and component/provider where possible;
2. stopping, disabling or isolating the affected capability/provider/path through separately authorized deterministic controls where required;
3. preventing further distribution or reuse of suspect outputs/state;
4. preserving sufficient evidence and provenance for investigation;
5. revalidating credentials/authority/configuration and rotating/revoking credentials where required;
6. quarantining affected derived intelligence where its scope/integrity is uncertain;
7. escalating to the authorized HUBFLO security/supervisory principal;
8. restoring service only after applicable security conditions are revalidated.

Incident response cannot rely solely on the same agent/context whose behavior is under investigation.

An incident does not authorize cross-client visibility, public disclosure or external notification beyond independently applicable authority/legal obligations.

Exact notification timelines and external legal/reporting duties remain separately governed.

## 21. Revocation, disablement and recomposition

Revocation or narrowing of:
- user/principal authority;
- client/project allocation;
- provider approval;
- capability availability;
- distribution rights;
- learning eligibility;
- delegation;
- entitlement

must prevent future eligibility after authoritative propagation.

Active consequential work MUST revalidate before further progression where the changed state may affect legality, scope or security.

Safe recomposition MUST calculate the applicable security, confidentiality, data-use, provider, authority and distribution constraints for the new composition.

A new graph, specialist, agent, tool or provider combination cannot require manual recreation of security from scratch and cannot silently omit an existing invariant.

## 22. No-agent / degraded operation

Security, scope, provider-eligibility, distribution and secret-protection enforcement MUST remain deterministic and active when model/agent reasoning is unavailable.

Model outage MUST NOT:
- widen access;
- bypass authorization;
- expose secrets;
- use an unapproved provider;
- permit cross-client retrieval;
- allow distribution without current permission;
- invent a retention or learning decision.

Judgment-dependent security decisions remain pending, escalate, or stop fail-closed.

Recovery MUST revalidate current authority, configuration, provider eligibility and material state.

## 23. Security of consequential execution

Agent reasoning never directly authorizes consequential execution.

Consequential actions continue to require Annex B's governed capability, authority, risk, side-effect, approval, verification, replay/idempotency and current-state gates.

Security/permission/autonomy change, destructive deletion, major infrastructure/provider migration and similar R4/S4 actions require the applicable stronger approval and independent verification.

Security policy configuration cannot be altered by an agent in order to authorize its own otherwise prohibited action.

## 24. IP, source data and derived intelligence rights boundary

For security/control purposes:
- HUBFLO source/engineering assets remain protected HUBFLO engineering material;
- client operational/source data remains client-confidential and scope-bound;
- learned/derived intelligence remains HUBFLO-governed durable state under Annex B, with provenance and source-scope restrictions.

This policy does not invent contractual ownership or commercial reuse rights not already established by controlling agreements/authority.

Client data or client-derived intelligence MUST NOT be sold, licensed, published, disclosed to another client or used for cross-client commercial intelligence merely because HUBFLO can technically derive it.

Commercial/cross-client reuse that depends on unresolved legal/contractual rights remains disabled until separately authorized.

A generalized or de-identified derivative does not automatically become unrestricted; it must pass the approved aggregation/generalization policy.

## 25. Encryption and transport protection

Protected data transmitted between HUBFLO-controlled components or approved providers MUST use authenticated encrypted transport.

Protected persisted data MUST use the applicable platform/storage encryption-at-rest controls.

Exact cryptographic library, cloud service or key-management implementation is a later technical decision; implementation may not deliberately weaken the security controls provided by the chosen approved platform.

Secret/key material remains SD2 and is not exposed to reasoning components.

## 26. Acceptance requirements

A candidate implementation claiming compliance with this policy MUST provide evidence sufficient for RTW7 to test the following.

**SDIP-T01 — Tenant/client isolation**
- wrong-client retrieval/context is denied;
- no protected data appears in prompt, cache, embedding, trace, learning or output across the client boundary.

**SDIP-T02 — Project scope**
- unauthorized cross-project context is denied;
- broader reasoning works only under independently authorized broader scope.

**SDIP-T03 — Secrets**
- secrets are excluded from model context, learning, embeddings and ordinary logs;
- secret-reference execution can function without exposing the secret value to reasoning.

**SDIP-T04 — Source/engineering boundary**
- runtime agents cannot obtain source/repository/deployment authority by runtime role;
- engineering access remains separately scoped.

**SDIP-T05 — Provider gate**
- unapproved/incompatible provider cannot receive protected context;
- provider failure/substitution does not broaden exposure or authority.

**SDIP-T06 — Provider-use/retention protection**
- provider configuration/record identifies applicable training/use/retention restrictions;
- an incompatible retention/training mode fails closed.

**SDIP-T07 — Learning/intelligence isolation**
- client learning remains client-private;
- individual-user learning does not silently propagate;
- derived intelligence remains distinct from Stage 2 truth.

**SDIP-T08 — Cross-client generalization**
- cross-client proprietary reuse is denied while no approved generalization authority is active.

**SDIP-T09 — Novel intelligence**
- novelty does not itself grant supervisory or recipient access;
- unauthorized supervisory visibility is denied/held.

**SDIP-T10 — Distribution**
- agent awareness and entitlement do not confer recipient access;
- prohibited recipient/channel distribution fails closed.

**SDIP-T11 — Revocation**
- revoked principal/provider/capability/distribution/learning authority is blocked for future use;
- active consequential work revalidates where required.

**SDIP-T12 — Retention/withdrawal**
- new optional persistent state without an authorized retention basis does not silently persist;
- withdrawal stops future governed use and produces the required restricted/quarantine/delete state.

**SDIP-T13 — Backup/restore**
- restored data retains isolation and current authority is revalidated;
- revoked rights are not silently resurrected.

**SDIP-T14 — Development/test separation**
- test/engineering context does not silently import unauthorized production/client data;
- broader test visibility does not become production distribution authority.

**SDIP-T15 — Security incident containment**
- representative suspected breach/secret/provider event can be contained, evidenced and escalated without the implicated agent suppressing the event.

**SDIP-T16 — No-agent/degraded security**
- deterministic security/scope/distribution/provider gates continue when model reasoning is unavailable.

**SDIP-T17 — Recomposition**
- representative agent/tool/provider recomposition preserves effective tenant, security, provider, authority and distribution constraints.

**SDIP-T18 — Stage 2 preservation**
- Agent Layer security implementation does not alter accepted Stage 2 authorization, lifecycle, persistence, routing or exactly-once behavior.

Evidence classes SHALL use Annex B's T-D, T-I, T-S, T-F, T-X, T-L and T-R classifications as applicable. Security/isolation claims require negative cases. Unsafe destructive/high-risk live actions need not be created solely for testing where controlled negative/verification evidence proves the gate.

## 27. Deferred authority/configuration register

The following values are not selected by this RC1. They do not block structural implementation where the dependent behavior remains disabled or fail-closed.

**SDIP-AV-001 — Live provider allowlist and provider-specific permitted data classes**  
Default until approved: no provider is entitled to receive protected launch data merely because an adapter exists.

**SDIP-AV-002 — Exact persistent retention periods**  
Default until approved: no new optional persistent collection without a governed retention basis; existing Stage 2 retention remains unchanged.

**SDIP-AV-003 — Backup retention and RPO/RTO values**  
Default until approved: implementation may provide configurable backup/recovery machinery but cannot invent binding operational targets.

**SDIP-AV-004 — Cross-client aggregation/generalization criteria**  
Default until approved: disabled.

**SDIP-AV-005 — Individual-user persistent learning/adaptation retention and promotion**  
Default until approved: persistent adaptation behavior requiring the unresolved value remains disabled/fail-closed.

**SDIP-AV-006 — Novel → established intelligence governance criteria**  
Default until approved: novel intelligence may be retained/quarantined only under authorized scope; routine distribution/generalization remains disabled.

**SDIP-AV-007 — Production engineering/source-access assignments**  
Default until approved: runtime principals have none; engineering access requires explicit separate authorization.

**SDIP-AV-008 — Commercial/legal cross-client reuse and ownership rights beyond existing authority**  
Default until approved: no cross-client/commercial reuse.

**SDIP-AV-009 — Incident external-notification/legal-reporting timing**  
Default until applicable authority/law is determined: preserve evidence, contain internally and do not invent external disclosure authority.

**SDIP-AV-010 — Automated destructive retention-expiry classes**  
Default until approved: no new automated destructive deletion of authoritative Stage 2 business records.

These values become implementation blockers only for a claimed launch behavior that cannot remain disabled/fail-closed without them.

## 28. Implementation freedom

This policy fixes required outcomes and boundaries, not unnecessary mechanics.

A later authorized implementation agent retains source-aware freedom to choose, subject to RTW7 review:
- schemas/tables;
- classes/modules;
- service boundaries;
- policy representation;
- access-control implementation;
- encryption/key-management technology;
- provider adapters;
- secret manager;
- cache/index technology;
- logging/observability stack;
- queue/orchestrator;
- deployment topology;
- transaction/concurrency mechanisms;
- backup technology.

No implementation choice may weaken this policy or Annex B.

## 29. Pre-implementation gate

This final policy satisfies Annex B 1.1's mandatory **Security, Data & IP Protection Policy version-lock gate** for the Agent Layer launch scope.

It does **not** itself authorize Agent Layer implementation.

Before any Agent Layer implementation package is authorized:

1. the implementation record MUST identify this locked policy version;
2. any authority/configuration value required by the claimed implementation scope MUST be supplied, or the dependent behavior MUST remain explicitly disabled/fail-closed;
3. the canonical Take-on authority instrument MUST be available where the claimed scope depends on detailed Take-on authority;
4. any deferred security value required by the claimed scope MUST be resolved before that behavior is enabled;
5. O3/RTW7 source, lineage, scope and acceptance gates remain applicable.

If an implementation package cannot identify an applicable locked policy version or depends on an unresolved binding security value that cannot remain disabled/fail-closed:

**STOP — PRE-IMPLEMENTATION SECURITY/DATA/IP POLICY GATE NOT SATISFIED**

## 30. Conflict, version-lock and remaining-gate report

**AUTHORITY CONFLICT:** NONE identified with Annex B 1.1, HES1.0, CE2.0 or closed Stage 2.

**VERSION-LOCK BASIS:** `PASS — SECURITY, DATA & IP PROTECTION POLICY 1.0-RC1 ACCEPTED FOR NEXT AUTHORITY GATE`.

**SUBSTANTIVE CHANGE FROM ACCEPTED RC1:** NONE.

The deferred values in Section 27 remain intentionally fail-closed. They need be resolved only when a claimed launch behavior materially depends on them.

**AGENT LAYER IMPLEMENTATION AUTHORITY:** NONE.

**NEXT GATE:** define the first coherent Agent Layer implementation scope; resolve only the authority/configuration, Take-on and deferred security values that scope actually requires; then proceed through O3 authorization to RTW7's bounded Codex generation instruction.

---

**HUBFLO SECURITY, DATA & IP PROTECTION POLICY 1.0 — FINAL / AUTHORITY-LOCKED — AGENT LAYER IMPLEMENTATION NOT AUTHORIZED**
