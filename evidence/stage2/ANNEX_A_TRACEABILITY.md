# Annex A 1.0 — Deterministic/Controlled Traceability

Candidate identity: the Stage 2 candidate commit containing this evidence
package. Resolve it with `git rev-parse HEAD` and verify ancestry with
`git merge-base --is-ancestor 29fcfd9 HEAD`.

Evidence labels in this record retain Annex A semantics:

- D = deterministic unit/database/concurrency/static verification.
- C = isolated Flask webhook/application-stack execution.
- P = isolated authoritative SQLite persisted-state inspection.
- L = genuine WhatsApp → 9122 → HUBFLO `/webhook`; no L result is claimed.

## Traceability matrix

| Annex A cases | D/C implementation and evidence | Internal result | Required L status |
|---|---|---|---|
| A-BASE-001–007 | Git ancestry; accepted MU1–MU13 regression; static Core/Industry and handler checks; legacy await C tests | D/C PASS | A-BASE-007 live compatibility remains outstanding where bound to L cases |
| A-TASK-001–004 | `interpret_supported_message`; Task handler C/P tests for ordinary, assigned, self and urgent semantics | D/C PASS | NOT TESTED — L+P required |
| A-NOTE-001–002 | Note/pinned-note convergence to authoritative Task handler with distinct tag/subtype | D/C PASS | NOT TESTED — L+P required |
| A-ORDER-001–003 | natural field extraction; complete Order; missing-only state; continuation | D/C/P PASS | NOT TESTED — L+P required |
| A-DELIV-001 | Delivery route and persisted authoritative Task representation | D/C/P PASS | NOT TESTED — L+P required |
| A-CO-001–002 | approval route; client/project authorization; webhook-arrival with unchanged unauthorized object | D/C/P PASS | NOT TESTED — L+P required |
| A-STOCK-001–002 | tenant-isolated Stock mutation; legacy Stock await and opening-balance persistence | D/C/P PASS | NOT TESTED — L+P required |
| A-INSP-001 | shared date consumer, route, project/client persistence | D/C/P PASS | NOT TESTED — L+P required |
| A-DELAY-001–002 | natural unique Task resolution and explicit numeric-ID path through `log_delay` | D/C/P PASS | NOT TESTED — L+P required |
| A-REM-001–004 | creation/date persistence; ordinary lifecycle recognition; natural and explicit-ID cancellation | D/C/P PASS | NOT TESTED — L+P required |
| A-MEET-001 | natural structured schedule and authoritative Meeting handler | D/C/P PASS | NOT TESTED — L+P required |
| A-SEARCH-001, A-STATUS-001 | client/project-scoped exact controlled result; persisted no-mutation verification | C/P PASS | NOT TESTED — L route required |
| A-CONTEXT-001–004 | converged project/phase/zone/trade entities and project-bearing consumer tests | D/C PASS | NOT TESTED — L plus C/P as applicable |
| A-RES-R0/R1/RM | actual Reminder zero/one/many webhook orchestration and persistence | D/C/P PASS | NOT TESTED — L+P required |
| A-RES-P0/P1/PM | neutral 0/1/many resolution plus authorized assigned/Reminder-recipient candidate construction | D/C PASS | NOT TESTED — L+P required |
| A-RES-J0/J1/JM | neutral 0/1/many resolution and authorized project candidate discipline | D/C PASS | NOT TESTED — L+C/P required |
| A-RES-D0/D1/DM | Delay resolution 0/1/many contract and natural unique mutation | D/C/P PASS | Conditional L remains as specified by Annex A |
| A-CLAR-001–005 | persisted separate webhook turns, invalid/still-ambiguous, Reminder/person/project bounded continuation | D/C/P PASS | NOT TESTED where L required |
| A-BYPASS-001–005 | await and clarification deterministic Inspection bypass; unchanged candidates/activity; resume | D/C/P PASS | NOT TESTED — L+P required cases |
| A-LIFE-001–010 | exact 24-hour UTC boundary for await/clarification; stale retirement; cancel/restart/resume/abandon; object separation | D/C/P PASS | NOT TESTED — required operational L cases |
| A-DT-001–016 | ISO/regional/ambiguity/separators/named/relative/weekday/12h/24h/timezone/voice/display/persistent clarification | D/C/P PASS | NOT TESTED — representative L cases |
| A-MU16-001–005 | Order, Reminder lifecycle, Meeting and structured-handler boundary suite | D/C/P PASS | Covered by outstanding feature L cases |
| A14 MU17 matrix | all required feature families route through converged meaning to existing handlers | D/C/P PASS | Covered by outstanding A7 L surface |
| A-AUTH-001–007 | cross-client Task/Stock/Delay/approval/search; project denial; candidate narrow/no-broaden/attribute/current-record/canonical-client checks | D/C/P PASS | NOT TESTED — representative L isolation required |
| A-XO-001–006 | sequential replay, completed retry, concurrent duplicate, continuing loser, invalid then valid continuation | D/C/P PASS | NOT TESTED — A-XO-001 L+P required |
| A-COMP-001–005 | Stock/Order awaits; Reminder/Delay numeric IDs; accepted MU5–MU9 temporal forms | D/C/P PASS | NOT TESTED — specified L+P paths |
| A-FALL-001–005 | ordinary fallback, pending-state fallback, generic Order vocabulary and specialized route matrix | D/C PASS | NOT TESTED — representative L required |
| A-EVID-001–005 | controlled arrival/no-mutation, persisted mutation sets, read-only no-mutation, provider disabled/separate, honest labels | C/P PASS for controlled legs | NOT TESTED — required L artifacts and provider log |

## Automated test ownership

- `test_accepted_mu1_mu13.py`: accepted baseline regression.
- `test_mu14_lifecycle.py`, `test_mu14_webhook.py`: MU14 D/C/P.
- `test_mu15_datetime.py`: MU15 D/C/P.
- `test_mu16_features.py`: MU16 D/C/P.
- `test_mu17_convergence.py`: MU17 routing, authorization and persistence.
- `test_annex_a_controlled.py`: cross-MU Annex A orchestration, compatibility,
  authorization and concurrency.
- `test_annex_a_architecture.py`: dependency, handler and schema invariants.

No row marked D/C PASS is represented as an Annex case PASS when Annex A also
requires L. Those cases remain open pending independent live execution.
