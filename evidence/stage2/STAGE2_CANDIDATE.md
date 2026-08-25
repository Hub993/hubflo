# HUBFLO Stage 2 — Internally Green Candidate

## Source progression

```text
29fcfd9  accepted MU13 starting source
e6163a7  MU14 conversation lifecycle policy
c92b259  MU15 shared date/time completion
ed34280  MU16 natural feature completion
ad419ba  MU17 routing convergence checkpoint
HEAD     closure-harness repairs and evidence package
```

The candidate is descended directly from `29fcfd9`; no historical source was
reconstructed. Each MU checkpoint contains its own implementation, tests and
evidence record.

## Reproduction

From the repository root with Package 1.2 dependencies installed:

```text
HUBFLO_TEST_PYTHON=venv/bin/python scripts/run_stage2_regression.sh
```

The runner creates a private temporary SQLite database, disables outbound
provider credentials, runs all deterministic/controlled tests, compiles every
runtime module, and removes only its validated temporary directory.

Final recorded result: **59 tests passed** plus successful byte compilation.

## Repair record from closure regression

The post-MU17 controlled closure run found and corrected two in-scope defects:

1. An abandoned legacy await could call the MU16 natural-Order continuation
   probe without active persisted state. The probe now safely declines when no
   state exists; the retired await cannot revive or mutate its business record.
2. The accepted legacy new-Stock await supplied `opening_qty`, but the Stock
   handler discarded it. The authoritative handler now persists the opening
   balance and one corresponding movement without resetting an existing item.

Both repairs have focused regression coverage and are included in the complete
candidate rerun.

## Dependency and license impact

No third-party dependency was added. Runtime requirements remain those in
`requirements.txt`. The test harness uses Python `unittest`, temporary SQLite,
and standard-library concurrency utilities.

## Security impact

- Canonical sender client identity is persisted for Stage 2 business records.
- Client/project checks precede approval, Delay numeric-ID and recipient action.
- Stock identity includes client scope, preventing same-name tenant collision.
- Conversation state remains keyed and claimed by client/sender/project.
- Current candidate records and authorization-bearing attributes are rechecked
  before clarification mutation; authorization can narrow but never broaden a
  persisted universe.
- No production credential, deployment, protected-branch merge or outbound
  provider call was used.

## Data model and migration impact

- `conversation_states`: lifecycle activity/retirement columns.
- `users`: date order, time format and date-display configuration.
- `tasks`, `inspections`, `delay_logs`, `stock_items`: client ownership is
  retained/added and legacy rows are backfilled to client 1.
- Existing application startup repair performs additive, backward-compatible
  migration. No destructive rollback is required; older source can ignore the
  added columns.

## Internal status

All automatable D/C legs exercised by this environment are green. This is an
**internally green Stage 2 candidate**, not `PASS — STAGE 2 COMPLETE`.

Stage 2 remains open until the final candidate is deployed, every required
Annex A L case is executed through genuine WhatsApp → 9122 → HUBFLO `/webhook`
with P evidence, and RTW issues the independent completion verdict.
