# Poly Alpha durable status

Updated: `2026-08-25T18:01:49.1187322+07:00`

## Current boundary

`SUCCESSOR24_HEALTHY_PHASE_ONE_ACQUISITION_RUNNING`

The canonical current record is
`.codex/SUCCESSOR24_INITIAL_HEALTHY_HANDOFF.json`. Successors 1 through 23 are
terminal, permanently ineligible, and may not be relaunched, repaired, pooled,
or reused.

The accounting root cause is fixed without changing raw telemetry diagnostics,
controller behavior, loss taxonomy, or fail-closed mismatch gating. Mandatory
RED was followed by 1 focused, 42 subsystem, 241 concurrency/persistence, 1
bounded stress, and 287 broader runtime/safety tests, all green with zero skips.

Successor24 source qualification ran exactly once and passed 4,192/4,192 with
zero failures, errors, or skips. Tested and post-test tree are exactly
`7a1c9c09d124a44d732e0cbdd26336616e2804d9a97c1fc6f8ccd6bfed08f3b5`;
authoritative v5 remained byte-identical and journal-free.

Successor24 materialized exactly once, passed independent cold verification,
and launched Phase One exactly once. The same session
`b1e0463d97b54df888535ce5ef0883b5` is running under nonce
`f138bd0a49fb413c9fb29e9911163c93` and PID pair `15836 -> 16544`.
The sole independent guardian is running as `4016 -> 19272`.

At the durable initial capture, four guardian observations proved forward
progress from 26 to 39 capsules, 51 to 76 predictions, and 514 to 895 committed
commands. There are zero binding persistence, integrity, source, safety, or
holdout violations; reconciliation is exact and both stderr files are empty.

Exact next action: resume from current disk authority, verify only this same
identity and the latest single guardian state, then continue bounded Phase-One
acquisition under the frozen OOS/readiness protocol. Never relaunch the runtime
or duplicate the guardian while it is running.

Known non-authoritative path artifact: the similarly named root under
`D:\claude` contains only an empty `operator` directory and is not an
acquisition. The canonical Successor24 root is directly under `D:\`.

The bounded two-hour classification remains `TWO_HOUR_REVIEW_FAILED`. Its
canonical forensic record is `.codex/SUCCESSOR23_TWO_HOUR_REVIEW.json`; earlier
material below remains historical.

Successor22 remains gracefully terminal, permanently forensic/ineligible, and
`MUST_NOT_RELAUNCH`. Its database and immutable failure evidence were not
changed.

The canonical retention contract is now implemented as explicit monotonic
lifecycle state: `RAW < TRADE_EVIDENCE < PERMANENT`. Source and book rows are
promoted only inside an owning transaction after all immutable evidence fields
match. Weaker re-offers preserve stronger state, trade pinning cannot demote
permanent source/book/CEX evidence, and genuinely different evidence still
raises `V4EvidenceConflict`. Journal failure finalization now preserves the
actual semantic attempt count instead of stamping the configured retry ceiling.

Verification is green:

- final focused: 5/5;
- directly affected persistence/runtime/schema: 425/425;
- concurrency/adversarial/lineage/recovery: 177/177;
- broader runtime/config/safety: 208/208;
- zero failures, errors, or skips in every green layer.

The tests cover the exact RAW-first failure, partial graph retry, same-class
duplicate, repeated retry, reversed/weaker order, simultaneous same-identity
bundles, concurrent promotion ordering, invalid downgrade prevention,
unrelated identities, true immutable conflicts, dead/stopped workers, delayed
acknowledgement, saturation, shutdown, loss accounting, guardian-visible state,
journal/reconciliation, lineage, and safety.

Full evidence: `.codex/SUCCESSOR22_RETENTION_FIX_VERIFICATION.json`.

Safety remains fail-closed. The deliberate dirty tree is preserved at branch
`master`, HEAD `d3364f219feb37a09a547ff1daba6f0f96377fe4`, index tree
`700307bdbc9a4fdab7615d79eea83fe1bf6463cb`, with zero staged paths.

Successor23 source qualification passed exactly once at
`D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor23_20260825T060219Z`:
tested tree `249c1ec0aee2f38ccd954b9fe6a76f4be053dabcf1dc7ffa9d5eb880ec701afd`,
manifest `3c464a56a297532995bf80b1809baff79e2eec905ff10ec7b476bb1bd56b76dc`,
runner `d3d3d63b7ca2c6a511df424c25c1eb87a1353feb41fa94c0cda1f4f358acf48d`.
The sole launch passed 4,188 tests with zero failures, errors, or skips. Post-tree
equality is exact; post-verification
`9fb81c3adcda93b65d2727cd640b897e74c17c2f9e565bb3708ca125f7a411fe`
also proves authoritative v5 remains byte-identical with no journal.

The historical Successor23 acquisition
`V4-PR-001-PROSPECTIVE-SUCCESSOR23-20260825T063851Z` is terminal at
`D:\poly_alpha_prospective_exact_v6_successor23_20260825T063851Z`.

Materialization ran once and exited zero. Manifest
`bd1aa9c5c0967200317e3c21ce13209b3bce8aed4a6c3697a9a6c26d4e637d8d`
binds fresh database
`7866fc6bee896cdcc4ef0d53b0ae544407cc3adf85f767c11ae15da8cdfaba53`.
Independent immutable cold verification
`3fd55f19f77ca42d3dfdbf1ae537102bef820ddb0fc48a9e9656aefcac39c3e6`
passed exact-v6 schema, quick/FK, empty business/research/holdout state, released
lease, zero runtime processes, and unchanged v5. A corrected verifier predicate
was rerun only because its first invocation stopped before creating either
output; materialization was not rerun.

Phase One launched exactly once: session
`e8cc5e49b94d4f59b9e1eb5f16e5544e`, nonce
`cb94a3af73994324b3290ab599f0467a`, PID pair `16088 -> 11856`.
It remains the same consumed terminal identity and `MUST_NOT_RELAUNCH`.

The independent guardian `11512 -> 15300` is now terminal after 230 clean
observations and one binding create-once failure. The first failure artifact is
`guardian_failure_1787648489397.json`, SHA-256
`26169b2c3fca7e9db9f0f9ef850e9d96bee4b7c7b2b2bc14395523696145a490`:
`telemetry_data_safety:UNSAFE`, caused by accounting reconciliation mismatch
`-1`. Critical evidence incomplete/lost, true critical loss, overflow, source
discard, and unexpected loss remain zero. Later capacity recovery does not cure
the first immutable failure.

The consistent review census has 1,447 capsules, 2,885 predictions, 238 markets,
181 outcomes, and 25,654/25,654 committed journal commands. Both ensemble and
model readiness are 211 rank-1 rows, 211 unique markets, 82 positive, 99
negative, and 30 unlabeled; each misses 89 rows/markets and requires all 30
unlabeled rows to resolve. Lineage, temporal validity, SQLite quick-check, FK,
schema fingerprint, Successor22 failure-class regression, safety, v5
immutability, and holdout separation pass.

Codex active work is stopped at the healthy initial-acquisition handoff.
Successor24 and its independent guardian remain running unchanged. No stop,
calibration, tournament, Phase Two, holdout, Phase 3, live/authenticated
trading, signing, orders/cancels, or v5 mutation is authorized.
