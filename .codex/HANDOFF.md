# Poly Alpha continuation handoff

Updated: `2026-08-25T19:59:48.4651722+07:00`

## Durable boundary

`SUCCESSOR25_SOURCE_SEAL_PREPARED`

Canonical transition detail is in
`.codex/SUCCESSOR25_SOURCE_SEAL_PREPARED.json`.
The boot-evidence correction remains at
`.codex/SUCCESSOR24_BOOT_EVIDENCE_CORRECTION.json`, SHA-256
`d7dc60eebfe94c4a474e1eb22b88c554d4a34962efb5cc2b4d1d49fc54377377`.
The refused v1 record remains immutable at
`.codex/SUCCESSOR24_HOST_LOSS_APPLY_V1_REFUSAL.json`, SHA-256
`4899944a75c9d72955c762994ef46f3282539b705ca3623cd786042083070ff1`.
The external refusal artifact is SHA-256
`ed111670da59041bf9082d05dfacfc5638a38c73a9670f2d6386e9bea9f83129`.
The causal fix verification remains in
`.codex/SUCCESSOR24_POWER_LOSS_DURABILITY_FIX_VERIFICATION.json`, SHA-256
`c7ddd3a9f434dfb958d005846332418effae815b2260f5a4018cb9ed597acaf2`.
Its immutable forensic input is
`.codex/SUCCESSOR24_POST_HOST_LOSS_FORENSIC_BOUNDARY.json`, SHA-256
`cf996709d3bf1776f6a5b80c8132441e6a801a3a840efe54e4098176bf978a5e`.

Successor24 is permanently ineligible. Never relaunch, repair, finalize,
calibrate, pool, or reuse it. Successors 1 through 23 remain terminal and
permanently ineligible under their existing immutable dispositions.

## Exact Successor24 identity

- Root: `D:\poly_alpha_prospective_exact_v6_successor24_20260825T104125Z`
- Acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR24-20260825T104125Z`
- Tested tree: `7a1c9c09d124a44d732e0cbdd26336616e2804d9a97c1fc6f8ccd6bfed08f3b5`
- Materialization nonce: `a4d60ae0194c4850ad64c2ad8b0e8c79`
- Phase-One nonce: `f138bd0a49fb413c9fb29e9911163c93`
- Session: `b1e0463d97b54df888535ce5ef0883b5`
- Runtime PID pair: `15836 -> 16544`
- Guardian PID pair: `4016 -> 19272`
- Reserved Phase-Two nonce `779cf700edeb4b57a57a7b8d111e2df9`
  remains unauthorized and unconsumed.

The current Windows boot began at `2026-08-25T11:31:46.500Z`. None of the
Successor24 runtime/guardian processes exists, no automatic relaunch occurred,
and the old PIDs were not reused. Canonical forensic closure ended the sole
runtime session at `1787661736890` with stop reason
`host_loss_integrity_failure_forensic_only`. The original Phase-One lease
remains stranded as evidence; no false release was written.

## Binding integrity failure

The last durable guardian snapshot is
`D:\poly_alpha_prospective_exact_v6_successor24_20260825T104125Z\operator\guardian_snapshot_1787657094098.json`,
SHA-256 `974c2a09c4cd7455700c66e69fb959b759aa9dc59f5729322c800c16234ae1f3`.
It was clean at 355 capsules, 705 predictions, 88 markets, 25 outcomes, and
6,387 committed commands. Guardian stdout remained clean through timestamp
`1787657367829`, reaching 421/837/96/33 and 7,379 committed commands.

The recovered database passes quick-check and foreign-key checks and has one
contiguous committed journal sequence `1..7431`, with zero failed, unresolved,
duplicate, or retried commands. Its last command is
`...:000000007431:persist-fee-authority-bundle` at `1787657380994`.

The last pre-loss state file reports 7,432 submitted, 7,432 committed, zero
failed/unconfirmed/inflight commands, and last acknowledged command
`...:000000007432:persist-fee-authority-bundle` at `1787657383322`. Command
7,432 is absent from the recovered database. The persistence worker publishes
that acknowledgement only after SQLite commit returns. This is one lost
acknowledged command across physical power loss and is binding even though the
guardian vanished before it could record a failure.

The causal defect was WAL `synchronous=NORMAL` on the acknowledged final
domain-plus-journal commit. The mandatory regression first failed with the
actual dispatch at NORMAL. The narrow fix keeps SUBMITTED, EXECUTING, recovery,
and failure finalization at NORMAL, elevates only the atomic domain-plus-
COMMITTED-journal transaction to FULL, restores NORMAL before acknowledgement,
and fails closed if the transition cannot be made or restored.

Final verification is green across 539 unique tests: 96 exact-v6 schema, 62
persistence, 117 crash/recovery/runtime, 141 concurrency/telemetry, and 123
broader config/engine/safety tests, with zero failures, errors, or skips. The
post-fix working-tree identity is
`d9cf35ce94c9cca1e94e562e5bf95c858b6589ae80ce3e914b4af0d4fb4a376a`.

## Evidence preservation note

The original post-reboot Successor24 SHM was 294,912 bytes with SHA-256
`b9c048de34d844a2381155656d249036a85a52b60cb9fd06bf1600e44d7aa3ae`.
The first query-only repository monitor caused SQLite to rebuild that SHM to
32,768 bytes with SHA-256
`430edda9cce8a6ac04aa0873539c2dffd26234f5a3b3cc084c7176d96504990d`.
The DB and WAL bytes, hashes, and mtimes did not change. Both SHM identities
are recorded; nothing was restored or concealed.

Recovered Successor24 DB SHA-256 is
`fa79e3bcda893fd420092dfff370b768f0bb25277771bf1e6f8ca6f056484fb0`;
WAL SHA-256 is
`196f6868cc2ae9045338b11a7077732f6ddb0c82cdac27dfd70157841cec1841`.

## Current readiness and safety

- Ensemble: 56 rank-1 rows, 56 markets, 9 positive, 24 negative, 23 unlabeled.
- Model: 55 rank-1 rows, 55 markets, 9 positive, 23 negative, 23 unlabeled.
- Phase One was not ready; no calibration/tournament/holdout/Phase-Two work ran.
- `LIVE_ENABLED=false`; `REAL_ORDERS_POSSIBLE=false`; wallet signing,
  authenticated trading, placement, and cancellation are unavailable.
- Kill switch remains engaged; no Phase Two or Phase Three; `V4-HO-001`
  remains nonexistent/unconsumed.
- Authoritative v5 was independently rehashed after reboot and is exactly the
  pinned DB/WAL/SHM identity, with its WAL still empty.

## Exact next action

Preview/apply v1 and nonce `f42ba7127c164e678d905490bdee7b6d` remain terminal
refused. Correction preview v2 passed exactly once and its apply ran exactly
once under nonce
`506642682e50407c86f458976726bf8b`, timestamp `1787661736890`, and stable
precondition SHA-256
`b27ce7fc70dfb6e210295e94a341e2c114facac753599d6776cba1ec01786759`.
The terminal closure is
`operator\successor24_host_loss_closure_v2.json`, SHA-256
`72957f833524a2bdd2d724e1c792c945d9984eb93b60570bc0d50075c1424f12`.
The Successor25 create-once source-seal authority is prepared at
`D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor25_20260825T1257Z`.
It binds manifest SHA-256
`91f5c2bb8bb44096071752aac783e5115aad6e09caa0f946469dedbaa8a6bafe`,
tested tree SHA-256
`7d08828b6a5cb3a04001e47ecb0dda624dd6e632ea5b0345f888544eab68bae7`,
and an exact 4,208-test inventory. Invoke `run_full_suite_once.py` exactly once.
Never rerun this seal identity or relaunch/reuse Successor24.

Do not enter calibration, tournament, Phase Two, holdout, Phase Three, live or
authenticated trading, signing, orders/cancels, or authoritative-v5 mutation.
