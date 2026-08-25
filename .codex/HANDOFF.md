# Poly Alpha continuation handoff

Updated: `2026-08-25T18:53:36.4104535+07:00`

## Durable boundary

`SUCCESSOR24_HOST_LOSS_INTEGRITY_FAILURE_IDENTIFIED`

Canonical primary-evidence reconciliation is in
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
and the old PIDs were not reused. The acquired Phase-One lease and sole runtime
session remain stranded/open on disk. No stop, terminal, recovery, readiness,
or guardian-failure artifact existed at recovery census.

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

The causal defect is narrowed to WAL `synchronous=NORMAL` on the acknowledged
final domain-plus-journal commit. A fail-first regression has not yet been
written, and production code has not been changed.

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

Write the smallest fail-first power-loss durability regression and observe the
mandatory RED without production changes. Then implement the narrow final
acknowledged-commit durability barrier while preserving the normal baseline
policy, validate affected concurrency/crash/recovery/safety behavior, and use
one exact-identity authority to canonically close Successor24. Only after the
fix and closure are proven may a fresh successor be source-qualified,
materialized, cold-verified, and launched exactly once.

Do not enter calibration, tournament, Phase Two, holdout, Phase Three, live or
authenticated trading, signing, orders/cancels, or authoritative-v5 mutation.
