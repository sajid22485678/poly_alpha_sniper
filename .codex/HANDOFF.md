# Poly Alpha continuation handoff

## Current boundary

`POST_OOS_FAILED`

Successor22's numerical OOS readiness and target lineage both pass, but the
cohort is permanently inadmissible. Its immutable guardian failure at
`1787586120094` records a binding persistence-incompleteness breach, and its
same Phase-One session later finalized one critical evidence command as failed
with `V4EvidenceConflict`, producing two recorded lost critical-evidence rows.

Authoritative review records:

- `.codex/SUCCESSOR22_POST_OOS_REVIEW.json`, SHA-256
  `e7898c69889b86a0b928f39a949b08f64781cf2de6cc9feb603225283e8226f8`
- `.codex/SUCCESSOR22_POST_OOS_REVIEW.md`, SHA-256
  `6a3170a8eabcf612e1931510576c3f64f3b69d46dd0dd336156cd6d02138ef78`

The exact source/acquisition/materialization identities remain valid and the
current source bytes still equal tested tree
`386991dc9c001ddf54290c2092e09725be7f5ad5a5f2d51bc901322cba2d23cc`.
No source qualification was rerun.

## Runtime ownership

- Acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR22-20260824T151804Z`.
- Materialization nonce: `6871292fc9674d9196c4660a666c84e0`.
- Consumed Phase-One nonce: `99d60fab076e4bcfae6e3a8831d6393a`.
- Session: `bc0614b5d1494a91854863b9ef527f13`.
- Original process chain: `1964 -> 8536`, both still present at the final
  bounded process census; exactly one root, lease and runtime session exist.
- Runtime is open and `DEGRADED_PERSISTENCE`, with latched
  `V4EvidenceConflict`. `MUST_NOT_RELAUNCH`.
- Guardian PIDs `11624/13504` exited on their first binding failure. No ready
  observation exists.

## Frozen review result

At the query-only snapshot beginning `1787632063381`:

- acquisition: 9,570 capsules / 18,339 predictions / 1,280 markets / 1,184
  outcomes;
- journal: 109,790 total / 109,788 committed / one failed / one ordinary
  `SUBMITTED` in-flight / zero duplicate command IDs;
- two true lost critical-evidence rows;
- zero queue overflow, CEX discard, Polymarket discard, reconciliation mismatch
  or unexpected-loss accounting;
- `quick_check=ok`, zero foreign-key violations and exact-v6 fingerprint match;
- zero business/economic rows, calibration artifacts, tournament state or
  holdout rows.

Independent target results:

- Ensemble: 1,120 rank-1, 1,120 unique markets, 543 YES, 577 NO, zero
  unlabeled—quota PASS, admissibility FAIL.
- Model `paired_book_fair_value_parity`: 1,114 rank-1, 1,114 unique markets,
  541 YES, 573 NO, zero unlabeled—quota PASS, admissibility FAIL.
- Both repository-defined manifest recomputations pass temporal, resolution,
  source, cohort and uniqueness lineage. OOS bounds are
  `1787585613004..1787628813002`.

## Safety and exact next action

Safety remains fail-closed: `LIVE_ENABLED=false`,
`REAL_ORDERS_POSSIBLE=false`, signing/authenticated trading/order/cancel paths
unavailable, dry-run true, kill switch engaged, no Phase 3, authoritative v5
byte-identical, and `V4-HO-001=NONEXISTENT_UNCONSUMED`. Reserved Phase-Two
nonce `9f22eca6d2c548c8bcd6b8d687a3f261` remains unauthorized/unconsumed. No
calibration, tournament, Phase Two or holdout action occurred.

Exact next admissible gate: separate owner authorization for one create-once
normal-stop request bound exactly to session
`bc0614b5d1494a91854863b9ef527f13`, nonce
`99d60fab076e4bcfae6e3a8831d6393a`, launcher PID `1964`, and runtime PID
`8536`, followed only by terminal snapshot, journal reconciliation and forensic
closure. Do not infer authority for replay, repair, rehabilitation, replacement
or any later phase.

No stop request was created. No Successor22 relaunch was performed. Codex
active work stops after the durable bridge push.
