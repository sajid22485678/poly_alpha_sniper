# Poly Alpha durable status

Updated: `2026-08-25T11:31:26.2198199+07:00`

## State

`SUCCESSOR22_POST_OOS_FAILED_RUNTIME_STILL_RUNNING_OWNER_STOP_AUTHORITY_REQUIRED`

The bounded same-identity post-OOS review classified Successor22
`POST_OOS_FAILED`. Both target populations pass every numerical quota and
their repository-defined lineage recomputations pass, but the cohort is
inadmissible because its frozen guardian recorded a binding persistence breach
and the same session later finalized one critical evidence command as failed,
with two critical-evidence rows recorded lost.

Full review evidence:

- `.codex/SUCCESSOR22_POST_OOS_REVIEW.json`, SHA-256
  `e7898c69889b86a0b928f39a949b08f64781cf2de6cc9feb603225283e8226f8`
- `.codex/SUCCESSOR22_POST_OOS_REVIEW.md`, SHA-256
  `6a3170a8eabcf612e1931510576c3f64f3b69d46dd0dd336156cd6d02138ef78`
- prior bridge commit: `d2443a753dae4f76e247ca2979bf9af8e8049ac9`

## Same identity

- Source tree: `386991dc9c001ddf54290c2092e09725be7f5ad5a5f2d51bc901322cba2d23cc`,
  independently recomputed equal to the current deliberate dirty source tree.
- Acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR22-20260824T151804Z` at
  `D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z`.
- Materialization nonce: `6871292fc9674d9196c4660a666c84e0`.
- Consumed Phase-One nonce: `99d60fab076e4bcfae6e3a8831d6393a`.
- Session: `bc0614b5d1494a91854863b9ef527f13`.
- Original process chain `1964 -> 8536` remained present at
  `2026-08-25T11:30:34.9928021+07:00`; exactly one root, lease and runtime
  session exist. No relaunch or identity fork occurred.
- Runtime remains open and `DEGRADED_PERSISTENCE`, latched on
  `V4EvidenceConflict`. No stop request was created.

## Binding result

Guardian PIDs `11624/13504` are absent. Its first failure is
`D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z\operator\guardian_failure_1787586120094.json`,
SHA-256 `af8d57a9cdc8788c2e320bfd045e2ebeeb403a369c9a4eef8f88474ccc8cd313`.
At `1787586120094` it froze three violations:

- `runtime_state:DEGRADED_PERSISTENCE:True`
- `critical_health:FAILED:False`
- `telemetry:critical_evidence_incomplete_count:1`

No Phase-One ready observation exists. Later, command
`bc0614b5d1494a91854863b9ef527f13:000000093248:persist-execution-book-bundle`
failed before OOS end after three attempts because the same source event
collided with `retention_class` changing from `RAW` to `PERMANENT`. Its payload
SHA-256 is
`27b4f48a4f196252ffcf091d6cc3e52c4b028bfdf0f4ff65cafa9f846fd46b04`.
Current telemetry records two lost critical-evidence rows.

## Review snapshot

The query-only snapshot at `1787632063381` contains 9,570 capsules, 18,339
predictions, 1,280 markets, 1,184 outcomes and 109,790 journal commands:
109,788 committed, one failed and one ordinary `SUBMITTED` in-flight command.
Duplicate command IDs are zero. Queue overflow, CEX discard, Polymarket
discard, reconciliation mismatch and unexpected-loss accounting are zero.

SQLite `quick_check=ok`, foreign-key violations are zero, and exact-v6 schema
fingerprint
`3289a18ccc9f6287360fcab9338e5e36d4070532f27b0d12377f9555bd7954dd`
matches. Structural integrity does not cure the semantic persistence failure.

| Target | Rank-1 | Unique markets | Positive | Negative | Unlabeled | Quota | Admissible |
|---|---:|---:|---:|---:|---:|---|---|
| Ensemble | 1,120 | 1,120 | 543 | 577 | 0 | PASS | FAIL |
| Model | 1,114 | 1,114 | 541 | 573 | 0 | PASS | FAIL |

Both target manifests passed independent lineage and temporal recomputation;
zero labels are non-pre-resolution, and all selected predictions lie inside
`1787585613004..1787628813002`. Numerical quotas have no deficit.

## Safety and exact next authority

`LIVE_ENABLED=false`, `REAL_ORDERS_POSSIBLE=false`, wallet signing,
authenticated trading and real order/cancel paths unavailable, kill switch
engaged, Phase 3 absent, authoritative v5 byte-identical, and
`V4-HO-001=NONEXISTENT_UNCONSUMED`. Calibration, tournament, Phase Two and
holdout remain untouched. Reserved Phase-Two nonce
`9f22eca6d2c548c8bcd6b8d687a3f261` is unauthorized and unconsumed.

Exact next admissible gate: obtain separate owner authorization for one
create-once normal-stop request bound to session
`bc0614b5d1494a91854863b9ef527f13`, nonce
`99d60fab076e4bcfae6e3a8831d6393a`, launcher PID `1964` and runtime PID `8536`,
followed only by terminal snapshot, journal reconciliation and forensic
closure. It must not imply replay, rehabilitation, replacement or any later
phase.

`MUST_NOT_RELAUNCH`. Codex active work stops after this review checkpoint.
