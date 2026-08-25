# Successor22 post-OOS same-identity review

Review authority: owner packet SHA-256
`b076678a87c73de2c6a96e4fc89441f8d3babb911858b22f2b510b64b42f6150`.
Prior bridge commit: `d2443a753dae4f76e247ca2979bf9af8e8049ac9`.

## Classification

`POST_OOS_FAILED`

Both independently recomputed OOS target populations pass every numerical
quota and the repository-defined prediction-manifest lineage verifier accepts
every counted row. The cohort is nevertheless inadmissible because its frozen
guardian recorded a binding persistence failure at `1787586120094`, and the
same session later finalized one `EXECUTION_BOOK_EVIDENCE` command as failed
with `V4EvidenceConflict` before the OOS endpoint. Current telemetry records
two lost critical-evidence rows. Later drainage, complete labels, and structural
SQLite integrity cannot cure those immutable first results.

## Identity

- Tested source tree: `386991dc9c001ddf54290c2092e09725be7f5ad5a5f2d51bc901322cba2d23cc`;
  independently recomputed equal to current source bytes.
- Acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR22-20260824T151804Z` at
  `D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z`.
- Materialization nonce: `6871292fc9674d9196c4660a666c84e0`.
- Phase-One nonce/session: `99d60fab076e4bcfae6e3a8831d6393a` /
  `bc0614b5d1494a91854863b9ef527f13`.
- Exactly one acquisition root, one Phase-One lease, one runtime session and
  the original process chain `1964 -> 8536`; no relaunch or identity fork.
- At `2026-08-25T11:30:34.9928021+07:00`, PIDs `1964/8536` were present and
  the historical guardian PIDs `11624/13504` were absent.

## Guardian and binding evidence

- Last clean snapshot:
  `operator/guardian_snapshot_1787585848823.json`, SHA-256
  `81aba2daf324068ed7b9467ccd26801a0163803568645363b5fd58b642aa2e9d`.
- First failure: `operator/guardian_failure_1787586120094.json`, SHA-256
  `af8d57a9cdc8788c2e320bfd045e2ebeeb403a369c9a4eef8f88474ccc8cd313`.
- Frozen violations: `runtime_state:DEGRADED_PERSISTENCE:True`,
  `critical_health:FAILED:False`, and
  `telemetry:critical_evidence_incomplete_count:1`.
- No Phase-One ready observation exists. Guardian stderr is zero bytes.

The later failed command is
`bc0614b5d1494a91854863b9ef527f13:000000093248:persist-execution-book-bundle`,
submitted at `1787625805242` and finalized failed at `1787625808458` after
three attempts. Its payload SHA-256 is
`27b4f48a4f196252ffcf091d6cc3e52c4b028bfdf0f4ff65cafa9f846fd46b04`.
The collision differed in `retention_class` (`RAW` versus `PERMANENT`). It
occurred before frozen OOS end `1787628813002`.

## Bounded acquisition census

The query-only snapshot began at `1787632063381`:

- 9,570 capsules; 18,339 calibration predictions; 1,280 markets; 1,184
  outcomes.
- Journal 109,790 total: 109,788 committed, one failed, one ordinary
  `SUBMITTED` in-flight command, zero duplicate command IDs.
- Zero entries, positions, exits, PnL, calibration artifacts, tournament
  objects or holdout registrations.
- Zero queue overflow, CEX discard or Polymarket discard; reconciliation
  mismatch and unexpected-loss accounting are zero.
- Binding loss remains two critical-evidence rows; runtime state remains
  `DEGRADED_PERSISTENCE` with latched `V4EvidenceConflict`.
- All 9,570 capsules are sealed, point-in-time complete, lineage complete and
  bound to the tested tree.

SQLite `quick_check` is `ok`; foreign-key violations are zero; schema v6 and
managed fingerprint
`3289a18ccc9f6287360fcab9338e5e36d4070532f27b0d12377f9555bd7954dd`
match. This structural PASS does not override semantic evidence failure.

## Independent readiness

Thresholds for each target are rank-1 >=300, unique markets >=300, positive
>=60, negative >=60 and unlabeled =0.

| Target | Rank-1 | Unique markets | Positive | Negative | Unlabeled | Quota |
|---|---:|---:|---:|---:|---:|---|
| Ensemble | 1,120 | 1,120 | 543 | 577 | 0 | PASS |
| Model `paired_book_fair_value_parity` | 1,114 | 1,114 | 541 | 573 | 0 | PASS |

The repository-defined `calibration_prediction_manifest` recomputation passed
for both targets at cutoff `1787631997148`. Prediction bounds are
`1787585753105..1787628660679`, inside the frozen OOS interval
`1787585613004..1787628813002`; zero counted labels are non-pre-resolution.
Candidate session/cohort isolation, source-tree binding and holdout isolation
pass. Numerical readiness therefore has no deficit, but admissibility fails.

## Safety and decision

`LIVE_ENABLED=false`, `REAL_ORDERS_POSSIBLE=false`, signing/authenticated
trading/order placement/order cancellation unavailable, dry-run true and kill
switch engaged. Authoritative v5 remains byte-identical at DB/WAL/SHM hashes
`92ee57b53468e11bdec2dd9082d3451f980d596212e10198ffd9bf8db7bd94d2` /
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` /
`fd4c9fda9cd3f9ae7c962b0ddf37232294d55580e1aa165aa06129b8549389eb`.
`V4-HO-001` remains nonexistent/unconsumed; Phase Two nonce
`9f22eca6d2c548c8bcd6b8d687a3f261` is unauthorized/unconsumed; no later phase
was entered.

The next admissible gate is a separately owner-authorized, create-once normal
stop bound exactly to session `bc0614b5d1494a91854863b9ef527f13`, nonce
`99d60fab076e4bcfae6e3a8831d6393a`, launcher PID `1964` and runtime PID `8536`,
followed only by terminal snapshot, journal reconciliation and forensic
closure. It must not imply replay, rehabilitation, replacement, calibration,
tournament, Phase Two, holdout or Phase 3.

No stop request was created. No Successor22 relaunch was performed. No later
phase was entered.
