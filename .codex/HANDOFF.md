# Poly Alpha durable Codex handoff

Checkpoint: 2026-08-23T20:51:55+07:00

Successor19 is the sole eligible acquisition and remains in its one original,
shadow-only Phase-One session. Do not stop or relaunch it. Acquisition
`V4-PR-001-PROSPECTIVE-SUCCESSOR19-20260823T1225Z`, session
`ee3a18f37f98459ebc71a7126b29878a`, phase-one nonce
`0b9c9b75c9f149798948449673963b30`, launcher/runtime PIDs `18280/2264`, source
tree `b202ebfd07c92c9e57e3dd77fc5d983792beb765a868884d5c3296ba4f9d7fcc`.
The 4,168-test source seal, materialization nonce
`260277f15ab4434a9082d11774ab2b22`, and phase-one nonce are consumed. Reserved
phase-two nonce `c66f6a80e1624b4a9eff5b4fa3450e7a` is unconsumed and unauthorized.

The independent guardian is active under Codex session `35793`, process chain
`9480 -> 14588 -> 16160`, using `watch_phase_one.py --duration-s 3600
--interval-s 30 --snapshot-s 300`. The five-minute heartbeat automation
`poly-alpha-exact-v6-acquisition-guard` is ACTIVE and must preserve watcher
coverage without relaunching the runtime.

Latest durable clean snapshot:
`D:\poly_alpha_prospective_exact_v6_successor19_20260823T1225Z\operator\guardian_snapshot_1787493105462.json`,
SHA-256 `72ff6a0a176aaf0032620645440b715798ea2acfad023cbe75f078e1b39b5876`.
It contains 2,391 complete capsules/candidates, 4,761 predictions, 181 markets,
145 outcomes, journal `COMMITTED=16,676`, and zero incomplete commands. There
are zero critical failures, incomplete/lost evidence, overflow, source discard,
trade/PnL, calibration, tournament, or holdout effects. The live observation
immediately afterward had 16,699 submitted, 16,696 committed, three pending,
one in flight, zero failed, zero reconciliation blockers, zero accounting
mismatch, and zero unexpected loss.

Readiness is not met. Ensemble: 163 rank-1/unique, 77 positive, 68 negative,
18 unlabeled. Model: 156 rank-1/unique, 73 positive, 65 negative, 18 unlabeled.
The exact OOS endpoint is `1787530026799`. Each target independently requires
rank-1 >=300, unique markets >=300, positive >=60, negative >=60, and unlabeled
=0; capsules must be >=300. Every condition is conjunctive.

Telemetry is operationally degraded under active policy sampling because the
queue is accumulating during uncontrolled overload. The data-safety reasons
are recovered historical deadline/sink events only; queue overflow,
acknowledgement loss, critical loss, accounting mismatch and unexpected loss
remain zero. This is accepted by the frozen guardian but is not proof of full
operational recovery; do not erase the distinction.

Safety is fail-closed: dry-run true; live, real orders, wallet signing,
authenticated client, live adapter, order placement and cancellation false;
kill switch engaged; no Phase 3; V4-HO-001 nonexistent/unconsumed. Authoritative
v5 remains write-prohibited. Its last independently verified hashes are DB
`92ee57b53468e11bdec2dd9082d3451f980d596212e10198ffd9bf8db7bd94d2`, empty
WAL `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`,
SHM `fd4c9fda9cd3f9ae7c962b0ddf37232294d55580e1aa165aa06129b8549389eb`.
They were not expensively rehashed for this quota-preservation checkpoint.

Prepared but never executed:

- `operator\phase_one_ready_stop_once.ps1`, SHA-256
  `41720472f879e50f9c8f952af6c213cabed9e499942f160c0b725e17c213412a`,
  PowerShell parse PASS.
- `operator\create_or_verify_terminal_snapshot.py`, SHA-256
  `0ec69a54da8d1dc38d2c9025fbfaf2f4da92568d83e731c989c0b6098f655bda`,
  AST parse PASS.
- No `stop.request`, ready-stop authority, or stop-created evidence exists.

Graceful stop is authorized only after the frozen watcher creates a genuine
`POLY_ALPHA_SUCCESSOR19_PHASE_ONE_READY_OBSERVATION_V1` proving OOS end, both
complete target rows, the same protocol/session/nonce/PID/source/database,
fail-closed safety, and zero binding loss. Then run the stop script in Validate
mode, reconcile once, Execute once, and allow graceful drain. Never kill.

Post-stop order is fixed: prove drain/session/lease closure; create and verify
one WAL-aware immutable terminal snapshot; reverify v5 and V4-HO-001; run one
lease-owned journaled calibration finalization; require two verified PASS
artifacts and protocol consumption; register the tournament definition; only
then consider the reserved Phase-Two authority and proceed to evaluation.

Successors 1-18 remain permanently ineligible. Successors17/18 failed binding
critical-evidence-incompleteness gates and must never be repaired, relaunched,
finalized, registered, evaluated, pooled, or reused.

Next action: read this checkpoint plus exactly one latest guardian snapshot and
process census. If healthy and not ready, leave successor19 untouched and
continue bounded independent watcher coverage. Act only on a valid ready
artifact or a binding guardian failure; preserve first evidence in either case.
