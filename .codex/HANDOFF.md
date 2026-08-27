# Poly Alpha durable handoff

## Current decision

**SUCCESSOR33 IS RUNNING CLEANLY UNDER ITS PREREGISTERED OOS PROTOCOL.**

Successor32 remains immutable. Its Phase-Two nonce `793ff57b339344768a2fab76a6f318b5` was consumed by a startup failure before any Phase-Two session row or database mutation. The failure is forensic and must not be relaunched.

The root cause was a timeout-domain defect: exact-v6 open validation took `129.5945s` and its startup census `22.7788s`, but engine startup reused the `15s` post-admission command acknowledgement deadline. The corrected source separates a bounded `300s` startup deadline while retaining the `15s` acknowledgement gate; strategy and prediction behavior are unchanged.

## Successor33 authority

- Source tree: `7f34c14ef1a4016f576b517f28d912d569a76b98fd0a0f151932fee89bf3fb40`; `4,254 / 4,254` PASS exactly once.
- Acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR33-20260827T0559Z`.
- Session: `2496e04267c7462bbe37c569bc01edcd`.
- Phase-One nonce: `35b603969c314957855eb2f6ebceb9b1`.
- Runtime: `17424 -> 8696`; MUST NOT RELAUNCH.
- Guardian: `10384 -> 4348`; MUST NOT RESTART.
- OOS start: `1787811821267`; early review: `1787818421267`; frozen end: `1787823821267`.

Initial clean observations grew from 74 to 95 capsules and from 895 to 1,232 committed commands, with zero critical failure/loss, reconciliation mismatch, overflow, discard, or terminally incomplete work. The first guardian snapshot is `D:\poly_alpha_prospective_exact_v6_successor33_20260827T0559Z\operator\guardian_snapshot_1787811558694.json`, SHA-256 `dce6aa44c9e54663f023db6c0d4646ab3c8e57c0ef9c2d9b60f1f8ef84ed26e6`.

## Next action and safety

At or after `1787818421267`, perform one bounded same-identity review. Do not finalize before `1787823821267`. Do not stop/relaunch/restart healthy identities or enter calibration, tournament, Phase Two, holdout, Phase Three, v5 cutover, signing, authenticated trading, or real-order capability early.

Use `.codex/SUCCESSOR33_POST_OOS_RECOVERY_PROGRESS.json` as the canonical evidence index.
