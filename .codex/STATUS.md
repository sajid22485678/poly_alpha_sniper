# Poly Alpha durable status

Updated: `2026-08-25T22:07:36.0027011+07:00`

## Current boundary

`SUCCESSOR26_SOURCE_SEAL_PREPARATION_READY`

Successor25 is permanently ineligible. Its one Phase-One identity was session
`50ac10343a4044fa8f2fe71113ac014f`, nonce
`8ab5f199f40b47b5963944e4bc356f59`, launcher/runtime PID pair
`8320 -> 17108`, and guardian PID pair `5904 -> 4480`. It must never be
relaunched, finalized, calibrated, pooled, repaired in place, or reused.

The first binding guardian failure is
`D:\poly_alpha_prospective_exact_v6_successor25_20260825T1324Z\operator\guardian_failure_1787665558289.json`,
SHA-256 `312ae0dfabc19eb4e6e98d57cc8dbf1e89f7a0fa1b26ca5b22ed9644064d3df2`.
It recorded two unconfirmed critical commands, one in flight, critical state
`FAILED`, and `DEGRADED_PERSISTENCE`. There were no command failures, journal
losses, accounting mismatches, queue overflows, source discards, safety
violations, or holdout/phase violations. Later recovery does not cure this
first binding result.

The v1 stop execution refused before any mutation and is terminal. Distinct v2
executed exactly once and produced the PID/nonce/session-bound graceful stop.
Both runtime PIDs and both guardian PIDs are absent, the Phase-One lease is
released, and no stop request or process lock remains.

The WAL-aware terminal snapshot is
`audit\critical_incomplete_breach_terminal\consistent_snapshot.db`, SHA-256
`ee32efe64fe910219abf7c0a1c0392a8f8152d91a2860d8fc77176d2ca080d15`.
It passes quick-check and foreign-key checks and contains 1,517 committed
commands with zero incomplete/failed rows; 115 capsules, 230 predictions, 31
markets, and 2 outcomes; zero calibration artifacts, tournament, holdout, or
economic rows. Terminal reconciliation SHA-256 is
`a024bfae2f621f583e2f6645c6c5ac8412cd7056e9e8ba001c9faaa68b6859de`.

The causal boundary is not a checkpoint collision: command 830 was still
`EXECUTING` at the binding observation and later committed after 10,556 ms
execution / 10,697 ms acknowledgement latency; no registered checkpoint
overlapped it. The mandatory RED is preserved and the causal correction is now
green: accepted work has one monotonic per-envelope lifetime and uses the exact
configured acknowledgement deadline, while genuinely idle writers retain the
short heartbeat boundary. A dead/stopped writer, unknown/overdue age, or actual
timeout remains fail-closed; a later commit cannot clear a timeout latch.

The accepted-command registry now publishes before scheduler visibility, uses
unique envelope sequence identity, and supplies atomic count/age metrics.
Shutdown uses one terminal fence after all accepted cross-key work, completes
accepted full-queue handoffs, quiesces background producers, and requires the
writer to close before STOPPED or runtime-lock release. Successor26 must use a
standalone monotonic guardian; the predecessor wall-clock guardian is forbidden.

The deliberate source tree remains preserved at branch `master`, HEAD
`d3364f219feb37a09a547ff1daba6f0f96377fe4`, index tree
`700307bdbc9a4fdab7615d79eea83fe1bf6463cb`, with zero staged paths, 57
tracked modifications, and 113 untracked paths. Successor25’s tested tree is
`7d08828b6a5cb3a04001e47ecb0dda624dd6e632ea5b0345f888544eab68bae7`;
its single full suite passed 4,208/4,208.

Authoritative v5 remains byte-identical. Safety is fail-closed:
`LIVE_ENABLED=false`, `REAL_ORDERS_POSSIBLE=false`, signing/authenticated
trading/orders/cancels unavailable, kill switch engaged, no Phase Two/Three,
and `V4-HO-001` nonexistent/unconsumed.

Exact next action: prepare one fresh Successor26 source-seal root and run the
repository full suite exactly once. Preserve the identity on any failure or
ambiguity. Only a green exact post-tree/v5 verification may authorize fresh
materialization, cold verification, one Phase-One launch and one guardian.
