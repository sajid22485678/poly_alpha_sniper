# Poly Alpha durable status

Updated: `2026-08-25T22:34:20.2489449+07:00`

## Current boundary

`SUCCESSOR26_PHASE_ONE_AUTHORIZED_NOT_EXECUTED`

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

Successor26 seal root
`D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor26_20260825T1509Z`
passed its exact-once qualification at tested tree
`9a8ee9b4e6e3840775ab7e50576e545a3c67ef27c44233ef6edd2c2a21b25f21`.
Manifest SHA-256 is
`42f1d43bf674074793ce851d9362adf25cb853f3a06ae0871556f403f057a5d6`;
pytest launch count is one: 4,246 tests, zero failure/error/skip, exit 0. JUnit
SHA-256 is
`2b6194b5286de37c267fcb21b2a6eaae76ec33dd89ba238657dd2d928e306564`;
post-verification SHA-256 is
`50b1014cb90fc8df4c6310a03df726bde4a57506a7013f2c61cace7ce297dcd4`.
Exact post-tree equality and authoritative-v5 equality passed.

Fresh Successor26 acquisition identity
`V4-PR-001-PROSPECTIVE-SUCCESSOR26-20260825T1537Z` is now bound to root
`D:\poly_alpha_prospective_exact_v6_successor26_20260825T1537Z`. Its distinct
materialization, Phase-One, and reserved unauthorized Phase-Two nonces are
`fbc7431281e24a0c90d5ffb18e55e949`,
`6e80d3ea67cc4d12994c1f20427de260`, and
`2a8e2b8caabe4f22a725e7a5fd641362`. The target root remains absent and the
complete exact-v4 process census is zero. Authority SHA-256 is
`ad5b00bbec303b0a40a2ba383cfa288fc4c9bb002bd5d2d39cbfe526ce39bcd9`;
runner SHA-256 is
`eb72b12eab741668f0872addd9862d3955d6445aa5a9db00063d39314924a008`.

Materialization executed exactly once and exited zero. Manifest SHA-256 is
`bf70107800e0f623c23db9c3e733d2c52ee51eb876e43f6da3278f4c6245d1b6`.
Independent cold verification SHA-256 is
`568887dce73cf8bd2a470e8b56ac99b3ae1d55e780312b09ab95f86eefab8693`:
quick-check `ok`, zero foreign-key violations, exact v6 schema, zero research,
economic, tournament, calibration, or holdout rows, released lease, and v5
byte-identical. The self-contained monotonic guardian operator passed 25/25
tests exactly once; JUnit SHA-256 is
`6acc3ea0c654c64619b9c427cddc561615ab4fce13dd856fee39ae3e6376741b`.

Phase-One authority SHA-256 is
`4d479f74b431ab592d217a8e60e02c7975b397c4d09c754cc848f130332a9ace`;
launcher SHA-256 is
`1813fb30dcdd9444bb9997246eaffc19bb5e4843cf2fe472fe7ad62d762dbb39`.
Exact next action: execute that launcher once, never relaunch its nonce, resolve
the generated session/PID identity, and launch exactly one bound guardian.
