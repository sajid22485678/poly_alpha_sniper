# Codex progress bridge

Updated: 2026-08-24T10:56:20+07:00

Successor20 materialized exactly once and cold-verified PASS under materialization
nonce `531571f5ec6b412fb0ff03f10f57f0cf`. The database is empty exact-v6,
quick/FK/schema clean, has zero research/tournament/holdout/trade state, and v5
remains byte-identical. Materialization manifest SHA-256 is
`ccffce5672436041f7f76df1e7cb0b21d2b07452d5fdc27f9b3dfc24557f25f4`.

Phase One launched exactly once under nonce
`13a4198c8f96427582a3e8c7e7b8bb4c`, session
`27cad22fef8f4d43a68034c7e9f18af2`, launcher/runtime PIDs `14236/11508`.
It is RUNNING, shadow-only, live-disabled, unsigned, order-incapable and
kill-switched. Never relaunch it. Guardian coverage extends across OOS under
background chain `7424 -> 6692` for 46,800 seconds with 30-second checks and
300-second snapshots. Output is `operator\guardian_oos.stdout.txt`; stderr is
empty. Latest durable snapshot `guardian_snapshot_1787546093021.json` has
SHA-256 `90e752c3caee96dd34d336ffe2cf5e160792733a2d80968359340637f25d06c0`
and records 151 capsules, 300 predictions, 48 markets, zero outcomes, 1,324
committed, zero incomplete/loss/overflow/failure. Reserved Phase-Two nonce
`d8c06d01b91940889705983966d071a4` remains unauthorized.

Successor20 source seal is prepared and source-frozen at
`D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor20_20260824T0355Z`.
Tested-tree SHA-256 is
`e532aa398eca8154c3df5603242806a6d089651e54033829a60f0e464576543b`;
manifest SHA-256 is
`cad7de6fb24c2d41f582c1c4d629f27000ca60b1f40502f35f589850e122f38f`.
The create-once full-suite authority is consumed PASS: 4,169/4,169, exit 0,
zero failure/error/skip, JUnit SHA-256
`3afbd99d1df3158768d47e8986bf0002727df739c4a4f79972717c04af5386ff`,
and exact post-tree equality. Authoritative v5 is independently byte-identical;
materialization is authorized. Never rerun this seal identity.

Successor19 is permanently FAILED / INELIGIBLE and must never be relaunched,
finalized, calibrated, registered, evaluated, pooled, repaired, or reused.
Session `ee3a18f37f98459ebc71a7126b29878a`, Phase-One nonce
`0b9c9b75c9f149798948449673963b30`, launcher/runtime PIDs `18280/2264` are
terminal and absent. Session ended `1787497584655`; lease released
`1787497585300`; no ready observation exists.

Binding result: `fatal_V4WorkerJobTimeout` plus 33,815 accepted Polymarket
events discarded by `polymarket_ingest_queue_overflow_fail_closed`. Final
journal is 26,391 committed, zero incomplete/failed/reconciliation blockers;
database integrity is OK. Final readiness was ensemble 275 rank-1/unique,
135 positive, 122 negative, 18 unlabeled; model 264 rank-1/unique, 129
positive, 117 negative, 18 unlabeled.

Primary evidence proves two sequential defects. A short saturation episode
reached ingest high-water/capacity 50,000 during measured event-loop lag up to
5,307 ms; the callback used `put_nowait` and discarded rather than applying
backpressure. Depth later returned to zero. Separately, the supervisor's
two-second stop poll queued on the shared FIFO runtime-I/O lane, expired while
the worker remained healthy, and the uncaught timeout terminated the runtime.
Fatal diagnostic SHA-256 is
`228abb43b9ba118571e70ee9aba364aaeb292d9aa24c3e4151ef78b3a2b7671f`.

Fail-first regressions reproduced both invariants. The minimal correction now
uses cancellable bounded backpressure for accepted source evidence and retries
only transient stop-probe timeout/queue-full results while worker health proves
RUNNING; a dead worker still fails closed. Focused results: 2/2 regressions,
36/36 worker+Polymarket tests, 41/41 engine tests, and 82/82 runtime/config/fatal
diagnostic tests pass.

Next action: preserve the same runtime/session/nonce and bounded guardian
coverage. Do not finalize or stop before OOS and every conjunctive readiness
gate; fail closed on any first binding breach without relaunching.

Independent work is blocked only on prospective market time/data: OOS ends at
`1787588920488`; each target requires rank-1/unique >=300, positive/negative
>=60 and unlabeled=0. Runtime/guardian continue independently. Resume from
current disk authority, never from these counters.
Safety remains live-disabled, unsigned, unauthenticated, order/cancel-incapable,
kill-switched; Phase 3 prohibited; V4-HO-001 nonexistent/unconsumed.
