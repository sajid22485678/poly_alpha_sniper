# Codex progress bridge

Updated: 2026-08-24T10:56:20+07:00

Successor20 source seal is prepared and source-frozen at
`D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor20_20260824T0355Z`.
Tested-tree SHA-256 is
`e532aa398eca8154c3df5603242806a6d089651e54033829a60f0e464576543b`;
manifest SHA-256 is
`cad7de6fb24c2d41f582c1c4d629f27000ca60b1f40502f35f589850e122f38f`.
The create-once full-suite authority is prepared but not yet consumed at this
checkpoint. Do not mutate the repository until its run and post-tree verification
finish. Never rerun this identity after consumption.

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

Next action: finish durable ledger reconciliation, freeze the stable dirty tree,
then create and run one distinct successor20 exact-once full-repository source
seal. Materialize/launch only if that seal passes and v5 remains byte-identical.
Safety remains live-disabled, unsigned, unauthenticated, order/cancel-incapable,
kill-switched; Phase 3 prohibited; V4-HO-001 nonexistent/unconsumed.
