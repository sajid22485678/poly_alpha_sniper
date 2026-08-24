# Poly Alpha durable Codex handoff

Checkpoint: 2026-08-24T10:50:03+07:00

Successor20 source is now frozen at
`D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor20_20260824T0355Z`
with tested tree
`e532aa398eca8154c3df5603242806a6d089651e54033829a60f0e464576543b`
and manifest
`cad7de6fb24c2d41f582c1c4d629f27000ca60b1f40502f35f589850e122f38f`.
Its full-suite authority is consumed PASS: launch count one, exit 0,
4,169/4,169, zero failure/error/skip, JUnit
`3afbd99d1df3158768d47e8986bf0002727df739c4a4f79972717c04af5386ff`,
exact post-tree equality and unchanged authoritative v5. Never rerun it. The
exact next action is one new successor20 exact-v6 materialization and cold
verification under a fresh nonce/root.

Successor19 is terminal, permanently failed, and ineligible. Its sole session
`ee3a18f37f98459ebc71a7126b29878a` and Phase-One nonce
`0b9c9b75c9f149798948449673963b30` ended without a valid ready observation.
PIDs `18280/2264` are absent, session/lease closure is durable, final journal is
26,391/26,391 committed, and the exact-v6 database passes integrity. Its 33,815
lost accepted ingress events are binding and cannot be cured. Never execute its
prepared normal-stop path and never reuse any successor19 identity or population.

The preserved fatal diagnostic at
`D:\poly_alpha_prospective_exact_v6_successor19_20260823T1225Z\runtime\fatal_diagnostic.json`
has SHA-256
`228abb43b9ba118571e70ee9aba364aaeb292d9aa24c3e4151ef78b3a2b7671f`.
It proves sequential defects: saturation reached queue capacity 50,000 during
measured loop lag and the non-backpressured callback discarded 33,815 accepted
events; the queue then drained. Later the supervisor's stop-request read expired
behind the shared FIFO I/O lane and propagated as fatal while worker and
persistence health remained valid.

Two deterministic regressions were observed RED before correction and are now
GREEN. Accepted evidence uses bounded cancellable backpressure. A transient
stop-poll timeout/queue-full is retried only when worker health is RUNNING;
dead/not-running state remains fatal. Focused validation is green: 2/2, 36/36,
41/41, and 82/82 across regression, ingestion/worker, engine, and runtime.

Next action is ledger reconciliation followed by one new, non-overwrite
successor20 source manifest and exact-once full-suite seal. Only exit 0, zero
failure/error/skip, exact post-tree equality, and unchanged v5 may authorize a
fresh successor20 materialization and shadow-only Phase-One launch.

Successors1-19 remain unavailable as dictated by terminal history. V4-HO-001
remains nonexistent/unconsumed. Authoritative v5 stays immutable. Live/auth
trading, signing, real orders/cancels, Phase 3, failed-cohort pooling, and early
calibration/tournament/holdout work remain prohibited.
