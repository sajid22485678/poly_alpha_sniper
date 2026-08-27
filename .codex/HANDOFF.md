# Poly Alpha handoff

Canonical evidence: `.codex/SUCCESSOR33_TERMINAL_EVENT_LOOP_LAG_FAILURE.json`.

Successor33 session `2496e04267c7462bbe37c569bc01edcd` and Phase-One nonce `35b603969c314957855eb2f6ebceb9b1` are consumed and MUST NOT RELAUNCH. Guardian coverage ended at its first binding `DEGRADED_EVENT_LOOP_LAG` failure. The exact graceful stop ran once and terminal reconciliation is clean.

The causal defect is an asyncio fairness failure under a continuously nonempty accepted-event queue. A fail-first regression reproduced multi-second starvation in both ingestion consumers. The minimal fix yields after at most 64 events or 10 ms; the ingestion, engine, and persistence-integration surfaces are green.

Exact next action: qualify corrected source tree `10025a6353312e23fa87db11c37e5feab4d4f901058312f255d704e47ffb5936` once. If and only if the seal passes with post-tree equality and unchanged v5, materialize and launch a distinct Successor34 with fresh nonces/session and a fresh independent guardian.

Forbidden: any Successor33 relaunch or guardian restart, consuming its reserved Phase-Two nonce, calibration/tournament/holdout, Phase Three, authoritative-v5 mutation/cutover, or any live/signing/authenticated/real-order capability.
