# Poly Alpha durable status

Updated: `2026-08-25T12:24:09.6209010+07:00`

## Current boundary

`SUCCESSOR22_TERMINAL_FORENSIC_CLOSURE_COMPLETE_ROOT_CAUSE_TDD_NEXT`

Successor22 consumed exactly one owner-authorized, session/nonce/PID-bound
normal-stop request and ended gracefully. The original `1964 -> 8536` process
pair is absent; the request and process lock are absent; the Phase-One lease is
released; and the session ended at `1787635017509` with reason
`graceful_stop`. Successor22 is permanently forensic and ineligible and must
never be relaunched, repaired, replayed, rehabilitated, pooled, or reused.

Terminal journal: 117,303 total, 117,302 committed, one preserved failed
command, zero unresolved, and a committed session-terminal command. Telemetry
drained successfully with zero CEX or Polymarket discard, zero queue overflow,
zero accounting mismatch, and the already-bound two lost critical rows
preserved. The terminal acquisition contains 9,865 capsules, 18,339
predictions, 1,360 markets, and 1,257 outcomes; economic, calibration-artifact,
tournament, and holdout rows remain zero.

The failed command is
`bc0614b5d1494a91854863b9ef527f13:000000093248:persist-execution-book-bundle`
(payload SHA-256
`27b4f48a4f196252ffcf091d6cc3e52c4b028bfdf0f4ff65cafa9f846fd46b04`).
It failed after three attempts because the same source-event identity was
stored as `RAW` and offered as `PERMANENT`.

Forensic closure:

- external artifact:
  `D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z\operator\successor22_terminal_forensic_closure.json`
- SHA-256: `6b59cc1694bea110e54d926422293e68f2536991b347e5ef58d58ca25eb4d346`
- DB verification SHA-256:
  `34ccb53b35a130ea32a1117c9ed9f64bb768db9a5e1e58710c24973378bfae82`
- SQLite `quick_check=ok`; foreign-key violations: zero.

Authoritative v5 remains byte-identical. `LIVE_ENABLED=false`,
`REAL_ORDERS_POSSIBLE=false`, signing/auth/order/cancel paths unavailable,
kill switch engaged, Phase 3 absent, and
`V4-HO-001=NONEXISTENT_UNCONSUMED`.

Exact next action: trace the complete RAW-to-PERMANENT producer and retry path,
establish the canonical retention invariant from source, then add and observe
the mandatory pre-fix RED regression before any production change.
