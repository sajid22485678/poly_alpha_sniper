# Poly Alpha status

- State: `COMBINED_POST_OOS_FINAL15_SOURCE_SEAL_FAILED`.
- Final14 remains terminal/ineligible; its exact graceful stop and critical-priority causal fix are immutable.
- Final15: exact suite ran once on tree `788ef689…`; 4,285 passed / 1 failed, zero errors/skips, tree exact, v5 unchanged.
- Failure: `test_async_execute_waits_out_maintenance_before_journal_admission` used a fragile 50 ms post-admission acknowledgement margin and leaked its worker after complete JUnit output. Final15 is `MUST_NOT_RERUN` and materialization is unauthorized.
- Correction: the same maintenance invariant now uses a 500 ms post-admission margin while maintenance is held for 600 ms; five focused repetitions and the 83-test persistence/write-gate suite pass.
- Next: prepare one distinct Final16 exact source seal. Only an exact PASS may authorize a new Phase-Two identity.
- Safety: shadow only; no live/signing/authenticated/order capability; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
