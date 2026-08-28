# Poly Alpha status

- State: `PHASE_TWO_FINAL14_CAUSAL_FIX_VERIFIED`.
- Final14: nonce `95d21ed…`, session `798615e2…`, PIDs `2708→8072` are terminal and `MUST_NOT_RELAUNCH`.
- First binding result: `V4PersistenceTimeout`, followed by `V4PersistenceIdempotencyConflict`; later clean drain does not cure inadmissibility.
- Terminal proof: exact graceful stop, lease released, session ended, zero exact processes, 259/259 Final14 journal commands committed, quick-check/FK clean.
- Cause: obsolete write-gate fairness admitted telemetry after 16 rapid skips while critical work was still pending; repeated short telemetry transactions starved critical lock admission to 166.0 seconds.
- Fix: pending critical work now exclusively owns the next write-transaction admission; telemetry priority skips remain lossless and do not consume row retry budget.
- Validation: focused 3 passed; affected persistence/telemetry 186 passed; engine/runtime/store/schema/guardian 355 passed.
- Next: one fresh exact-once full-repository source seal. Only an exact PASS may authorize a distinct Phase-Two nonce.
- Safety: shadow only; no live/signing/authenticated/order capability; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
