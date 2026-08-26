# Poly Alpha status

Successor29 remains terminal after its persistence acknowledgement failure; its 5,752-row write-amplification correction is verified.

The changed tree then passed the Successor30 exact source seal once: 4,252/4,252, zero failures/errors/skips, tested tree `1e608dd9611d8ef0c0dfa3b00ee4ee3d1c5f152df5db3160c4c9d15e07d02556`, JUnit `b853ca3b74d679572e950d9259b8ecc1fc25599491fbaf3d8516aa957dd05155`, exact post-tree equality, and authoritative v5 unchanged. The first Successor30 materialization operator failed before creating a root because of an invalid helper import and was retired.

Successor31 materialized once and passed independent cold verification. Its Phase-One runtime and guardian each launched once under session `626154fc536b4716919b7d58124b6f70`, nonce `1aeacf65f2224296b1056e7ff944d3e3`, runtime `15632 -> 18112`, guardian `17404 -> 10332`. Runtime and persistence were clean, but the five-minute guarded-start reserve expired before independent guardian admission: OOS prediction start `1787752157198`, guardian start `1787752211715`, a binding 54,517 ms unguarded interval. Successor31 is permanently `OOS_FAILED` and must not be relaunched.

The exact graceful stop drained cleanly. Terminal state is 1,223/1,223 committed commands, zero incomplete/failed/duplicates/loss/mismatch/overflow/discard, SQLite quick-check `ok`, FK zero, schema 6, released lease, and no remaining runtime/guardian process. Closure SHA-256 is `d0d39b7c9c0b910cfe5f05158f4be543fed86d589934793b97e68c0432a70a71`.

The causal protocol defect is the five-minute bootstrap reserve. On the immutable S27 corpus, a ten-minute start with the same +3h30 cutoff and +4h close yields 321/320 unique fully labeled rows for ensemble/model, still above every numerical threshold. Next: RED/GREEN the ten-minute guarded start, source-qualify the changed tree, then create a distinct successor.
