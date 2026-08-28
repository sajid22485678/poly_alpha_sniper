# Poly Alpha status

- State: `PHASE_TWO_FINAL7_LAUNCH_PREPARATION`.
- Tournament: empty expired parent preserved; child experiment `d8862bbd…` remains the exact preregistered ordinal-2 authority with no retuning.
- Failed launches: five distinct Phase-Two startup nonces are terminal and `MUST_NOT_RELAUNCH`; Final-6 nonce `d346e4fa…` failed before session admission.
- Final-6 effect: the existing cohort verification committed idempotently; release-risk verification failed; candidates stayed 2,849; runtime sessions stayed 1 historical; journal is 36,897 committed / 5 failed.
- Root cause: the release-risk builder alone still used the newer runtime source tree for immutable research policy identity. Cohort, config, calibration, and tournament identity correctly remain bound to the S32 materialization tree; executable provenance remains separately runtime-tree guarded.
- Fix state: fail-first test reproduced the exact wrong tree; the builder now uses the materialization tree for research policy identity. Final-7 tree `b6463d31…` passed 4,284/4,284 exactly once with exact tree equality and v5 byte identity.
- Next: create the Final-7 combined authority and launch only one fresh distinct Phase-Two identity.
- Safety: shadow only; no admitted Phase Two/Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
