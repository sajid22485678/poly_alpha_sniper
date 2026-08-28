# Poly Alpha status

- State: `PHASE_TWO_FINAL8_FAILURE_ROOT_CAUSE`.
- Tournament: empty expired parent preserved; child experiment `d8862bbd…` remains the exact preregistered ordinal-2 authority with no retuning.
- Failed launches: five distinct Phase-Two startup nonces are terminal and `MUST_NOT_RELAUNCH`; Final-6 nonce `d346e4fa…` failed before session admission.
- Final-6 effect: the existing cohort verification committed idempotently; release-risk verification failed; candidates stayed 2,849; runtime sessions stayed 1 historical; journal is 36,897 committed / 5 failed.
- Root cause: the release-risk builder alone still used the newer runtime source tree for immutable research policy identity. Cohort, config, calibration, and tournament identity correctly remain bound to the S32 materialization tree; executable provenance remains separately runtime-tree guarded.
- Final-8: session `6c19ac88…` admitted the exact S32 cohort/config, then became bindingly ineligible: acknowledgement deadlines were exceeded, 12 current-session outcome-authority commands failed replay identity, and critical-evidence-lost reached 26.
- Disposition: no guardian was launched over the failed runtime; the exact nonce/PID-bound graceful stop completed, lease released, session terminal, zero processes remain, DB quick-check/FK clean.
- Next: causal RED/fix for restart outcome-authority/idempotency semantics, then verification/reseal before any distinct Phase-Two retry.
- Safety: shadow only; no admitted Phase Two/Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
