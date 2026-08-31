# Poly Alpha status

## Budget epoch 2 corrected source authority

- Corrected tree `acfba2d9...3ef1` passed 4,310/4,310 exactly once with zero failures/errors/skips, exact post-tree equality, and unchanged v5.
- The earlier `dea55c83...152b0` seal remains an immutable PASS but is not registration authority: a post-seal disk-binding check found that the historical candidate evaluator hash must transition to the newly qualified evaluator hash while every candidate semantic field remains exact.
- The narrow evaluator-authority transition is implemented and all affected suites are green. No registration, runtime, guardian, evaluation, holdout, or later phase exists yet.
- Next: create the canonical hash-bound one-test epoch authority and execute one fresh registration nonce with the existing two-hour guarded-start lead.

## Prospective research-budget epoch 2 causal fix

- The preregistered epoch-2 mechanism is implemented without rewriting the original ten-slot budget or any predecessor definition.
- RED was proven at the pure experiment contract, promotion gate, and exact-v6 registration boundaries. Focused contract/statistics/schema-v6 and broader persistence/store/tournament suites are green.
- The only admissible extension is global ordinal 11 as epoch 2 local slot 1, with one `0.005` test after disk proves zero prior evaluation runs and commands. Cumulative alpha would be `0.005` under the unchanged `0.05` family cap.
- Registration fails closed unless the database proves the ten contiguous predecessor slots, identical historical fixed budgets, exact parent-budget and authority hashes, unchanged research/candidate semantics, and the existing guarded-start authority.
- Next: one distinct exact source qualification. No registration, runtime, session, guardian, evaluation, holdout, or later phase has been created.

## Recovery_10 owner boundary

- The exactly one owner-authorized replacement registration was consumed once and failed closed: `tournament continuation exceeds research budget`.
- Parent ordinal is 10; a child would be ordinal 11, while the frozen research budget permits at most 10 confirmatory challengers. No research parameter was changed or retuned.
- The failed transaction created no child definition. Lease release, quick-check, FK state, journal terminality, zero open sessions/evaluations/holdout, and authoritative-v5 hashes are clean.
- No runtime or guardian was launched. The registration and nonce must not be retried.
- Next: stop and obtain distinct owner authority before any research-budget change or further experiment.

## Recovery_10 exact source qualification

- Recovery_10 tested tree `7a938096...cddc8` passed 4,300/4,300 exactly once with zero failures, errors, or skips, exact post-tree equality, and byte-identical authoritative v5.
- The causal contract now admits a zero-collection guarded-start failure captured after parent registration and strictly before development opens. Its two-hour evidence-derived lead, safety margin, and strict acknowledgement/persistence deadlines remain unchanged.
- The earlier `RECOVERY10` preparation-only identity hit an old-token naming collision and never launched pytest; it is preserved. The non-colliding `RECOVERY_10` seal is authoritative.
- Exactly one owner-authorized replacement experiment remains unconsumed. No runtime, session, lease, or guardian exists.
- Next: bind the Recovery_10 source authority, register the single replacement with a 7,200,000 ms lead, and prove a clean exact-session guardian observation before development collection becomes admissible.
- Safety: shadow only; live/signing/authenticated placement/cancellation unavailable; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.

## Recovery7 exact source qualification

- Ordinal9 is terminal, permanently inadmissible, reconciled, and was not evaluated or relaunched.
- The causal RED cases reproduced the missing coverage-authority registration, stale Phase-Two session binding, and heartbeat/state-export coupling.
- Minimal fixes are green across guardian, runtime, engine, exact-v6 schema/reconciliation, prospective runtime/research, and persistence suites. Frozen research semantics and all strict deadlines remain unchanged.
- Recovery7 tested tree `aae75d18...049f0` passed 4,299/4,299 exactly once with zero failures/errors/skips, exact post-tree equality, and byte-identical authoritative v5.
- Exactly one guarded replacement authority is admitted and still unconsumed. Next: canonical source adoption, then fresh experiment/runtime/session/guardian identities with a verified first clean exact-session guardian observation before development opens.

- State: `FINAL31_RECOVERY6_ORDINAL9_PHASE_TWO_DEVELOPMENT_ACTIVE`.
- Ordinal8 remains terminal and permanently inadmissible. Ordinal9 experiment `76500adc...cae3`, parent-linked to Ordinal8, is sealed with unchanged research semantics for development `1788063000000..1788106200000` and validation through `1788149400000`.
- Runtime nonce `5efeb9de...cec7b`, session `fee83574...46fb2`, process pair `19556 -> 8832`; consumed and must not relaunch.
- Guardian nonce `de504f82...cfaaf`, process pair `19960 -> 6968`; consumed once and must not restart.
- Two clean observations show candidates `12991 -> 12999`, 84,130 committed current-session commands, zero current-session failed/incomplete, zero acknowledgement violations, zero true loss/mismatch/unexpected loss/overflow, clean DB/FK, and fail-closed safety.
- Independent guardian authority begins at `1788085382134`; the earlier runtime interval is explicitly not backdated as guarded. Final disposition must apply repository coverage requirements fail-closed.
- Recovery6 tree `d6a1253d...c6365` passed 4,295/4,295 exactly once. Do not rerun the seal or mutate source during acquisition.
- Disk authority resolves the coverage question: S31's 54,517 ms pre-guardian collection was a binding defect, and the active guarded-start invariant prohibits Ordinal9's 13,075,246 ms gap from entering selection. Ordinal9 is forensic/supporting evidence only.
- The exact guardian-terminal, runtime-stop, and terminal-reconciliation operators are prepared and inertly verified. None has executed. Evaluation is deliberately unprepared and unauthorized.
- A distinct owner packet now authorizes exactly one replacement parent-linked Phase-Two development experiment after Ordinal9 is terminal and reconciled. The authority is unconsumed and cannot be used early.
- The replacement must keep every frozen research semantic unchanged and must prove guardian admission, identity verification, and a first clean observation before admissible development collection begins. No retuning or Ordinal9-based selection is permitted.
- Narrow contract audit: current exact-v6 registration cannot honestly continue a collected parent invalidated only by a pre-guardian coverage gap. Its failed-parent shape is guardian-failure-specific, and reconciliation allowlists that same shape. A dedicated coverage-authority continuation payload is required; the source RED/fix is embargoed until Ordinal9 is terminal.
- The existing future `development_not_before_ts_ms` contract and interval-selected tournament population are sufficient for guarded start once the new authority class is admitted: launch and verify the guardian during the ten-minute lead, before the preregistered development interval opens.
- Next: leave runtime and guardian untouched. At or after development end `1788106200000`, verify exact identity and first-failure authority, then execute the three prepared terminal operators once in order. Do not evaluate Ordinal9. After clean terminal reconciliation, consume the new authority exactly once for the guarded replacement.
- Safety: shadow only; no live/signing/authenticated placement/cancellation; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.

## Ordinal9 terminal result

- A binding guardian failure occurred at `1788104520596`: `heartbeat_age_ms:34653`, SHA-256 `5b9efc01...1e61df0`. It is genuine under the frozen contract and later heartbeat recovery does not cure it.
- The prepared clean guardian-terminal operator was not executed because it correctly rejects any existing guardian failure artifact.
- A new exact failure-closure operator created one nonce/PID-bound stop request. The runtime exited gracefully, released its lease, removed its lock and stop request, and was not relaunched.
- Terminal reconciliation passed: `53,596` exact-session commands committed, quick-check `ok`, zero FK/duplicate/open-session/evaluation/holdout/mismatch/unexpected-loss/true-critical-loss.
- Additional defects are preserved for RED: stale predecessor-session binding in `phase_two_integrity`, and a terminal writer timeout latch followed by a durable late critical success.
- The single guarded replacement authority remains admitted but unconsumed. Source RED/fix/verification and a fresh exact seal are mandatory before it can be used.
