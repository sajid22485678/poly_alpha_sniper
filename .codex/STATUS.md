# Poly Alpha status

- State: `COMBINED_POST_OOS_FINAL17_SOURCE_SEAL_PASSED`.
- Final16 source seal remains an immutable 4,286/4,286 PASS, but its fresh Phase-Two identity is terminal/ineligible and `MUST_NOT_RELAUNCH`.
- Identity: nonce `73d9261b11b44aa19cef4a2821464770`, session `08f73f206d7c42c186d00e4b93983ff9`, PIDs `8464 -> 11372`.
- First binding failure: `V4PersistenceTimeout`; 62 trade-critical acknowledgements overdue in the preserved failure capture, with a maximum 171.5 s queue/ack delay. No guardian was launched.
- Cause: first telemetry-store connection/schema hydration ran while telemetry marked the shared gate active. The 171.7 s batch contained only 12–60 ms transactions, but critical admission could not pass the in-process gate during initialization.
- Fix: prepare the auxiliary telemetry store before gate activation, retain exact factory/contention error classification, then re-check critical priority immediately before transaction admission.
- RED reproduced the race; 221 focused persistence/telemetry and 306 broader engine/runtime/store/guardian/prospective tests pass.
- Terminal reconciliation: exact processes 0; lease released; session graceful-stopped; all 215 Final16 commands committed; no Final16 failed/incomplete journal row; DB quick-check `ok`; FK 0; no evaluation or holdout.
- Final17 exact seal: tree `f611c2bf1c5d37ea6722db3fd1ffa552ce0bc8e068fe0d9f48e69ccc124eee91`; 4,287/4,287 passed; zero failures/errors/skips; exact post-tree equality; v5 unchanged.
- JUnit SHA-256: `d82d7bd0a2c43a42c5bc04d165cb12f5cb66a7dbd5d21be1e4d8f83817b8cfdf`; post-verification SHA-256: `4b9c8a6507e8937881688b4416973cca222aafe927740025d4ec5725be5e1801`.
- Next: bind immutable S32 OOS research authority to Final17 and launch one distinct Phase-Two identity. Final16 may never be relaunched, and a later clean drain does not cure its first failure.
- Safety: shadow only; no live/signing/authenticated/order capability; kill engaged; no Phase Three; `V4-HO-001` nonexistent/unconsumed; authoritative v5 immutable.
