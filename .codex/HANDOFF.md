# Poly Alpha Handoff

Successor26 is terminal and permanently ineligible. Its exact Phase-One identity was not relaunched. One owner-authorized graceful-stop request was consumed; both processes are absent, the lock/request are gone, the lease is released, and the session ended `graceful_stop`.

The preserved first failure is `V4PersistenceTimeout`. Two commands exceeded the 15,000 ms acknowledgement deadline: `ENTRY_DECISION_EVIDENCE` at 15,133 ms (189 queue + 14,944 execute) and `MARKET_DISCOVERY` at 15,221 ms (15,204 queue + 17 execute). Later commits do not cure the failure.

Terminal evidence is lossless: 62,306/62,306 committed, one committed terminal command, zero failed/unresolved/duplicate/retried/incomplete/lost/mismatched/overflow rows. Exact-v6 quick-check is `ok`, FK 0, schema 6, and the managed fingerprint matches. Safety remains fail-closed and v5 is byte-identical.

Next: causal latency forensics, a real per-class budget, witnessed causal RED, then the minimal fix. Do not relaunch Successor26 or enter any later phase.

