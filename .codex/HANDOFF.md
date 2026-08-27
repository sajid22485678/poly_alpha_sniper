# Poly Alpha handoff

Canonical evidence: `.codex/STABILIZATION_FAILURE_MATRIX_AND_ADJACENT_FIXES.json`.

The full predecessor failure matrix is complete. All S19/S20/S22/S23/S24/S25/S26/S27/S29/S31/S33/S34 classes remain preserved and map to current controls and tests. Deep adjacent review found and fixed six related gaps: incomplete critical-task supervision, missing adapter-owner supervision, silent heartbeat send failures, failed dynamic-subscription state convergence, non-latching accepted-processing failures plus stop masking, and a test HTTP-session leak.

Current evidence: 288/288 broad affected tests pass; focused persistence/reconnect/crash/host-loss/guardian/shutdown batches pass; the 20,000-event worker-backed combined-feed soak is clean with zero overflow/loss/mismatch/latch and scheduler gaps below 100 ms. The final full repository suite has deliberately not run yet—it is reserved for the final frozen tree.

The Successor35-named source seal is superseded evidence only after these source mutations. Do not materialize or launch it. Successor34 remains terminal and permanently unavailable.

Successor32 research-core file hashes remain exact for features, models, economics, execution, risk, replay, resolution, model health, positions, and ledger. Changed families are runtime orchestration, transports, startup timeout, health/export observation, and tests. Complete the semantic diff review before asserting final behavioral equivalence.

Next action: finish that equivalence/diff review; if no binding uncertainty remains, create one fresh final source-freeze identity and run the exact full qualification once. No prospective launch occurs before its PASS.
