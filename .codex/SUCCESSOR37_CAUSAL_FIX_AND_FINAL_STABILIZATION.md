# Successor37 causal fix and final stabilization

Successor37 remains terminal and permanently inadmissible. Its first guardian failure is immutable; the later forensic result does not rehabilitate or relaunch it.

The exact journal disproves a real 16.218-second acknowledgement. Commands 24,357 and 24,358 committed in 147 ms and 93 ms. A 15.188-second checkpoint interval overlapped a 14.125-second audit-snapshot cleanup on the same control lane used to publish runtime state and heartbeat. The guardian then projected a stale 171 ms aggregate by another 16,047 ms after both exact commands had already committed.

The causal correction keeps the 15-second deadline. Snapshot cleanup no longer occupies the state/heartbeat I/O lane, and the writer publishes exact envelope/command identities so a guardian can reconcile a stale snapshot only against those commands' read-only durable journal rows. Missing, active, malformed, or failed rows still fail closed.

Validation is green: 584/584 broad affected tests and 162/162 final changed-module tests, with persistence/WAL pressure, reconnect, event-loop fairness, telemetry accounting, shutdown, guardian, integrity, prospective configuration, and soak coverage. Successor32's feature/model/economics/execution/risk/replay/resolution families remain byte-identical; changes are infrastructure-only.

Exact next action: one fresh final source freeze and one exact full-suite qualification. No predecessor seal, materialization, nonce, runtime, or guardian may be rerun.
