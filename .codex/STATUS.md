# Poly Alpha status

Successor37 remains terminal and permanently inadmissible. Its exact journal proves the guardian's `16,218 ms` classification came from projecting a stale `171 ms` state snapshot across a control-lane audit-cleanup stall after both named commands had already committed in `147 ms` and `93 ms`.

The 15-second acknowledgement deadline was not increased. Audit cleanup is isolated from state/heartbeat publication, and guardian classification now binds exact envelope identities to read-only durable journal status. Missing, active, malformed, or failed evidence remains fail-closed.

Deep stabilization is green: `584 / 584` broad affected tests and `162 / 162` final changed-module tests. Successor32 strategy semantics remain byte-identical. Next is one fresh final exact source qualification; no predecessor identity is reusable.
