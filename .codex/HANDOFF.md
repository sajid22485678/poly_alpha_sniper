# Poly Alpha continuation handoff

## Durable boundary

`SUCCESSOR22_ROOT_CAUSE_AND_MANDATORY_RED_COMPLETE_FIX_NEXT`

Successor22 is terminal, permanently forensic/ineligible, and unavailable.
Its exact closure evidence remains at bridge commit
`8f121530b2e72a432488af76ebc793cd15c3f4dc`.

Root cause is proven end-to-end: the same accepted source observation and first
causal book can commit as RAW through telemetry/evaluation before the critical
execution-book command asks for PERMANENT. Retention is documented in source as
mutable lifecycle state, but the bundle treats it as immutable evidence after
duplicate resolution. This deterministic ordering rolls back the critical
transaction, accounts two lost logical rows, and trips the guardian. The
durable journal's attempt count of three is misleading: non-retryable
`V4EvidenceConflict` dispatched once, while failure finalization stamps the
configured maximum.

The canonical invariant is Model B:
`RAW < TRADE_EVIDENCE < PERMANENT`, monotonic and atomic after immutable-field
equivalence. No last-write-wins behavior and no weakening of
`V4EvidenceConflict` is permitted.

Mandatory RED is preserved at
`D:\pytest_tmp_v4\poly_alpha_successor22_retention_red2_20260825T0530Z`.
The single production-path regression executed and failed exactly on
`RAW -> PERMANENT`; JUnit SHA-256 is
`9663f37e8ee0329142b120ea9cdea20545f9f92c96584a54da9c875df85f8331`.
No production code had changed.

Exact next action: implement the minimal monotonic transition in the store,
guard all trade-evidence pin paths against downgrading PERMANENT, then make the
causal and required adjacent cases GREEN before broader qualification.
