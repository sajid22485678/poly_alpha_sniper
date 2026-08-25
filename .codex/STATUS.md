# Poly Alpha durable status

Updated: `2026-08-25T12:36:57.7077092+07:00`

## Current boundary

`SUCCESSOR22_ROOT_CAUSE_AND_MANDATORY_RED_COMPLETE_FIX_NEXT`

Successor22 is gracefully terminal, permanently forensic/ineligible, and
`MUST_NOT_RELAUNCH`. Its terminal closure remains unchanged at bridge commit
`8f121530b2e72a432488af76ebc793cd15c3f4dc`.

The complete causal path is established. The accepted Polymarket observation
is first eligible for non-critical/evaluation persistence as `RAW`. The later
critical `persist_execution_book_bundle` is built from the same frozen source
observation and first causal book, then explicitly requests `PERMANENT` inside
the store. Duplicate resolution preserves the RAW row but the bundle compares
retention as immutable evidence, causing deterministic `V4EvidenceConflict`.
The transaction rolls back; `logical_rows=2` becomes two critical lost rows;
the guardian fails closed. The same lifecycle conflict applies to the matching
book. Command hashing occurs before store-local retention escalation.

Canonical contract: Model B monotonic lifecycle promotion,
`RAW < TRADE_EVIDENCE < PERMANENT`. Promotion must occur atomically only after
immutable-field equivalence is proven. Weaker re-offers never lower stored
state. Genuine evidence differences remain conflicts.

The production-path regression was observed RED before any production edit:
1 executed / 1 failed / 0 errors / 0 skips, exact field mismatch
`retention_class: RAW -> PERMANENT`. JUnit SHA-256:
`9663f37e8ee0329142b120ea9cdea20545f9f92c96584a54da9c875df85f8331`.
The failed system-Python invocation lacked pytest and received no test credit.

Full root-cause/RED record:
`.codex/SUCCESSOR22_RETENTION_ROOT_CAUSE_RED.json`.

Safety remains fail-closed and authoritative v5 remains byte-identical.

Exact next action: implement the minimal explicit monotonic promotion for
equivalent source/book rows, prevent trade-evidence pinning from lowering
`PERMANENT`, preserve conflict semantics, and execute the causal plus adjacent
verification ladder.
