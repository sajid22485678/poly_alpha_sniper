# Poly Alpha durable status

Updated: `2026-08-25T12:59:32.6995202+07:00`

## Current boundary

`SUCCESSOR22_RETENTION_FIX_VERIFIED_SUCCESSOR23_SOURCE_SEAL_NEXT`

Successor22 remains gracefully terminal, permanently forensic/ineligible, and
`MUST_NOT_RELAUNCH`. Its database and immutable failure evidence were not
changed.

The canonical retention contract is now implemented as explicit monotonic
lifecycle state: `RAW < TRADE_EVIDENCE < PERMANENT`. Source and book rows are
promoted only inside an owning transaction after all immutable evidence fields
match. Weaker re-offers preserve stronger state, trade pinning cannot demote
permanent source/book/CEX evidence, and genuinely different evidence still
raises `V4EvidenceConflict`. Journal failure finalization now preserves the
actual semantic attempt count instead of stamping the configured retry ceiling.

Verification is green:

- final focused: 5/5;
- directly affected persistence/runtime/schema: 425/425;
- concurrency/adversarial/lineage/recovery: 177/177;
- broader runtime/config/safety: 208/208;
- zero failures, errors, or skips in every green layer.

The tests cover the exact RAW-first failure, partial graph retry, same-class
duplicate, repeated retry, reversed/weaker order, simultaneous same-identity
bundles, concurrent promotion ordering, invalid downgrade prevention,
unrelated identities, true immutable conflicts, dead/stopped workers, delayed
acknowledgement, saturation, shutdown, loss accounting, guardian-visible state,
journal/reconciliation, lineage, and safety.

Full evidence: `.codex/SUCCESSOR22_RETENTION_FIX_VERIFICATION.json`.

Safety remains fail-closed. The deliberate dirty tree is preserved at branch
`master`, HEAD `d3364f219feb37a09a547ff1daba6f0f96377fe4`, index tree
`700307bdbc9a4fdab7615d79eea83fe1bf6463cb`, with zero staged paths.

Exact next action: freeze this current source, create a fresh Successor23
create-once source-seal root/manifest/runner, and execute the repository-defined
full suite exactly once.
