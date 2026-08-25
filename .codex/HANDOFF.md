# Poly Alpha continuation handoff

## Durable boundary

`SUCCESSOR22_RETENTION_FIX_VERIFIED_SUCCESSOR23_SOURCE_SEAL_NEXT`

Successor22 is terminal, forensic-only, permanently ineligible, and cannot be
relaunched. Root cause and mandatory RED are at bridge commit
`b21c7f8329462a27be3096879731b32889d5b275`.

The minimal fix is complete. Retention is an explicit monotonic lifecycle
transition (`RAW < TRADE_EVIDENCE < PERMANENT`) performed only after immutable
source/book equivalence is proven inside the critical transaction. No
last-write-wins behavior or exception suppression was introduced. Permanent
rows cannot be lowered by later telemetry, evaluation, or trade pinning.
Deterministic failure accounting now records the actual dispatch count.

All pre-seal layers pass: focused 5, affected 425, adversarial 177, and broader
runtime/safety 208, each with zero failures/errors/skips. Exact paths, hashes,
case coverage, and source-file hashes are in
`.codex/SUCCESSOR22_RETENTION_FIX_VERIFICATION.json`.

Repository authority remains branch `master`, HEAD
`d3364f219feb37a09a547ff1daba6f0f96377fe4`, index tree
`700307bdbc9a4fdab7615d79eea83fe1bf6463cb`; deliberate unknown changes are
preserved and nothing is staged.

Exact next action: create a unique Successor23 source-seal evidence root,
capture the current canonical working-tree manifest, construct a create-once
runner bound to its hashes, then launch the full repository suite exactly once.
