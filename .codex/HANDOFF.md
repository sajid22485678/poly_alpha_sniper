# Poly Alpha continuation handoff

## Durable boundary

`SUCCESSOR23_SOURCE_SEAL_PREPARED_EXACT_ONCE_LAUNCH_NEXT`

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

The frozen Successor23 source-seal root is
`D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor23_20260825T060219Z`.
Tree `249c1ec0aee2f38ccd954b9fe6a76f4be053dabcf1dc7ffa9d5eb880ec701afd`,
manifest `3c464a56a297532995bf80b1809baff79e2eec905ff10ec7b476bb1bd56b76dc`,
runner `d3d3d63b7ca2c6a511df424c25c1eb87a1353feb41fa94c0cda1f4f358acf48d`;
launch count zero; v5 unchanged.

Exact next action: execute the bound runner exactly once and never retry this
seal identity after failure, interruption, or ambiguity.
