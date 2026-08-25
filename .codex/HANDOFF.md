# Poly Alpha continuation handoff

## Durable boundary

`SUCCESSOR23_MATERIALIZED_COLD_VERIFIED_PHASE_ONE_PREPARATION_NEXT`

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
the single launch passed 4,188 tests with zero failures, errors, or skips.
Post-tree equality is exact and post-verification
`9fb81c3adcda93b65d2727cd640b897e74c17c2f9e565bb3708ca125f7a411fe`
proves v5 unchanged with no journal.

Fresh Successor23 acquisition
`V4-PR-001-PROSPECTIVE-SUCCESSOR23-20260825T063851Z` is reserved at
`D:\poly_alpha_prospective_exact_v6_successor23_20260825T063851Z` with
materialization nonce `9267184128614eb29a904c8c2654e427`, Phase-One nonce
`cb94a3af73994324b3290ab599f0467a`, and a distinct Phase-Two nonce that remains
unauthorized. Startup process census is zero and the target root is absent.

Materialization ran once and exited zero. Manifest
`bd1aa9c5c0967200317e3c21ce13209b3bce8aed4a6c3697a9a6c26d4e637d8d`
binds fresh database
`7866fc6bee896cdcc4ef0d53b0ae544407cc3adf85f767c11ae15da8cdfaba53`.
Independent immutable cold verification
`3fd55f19f77ca42d3dfdbf1ae537102bef820ddb0fc48a9e9656aefcac39c3e6`
passed exact-v6 schema, quick/FK, empty business/research/holdout state, released
lease, zero runtime processes, and unchanged v5. A corrected verifier predicate
was rerun only because its first invocation stopped before creating either
output; materialization was not rerun.

Exact next action: prepare a create-once Phase-One launcher bound to this cold
verification and sealed source, then execute it exactly once.
