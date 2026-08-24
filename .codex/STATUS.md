# Poly Alpha durable status

Updated: 2026-08-24T21:52:09+07:00

## State

`SUCCESSOR20_CANONICALLY_CLOSED_SUCCESSOR22_REPLACEMENT_SEAL_PREPARED`

The owner authorized one canonical Successor20 host-loss closure followed by a
replacement Successor22 source seal, materialization, cold verification,
Phase-One launch and independent guardian if every gate passes.

The repository now contains a purpose-built, non-generic recovery authority hard-bound to session `27cad22fef8f4d43a68034c7e9f18af2`, runtime nonce `13a4198c8f96427582a3e8c7e7b8bb4c`, former launcher/runtime PIDs `14236/11508`, and the existing Successor20 acquisition paths. It uses a create-once preview, the existing prospective-v6 OS guard, complete process absence proof, exact-v6 census, preview-drift binding, a SQLite authorizer, and one atomic transaction. The only permitted database effects are one committed `end_runtime_session` lifecycle command and the target session's `ended_ts_ms`/`stop_reason` columns.

Fail-first evidence: the initial causal suite failed 8/8 because no canonical repository module existed. Current focused/affected union is 437/437 PASS, zero failures/errors/skips, exit 0. JUnit: `D:\pytest_tmp_v4\poly_alpha_successor20_host_loss_affected_green_20260824T1900Z\junit.xml`; SHA-256 `c3ab0fad55a9384103fce77d6fca082cdaccf510a9cc24d142a2561bfd1f63e7`.

The create-once preview was admitted without regeneration. Preview file SHA-256
is `7913c0168c2531d2ea6829e63017eaff399db7445b91ee59d525d0fb059e3776`;
bound content SHA-256 is
`47d489cc4ea40ee25d9609436fbd34b05468ee57ec717f50c847512eba9ef225`.
Canonical apply completed exactly once under closure nonce
`03fce00bffed4963a1cb953d5dcf0e8b` at `1787574261428`. Closure artifact:
`D:\poly_alpha_prospective_exact_v6_successor20_20260824T0355Z\operator\successor20_host_loss_closure.json`;
SHA-256 `c2b89e9724287b5c8224c9add169acc894f097a2e0fce5396b4ecb1ca5d85fd7`.

Independent post-closure verification: quick-check `ok`, foreign keys `0`,
open sessions `0`, commands `55,500` (`55,499 COMMITTED / 1 FAILED / 0
unresolved`), terminal commands `1`, duplicate command IDs `0`, startup
blockers `0`, and all economic/research counts remain zero. Guardian failure
SHA-256 remains `4848a897f4eff16181756dbefcfbbb6f0306093018d7102656772f7996ce22a7`.
Successor20 remains permanently failed/forensic/ineligible; Successor21 remains
immutable/ineligible.

The former Successor22 seal (`66f373…`) is superseded. One distinct replacement
seal is prepared but its pytest launch count is still zero:

- root: `D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor22_replacement1_20260824T144950Z`
- tested tree: `386991dc9c001ddf54290c2092e09725be7f5ad5a5f2d51bc901322cba2d23cc`
- manifest: `1d5de7656c641a55f88bf0571f0a684e8bcb7c509c352bd6c580bd055fbee8bf`
- runner: `780ef5cf92da7d60f952b3de5863e77ec4f204f0c31249047a2f083791475f8a`

Safety remains `LIVE_ENABLED=false`, `REAL_ORDERS_POSSIBLE=false`, signing/authenticated trading unavailable, kill switch engaged, no Phase 3, `V4-HO-001=NONEXISTENT_UNCONSUMED`, authoritative v5 immutable.

Exact next action: run the prepared replacement seal runner exactly once. If
and only if the full suite and independent post-tree/v5 verification pass,
perform the zero-owner startup census, materialize Successor22 once, cold
verify it, launch Phase One once, and start its independent guardian.
