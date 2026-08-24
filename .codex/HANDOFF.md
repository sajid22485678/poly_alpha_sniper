# Poly Alpha continuation handoff

Current boundary: Successor20 was canonically closed exactly once and remains
permanently forensic/ineligible. The prior Successor22 source seal is
superseded; one distinct replacement seal passed exactly once and its fresh
materialization identity is reserved but unconsumed.

Implementation:

- `lite_frequency_v4/host_loss_recovery.py`: hard-bound preview/apply authority, exact process/lock/DB/v5/guardian proof, preview-drift refusal, prospective-v6 OS lease, SQLite write authorizer, atomic lifecycle journal/session closure, idempotent terminal handling, and post-state verification.
- `tools/v4_successor20_host_loss_close.py`: direct create-once operator CLI with `preview` and `apply` subcommands.
- `tests/test_frequency_v4_host_loss_recovery.py`: exact success, wrong session, wrong nonce, living owner, competing owner, pending work, already-terminal idempotence, preview drift, create-once evidence, and direct CLI invocation.

Closure evidence:

- Initial RED: 8/8 failed because the canonical module did not exist.
- Final affected union: 437 passed, zero failure/error/skip, exit 0.
- JUnit SHA-256: `c3ab0fad55a9384103fce77d6fca082cdaccf510a9cc24d142a2561bfd1f63e7`.
- Preview file/content SHA-256: `7913c0168c2531d2ea6829e63017eaff399db7445b91ee59d525d0fb059e3776` / `47d489cc4ea40ee25d9609436fbd34b05468ee57ec717f50c847512eba9ef225`.
- Closure nonce/timestamp: `03fce00bffed4963a1cb953d5dcf0e8b` / `1787574261428`.
- Closure file SHA-256: `c2b89e9724287b5c8224c9add169acc894f097a2e0fce5396b4ecb1ca5d85fd7`.
- Independent DB state: quick-check/FK clean, zero open sessions/startup blockers, `55,499 COMMITTED / 1 FAILED / 0 unresolved`, one terminal command, unchanged zero economic/research state.

Prepared replacement seal:

- Root: `D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor22_replacement1_20260824T144950Z`.
- Tree SHA-256: `386991dc9c001ddf54290c2092e09725be7f5ad5a5f2d51bc901322cba2d23cc`.
- Manifest SHA-256: `1d5de7656c641a55f88bf0571f0a684e8bcb7c509c352bd6c580bd055fbee8bf`.
- Runner SHA-256: `780ef5cf92da7d60f952b3de5863e77ec4f204f0c31249047a2f083791475f8a`.
- Full suite: `4,185 / 4,185 PASS`, zero failure/error/skip, exit `0`, launch count `1`.
- JUnit SHA-256: `06db06cbd51822208c8819a07655805d6f97a607016af5f957f21f0f9bc0bd16`.
- Post-tree equals tested tree exactly; authoritative v5 is byte-identical.
- Post-verification SHA-256: `e2ffa43c0b77ec4a54cd195b3987339650d64d9337d7612e5685340596554287`.

Still forbidden until the replacement source seal passes: Successor22
materialization and Phase-One launch. Always forbidden in this run:
calibration/tournament/Phase Two/holdout, Phase 3, live/authenticated trading,
signing, real orders/cancels, authoritative-v5 mutation, and any reuse or
rehabilitation of Successors20/21.

Startup census: zero exact V4 processes, zero open Successor20 sessions, zero
incomplete commands/blockers, and no pre-existing Successor22 root/session.
Reserved materialization: root
`D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z`, acquisition
`V4-PR-001-PROSPECTIVE-SUCCESSOR22-20260824T151804Z`, nonce
`6871292fc9674d9196c4660a666c84e0`, timestamp `1787584684126`. Materialize it
once, cold-verify, then launch Phase One once with an independent guardian. Stop
Codex when healthy advancing collection is proven.
