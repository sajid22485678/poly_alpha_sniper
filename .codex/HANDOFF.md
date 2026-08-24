# Poly Alpha continuation handoff

Current boundary: Successor20 was canonically closed exactly once and remains
permanently forensic/ineligible. Successor22 passed one replacement source
seal, materialized/cold-verified once, launched Phase One once, and is now
healthy under its independent guardian. Codex active work stops at this boundary.

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

The Phase-One launch authority is consumed. Always forbidden in this run:
calibration/tournament/Phase Two/holdout, Phase 3, live/authenticated trading,
signing, real orders/cancels, authoritative-v5 mutation, and any reuse or
rehabilitation of Successors20/21.

Startup census: zero exact V4 processes, zero open Successor20 sessions, zero
incomplete commands/blockers, and no pre-existing Successor22 root/session.
Materialized root:
`D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z`, acquisition
`V4-PR-001-PROSPECTIVE-SUCCESSOR22-20260824T151804Z`, nonce
`6871292fc9674d9196c4660a666c84e0`, timestamp `1787584684126`. Manifest SHA-256
is `2af2f9ba42d00034cde02447ba2f054ec27dea6915600864eee59a5f513dc9e3`;
fresh DB SHA-256 is
`0adfd163f7094d2e623d3592f097efe700658686f4728d2d77e04172854602d9`;
materialization-verification SHA-256 is
`e56014466896587de2a5425e9eff7869ab460f495c0a4e466c1dd7d41cb39b6d`.
Immutable census is quick-check/FK clean, schema v6, only three migration rows,
and zero business/research/holdout rows.

Running identity:

- Phase-One nonce: `99d60fab076e4bcfae6e3a8831d6393a` — consumed, `MUST_NOT_RELAUNCH`.
- Session: `bc0614b5d1494a91854863b9ef527f13`; started `1787585613002`.
- Runtime chain: launcher `1964` → runtime `8536`; state `RUNNING`, source `READY`.
- Runtime request/start hashes: `76f68755b576e89f8bc6bcfea72a0018690575e192d7d23f9a4d3ace61599a5d` / `b0b2bbc82b43ae3e37b953ddf3ba3fbf4ee6c8eb7e8ece331e1ca793ab7469b8`.
- Guardian chain: launcher `11624` → worker `13504`; four-hour bounded command.
- Guardian request/start hashes: `f818ef29c1e57be0a573b2ddbe9f603ced8858bb58ea0905d2247baa86f999c8` / `cf1b0b7e684628b1dc4494bd84dfca37dee1ef409b2d4634797c0a09bdd12404`.
- First snapshot: `D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z\operator\guardian_snapshot_1787585848823.json`; SHA-256 `81aba2daf324068ed7b9467ccd26801a0163803568645363b5fd58b642aa2e9d`.
- Latest durable stream at `1787585939240`: 65 capsules, 130 predictions, 32 markets, 0 outcomes, 700 committed; zero incomplete/failure/loss/overflow/discard, heartbeat 665 ms.
- No guardian failure file and zero guardian stderr.
- Phase-Two nonce `9f22eca6d2c548c8bcd6b8d687a3f261` remains reserved and unauthorized.

Leave runtime and guardian running unchanged. At approximately two hours
(`1787592813002`, `2026-08-25T00:33:33.002+07:00`), perform one bounded review
and classify `EARLY_STOP_SUFFICIENT` or `CONTINUE_TO_4H` using both targets'
rank-1, unique-market, positive, negative and unlabeled counts plus lineage and
guardian integrity. At four hours (`1787600013002`,
`2026-08-25T02:33:33.002+07:00`), perform the hard review and do not extend
without a new evidence-based reason. OOS end `1787628813002` does not authorize
calibration, tournament, Phase Two or holdout. Codex must not remain active.
