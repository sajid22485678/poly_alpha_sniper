# Poly Alpha continuation handoff

Current boundary: the canonical Successor20 host-loss recovery mechanism is implemented and verified but has not touched the real Successor20 database. The prior Successor22 source seal is invalidated by the authorized source change.

Implementation:

- `lite_frequency_v4/host_loss_recovery.py`: hard-bound preview/apply authority, exact process/lock/DB/v5/guardian proof, preview-drift refusal, prospective-v6 OS lease, SQLite write authorizer, atomic lifecycle journal/session closure, idempotent terminal handling, and post-state verification.
- `tools/v4_successor20_host_loss_close.py`: direct create-once operator CLI with `preview` and `apply` subcommands.
- `tests/test_frequency_v4_host_loss_recovery.py`: exact success, wrong session, wrong nonce, living owner, competing owner, pending work, already-terminal idempotence, preview drift, create-once evidence, and direct CLI invocation.

Evidence:

- Initial RED: 8/8 failed because the canonical module did not exist.
- Final affected union: 437 passed, zero failure/error/skip, exit 0.
- JUnit SHA-256: `c3ab0fad55a9384103fce77d6fca082cdaccf510a9cc24d142a2561bfd1f63e7`.

Still forbidden until the replacement source seal passes: real closure apply, Successor22 materialization, Phase-One launch, calibration/tournament/Phase Two/holdout, Phase 3, live/authenticated trading, signing, real orders/cancels, and authoritative-v5 mutation.

Next sequence: document final authority contract; consume one replacement exact-once source seal; independently verify current-tree equality and v5 identity; preview/apply Successor20 closure exactly once; verify accounting/forensics and zero startup blockers; then canonically materialize/cold-verify/launch Successor22 and its independent guardian. Stop Codex when healthy independent collection is proven.
