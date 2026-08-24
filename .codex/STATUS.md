# Poly Alpha durable status

Updated: 2026-08-24T22:39:45+07:00

## State

`SUCCESSOR22_PHASE_ONE_HEALTHY_GUARDIAN_RUNNING_CODEX_STOP`

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

The former Successor22 seal (`66f373…`) is superseded. Its one distinct
replacement seal completed exactly one full-suite launch and independently
post-verified PASS:

- root: `D:\pytest_tmp_v4\poly_alpha_prospective_exact_v6_source_freeze_successor22_replacement1_20260824T144950Z`
- tested tree: `386991dc9c001ddf54290c2092e09725be7f5ad5a5f2d51bc901322cba2d23cc`
- manifest: `1d5de7656c641a55f88bf0571f0a684e8bcb7c509c352bd6c580bd055fbee8bf`
- runner: `780ef5cf92da7d60f952b3de5863e77ec4f204f0c31249047a2f083791475f8a`
- full suite: `4,185 / 4,185 PASS`, zero failure/error/skip, exit `0`, launch count `1`
- JUnit: `06db06cbd51822208c8819a07655805d6f97a607016af5f957f21f0f9bc0bd16`
- exact post-tree equality: `true`
- post-verification: `e2ffa43c0b77ec4a54cd195b3987339650d64d9337d7612e5685340596554287`
- authoritative-v5 DB/WAL/SHM exact equality: `true`

Safety remains `LIVE_ENABLED=false`, `REAL_ORDERS_POSSIBLE=false`, signing/authenticated trading unavailable, kill switch engaged, no Phase 3, `V4-HO-001=NONEXISTENT_UNCONSUMED`, authoritative v5 immutable.

Startup census was clean: exact V4 process count `0`, Successor20 open sessions
`0`, incomplete journal commands `0`, startup blockers `0`, and no pre-existing
Successor22 root/session. Successor22 then materialized exactly once at
`D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z` using nonce
`6871292fc9674d9196c4660a666c84e0`. Manifest SHA-256 is
`2af2f9ba42d00034cde02447ba2f054ec27dea6915600864eee59a5f513dc9e3`;
fresh DB SHA-256 is
`0adfd163f7094d2e623d3592f097efe700658686f4728d2d77e04172854602d9`.
CLI cold verification and a separate immutable census both passed: quick-check
`ok`, FK `0`, schema v6, 70 application tables, only three schema-migration
rows, and zero business/research/holdout rows. Materialization-verification
SHA-256 is
`e56014466896587de2a5425e9eff7869ab460f495c0a4e466c1dd7d41cb39b6d`.

Phase-One nonce `99d60fab076e4bcfae6e3a8831d6393a` was consumed exactly once.
Phase-Two nonce `9f22eca6d2c548c8bcd6b8d687a3f261` is reserved and unauthorized.

## Healthy running boundary

- acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR22-20260824T151804Z`
- session: `bc0614b5d1494a91854863b9ef527f13`
- launch timestamp: `1787585613002` (`2026-08-24T22:33:33.002+07:00`)
- runtime process chain: launcher `1964` → runtime `8536`
- source/tree: `386991dc9c001ddf54290c2092e09725be7f5ad5a5f2d51bc901322cba2d23cc`
- runtime launch request SHA-256: `76f68755b576e89f8bc6bcfea72a0018690575e192d7d23f9a4d3ace61599a5d`
- runtime process-start SHA-256: `b0b2bbc82b43ae3e37b953ddf3ba3fbf4ee6c8eb7e8ece331e1ca793ab7469b8`
- guardian process chain: launcher `11624` → worker `13504`
- guardian command: four hours, 30-second interval, five-minute snapshots
- guardian launch request SHA-256: `f818ef29c1e57be0a573b2ddbe9f603ced8858bb58ea0905d2247baa86f999c8`
- guardian process-start SHA-256: `cf1b0b7e684628b1dc4494bd84dfca37dee1ef409b2d4634797c0a09bdd12404`
- first healthy snapshot: `D:\poly_alpha_prospective_exact_v6_successor22_20260824T151804Z\operator\guardian_snapshot_1787585848823.json`
- first snapshot SHA-256: `81aba2daf324068ed7b9467ccd26801a0163803568645363b5fd58b642aa2e9d`
- first snapshot: 41 capsules, 82 predictions, 30 markets, 0 outcomes, 440 committed, 0 incomplete/failure/loss/overflow/discard
- latest durable guardian stream observation at `1787585939240`: 65 capsules, 130 predictions, 32 markets, 0 outcomes, 700 committed, 0 incomplete/failure/loss/overflow/discard, source `READY`, heartbeat age 665 ms
- guardian failures: none; guardian stderr bytes: 0

Safety remains `dry_run=true`, `LIVE_ENABLED=false`,
`REAL_ORDERS_POSSIBLE=false`, signing/authenticated trading/live adapter/order
placement/order cancellation unavailable, kill switch engaged, no Phase 3,
and `V4-HO-001=NONEXISTENT_UNCONSUMED`. Calibration artifacts, tournament,
Phase Two and holdout remain untouched.

`MUST_NOT_RELAUNCH`: the Successor22 materialization nonce and Phase-One nonce,
session, launcher/runtime PID pair, and guardian identity are consumed. Leave
the healthy runtime and guardian running independently when Codex stops.

Next action is a new-session bounded review at approximately two hours:
`1787592813002` / `2026-08-25T00:33:33.002+07:00`. Verify the same identities,
guardian first-failure state, safety, persistence accounting and both targets'
rank-1/unique-market/positive/negative/unlabeled counts; classify only
`EARLY_STOP_SUFFICIENT` or `CONTINUE_TO_4H`. The hard review is
`1787600013002` / `2026-08-25T02:33:33.002+07:00`; do not extend beyond it
without a new evidence-based reason. OOS end remains `1787628813002` and does
not itself authorize calibration or any later phase.
