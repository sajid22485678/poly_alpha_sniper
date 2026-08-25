# Poly Alpha continuation handoff

Updated: `2026-08-25T18:01:49.1187322+07:00`

## Durable boundary

`SUCCESSOR24_HEALTHY_PHASE_ONE_ACQUISITION_RUNNING`

Canonical detail is in `.codex/SUCCESSOR24_INITIAL_HEALTHY_HANDOFF.json`.

Successors 1 through 23 are terminal and permanently ineligible. Never
relaunch, repair, rehabilitate, pool, or reuse them. Successor23's first
guardian failure and terminal forensic closure remain immutable.

## Successor24 identity

- Acquisition: `V4-PR-001-PROSPECTIVE-SUCCESSOR24-20260825T104125Z`
- Root: `D:\poly_alpha_prospective_exact_v6_successor24_20260825T104125Z`
- Tested tree: `7a1c9c09d124a44d732e0cbdd26336616e2804d9a97c1fc6f8ccd6bfed08f3b5`
- Source seal: 4,192/4,192 passed exactly once; zero failure/error/skip; exact post-tree equality.
- Materialization nonce: `a4d60ae0194c4850ad64c2ad8b0e8c79` — consumed once and released.
- Phase-One nonce: `f138bd0a49fb413c9fb29e9911163c93` — consumed once and active.
- Reserved Phase-Two nonce: `779cf700edeb4b57a57a7b8d111e2df9` — unauthorized.
- Session: `b1e0463d97b54df888535ce5ef0883b5`.
- Runtime PID pair: `15836 -> 16544`.
- Guardian PID pair: `4016 -> 19272`.

Never relaunch or duplicate this runtime. Never launch another guardian while
the current watcher is running.

An empty look-alike path exists at
`D:\claude\poly_alpha_prospective_exact_v6_successor24_20260825T104125Z`
from correcting an external patch target. It contains only an empty `operator`
directory—no files, database, manifest, runtime, lease, or process—and is not
an acquisition. The canonical Successor24 root is the `D:\...` path above.

## Proven current state

The independent cold verifier passed exact-v6 schema identity, quick/FK
checks, empty business/research/holdout state, released materialization lease,
absent prelaunch runtime/sidecars, and byte-identical authoritative v5.

At the create-once initial healthy capture, four guardian observations proved
forward progress:

- capsules `26 -> 39`;
- predictions `51 -> 76`;
- markets `41 -> 56`;
- committed commands `514 -> 895`.

The contemporaneous monitor recorded 44 capsules, 86 predictions, 56 markets,
0 outcomes, and 934 committed journal commands. Reconciliation was
`submitted=21151`, `accounted=21151`, `mismatch=0`, with zero unexpected loss.
Critical failed/lost, incomplete commands, queue overflow, and CEX/Polymarket
discard were all zero. Runtime and guardian stderr were empty. One durable
guardian snapshot exists at
`D:\poly_alpha_prospective_exact_v6_successor24_20260825T104125Z\operator\guardian_snapshot_1787655579459.json`,
SHA-256 `782fb899847e48395bbd32c7863696d8e0f05d378556323acac1173726374e8d`.

A transient market reconnect/backoff detail is observational: the guardian and
runtime source state at the binding capture were READY, no source row was
discarded, and no guardian violation exists. Treat only a durable guardian
failure artifact as binding.

## Frozen Phase-One gate

- OOS start: `1787655323641` (`2026-08-25T10:55:23.641Z`).
- OOS end: `1787698523639` (`2026-08-25T22:55:23.639Z`).
- Two-hour review: `1787662523639`.
- Four-hour hard review: `1787669723639`.
- Each target requires 300 rank-1 rows, 300 unique markets, 60 positive labels,
  60 negative labels, and zero unlabeled rows.
- At initial capture, ensemble readiness was 15/15/0/0/15 and model readiness
  was 14/14/0/0/14 in rank1/markets/positive/negative/unlabeled order.
- At least 300 sealed decision-replay capsules are required.

Phase One is not ready and must not be finalized early.

## Safety

`LIVE_ENABLED=false`, `REAL_ORDERS_POSSIBLE=false`, wallet signing and
authenticated trading unavailable, real placement/cancellation unavailable,
kill switch engaged, no Phase Two or Phase Three, and `V4-HO-001` remains
nonexistent/unconsumed. Authoritative v5 remains byte-identical and must never
be opened for write or cut over.

## Exact next action

Resume from current disk authority. Perform only a minimal bounded verification
of the same Successor24 session/nonce/PID pair and the latest single guardian
state, then continue Phase-One acquisition under the frozen OOS/readiness
protocol. Preserve any first failure artifact. Do not create a stop request or
terminate the runtime without a distinct exact identity-bound authority.

No calibration finalization, tournament registration/evaluation, Phase Two,
holdout creation/consumption, Phase Three, live/authenticated trading, signing,
orders/cancels, source mutation, source-seal rerun, or authoritative-v5
mutation is authorized.

Codex active work is stopped; Successor24 and its guardian continue
independently.
