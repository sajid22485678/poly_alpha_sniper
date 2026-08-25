# Poly Alpha durable status

Updated: `2026-08-25T19:43:52.8859091+07:00`

## Current boundary

`SUCCESSOR24_HOST_LOSS_CLOSURE_PREVIEW_V2_READY`

Successor24 did not survive the host power loss. Its exact runtime and guardian
are absent after reboot and were not relaunched. The recovered SQLite journal
ends cleanly at committed command 7,431, while the last pre-loss runtime state
records command 7,432 as committed and acknowledged with nothing pending.
Command 7,432 is absent from disk. Successor24 is therefore permanently
ineligible and must never be relaunched, repaired, finalized, calibrated,
pooled, or reused.

Primary fix evidence and hashes are in
`.codex/SUCCESSOR24_POWER_LOSS_DURABILITY_FIX_VERIFICATION.json` (SHA-256
`c7ddd3a9f434dfb958d005846332418effae815b2260f5a4018cb9ed597acaf2`).
The preceding forensic record remains immutable at
`.codex/SUCCESSOR24_POST_HOST_LOSS_FORENSIC_BOUNDARY.json`.

The causal boundary was WAL `synchronous=NORMAL`: the final atomic
domain-plus-journal commit could be acknowledged after SQLite returned yet be
rolled back by physical power loss. Mandatory fail-first TDD proved it. The
narrow fix applies FULL only to the final atomic domain-plus-COMMITTED-journal
transaction, restores NORMAL before acknowledgement, and leaves the earlier
journal transitions on NORMAL. All 539 unique affected, recovery, concurrency,
telemetry, runtime, and broader safety tests pass with zero skips. Exact
Successor24 terminal closure remains pending.

The deliberate source tree remains preserved at branch `master`, HEAD
`d3364f219feb37a09a547ff1daba6f0f96377fe4`, index tree
`700307bdbc9a4fdab7615d79eea83fe1bf6463cb`, with zero staged paths,
56 tracked modifications, and 108 untracked paths.

Authoritative v5 is independently byte-identical after reboot. Safety remains
fail-closed: live disabled, real orders impossible, signing and authenticated
trading unavailable, kill switch engaged, no Phase Two/Three, and `V4-HO-001`
nonexistent/unconsumed.

The sole v1 apply invocation refused before lease acquisition or database
mutation. Exact read-only comparison found one difference only: the raw
Windows `psutil.boot_time()` estimate moved from `1787657510049` to
`1787657510054`. DB/WAL/SHM remain byte-identical to the preview; the journal
is still 7,431 committed rows, the session is open, and no closure row, output,
or lease exists. Preview/apply v1 and its nonce are terminal refused.

Correction preview v2 passed once at
`operator\successor24_host_loss_preview_v2.json`, file SHA-256
`50dbabc03f6561ea26a435ee562962c4eb07f71d796372cf54855409dc60bbc0`,
under distinct nonce `506642682e50407c86f458976726bf8b`. Exact next
action: invoke v2 apply exactly once. Never retry preview/apply v1.
