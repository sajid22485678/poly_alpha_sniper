# Poly Alpha durable status

Updated: `2026-08-25T19:09:31.8145868+07:00`

## Current boundary

`SUCCESSOR24_CAUSAL_DURABILITY_FIX_VERIFIED_CLOSURE_PENDING`

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

Exact next action: build and fail-first test the exact Successor24 host-loss
closure authority, then execute it exactly once before qualifying any fresh
successor.
