# Poly Alpha durable status

Updated: `2026-08-25T18:53:36.4104535+07:00`

## Current boundary

`SUCCESSOR24_HOST_LOSS_INTEGRITY_FAILURE_IDENTIFIED`

Successor24 did not survive the host power loss. Its exact runtime and guardian
are absent after reboot and were not relaunched. The recovered SQLite journal
ends cleanly at committed command 7,431, while the last pre-loss runtime state
records command 7,432 as committed and acknowledged with nothing pending.
Command 7,432 is absent from disk. Successor24 is therefore permanently
ineligible and must never be relaunched, repaired, finalized, calibrated,
pooled, or reused.

Primary evidence and hashes are in
`.codex/SUCCESSOR24_POST_HOST_LOSS_FORENSIC_BOUNDARY.json` (SHA-256
`cf996709d3bf1776f6a5b80c8132441e6a801a3a840efe54e4098176bf978a5e`).

The causal boundary is WAL `synchronous=NORMAL`: the final atomic
domain-plus-journal commit can be acknowledged after SQLite returns yet be
rolled back by physical power loss. Production source is still unchanged at
this checkpoint. Mandatory fail-first TDD and exact Successor24 terminal
closure remain pending.

The deliberate source tree remains preserved at branch `master`, HEAD
`d3364f219feb37a09a547ff1daba6f0f96377fe4`, index tree
`700307bdbc9a4fdab7615d79eea83fe1bf6463cb`, with zero staged paths,
56 tracked modifications, and 108 untracked paths.

Authoritative v5 is independently byte-identical after reboot. Safety remains
fail-closed: live disabled, real orders impossible, signing and authenticated
trading unavailable, kill switch engaged, no Phase Two/Three, and `V4-HO-001`
nonexistent/unconsumed.

Exact next action: observe the smallest mandatory RED for final acknowledged
commit durability, apply and validate the narrow fix, then canonically close
Successor24 under its exact identity before qualifying any fresh successor.
