# Lite Frequency V4 — persistence rearchitecture status

Updated: 2026-07-15 (Fable continuation session). Base commit: `196ee05`.

## Architecture (implemented, uncommitted → being committed this session)

Four explicit SQLite ownership lanes, none of which run on the asyncio event loop:

1. **Critical transactional writer** — `lite_frequency_v4/persistence.py`
   (`V4PersistenceWriter`): dedicated writer thread + connection, bounded priority
   queue, journaled commands with idempotency keys, durable commit before
   acknowledgement, fail-closed on queue-full/ack-timeout, terminal-command priority,
   launch-nonce ownership, startup reconciliation of committed-unacknowledged /
   uncommitted commands.
2. **Telemetry lane** — `lite_frequency_v4/telemetry.py` (`V4TelemetryWriter`
   aggregator thread) + `V4TelemetryStoreSink` (second connection): bounded queue,
   batching, coalescing, dedup, state-signature suppression; intentionally lossy with
   exact loss accounting (`priority_skipped_rows`, `deadline_exceeded_rows`, overflow);
   critical-first write gate — telemetry never blocks critical persistence, and a
   100 ms cooperative transaction deadline stops telemetry from holding the gate.
3. **Read-only reporting** — `lite_frequency_v4/workers.py`: private `mode=ro` URI
   connection with `PRAGMA query_only` invariant enforcement; single-flight reports;
   off-loop dashboard export with atomic `os.replace`; off-loop integrity checks.
4. **Maintenance/checkpoint** — `lite_frequency_v4/maintenance.py`: dedicated
   connection; PASSIVE bounded checkpoints (TRUNCATE only offline-quiescent), busy
   results handled, WAL before/after metrics; chunked retention with row budget
   (4000), chunk size (250) and 1 s wall-clock budget checked inside chunk loops;
   trade/accounting tables never auto-deleted.

Engine hot path (`engine.py`): zero direct `self.store.` calls remain; runtime file
writes go through a bounded runtime-IO worker with atomic replacement; event-loop lag
fails closed (`DEGRADED_EVENT_LOOP_LAG` blocks execution).

## Test status

- All focused V4 suites green: 107 persistence-lane tests + 117 engine/store/runtime/
  export/ingestion/config tests.
- Two stale test expectations repaired in
  `test_slow_telemetry_batch_is_lossy_and_loop_safe` (loss-accounting identity now
  covers priority-skip + cooperative-deadline buckets; warmup row makes evidence
  assertions deterministic).
- Full repository suite + dashboard lint/build: in progress.

## Safety

`dry_run=true`, `live_enabled=false`, `fixed_shares=5`, no live adapter, shadow-only.
No strategy thresholds changed. Secret scan of the full diff: clean.

## Migration

Additive schema hydration happens on store open. Pre-change backup preserved at
`D:\claude\agent_readonly\poly_alpha_frequency_v4\backup_pre_persistence_20260714T202644Z`
(integrity ok, FK 0, entries 76 / positions 76 / exits 74 / pnl 74). Production DB
untouched so far; idempotency will be proven on a copy before first runtime start.
