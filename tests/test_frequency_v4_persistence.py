from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
import hashlib
import json
import threading
import time

import pytest

from poly_alpha_sniper.lite_frequency_v4.persistence import (
    V4PersistenceCommand,
    V4PersistenceIdempotencyConflict,
    V4PersistenceQueueFull,
    V4PersistenceWriter,
    V4TelemetryPrioritySkip,
    V4TelemetryStoreSink,
    _BoundedPriorityScheduler,
    _Envelope,
)
from poly_alpha_sniper.lite_frequency_v4.store import (
    SCHEMA_VERSION,
    V4ReadOnlyStore,
    V4Store,
    V4StoreError,
)
from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    TelemetryDisposition,
    V4TelemetryWriter,
)


NOW = 1_800_000_000_000


def session_row(suffix: str = "one") -> dict:
    return {
        "session_id": f"session-{suffix}",
        "launch_nonce": f"nonce-{suffix}",
        "pid": 1234 + len(suffix),
        "git_commit": "a" * 40,
        "config_hash": "b" * 64,
        "started_ts_ms": NOW,
    }


def market_bundle(suffix: str = "one", *, open_ts: int = NOW) -> dict:
    return {
        "market": {
            "polymarket_market_id": f"market-{suffix}",
            "asset": "BTC",
            "slug": f"btc-updown-{suffix}",
            "question": "BTC Up or Down",
            "duration_ms": 300_000,
            "open_ts_ms": open_ts,
            "close_ts_ms": open_ts + 300_000,
            "status": "ACTIVE",
            "accepting_orders": 1,
            "first_seen_ts_ms": open_ts,
            "last_seen_ts_ms": open_ts,
            "raw_identity_hash": f"identity-{suffix}",
        },
        "identity": {
            "event_id": f"event-{suffix}",
            "condition_id": f"condition-{suffix}",
            "yes_token_id": f"yes-{suffix}",
            "no_token_id": f"no-{suffix}",
            "association_valid": 1,
            "token_pair_valid": 1,
            "ambiguous": 0,
            "verification_reason": "verified",
            "verified_ts_ms": open_ts,
        },
        "window": {
            "asset": "BTC",
            "window_open_ts_ms": open_ts,
            "window_close_ts_ms": open_ts + 300_000,
            "expected": 1,
            "lifecycle_status": "ACTIVE",
            "created_ts_ms": open_ts,
            "updated_ts_ms": open_ts,
        },
        "link": {
            "eligibility_status": "ELIGIBLE",
            "reject_reason": None,
            "selected": 1,
            "linked_ts_ms": open_ts,
        },
        "anchor": {
            "status": "ANCHOR_FIELD_MISSING",
            "price_to_beat": None,
            "source_field": None,
            "parse_error": None,
            "provider_ts_ms": None,
            "receipt_ts_ms": open_ts,
        },
        "funnel": {
            "now_ms": open_ts,
            "changes": {"available": 1, "eligible": 1},
        },
    }


def test_fresh_v2_schema_disables_automatic_checkpoint(tmp_path):
    path = tmp_path / "fresh.db"
    store = V4Store(path)
    try:
        assert store.query_one("PRAGMA user_version") == {"user_version": SCHEMA_VERSION}
        assert store.query_one("PRAGMA wal_autocheckpoint") == {"wal_autocheckpoint": 0}
        tables = {
            row["name"] for row in store.query(
                "SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {"persistence_commands", "persistence_worker_samples", "checkpoint_runs"} <= tables
        assert store.query_one("SELECT COUNT(*) n FROM checkpoint_runs") == {"n": 0}
    finally:
        store.close()
    reopened = V4Store(path)
    try:
        assert reopened.query_one("SELECT COUNT(*) n FROM checkpoint_runs") == {"n": 0}
    finally:
        reopened.close()


def test_additive_v1_to_v2_migration_preserves_rows(tmp_path):
    path = tmp_path / "migrate.db"
    store = V4Store(path)
    store.record_runtime_session(session_row("preserved"))
    with store.transaction(immediate=True) as conn:
        conn.execute("DROP TABLE persistence_commands")
        conn.execute("DROP TABLE persistence_worker_samples")
        conn.execute("DROP TABLE checkpoint_runs")
        conn.execute("DELETE FROM schema_migrations")
        conn.execute(
            "INSERT INTO schema_migrations(version,applied_ts_ms,schema_hash) VALUES(1,0,'v1')")
        conn.execute("PRAGMA user_version=1")
    store.close()

    migrated = V4Store(path)
    try:
        assert migrated.query_one(
            "SELECT session_id FROM runtime_sessions WHERE session_id='session-preserved'"
        ) == {"session_id": "session-preserved"}
        # The chain now continues additively through v3 (Phase 1 cohorts).
        assert migrated.query_one("PRAGMA user_version") == {
            "user_version": SCHEMA_VERSION}
        assert migrated.query_one(
            "SELECT COUNT(*) n FROM schema_migrations WHERE version=2") == {"n": 1}
        assert migrated.query_one(
            "SELECT COUNT(*) n FROM schema_migrations WHERE version=3") == {"n": 1}
        assert migrated.query_one(
            "SELECT COUNT(*) n FROM persistence_commands") == {"n": 0}
    finally:
        migrated.close()


def test_nested_store_calls_share_one_outer_transaction_and_rollback(tmp_path):
    store = V4Store(tmp_path / "nested.db")
    before = store.transaction_counters
    with store.transaction(immediate=True):
        store.record_event_count(
            receipt_ts_ms=NOW, source="okx", channel="ticker", asset="BTC",
            event_type="ticker", classification="NEW_TICK",
            unique=True, duplicate=False, invalid=False)
        store.record_event_count(
            receipt_ts_ms=NOW, source="okx", channel="trades", asset="BTC",
            event_type="trade", classification="NEW_TICK",
            unique=True, duplicate=False, invalid=False)
    after = store.transaction_counters
    assert after["committed"] - before["committed"] == 1
    assert after["nested"] - before["nested"] == 2

    with pytest.raises(RuntimeError, match="rollback"):
        with store.transaction(immediate=True):
            store.record_event_count(
                receipt_ts_ms=NOW + 1_000, source="okx", channel="ticker",
                asset="ETH", event_type="ticker", classification="NEW_TICK",
                unique=True, duplicate=False, invalid=False)
            raise RuntimeError("rollback")
    assert store.query_one(
        "SELECT COUNT(*) n FROM event_buckets WHERE asset='ETH'") == {"n": 0}
    assert store.transaction_counters["rolled_back"] >= 1
    store.close()


def test_strict_connection_owner_and_separate_worker_connection(tmp_path):
    path = tmp_path / "owner.db"
    store = V4Store(path)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(V4StoreError, match="owner thread"):
            pool.submit(store.query_one, "SELECT 1 AS n").result()

        def independently_owned() -> dict:
            other = V4Store(path)
            try:
                return other.query_one("SELECT 1 AS n") or {}
            finally:
                other.close()

        assert pool.submit(independently_owned).result() == {"n": 1}
    store.close()


def test_readonly_store_is_also_thread_affine(tmp_path):
    path = tmp_path / "readonly.db"
    V4Store(path).close()
    reader = V4ReadOnlyStore(path)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(V4StoreError, match="owner thread"):
            pool.submit(reader.query_one, "SELECT 1 AS n").result()
    reader.close()


def test_market_bundle_is_atomic_and_idempotent(tmp_path):
    store = V4Store(tmp_path / "market-bundle.db")
    first = store.persist_market_bundle(market_bundle())
    second = store.persist_market_bundle(market_bundle())
    assert second["market_id"] == first["market_id"]
    assert second["market_identity_id"] == first["market_identity_id"]
    assert second["window_id"] == first["window_id"]
    assert second["anchor_observation_id"] == first["anchor_observation_id"]
    assert second["link_idempotent"] is True
    assert store.query_one("SELECT COUNT(*) n FROM markets") == {"n": 1}
    assert store.query_one("SELECT COUNT(*) n FROM market_identities") == {"n": 1}
    assert store.query_one("SELECT COUNT(*) n FROM anchor_observations") == {"n": 1}
    store.close()


def test_entry_bundle_rolls_back_reservation_when_entry_validation_fails(tmp_path):
    store = V4Store(tmp_path / "entry-bundle.db")
    store.record_runtime_session(session_row())
    refs = store.persist_market_bundle(market_bundle())
    reservation = {
        "window_id": refs["window_id"], "session_id": "session-one",
        "market_identity_id": refs["market_identity_id"],
        "owner_launch_nonce": "nonce-one", "outcome_side": "YES",
        "state": "RESERVED", "idempotency_key": "reservation-one",
        "candidate_id": None, "decision_id": None,
        "reserved_ts_ms": NOW + 1, "updated_ts_ms": NOW + 1,
    }
    with pytest.raises(ValueError, match="maker fill"):
        store.reserve_and_create_entry_bundle({
            "reservation": reservation,
            "entry": {"maker_fill_assumed": True},
            "max_concurrent_positions": 4,
            "global_exposure_cap_usd": 10.0,
            "per_asset_exposure_cap_usd": 10.0,
        })
    assert store.query_one("SELECT COUNT(*) n FROM window_locks") == {"n": 0}
    store.close()


def test_writer_commits_before_ack_and_replays_committed_result_idempotently(tmp_path):
    path = tmp_path / "writer.db"
    command = V4PersistenceCommand(
        command_id="window-create-one", method="ensure_asset_window",
        args=({
            "asset": "BTC", "window_open_ts_ms": NOW,
            "window_close_ts_ms": NOW + 300_000, "expected": 1,
            "lifecycle_status": "DISCOVERING", "created_ts_ms": NOW,
            "updated_ts_ms": NOW,
        },), ordering_key=f"BTC:{NOW}", associated_asset="BTC",
    )
    writer = V4PersistenceWriter(path, sample_interval_s=0.0)
    result = asyncio.run(writer.execute(command, timeout_s=5.0))
    assert isinstance(result, int)
    assert writer.health()["state"] == "HEALTHY"
    assert writer.health()["last_committed_command_id"] == "window-create-one"
    assert writer.health()["commit_latency_ms"]["max"] >= 0.0
    writer.close()

    # A new physical writer sees the committed journal result and never invokes
    # ensure_asset_window a second time.
    replacement = V4PersistenceWriter(path)
    assert replacement.execute_sync(command, timeout_s=5.0) == result
    replacement.close()
    store = V4Store(path)
    try:
        assert store.query_one("SELECT COUNT(*) n FROM asset_windows") == {"n": 1}
        row = store.query_one(
            "SELECT status,transaction_reference FROM persistence_commands "
            "WHERE command_id='window-create-one'")
        assert row["status"] == "COMMITTED"
        assert row["transaction_reference"].startswith("v4tx:window-create-one:")
    finally:
        store.close()


def test_writer_rejects_changed_payload_for_committed_command(tmp_path):
    path = tmp_path / "conflict.db"
    one = V4PersistenceCommand(
        command_id="same-command", method="ensure_asset_window",
        args=({"asset": "BTC", "window_open_ts_ms": NOW,
               "window_close_ts_ms": NOW + 300_000, "expected": 1,
               "lifecycle_status": "DISCOVERING", "created_ts_ms": NOW,
               "updated_ts_ms": NOW},), ordering_key="BTC:one")
    changed = V4PersistenceCommand(
        command_id="same-command", method="ensure_asset_window",
        args=({"asset": "ETH", "window_open_ts_ms": NOW,
               "window_close_ts_ms": NOW + 300_000, "expected": 1,
               "lifecycle_status": "DISCOVERING", "created_ts_ms": NOW,
               "updated_ts_ms": NOW},), ordering_key="ETH:one")
    writer = V4PersistenceWriter(path)
    writer.execute_sync(one, timeout_s=5.0)
    with pytest.raises(V4PersistenceIdempotencyConflict):
        writer.execute_sync(changed, timeout_s=5.0)
    writer.close()


def test_submit_deep_snapshots_nested_payload_before_caller_mutation(
        tmp_path, monkeypatch):
    path = tmp_path / "immutable-admission.db"
    entered = threading.Event()
    release = threading.Event()

    def block_writer(_store, _rows):
        entered.set()
        assert release.wait(3.0)
        return None

    monkeypatch.setattr(V4Store, "record_event_count_batch", block_writer)
    writer = V4PersistenceWriter(path, sample_interval_s=60.0)
    blocker = writer.submit(V4PersistenceCommand(
        command_id="payload-blocker", method="record_event_count_batch",
        args=([],), ordering_key="blocker",
    ))
    assert entered.wait(3.0)

    caller_row = {
        "asset": "BTC", "window_open_ts_ms": NOW,
        "window_close_ts_ms": NOW + 300_000, "expected": 1,
        "lifecycle_status": "DISCOVERING", "created_ts_ms": NOW,
        "updated_ts_ms": NOW, "nested": {"labels": ["original"]},
    }
    command = V4PersistenceCommand(
        command_id="immutable-window", method="ensure_asset_window",
        args=(caller_row,), ordering_key=f"BTC:{NOW}")
    target = writer.submit(command)
    # Mutate both the constructor source and the caller-retained command after
    # admission.  Neither object is the writer's private envelope snapshot.
    caller_row["asset"] = "ETH"
    command.args[0]["asset"] = "SOL"
    command.args[0]["nested"]["labels"].append("mutated")
    release.set()
    blocker.result(timeout=3.0)
    target.result(timeout=3.0)
    writer.close()

    check = V4Store(path)
    try:
        row = check.query_one(
            "SELECT asset FROM asset_windows WHERE window_open_ts_ms=?", (NOW,))
        assert row == {"asset": "BTC"}
        journal = check.query_one(
            "SELECT payload_json FROM persistence_commands "
            "WHERE command_id='immutable-window'")
        payload = json.loads(journal["payload_json"])
        assert payload["args"][0]["asset"] == "BTC"
        assert payload["args"][0]["nested"]["labels"] == ["original"]
    finally:
        check.close()


def test_telemetry_nonblocking_skip_while_critical_commit_is_inflight(
        tmp_path, monkeypatch):
    path = tmp_path / "critical-first-gate.db"
    entered = threading.Event()
    release = threading.Event()

    def block_writer(_store, _rows):
        entered.set()
        assert release.wait(3.0)
        return None

    monkeypatch.setattr(V4Store, "record_event_count_batch", block_writer)
    writer = V4PersistenceWriter(path, sample_interval_s=60.0)
    critical = writer.submit(V4PersistenceCommand(
        command_id="critical-inflight", method="record_event_count_batch",
        args=([],), ordering_key="critical",
    ))
    assert entered.wait(3.0)
    assert writer.critical_write_pending() is True
    assert writer.try_acquire_background_write() is False

    started = time.monotonic()
    with pytest.raises(V4TelemetryPrioritySkip, match="critical persistence"):
        writer.submit_telemetry_batch([{
            "method": "record_event_count",
            "kwargs": {
                "receipt_ts_ms": NOW, "source": "okx", "channel": "ticker",
                "asset": "BTC", "event_type": "ticker",
                "classification": "NEW_TICK", "unique": True,
                "duplicate": False, "invalid": False,
            },
        }], timeout_s=1.0)
    assert time.monotonic() - started < 0.1
    assert writer.metrics()["telemetry_priority_skips"] >= 2

    release.set()
    critical.result(timeout=3.0)
    deadline = time.monotonic() + 1.0
    while writer.critical_write_pending() and time.monotonic() < deadline:
        time.sleep(0.001)
    assert writer.critical_write_pending() is False
    result = writer.submit_telemetry_batch([{
        "method": "record_event_count",
        "kwargs": {
            "receipt_ts_ms": NOW, "source": "okx", "channel": "ticker",
            "asset": "BTC", "event_type": "ticker",
            "classification": "NEW_TICK", "unique": True,
            "duplicate": False, "invalid": False,
        },
    }], timeout_s=1.0)
    assert result == [None]
    assert writer.close_telemetry_sink() is True
    writer.close()


def test_startup_reconciliation_forwards_nonce_proof_and_surfaces_orphans(
        tmp_path, monkeypatch):
    captured = {}
    original = V4Store.reconcile_startup_state

    def reconcile(store, **kwargs):
        captured.update(kwargs)
        return original(store, **kwargs)

    monkeypatch.setattr(V4Store, "reconcile_startup_state", reconcile)
    writer = V4PersistenceWriter(
        tmp_path / "nonce-reconcile.db",
        current_launch_nonce="nonce-current",
        proven_absent_launch_nonces=("nonce-old-b", "nonce-old-a"),
    )
    writer.start()
    health = writer.health()
    assert captured == {
        "current_launch_nonce": "nonce-current",
        "proven_absent_launch_nonces": ("nonce-old-a", "nonce-old-b"),
    }
    for key in (
        "unfinished_maker_observations",
        "reconciled_abandoned_maker_observations",
        "unfinished_makers_left_fail_closed",
    ):
        assert key in health["recovery_reconciliation"]
        assert health[key] == health["recovery_reconciliation"][key]
    writer.close()


def test_failed_journal_finalization_remains_unconfirmed_and_degraded(
        tmp_path, monkeypatch):
    path = tmp_path / "journal-finalization.db"
    writer = V4PersistenceWriter(path, sample_interval_s=60.0)
    monkeypatch.setattr(writer, "_mark_failed", lambda *_args: False)
    command = V4PersistenceCommand(
        command_id="cannot-finalize",
        method="reserve_and_create_entry_bundle",
        args=({
            "reservation": {}, "entry": {}, "max_concurrent_positions": 1,
            "global_exposure_cap_usd": 1.0,
            "per_asset_exposure_cap_usd": 1.0,
            "commit_deadline_ts_ms": int(time.time() * 1_000) - 1,
        },), ordering_key="BTC:broken")
    with pytest.raises(V4StoreError, match="commit deadline expired"):
        writer.execute_sync(command, timeout_s=3.0)
    health = writer.health()
    assert health["raw_state"] == "DEGRADED"
    assert health["healthy"] is False
    assert health["journal_finalization_failures"] == 1
    assert health["unconfirmed_command_count"] >= 1
    assert "JournalFinalizationFailed" in health["last_error"]
    writer.close()

    check = V4Store(path)
    try:
        assert check.query_one(
            "SELECT status FROM persistence_commands "
            "WHERE command_id='cannot-finalize'") == {"status": "EXECUTING"}
    finally:
        check.close()

    replacement = V4PersistenceWriter(path)
    replacement.start()
    assert replacement.health()["recovered_abandoned"] == 1
    replacement.close()


def test_writer_rejects_entry_after_commit_deadline_before_reservation(tmp_path):
    path = tmp_path / "entry-deadline.db"
    seed = V4Store(path)
    seed.record_runtime_session(session_row())
    refs = seed.persist_market_bundle(market_bundle())
    seed.close()
    reservation = {
        "window_id": refs["window_id"], "session_id": "session-one",
        "market_identity_id": refs["market_identity_id"],
        "owner_launch_nonce": "nonce-one", "outcome_side": "YES",
        "state": "RESERVED", "idempotency_key": "expired-reservation",
        "candidate_id": None, "decision_id": None,
        "reserved_ts_ms": NOW + 1, "updated_ts_ms": NOW + 1,
    }
    command = V4PersistenceCommand(
        command_id="expired-entry", method="reserve_and_create_entry_bundle",
        ordering_key=f"BTC:{NOW}", associated_asset="BTC",
        associated_window_id=refs["window_id"], priority=10,
        args=({
            "reservation": reservation,
            # This deliberately malformed entry proves the deadline check runs
            # before either reservation or entry validation/mutation.
            "entry": {"maker_fill_assumed": True},
            "max_concurrent_positions": 4,
            "global_exposure_cap_usd": 10.0,
            "per_asset_exposure_cap_usd": 10.0,
            "commit_deadline_ts_ms": int(time.time() * 1_000) - 1,
        },),
    )
    writer = V4PersistenceWriter(path)
    with pytest.raises(V4StoreError, match="commit deadline expired"):
        writer.execute_sync(command, timeout_s=5.0)
    writer.close()
    check = V4Store(path)
    try:
        assert check.query_one("SELECT COUNT(*) n FROM window_locks") == {"n": 0}
        assert check.query_one(
            "SELECT status,error_type FROM persistence_commands "
            "WHERE command_id='expired-entry'") == {
                "status": "FAILED", "error_type": "V4StoreError"}
    finally:
        check.close()


def test_crash_recovery_marks_uncommitted_command_failed_without_replay(tmp_path):
    path = tmp_path / "recovery.db"
    store = V4Store(path)
    payload = json.dumps({"method": "create_entry", "args": [], "kwargs": {}},
                         sort_keys=True, separators=(",", ":"))
    with store.transaction(immediate=True) as conn:
        conn.execute(
            """INSERT INTO persistence_commands(
               command_id,command_type,method,idempotency_key,ordering_key,
               priority,terminal,payload_hash,payload_json,status,attempt_count,
               submitted_ts_ms)
               VALUES(?,?,?,?,?,?,?,?,?,'SUBMITTED',0,?)""",
            ("abandoned-entry", "STORE_CALL", "create_entry", "abandoned-entry",
             "BTC:abandoned", 10, 1, hashlib.sha256(payload.encode()).hexdigest(),
             payload, NOW),
        )
    store.close()

    writer = V4PersistenceWriter(path)
    writer.start()
    health = writer.health()
    assert health["recovered_abandoned"] == 1
    assert health["recovery_reconciliation"]["consistency_errors"] == 0
    writer.close()
    check = V4Store(path)
    try:
        assert check.query_one(
            "SELECT status,error_type FROM persistence_commands "
            "WHERE command_id='abandoned-entry'") == {
                "status": "FAILED", "error_type": "CrashRecovery"}
        assert check.query_one("SELECT COUNT(*) n FROM entries") == {"n": 0}
    finally:
        check.close()


def test_priority_scheduler_preserves_key_fifo_and_prioritizes_terminal_other_key():
    scheduler = _BoundedPriorityScheduler(4)

    def envelope(command_id: str, key: str, sequence: int, priority: int) -> _Envelope:
        return _Envelope(
            V4PersistenceCommand(
                command_id=command_id, method="record_event_count_batch",
                args=([],), ordering_key=key, priority=priority,
                terminal=priority == 10),
            Future(), sequence, NOW + sequence)

    scheduler.put(envelope("a-normal", "A", 0, 50))
    scheduler.put(envelope("b-normal", "B", 1, 50))
    scheduler.put(envelope("a-terminal", "A", 2, 10))
    scheduler.put(envelope("c-terminal", "C", 3, 10))
    assert [scheduler.get().command.command_id for _ in range(4)] == [
        "c-terminal", "a-normal", "a-terminal", "b-normal"]


def test_explicit_terminal_flag_always_receives_terminal_priority():
    command = V4PersistenceCommand(
        command_id="terminal-flag", method="record_event_count_batch",
        args=([],), ordering_key="terminal", terminal=True, priority=999,
    )
    assert command.terminal is True
    assert command.priority == 10


def test_bounded_scheduler_fails_closed_when_full():
    scheduler = _BoundedPriorityScheduler(1)
    command = V4PersistenceCommand(
        command_id="only", method="record_event_count_batch",
        args=([],), ordering_key="one")
    scheduler.put(_Envelope(command, Future(), 0, NOW))
    with pytest.raises(V4PersistenceQueueFull):
        scheduler.put(_Envelope(command, Future(), 1, NOW + 1))


def test_telemetry_sink_accepts_mapping_batch_on_separate_owned_connection(tmp_path):
    path = tmp_path / "telemetry.db"
    seed = V4Store(path)
    seed.record_runtime_session(session_row())
    seed.close()
    sink = V4TelemetryStoreSink(path)
    result = sink.submit_telemetry_batch([
        {
            "method": "record_event_count",
            "kwargs": {
                "receipt_ts_ms": NOW, "source": "okx", "channel": "ticker",
                "asset": "BTC", "event_type": "ticker", "classification": "NEW_TICK",
                "unique": True, "duplicate": False, "invalid": False,
            },
        },
        {
            "method": "record_source_health",
            "args": ({
                "session_id": "session-one", "source": "okx", "channel": "public",
                "sample_ts_ms": NOW, "status": "READY", "connected": 1,
                "hydrated": 1,
            },),
        },
        {
            "method": "record_runtime_health",
            "args": ({
                "session_id": "session-one", "sample_ts_ms": NOW,
                "heartbeat_ts_ms": NOW, "pid": 1235, "state": "RUNNING",
            },),
        },
    ], timeout_s=5.0)
    assert result == [None, 1, 1]
    health = sink.health()
    assert health["state"] == "HEALTHY"
    assert health["last_outer_transactions"] == 1
    sink.close()
    check = V4Store(path)
    try:
        assert check.query_one("SELECT SUM(raw_count) n FROM event_buckets") == {"n": 1}
        assert check.query_one("SELECT COUNT(*) n FROM source_health") == {"n": 1}
        assert check.query_one("SELECT COUNT(*) n FROM runtime_health") == {"n": 1}
        assert check.query_one("SELECT COUNT(*) n FROM persistence_commands") == {"n": 0}
    finally:
        check.close()


def test_telemetry_worker_closes_lazy_sink_on_its_owner_thread(tmp_path):
    path = tmp_path / "telemetry-lifecycle.db"
    V4Store(path).close()
    persistence = V4PersistenceWriter(path)
    telemetry = V4TelemetryWriter(
        persistence, capacity=8, batch_size=4, flush_interval_s=0.01,
        coalescing_interval_s=60.0, submit_timeout_s=1.0,
        heartbeat_interval_s=0.01,
    )
    assert telemetry.submit(
        "record_event_count",
        kwargs={
            "receipt_ts_ms": NOW, "source": "okx", "channel": "ticker",
            "asset": "BTC", "event_type": "ticker",
            "classification": "NEW_TICK", "unique": True,
            "duplicate": False, "invalid": False,
        },
    ) is TelemetryDisposition.ACCEPTED
    telemetry.start()
    assert telemetry.stop(drain=True, timeout_s=3.0)
    # The hook ran before STOPPED on the aggregation thread.  A caller-side
    # close is a no-op instead of an illegal cross-thread SQLite close.
    assert persistence.close_telemetry_sink() is False
    persistence.close()
    check = V4Store(path)
    try:
        assert check.query_one("SELECT SUM(raw_count) n FROM event_buckets") == {"n": 1}
    finally:
        check.close()


def test_explicit_checkpoint_records_full_wal_evidence(tmp_path):
    store = V4Store(tmp_path / "checkpoint.db")
    store.record_event_count(
        receipt_ts_ms=NOW, source="okx", channel="ticker", asset="BTC",
        event_type="ticker", classification="NEW_TICK",
        unique=True, duplicate=False, invalid=False)
    result = store.checkpoint(mode="PASSIVE", reason="focused_test")
    assert result["success"] is True
    row = store.query_one("SELECT * FROM checkpoint_runs")
    assert row["reason"] == "focused_test"
    assert row["before_wal_bytes"] >= row["after_wal_bytes"]
    assert row["duration_ms"] >= 0
    assert row["frames_total"] >= row["frames_checkpointed"]
    store.close()


def test_latest_source_health_can_be_scoped_to_session(tmp_path):
    store = V4Store(tmp_path / "health.db")
    store.record_runtime_session(session_row("one"))
    store.record_runtime_session(session_row("two"))
    for session, timestamp, status in (
        ("session-one", NOW, "READY_ONE"),
        ("session-two", NOW + 1, "READY_TWO"),
    ):
        store.record_source_health({
            "session_id": session, "source": "okx", "channel": "public",
            "sample_ts_ms": timestamp, "status": status,
            "connected": 1, "hydrated": 1,
        })
    assert store.latest_source_health()[0]["status"] == "READY_TWO"
    assert store.latest_source_health("session-one")[0]["status"] == "READY_ONE"
    store.close()


def test_reconciliation_releases_only_explicitly_proven_stale_reservation(tmp_path):
    store = V4Store(tmp_path / "reconcile.db")
    store.record_runtime_session(session_row("old"))
    refs = store.persist_market_bundle(market_bundle("old"))
    store.reserve_window({
        "window_id": refs["window_id"], "session_id": "session-old",
        "market_identity_id": refs["market_identity_id"],
        "owner_launch_nonce": "nonce-old", "outcome_side": "YES",
        "state": "RESERVED", "idempotency_key": "old-reservation",
        "candidate_id": None, "decision_id": None,
        "reserved_ts_ms": NOW + 1, "updated_ts_ms": NOW + 1,
    })
    first = store.reconcile_startup_state()
    assert first["released_proven_stale_reservations"] == 0
    assert first["reserved_left_fail_closed"] == 1
    second = store.reconcile_startup_state(
        current_launch_nonce="nonce-current",
        proven_absent_launch_nonces=("nonce-old",),
        reconciled_ts_ms=NOW + 2)
    assert second["released_proven_stale_reservations"] == 1
    assert store.query_one("SELECT COUNT(*) n FROM window_locks") == {"n": 0}
    store.close()


def _event_count_batch_row(offset: int = 0) -> dict:
    return {
        "receipt_ts_ms": NOW + offset, "source": "okx", "channel": "ticker",
        "asset": "BTC", "event_type": "ticker", "classification": "NEW_TICK",
        "raw_count": 3, "unique_count": 3, "duplicate_count": 0,
        "invalid_count": 0,
    }


_COMPACT_PREFIX = '{"compacted":true,'


def test_evidence_payload_tombstones_atomically_with_commit_only(tmp_path):
    path = tmp_path / "compact-on-commit.db"
    writer = V4PersistenceWriter(path, sample_interval_s=0.0)
    evidence = V4PersistenceCommand(
        command_id="evidence-compact-1", method="record_event_count_batch",
        args=([_event_count_batch_row()],), ordering_key="evidence",
        command_type="ENTRY_DECISION_EVIDENCE",
    )
    terminal_evidence = V4PersistenceCommand(
        command_id="evidence-terminal-1", method="record_event_count_batch",
        args=([_event_count_batch_row(1)],), ordering_key="evidence",
        command_type="ENTRY_DECISION_EVIDENCE", terminal=True,
    )
    trade_linked = V4PersistenceCommand(
        command_id="evidence-trade-linked-1",
        method="record_event_count_batch",
        args=([_event_count_batch_row(2)],), ordering_key="evidence",
        command_type="ENTRY_DECISION_EVIDENCE", associated_trade_id=7,
    )
    trade_critical_method = V4PersistenceCommand(
        command_id="window-keep-1", method="ensure_asset_window",
        args=({
            "asset": "BTC", "window_open_ts_ms": NOW,
            "window_close_ts_ms": NOW + 300_000, "expected": 1,
            "lifecycle_status": "DISCOVERING", "created_ts_ms": NOW,
            "updated_ts_ms": NOW,
        },), ordering_key="BTC:compact",
        command_type="ENTRY_DECISION_EVIDENCE",
    )
    result = writer.execute_sync(evidence, timeout_s=5.0)
    writer.execute_sync(terminal_evidence, timeout_s=5.0)
    writer.execute_sync(trade_linked, timeout_s=5.0)
    writer.execute_sync(trade_critical_method, timeout_s=5.0)
    assert writer.metrics()["journal_payloads_compacted_on_commit"] == 1
    writer.close()

    store = V4Store(path)
    try:
        compacted = store.query_one(
            "SELECT status,payload_json,payload_hash,result_json "
            "FROM persistence_commands WHERE command_id='evidence-compact-1'")
        assert compacted["status"] == "COMMITTED"
        assert compacted["payload_json"].startswith(_COMPACT_PREFIX)
        assert json.loads(compacted["payload_json"]) == {
            "compacted": True, "payload_hash": compacted["payload_hash"]}
        assert compacted["result_json"] is not None
        for retained_id in ("evidence-terminal-1", "evidence-trade-linked-1",
                            "window-keep-1"):
            row = store.query_one(
                "SELECT status,payload_json FROM persistence_commands "
                "WHERE command_id=?", (retained_id,))
            assert row["status"] == "COMMITTED"
            assert not row["payload_json"].startswith(_COMPACT_PREFIX)
            assert json.loads(row["payload_json"])["method"]
    finally:
        store.close()

    # Idempotent replay of a compacted command still returns the committed
    # result deterministically from the journal without re-executing it.
    replacement = V4PersistenceWriter(path)
    assert replacement.execute_sync(evidence, timeout_s=5.0) == result
    assert replacement.metrics()["idempotent_replays"] == 1
    replacement.close()


def test_failed_and_inflight_evidence_payloads_are_never_compacted(
        tmp_path, monkeypatch):
    path = tmp_path / "compact-failed.db"

    def boom(_store, _rows):
        raise ValueError("evidence write failed")

    monkeypatch.setattr(V4Store, "record_event_count_batch", boom)
    writer = V4PersistenceWriter(path, sample_interval_s=0.0)
    failing = V4PersistenceCommand(
        command_id="evidence-fail-1", method="record_event_count_batch",
        args=([_event_count_batch_row()],), ordering_key="evidence",
        command_type="ENTRY_DECISION_EVIDENCE",
    )
    with pytest.raises(ValueError, match="evidence write failed"):
        writer.execute_sync(failing, timeout_s=5.0)
    writer.close()
    store = V4Store(path)
    try:
        row = store.query_one(
            "SELECT status,payload_json,error_type FROM persistence_commands "
            "WHERE command_id='evidence-fail-1'")
        assert row["status"] == "FAILED"
        assert row["error_type"] == "ValueError"
        assert not row["payload_json"].startswith(_COMPACT_PREFIX)
        assert json.loads(row["payload_json"])["method"] == (
            "record_event_count_batch")
    finally:
        store.close()

    # An in-flight (SUBMITTED/EXECUTING) command retains its full recoverable
    # payload while the mutation is still executing.
    monkeypatch.undo()
    entered = threading.Event()
    release = threading.Event()

    def block_writer(_store, _rows):
        entered.set()
        assert release.wait(5.0)
        return None

    monkeypatch.setattr(V4Store, "record_event_count_batch", block_writer)
    blocked_path = tmp_path / "compact-inflight.db"
    blocked_writer = V4PersistenceWriter(blocked_path, sample_interval_s=60.0)
    inflight = blocked_writer.submit(V4PersistenceCommand(
        command_id="evidence-inflight-1", method="record_event_count_batch",
        args=([_event_count_batch_row()],), ordering_key="evidence",
        command_type="ENTRY_DECISION_EVIDENCE",
    ))
    assert entered.wait(3.0)
    observer = V4Store(blocked_path)
    try:
        row = observer.query_one(
            "SELECT status,payload_json FROM persistence_commands "
            "WHERE command_id='evidence-inflight-1'")
        assert row["status"] in {"SUBMITTED", "EXECUTING"}
        assert not row["payload_json"].startswith(_COMPACT_PREFIX)
        assert json.loads(row["payload_json"])["method"] == (
            "record_event_count_batch")
    finally:
        observer.close()
    release.set()
    assert inflight.result(timeout=5.0) is None
    blocked_writer.close()


def test_unconfirmed_gate_counts_only_trade_critical_commands(
        tmp_path, monkeypatch):
    path = tmp_path / "scoped-unconfirmed.db"
    entered = threading.Event()
    release = threading.Event()

    def block_writer(_store, _rows):
        entered.set()
        assert release.wait(5.0)
        return None

    monkeypatch.setattr(V4Store, "record_event_count_batch", block_writer)
    writer = V4PersistenceWriter(path, sample_interval_s=60.0)
    evidence = writer.submit(V4PersistenceCommand(
        command_id="scoped-evidence-1", method="record_event_count_batch",
        args=([_event_count_batch_row()],), ordering_key="evidence",
        command_type="ENTRY_DECISION_EVIDENCE",
    ))
    assert entered.wait(3.0)
    metrics = writer.metrics()
    assert metrics["unconfirmed_command_count"] >= 1
    assert metrics["unconfirmed_trade_critical_count"] == 0

    # A queued trade-critical command is visible to the execution gate
    # immediately at submission, before journal admission, with no gap.
    critical = writer.submit(V4PersistenceCommand(
        command_id="scoped-critical-1", method="record_event_count_batch",
        args=([_event_count_batch_row(1)],), ordering_key="critical",
        terminal=True,
    ))
    metrics = writer.metrics()
    assert metrics["unconfirmed_trade_critical_count"] >= 1

    release.set()
    assert evidence.result(timeout=5.0) is None
    assert critical.result(timeout=5.0) is None
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        metrics = writer.metrics()
        if (metrics["unconfirmed_trade_critical_count"] == 0
                and metrics["unconfirmed_command_count"] == 0):
            break
        time.sleep(0.005)
    # Counters return exactly to zero: no residue, no negative values, no
    # double decrements.
    assert metrics["unconfirmed_trade_critical_count"] == 0
    assert metrics["unconfirmed_command_count"] == 0
    writer.close()
    final = writer.metrics()
    assert final["unconfirmed_trade_critical_count"] == 0
    assert final["unconfirmed_command_count"] == 0


def test_failed_trade_critical_command_finalizes_scoped_unconfirmed(
        tmp_path, monkeypatch):
    path = tmp_path / "scoped-failed.db"

    def boom(_store, _rows):
        raise ValueError("critical write failed")

    monkeypatch.setattr(V4Store, "record_event_count_batch", boom)
    writer = V4PersistenceWriter(path, sample_interval_s=0.0)
    with pytest.raises(ValueError, match="critical write failed"):
        writer.execute_sync(V4PersistenceCommand(
            command_id="scoped-critical-fail-1",
            method="record_event_count_batch",
            args=([_event_count_batch_row()],), ordering_key="critical",
            terminal=True,
        ), timeout_s=5.0)
    metrics = writer.metrics()
    assert metrics["commands_failed"] == 1
    # The durable FAILED finalization releases the unconfirmed hold exactly
    # once; the engine latches the failure separately and stays fail-closed.
    assert metrics["unconfirmed_trade_critical_count"] == 0
    assert metrics["unconfirmed_command_count"] == 0
    store = V4Store(path)
    try:
        row = store.query_one(
            "SELECT status,payload_json FROM persistence_commands "
            "WHERE command_id='scoped-critical-fail-1'")
        assert row["status"] == "FAILED"
        assert not row["payload_json"].startswith(_COMPACT_PREFIX)
    finally:
        store.close()
    writer.close()
