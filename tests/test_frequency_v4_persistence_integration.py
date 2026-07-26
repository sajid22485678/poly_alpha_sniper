"""Production-lifecycle integration checks for Frequency V4 persistence.

These tests deliberately use :class:`FrequencyV4Engine.start` and ``stop``.
Only the public network adapters are replaced; the critical writer, telemetry
aggregator, read/report worker, maintenance worker, runtime-I/O worker, schema
migration, integrity check, and dashboard export are the production objects.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import inspect
import json
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, AsyncIterator, Callable

import pytest

from poly_alpha_sniper.lite_frequency_v4 import engine as engine_module
from poly_alpha_sniper.lite_frequency_v4 import export as export_module
from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config
from poly_alpha_sniper.lite_frequency_v4.contracts import (
    BookLevel,
    BookState,
    CexObservation,
    MarketIdentity,
)
from poly_alpha_sniper.lite_frequency_v4.discovery import DiscoveryBatch
from poly_alpha_sniper.lite_frequency_v4.edge_models import EnsembleResult
from poly_alpha_sniper.lite_frequency_v4.engine import (
    EvaluationTrigger,
    FrequencyV4Engine,
)
from poly_alpha_sniper.lite_frequency_v4.runtime import (
    V4RuntimeFiles,
    immutable_safety_state,
    now_ms,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store


class _FakePolymarketSource:
    """No-network stand-in retaining the production adapter surface."""

    source = "polymarket"

    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.subscriptions: dict[str, str] = {}

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def set_subscriptions(self, values: dict[str, str]) -> None:
        self.subscriptions = dict(values)

    async def accept_rest_book(self, _payload: dict[str, Any]) -> None:
        return

    def current_book(self, _token_id: str) -> None:
        return None

    def health(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "state": "READY" if self.started and not self.stopped else "STOPPED",
            "connected": self.started and not self.stopped,
            "desired_subscriptions": len(self.subscriptions),
            "hydrated_subscriptions": len(self.subscriptions),
            "reconnect_count": 0,
        }


class _FakeOkxSource:
    source = "okx"

    def __init__(self, assets: list[str]) -> None:
        self.assets = tuple(assets)
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def set_assets(self, assets: Any) -> None:
        self.assets = tuple(sorted(str(asset) for asset in assets))

    async def hydrate_all(self) -> list[Any]:
        await asyncio.sleep(0)
        return []

    @property
    def health(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "state": "READY" if self.started and not self.stopped else "STOPPED",
            "connected": self.started and not self.stopped,
            "assets": list(self.assets),
            "hydrated_assets": [],
            "reconnect_count": 0,
        }


class _EmptyDiscovery:
    async def discover(self, current: int) -> DiscoveryBatch:
        return DiscoveryBatch(generated_ts_ms=int(current))


def _test_config(tmp_path: Path) -> FrequencyV4Config:
    cfg = FrequencyV4Config()
    cfg.db_path = str(tmp_path / "data" / "poly_alpha_frequency_v4.db")
    cfg.runtime_dir = str(
        tmp_path / "runtime" / "lite_frequency_v4_shadow"
    )
    cfg.export_dir = str(
        tmp_path / "export" / "poly_alpha_frequency_v4"
    )
    cfg.critical_queue_capacity = 64
    cfg.telemetry_queue_capacity = 256
    cfg.telemetry_batch_size = 32
    cfg.telemetry_flush_interval_ms = 20
    cfg.telemetry_coalescing_interval_ms = 100
    cfg.writer_heartbeat_interval_ms = 100
    cfg.writer_failure_timeout_ms = 2_000
    cfg.reporting_queue_capacity = 8
    cfg.maintenance_queue_capacity = 4
    cfg.critical_command_timeout_s = 5.0
    cfg.reporting_worker_timeout_s = 5.0
    cfg.maintenance_worker_timeout_s = 5.0
    cfg.shutdown_drain_timeout_s = 1.0
    return cfg


def _build_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    FrequencyV4Engine, V4RuntimeFiles,
    _FakePolymarketSource, _FakeOkxSource,
]:
    # Path locks intentionally reject temp directories in production config.
    # The integration test changes only those paths and bypasses validation at
    # the constructor seam; every immutable live-safety value remains intact.
    monkeypatch.setattr(
        engine_module, "validate_frequency_v4_config", lambda _cfg: None
    )
    cfg = _test_config(tmp_path)
    # C1.H strict canonical containment: the export writer accepts writes only
    # under canonical_export_dir().  This integration test writes the dashboard
    # to a tmp_path-derived cfg.export_dir, so repoint the single canonical
    # derivation at that tmp root -- the same test-only seam used by
    # test_frequency_v4_export.py.  No production guard is loosened and no
    # cfg.export_dir override is added; production still requires canonical-
    # only export.  (This file is part of the authoritative ten-path primary
    # C1 commit inventory; the adaptation remains strictly test-only.)
    monkeypatch.setattr(
        export_module, "canonical_export_dir", lambda: Path(cfg.export_dir)
    )
    runtime = V4RuntimeFiles(cfg.runtime_dir, repo_root=tmp_path)
    runtime.acquire()
    monkeypatch.setattr(runtime, "process_ownership", lambda: {
        "process_ownership_valid": True,
        "exact_v4_processes": 1,
        "owned_v4_processes": 1,
        "orphan_processes": 0,
        "exact_pids": [runtime.pid],
    })
    runtime.verify_process_ownership()
    engine = FrequencyV4Engine(cfg, runtime)
    polymarket = _FakePolymarketSource()
    okx = _FakeOkxSource(cfg.required_assets)
    engine.discovery = _EmptyDiscovery()
    engine.poly_ws = polymarket
    engine.okx = okx
    return engine, runtime, polymarket, okx


@asynccontextmanager
async def _running_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[FrequencyV4Engine, V4RuntimeFiles]]:
    engine, runtime, _, _ = _build_engine(tmp_path, monkeypatch)
    try:
        await engine.start()
        yield engine, runtime
    finally:
        if engine._started and not engine._stop_complete:
            await engine.stop("integration_test_stop")
        final = engine._runtime_state("STOPPED") if engine._started else None
        runtime.release(final)


async def _scheduler_probe(
    *, duration_s: float = 0.20, interval_s: float = 0.005,
) -> tuple[int, float]:
    """A small PONG-like cadence that detects event-loop starvation."""

    ticks = 0
    largest_gap = 0.0
    prior = time.monotonic()
    deadline = prior + duration_s
    while time.monotonic() < deadline:
        await asyncio.sleep(interval_s)
        current = time.monotonic()
        largest_gap = max(largest_gap, current - prior)
        prior = current
        ticks += 1
    return ticks, largest_gap


def _assert_responsive(probe: tuple[int, float]) -> None:
    ticks, largest_gap = probe
    # Windows timer granularity and the engine's concurrently starting report
    # task can reduce the raw tick count. A synchronous 150 ms persistence call
    # would still create a >=150 ms gap, which this bound detects directly.
    assert ticks >= 6
    assert largest_gap < 0.12


def _runtime_health_row(engine: FrequencyV4Engine, sample: int) -> dict[str, Any]:
    timestamp = now_ms() + int(sample)
    return {
        "session_id": engine.session_id,
        "sample_ts_ms": timestamp,
        "heartbeat_ts_ms": timestamp,
        "pid": engine.runtime.pid,
        "state": "RUNNING",
        "loop_lag_ms": 0.0,
        "db_writes_per_min": 0,
        "db_size_bytes": 0,
        "open_positions": 0,
        "last_error": None,
    }


def _identity(current: int) -> MarketIdentity:
    opening = current - 30_000
    return MarketIdentity(
        asset="BTC",
        slug=f"btc-updown-5m-{opening // 1000}",
        market_id="integration-market",
        event_id="integration-event",
        condition_id="integration-condition",
        yes_token_id="integration-yes",
        no_token_id="integration-no",
        window_open_ms=opening,
        window_close_ms=opening + 300_000,
    )


def _neutral_book(
    identity: MarketIdentity, side: str, current: int,
) -> BookState:
    token_id = (
        identity.yes_token_id if side == "YES" else identity.no_token_id
    )
    return BookState(
        token_id=token_id,
        condition_id=identity.condition_id,
        market_id=identity.market_id,
        bids=(BookLevel(0.49, 20.0),),
        asks=(BookLevel(0.50, 20.0),),
        provider_ts_ms=current,
        receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(),
        source="integration_public_book",
        event_id=f"integration-book-{side.lower()}",
        payload_hash=("a" if side == "YES" else "b") * 64,
        connection_epoch=1,
        min_order_size=5.0,
        tick_size=0.01,
        hydrated=True,
    )


def _cex_observation(current: int) -> CexObservation:
    return CexObservation(
        provider="okx",
        asset="BTC",
        instrument="BTC-USDT",
        price=100_000.0,
        bid=99_999.0,
        ask=100_001.0,
        provider_ts_ms=current,
        receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(),
        event_id="integration-neutral-tick",
        event_type="ticker",
        connection_epoch=1,
    )


@pytest.mark.asyncio
async def test_production_start_stop_owns_all_blocking_resources_off_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_thread = threading.get_ident()
    connection_threads: list[int] = []
    runtime_publish_threads: list[int] = []
    original_connect = sqlite3.connect

    def tracking_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection_threads.append(threading.get_ident())
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", tracking_connect)
    engine, runtime, polymarket, okx = _build_engine(tmp_path, monkeypatch)
    original_publish = runtime.publish

    def tracking_publish(state: dict[str, Any]) -> dict[str, Any]:
        runtime_publish_threads.append(threading.get_ident())
        return original_publish(state)

    def deterministic_ownership() -> dict[str, Any]:
        runtime_publish_threads.append(threading.get_ident())
        return {
            "process_ownership_valid": True,
            "exact_v4_processes": 1,
            "owned_v4_processes": 1,
            "orphan_processes": 0,
            "exact_pids": [runtime.pid],
        }

    monkeypatch.setattr(runtime, "publish", tracking_publish)
    monkeypatch.setattr(runtime, "process_ownership", deterministic_ownership)

    released = False
    try:
        await engine.start()
        assert engine._last_integrity_ok is True
        assert engine._integrity_runs >= 1
        assert engine._execution_blocked_reason() == ""
        assert polymarket.started is True
        assert okx.started is True

        # Force creation of the physical telemetry connection, then capture all
        # owners while every production worker is live.
        assert engine._telemetry_submit(
            "record_runtime_health", _runtime_health_row(engine, 1),
            state_key=("integration-runtime-health", engine.session_id),
            state_value={"state": "RUNNING"},
        )
        assert await asyncio.to_thread(engine.telemetry.flush, timeout_s=5.0)
        telemetry_sink = engine.persistence._telemetry_sink
        assert telemetry_sink is not None

        critical_owner = int(engine.persistence.health()["worker_thread_id"])
        telemetry_owner = int(telemetry_sink.health()["owner_thread_id"])
        read_owner = int(engine.read_worker.health()["owner_thread_id"])
        report_owner = int(engine.report_worker.health()["owner_thread_id"])
        maintenance_owner = int(
            engine.maintenance_worker.health()["owner_thread_id"]
        )
        runtime_owner = int(engine.runtime_io_worker.health()["owner_thread_id"])
        sqlite_owners = {
            critical_owner, telemetry_owner, read_owner, report_owner,
            maintenance_owner,
        }
        assert main_thread not in sqlite_owners
        assert len(sqlite_owners) == 5
        assert runtime_owner not in sqlite_owners
        assert set(connection_threads) == sqlite_owners
        assert runtime_publish_threads
        assert set(runtime_publish_threads) == {runtime_owner}
        pragma = await engine.read_worker.query_one("PRAGMA wal_autocheckpoint")
        assert pragma is not None
        assert list(pragma.values()) == [0]

        state = json.loads(runtime.state_path.read_text(encoding="utf-8"))
        assert state["mode"] == "lite_frequency_v4_shadow"
        assert state["dry_run"] is True
        assert state["live_enabled"] is False
        assert state["real_orders_possible"] is False
        assert state["fixed_shares"] == 5.0
        assert Path(engine.cfg.db_path).exists()
        assert runtime.heartbeat_path.exists()

        await engine.stop("production_lifecycle_test")
        final = engine._runtime_state("STOPPED")
        runtime.release(final)
        released = True

        assert polymarket.stopped is True
        assert okx.stopped is True
        assert engine.telemetry.snapshot()["health"] == "STOPPED"
        assert engine.persistence.health()["raw_state"] == "STOPPED"
        assert engine.read_worker.health()["state"] == "STOPPED"
        assert engine.report_worker.health()["state"] == "STOPPED"
        assert engine.maintenance_worker.health()["state"] == "STOPPED"
        assert engine.runtime_io_worker.health()["state"] == "STOPPED"
        stopped_state = json.loads(
            runtime.state_path.read_text(encoding="utf-8")
        )
        assert stopped_state["running"] is False
        assert not runtime.lock_path.exists()
        assert engine.export_path.exists()

        # Runtime is stopped, so an ordinary inspection connection is safe.
        with original_connect(engine.cfg.db_path) as connection:
            session = connection.execute(
                "SELECT ended_ts_ms,stop_reason FROM runtime_sessions "
                "WHERE session_id=?", (engine.session_id,),
            ).fetchone()
        assert session is not None
        assert int(session[0]) > 0
        assert session[1] == "production_lifecycle_test"
    finally:
        if engine._started and not engine._stop_complete:
            await engine.stop("failed_lifecycle_test_cleanup")
        if not released:
            final = engine._runtime_state("STOPPED") if engine._started else None
            runtime.release(final)


@pytest.mark.asyncio
async def test_slow_critical_commit_ack_never_starves_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        # Let the intentionally immediate startup report/maintenance passes
        # leave the timing sample before introducing the controlled slow call.
        await asyncio.sleep(0.10)
        original = V4Store.ensure_asset_window

        def slow_window(store: V4Store, value: Any) -> int:
            if dict(value).get("asset") == "SLOW":
                time.sleep(0.15)
            return original(store, value)

        monkeypatch.setattr(V4Store, "ensure_asset_window", slow_window)
        current = now_ms()
        opening = current // 300_000 * 300_000 + 3_000_000
        command = engine._critical_execute(
            "ensure_asset_window",
            {
                "asset": "SLOW",
                "window_open_ts_ms": opening,
                "window_close_ts_ms": opening + 300_000,
                "expected": 1,
                "lifecycle_status": "DISCOVERING",
                "created_ts_ms": current,
                "updated_ts_ms": current,
            },
            ordering_key=f"SLOW:{opening}",
            idempotency_key=f"integration-slow-window:{opening}",
        )
        window_id, probe = await asyncio.gather(command, _scheduler_probe())
        assert int(window_id) > 0
        _assert_responsive(probe)
        health = engine.persistence.health()
        assert health["commit_latency_ms"]["max"] >= 120.0
        assert health["commands_failed"] == 0


@pytest.mark.asyncio
async def test_slow_telemetry_batch_is_lossy_and_loop_safe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        # Durably commit one fast warmup row before slowing the sink, so the
        # later "telemetry did write evidence" assertions are deterministic
        # instead of racing the engine's first heartbeat flush.
        for attempt in range(10):
            assert engine._telemetry_submit(
                "record_runtime_health", _runtime_health_row(engine, -(attempt + 1)),
                state_key=("slow-telemetry-warmup", engine.session_id, attempt),
                state_value={"state": "WARMUP"},
            )
            assert await asyncio.to_thread(engine.telemetry.flush, timeout_s=5.0)
            warmup = await engine.read_worker.query_one(
                "SELECT COUNT(*) AS n FROM runtime_health WHERE session_id=?",
                (engine.session_id,),
            )
            if warmup is not None and int(warmup["n"]) >= 1:
                break
        else:
            pytest.fail("warmup telemetry row never committed")
        original = V4Store.record_runtime_health

        def slow_runtime_health(store: V4Store, value: Any) -> int:
            time.sleep(0.15)
            return original(store, value)

        monkeypatch.setattr(V4Store, "record_runtime_health", slow_runtime_health)
        admitted = 0
        for index in range(100):
            admitted += int(engine._telemetry_submit(
                "record_runtime_health", _runtime_health_row(engine, index),
                state_key=("slow-telemetry", engine.session_id),
                state_value={"state": "RUNNING"},
            ))
        assert admitted == 100

        flush = asyncio.to_thread(engine.telemetry.flush, timeout_s=5.0)
        flushed, probe = await asyncio.gather(flush, _scheduler_probe())
        assert flushed is True
        _assert_responsive(probe)
        telemetry = engine.telemetry.snapshot()
        assert telemetry["submitted"] >= 100
        assert telemetry["coalesced"] + telemetry["deduplicated"] >= 90
        # A dispatched slow batch must fail closed in one of two explicit,
        # separately counted ways: yielding to newly pending critical
        # persistence, or exceeding the cooperative per-transaction deadline
        # that stops telemetry from holding the shared write gate.  Either
        # way the loss is accounted exactly, never silently.
        assert telemetry["overflow_count"] == 0
        assert telemetry["dropped"] >= 1
        assert telemetry["dropped"] == (
            telemetry["priority_skipped_rows"]
            + telemetry["deadline_exceeded_rows"])
        assert telemetry["failed_batches"] == (
            telemetry["priority_skipped_batches"]
            + telemetry["deadline_exceeded_batches"])
        assert telemetry["batches"] >= 1
        assert telemetry["batch_latency_max_ms"] >= 120.0
        count = await engine.read_worker.query_one(
            "SELECT COUNT(*) AS n FROM runtime_health WHERE session_id=?",
            (engine.session_id,),
        )
        assert count is not None
        assert 1 <= int(count["n"]) <= 10


@pytest.mark.asyncio
async def test_deferred_reclamation_keeps_the_escalation_armed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A checkpoint that never ran must not cancel its own trigger.

    The background write gate refuses whenever a critical command is in
    flight, so an escalated TRUNCATE is frequently deferred.  Treating that
    deferral as progress reset the no-progress counter, and the policy cycled
    escalate -> deferred -> re-arm forever while the WAL grew unbounded.
    """

    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        engine._consecutive_no_progress_passive = 0
        engine._checkpoint_escalations = 0
        engine._wal_bytes_reclaimed_total = 0

        def passive(before: int, after: int) -> dict[str, Any]:
            return {"mode": "PASSIVE", "status": "SUCCESS",
                    "before_wal_bytes": before, "after_wal_bytes": after}

        # Full frame backfill, not one byte back: that is no progress.
        engine._apply_checkpoint_result(passive(200_000_000, 200_016_480))
        assert engine._consecutive_no_progress_passive == 1
        engine._apply_checkpoint_result(passive(240_000_000, 240_000_000))
        assert engine._consecutive_no_progress_passive == 2

        # The escalation is decided but the gate refuses it.
        engine._apply_checkpoint_result({
            "mode": "TRUNCATE", "status": "SKIPPED",
            "reason": "critical_write_pending",
            "before_wal_bytes": 260_000_000, "after_wal_bytes": 260_000_000,
        })
        assert engine._consecutive_no_progress_passive == 2, (
            "a deferred checkpoint must leave the escalation armed")
        assert engine._checkpoint_escalations == 0

        # It runs but loses the race for the locks: still armed, still no bytes.
        engine._apply_checkpoint_result({
            "mode": "TRUNCATE", "status": "BUSY",
            "before_wal_bytes": 280_000_000, "after_wal_bytes": 280_000_000,
        })
        assert engine._consecutive_no_progress_passive == 2
        assert engine._checkpoint_escalations == 1

        # It succeeds: only real reclamation clears the escalation state.
        engine._apply_checkpoint_result({
            "mode": "TRUNCATE", "status": "SUCCESS",
            "before_wal_bytes": 300_000_000, "after_wal_bytes": 0,
        })
        assert engine._consecutive_no_progress_passive == 0
        assert engine._checkpoint_escalations == 2
        assert engine._wal_bytes_reclaimed_total == 300_000_000
        assert engine._wal_size_cache == 0

        state = engine._runtime_state()
        assert state["persistence"]["wal_reclamation"] == {
            "consecutive_no_progress_passive": 0,
            "checkpoint_escalations": 2,
            "bytes_reclaimed_total": 300_000_000,
        }


@pytest.mark.asyncio
async def test_slow_checkpoint_and_retention_are_engine_worker_responsive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        worker = engine.maintenance_worker
        owner = int(worker.health()["owner_thread_id"])
        operation_threads: list[int] = []

        def slow_checkpoint(store: V4Store) -> dict[str, Any]:
            operation_threads.append(threading.get_ident())
            time.sleep(0.13)
            return store.checkpoint(
                mode="PASSIVE", reason="integration_slow_checkpoint"
            )

        def slow_retention(store: V4Store) -> dict[str, Any]:
            operation_threads.append(threading.get_ident())
            time.sleep(0.13)
            return store.compact_raw_evidence(
                now_ms(), retention_ms=60_000, batch_size=10,
                run_integrity=False,
            )

        checkpoint, checkpoint_probe = await asyncio.gather(
            worker.run_checkpoint(
                slow_checkpoint, timeout_s=2.0,
                name="integration_slow_checkpoint",
            ),
            _scheduler_probe(),
        )
        retention, retention_probe = await asyncio.gather(
            worker.run_retention(
                slow_retention, timeout_s=2.0,
                name="integration_slow_retention",
            ),
            _scheduler_probe(),
        )
        assert checkpoint["mode"] == "PASSIVE"
        assert int(checkpoint["checkpoint_run_id"]) > 0
        assert int(retention["retention_run_id"]) > 0
        assert set(operation_threads) == {owner}
        _assert_responsive(checkpoint_probe)
        _assert_responsive(retention_probe)
        assert worker.health()["max_duration_ms"] >= 120.0


@pytest.mark.asyncio
async def test_writer_health_and_integrity_gates_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        assert engine._last_integrity_ok is True
        assert engine._execution_blocked_reason() == ""

        healthy = engine.persistence.health

        def failed_health(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            result = healthy()
            result.update({
                "state": "FAILED",
                "healthy": False,
                "last_error": "integration_writer_failure",
            })
            return result

        monkeypatch.setattr(engine.persistence, "health", failed_health)
        assert engine._execution_blocked_reason() == "critical_writer_unhealthy"
        assert engine._runtime_state_name(now_ms()) == "DEGRADED_PERSISTENCE"

        monkeypatch.setattr(engine.persistence, "health", healthy)
        engine._last_integrity_ok = None
        assert engine._execution_blocked_reason() == "sqlite_integrity_unknown"
        engine._last_integrity_ok = False
        assert engine._execution_blocked_reason() == "sqlite_integrity_degraded"
        engine._last_integrity_ok = True
        assert engine._execution_blocked_reason() == ""

        engine._process_ownership_cache["process_ownership_valid"] = False
        assert (
            engine._execution_blocked_reason()
            == "critical_process_ownership_unverified"
        )
        engine._process_ownership_cache.update({
            "process_ownership_valid": True,
            "exact_v4_processes": 1,
            "owned_v4_processes": 1,
            "orphan_processes": 1,
        })
        assert engine._execution_blocked_reason() == "critical_orphan_process_detected"
        engine._process_ownership_cache["orphan_processes"] = 0
        engine._last_integrity_ts_ms = now_ms() - engine_module.INTEGRITY_MAX_AGE_MS - 1
        assert engine._execution_blocked_reason() == "sqlite_integrity_stale"
        engine._last_integrity_ts_ms = now_ms()
        assert engine._execution_blocked_reason() == ""


@pytest.mark.asyncio
async def test_execution_gate_scopes_unconfirmed_to_trade_critical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        assert engine._execution_blocked_reason() == ""
        healthy = engine.persistence.health

        def with_counts(total: int, critical: Any) -> Any:
            def fake(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
                result = healthy()
                result["unconfirmed_command_count"] = total
                if critical is None:
                    result.pop("unconfirmed_trade_critical_count", None)
                else:
                    result["unconfirmed_trade_critical_count"] = critical
                return result
            return fake

        # Unconfirmed evidence-only commands never block entry execution.
        monkeypatch.setattr(engine.persistence, "health", with_counts(7, 0))
        assert engine._execution_blocked_reason() == ""

        # A single unconfirmed trade-critical command stays fail-closed.
        monkeypatch.setattr(engine.persistence, "health", with_counts(7, 1))
        assert engine._execution_blocked_reason() == (
            "critical_command_unconfirmed")

        # A writer that cannot report the scoped count falls back to the
        # total and remains fully fail-closed.
        monkeypatch.setattr(engine.persistence, "health", with_counts(3, None))
        assert engine._execution_blocked_reason() == (
            "critical_command_unconfirmed")

        monkeypatch.setattr(engine.persistence, "health", healthy)
        assert engine._execution_blocked_reason() == ""


@pytest.mark.asyncio
async def test_execution_gate_blocks_on_overdue_not_merely_inflight_commands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An in-flight critical command is pipelining; an overdue one is a fault.

    Gating on the bare in-flight count blocked execution permanently under
    continuous load -- some trade-critical command is almost always in flight --
    and reported a persistence fault that did not exist.
    """

    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        assert engine._execution_blocked_reason() == ""
        healthy = engine.persistence.health
        timeout_ms = engine.cfg.writer_failure_timeout_ms

        def with_age(count: int, age_ms: Any) -> Any:
            def fake(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
                result = healthy()
                result["unconfirmed_trade_critical_count"] = count
                result["unconfirmed_trade_critical_oldest_age_ms"] = age_ms
                result["unconfirmed_command_count"] = count
                result["unconfirmed_command_oldest_age_ms"] = age_ms
                return result
            return fake

        # Normal pipelining: outstanding, well inside the acknowledgement
        # deadline.  Execution continues.
        monkeypatch.setattr(engine.persistence, "health", with_age(3, 5))
        assert engine._execution_blocked_reason() == ""
        monkeypatch.setattr(
            engine.persistence, "health", with_age(1, timeout_ms))
        assert engine._execution_blocked_reason() == ""

        # Past the deadline: genuinely unresolved, fail closed.
        monkeypatch.setattr(
            engine.persistence, "health", with_age(1, timeout_ms + 1))
        assert engine._execution_blocked_reason() == (
            "critical_command_unconfirmed")

        # An unreportable age stays fail-closed.
        monkeypatch.setattr(engine.persistence, "health", with_age(1, None))
        assert engine._execution_blocked_reason() == (
            "critical_command_unconfirmed")
        monkeypatch.setattr(
            engine.persistence, "health", with_age(1, "not-a-number"))
        assert engine._execution_blocked_reason() == (
            "critical_command_unconfirmed")

        # Confirmation clears the blocker; it never latches.
        monkeypatch.setattr(engine.persistence, "health", with_age(0, None))
        assert engine._execution_blocked_reason() == ""
        monkeypatch.setattr(engine.persistence, "health", healthy)
        assert engine._execution_blocked_reason() == ""


@pytest.mark.asyncio
async def test_inflight_commands_are_not_reported_as_incomplete_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        healthy = engine.persistence.health
        timeout_ms = engine.cfg.writer_failure_timeout_ms

        def with_age(count: int, age_ms: Any) -> Any:
            def fake(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
                result = healthy()
                result["unconfirmed_command_count"] = count
                result["unconfirmed_command_oldest_age_ms"] = age_ms
                result["unconfirmed_trade_critical_count"] = 0
                result["unconfirmed_trade_critical_oldest_age_ms"] = None
                return result
            return fake

        monkeypatch.setattr(engine.persistence, "health", with_age(9, 12))
        state = engine._runtime_state()
        telemetry = state["persistence"]["telemetry"]
        assert telemetry["critical_evidence_incomplete_count"] == 0
        assert telemetry["incomplete_evidence_count"] == 0

        monkeypatch.setattr(
            engine.persistence, "health", with_age(9, timeout_ms + 1))
        overdue = engine._runtime_state()
        assert overdue["persistence"]["telemetry"][
            "critical_evidence_incomplete_count"] == 9


@pytest.mark.asyncio
async def test_unchanged_evaluation_is_materially_coalesced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        current = now_ms()
        identity = _identity(current)
        state = await engine._persist_market(identity, current)
        state.books = {
            "YES": _neutral_book(identity, "YES", current),
            "NO": _neutral_book(identity, "NO", current),
        }
        engine.cex_features.append(_cex_observation(current))
        engine._okx_connected = True
        engine._poly_connected = True
        neutral = EnsembleResult(
            regime="INTEGRATION_NEUTRAL",
            fair_probability_yes=0.50,
            reliability=0.90,
            outputs=(),
            model_uncalibrated=True,
        )
        monkeypatch.setattr(engine.ensemble, "evaluate", lambda _ctx, **_kw: neutral)

        before = await engine.read_worker.query_one(
            "SELECT COUNT(*) AS n FROM candidates"
        )
        trigger = EvaluationTrigger(
            source="integration",
            receipt_ts_ms=current,
            receipt_monotonic_ns=time.monotonic_ns(),
        )
        await engine._evaluate(state, trigger)
        first = await engine.read_worker.query_one(
            "SELECT COUNT(*) AS n FROM candidates"
        )
        await engine._evaluate(state, EvaluationTrigger(
            source="integration-repeat",
            receipt_ts_ms=now_ms(),
            receipt_monotonic_ns=time.monotonic_ns(),
        ))
        second = await engine.read_worker.query_one(
            "SELECT COUNT(*) AS n FROM candidates"
        )
        assert before is not None and first is not None and second is not None
        assert int(first["n"]) == int(before["n"]) + 1
        assert int(second["n"]) == int(first["n"])
        assert state.evaluation_seq == 2
        assert engine.counters["evaluations"] == 2
        assert engine.counters["entries"] == 0
        assert state.last_candidate_fingerprint


@pytest.mark.asyncio
async def test_repeated_same_lifecycle_rediscovery_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _running_engine(tmp_path, monkeypatch) as (engine, _runtime):
        current = now_ms()
        identity = _identity(current)

        class _OneMarketDiscovery:
            async def discover(self, generated: int) -> DiscoveryBatch:
                return DiscoveryBatch(
                    generated_ts_ms=int(generated),
                    eligible_markets=(identity,),
                    request_count=1,
                    row_count=1,
                )

        engine.discovery = _OneMarketDiscovery()
        await engine.discover_once()
        first = engine.markets[identity.window_key]
        # The second active-lifecycle hydration has a later observation time.
        # It must update last-seen evidence without conflicting with the first
        # durable command or creating a second identity/window association.
        await asyncio.sleep(0.002)
        await engine.discover_once()
        second = engine.markets[identity.window_key]

        market_count, identity_count, window_count, link_count = (
            await asyncio.gather(
                engine.read_worker.query_one(
                    "SELECT COUNT(*) AS n FROM markets "
                    "WHERE polymarket_market_id=?", (identity.market_id,),
                ),
                engine.read_worker.query_one(
                    "SELECT COUNT(*) AS n FROM market_identities "
                    "WHERE condition_id=?", (identity.condition_id,),
                ),
                engine.read_worker.query_one(
                    "SELECT COUNT(*) AS n FROM asset_windows "
                    "WHERE asset=? AND window_open_ts_ms=?",
                    (identity.asset, identity.window_open_ms),
                ),
                engine.read_worker.query_one(
                    "SELECT COUNT(*) AS n FROM window_market_links "
                    "WHERE window_id=? AND market_identity_id=?",
                    (first.window_id, first.market_identity_id),
                ),
            )
        )
        # A deliberately overlapping background discovery may replace the
        # in-memory wrapper, but it must resolve to the exact same durable
        # market/identity/window owner.
        assert second.db_market_id == first.db_market_id
        assert second.market_identity_id == first.market_identity_id
        assert second.window_id == first.window_id
        assert int(market_count["n"]) == 1
        assert int(identity_count["n"]) == 1
        assert int(window_count["n"]) == 1
        assert int(link_count["n"]) == 1
        assert engine.persistence.health()["commands_failed"] == 0


def test_engine_source_has_no_direct_sqlite_or_runtime_store_path() -> None:
    source = inspect.getsource(FrequencyV4Engine)
    assert "self.store." not in source
    assert "sqlite3.connect" not in source
    assert "V4Store(" not in source
    # Blocking runtime-file operations are submitted to the runtime-I/O owner.
    assert "self.runtime.publish(" not in source
    ownership = source.index("self.runtime.process_ownership()")
    runtime_dispatch = source.rfind(
        "self.runtime_io_worker.run_io", 0, ownership
    )
    assert runtime_dispatch >= 0
    assert ownership - runtime_dispatch < 500


def test_persistence_refactor_preserves_permanent_shadow_safety() -> None:
    safety = immutable_safety_state()
    assert safety == {
        "strategy_id": "lite_frequency_v4",
        "mode": "lite_frequency_v4_shadow",
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "kill_switch_engaged": True,
        "fixed_shares": 5.0,
        "real_wallet_signing": False,
        "authenticated_trading_client": False,
        "real_order_placement": False,
        "real_order_cancellation": False,
    }
    source = inspect.getsource(engine_module)
    for forbidden in (
        "place_order(", "cancel_order(", "private_key", "api_secret",
        "wallet_sign", "authenticated_trading_client = True",
    ):
        assert forbidden not in source


# --- C1.H canonical/config divergence coverage (Human Authority Ruling 7) -
#
# The integration helper ``_build_engine`` monkeypatches
# ``canonical_export_dir`` to return ``cfg.export_dir`` so the strict
# canonical-containment guard permits the dashboard write under
# ``tmp_path``.  These focused assertions prove:
#
# 1. WITHOUT the monkeypatch, a real divergence between the unpatched
#    ``canonical_export_dir()`` and the test's ``cfg.export_dir`` IS
#    refused -- so the monkeypatch cannot be silently dropped without
#    the test catching it.  This keeps canonical/config divergence
#    visible rather than hidden by the helper.
# 2. WITH the test-only monkeypatch, the same ``cfg.export_dir`` path is
#    accepted, and the canonical root really was repointed (not bypassed).
#
# These cover the divergence class even though the helper monkeypatches
# the canonical derivation: the helper is the authorized test-only seam,
# and these tests prove that seam is doing what it claims.

def test_C1H_unpatched_canonical_refuses_tmp_export_dir(tmp_path):
    """A tmp-derived cfg.export_dir differs from the real canonical
    export dir, so the UNPATCHED export guard must refuse it.

    This proves the monkeypatch in ``_build_engine`` is load-bearing:
    if it were removed, the integration test's dashboard write would
    fail closed here.  The production guard is strict canonical
    containment; it does not accept an arbitrary cfg.export_dir.
    """
    cfg = _test_config(tmp_path)
    real_canonical = export_module.canonical_export_dir()
    # Precondition: the test config's export dir really is outside the
    # real canonical dir (they live under different tmp roots).
    assert Path(cfg.export_dir).resolve() != Path(real_canonical).resolve()
    # The unpatched guard refuses the divergent cfg.export_dir.
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        export_module.assert_canonical_export_path(cfg.export_dir)


def test_C1H_monkeypatch_repoints_canonical_root_for_tmp_export_dir(
        tmp_path, monkeypatch):
    """The test-only monkeypatch repoints ``canonical_export_dir`` at
    ``cfg.export_dir``, after which the same path is accepted -- proving
    the seam repoints the canonical root rather than bypassing the guard.

    This mirrors exactly what ``_build_engine`` does (Phase 10
    JUSTIFIED_TEST_ONLY_ADAPTATION): it monkeypatches the single
    canonical derivation symbol that the guard resolves, so production
    canonical-containment stays intact and only the test environment's
    canonical root moves.
    """
    cfg = _test_config(tmp_path)
    # Before the monkeypatch, the divergent path is refused.
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        export_module.assert_canonical_export_path(cfg.export_dir)
    # Apply the same test-only seam as _build_engine.
    monkeypatch.setattr(
        export_module, "canonical_export_dir", lambda: Path(cfg.export_dir)
    )
    # After repointing, the canonical derivation really was changed.
    assert export_module.canonical_export_dir() == Path(cfg.export_dir)
    # The same path is now accepted by the (unchanged) guard.
    resolved = export_module.assert_canonical_export_path(cfg.export_dir)
    assert resolved == (Path(cfg.export_dir) / export_module.EXPORT_FILENAME).resolve()


def test_C1H_guard_does_not_inspect_cfg_export_dir_directly(tmp_path, monkeypatch):
    """Defense-in-depth: the export guard resolves only
    ``canonical_export_dir()`` -- it never reads ``cfg.export_dir``.  So a
    divergent ``cfg.export_dir`` cannot smuggle a path past the guard; the
    only way to relocate the accepted root is to repoint the canonical
    derivation itself (which is what the authorized test seam does).

    Confirmed by source inspection: ``assert_canonical_export_path`` must
    not reference a config object or an ``export_dir`` attribute.
    """
    import inspect as _inspect
    src = _inspect.getsource(export_module.assert_canonical_export_path)
    # The guard must be agnostic of the config's export_dir attribute.
    assert "cfg.export_dir" not in src
    assert ".export_dir" not in src
    assert "config" not in src.lower()
