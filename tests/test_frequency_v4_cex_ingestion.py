"""Regression coverage for the isolated CEX ingestion queue and the shared
event-loop responsiveness / graceful-shutdown behaviour it depends on.

These tests pin the fix for the confirmed defect where the OKX/CEX receive
path performed synchronous SQLite writes on the shared asyncio event loop,
which could stall Polymarket reconnect/heartbeat and freeze fresh-CEX
evidence.  The CEX path now mirrors the proven Polymarket pattern: a bounded
queue, a non-blocking ``put_nowait`` callback, and a dedicated consumer task.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from poly_alpha_sniper.lite_frequency_v4 import engine as engine_module
from poly_alpha_sniper.lite_frequency_v4.config import (
    FrequencyV4Config,
    validate_frequency_v4_config,
)
from poly_alpha_sniper.lite_frequency_v4.contracts import (
    BookLevel,
    BookState,
    CexObservation,
    MarketIdentity,
    SourceEvent,
)
from poly_alpha_sniper.lite_frequency_v4 import export as export_module
from poly_alpha_sniper.lite_frequency_v4.edge_models import EnsembleResult
from poly_alpha_sniper.lite_frequency_v4.engine import (
    EvaluationTrigger,
    FrequencyV4Engine,
)
from poly_alpha_sniper.lite_frequency_v4.events import (
    EventDecision,
    EventDisposition,
)
from poly_alpha_sniper.lite_frequency_v4.persistence import V4PersistenceWriter
from poly_alpha_sniper.lite_frequency_v4.runtime import (
    V4RuntimeFiles,
    immutable_safety_state,
    now_ms,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4ReadOnlyStore, V4Store
from poly_alpha_sniper.lite_frequency_v4.telemetry import V4TelemetryWriter
from poly_alpha_sniper.lite_frequency_v4.workers import (
    V4MaintenanceWorker,
    V4ReadWorker,
    V4RuntimeIOWorker,
    V4WorkerJobTimeout,
    V4WorkerNotRunning,
    V4WorkerQueueFull,
)


@pytest.fixture
def engine_harness(tmp_path, monkeypatch):
    """Real engine with production-shaped, single-owner persistence workers."""

    cfg = FrequencyV4Config()
    cfg.db_path = str(tmp_path / "poly_alpha_frequency_v4.db")
    cfg.runtime_dir = str(tmp_path / "runtime" / "lite_frequency_v4_shadow")
    cfg.export_dir = str(tmp_path / "export" / "poly_alpha_frequency_v4")
    monkeypatch.setattr(engine_module, "validate_frequency_v4_config", lambda _cfg: None)

    runtime = V4RuntimeFiles(cfg.runtime_dir, repo_root=tmp_path)
    runtime.acquire()
    monkeypatch.setattr(runtime, "process_ownership", lambda: {
        "process_ownership_valid": True,
        "exact_v4_processes": 1,
        "owned_v4_processes": 1,
        "orphan_processes": 0,
        "exact_pids": [runtime.pid],
    })
    engine = FrequencyV4Engine(cfg, runtime)
    persistence = V4PersistenceWriter(
        cfg.db_path,
        queue_capacity=cfg.critical_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        checkpoint_on_close=False,
    )
    persistence.start()
    telemetry = V4TelemetryWriter(
        persistence,
        capacity=cfg.telemetry_queue_capacity,
        batch_size=cfg.telemetry_batch_size,
        flush_interval_s=cfg.telemetry_flush_interval_ms / 1_000.0,
        coalescing_interval_s=cfg.telemetry_coalescing_interval_ms / 1_000.0,
        submit_timeout_s=cfg.critical_command_timeout_s,
        heartbeat_interval_s=cfg.writer_heartbeat_interval_ms / 1_000.0,
    )
    telemetry.start()
    read_worker = V4ReadWorker(
        cfg.db_path,
        queue_capacity=cfg.reporting_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        default_timeout_s=cfg.reporting_worker_timeout_s,
    ).start()
    report_worker = V4ReadWorker(
        cfg.db_path,
        worker_name="test-v4-cex-report-reader",
        worker_kind="READ_REPORT",
        queue_capacity=cfg.reporting_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        default_timeout_s=cfg.reporting_worker_timeout_s,
    ).start()
    # Production wires a dedicated integrity reader: its own thread and its own
    # read-only connection, shared with nothing.  The harness must have one too,
    # or "the scan cannot delay the export" would be tested against a single
    # worker that serialises both and would pass for the wrong reason.
    integrity_worker = V4ReadWorker(
        cfg.db_path,
        worker_name="test-v4-cex-integrity-reader",
        worker_kind="READ_INTEGRITY",
        queue_capacity=cfg.reporting_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        default_timeout_s=cfg.reporting_worker_timeout_s,
    ).start()
    maintenance_worker = V4MaintenanceWorker(
        cfg.db_path,
        queue_capacity=cfg.maintenance_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        default_timeout_s=cfg.maintenance_worker_timeout_s,
    ).start()
    runtime_io_worker = V4RuntimeIOWorker(
        queue_capacity=cfg.reporting_queue_capacity,
        default_timeout_s=cfg.reporting_worker_timeout_s,
    ).start()
    engine.persistence = persistence
    engine.telemetry = telemetry
    engine.read_worker = read_worker
    engine.report_worker = report_worker
    engine.integrity_worker = integrity_worker
    engine.maintenance_worker = maintenance_worker
    engine.runtime_io_worker = runtime_io_worker
    engine._process_ownership_cache = {
        "process_ownership_valid": True,
        "exact_v4_processes": 1,
        "owned_v4_processes": 1,
        "orphan_processes": 0,
    }
    # Execution stays fail-closed until the dedicated read worker has verified
    # the isolated test database, just as production startup does.
    asyncio.run(engine._record_session())
    asyncio.run(engine._run_integrity_check())
    try:
        yield SimpleNamespace(
            engine=engine, persistence=persistence, telemetry=telemetry,
            read_worker=read_worker, maintenance_worker=maintenance_worker,
            report_worker=report_worker, integrity_worker=integrity_worker,
            runtime_io_worker=runtime_io_worker, runtime=runtime, cfg=cfg,
            root=tmp_path,
        )
    finally:
        telemetry.stop(drain=True, timeout_s=5.0)
        report_worker.stop(timeout_s=5.0)
        integrity_worker.stop(timeout_s=5.0)
        read_worker.stop(timeout_s=5.0)
        maintenance_worker.stop(timeout_s=5.0)
        runtime_io_worker.stop(timeout_s=5.0)
        persistence.close(timeout_s=5.0)
        runtime.release()


def _flush_telemetry(harness) -> None:
    assert harness.telemetry.flush(timeout_s=5.0)


def _query_one(harness, sql: str, params=()):
    _flush_telemetry(harness)
    return harness.read_worker.query_one_sync(sql, params)


def _identity(current: int, *, asset: str = "BTC", suffix: str = "1") -> MarketIdentity:
    opening = current - 30_000
    return MarketIdentity(
        asset=asset,
        slug=f"{asset.lower()}-updown-5m-{opening // 1000}",
        market_id=f"market-{suffix}",
        event_id=f"event-{suffix}",
        condition_id=f"condition-{suffix}",
        yes_token_id=f"yes-{suffix}",
        no_token_id=f"no-{suffix}",
        window_open_ms=opening,
        window_close_ms=opening + 300_000,
    )


def _book(identity: MarketIdentity, side: str, current: int) -> BookState:
    token = identity.yes_token_id if side == "YES" else identity.no_token_id
    if side == "YES":
        bids = (BookLevel(0.39, 20.0),)
        asks = (BookLevel(0.40, 20.0),)
    else:
        bids = (BookLevel(0.59, 20.0),)
        asks = (BookLevel(0.60, 20.0),)
    return BookState(
        token_id=token,
        condition_id=identity.condition_id,
        market_id=identity.market_id,
        bids=bids,
        asks=asks,
        provider_ts_ms=current,
        receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(),
        source="test_public_book",
        event_id=f"book-{side.lower()}-{current}",
        payload_hash=("a" if side == "YES" else "b") * 64,
        connection_epoch=1,
        min_order_size=5.0,
        tick_size=0.01,
        hydrated=True,
    )


def _observation(current: int, *, asset: str = "BTC",
                 event_id: str = "tick-1", epoch: int = 1) -> CexObservation:
    return CexObservation(
        provider="okx",
        asset=asset,
        instrument=f"{asset}-USDT",
        price=100_000.0,
        bid=99_999.0,
        ask=100_001.0,
        provider_ts_ms=current,
        receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(),
        event_id=event_id,
        event_type="ticker",
        connection_epoch=epoch,
    )


def _poly_event(current: int, key: str, *, token_id: str = "poly-token",
                condition_id: str = "poly-cond") -> SourceEvent:
    return SourceEvent(
        source="polymarket", channel="market", event_type="price_change",
        event_key=key, payload_hash="d" * 64,
        provider_ts_ms=current, receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(), connection_epoch=1,
        token_id=token_id, condition_id=condition_id, payload_json="{}",
    )


def _accepted(event) -> EventDecision:
    return EventDecision(EventDisposition.ACCEPT_NEW, event, "accepted_public_evidence")


def _strong_yes_ensemble() -> EnsembleResult:
    return EnsembleResult(
        regime="TEST_STRONG_YES",
        fair_probability_yes=0.75,
        reliability=0.90,
        outputs=(),
        model_uncalibrated=True,
    )


async def _mark_source_ready(engine: FrequencyV4Engine) -> None:
    await engine._on_source_health({
        "source": "polymarket", "state": "READY", "connected": True,
        "connection_epoch": 1, "desired_subscriptions": 2,
        "hydrated_subscriptions": 2,
    })
    await engine._on_source_health({
        "source": "okx", "state": "READY", "connected": True,
        "connection_epoch": 1, "desired_subscriptions": 2,
        "acknowledged_subscriptions": 2, "hydrated_assets": ["BTC"],
        "assets": ["BTC"],
    })


# 1. The OKX receive-path callback must not touch SQLite; persistence is the
#    bounded worker's job.
def test_cex_callback_never_writes_sqlite_on_receive_path(engine_harness):
    engine = engine_harness.engine

    async def scenario():
        obs = _observation(now_ms(), event_id="no-write")
        before = _query_one(
            engine_harness, "SELECT COUNT(*) AS n FROM cex_observations")["n"]
        await engine._on_cex_observation(obs, _accepted(obs))
        # Callback only enqueued: no rows changed, and the item is queued.
        assert _query_one(
            engine_harness, "SELECT COUNT(*) AS n FROM cex_observations")["n"] == before
        assert engine._cex_ingest_queue.qsize() == 1
        await engine._drain_cex_ingest_once()
        # The dedicated consumer admitted telemetry and its owner thread
        # performed the SQLite write.
        _flush_telemetry(engine_harness)
        assert _query_one(
            engine_harness, "SELECT COUNT(*) AS n FROM cex_observations")["n"] > before
        assert engine._cex_ingest_queue.qsize() == 0

    asyncio.run(scenario())


# 2. A slow persistence layer cannot stall CEX frame receipt because receipt no
#    longer calls persistence.
def test_slow_cex_persistence_does_not_block_receive(engine_harness, monkeypatch):
    engine = engine_harness.engine
    persist_calls = {"n": 0}
    original = engine_harness.persistence.submit_telemetry_batch

    def slow_record(*args, **kwargs):
        persist_calls["n"] += 1
        time.sleep(1.0)
        return original(*args, **kwargs)

    monkeypatch.setattr(
        engine_harness.persistence, "submit_telemetry_batch", slow_record)

    async def scenario():
        start = time.monotonic()
        for i in range(25):
            obs = _observation(now_ms() + i, event_id=f"fast-{i}")
            await engine._on_cex_observation(obs, _accepted(obs))
        return time.monotonic() - start

    elapsed = asyncio.run(scenario())
    assert persist_calls["n"] == 0            # receive path never persisted
    assert elapsed < 0.5                       # 25 frames admitted in well under one slow write
    assert engine._cex_ingest_queue.qsize() == 25


# 3 & 11. A slow CEX consumer must not serialise Polymarket receipt or freeze a
#    concurrent heartbeat/reconnect coroutine.
def test_slow_cex_persistence_does_not_block_polymarket(engine_harness, monkeypatch):
    engine = engine_harness.engine
    heartbeat = {"ticks": 0}

    async def slow_process(_obs, _dec):
        await asyncio.sleep(0.05)

    monkeypatch.setattr(engine, "_process_cex_observation", slow_process)

    async def polymarket_heartbeat():
        while not engine._stopping.is_set():
            await asyncio.sleep(0.01)
            heartbeat["ticks"] += 1

    async def scenario():
        for i in range(8):
            obs = _observation(now_ms() + i, event_id=f"cex-{i}")
            await engine._on_cex_observation(obs, _accepted(obs))
        consumer = asyncio.create_task(engine._cex_ingest_loop())
        beat = asyncio.create_task(polymarket_heartbeat())
        # While the CEX consumer chews through its backlog, Polymarket receipt
        # must complete promptly and the heartbeat must keep ticking.
        start = time.monotonic()
        for i in range(10):
            event = _poly_event(now_ms() + i, f"poly-{i}")
            await engine._on_polymarket_event(event, _accepted(event))
        poly_elapsed = time.monotonic() - start
        await asyncio.sleep(0.2)
        engine._stopping.set()
        for task in (consumer, beat):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        return poly_elapsed

    poly_elapsed = asyncio.run(scenario())
    assert poly_elapsed < 0.2                       # not serialised behind CEX backlog
    assert engine._polymarket_ingest_queue.qsize() == 10
    assert heartbeat["ticks"] >= 5                  # heartbeat progressed during CEX work


# 4. CEX queue overflow is counted.
def test_cex_overflow_is_counted(engine_harness):
    engine = engine_harness.engine
    engine._cex_ingest_queue = asyncio.Queue(maxsize=1)

    async def scenario():
        first = _observation(now_ms(), event_id="first")
        second = _observation(now_ms() + 1, event_id="second")
        await engine._on_cex_observation(first, _accepted(first))
        await engine._on_cex_observation(second, _accepted(second))

    asyncio.run(scenario())
    assert engine.counters["cex_ingest_overflow"] == 1
    assert engine._cex_ingest_queue.qsize() == 1


# 5. Overflow fails closed: dropped + counted + aggregated, never silently
#    admitted.
def test_cex_overflow_fails_closed(engine_harness):
    engine = engine_harness.engine
    engine._cex_ingest_queue = asyncio.Queue(maxsize=1)

    async def scenario():
        first = _observation(now_ms(), event_id="first")
        second = _observation(now_ms() + 1, event_id="second")
        await engine._on_cex_observation(first, _accepted(first))
        await engine._on_cex_observation(second, _accepted(second))

    asyncio.run(scenario())
    assert engine._last_error == "cex_ingest_queue_overflow_fail_closed"
    classifications = [key[5] for key in engine._event_count_buffer]
    assert "INGEST_QUEUE_OVERFLOW" in classifications
    # The overflowed observation was never enqueued for processing.
    assert engine._cex_ingest_queue.qsize() == 1


# 6. Overflow invalidates that asset's executable feature history.
def test_cex_overflow_invalidates_executable_feature_state(engine_harness):
    engine = engine_harness.engine
    engine.cex_features.append(_observation(now_ms(), event_id="pre-overflow"))
    assert engine.cex_features.latest("BTC") is not None
    engine._cex_ingest_queue = asyncio.Queue(maxsize=1)

    async def scenario():
        first = _observation(now_ms() + 1, event_id="queued")
        second = _observation(now_ms() + 2, event_id="overflow")
        await engine._on_cex_observation(first, _accepted(first))
        await engine._on_cex_observation(second, _accepted(second))

    asyncio.run(scenario())
    assert engine.counters["cex_ingest_overflow"] == 1
    assert engine.cex_features.latest("BTC") is None


# 7. Evidence superseded by a reconnect while queued is audited but never
#    re-enters the executable feature buffer.
def test_superseded_epoch_observation_is_audited_but_not_reused(engine_harness):
    engine = engine_harness.engine
    current = now_ms()

    async def scenario():
        await engine._persist_market(_identity(current), current)
        obs = _observation(current, event_id="epoch-1", epoch=1)
        await engine._on_cex_observation(obs, _accepted(obs))
        # A reconnect advances the OKX epoch while the observation waits.
        engine._okx_epoch = 2
        before = _query_one(
            engine_harness, "SELECT COUNT(*) AS n FROM cex_observations")["n"]
        await engine._drain_cex_ingest_once()
        _flush_telemetry(engine_harness)
        after = _query_one(
            engine_harness, "SELECT COUNT(*) AS n FROM cex_observations")["n"]
        return before, after

    before, after = asyncio.run(scenario())
    assert after > before                          # persisted for audit
    assert engine.cex_features.latest("BTC") is None  # not reused as executable


# 8. Queue depth and high-water telemetry are correct and observable.
def test_cex_queue_depth_and_high_water_telemetry(engine_harness):
    engine = engine_harness.engine

    async def scenario():
        for i in range(3):
            obs = _observation(now_ms() + i, event_id=f"depth-{i}")
            await engine._on_cex_observation(obs, _accepted(obs))
        state = engine._runtime_state()
        assert state["cex_ingest_queue_depth"] == 3
        assert state["cex_ingest_queue_high_water"] == 3
        assert state["cex_ingest_queue_capacity"] == engine.cfg.cex_writer_queue_max
        await engine._drain_cex_ingest_once()
        await engine._drain_cex_ingest_once()
        drained = engine._runtime_state()
        assert drained["cex_ingest_queue_depth"] == 1
        # High-water is a since-start maximum and does not fall back.
        assert drained["cex_ingest_queue_high_water"] == 3

    asyncio.run(scenario())


# 9. A connected OKX socket with stale evidence must not read healthy.
def test_cex_data_health_is_stale_when_connected_but_evidence_old(engine_harness):
    engine = engine_harness.engine
    current = now_ms()
    asyncio.run(engine._persist_market(_identity(current), current))
    engine._okx_connected = True
    stale = current - engine.cfg.cex_max_age_ms - 5_000
    engine.cex_features.append(_observation(stale, event_id="stale"))
    assert engine._cex_data_health(current) == "STALE"
    engine.cex_features.append(_observation(current, event_id="fresh"))
    assert engine._cex_data_health(current) == "FRESH"


# 10. Socket-connected-but-no-fresh-events is reported DEGRADED, not healthy.
def test_connected_but_no_fresh_cex_reports_degraded(engine_harness, monkeypatch):
    engine = engine_harness.engine
    current = now_ms()
    asyncio.run(engine._persist_market(_identity(current), current))
    engine._okx_connected = True
    engine._poly_connected = True
    stale = current - engine.cfg.cex_max_age_ms - 5_000
    engine.cex_features.append(_observation(stale, event_id="stale"))
    captured: dict = {}

    def publish_once(state):
        captured.update(state)
        engine._stopping.set()
        return state

    monkeypatch.setattr(engine.runtime, "publish", publish_once)
    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", lambda *a, **k: {})
    asyncio.run(engine._heartbeat_export_loop())
    assert captured["state"] == "DEGRADED_NO_FRESH_CEX"
    assert captured["cex_data_health"] == "STALE"


# 11 is covered together with 3 above (heartbeat progression).


# 12. Graceful shutdown drains the Polymarket queue.
def test_graceful_shutdown_drains_polymarket_queue(engine_harness, monkeypatch):
    engine = engine_harness.engine
    engine.poly_ws.stop = AsyncMock()
    engine.okx.stop = AsyncMock()
    engine.gamma.close = AsyncMock()
    engine.clob.close = AsyncMock()
    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", lambda *a, **k: {})
    processed = []
    original = engine._process_polymarket_event

    async def counting(event, decision):
        processed.append(event)
        await original(event, decision)

    engine._process_polymarket_event = counting

    async def scenario():
        for i in range(5):
            event = _poly_event(now_ms() + i, f"drain-poly-{i}")
            await engine._on_polymarket_event(event, _accepted(event))
        assert engine._polymarket_ingest_queue.qsize() == 5
        engine._tasks = [
            asyncio.create_task(
                engine._polymarket_ingest_loop(), name="v4-polymarket-ingest"),
            asyncio.create_task(engine._cex_ingest_loop(), name="v4-cex-ingest"),
        ]
        await engine.stop("test_stop")

    asyncio.run(scenario())
    assert len(processed) == 5
    assert engine._polymarket_ingest_queue.qsize() == 0
    assert engine._shutdown_drain_timed_out is False
    assert engine._polymarket_queue_discarded == 0


# 13. Graceful shutdown drains the CEX queue.
def test_graceful_shutdown_drains_cex_queue(engine_harness, monkeypatch):
    engine = engine_harness.engine
    engine.poly_ws.stop = AsyncMock()
    engine.okx.stop = AsyncMock()
    engine.gamma.close = AsyncMock()
    engine.clob.close = AsyncMock()
    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", lambda *a, **k: {})
    processed = []
    original = engine._process_cex_observation

    async def counting(observation, decision):
        processed.append(observation)
        await original(observation, decision)

    engine._process_cex_observation = counting

    async def scenario():
        for i in range(5):
            obs = _observation(now_ms() + i, event_id=f"drain-cex-{i}")
            await engine._on_cex_observation(obs, _accepted(obs))
        assert engine._cex_ingest_queue.qsize() == 5
        engine._tasks = [
            asyncio.create_task(
                engine._polymarket_ingest_loop(), name="v4-polymarket-ingest"),
            asyncio.create_task(engine._cex_ingest_loop(), name="v4-cex-ingest"),
        ]
        await engine.stop("test_stop")

    asyncio.run(scenario())
    assert len(processed) == 5
    assert engine._cex_ingest_queue.qsize() == 0
    assert engine._shutdown_drain_timed_out is False
    assert engine._cex_queue_discarded == 0


# 14. A drain timeout is accounted for, never silently pretended-persisted.
def test_shutdown_drain_timeout_accounts_for_remaining_items(engine_harness, monkeypatch):
    engine = engine_harness.engine
    engine.poly_ws.stop = AsyncMock()
    engine.okx.stop = AsyncMock()
    engine.gamma.close = AsyncMock()
    engine.clob.close = AsyncMock()
    engine.cfg.shutdown_drain_timeout_s = 0.1
    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", lambda *a, **k: {})

    async def stuck(_observation, _decision):
        await asyncio.sleep(5.0)

    engine._process_cex_observation = stuck

    async def scenario():
        for i in range(4):
            obs = _observation(now_ms() + i, event_id=f"stuck-{i}")
            await engine._on_cex_observation(obs, _accepted(obs))
        engine._tasks = [
            asyncio.create_task(
                engine._polymarket_ingest_loop(), name="v4-polymarket-ingest"),
            asyncio.create_task(engine._cex_ingest_loop(), name="v4-cex-ingest"),
        ]
        await engine.stop("test_stop")

    asyncio.run(scenario())
    assert engine._shutdown_drain_timed_out is True
    assert engine._cex_queue_discarded >= 1
    assert "shutdown_drain_timeout" in engine._last_error


# 15. Retention never removes trade evidence.
def test_retention_never_deletes_trade_evidence(engine_harness, monkeypatch):
    engine = engine_harness.engine
    current = now_ms()
    identity = _identity(current)
    state = asyncio.run(engine._persist_market(identity, current))
    asyncio.run(_mark_source_ready(engine))
    state.books = {
        "YES": _book(identity, "YES", current),
        "NO": _book(identity, "NO", current),
    }
    engine.cex_features.append(_observation(current))
    monkeypatch.setattr(engine.ensemble, "evaluate", lambda _c, **_kw: _strong_yes_ensemble())
    monkeypatch.setattr(engine, "_manage_open_position", AsyncMock())
    asyncio.run(engine._evaluate(state, EvaluationTrigger(
        source="test-entry", receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns())))
    assert _query_one(
        engine_harness, "SELECT COUNT(*) AS n FROM entries")["n"] == 1

    # Aggressive retention: a far-future "now" with the minimum retention
    # window puts every raw row past the deletion cutoff.
    asyncio.run(engine_harness.maintenance_worker.compact_raw_evidence(
        current + 10_000_000, retention_ms=60_000, batch_size=1_000))
    asyncio.run(engine_harness.maintenance_worker.enforce_raw_row_cap(1_000))

    assert _query_one(
        engine_harness, "SELECT COUNT(*) AS n FROM entries")["n"] == 1
    assert _query_one(
        engine_harness, "SELECT COUNT(*) AS n FROM positions")["n"] == 1
    integrity = engine_harness.read_worker.run_report_sync(
        lambda store: store.integrity_check())
    assert integrity["integrity"] == "ok"
    assert integrity["foreign_key_violations"] == []


# 16. sqlite_busy_timeout_ms is honoured, validated, and defaulted.
def test_sqlite_busy_timeout_is_configurable_and_applied(tmp_path):
    store = V4Store(tmp_path / "busy.db", busy_timeout_ms=54_321)
    try:
        assert store.busy_timeout_ms == 54_321
        applied = store.connection.execute("PRAGMA busy_timeout").fetchone()[0]
        assert int(applied) == 54_321
    finally:
        store.close()

    with pytest.raises(ValueError):
        V4Store(tmp_path / "bad.db", busy_timeout_ms=0)

    default_store = V4Store(tmp_path / "default.db")
    try:
        assert default_store.busy_timeout_ms == 10_000
        applied = default_store.connection.execute("PRAGMA busy_timeout").fetchone()[0]
        assert int(applied) == 10_000
    finally:
        default_store.close()


def test_config_busy_timeout_flows_into_store(tmp_path):
    cfg = FrequencyV4Config()
    cfg.sqlite_busy_timeout_ms = 22_222
    store = V4Store(tmp_path / "wired.db", busy_timeout_ms=cfg.sqlite_busy_timeout_ms)
    try:
        applied = store.connection.execute("PRAGMA busy_timeout").fetchone()[0]
        assert int(applied) == 22_222
    finally:
        store.close()


# 17. Fixed five-share sizing remains a locked safety invariant.
def test_fixed_five_shares_remains_locked():
    assert FrequencyV4Config().fixed_shares == 5.0
    cfg = FrequencyV4Config()
    cfg.fixed_shares = 1.0
    with pytest.raises(RuntimeError):
        validate_frequency_v4_config(cfg)


# 18. No maker fill can ever be assumed: the schema forbids it outright.
def test_entries_schema_forbids_assumed_maker_fill(tmp_path):
    store = V4Store(tmp_path / "maker.db")
    try:
        ddl = store.query_one(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='entries'"
        )["sql"]
        assert "maker_fill_assumed" in ddl
        assert "CHECK(maker_fill_assumed = 0)" in ddl
    finally:
        store.close()


# 19. One position per asset/window remains a locked invariant.
def test_one_side_per_asset_window_remains_locked():
    assert FrequencyV4Config().max_open_per_asset == 1
    cfg = FrequencyV4Config()
    cfg.max_open_per_asset = 2
    with pytest.raises(RuntimeError):
        validate_frequency_v4_config(cfg)


# 21. The cheap ~2s heartbeat/state publish must NOT run the heavy whole-
#     database dashboard export or integrity scan; those are off-loop now.
def test_heartbeat_loop_does_not_run_whole_db_scans(engine_harness, monkeypatch):
    engine = engine_harness.engine
    frozen = now_ms()
    monkeypatch.setattr(engine_module, "now_ms", lambda: frozen)
    exports = {"n": 0}
    integrities = {"n": 0}
    monkeypatch.setattr(
        engine_module, "write_frequency_v4_dashboard",
        lambda *_a, **_k: exports.__setitem__("n", exports["n"] + 1))

    def counting_integrity(_store):
        integrities["n"] += 1
        return {"integrity": "ok", "foreign_key_violations": []}

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", counting_integrity)
    iterations = {"n": 0}
    original_publish = engine.runtime.publish

    def publish_hook(state):
        iterations["n"] += 1
        if iterations["n"] >= 2:
            engine._stopping.set()
        return original_publish(state)

    monkeypatch.setattr(engine.runtime, "publish", publish_hook)
    asyncio.run(engine._heartbeat_export_loop())
    assert iterations["n"] >= 2
    # Neither the whole-database export nor the integrity scan runs on the
    # cheap heartbeat cadence any more.
    assert exports["n"] == 0
    assert integrities["n"] == 0


# 22. A persistence failure while recording a reject must never propagate and
#     crash a critical task (the observed crash site was the subscription loop).
def test_reject_persistence_failure_never_propagates(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def boom(*_a, **_k):
        raise RuntimeError("db exploded")

    monkeypatch.setattr(
        engine_harness.persistence, "submit_telemetry_batch", boom)
    result = engine._reject(
        None, "SCHEDULER", "active_subscription_ConnectionClosedError",
        recoverable=True)
    # Admission succeeded synchronously; the physical sink then failed on its
    # own thread without propagating into this caller.
    assert result == 1
    assert engine_harness.telemetry.flush(timeout_s=2.0)
    telemetry = engine_harness.telemetry.snapshot()
    assert telemetry["failed_batches"] >= 1
    assert telemetry["dropped"] >= 1


# 20. The ingestion refactor introduces no live-order surface.
def test_no_live_order_surface_introduced(engine_harness):
    engine = engine_harness.engine
    for forbidden in (
        "place_order", "cancel_order", "submit_order", "send_order",
        "wallet", "signer", "authenticated_client", "live_adapter",
    ):
        assert not hasattr(engine, forbidden)
    safety = immutable_safety_state()
    assert safety["real_orders_possible"] is False
    assert safety["live_enabled"] is False
    assert safety["dry_run"] is True
    for name in ("_cex_ingest_loop", "_process_cex_observation",
                 "_on_cex_observation", "_drain_cex_ingest_once"):
        assert callable(getattr(engine, name))


# ---------------------------------------------------------------------------
# Off-loop reporting / integrity / maintenance (durable event-loop fix).
# ---------------------------------------------------------------------------


def _fresh_db(tmp_path, name: str = "seed.db"):
    """Create a valid, closed V4 database file for a read-only reader."""
    path = tmp_path / name
    V4Store(path).close()
    return path


# 1. The read-only store opens mode=ro with query_only enforced.
def test_readonly_store_is_mode_ro_and_query_only(tmp_path):
    ro = V4ReadOnlyStore(_fresh_db(tmp_path))
    try:
        assert int(ro.connection.execute("PRAGMA query_only").fetchone()[0]) == 1
        rows = ro.query("SELECT COUNT(*) AS n FROM runtime_sessions")
        assert rows[0]["n"] >= 0
        assert ro.integrity_check()["integrity"] == "ok"
    finally:
        ro.close()


# 2. The read-only store cannot mutate the database.
def test_readonly_store_cannot_mutate(tmp_path):
    ro = V4ReadOnlyStore(_fresh_db(tmp_path))
    try:
        with pytest.raises(sqlite3.OperationalError):
            ro.connection.execute("CREATE TABLE illegal_writes(a INTEGER)")
        with pytest.raises(sqlite3.OperationalError):
            ro.connection.execute(
                "INSERT INTO runtime_sessions(session_id) VALUES('x')")
    finally:
        ro.close()


# 3. The dashboard export uses the read-only store, not the writer connection.
def test_export_uses_readonly_store_not_writer(engine_harness, monkeypatch):
    engine = engine_harness.engine
    captured = {}
    main_thread = threading.get_ident()

    def fake_export(store, _path, **_k):
        captured["store"] = store
        captured["thread"] = threading.get_ident()
        captured["query_only"] = int(
            store.connection.execute("PRAGMA query_only").fetchone()[0])
        return {}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", fake_export)
    asyncio.run(engine._run_dashboard_export())
    assert isinstance(captured["store"], V4ReadOnlyStore)
    assert captured["store"] is not engine.persistence
    assert captured["thread"] != main_thread
    assert captured["query_only"] == 1


# 4. A slow dashboard export runs off-loop and does not block the event loop.
def test_slow_export_does_not_block_event_loop(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def slow_export(*_a, **_k):
        time.sleep(0.4)
        return {}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", slow_export)
    ticks = {"n": 0}

    async def ticker():
        for _ in range(20):
            await asyncio.sleep(0.02)
            ticks["n"] += 1

    async def scenario():
        await asyncio.gather(engine._run_dashboard_export(), ticker())

    asyncio.run(scenario())
    assert ticks["n"] >= 15


# 5. A slow integrity check runs off-loop and does not block heartbeat coroutines.
def test_slow_integrity_does_not_block_heartbeat(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def slow_integrity(_store):
        time.sleep(0.4)
        return {"integrity": "ok", "foreign_key_violations": []}

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", slow_integrity)
    ticks = {"n": 0}

    async def heartbeat():
        for _ in range(20):
            await asyncio.sleep(0.02)
            ticks["n"] += 1

    async def scenario():
        await asyncio.gather(engine._run_integrity_check(), heartbeat())

    asyncio.run(scenario())
    assert ticks["n"] >= 15


# 6 & 8. Export is single-flight: concurrent/duplicate runs are coalesced.
def test_export_is_single_flight(engine_harness, monkeypatch):
    engine = engine_harness.engine
    concurrent = {"max": 0, "cur": 0}

    def export(*_a, **_k):
        concurrent["cur"] += 1
        concurrent["max"] = max(concurrent["max"], concurrent["cur"])
        time.sleep(0.15)
        concurrent["cur"] -= 1
        return {}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", export)

    async def scenario():
        await asyncio.gather(
            engine._run_dashboard_export(),
            engine._run_dashboard_export(),
            engine._run_dashboard_export(),
        )

    asyncio.run(scenario())
    assert concurrent["max"] == 1
    assert engine._export_runs == 1


def test_duplicate_export_is_coalesced_while_inflight(engine_harness, monkeypatch):
    engine = engine_harness.engine
    calls = {"n": 0}
    monkeypatch.setattr(
        engine_module, "write_frequency_v4_dashboard",
        lambda *_a, **_k: calls.__setitem__("n", calls["n"] + 1))
    engine._export_inflight = True
    try:
        asyncio.run(engine._run_dashboard_export())
        assert calls["n"] == 0
    finally:
        engine._export_inflight = False


# 7. Integrity is single-flight.
def test_integrity_is_single_flight(engine_harness, monkeypatch):
    engine = engine_harness.engine
    concurrent = {"max": 0, "cur": 0}
    runs_before = engine._integrity_runs

    def integrity(_store, **_kw):
        concurrent["cur"] += 1
        concurrent["max"] = max(concurrent["max"], concurrent["cur"])
        time.sleep(0.15)
        concurrent["cur"] -= 1
        return {"integrity": "ok", "foreign_key_violations": []}

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", integrity)

    async def scenario():
        await asyncio.gather(
            engine._run_integrity_check(),
            engine._run_integrity_check(),
            engine._run_integrity_check(),
        )

    asyncio.run(scenario())
    assert concurrent["max"] == 1
    assert engine._integrity_runs == runs_before + 1


# 9. An export failure does not crash the caller (a critical loop).
def test_export_failure_does_not_crash(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def boom(*_a, **_k):
        raise RuntimeError("export boom")

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", boom)
    asyncio.run(engine._run_dashboard_export())  # must not raise
    assert engine._last_export_ok is False
    assert "export:" in engine._last_error


# 10. A failed integrity result degrades health and fails closed.
def test_integrity_failure_degrades_health_and_fails_closed(engine_harness, monkeypatch):
    engine = engine_harness.engine
    monkeypatch.setattr(
        V4ReadOnlyStore, "integrity_check",
        lambda _store, **_kw: {
            "integrity": "malformed database", "foreign_key_violations": []})
    asyncio.run(engine._run_integrity_check())
    assert engine._last_integrity_ok is False
    assert engine._execution_blocked_reason() == "sqlite_integrity_degraded"
    assert engine._runtime_state_name(now_ms()) == "DEGRADED_INTEGRITY"


# 11. Shutdown waits for active reporting and closes its owner worker.
def test_shutdown_waits_for_reporting_and_closes_readonly_store(engine_harness, monkeypatch):
    engine = engine_harness.engine
    engine.poly_ws.stop = AsyncMock()
    engine.okx.stop = AsyncMock()
    engine.gamma.close = AsyncMock()
    engine.clob.close = AsyncMock()
    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", lambda *a, **k: {})
    finished = {"reporting": False}

    async def slow_reporting():
        try:
            await asyncio.sleep(0.3)
        finally:
            finished["reporting"] = True

    async def scenario():
        engine._tasks = [
            asyncio.create_task(slow_reporting(), name="v4-reporting"),
            asyncio.create_task(
                engine._polymarket_ingest_loop(), name="v4-polymarket-ingest"),
            asyncio.create_task(engine._cex_ingest_loop(), name="v4-cex-ingest"),
        ]
        await engine.stop("test_stop")

    asyncio.run(scenario())
    assert finished["reporting"] is True
    assert engine_harness.read_worker.health()["state"] == "STOPPED"


# 12. Retention SQL executes off the event loop (a worker thread), never on it.
def test_maintenance_runs_off_the_event_loop(engine_harness, monkeypatch):
    engine = engine_harness.engine
    main_thread = threading.get_ident()
    seen_threads = []

    def spy(_store, **_kwargs):
        seen_threads.append(threading.get_ident())
        return SimpleNamespace(as_dict=lambda: {
            "rows_deleted": 0, "checkpoint": None,
        })

    monkeypatch.setattr(engine_module, "run_bounded_maintenance_pass", spy)
    asyncio.run(engine._run_maintenance_pass())
    assert seen_threads
    assert all(tid != main_thread for tid in seen_threads)


def _maintenance_snapshot(current: int | None = None):
    return engine_module.MaintenanceSnapshot(
        now_ms=current or now_ms(), wal_bytes=0,
        critical_queue_depth=0, telemetry_queue_depth=0,
        runtime_active=True, runtime_health="HEALTHY", writer_healthy=True,
        open_positions=0, time_to_window_boundary_ms=60_000,
        critical_commit_p95_ms=1.0,
    )


def _maintenance_policy(*, chunk: int, rows: int, budget_ms: int):
    return engine_module.MaintenancePolicy(
        wal_trigger_bytes=1_000_000_000,
        restart_trigger_bytes=1_000_000_000,
        truncate_trigger_bytes=1_000_000_000,
        checkpoint_min_interval_ms=60_000,
        retention_ms=60_000,
        retention_chunk_rows=chunk,
        retention_row_budget=rows,
        retention_time_budget_ms=budget_ms,
    )


# 13. Maintenance deletes only within configured bounded-store chunks.
def test_maintenance_is_chunked_and_bounded(engine_harness, monkeypatch):
    calls = []

    def fake_compact(_store, *, max_rows, deadline_monotonic, **kwargs):
        calls.append((max_rows, deadline_monotonic, kwargs))
        return {"rows_deleted": max_rows, "budget_units": max_rows}

    monkeypatch.setattr(V4Store, "bounded_retention_step", fake_compact)
    result = engine_harness.maintenance_worker.run_maintenance_sync(
        engine_module.run_bounded_maintenance_pass,
        snapshot=_maintenance_snapshot(),
        policy=_maintenance_policy(chunk=100, rows=300, budget_ms=2_000),
    )
    assert calls
    assert all(1 <= rows <= 100 for rows, _deadline, _kwargs in calls)
    assert all(call[2]["protect_trade_evidence"] is True for call in calls)
    assert result.rows_deleted <= 300
    assert sum(rows for rows, _deadline, _kwargs in calls) <= 300


# 14. Maintenance respects the wall-clock budget even with rows remaining.
def test_maintenance_respects_time_budget(engine_harness, monkeypatch):
    calls = {"n": 0}

    def slow_compact(_store, *, max_rows, deadline_monotonic, **_kwargs):
        calls["n"] += 1
        time.sleep(0.05)
        exhausted = time.monotonic() >= deadline_monotonic
        rows = 0 if exhausted else min(max_rows, 100)
        return {
            "rows_deleted": rows,
            "budget_units": rows,
            "deadline_exhausted": exhausted,
        }

    monkeypatch.setattr(V4Store, "bounded_retention_step", slow_compact)
    start = time.monotonic()
    result = engine_harness.maintenance_worker.run_maintenance_sync(
        engine_module.run_bounded_maintenance_pass,
        snapshot=_maintenance_snapshot(),
        policy=_maintenance_policy(
            chunk=300, rows=10_000_000, budget_ms=200),
    )
    elapsed = time.monotonic() - start
    assert elapsed < 1.0
    assert result.time_budget_exhausted is True
    assert calls["n"] <= 5
    assert result.rows_deleted <= 400


# 16. Maintenance contention does not block the WebSocket receive callbacks.
def test_maintenance_does_not_block_ws_callbacks(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def slow_maintenance(_store, **_kwargs):
        time.sleep(0.3)
        return SimpleNamespace(as_dict=lambda: {
            "rows_deleted": 0, "checkpoint": None,
        })

    monkeypatch.setattr(
        engine_module, "run_bounded_maintenance_pass", slow_maintenance)

    async def scenario():
        maintenance = asyncio.create_task(engine._run_maintenance_pass())
        start = time.monotonic()
        for i in range(20):
            obs = _observation(now_ms() + i, event_id=f"maint-{i}")
            await engine._on_cex_observation(obs, _accepted(obs))
        elapsed = time.monotonic() - start
        await maintenance
        return elapsed

    elapsed = asyncio.run(scenario())
    assert elapsed < 0.2
    assert engine.counters["accepted_events"] == 20


# The retention indexes that make maintenance fast exist on open and migrate
# into a database created before they were added.
def _index_names(store):
    return {row["name"] for row in store.query(
        "SELECT name FROM sqlite_master WHERE type='index'")}


def test_retention_indexes_are_created_on_open(tmp_path):
    store = V4Store(tmp_path / "idx.db")
    try:
        names = _index_names(store)
        assert "ix_cex_retention" in names
        assert "ix_books_retention" in names
        # Foreign-key child indexes that keep raw-row deletes from full-scanning
        # the referencing tables for the ON DELETE RESTRICT check.
        assert "ix_candidate_cex_evidence_obs" in names
        assert "ix_candidate_book_evidence_snap" in names
        assert "ix_candidates_trigger_source" in names
    finally:
        store.close()


def test_retention_indexes_migrate_into_existing_db(tmp_path):
    path = tmp_path / "migrate.db"
    first = V4Store(path)
    first.connection.execute("DROP INDEX ix_cex_retention")
    assert "ix_cex_retention" not in _index_names(first)
    first.close()
    # Reopening an existing database recreates the additive index in place.
    second = V4Store(path)
    try:
        assert "ix_cex_retention" in _index_names(second)
    finally:
        second.close()


def test_maintenance_skips_compaction_when_nothing_old(engine_harness, monkeypatch):
    called = {"n": 0}
    original = V4Store.bounded_retention_step

    def spy(store, *a, **k):
        called["n"] += 1
        return original(store, *a, **k)

    monkeypatch.setattr(V4Store, "bounded_retention_step", spy)
    # Fresh-only evidence yields one bounded zero-result probe. The policy then
    # stops immediately instead of consuming the remaining row/time budget.
    result = engine_harness.maintenance_worker.run_maintenance_sync(
        engine_module.run_bounded_maintenance_pass,
        snapshot=_maintenance_snapshot(),
        policy=_maintenance_policy(chunk=300, rows=3_000, budget_ms=1_000),
    )
    assert called["n"] == 1
    assert result.rows_deleted == 0
    assert result.chunks_completed == 1


# 17. A maintenance failure does not crash the engine.
def test_maintenance_failure_does_not_crash(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def boom(*_a, **_k):
        raise RuntimeError("maintenance boom")

    monkeypatch.setattr(engine_module, "run_bounded_maintenance_pass", boom)
    asyncio.run(engine._run_maintenance_pass())  # must not raise
    assert "maintenance:" in engine._last_error


# 18. Severe event-loop lag fails closed for new executable candidates.
def test_loop_lag_fails_closed_candidate_execution(engine_harness, monkeypatch):
    engine = engine_harness.engine
    current = now_ms()
    identity = _identity(current)
    state = asyncio.run(engine._persist_market(identity, current))
    asyncio.run(_mark_source_ready(engine))
    state.books = {
        "YES": _book(identity, "YES", current),
        "NO": _book(identity, "NO", current),
    }
    engine.cex_features.append(_observation(current))
    monkeypatch.setattr(engine.ensemble, "evaluate", lambda _c, **_kw: _strong_yes_ensemble())
    monkeypatch.setattr(engine, "_manage_open_position", AsyncMock())
    engine._loop_lag_ms = engine.cfg.loop_lag_safety_ms + 500
    assert engine._execution_blocked_reason() == "event_loop_lag_degraded"
    asyncio.run(engine._evaluate(state, EvaluationTrigger(
        source="lag-test", receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns())))
    assert _query_one(
        engine_harness, "SELECT COUNT(*) AS n FROM entries")["n"] == 0
    decision = _query_one(engine_harness,
        "SELECT reason FROM decisions ORDER BY decision_id DESC LIMIT 1")
    assert "event_loop_lag" in decision["reason"]


# 19 & 20. Heartbeat and OKX admission stay responsive during slow reporting.
def test_ws_callbacks_stay_responsive_during_slow_reporting(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def slow_export(*_a, **_k):
        time.sleep(0.3)
        return {}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", slow_export)
    ticks = {"n": 0}

    async def heartbeat():
        for _ in range(20):
            await asyncio.sleep(0.02)
            ticks["n"] += 1

    async def scenario():
        report = asyncio.create_task(engine._run_dashboard_export())
        beat = asyncio.create_task(heartbeat())
        start = time.monotonic()
        for i in range(20):
            obs = _observation(now_ms() + i, event_id=f"rep-{i}")
            await engine._on_cex_observation(obs, _accepted(obs))
        elapsed = time.monotonic() - start
        await asyncio.gather(report, beat)
        return elapsed

    elapsed = asyncio.run(scenario())
    assert elapsed < 0.15                       # OKX admission not blocked
    assert engine.counters["accepted_events"] == 20
    assert ticks["n"] >= 15                     # heartbeat kept ticking


# ---------------------------------------------------------------------------
# Reporting isolation: a transient heartbeat/export publish timeout must
# degrade readiness and retry, not terminate the runtime.  A multi-GB integrity
# scan must never share the reporting/export worker.
# ---------------------------------------------------------------------------


# 13. One transient publish timeout does not propagate out of the publish path.
def test_transient_publish_timeout_does_not_kill_publish(engine_harness, monkeypatch):
    engine = engine_harness.engine
    payload = engine._runtime_state("RUNNING")

    def boom(*_a, **_k):
        raise V4WorkerJobTimeout("runtime-io-worker job 'runtime_publish' exceeded 10.000s")

    # The dedicated runtime_io_worker wraps the synchronous callable; patch the
    # callable itself so the real worker thread raises the transient failure.
    monkeypatch.setattr(engine.runtime, "publish", boom)

    # Must not raise: the transient timeout degrades readiness instead.
    result = asyncio.run(engine._publish_runtime_state(payload))
    assert result is not None                       # last valid export retained
    assert engine._last_export_publish_ok is False
    assert engine._export_publish_failures == 1
    assert engine._export_degraded_since_ms > 0
    assert "reporting_export_degraded" in engine._last_error


# 14. The next successful publish counts toward recovery; the flag only clears
# after the bounded healthy recovery window.
def test_publish_recovers_after_healthy_window(engine_harness, monkeypatch):
    engine = engine_harness.engine
    payload = engine._runtime_state("RUNNING")
    recovery_target = engine_module.REPORTING_EXPORT_RECOVERY_PUBLISHES
    calls = {"n": 0}
    real_publish = engine.runtime.publish

    def flaky(state=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise V4WorkerJobTimeout("transient publish timeout")
        return real_publish(state)

    monkeypatch.setattr(engine.runtime, "publish", flaky)

    async def scenario():
        # First call: transient failure -> degraded.
        await engine._publish_runtime_state(payload)
        assert engine._export_degraded_since_ms > 0
        # Subsequent successful calls: count toward recovery but do not clear
        # until the window is reached.
        for _ in range(recovery_target - 1):
            await engine._publish_runtime_state(payload)
            assert engine._export_degraded_since_ms > 0
        # The window-th successful publish clears the degradation.
        await engine._publish_runtime_state(payload)
        assert engine._export_degraded_since_ms == 0
        assert engine._last_export_publish_ok is True

    asyncio.run(scenario())


# 15. Repeated timeouts keep the degradation visible and accumulate failures.
def test_repeated_publish_timeouts_remain_visible(engine_harness, monkeypatch):
    engine = engine_harness.engine
    payload = engine._runtime_state("RUNNING")

    def always_timeout(*_a, **_k):
        raise V4WorkerJobTimeout("stuck publish")

    monkeypatch.setattr(engine.runtime, "publish", always_timeout)
    before = engine._export_publish_failures
    for _ in range(4):
        asyncio.run(engine._publish_runtime_state(payload))
    assert engine._export_publish_failures == before + 4
    assert engine._export_degraded_since_ms > 0
    # The degradation is published honestly so operational readiness fails closed.
    state = engine._runtime_state("RUNNING")
    assert state["reporting_export_degraded"] is True
    assert state["reporting_export_publish_failures"] == before + 4


# 16. A non-transient programming error from publish still propagates loudly.
def test_non_transient_publish_error_propagates(engine_harness, monkeypatch):
    engine = engine_harness.engine
    payload = engine._runtime_state("RUNNING")

    def bug(*_a, **_k):
        raise RuntimeError("genuine publish bug, not a transient timeout")

    monkeypatch.setattr(engine.runtime, "publish", bug)
    with pytest.raises(RuntimeError, match="genuine publish bug"):
        asyncio.run(engine._publish_runtime_state(payload))


# 17. The recoverable worker-error family (queue full / not running) also
# degrades rather than killing the loop.
def test_queue_full_and_not_running_degrade(engine_harness, monkeypatch):
    engine = engine_harness.engine
    payload = engine._runtime_state("RUNNING")

    for exc in (V4WorkerQueueFull("queue full"),
                V4WorkerNotRunning("not running")):
        def fail(_state=None, _exc=exc):
            raise _exc
        monkeypatch.setattr(engine.runtime, "publish", fail)
        asyncio.run(engine._publish_runtime_state(payload))   # must not raise
        assert engine._last_export_publish_ok is False
        assert engine._export_degraded_since_ms > 0


# 18. The heartbeat file stays fresh through the cheap independent publish path
# even while the export is degraded.
def test_heartbeat_stays_fresh_during_degraded_publish(engine_harness, monkeypatch):
    engine = engine_harness.engine
    runtime = engine_harness.runtime
    payload = engine._runtime_state("RUNNING")
    real_publish = engine.runtime.publish

    # Establish a baseline heartbeat file with one real publish.
    asyncio.run(engine._publish_runtime_state(payload))
    before = runtime.heartbeat_path.stat().st_mtime_ns

    def timeout(*_a, **_k):
        raise V4WorkerJobTimeout("transient")

    monkeypatch.setattr(engine.runtime, "publish", timeout)
    asyncio.run(engine._publish_runtime_state(payload))
    assert engine._export_degraded_since_ms > 0

    # Recovery: a real publish rewrites the heartbeat file.
    monkeypatch.setattr(engine.runtime, "publish", real_publish)
    asyncio.run(engine._publish_runtime_state(payload))
    after = runtime.heartbeat_path.stat().st_mtime_ns
    assert after > before


# 19. The supervisor does not classify a recoverable publish timeout as fatal:
# driving the heartbeat loop body for a few iterations with transient failures
# must not raise out of the loop.
def test_heartbeat_loop_survives_transient_publish_timeouts(engine_harness, monkeypatch):
    engine = engine_harness.engine
    state = {"failures_remaining": 3}

    real_publish = engine.runtime.publish

    def intermittent(s=None):
        if state["failures_remaining"] > 0:
            state["failures_remaining"] -= 1
            raise V4WorkerJobTimeout("transient")
        return real_publish(s)

    monkeypatch.setattr(engine.runtime, "publish", intermittent)

    async def scenario():
        # Run a handful of loop iterations; the loop sleeps ~2s per iteration so
        # shorten the wait by stopping after a bounded number of publishes.
        task = asyncio.create_task(engine._heartbeat_export_loop())
        await asyncio.sleep(0.05)
        engine._stopping.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    # The loop tolerated the transient timeouts; degradation was recorded.
    assert engine._export_publish_failures >= 1


# ---------------------------------------------------------------------------
# Integrity scan isolation: the reporting/export worker never waits on a full
# multi-GB scan; the scan runs on a separate worker and connection.
# ---------------------------------------------------------------------------


# 20. The export path never scans the database at all -- not the full multi-GB
# integrity_check (B-tree ordering, ~66s on the 6.50 GB production store) and
# not the quick_check either (~17s on the same store).  Both would monopolise
# the reporting worker and stall the 5s export cadence.  With no cached result
# the payload is an honest UNKNOWN that fails closed.
def test_export_never_scans_on_hot_path(engine_harness, monkeypatch):
    engine = engine_harness.engine
    scans: list[bool] = []

    def tracking(self, *, quick=False):
        scans.append(quick)
        return {"integrity": "ok", "foreign_key_violations": []}

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", tracking)

    # Build the dashboard payload directly with integrity=None: this is the
    # hot-path call that previously triggered a synchronous scan.
    from poly_alpha_sniper.lite_frequency_v4.export import build_frequency_v4_dashboard
    store = engine_harness.read_worker
    payload = store.run_report_sync(
        lambda s: build_frequency_v4_dashboard(
            s, now_ms=now_ms(), config=engine.cfg,
            runtime_state=engine._runtime_state("RUNNING"),
            session_id=engine.session_id, integrity=None,
        ))
    assert scans == []                       # neither quick nor full, ever
    assert payload["integrity"]["sqlite_integrity"] == "UNKNOWN"
    assert "sqlite_integrity_unhealthy" in (
        payload["persistence"]["critical_blocked_reasons"])
    assert payload["persistence"]["operational_ready"] is False

    # The runtime's own export passes its cached off-loop result, which is how a
    # healthy verdict is earned -- without a scan on this path.
    cached = store.run_report_sync(
        lambda s: build_frequency_v4_dashboard(
            s, now_ms=now_ms(), config=engine.cfg,
            runtime_state=engine._runtime_state("RUNNING"),
            session_id=engine.session_id,
            integrity={"integrity": "ok", "foreign_key_violations": []},
        ))
    assert scans == []
    assert cached["integrity"]["sqlite_integrity"] == "ok"


# 20b. The engine's own export loop feeds the cached result through, so a live
# runtime reports a real verdict without the export path ever scanning.
def test_engine_export_uses_cached_integrity_without_scanning(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    scans: list[bool] = []
    real_integrity_check = V4ReadOnlyStore.integrity_check

    def tracking(self, *, quick=False):
        scans.append(quick)
        return real_integrity_check(self, quick=quick)

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", tracking)

    # The real writer refuses any path outside the canonical export directory,
    # so build the real payload and capture it instead of writing it.  Everything
    # up to the write -- including how the engine sources integrity -- is real.
    built: list[dict] = []

    def capture(store, _output_path, **kwargs):
        payload = export_module.build_frequency_v4_dashboard(store, **kwargs)
        built.append(payload)
        return {"path": str(_output_path), "payload": payload, "bytes": 0}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", capture)

    asyncio.run(engine._run_integrity_check())   # off-loop scan populates cache
    assert scans == [True]                       # exactly one, and it is quick
    assert engine._last_integrity["integrity"] == "ok"

    asyncio.run(engine._run_dashboard_export())
    assert engine._last_export_ok is True, engine._last_error
    assert scans == [True]                       # export added no scan of its own
    assert built[-1]["integrity"]["sqlite_integrity"] == "ok"
    assert "sqlite_integrity_unhealthy" not in (
        built[-1]["persistence"]["critical_blocked_reasons"])

    # With the cache cleared the very same export path reports UNKNOWN rather
    # than reaching for the database itself, and fails closed on it.
    engine._last_integrity = {}
    asyncio.run(engine._run_dashboard_export())
    assert engine._last_export_ok is True, engine._last_error
    assert scans == [True]                       # still no scan on this path
    assert built[-1]["integrity"]["sqlite_integrity"] == "UNKNOWN"
    assert "sqlite_integrity_unhealthy" in (
        built[-1]["persistence"]["critical_blocked_reasons"])
    assert built[-1]["persistence"]["operational_ready"] is False


# 21. A long integrity scan does not delay the reporting/export cadence.  The
# real scan on the 6.50 GB production store takes ~17s (quick) to ~66s (full);
# this holds one open across many export cadences without burning that wall
# clock, which is a strictly stronger claim than sleeping a fixed interval.
def test_long_integrity_scan_does_not_block_reporting(engine_harness, monkeypatch):
    engine = engine_harness.engine
    scanning = threading.Event()      # set once the scan is genuinely in flight
    release = threading.Event()       # held until the exports have all completed

    def blocking_scan(_store, **_kw):
        scanning.set()
        # Stands in for the 31s+ scan: the reporting worker must not wait on it.
        assert release.wait(timeout=30.0), "test deadlock: scan never released"
        return {"integrity": "ok", "foreign_key_violations": []}

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", blocking_scan)
    # Build the real payload on the real reporting worker; only the canonical-
    # path write is stubbed out, since tmp_path is outside the export root.
    exports: list[dict] = []

    def capture(store, _output_path, **kwargs):
        payload = export_module.build_frequency_v4_dashboard(store, **kwargs)
        exports.append(payload)
        return {"path": str(_output_path), "payload": payload, "bytes": 0}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", capture)

    async def scenario():
        scan = asyncio.create_task(engine._run_integrity_check())
        await asyncio.get_running_loop().run_in_executor(
            None, scanning.wait, 10.0)
        assert scanning.is_set()
        assert engine._integrity_scan_in_progress is True

        # Six export cadences run to completion while the scan is still blocked.
        started = time.monotonic()
        for _ in range(6):
            await engine._run_dashboard_export()
        elapsed = time.monotonic() - started
        assert engine._integrity_scan_in_progress is True   # still mid-scan
        assert engine._export_runs >= 6
        assert engine._last_export_ok is True, engine._last_error

        release.set()
        await scan
        return elapsed

    elapsed = asyncio.run(scenario())
    assert len(exports) == 6              # every cadence produced a real payload
    # The exports never queued behind the scan: separate workers, separate
    # connections.  Six full exports in well under one simulated scan.
    assert elapsed < 10.0
    assert engine._last_integrity_success is True
    assert engine._integrity_scan_in_progress is False


# 22. The quick integrity scan exposes its lifecycle and is single-flight.
def test_integrity_scan_exposes_lifecycle_and_is_single_flight(engine_harness, monkeypatch):
    engine = engine_harness.engine
    concurrent = {"max": 0, "cur": 0}

    def tracking_scan(_store, **_kw):
        concurrent["cur"] += 1
        concurrent["max"] = max(concurrent["max"], concurrent["cur"])
        time.sleep(0.1)
        concurrent["cur"] -= 1
        return {"integrity": "ok", "foreign_key_violations": []}

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", tracking_scan)

    async def scenario():
        await asyncio.gather(
            engine._run_integrity_check(),
            engine._run_integrity_check(),
            engine._run_integrity_check(),
        )

    asyncio.run(scenario())
    assert concurrent["max"] == 1                       # single-flight
    assert engine._last_integrity_success is True
    assert engine._integrity_scan_started_ms > 0
    assert engine._integrity_scan_completed_ms >= engine._integrity_scan_started_ms
    assert engine._last_integrity_failure == ""
    state = engine._runtime_state("RUNNING")
    assert state["integrity_scan_in_progress"] is False
    assert state["last_integrity_success"] is True


# 23. An integrity scan failure is reported fail-closed without killing the
# runtime: _last_integrity_ok becomes UNKNOWN and execution stays blocked.
def test_integrity_scan_failure_fails_closed_without_crash(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def failing_scan(_store, **_kw):
        raise RuntimeError("scan worker blew up")

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", failing_scan)
    asyncio.run(engine._run_integrity_check())          # must not raise
    assert engine._last_integrity_ok is None            # UNKNOWN -> fail closed
    assert engine._last_integrity_success is False
    assert "RuntimeError" in engine._last_integrity_failure
    assert engine._execution_blocked_reason() == "sqlite_integrity_unknown"


# 24. The full integrity audit runs the real FULL scan on the dedicated
# integrity connection -- never the reporting/export worker (which would stall
# the export cadence) and never the maintenance worker (which owns WAL
# checkpoint decisions and retention) -- and exposes its lifecycle.
def test_full_integrity_audit_runs_on_dedicated_integrity_worker(engine_harness):
    engine = engine_harness.engine
    main_thread = threading.get_ident()
    audit: dict[str, object] = {}

    def probe_thread(worker):
        return worker.run_report_sync(
            lambda _s: threading.get_ident(), name="thread_probe")

    report_thread = probe_thread(engine.report_worker)
    integrity_thread = probe_thread(engine.integrity_worker)
    maintenance_thread = engine.maintenance_worker.run_maintenance_sync(
        lambda _s: threading.get_ident(), name="thread_probe")
    assert len({report_thread, integrity_thread, maintenance_thread}) == 3

    real_integrity_check = V4ReadOnlyStore.integrity_check

    def tracking(self, *, quick=False):
        audit["quick"] = quick
        audit["thread"] = threading.get_ident()
        return real_integrity_check(self, quick=quick)

    original = V4ReadOnlyStore.integrity_check
    V4ReadOnlyStore.integrity_check = tracking
    try:
        assert asyncio.run(engine._run_full_integrity_audit()) is True
    finally:
        V4ReadOnlyStore.integrity_check = original

    assert audit["quick"] is False                 # a real FULL integrity_check
    assert audit["thread"] == integrity_thread     # on the integrity connection
    assert audit["thread"] != report_thread        # not the reporting worker
    assert audit["thread"] != maintenance_thread   # not the maintenance worker
    assert audit["thread"] != main_thread          # never the event loop

    state = engine._runtime_state("RUNNING")
    assert state["full_integrity_audit_ok"] is True
    assert state["full_integrity_audit_runs"] == 1
    assert state["full_integrity_audit_in_progress"] is False
    assert state["full_integrity_audit_started_ts_ms"] > 0
    assert (state["full_integrity_audit_completed_ts_ms"]
            >= state["full_integrity_audit_started_ts_ms"])
    assert state["full_integrity_audit_last_failure"] is None
    # The audit uses its own budget, not the 30s maintenance one, which is
    # shorter than a single real full scan on the production store.
    assert engine.cfg.full_integrity_audit_timeout_s > (
        engine.cfg.maintenance_worker_timeout_s)


# 25. The two scans never overlap in either direction, and a deferral does not
# consume the audit interval (which would silently skip a whole 6-hour window).
def test_integrity_scans_never_overlap_in_either_direction(engine_harness):
    engine = engine_harness.engine
    calls: list[bool] = []
    real_integrity_check = V4ReadOnlyStore.integrity_check

    def tracking(self, *, quick=False):
        calls.append(quick)
        return real_integrity_check(self, quick=quick)

    original = V4ReadOnlyStore.integrity_check
    V4ReadOnlyStore.integrity_check = tracking
    try:
        async def scenario():
            # A quick scan in flight defers the audit -- and reports that it
            # deferred, so the caller does not burn the interval.
            engine._integrity_inflight = True
            assert await engine._run_full_integrity_audit() is False
            assert engine._full_integrity_inflight is False
            engine._integrity_inflight = False
            assert calls == []

            # An audit in flight defers the quick scan.
            engine._full_integrity_inflight = True
            before_runs = engine._integrity_runs
            await engine._run_integrity_check()
            assert engine._integrity_runs == before_runs
            assert calls == []
            engine._full_integrity_inflight = False

            # With neither in flight both run, one at a time.
            assert await engine._run_full_integrity_audit() is True
            await engine._run_integrity_check()

        asyncio.run(scenario())
    finally:
        V4ReadOnlyStore.integrity_check = original

    assert calls == [False, True]   # full audit, then quick scan; never nested
    assert engine._full_integrity_runs == 1


# 25b. The audit loop does not fire a ~66s full scan at every startup: the
# interval clock starts at launch, not at zero.
def test_full_integrity_audit_does_not_run_at_startup(engine_harness):
    engine = engine_harness.engine
    ran: list[bool] = []

    async def scenario():
        async def spy():
            ran.append(True)
            return True

        engine._run_full_integrity_audit = spy
        loop_task = asyncio.create_task(engine._full_integrity_audit_loop())
        await asyncio.sleep(0.1)
        engine._stopping.set()
        await asyncio.wait_for(loop_task, timeout=5.0)

    asyncio.run(scenario())
    assert ran == []            # nothing at startup
    assert engine._full_integrity_runs == 0


# 26. A degraded publish must not buy freshness it does not have: the export
# keeps reporting the age of the last state actually written to disk, and says
# so, instead of stamping a fresh in-memory timestamp over a stuck publish.
def test_stale_export_age_stays_honest_while_publish_is_degraded(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    exports: list[dict] = []

    def capture(store, _output_path, **kwargs):
        payload = export_module.build_frequency_v4_dashboard(store, **kwargs)
        exports.append(payload)
        return {"path": str(_output_path), "payload": payload, "bytes": 0}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", capture)
    real_publish = engine.runtime.publish

    async def scenario():
        # One healthy publish establishes the baseline heartbeat.
        await engine._publish_runtime_state(engine._runtime_state("RUNNING"))
        await engine._run_dashboard_export()
        fresh_age = exports[-1]["runtime_heartbeat_age_ms"]
        assert fresh_age is not None and fresh_age < 5_000
        assert exports[-1]["persistence"]["operational_degraded_reasons"].count(
            "reporting_export_degraded") == 0

        published_ts = exports[-1]["heartbeat_ts_ms"]

        # Now every publish times out.  The heartbeat file stops advancing.
        def timeout(*_a, **_k):
            raise V4WorkerJobTimeout("stuck publish")

        monkeypatch.setattr(engine.runtime, "publish", timeout)
        for _ in range(3):
            await engine._publish_runtime_state(
                engine._runtime_state("RUNNING"))
        await asyncio.sleep(0.05)
        await engine._run_dashboard_export()

        stale = exports[-1]
        # The exported heartbeat is still the last one truly written -- not
        # refreshed -- so its age grows honestly.
        assert stale["heartbeat_ts_ms"] == published_ts
        assert stale["runtime_heartbeat_age_ms"] >= fresh_age
        # And the degradation itself is visible rather than frozen at the
        # pre-failure value captured in the stale snapshot.
        assert stale["persistence"]["operational_ready"] is False
        assert "reporting_export_degraded" in (
            stale["persistence"]["operational_degraded_reasons"])

        # Recovery restores both freshness and readiness.
        monkeypatch.setattr(engine.runtime, "publish", real_publish)
        for _ in range(engine_module.REPORTING_EXPORT_RECOVERY_PUBLISHES):
            await engine._publish_runtime_state(
                engine._runtime_state("RUNNING"))
        await engine._run_dashboard_export()
        assert engine._export_degraded_since_ms == 0
        assert "reporting_export_degraded" not in (
            exports[-1]["persistence"]["operational_degraded_reasons"])
        assert exports[-1]["heartbeat_ts_ms"] > published_ts

    asyncio.run(scenario())


# 27. Every integrity scan outcome -- success, worker timeout and failure --
# leaves the integrity connection clean: no open transaction, no pinned
# snapshot, and the worker still usable for the next job.
def test_integrity_scan_cleans_up_on_success_timeout_and_failure(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    worker = engine.integrity_worker

    def assert_connection_clean(label: str) -> None:
        # Runs on the owner thread: the only place the connection may be read.
        state = worker.run_report_sync(
            lambda store: {
                "in_transaction": store.connection.in_transaction,
                "usable": store.connection.execute(
                    "SELECT 1").fetchone()[0],
            },
            name=f"cleanup_probe_{label}",
        )
        assert state["in_transaction"] is False, label
        assert state["usable"] == 1, label
        # The worker survived the job: no invariant violation was recorded.
        assert worker.health()["state"] == "RUNNING", label

    real_integrity_check = V4ReadOnlyStore.integrity_check

    # --- success -----------------------------------------------------------
    asyncio.run(engine._run_integrity_check())
    assert engine._last_integrity_success is True
    assert engine._integrity_scan_in_progress is False
    assert engine._integrity_inflight is False
    assert_connection_clean("success")

    # --- worker timeout ----------------------------------------------------
    release = threading.Event()

    def slow(self, *, quick=False):
        release.wait(timeout=30.0)
        return real_integrity_check(self, quick=quick)

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", slow)
    monkeypatch.setattr(engine.cfg, "reporting_worker_timeout_s", 0.15)
    asyncio.run(engine._run_integrity_check())          # must not raise
    assert engine._last_integrity_ok is None            # UNKNOWN, fail closed
    assert engine._last_integrity_success is False
    assert "V4WorkerJobTimeout" in engine._last_integrity_failure
    assert engine._integrity_scan_in_progress is False
    assert engine._integrity_inflight is False
    release.set()
    monkeypatch.undo()
    assert_connection_clean("timeout")

    # --- failure -----------------------------------------------------------
    def boom(self, *, quick=False):
        raise sqlite3.OperationalError("injected scan failure")

    monkeypatch.setattr(V4ReadOnlyStore, "integrity_check", boom)
    asyncio.run(engine._run_integrity_check())          # must not raise
    assert engine._last_integrity_ok is None
    assert engine._last_integrity_success is False
    assert "OperationalError" in engine._last_integrity_failure
    assert engine._integrity_scan_in_progress is False
    assert engine._integrity_inflight is False
    monkeypatch.undo()
    assert_connection_clean("failure")

    # Fail-closed but nonfatal: execution is blocked, the runtime is alive, and
    # the next scan still succeeds.
    assert engine._execution_blocked_reason() == "sqlite_integrity_unknown"
    asyncio.run(engine._run_integrity_check())
    assert engine._last_integrity_success is True


# 28. An integrity scan must not pin the WAL after it completes: a TRUNCATE
# checkpoint still fully reclaims once the scan is done.
def test_integrity_scan_leaves_wal_checkpoint_reachable(engine_harness):
    engine = engine_harness.engine
    wal_path = Path(f"{engine_harness.cfg.db_path}-wal")

    # Telemetry from the harness start-up has already produced WAL frames.
    _flush_telemetry(engine_harness)
    assert wal_path.exists() and wal_path.stat().st_size > 0

    # Both scan kinds run to completion on the integrity connection.
    asyncio.run(engine._run_integrity_check())
    assert engine._last_integrity_success is True
    assert asyncio.run(engine._run_full_integrity_audit()) is True
    assert engine._last_full_integrity_success is True
    assert engine._integrity_scan_in_progress is False
    assert engine._full_integrity_inflight is False

    # Nothing holds a read snapshot any more, so TRUNCATE fully reclaims.
    result = engine_harness.maintenance_worker.run_maintenance_sync(
        lambda store: store.checkpoint(mode="TRUNCATE", reason="test_reclaim"),
        name="reclaim_checkpoint",
    )
    assert result["success"] is True, result
    assert result["busy_result"] == 0, result
    # Fully reclaimed at the moment of measurement.  The file is non-zero again
    # immediately after, because journalling the checkpoint_runs row is itself a
    # write -- that is reclaim working, not reclaim blocked.
    assert result["after_wal_bytes"] == 0, result
    assert result["before_wal_bytes"] > 0, result


# 29. A clean shutdown leaves zero open runtime sessions.
def test_shutdown_leaves_zero_open_runtime_sessions(engine_harness, monkeypatch):
    engine = engine_harness.engine
    monkeypatch.setattr(
        engine_module, "write_frequency_v4_dashboard", lambda *a, **k: {})

    assert int(_query_one(
        engine_harness,
        "SELECT COUNT(*) AS count FROM runtime_sessions "
        "WHERE ended_ts_ms IS NULL")["count"]) == 1     # one while running

    # stop() closes the workers too, so the post-shutdown read has to come from
    # an independent connection -- which is also the honest way to check it.
    asyncio.run(engine.stop("test_shutdown"))

    conn = sqlite3.connect(f"file:{engine_harness.cfg.db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_sessions WHERE ended_ts_ms IS NULL"
        ).fetchone()[0] == 0                            # none afterwards
        row = conn.execute(
            "SELECT ended_ts_ms, stop_reason, dry_run, live_enabled, "
            "real_orders_possible, live_adapter_present, kill_switch_engaged, "
            "fixed_shares FROM runtime_sessions WHERE session_id=?",
            (engine.session_id,)).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["ended_ts_ms"] is not None
    assert row["stop_reason"] == "test_shutdown"
    # The durable session evidence still carries the shadow safety tuple.
    assert row["dry_run"] == 1
    assert row["live_enabled"] == 0
    assert row["real_orders_possible"] == 0
    assert row["live_adapter_present"] == 0
    assert row["kill_switch_engaged"] == 1
    assert row["fixed_shares"] == 5
