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
import time
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
from poly_alpha_sniper.lite_frequency_v4.edge_models import EnsembleResult
from poly_alpha_sniper.lite_frequency_v4.engine import (
    EvaluationTrigger,
    FrequencyV4Engine,
)
from poly_alpha_sniper.lite_frequency_v4.events import (
    EventDecision,
    EventDisposition,
)
from poly_alpha_sniper.lite_frequency_v4.runtime import (
    V4RuntimeFiles,
    immutable_safety_state,
    now_ms,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store


@pytest.fixture
def engine_harness(tmp_path, monkeypatch):
    """Real engine/store around isolated paths with no network I/O."""

    cfg = FrequencyV4Config()
    cfg.db_path = str(tmp_path / "poly_alpha_frequency_v4.db")
    cfg.runtime_dir = str(tmp_path / "runtime" / "lite_frequency_v4_shadow")
    cfg.export_dir = str(tmp_path / "export" / "poly_alpha_frequency_v4")
    monkeypatch.setattr(engine_module, "validate_frequency_v4_config", lambda _cfg: None)

    runtime = V4RuntimeFiles(cfg.runtime_dir, repo_root=tmp_path)
    runtime.acquire()
    store = V4Store(cfg.db_path)
    engine = FrequencyV4Engine(cfg, runtime, store)
    engine._record_session()
    try:
        yield SimpleNamespace(
            engine=engine, store=store, runtime=runtime, cfg=cfg, root=tmp_path,
        )
    finally:
        store.close()
        runtime.release()


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
        before = engine.store.connection.total_changes
        await engine._on_cex_observation(obs, _accepted(obs))
        # Callback only enqueued: no rows changed, and the item is queued.
        assert engine.store.connection.total_changes == before
        assert engine._cex_ingest_queue.qsize() == 1
        await engine._drain_cex_ingest_once()
        # The dedicated consumer performed the write.
        assert engine.store.connection.total_changes > before
        assert engine._cex_ingest_queue.qsize() == 0

    asyncio.run(scenario())


# 2. A slow persistence layer cannot stall CEX frame receipt because receipt no
#    longer calls persistence.
def test_slow_cex_persistence_does_not_block_receive(engine_harness, monkeypatch):
    engine = engine_harness.engine
    persist_calls = {"n": 0}

    def slow_record(*_a, **_k):
        persist_calls["n"] += 1
        time.sleep(1.0)
        return {"cex_observation_id": persist_calls["n"]}

    monkeypatch.setattr(engine.store, "record_cex_observation", slow_record)

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
    engine._persist_market(_identity(current), current)

    async def scenario():
        obs = _observation(current, event_id="epoch-1", epoch=1)
        await engine._on_cex_observation(obs, _accepted(obs))
        # A reconnect advances the OKX epoch while the observation waits.
        engine._okx_epoch = 2
        before = engine.store.connection.total_changes
        await engine._drain_cex_ingest_once()
        after = engine.store.connection.total_changes
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
    engine._persist_market(_identity(current), current)
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
    engine._persist_market(_identity(current), current)
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
    engine, store = engine_harness.engine, engine_harness.store
    current = now_ms()
    identity = _identity(current)
    state = engine._persist_market(identity, current)
    asyncio.run(_mark_source_ready(engine))
    state.books = {
        "YES": _book(identity, "YES", current),
        "NO": _book(identity, "NO", current),
    }
    engine.cex_features.append(_observation(current))
    monkeypatch.setattr(engine.ensemble, "evaluate", lambda _c: _strong_yes_ensemble())
    monkeypatch.setattr(engine, "_manage_open_position", AsyncMock())
    asyncio.run(engine._evaluate(state, EvaluationTrigger(
        source="test-entry", receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns())))
    assert store.query_one("SELECT COUNT(*) AS n FROM entries")["n"] == 1

    # Aggressive retention: a far-future "now" with the minimum retention
    # window puts every raw row past the deletion cutoff.
    store.compact_raw_evidence(
        current + 10_000_000, retention_ms=60_000, batch_size=1_000)
    store.enforce_raw_row_cap(1_000)

    assert store.query_one("SELECT COUNT(*) AS n FROM entries")["n"] == 1
    assert store.query_one("SELECT COUNT(*) AS n FROM positions")["n"] == 1
    integrity = store.integrity_check()
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


# 21. The heavy whole-database dashboard export is rate-limited off the ~2s
#     state/heartbeat cadence so it cannot monopolise the shared event loop.
def test_dashboard_export_is_rate_limited_off_heartbeat_cadence(engine_harness, monkeypatch):
    engine = engine_harness.engine
    frozen = now_ms()
    # Freeze the clock so every heartbeat iteration falls inside one export
    # window; the export must therefore run at most once across them.
    monkeypatch.setattr(engine_module, "now_ms", lambda: frozen)
    exports = {"n": 0}

    def fake_export(*_a, **_k):
        exports["n"] += 1
        return {}

    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", fake_export)
    monkeypatch.setattr(
        engine.store, "integrity_check",
        lambda: {"integrity": "ok", "foreign_key_violations": []})
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
    assert exports["n"] == 1


# 22. A persistence failure while recording a reject must never propagate and
#     crash a critical task (the observed crash site was the subscription loop).
def test_reject_persistence_failure_never_propagates(engine_harness, monkeypatch):
    engine = engine_harness.engine

    def boom(*_a, **_k):
        raise RuntimeError("db exploded")

    monkeypatch.setattr(engine.store, "record_reject", boom)
    result = engine._reject(
        None, "SCHEDULER", "active_subscription_ConnectionClosedError",
        recoverable=True)
    assert result == 0
    assert "reject_persist" in engine._last_error


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
