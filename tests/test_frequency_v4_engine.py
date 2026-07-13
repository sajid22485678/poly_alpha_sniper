from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from poly_alpha_sniper.lite_frequency_v4 import engine as engine_module
from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config
from poly_alpha_sniper.lite_frequency_v4.contracts import (
    BookLevel,
    BookState,
    CexObservation,
    MarketIdentity,
    SourceEvent,
)
from poly_alpha_sniper.lite_frequency_v4.edge_models import EnsembleResult
from poly_alpha_sniper.lite_frequency_v4.economics import exact_sweep, sweep_fee
from poly_alpha_sniper.lite_frequency_v4.discovery import DiscoveryBatch
from poly_alpha_sniper.lite_frequency_v4.engine import (
    EvaluationTrigger,
    FrequencyV4Engine,
)
from poly_alpha_sniper.lite_frequency_v4.events import (
    EventDecision,
    EventDisposition,
)
from poly_alpha_sniper.lite_frequency_v4.runtime import V4RuntimeFiles, now_ms
from poly_alpha_sniper.lite_frequency_v4.store import V4Store


@pytest.fixture
def engine_harness(tmp_path, monkeypatch):
    """Build the real engine/store around isolated paths without network I/O."""

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


def _observation(current: int, *, event_id: str = "tick-1") -> CexObservation:
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
        event_id=event_id,
        event_type="ticker",
        connection_epoch=1,
    )


def _accepted(event) -> EventDecision:
    return EventDecision(EventDisposition.ACCEPT_NEW, event, "accepted_public_evidence")


def _strong_yes_ensemble() -> EnsembleResult:
    # The engine still computes the full fee/buffer/depth gate from the books.
    return EnsembleResult(
        regime="TEST_STRONG_YES",
        fair_probability_yes=0.75,
        reliability=0.90,
        outputs=(),
        model_uncalibrated=True,
    )


async def _mark_source_ready(engine: FrequencyV4Engine) -> None:
    await engine._on_source_health({
        "source": "polymarket",
        "state": "READY",
        "connected": True,
        "connection_epoch": 1,
        "desired_subscriptions": 2,
        "hydrated_subscriptions": 2,
    })
    await engine._on_source_health({
        "source": "okx",
        "state": "READY",
        "connected": True,
        "connection_epoch": 1,
        "desired_subscriptions": 2,
        "acknowledged_subscriptions": 2,
        "hydrated_assets": ["BTC"],
        "assets": ["BTC"],
    })


def test_discovery_subscribes_only_active_tokens_and_scheduler_rolls_exactly(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    current = now_ms()
    active = _identity(current, suffix="active")
    upcoming = _identity(current + 300_000, suffix="next")
    future = _identity(current + 600_000, suffix="future")
    monkeypatch.setattr(engine_module, "now_ms", lambda: current)
    engine.discovery.discover = AsyncMock(return_value=DiscoveryBatch(
        generated_ts_ms=current,
        eligible_markets=(active, upcoming, future),
    ))
    engine.poly_ws.set_subscriptions = AsyncMock()
    engine.okx.set_assets = AsyncMock()

    def close_background(coroutine, *, name):
        del name
        coroutine.close()

    monkeypatch.setattr(engine, "_spawn_background", close_background)
    asyncio.run(engine.discover_once())

    subscriptions = engine.poly_ws.set_subscriptions.await_args.args[0]
    assert set(subscriptions) == {active.yes_token_id, active.no_token_id}
    assert upcoming.yes_token_id not in subscriptions
    assert upcoming.no_token_id not in subscriptions
    assert future.yes_token_id not in subscriptions
    assert future.no_token_id not in subscriptions

    asyncio.run(engine._sync_active_subscriptions(upcoming.window_open_ms + 1))
    rollover = engine.poly_ws.set_subscriptions.await_args.args[0]
    assert set(rollover) == {upcoming.yes_token_id, upcoming.no_token_id}
    assert active.yes_token_id not in rollover
    assert active.no_token_id not in rollover


def test_callbacks_admit_only_accepted_evidence_and_coalesce_latest_trigger(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    current = now_ms()
    identity = _identity(current)
    state = engine._persist_market(identity, current)
    engine.pending.clear()

    first = _observation(current, event_id="tick-first")
    asyncio.run(engine._on_cex_observation(first, _accepted(first)))
    assert engine.cex_features.latest("BTC") == first
    assert engine.pending[identity.window_key].event == first

    rejected = _observation(current + 1, event_id="tick-rejected")
    rejected_decision = EventDecision(
        EventDisposition.REJECT_TIMESTAMP_REGRESSION,
        rejected,
        "test_rejected",
    )
    asyncio.run(engine._on_cex_observation(rejected, rejected_decision))
    assert engine.cex_features.latest("BTC") == first
    assert engine.pending[identity.window_key].event == first

    websocket_book = {
        "hydrated": True,
        "bids": [(0.39, 20.0)],
        "asks": [(0.40, 20.0)],
        "provider_ts_ms": current + 2,
        "hash": "ws-book-hash",
        "connection_epoch": 1,
    }
    monkeypatch.setattr(engine.poly_ws, "current_book", lambda _token: websocket_book)
    event = SourceEvent(
        source="polymarket",
        channel="market",
        event_type="book",
        event_key="book-event-1",
        payload_hash="c" * 64,
        provider_ts_ms=current + 2,
        receipt_ts_ms=current + 2,
        receipt_monotonic_ns=time.monotonic_ns(),
        connection_epoch=1,
        asset="BTC",
        market_id=identity.market_id,
        condition_id=identity.condition_id,
        token_id=identity.yes_token_id,
        window_open_ms=identity.window_open_ms,
        payload_json="{}",
    )
    async def deliver_book():
        await engine._on_polymarket_event(event, _accepted(event))
        await engine._drain_polymarket_ingest_once()

    asyncio.run(deliver_book())

    assert state.books["YES"].token_id == identity.yes_token_id
    assert engine.pending[identity.window_key].event == event
    assert engine.counters["coalesced_triggers"] == 1
    assert engine.store.query_one(
        "SELECT COUNT(*) AS n FROM cex_observations"
    )["n"] == 2  # accepted and rejected evidence are both auditable
    assert engine.store.query_one(
        "SELECT COUNT(*) AS n FROM source_events"
    )["n"] >= 1


def test_rejected_polymarket_backlog_is_batched_off_receive_path(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    current = now_ms()
    event = SourceEvent(
        source="polymarket", channel="market", event_type="book",
        event_key="stale-backlog-event", payload_hash="d" * 64,
        provider_ts_ms=current-10_000, receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(), connection_epoch=1,
        token_id="stale-token", condition_id="stale-condition",
        payload_json="{}",
    )
    decision = EventDecision(
        EventDisposition.REJECT_STALE, event, "book_too_old_at_receipt")
    batch = Mock()
    monkeypatch.setattr(engine.store, "record_event_count_batch", batch)

    asyncio.run(engine._on_polymarket_event(event, decision))

    assert engine._polymarket_ingest_queue.empty()
    assert len(engine._event_count_buffer) == 1
    batch.assert_not_called()
    engine._flush_event_counts()
    batch.assert_called_once()
    row = batch.call_args.args[0][0]
    assert row["raw_count"] == 1
    assert row["invalid_count"] == 1
    assert row["classification"] == "REJECT_STALE"


def test_polymarket_ingest_queue_overflow_clears_executable_book(engine_harness):
    engine = engine_harness.engine
    current = now_ms()
    identity = _identity(current)
    state = engine._persist_market(identity, current)
    state.books["YES"] = _book(identity, "YES", current)
    engine._polymarket_ingest_queue = asyncio.Queue(maxsize=1)
    first = SourceEvent(
        source="polymarket", channel="market", event_type="price_change",
        event_key="queued-event", payload_hash="e" * 64,
        provider_ts_ms=current, receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(), connection_epoch=1,
        token_id=identity.yes_token_id, condition_id=identity.condition_id,
        payload_json="{}",
    )
    second = SourceEvent(
        source="polymarket", channel="market", event_type="price_change",
        event_key="overflow-event", payload_hash="f" * 64,
        provider_ts_ms=current+1, receipt_ts_ms=current+1,
        receipt_monotonic_ns=time.monotonic_ns(), connection_epoch=1,
        token_id=identity.yes_token_id, condition_id=identity.condition_id,
        payload_json="{}",
    )

    async def overflow():
        await engine._on_polymarket_event(first, _accepted(first))
        await engine._on_polymarket_event(second, _accepted(second))

    asyncio.run(overflow())

    assert engine.counters["polymarket_ingest_overflow"] == 1
    assert "YES" not in state.books
    assert engine._last_error == "polymarket_ingest_queue_overflow_fail_closed"


def test_polymarket_disconnect_or_epoch_change_clears_engine_books(engine_harness):
    engine = engine_harness.engine
    current = now_ms()
    identity = _identity(current)
    state = engine._persist_market(identity, current)
    state.books = {
        "YES": _book(identity, "YES", current),
        "NO": _book(identity, "NO", current),
    }

    asyncio.run(engine._on_source_health({
        "source": "polymarket", "state": "READY", "connected": True,
        "connection_epoch": 1, "desired_subscriptions": 2,
        "hydrated_subscriptions": 2,
    }))
    assert state.books == {}

    state.books = {
        "YES": _book(identity, "YES", current),
        "NO": _book(identity, "NO", current),
    }
    asyncio.run(engine._on_source_health({
        "source": "polymarket", "state": "DISCONNECTED", "connected": False,
        "connection_epoch": 1, "desired_subscriptions": 2,
        "hydrated_subscriptions": 0,
    }))
    assert state.books == {}


def test_strong_edge_routes_one_exact_five_share_shadow_entry_idempotently(
        engine_harness, monkeypatch):
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
    monkeypatch.setattr(engine.ensemble, "evaluate", lambda _context: _strong_yes_ensemble())
    monkeypatch.setattr(engine, "_manage_open_position", AsyncMock())

    first = EvaluationTrigger(
        source="test",
        receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(),
    )
    asyncio.run(engine._evaluate(state, first))
    row = store.query_one("SELECT * FROM entries")
    assert row is not None
    assert row["outcome_side"] == "YES"
    assert row["shares"] == 5.0
    assert row["entry_mode"] == "CROSS_SPREAD"
    assert row["maker_fill_assumed"] == 0
    assert row["selected_net_edge"] > 0.0
    assert engine.counters["entries"] == 1

    # A verified pre-close book exit can occur before another high-frequency
    # evaluation is coalesced.  Window idempotency must still prevent both a
    # second row and a second increment of the runtime entry counter.
    position = store.query_one(
        "SELECT * FROM positions WHERE entry_id=?", (row["entry_id"],))
    terminal_ts = now_ms() + 10
    store.record_management_decision({
        "position_id": position["position_id"],
        "decision_seq": 0,
        "decision_ts_ms": terminal_ts,
        "monotonic_ns": time.monotonic_ns(),
        "book_snapshot_id": row["book_snapshot_id"],
        "fair_value_calculation_id": row["fair_value_calculation_id"],
        "updated_fair_probability": 0.75,
        "executable_exit_value": 1.8,
        "hold_to_resolution_value": 1.7,
        "remaining_time_ms": identity.window_close_ms - terminal_ts,
        "spread": 0.01,
        "depth_shares": 20.0,
        "estimated_fee": 0.0,
        "uncertainty": 0.1,
        "thesis_state": "INVALIDATED",
        "action": "EXIT_BOOK",
        "reason": "test_verified_book_exit",
    })
    sell_sweep = exact_sweep(state.books["YES"], buy=False)
    assert sell_sweep is not None
    exit_fee = sweep_fee(sell_sweep, engine.cfg.crypto_taker_fee_rate)
    gross_pnl = sell_sweep.notional - float(row["gross_cost"])
    store.close_position({
        "position_id": position["position_id"],
        "exit_ts_ms": terminal_ts,
        "exit_source": "BOOK",
        "book_snapshot_id": row["book_snapshot_id"],
        "shares": 5.0,
        "executable_vwap": sell_sweep.vwap,
        "worst_consumed_price": sell_sweep.worst_price,
        "payout_usd": sell_sweep.notional,
        "gross_pnl": gross_pnl,
        "exit_fee": exit_fee,
        "net_pnl": gross_pnl - float(row["estimated_fee"]) - exit_fee,
        "evidence_verified": 1,
        "resolution_outcome": None,
        "reason": "test_verified_book_exit",
    })

    second = EvaluationTrigger(
        source="test-repeat",
        receipt_ts_ms=now_ms(),
        receipt_monotonic_ns=time.monotonic_ns(),
    )
    asyncio.run(engine._evaluate(state, second))
    assert store.query_one("SELECT COUNT(*) AS n FROM entries")["n"] == 1
    assert store.query_one("SELECT COUNT(*) AS n FROM decisions")["n"] == 2
    repeated = store.query_one(
        "SELECT action,reason FROM decisions ORDER BY decision_id DESC LIMIT 1"
    )
    assert repeated == {
        "action": "NO_ACTION", "reason": "window_entry_already_exists"}
    assert engine.counters["entries"] == 1
    assert store.query_one(
        "SELECT COUNT(*) AS n FROM entries WHERE maker_fill_assumed=1"
    )["n"] == 0


def test_recent_cex_tick_cannot_authorize_entry_after_provider_disconnect(
        engine_harness, monkeypatch):
    engine, store = engine_harness.engine, engine_harness.store
    current = now_ms()
    identity = _identity(current)
    state = engine._persist_market(identity, current)
    asyncio.run(_mark_source_ready(engine))
    state.books = {
        "YES": _book(identity, "YES", current),
        "NO": _book(identity, "NO", current),
    }
    observation = _observation(current)
    asyncio.run(engine._on_cex_observation(observation, _accepted(observation)))
    engine.pending.clear()
    monkeypatch.setattr(engine.ensemble, "evaluate", lambda _context: _strong_yes_ensemble())
    monkeypatch.setattr(engine, "_manage_open_position", AsyncMock())

    asyncio.run(engine._on_source_health({
        "source": "okx",
        "state": "DISCONNECTED",
        "connected": False,
        "connection_epoch": 1,
        "desired_subscriptions": 2,
        "acknowledged_subscriptions": 0,
        "hydrated_assets": [],
        "assets": ["BTC"],
    }))
    trigger = EvaluationTrigger(
        source="polymarket",
        receipt_ts_ms=now_ms(),
        receipt_monotonic_ns=time.monotonic_ns(),
    )
    asyncio.run(engine._evaluate(state, trigger))

    assert store.query_one("SELECT COUNT(*) AS n FROM entries")["n"] == 0
    decision = store.query_one(
        "SELECT action,economic_gate_passed,evidence_fresh,reason "
        "FROM decisions ORDER BY decision_id DESC LIMIT 1"
    )
    assert decision is not None
    assert decision["action"] in {"NO_ACTION", "SAFETY_FAIL"}
    assert decision["evidence_fresh"] == 0
    assert "cex" in decision["reason"].lower() or "source" in decision["reason"].lower()


def test_unsafe_source_evidence_cannot_drive_open_position_management(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    current = now_ms()
    identity = _identity(current)
    state = engine._persist_market(identity, current)
    asyncio.run(_mark_source_ready(engine))
    state.books = {
        "YES": _book(identity, "YES", current),
        "NO": _book(identity, "NO", current),
    }
    engine.cex_features.append(_observation(current))
    monkeypatch.setattr(engine.ensemble, "evaluate", lambda _context: _strong_yes_ensemble())

    # Create the verified shadow position without allowing the same evaluation
    # to make a management decision immediately after entry.
    monkeypatch.setattr(engine, "_manage_open_position", AsyncMock())
    asyncio.run(engine._evaluate(state, EvaluationTrigger(
        source="test-entry",
        receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(),
    )))
    assert engine.store.query_one(
        "SELECT COUNT(*) AS n FROM positions WHERE status='OPEN'"
    )["n"] == 1

    original_management = FrequencyV4Engine._manage_open_position.__get__(
        engine, FrequencyV4Engine)
    monkeypatch.setattr(engine, "_manage_open_position", original_management)
    management_probe = Mock(return_value=SimpleNamespace(
        action="HOLD",
        fair_probability=0.75,
        executable_exit_value=1.95,
        hold_expected_value=3.75,
        remaining_ms=200_000,
        spread=0.01,
        depth_shares=20.0,
        exit_fee=0.0,
        uncertainty_usd=0.1,
        thesis_state="UNSAFE_EVIDENCE_MUST_NOT_BE_USED",
        reason="test_probe",
        exit_selected=False,
        sweep=None,
    ))
    monkeypatch.setattr(engine_module, "evaluate_exit_vs_hold", management_probe)

    asyncio.run(engine._on_source_health({
        "source": "okx",
        "state": "DISCONNECTED",
        "connected": False,
        "connection_epoch": 1,
        "desired_subscriptions": 2,
        "acknowledged_subscriptions": 0,
        "hydrated_assets": [],
        "assets": ["BTC"],
    }))
    asyncio.run(engine._evaluate(state, EvaluationTrigger(
        source="unsafe-polymarket-trigger",
        receipt_ts_ms=now_ms(),
        receipt_monotonic_ns=time.monotonic_ns(),
    )))

    assert management_probe.call_count == 0
    assert engine.store.query_one(
        "SELECT COUNT(*) AS n FROM management_decisions"
    )["n"] == 0
    assert engine.store.query_one(
        "SELECT COUNT(*) AS n FROM positions WHERE status='OPEN'"
    )["n"] == 1


def test_engine_owns_only_v4_paths_and_has_no_execution_adapter(engine_harness):
    engine, store, cfg = (
        engine_harness.engine, engine_harness.store, engine_harness.cfg)
    database = store.query_one("PRAGMA database_list")
    assert database is not None
    assert str(engine_harness.root.resolve()) in str(database["file"])
    assert "poly_alpha_lite.db" not in cfg.db_path
    assert "lite_shadow" not in cfg.runtime_dir.replace("lite_frequency_v4_shadow", "")
    assert engine.poly_ws.source == "polymarket"
    assert engine.okx.source == "okx"
    for forbidden in (
        "live_adapter", "authenticated_client", "wallet", "signer",
        "place_order", "cancel_order",
    ):
        assert not hasattr(engine, forbidden)


def test_runtime_health_does_not_call_stale_or_disconnected_cex_history_running(
        engine_harness, monkeypatch):
    engine = engine_harness.engine
    stale = now_ms() - engine.cfg.cex_max_age_ms - 1_000
    engine.cex_features.append(_observation(stale, event_id="stale-tick"))
    engine._okx_connected = False
    captured = {}

    def publish_once(state):
        captured.update(state)
        engine._stopping.set()
        return state

    monkeypatch.setattr(engine.runtime, "publish", publish_once)
    monkeypatch.setattr(engine_module, "write_frequency_v4_dashboard", lambda *_a, **_k: {})
    asyncio.run(engine._heartbeat_export_loop())

    assert captured["state"] == "DEGRADED_NO_FRESH_CEX"


def test_unresolved_official_resolution_uses_bounded_exponential_retry(
        engine_harness, monkeypatch):
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
    monkeypatch.setattr(engine.ensemble, "evaluate", lambda _context: _strong_yes_ensemble())
    monkeypatch.setattr(engine, "_manage_open_position", AsyncMock())
    asyncio.run(engine._evaluate(state, EvaluationTrigger(
        source="test-entry",
        receipt_ts_ms=current,
        receipt_monotonic_ns=time.monotonic_ns(),
    )))
    row = store.query_one(
        """SELECT p.position_id,p.entry_id,p.outcome_side,p.open_shares,
           e.market_identity_id,e.gross_cost,e.estimated_fee,
           w.window_close_ts_ms
           FROM positions p JOIN entries e ON e.entry_id=p.entry_id
           JOIN asset_windows w ON w.window_id=e.window_id
           WHERE p.status='OPEN'"""
    )
    assert row is not None

    market_request = AsyncMock(return_value={})
    event_request = AsyncMock(return_value={})
    monkeypatch.setattr(engine.gamma, "get_market", market_request)
    monkeypatch.setattr(engine.gamma, "get_event", event_request)
    resolution_time = identity.window_close_ms + 1_000

    asyncio.run(engine._resolve_position(row, resolution_time))
    asyncio.run(engine._resolve_position(row, resolution_time + 4_999))
    assert market_request.await_count == event_request.await_count == 1
    assert store.query_one(
        "SELECT COUNT(*) AS n FROM resolution_attempts"
    )["n"] == 1

    asyncio.run(engine._resolve_position(row, resolution_time + 5_000))
    asyncio.run(engine._resolve_position(row, resolution_time + 14_999))
    assert market_request.await_count == event_request.await_count == 2
    assert store.query_one(
        "SELECT COUNT(*) AS n FROM resolution_attempts"
    )["n"] == 2

    asyncio.run(engine._resolve_position(row, resolution_time + 15_000))
    assert market_request.await_count == event_request.await_count == 3
    assert store.query_one(
        "SELECT COUNT(*) AS n FROM resolution_attempts"
    )["n"] == 3
