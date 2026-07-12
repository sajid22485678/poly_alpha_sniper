from dataclasses import replace
from types import SimpleNamespace

import pytest

from poly_alpha_sniper.lite import lite_bot
from poly_alpha_sniper.lite.lite_bot import LiteBot
from poly_alpha_sniper.lite.lite_market import LiteMarket
from poly_alpha_sniper.lite.lite_store import WindowLockConflict
from poly_alpha_sniper.lite.lite_strategy import (
    DirectionDecision, EntryTimingDecision,
)


WINDOW_CLOSE_MS = 600_000


class _MarketFinder:
    def __init__(self, market, reason="valid_market"):
        self.market = market
        self.reason = reason

    async def current_market(self, _asset, _now_ms):
        return self.market, self.reason


class _Books:
    async def get_books(self, token_ids):
        return {token_id: object() for token_id in token_ids}


class _Cex:
    def latest(self, _asset, _now_ms):
        return 100.0, 100, "test"

    def feature_snapshot(self, _asset, _windows, now_ms, **_kwargs):
        return {"provider_ts_ms": now_ms - 100}


class _Strategy:
    def __init__(self, direction):
        self.direction = direction
        self.optimized_direction = None

    def choose_direction(self, *_args, **_kwargs):
        return self.direction

    def observation(self, *_args, **_kwargs):
        return None

    def optimize_entry(self, direction, _book, _market, now_ms, *, lock=None):
        self.optimized_direction = direction
        return EntryTimingDecision(
            "CROSS_SPREAD", "maker_expired_cross_edge_valid",
            entry_price=direction.executable_yes_price,
            max_chase_price=lock.get("max_chase_price", 0.50),
            deadline_ts=lock.get("maker_deadline_ts"),
            wait_duration_ms=now_ms-lock.get("maker_start_ts", now_ms),
            execution_state="CROSS_SPREAD",
            maker_start_ts=lock.get("maker_start_ts"),
            maker_fill_assumed=False)

    def build_entry_decision(self, _market, direction, timing, _book, _now_ms):
        return SimpleNamespace(direction=direction, timing=timing)


class _Broker:
    def __init__(self):
        self.opened = []

    def open_trade(self, market, decision, now_ms, **kwargs):
        self.opened.append((market, decision, kwargs))
        return {"entry_ts": now_ms}


class _ConflictBroker(_Broker):
    def open_trade(self, _market, _decision, _now_ms, **_kwargs):
        raise WindowLockConflict("equity_exposure_cap")


class _Store:
    def __init__(self, lock):
        self.lock = dict(lock)
        self.committed = []
        self.skips = []
        self.decisions = []
        self.rejects = []

    def committed_positions(self):
        return list(self.committed)

    def positions_for_window(self, _window_close_ts):
        return []

    def risk_snapshot(self, _now_ms):
        return {"committed_exposure_usd": 0.0}

    def get_window_lock(self, _asset, _window_close_ts):
        return dict(self.lock)

    def update_window_lock(self, _asset, _window_close_ts, **fields):
        self.lock.update(fields)

    def mark_window_skipped(self, _asset, _window_close_ts, now_ms, reason,
                            *, missed=False, chase_prevented=False,
                            final_direction=None):
        self.lock.update({
            "status": "SKIPPED", "lifecycle_status": "SKIPPED",
            "entry_state": "SKIPPED", "final_entry_reason": reason,
            "last_updated_ts": now_ms,
        })
        self.skips.append({
            "reason": reason, "missed": missed,
            "chase_prevented": chase_prevented,
            "final_direction": final_direction,
        })

    def record_decision(self, _now_ms, _asset, _slug, decision, _bucket_s):
        self.decisions.append(decision)

    def record_reject(self, _now_ms, _asset, _slug, reason, _bucket_s):
        self.rejects.append(reason)


def _market():
    return LiteMarket(
        asset="BTC", slug="btc-updown-5m-300", market_id="m1",
        event_id="e1", condition_id="c1", yes_token_id="yes",
        no_token_id="no", window_start_s=300, window_close_s=600,
        anchor_available=False, price_to_beat=None,
    )


def _direction(output, reason, selected_edge):
    return DirectionDecision(
        output=output, side=None, yes_score=0.5, no_score=0.5,
        score_difference=0.0, confidence=0.0, reason=reason,
        selected_net_edge=selected_edge,
    )


def _bot(direction, *, maker_start=400_000, deadline=404_000):
    bot = LiteBot.__new__(LiteBot)
    bot.cfg = SimpleNamespace(
        momentum_windows_s=[5, 10, 30, 60], reject_bucket_s=30,
        min_cross_edge=0.010, max_open_positions=5, max_open_per_asset=2,
        live_small_equity_usd=100.0, equity_exposure_cap_pct=0.10,
        crypto_taker_fee_rate=0.0, fee_buffer_usd=0.0)
    bot.market_finder = _MarketFinder(_market())
    bot.books = _Books()
    bot.cex = _Cex()
    bot.strategy = _Strategy(direction)
    bot.broker = _Broker()
    bot.store = _Store({
        "asset": "BTC", "window_close_ts": WINDOW_CLOSE_MS,
        "side": "BUY_YES", "status": "MAKER_WAIT",
        "lifecycle_status": "DIRECTION_LOCKED",
        "direction_decision_ts": maker_start,
        "maker_start_ts": maker_start,
        "maker_deadline_ts": deadline,
        "deadline_ts": deadline,
        "maker_fill_assumed": 0,
    })
    bot.current_markets = {}
    bot._cycle_quotes = {}
    bot._prior_observations = {}
    bot._cycle_observations = {}
    bot.current_commit = "a" * 40
    bot.last_trade_ts_ms = None
    return bot


@pytest.mark.parametrize(
    ("output", "reason", "selected_edge", "execution_state"),
    [
        ("BRIEF_CONFIRMATION_WAIT", "edge_below_entry_threshold", 0.001,
         "MAKER_WAIT"),
        ("NO_TRADE_TRULY_NO_EDGE", "no_positive_fee_net_edge", -0.001,
         "MAKER_WAIT_EDGE_DECAY"),
    ],
)
@pytest.mark.asyncio
async def test_locked_maker_wait_weak_edge_is_not_terminal_before_deadline(
        monkeypatch, output, reason, selected_edge, execution_state):
    evaluated_ms = 403_500
    monkeypatch.setattr(lite_bot, "_now_ms", lambda: evaluated_ms)
    bot = _bot(_direction(output, reason, selected_edge))

    await bot._scan_asset("BTC", evaluated_ms)

    assert bot.store.skips == []
    assert bot.store.lock["status"] == "MAKER_WAIT"
    assert bot.store.lock["execution_state"] == execution_state
    assert bot.store.lock["maker_wait_ms"] == 3_500
    assert bot.store.lock["wait_duration_ms"] == 3_500
    assert bot.store.lock["maker_fill_assumed"] == 0


@pytest.mark.asyncio
async def test_locked_maker_wait_weak_edge_expires_only_at_deadline(monkeypatch):
    evaluated_ms = 404_000
    monkeypatch.setattr(lite_bot, "_now_ms", lambda: evaluated_ms)
    bot = _bot(_direction(
        "BRIEF_CONFIRMATION_WAIT", "edge_below_entry_threshold", 0.001))

    await bot._scan_asset("BTC", evaluated_ms)

    assert bot.store.skips == [{
        "reason": "maker_edge_expired", "missed": True,
        "chase_prevented": True,
        "final_direction": bot.strategy.direction,
    }]
    assert bot.store.lock["status"] == "SKIPPED"
    assert bot.store.lock["maker_wait_ms"] == 4_000
    assert bot.store.lock["wait_duration_ms"] == 4_000
    assert bot.store.lock["maker_fill_assumed"] == 0


@pytest.mark.asyncio
async def test_locked_side_tied_edge_above_cross_threshold_crosses_at_deadline(
        monkeypatch):
    evaluated_ms = 404_000
    monkeypatch.setattr(lite_bot, "_now_ms", lambda: evaluated_ms)
    tied = _direction(
        "BRIEF_CONFIRMATION_WAIT", "edge_below_entry_threshold", 0.012)
    tied = replace(
        tied, net_edge_yes=0.012, net_edge_no=0.012,
        executable_yes_price=0.48, executable_no_price=0.53)
    bot = _bot(tied)
    bot.store.lock["max_chase_price"] = 0.50

    await bot._scan_asset("BTC", evaluated_ms)

    assert bot.store.skips == []
    assert bot.strategy.optimized_direction.side == "BUY_YES"
    assert bot.strategy.optimized_direction.selected_net_edge == pytest.approx(0.012)
    assert len(bot.broker.opened) == 1
    assert bot.store.decisions[-1] == "CROSS_SPREAD"


@pytest.mark.asyncio
async def test_locked_maker_wait_data_invalid_still_fails_closed_early(monkeypatch):
    evaluated_ms = 401_000
    monkeypatch.setattr(lite_bot, "_now_ms", lambda: evaluated_ms)
    bot = _bot(_direction(
        "NO_TRADE_DATA_INVALID", "future_cex_timestamp", None))

    await bot._scan_asset("BTC", evaluated_ms)

    assert bot.store.skips == [{
        "reason": "future_cex_timestamp", "missed": False,
        "chase_prevented": False,
        "final_direction": bot.strategy.direction,
    }]
    assert bot.store.lock["status"] == "SKIPPED"
    assert bot.store.lock["execution_state"] == "DATA_INVALID"
    assert bot.store.lock["maker_wait_ms"] == 1_000
    assert bot.store.lock["maker_fill_assumed"] == 0


@pytest.mark.asyncio
async def test_locked_maker_wait_invalid_market_identity_fails_closed(monkeypatch):
    evaluated_ms = 401_000
    monkeypatch.setattr(lite_bot, "_now_ms", lambda: evaluated_ms)
    bot = _bot(_direction(
        "BRIEF_CONFIRMATION_WAIT", "edge_below_entry_threshold", 0.001))
    bot.market_finder = _MarketFinder(None, "invalid_market_identity")

    await bot._scan_asset("BTC", evaluated_ms)

    assert bot.store.skips == [{
        "reason": "invalid_market_identity", "missed": False,
        "chase_prevented": False, "final_direction": None,
    }]
    assert bot.store.lock["status"] == "SKIPPED"


@pytest.mark.asyncio
async def test_deadline_cross_conflict_terminalizes_active_wait(monkeypatch):
    evaluated_ms = 404_000
    monkeypatch.setattr(lite_bot, "_now_ms", lambda: evaluated_ms)
    tied = replace(
        _direction(
            "BRIEF_CONFIRMATION_WAIT", "edge_below_entry_threshold", 0.012),
        net_edge_yes=0.012, net_edge_no=0.012,
        executable_yes_price=0.48, executable_no_price=0.53)
    bot = _bot(tied)
    bot.store.lock["max_chase_price"] = 0.50
    bot.broker = _ConflictBroker()

    await bot._scan_asset("BTC", evaluated_ms)

    assert len(bot.store.skips) == 1
    assert bot.store.skips[0]["reason"] == "equity_exposure_cap"
    assert bot.store.skips[0]["final_direction"].side == "BUY_YES"
    assert bot.store.lock["status"] == "SKIPPED"


@pytest.mark.asyncio
async def test_position_cap_terminalizes_active_wait_as_safety_failure(monkeypatch):
    evaluated_ms = 401_000
    monkeypatch.setattr(lite_bot, "_now_ms", lambda: evaluated_ms)
    direction = replace(
        _direction("BUY_YES", "strongest_positive_fee_net_edge", 0.02),
        output="BUY_YES", side="BUY_YES", net_edge_yes=0.02,
        executable_yes_price=0.48, executable_no_price=0.53)
    bot = _bot(direction)
    bot.cfg.max_open_positions = 1
    bot.store.committed = [{"asset": "ETH"}]

    await bot._scan_asset("BTC", evaluated_ms)

    assert len(bot.store.skips) == 1
    assert bot.store.skips[0]["reason"] == "max_open_positions"
    assert bot.store.skips[0]["final_direction"] is direction
    assert bot.store.lock["status"] == "SKIPPED"
