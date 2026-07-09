"""Candidate-specific book freshness and oracle-display safety tests."""
from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from poly_alpha_sniper.core.app import App
from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets, TradingMode, load_config
from poly_alpha_sniper.core.contracts import (
    BookLevel, Direction, OrderbookSnapshot, RejectReason, Shock)
from poly_alpha_sniper.dashboard.metrics import gate_waterfall
from poly_alpha_sniper.reporting.agent_export import build_candidate_book_status, build_oracle_status
from poly_alpha_sniper.tests.helpers import NOW_MS, market, view


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def app(tmp_path):
    cfg = load_config()
    cfg.telegram.enabled = False
    cfg.dashboard.auth_enabled = False
    db = (tmp_path / "book.db").as_posix()
    a = App(cfg, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                              "DATABASE_URL": f"sqlite:///{db}"}))
    a.clock = SimClock(NOW_MS)
    a.build()
    yield a
    a.store.close()


class _FakeClob:
    """Read-only stand-in for PolymarketClobPublic.get_book."""
    def __init__(self, result, raises=False):
        self._result = result
        self._raises = raises
        self.calls: list[str] = []
        self.submit_calls = 0
        self.cancel_calls = 0

    async def get_book(self, token_id: str):
        self.calls.append(token_id)
        if self._raises:
            raise RuntimeError("network down")
        return self._result

    async def submit(self, *args, **kwargs):  # pragma: no cover - must never be called
        self.submit_calls += 1
        raise AssertionError("candidate refresh must not submit orders")

    async def cancel(self, *args, **kwargs):  # pragma: no cover - must never be called
        self.cancel_calls += 1
        raise AssertionError("candidate refresh must not cancel orders")


def _shock(direction=Direction.UP):
    return Shock(asset="BTC", direction=direction, ts_ms=NOW_MS)


def _fresh_book(token, ts_ms, bid=0.48, ask=0.50, depth=500):
    return OrderbookSnapshot(
        token_id=token, ts_ms=ts_ms, source="rest",
        bids=[BookLevel(bid, depth)], asks=[BookLevel(ask, depth)])


def _empty_book(token, ts_ms):
    return OrderbookSnapshot(token_id=token, ts_ms=ts_ms, source="rest", bids=[], asks=[])


def _exec_token(m, shock):
    return m.token_for(m.side_for_direction(shock.direction).outcome)


async def test_stale_mirror_book_successful_direct_clob_refresh_continues(app):
    """D#1: stale mirror + one successful direct refresh produces a usable
    executable book and no final reject reason."""
    m = market()
    shock = _shock()
    token = _exec_token(m, shock)
    app.book_store.update_snapshot(_fresh_book(token, NOW_MS - 5_000))
    app.clob_public = _FakeClob(_fresh_book(token, NOW_MS + 10))

    status = await app._ensure_candidate_book_fresh(m, shock, NOW_MS + 10)

    assert status["status"] == "FRESH"
    assert status["direct_refresh_attempted"] is True
    assert status["direct_refresh_result"] == "success"
    assert status["final_reject_reason"] is None
    assert app.clob_public.calls == [token]
    assert app.clob_public.submit_calls == 0
    assert app.clob_public.cancel_calls == 0
    assert app.book_store.get(token).ts_ms == NOW_MS + 10


async def test_stale_mirror_book_failed_direct_refresh_rejects_fetch_failed(app):
    """D#2: stale mirror + failed direct refresh rejects with the canonical
    candidate-specific fetch-failed reason."""
    m = market()
    shock = _shock()
    token = _exec_token(m, shock)
    app.book_store.update_snapshot(_fresh_book(token, NOW_MS - 5_000))
    app.clob_public = _FakeClob(None)

    status = await app._ensure_candidate_book_fresh(m, shock, NOW_MS + 10)

    assert status["status"] == "FETCH_FAILED"
    assert status["direct_refresh_attempted"] is True
    assert status["direct_refresh_result"] == "fetch_failed"
    assert status["final_reject_reason"] == RejectReason.BOOK_FETCH_FAILED


async def test_stale_or_invalid_book_never_accepts_without_executable_bid_ask(app):
    """D#3: a refreshed book with no executable bid/ask stops before signal
    construction, so no prediction can be accepted."""
    app.cfg.oracle_ev.enabled = False
    m = market()
    app.market_cache.upsert([m])
    shock = _shock()
    token = _exec_token(m, shock)
    app.book_store.update_snapshot(_fresh_book(token, NOW_MS - 5_000))
    app.clob_public = _FakeClob(_empty_book(token, NOW_MS + 10))

    await app._evaluate_market(shock, m, view(asset="BTC"), NOW_MS + 10)

    status = app.diag["candidate_book_status"]
    assert status["status"] == "STALE"
    assert status["direct_refresh_result"] == "no_executable_bid_ask"
    assert status["final_reject_reason"] == RejectReason.STALE_BOOK
    assert app.store.query("SELECT * FROM predictions") == []
    rows = app.store.query("SELECT * FROM shadow_diagnostics ORDER BY id DESC")
    assert rows and rows[0]["reason"] == "rejected_by_stale_book"


async def test_candidate_book_status_exports_required_fields(app):
    """D#4: export preserves the full candidate-book contract."""
    m = market()
    shock = _shock()
    token = _exec_token(m, shock)
    app.book_store.update_snapshot(_fresh_book(token, NOW_MS))
    await app._ensure_candidate_book_fresh(m, shock, NOW_MS + 100)

    exported = build_candidate_book_status({"diagnostics": app.diag})

    required = {
        "market_id", "token_id", "asset", "side", "status", "earlier_gate_reason",
        "book_age_ms", "freshness_threshold_ms", "best_bid", "best_ask", "spread",
        "depth_near_best_usd", "direct_refresh_attempted", "direct_refresh_result",
        "final_reject_reason", "ts_ms",
    }
    assert required <= set(exported)
    assert exported["token_id"] == token
    assert exported["status"] == "FRESH"
    assert exported["book_age_ms"] == 100
    assert exported["freshness_threshold_ms"] == app.cfg.polymarket.max_orderbook_staleness_ms


async def test_candidate_fails_before_book_stage_exports_earlier_gate(app):
    """D#5: candidate exists but oracle gate rejects before book validation."""
    m = market()
    m.price_to_beat = None
    app.market_cache.upsert([m])
    shock = _shock()
    token = _exec_token(m, shock)
    app.book_store.update_snapshot(_fresh_book(token, NOW_MS))

    await app._evaluate_market(shock, m, view(asset="BTC"), NOW_MS + 10)
    exported = build_candidate_book_status({"diagnostics": app.diag})

    assert exported["status"] == "NOT_REACHED_BOOK_STAGE"
    assert exported["earlier_gate_reason"] == RejectReason.MISSING_ORACLE_ANCHOR
    assert exported["direct_refresh_attempted"] is False
    assert exported["token_id"] == token


async def test_aggregate_fresh_books_cannot_hide_candidate_specific_stale_book(app):
    """D#6: aggregate Fresh Books may look healthy while the candidate token is
    stale; the candidate status remains STALE."""
    m = market()
    shock = _shock()
    token = _exec_token(m, shock)
    for i in range(5):
        app.book_store.update_snapshot(_fresh_book(f"fresh_{i}", NOW_MS))
    app.book_store.update_snapshot(_fresh_book(token, NOW_MS - 5_000))
    app.clob_public = None

    fresh, total = app._book_freshness()
    status = await app._ensure_candidate_book_fresh(m, shock, NOW_MS + 10)

    assert fresh == 5
    assert total == 6
    assert status["status"] == "STALE"
    assert status["final_reject_reason"] == RejectReason.STALE_BOOK


def test_binance_metadata_url_is_not_settlement_anchor():
    """D#7: URL/source metadata is separate from the bot settlement anchor."""
    state = {"diagnostics": {
        "latest_oracle_anchor": {
            "market_id": "m1", "asset": "BTC", "oracle_open_price": 100_000.0,
            "oracle_source": "polymarket_event_metadata",
            "resolution_source_url": "https://www.binance.com/en/trade/BTC_USDT",
            "oracle_anchor_quality": "good",
        },
        "cex_selected_source": {"BTC": "binance"},
    }}

    status = build_oracle_status(state, load_config(), NOW_MS)

    assert status["settlement_anchor"] == "price_to_beat"
    assert status["anchor_source"] == "polymarket_event_metadata"
    assert status["metadata_url"] == "https://www.binance.com/en/trade/BTC_USDT"
    assert status["cex_lead_source"] == "binance"
    assert status["metadata_url"] != status["settlement_anchor"]


def test_dashboard_labels_metadata_url_correctly():
    """D#8: dashboard says metadata/reference URL, not settlement source."""
    src = (PROJECT_ROOT / "dashboard_v3" / "src" / "components" / "OracleStatusPanel.tsx").read_text(encoding="utf-8")

    assert "Price To Beat" in src
    assert "Anchor Source" in src
    assert "CEX Lead Source" in src
    assert "Metadata URL (Polymarket-declared reference URL)" in src
    assert "bot settlement source" not in src


def test_oracle_anchor_remains_price_to_beat():
    """D#9: price_to_beat is the explicit settlement anchor."""
    state = {"diagnostics": {"latest_oracle_anchor": {
        "market_id": "m1", "asset": "BTC", "oracle_open_price": 100_000.0,
        "oracle_source": "polymarket_event_metadata", "oracle_anchor_quality": "good",
    }}}

    status = build_oracle_status(state, load_config(), NOW_MS)

    assert status["price_to_beat"] == 100_000.0
    assert status["settlement_anchor"] == "price_to_beat"
    assert status["anchor_source"] == "polymarket_event_metadata"


def test_no_shock_ev_spread_and_risk_thresholds_unchanged():
    """D#10: this patch does not loosen strategy thresholds."""
    cfg = load_config()

    assert cfg.strategy.shock_min_abs_return == pytest.approx(0.0012)
    assert cfg.strategy.shock_min_zscore == pytest.approx(2.0)
    assert cfg.oracle_ev.min_ev_threshold == pytest.approx(0.0)
    assert cfg.microstructure.max_spread == pytest.approx(0.035)
    assert cfg.risk.max_total_exposure_pct_equity == pytest.approx(0.30)
    assert cfg.risk.max_market_exposure_pct_equity == pytest.approx(0.10)
    assert cfg.risk.fixed_order_shares == pytest.approx(5)


def test_live_remains_disabled():
    """D#11: repository config stays in shadow dry-run mode."""
    cfg = load_config()

    assert cfg.mode.trading_mode == "shadow_live"
    assert cfg.mode.dry_run is True
    assert TradingMode(cfg.mode.trading_mode).is_live is False


def test_candidate_book_refresh_places_or_cancels_no_orders():
    """D#12: direct refresh code is read-only and calls /book only."""
    src = inspect.getsource(App._ensure_candidate_book_fresh)

    assert "get_book" in src
    assert ".submit" not in src
    assert ".cancel" not in src
    assert "place_order" not in src
    assert "cancel_order" not in src


def test_candidate_book_refresh_and_export_do_not_touch_env_or_secrets():
    """D#13: candidate-book patch does not read .env or live-risk flags."""
    src = inspect.getsource(App._ensure_candidate_book_fresh)
    src += inspect.getsource(App._record_candidate_book_status)
    src += inspect.getsource(build_candidate_book_status)

    assert ".env" not in src
    assert "load_secrets" not in src
    assert "LIVE_TRADING_ENABLED" not in src
    assert "I_UNDERSTAND_REAL_MONEY_RISK" not in src


def test_gate_waterfall_uses_pipeline_order_and_real_accepted_count():
    diag_rows = [
        {"ts_ms": NOW_MS, "reason": "rejected_by_no_shock"},
        {"ts_ms": NOW_MS, "reason": "rejected_by_stale_book"},
        {"ts_ms": NOW_MS, "reason": "watchlist_no_shock_near_miss"},
    ]
    prediction_rows = [
        {"ts_ms": NOW_MS, "decision": "SHADOW_ONLY", "reject_reason": ""},
        {"ts_ms": NOW_MS, "decision": "WAIT", "reject_reason": ""},
        {"ts_ms": NOW_MS, "decision": "REJECT", "reject_reason": "REJECTED_SPREAD_TOO_WIDE"},
    ]

    out = gate_waterfall(diag_rows, prediction_rows, NOW_MS, window_minutes=60)

    assert out["stages"]["no_shock"] == 1
    assert out["stages"]["stale_book"] == 1
    assert out["stages"]["spread"] == 1
    assert out["accepted"] == 1
    assert out["total_candidates"] == 4
    assert out["stage_order"].index("no_shock") < out["stage_order"].index("stale_book")
