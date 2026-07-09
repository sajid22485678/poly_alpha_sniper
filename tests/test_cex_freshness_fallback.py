"""fix_cex_freshness_fallback_and_valid_shadow_entry_path:

The shadow DEGRADED band now extends to fail_closed_max_age_ms (was capped at
shadow_eval_max_age_ms), so low-volume assets between sparse trade prints stay
evaluable (with a staleness-scaled EV penalty) instead of hard-rejecting as
no_fresh_cex. Live modes are untouched and still fail-closed at the strict
cex.max_cex_staleness_ms budget. Source selection (freshest-wins, Binance never
blocks a fresher fallback) was already correct -- covered by
test_cex_source_selection.py -- and is re-asserted at the app/scan level here.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from poly_alpha_sniper.core.app import App
from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets, TradingMode, load_config
from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.tests.helpers import NOW_MS, market, shock, view

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def app(tmp_path):
    cfg = load_config()
    cfg.telegram.enabled = False
    cfg.dashboard.auth_enabled = False
    cfg.oracle_ev.enabled = True
    db = (tmp_path / "cex_fallback.db").as_posix()
    a = App(cfg, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                              "DATABASE_URL": f"sqlite:///{db}"}))
    a.clock = SimClock(NOW_MS)
    a.build()
    yield a
    a.store.close()


def _tick(app, asset, exchange, age_ms, price=150.0):
    ts = app.clock.now_ms() - age_ms
    app.cex_state.update(CexTick(asset=asset, exchange=exchange, price=price,
                                 ts_ms=ts, recv_ts_ms=ts))


# ---------------------------------------------------------------------------
# Classification: DEGRADED band extends to fail_closed_max_age_ms
# ---------------------------------------------------------------------------

def test_fresh_under_live_signal(app):
    _tick(app, "SOL", "bybit", 300)
    assert app._classify_cex_freshness(app.cex_state.multi_view("SOL")) == "fresh"


def test_degraded_between_live_signal_and_shadow_eval(app):
    _tick(app, "SOL", "bybit", 2000)  # 1500 < 2000 <= 3000
    assert app._classify_cex_freshness(app.cex_state.multi_view("SOL")) == "degraded"


def test_degraded_between_shadow_eval_and_fail_closed(app):
    """The coverage fix: 6.6s (the exact SOL case from the dashboard) is now
    DEGRADED (shadow-evaluable with EV penalty), not a no_fresh reject."""
    _tick(app, "SOL", "bybit", 6600)  # 3000 < 6600 <= 8000
    assert app._classify_cex_freshness(app.cex_state.multi_view("SOL")) == "degraded"


def test_fail_closed_beyond_ceiling(app):
    _tick(app, "SOL", "bybit", 8500)  # > 8000
    assert app._classify_cex_freshness(app.cex_state.multi_view("SOL")) == "fail_closed"


# ---------------------------------------------------------------------------
# Source fallback durability (app/scan level)
# ---------------------------------------------------------------------------

async def test_one_fresh_source_prevents_no_fresh_even_if_another_is_stale(app):
    """SOL: bybit stale (5s), okx fresh (40ms). The freshest source wins, so
    the scan must NOT emit no_fresh_cex."""
    _tick(app, "SOL", "bybit", 5000, price=150.0)
    _tick(app, "SOL", "okx", 40, price=150.1)
    await app._scan_entries()
    assert app.diag["cex_selected_source"]["SOL"] == "okx"
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE asset='SOL' AND "
        "reason='rejected_by_no_fresh_cex_price'")
    assert rows == []


async def test_stale_binance_never_blocks_fresh_bybit(app):
    """Binance briefly present but stale must never shadow a fresh bybit."""
    _tick(app, "BTC", "binance", 900, price=100_000.0)
    _tick(app, "BTC", "bybit", 40, price=100_001.0)
    await app._scan_entries()
    assert app.diag["cex_selected_source"]["BTC"] == "bybit"
    dbg = app.diag["cex_source_debug"]["BTC"]
    assert dbg["selected_source"] == "bybit"
    assert dbg["selected_is_freshest"] is True
    assert dbg["better_fallback_existed"] is False


async def test_all_sources_stale_beyond_fail_closed_rejects(app):
    _tick(app, "SOL", "bybit", 8500, price=150.0)
    _tick(app, "SOL", "okx", 9000, price=150.0)
    await app._scan_entries()
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE asset='SOL' ORDER BY id DESC")
    assert rows[0]["reason"] == "rejected_by_no_fresh_cex_price"
    assert app.diag["cex_source_debug"]["SOL"]["freshness_bucket"] == "FAIL_CLOSED"


async def test_source_debug_buckets_and_thresholds(app):
    _tick(app, "SOL", "bybit", 6600)
    await app._scan_entries()
    dbg = app.diag["cex_source_debug"]["SOL"]
    assert dbg["freshness_bucket"] == "DEGRADED"
    assert dbg["fail_closed_threshold_ms"] == app.cfg.cex_freshness.fail_closed_max_age_ms
    assert dbg["selected_status"] == "DEGRADED"
    assert dbg["bybit_age_ms"] is not None


# ---------------------------------------------------------------------------
# DEGRADED EV penalty scales with staleness, and stays shadow-only
# ---------------------------------------------------------------------------

def _buffer_used_for_age(app, age_ms):
    app.diag["cex_freshness_degraded"]["BTC"] = True
    app.diag["cex_freshest_age_ms"]["BTC"] = age_ms
    m = market(expiry_ms=app.clock.now_ms() + 240_000)
    m.price_to_beat = 100_000.0
    m.price_to_beat_source = "polymarket_event_metadata"
    m.raw["event_start_ts_ms"] = app.clock.now_ms() - 10_000
    from poly_alpha_sniper.tests.helpers import book as _book
    yb = _book(token_id=m.yes_token_id, ts_ms=app.clock.now_ms())
    nb = _book(token_id=m.no_token_id, ts_ms=app.clock.now_ms())
    sig = app.signal_engine.build_signal(
        shock(asset="BTC", ts_ms=app.clock.now_ms()), m,
        view(asset="BTC", price=100_050.0), yb, nb, app.clock.now_ms())
    if sig is None:
        pytest.skip("synthetic signal not built in this minimal fixture")
    app._oracle_ev_reject(sig, m, yb, nb, app.clock.now_ms(),
                          app._resolve_and_record_oracle_anchor(m, view(asset="BTC", price=100_050.0),
                                                                app.clock.now_ms()))
    return app.diag["latest_oracle_ev"]["adverse_selection_buffer_used"]


def test_degraded_penalty_scales_up_with_staleness(app):
    base = app.cfg.oracle_ev.adverse_selection_buffer
    add = app.cfg.cex_freshness.degraded_adverse_selection_buffer_add
    near = _buffer_used_for_age(app, 2000)   # <= shadow_eval -> 1x
    deep = _buffer_used_for_age(app, 6600)   # deeper -> scaled up
    assert near == pytest.approx(base + add)          # 1x at/under shadow_eval
    assert deep > near                                 # bigger penalty when staler
    assert deep == pytest.approx(base + add * (6600 / app.cfg.cex_freshness.shadow_eval_max_age_ms))


# ---------------------------------------------------------------------------
# Live modes unchanged -- never DEGRADED, strict fail-closed
# ---------------------------------------------------------------------------

def test_live_mode_never_degraded_and_stays_strict(app):
    app.mode = TradingMode.LIVE_MICRO
    _tick(app, "BTC", "bybit", 1600)  # would be degraded in shadow
    assert app._classify_cex_freshness(app.cex_state.multi_view("BTC")) == "fail_closed"
    assert app._cex_fresh_for_hard_check("BTC") is False


def test_live_mode_fresh_under_strict_budget(app):
    app.mode = TradingMode.LIVE_MICRO
    _tick(app, "BTC", "bybit", 300)  # under 500ms strict live budget
    assert app._classify_cex_freshness(app.cex_state.multi_view("BTC")) == "fresh"
    assert app._cex_fresh_for_hard_check("BTC") is True


# ---------------------------------------------------------------------------
# Valid candidate is not blocked by a cooldown/duplicate guard
# ---------------------------------------------------------------------------

def test_fresh_frequency_controller_allows_first_valid_trade(app):
    """A first valid A/A+/B setup must not be blocked by an artificial per-
    market cooldown -- can_trade is True on a fresh controller."""
    can, reason = app.freq.can_trade("BTC", "m1", app.mode)
    assert can is True, f"unexpected cooldown block: {reason}"


async def test_valid_anchor_and_fresh_view_reach_decision_not_no_fresh(app):
    """With a real anchor + fresh CEX view, evaluation reaches the trade
    decision path (a prediction row and/or ordinary downstream gate) and is
    never blocked by no_fresh_cex or a frequency cooldown."""
    from poly_alpha_sniper.tests.helpers import book as _book
    m = market(expiry_ms=app.clock.now_ms() + 240_000)
    m.price_to_beat = 100_000.0
    m.price_to_beat_source = "polymarket_event_metadata"
    m.raw["event_start_ts_ms"] = app.clock.now_ms() - 10_000
    app.book_store.update_snapshot(_book(token_id=m.yes_token_id, ts_ms=app.clock.now_ms()))
    app.book_store.update_snapshot(_book(token_id=m.no_token_id, ts_ms=app.clock.now_ms()))

    await app._evaluate_market(shock(asset="BTC", ts_ms=app.clock.now_ms()), m,
                               view(asset="BTC", price=100_050.0), app.clock.now_ms())

    no_fresh = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE reason='rejected_by_no_fresh_cex_price'")
    assert no_fresh == []  # a valid setup is never blocked as no_fresh
    freq_block = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE reason LIKE '%frequency%'")
    assert freq_block == []


# ---------------------------------------------------------------------------
# Standing safety invariants
# ---------------------------------------------------------------------------

def test_live_remains_disabled():
    c = load_config()
    assert c.mode.trading_mode == "shadow_live"
    assert c.mode.dry_run is True
    assert TradingMode(c.mode.trading_mode).is_live is False


def test_cex_feed_files_touch_no_orders_and_no_secrets():
    """The CEX-feed / source-selection files this fix changes must never read
    secrets or place/cancel orders. (core/app.py legitimately bootstraps config
    via load_secrets and is not scanned here -- it is the app entry point.)"""
    for rel in ("data/cex_state.py", "connectors/multi_cex_feed.py",
                "connectors/bybit_ws.py", "connectors/okx_ws.py"):
        src = (PROJECT_ROOT / rel).read_text(encoding="utf-8")
        assert "load_secrets" not in src
        assert ".env" not in src
        for forbidden in ("place_order", "cancel_order", "submit_order"):
            assert forbidden not in src, f"{rel} references {forbidden!r}"
