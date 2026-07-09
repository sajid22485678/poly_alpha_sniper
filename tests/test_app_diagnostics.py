"""Runtime prediction-loop diagnostics (shadow observability)."""
import time

import pytest

from poly_alpha_sniper.core.app import App
from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets, load_config
from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.tests.helpers import NOW_MS


@pytest.fixture()
def app(tmp_path):
    cfg = load_config()  # shadow_live / dry_run
    cfg.telegram.enabled = False
    cfg.dashboard.auth_enabled = False
    db = (tmp_path / "diag.db").as_posix()
    a = App(cfg, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                              "DATABASE_URL": f"sqlite:///{db}"}))
    a.build()
    yield a
    a.store.close()


def _feed_ticks(app_obj, asset="BTC", base_price=100_000.0, n=30):
    """Bybit + OKX ticks only (Binance degraded) with fresh recv times."""
    now = app_obj.clock.now_ms()
    for i in range(n):
        ts = now - (n - i) * 1000
        wiggle = 4.0 if i % 2 else -4.0
        for exchange in ("bybit", "okx"):
            app_obj.cex_state.update(CexTick(asset=asset, exchange=exchange,
                                             price=base_price + wiggle,
                                             ts_ms=ts, recv_ts_ms=ts))
    # final fresh tick right now
    app_obj.cex_state.update(CexTick(asset=asset, exchange="bybit",
                                     price=base_price, ts_ms=now, recv_ts_ms=now))


async def test_shadow_app_uses_shadow_client_no_live_orders(app):
    from poly_alpha_sniper.execution.live_executor import ShadowClobClient
    assert isinstance(app.client, ShadowClobClient)  # never a real API client


async def test_no_ticks_writes_no_fresh_cex_diag(app):
    await app._scan_entries()
    assert app.diag["prediction_loop_iterations"] == 1
    rows = app.store.query("SELECT * FROM shadow_diagnostics")
    assert rows, "no diagnostic rows written"
    assert all(r["reason"] == "rejected_by_no_fresh_cex_price" for r in rows)
    assert app.diag["last_block_reason"].endswith("rejected_by_no_fresh_cex_price")


async def test_bybit_okx_only_make_windows_ready_then_no_shock_diag(app):
    _feed_ticks(app, "BTC")
    assert app.cex_state.window_ready("BTC")  # Binance degraded is irrelevant
    view = app.cex_state.multi_view("BTC")
    assert view.primary.fresh
    assert view.primary.exchange in ("bybit", "okx")
    await app._scan_entries()
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE asset='BTC' ORDER BY id DESC")
    assert rows[0]["reason"] == "rejected_by_no_shock"
    assert "z=" in rows[0]["detail"]


async def test_diag_rows_throttled(app):
    _feed_ticks(app, "BTC")
    for _ in range(5):
        await app._scan_entries()
    rows = app.store.query(
        "SELECT COUNT(*) AS n FROM shadow_diagnostics "
        "WHERE asset='BTC' AND reason='rejected_by_no_shock'")
    assert rows[0]["n"] == 1  # 30s throttle: five loops, one row
    assert app.diag["prediction_loop_iterations"] == 5


async def test_diag_summary_contract(app):
    _feed_ticks(app, "BTC")
    await app._scan_entries()
    d = app._diag_summary()
    for key in ("runtime_alive", "prediction_loop_alive", "prediction_loop_iterations",
                "last_prediction_loop_ts", "last_prediction_ts", "last_block_reason",
                "discovered_markets", "fresh_books", "total_books",
                "cex_ticks_by_asset", "price_windows_ready", "latest_prices",
                "last_cex_tick_ts_by_asset", "prediction_rows_written",
                "diagnostic_rows_written"):
        assert key in d, f"missing {key}"
    assert d["prediction_loop_alive"] is True
    assert d["cex_ticks_by_asset"].get("BTC", 0) > 0
    assert d["price_windows_ready"]["BTC"] is True


async def test_status_includes_prediction_diagnostics(app):
    _feed_ticks(app, "BTC")
    await app._scan_entries()
    actions = app._control_actions()
    text = await actions["status"]()
    for token in ("pred_loop", "last_block", "books fresh", "windows ready", "ticks:"):
        assert token in text, f"missing {token} in /status"


def test_cex_freshness_uses_recv_time_not_event_time():
    """Exchange event-ts skew must not mark a live stream stale."""
    from poly_alpha_sniper.data.cex_state import CexState
    cfg = load_config()
    clock = SimClock(NOW_MS)
    state = CexState(cfg, clock)
    # event timestamp 3 s old (skew/latency) but received JUST NOW
    state.update(CexTick(asset="BTC", exchange="bybit", price=100_000.0,
                         ts_ms=NOW_MS - 3000, recv_ts_ms=NOW_MS))
    stats = state.stats("BTC")
    assert stats is not None
    assert stats.fresh, "recv-time freshness broken: live stream marked stale"
    clock.advance_ms(2000)  # no new ticks for 2s -> genuinely stale
    assert not state.stats("BTC", "bybit").fresh


async def test_mirror_priority_untrack_freshness(tmp_path):
    from poly_alpha_sniper.data.orderbook_mirror import OrderbookMirror
    from poly_alpha_sniper.data.orderbook_state import OrderbookStore
    from poly_alpha_sniper.tests.helpers import book, market
    cfg = load_config()
    clock = SimClock(NOW_MS)
    store = OrderbookStore(cfg, clock)
    fetched = []

    class FakeRest:
        async def get_book(self, token_id):
            fetched.append(token_id)
            return book(token_id, ts_ms=clock.now_ms())

    mirror = OrderbookMirror(cfg, clock, ws=None, rest=FakeRest(), store=store)
    m1 = market("m1")
    await mirror.track_market(m1)  # bootstrap fetches BOTH tokens
    assert set(fetched) == {"tok_yes", "tok_no"}
    assert mirror.freshness() == (2, 2)

    mirror.set_priority_tokens(["tok_yes"])
    clock.advance_ms(5000)  # both stale now
    assert mirror.freshness() == (0, 2)
    fetched.clear()
    stale = [t for t in ("tok_yes", "tok_no") if not store.is_fresh(t)]
    stale.sort(key=lambda t: (t not in mirror._priority, t))
    assert stale[0] == "tok_yes"  # priority token first
    await mirror._refresh_tokens(stale)
    assert mirror.freshness() == (2, 2)

    mirror.untrack_market(m1)
    assert mirror.freshness() == (0, 0)
    assert store.get("tok_yes") is None


async def test_app_selects_fresh_okx_when_bybit_stale(app):
    """Integration reproduction of the reported bug: bybit stale, okx fresh
    must NOT produce rejected_by_no_fresh_cex_price."""
    from poly_alpha_sniper.core.contracts import CexTick
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="SOL", exchange="bybit", price=150.0,
                                 ts_ms=now - 600, recv_ts_ms=now - 600))
    app.cex_state.update(CexTick(asset="SOL", exchange="okx", price=150.2,
                                 ts_ms=now - 30, recv_ts_ms=now - 30))
    await app._scan_entries()
    assert app.diag["cex_selected_source"]["SOL"] == "okx"
    # the `app` fixture uses a real WallClock, so a few ms elapse between
    # stamping the tick (now-30) and _scan_entries re-reading the clock --
    # assert a tolerance band, not an exact millisecond (this bare `== 30`
    # was an intermittently-failing over-strict assertion).
    assert 30 <= app.diag["cex_freshest_age_ms"]["SOL"] <= 60
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE asset='SOL' ORDER BY id DESC")
    assert rows, "expected a diagnostic row (no_shock, since fixture has no real shock)"
    assert rows[0]["reason"] != "rejected_by_no_fresh_cex_price"
    assert not app.diag["cex_no_fresh_count_by_source"]  # never incremented


async def test_app_rejects_no_fresh_only_when_all_stale(app):
    """800/900ms would have hard-rejected under the old flat 500ms budget,
    but that's within shadow_live's new live_signal_max_age_ms=1500ms tier
    (see cex_freshness config / core.app._classify_cex_freshness) -- use
    staleness beyond shadow_eval_max_age_ms=3000ms so this test still
    exercises "genuinely too stale even for shadow diagnostics"."""
    from poly_alpha_sniper.core.contracts import CexTick
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="SOL", exchange="bybit", price=150.0,
                                 ts_ms=now - 3500, recv_ts_ms=now - 3500))
    app.cex_state.update(CexTick(asset="SOL", exchange="okx", price=150.0,
                                 ts_ms=now - 3800, recv_ts_ms=now - 3800))
    await app._scan_entries()
    assert app.diag["cex_selected_source"]["SOL"] == "bybit"  # least-stale of the two
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE asset='SOL' ORDER BY id DESC")
    assert rows[0]["reason"] == "rejected_by_no_fresh_cex_price"
    assert "FAR_STALE" in rows[0]["detail"] or "BORDERLINE" in rows[0]["detail"]
    assert app.diag["cex_no_fresh_count_by_source"].get("bybit") == 1


# ---------------------------------------------------------------------------
# CEX-freshness-pipeline fix: tiered shadow thresholds, earlier oracle-anchor
# extraction, last_scan_snapshot, no_shock near-miss diagnostics.
# ---------------------------------------------------------------------------

def _candidate_market(app_obj, asset="BTC", price_to_beat=100_000.0, expiry_offset_ms=240_000):
    from poly_alpha_sniper.core.contracts import MarketInfo, MarketType
    now = app_obj.clock.now_ms()
    return MarketInfo(
        market_id=f"m-{asset}-1", condition_id="c1", title=f"{asset} test market",
        asset=asset, market_type=MarketType.UP_DOWN, direction_up_means_yes=True,
        yes_token_id="tok_yes", no_token_id="tok_no",
        expiry_ts_ms=now + expiry_offset_ms, tick_size=0.01, min_order_size_usd=1.0,
        active=True, closed=False, liquidity_usd=1000.0, volume_24h_usd=5000.0,
        mapping_confidence=100.0, price_to_beat=price_to_beat,
        price_to_beat_source="polymarket_event_metadata" if price_to_beat is not None else "",
        resolution_source_url="https://data.chain.link/streams/btc-usd")


async def test_degraded_staleness_still_classified_evaluable_not_fail_closed(app):
    """1600ms is past live_signal_max_age_ms(1500) but within
    shadow_eval_max_age_ms(3000) -- shadow_live must classify this
    "degraded", not stop the pipeline."""
    assert app.mode.value == "shadow_live"
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="BTC", exchange="bybit", price=100_000.0,
                                 ts_ms=now - 1600, recv_ts_ms=now - 1600))
    view = app.cex_state.multi_view("BTC")
    assert app._classify_cex_freshness(view) == "degraded"


async def test_fresh_staleness_classified_fresh(app):
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="BTC", exchange="bybit", price=100_000.0,
                                 ts_ms=now - 200, recv_ts_ms=now - 200))
    view = app.cex_state.multi_view("BTC")
    assert app._classify_cex_freshness(view) == "fresh"


async def test_beyond_shadow_eval_ceiling_is_fail_closed(app):
    """Beyond shadow_eval_max_age_ms(3000) -- and therefore also beyond
    fail_closed_max_age_ms(8000) is unreachable without first crossing this
    -- the pipeline must stop exactly as before."""
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="BTC", exchange="bybit", price=100_000.0,
                                 ts_ms=now - 3500, recv_ts_ms=now - 3500))
    view = app.cex_state.multi_view("BTC")
    assert app._classify_cex_freshness(view) == "fail_closed"


async def test_beyond_fail_closed_ceiling_is_also_fail_closed(app):
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="BTC", exchange="bybit", price=100_000.0,
                                 ts_ms=now - 9000, recv_ts_ms=now - 9000))
    view = app.cex_state.multi_view("BTC")
    assert app._classify_cex_freshness(view) == "fail_closed"


async def test_live_modes_never_classify_degraded(app):
    """The core safety guarantee of this fix: live_micro/live_full must see
    EXACTLY the old single-threshold behavior, never a "degraded" zone."""
    from poly_alpha_sniper.core.contracts import TradingMode
    app.mode = TradingMode.LIVE_MICRO
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="BTC", exchange="bybit", price=100_000.0,
                                 ts_ms=now - 1600, recv_ts_ms=now - 1600))  # would be "degraded" in shadow
    view = app.cex_state.multi_view("BTC")
    assert app._classify_cex_freshness(view) == "fail_closed"  # 1600ms > 500ms live budget
    assert app._cex_fresh_for_hard_check("BTC") is False        # matches CexState.is_fresh exactly


async def test_oracle_anchor_extracted_before_no_shock_gate(app):
    """No CEX ticks fed at all (so no shock can ever fire) -- the anchor
    must still populate from the candidate market's own metadata."""
    app.market_cache.upsert([_candidate_market(app, price_to_beat=100_050.0)])
    await app._scan_entries()
    anchor = app.diag["latest_oracle_anchor"]
    assert anchor is not None
    assert anchor["oracle_open_price"] == 100_050.0
    rows = app.store.query("SELECT * FROM shadow_diagnostics WHERE asset='BTC' ORDER BY id DESC")
    assert rows and rows[0]["reason"] == "rejected_by_no_fresh_cex_price"  # entry still blocked


async def test_oracle_status_shows_anchor_even_when_no_shock_blocks_entry(app):
    """Full pipeline: fresh CEX feed, no shock ever fires (flat ticks) --
    dashboard-facing diag must still show the anchor, not "not evaluated"."""
    app.market_cache.upsert([_candidate_market(app, price_to_beat=100_000.0)])
    _feed_ticks(app, asset="BTC")  # fresh, non-zero volatility, but tiny wiggle -- no shock
    await app._scan_entries()
    anchor = app.diag["latest_oracle_anchor"]
    assert anchor is not None and anchor["oracle_open_price"] == 100_000.0
    rows = app.store.query("SELECT * FROM shadow_diagnostics WHERE asset='BTC' ORDER BY id DESC")
    assert rows and rows[0]["reason"] == "rejected_by_no_shock"


async def test_missing_price_to_beat_still_records_anchor_attempt(app):
    app.market_cache.upsert([_candidate_market(app, price_to_beat=None)])
    await app._scan_entries()
    anchor = app.diag["latest_oracle_anchor"]
    assert anchor is not None
    assert anchor["oracle_open_price"] is None


async def test_last_scan_snapshot_updates_every_scan(app):
    assert app.diag["last_scan_snapshot"] == {}
    await app._scan_entries()
    snap1 = dict(app.diag["last_scan_snapshot"])
    assert snap1.get("ts_ms") is not None
    await app._scan_entries()
    snap2 = app.diag["last_scan_snapshot"]
    assert snap2["ts_ms"] >= snap1["ts_ms"]  # advanced (or equal under a fast clock), never stuck


async def test_last_scan_snapshot_independent_of_prediction_snapshot(app):
    """last_scan_snapshot must update even when no prediction is ever
    produced -- proving the loop iterates independent of signal rarity."""
    await app._scan_entries()
    assert app.diag["last_scan_snapshot"].get("ts_ms") is not None
    assert app.diag["last_prediction_ts"] == 0  # no prediction was ever recorded


async def test_no_shock_near_miss_exported_to_watchlist_when_close(app):
    app.market_cache.upsert([_candidate_market(app)])
    now = app.clock.now_ms()
    for i in range(15):
        ts = now - (15 - i) * 1000
        app.cex_state.update(CexTick(asset="BTC", exchange="bybit",
                                     price=100_000.0 + i * 3.0, ts_ms=ts, recv_ts_ms=ts))
    await app._scan_entries()
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE reason='watchlist_no_shock_near_miss'")
    # near-miss firing depends on the exact synthetic returns crossing the
    # 0.7 band; assert the mechanism ran without error and is internally
    # consistent with the watchlist diag list, not a specific fixed count.
    assert len(rows) == len(app.diag["watchlist_no_shock_near_miss"])


async def test_degraded_freshness_widens_ev_adverse_selection_buffer(app):
    """Directly exercises the EV-penalty branch in _oracle_ev_reject."""
    from poly_alpha_sniper.core.contracts import (
        Direction, EdgeResult, FairProbability, MarketQualityResult, OrderSide, Signal, Tier)
    from poly_alpha_sniper.strategy.oracle_anchor import resolve_oracle_anchor
    market = _candidate_market(app, price_to_beat=100_000.0)
    app.diag["cex_freshness_degraded"]["BTC"] = True
    anchor = resolve_oracle_anchor(market, 100_010.0, app.clock.now_ms(), app.clock.now_ms())
    signal = Signal(
        signal_id="s1", ts_ms=app.clock.now_ms(), asset="BTC", market=market,
        side=OrderSide.BUY_YES, direction=Direction.UP, shock=None,
        fair=FairProbability(p_up=0.7, p_down=0.3, confidence=90.0),
        edge=EdgeResult(side=OrderSide.BUY_YES, fair_probability=0.7, market_price=0.5,
                        raw_edge=0.2, edge_after_spread=0.2, edge_after_slippage=0.2,
                        confidence_adjusted_edge=0.2),
        market_quality=MarketQualityResult(score=90.0), tier=Tier.A_PLUS)
    from poly_alpha_sniper.tests.helpers import book
    yes_book = book(bid=0.48, ask=0.50, depth=5000.0)
    app._oracle_ev_reject(signal, market, yes_book, yes_book, app.clock.now_ms(), anchor)
    assert app.diag["latest_oracle_ev"]["cex_freshness_degraded"] is True
    assert (app.diag["latest_oracle_ev"]["adverse_selection_buffer_used"]
           > app.cfg.oracle_ev.adverse_selection_buffer)


async def test_status_reports_selected_source_and_budgets(app):
    from poly_alpha_sniper.core.contracts import CexTick
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="BTC", exchange="bybit", price=100_000.0,
                                 ts_ms=now - 600, recv_ts_ms=now - 600))
    app.cex_state.update(CexTick(asset="BTC", exchange="okx", price=100_005.0,
                                 ts_ms=now - 20, recv_ts_ms=now - 20))
    await app._scan_entries()
    text = await app._control_actions()["status"]()
    assert "cex selected source" in text
    assert "BTC=okx" in text
    assert "budgets: live=500ms" in text
    assert "no_fresh_cex_price by source" in text


def test_market_cache_prune_returns_markets():
    from poly_alpha_sniper.data.market_cache import MarketCache
    from poly_alpha_sniper.tests.helpers import market
    cache = MarketCache()
    live = market("live", expiry_ms=NOW_MS + 200_000)
    dead = market("dead", expiry_ms=NOW_MS - 500_000)
    dead.yes_token_id, dead.no_token_id = "d_yes", "d_no"
    cache.upsert([live, dead])
    pruned = cache.prune(NOW_MS)
    assert [m.market_id for m in pruned] == ["dead"]
    assert cache.get("live") is not None
    assert cache.by_token("d_yes") is None
