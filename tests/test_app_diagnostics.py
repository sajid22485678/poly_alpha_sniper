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
    assert app.diag["cex_freshest_age_ms"]["SOL"] == 30
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE asset='SOL' ORDER BY id DESC")
    assert rows, "expected a diagnostic row (no_shock, since fixture has no real shock)"
    assert rows[0]["reason"] != "rejected_by_no_fresh_cex_price"
    assert not app.diag["cex_no_fresh_count_by_source"]  # never incremented


async def test_app_rejects_no_fresh_only_when_all_stale(app):
    from poly_alpha_sniper.core.contracts import CexTick
    now = app.clock.now_ms()
    app.cex_state.update(CexTick(asset="SOL", exchange="bybit", price=150.0,
                                 ts_ms=now - 800, recv_ts_ms=now - 800))
    app.cex_state.update(CexTick(asset="SOL", exchange="okx", price=150.0,
                                 ts_ms=now - 900, recv_ts_ms=now - 900))
    await app._scan_entries()
    assert app.diag["cex_selected_source"]["SOL"] == "bybit"  # least-stale of the two
    rows = app.store.query(
        "SELECT * FROM shadow_diagnostics WHERE asset='SOL' ORDER BY id DESC")
    assert rows[0]["reason"] == "rejected_by_no_fresh_cex_price"
    assert "FAR_STALE" in rows[0]["detail"] or "BORDERLINE" in rows[0]["detail"]
    assert app.diag["cex_no_fresh_count_by_source"].get("bybit") == 1


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
