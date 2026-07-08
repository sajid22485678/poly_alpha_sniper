"""CexState source selection: the freshest AVAILABLE exchange must always
drive decisions, never a config-preferred-but-stale one.

Root cause this guards against: multi_view()/stats() used to pick
preferred > fallback > first-available WITHOUT checking freshness at all, so
a stale Bybit tick (natural for a low-volume pair like SOL between trade
prints) permanently shadowed a fresher OKX tick and the bot rejected
everything as rejected_by_no_fresh_cex_price even though usable data existed.
"""
from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.data.cex_state import CexState
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg


def _state(clock=None):
    return CexState(cfg(), clock or SimClock(NOW_MS))


def _tick(asset, exchange, price, ts_ms):
    return CexTick(asset=asset, exchange=exchange, price=price,
                   ts_ms=ts_ms, recv_ts_ms=ts_ms)


def test_bybit_stale_okx_fresh_selects_okx():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    state.update(_tick("SOL", "bybit", 150.0, NOW_MS - 600))   # 600ms stale
    state.update(_tick("SOL", "okx", 150.2, NOW_MS - 50))      # 50ms fresh
    view = state.multi_view("SOL")
    assert view is not None
    assert view.primary.exchange == "okx"
    assert view.primary.fresh is True
    assert view.primary.staleness_ms == 50


def test_okx_stale_bybit_fresh_selects_bybit():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    state.update(_tick("SOL", "okx", 150.0, NOW_MS - 700))     # 700ms stale
    state.update(_tick("SOL", "bybit", 150.1, NOW_MS - 20))    # 20ms fresh
    view = state.multi_view("SOL")
    assert view.primary.exchange == "bybit"
    assert view.primary.fresh is True


def test_binance_never_ticking_does_not_participate():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    # Binance geo-blocked: no ticks ever arrive for it (no window created)
    state.update(_tick("BTC", "bybit", 100_000.0, NOW_MS - 100))
    state.update(_tick("BTC", "okx", 100_005.0, NOW_MS - 30))
    view = state.multi_view("BTC")
    assert "binance" not in view.per_exchange
    assert view.primary.exchange == "okx"  # freshest of the two real sources


def test_stale_binance_data_does_not_block_fresh_fallback():
    """Even if Binance briefly reconnects with stale-by-then data, it must
    never win over a genuinely fresh fallback."""
    clock = SimClock(NOW_MS)
    state = _state(clock)
    state.update(_tick("BTC", "binance", 100_000.0, NOW_MS - 900))  # stale
    state.update(_tick("BTC", "bybit", 100_001.0, NOW_MS - 40))     # fresh
    view = state.multi_view("BTC")
    assert view.primary.exchange == "bybit"
    assert view.primary.fresh


def test_all_sources_stale_yields_least_stale_but_not_fresh():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    state.update(_tick("SOL", "bybit", 150.0, NOW_MS - 800))
    state.update(_tick("SOL", "okx", 150.0, NOW_MS - 900))
    view = state.multi_view("SOL")
    assert view.primary.exchange == "bybit"  # least stale of the two
    assert view.primary.fresh is False       # but still genuinely too old
    assert view.any_stale is True


def test_config_order_breaks_exact_ties():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    # identical staleness on both -> config preference (fallback=bybit
    # before optional=okx) decides
    state.update(_tick("ETH", "okx", 3000.0, NOW_MS - 10))
    state.update(_tick("ETH", "bybit", 3000.1, NOW_MS - 10))
    view = state.multi_view("ETH")
    assert view.primary.exchange == "bybit"


def test_stats_and_multi_view_agree_on_selection():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    state.update(_tick("BTC", "bybit", 100_000.0, NOW_MS - 550))
    state.update(_tick("BTC", "okx", 100_002.0, NOW_MS - 10))
    view = state.multi_view("BTC")
    single = state.stats("BTC")
    assert single is not None
    assert single.exchange == view.primary.exchange == "okx"


def test_is_fresh_true_when_any_source_fresh_despite_preferred_stale():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    state.update(_tick("ETH", "bybit", 3000.0, NOW_MS - 900))  # stale
    state.update(_tick("ETH", "okx", 3000.5, NOW_MS - 10))     # fresh
    assert state.is_fresh("ETH") is True


def test_explicit_exchange_request_bypasses_selection():
    clock = SimClock(NOW_MS)
    state = _state(clock)
    state.update(_tick("BTC", "bybit", 100_000.0, NOW_MS - 10))
    # asking for an exchange with no data returns None, not a silent
    # cross-exchange substitute
    assert state.stats("BTC", exchange="okx") is None
    assert state.stats("BTC", exchange="bybit") is not None


def test_price_windows_ready_independent_of_freshness_but_selection_still_fresh():
    """window_ready() answers 'enough history to compute indicators'; primary
    selection answers 'what's the live tradable price right now' — both must
    stay consistent about WHICH exchange is authoritative."""
    clock = SimClock(NOW_MS)
    state = _state(clock)
    for i in range(15):
        ts = NOW_MS - (15 - i) * 1000
        state.update(_tick("SOL", "bybit", 150.0 + i * 0.01, ts))
    state.update(_tick("SOL", "okx", 150.3, NOW_MS - 20))  # freshest
    assert state.window_ready("SOL") is True
    view = state.multi_view("SOL")
    assert view.primary.exchange == "okx"
    assert view.primary.fresh
