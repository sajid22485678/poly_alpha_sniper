from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import TradingMode
from poly_alpha_sniper.deterministic_intelligence.trade_frequency_controller import (
    TradeFrequencyController)
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg


def _tfc(clock=None, c=None):
    return TradeFrequencyController(c or cfg(), clock or SimClock(NOW_MS))


def test_allows_by_default():
    ok, reason = _tfc().can_trade("BTC", "m1", TradingMode.SHADOW_LIVE)
    assert ok, reason


def test_live_hourly_cap_blocks():
    c = cfg()
    clock = SimClock(NOW_MS)
    t = _tfc(clock, c)
    for i in range(c.trade_frequency.max_trades_per_hour_live_micro):
        # different assets/markets to avoid other caps
        t.record_entry(f"A{i}", f"m{i}")
    ok, reason = t.can_trade("BTC", "fresh_market", TradingMode.LIVE_MICRO)
    assert not ok
    assert "hourly" in reason


def test_hour_window_slides():
    c = cfg()
    clock = SimClock(NOW_MS)
    t = _tfc(clock, c)
    for i in range(c.trade_frequency.max_trades_per_hour_live_micro):
        t.record_entry(f"A{i}", f"m{i}")
    clock.advance_ms(3_600_001)
    ok, _ = t.can_trade("BTC", "fresh_market", TradingMode.LIVE_MICRO)
    assert ok


def test_per_asset_cap():
    c = cfg()
    t = _tfc(c=c)
    for i in range(c.trade_frequency.max_trades_per_asset_per_hour):
        t.record_entry("BTC", f"m{i}")
    ok, reason = t.can_trade("BTC", "m_new", TradingMode.SHADOW_LIVE)
    assert not ok
    assert "asset" in reason
    ok2, _ = t.can_trade("ETH", "m_eth", TradingMode.SHADOW_LIVE)
    assert ok2


def test_loss_cooldown_blocks_then_expires():
    c = cfg()
    clock = SimClock(NOW_MS)
    t = _tfc(clock, c)
    t.record_entry("BTC", "m1")
    t.record_result("BTC", "m1", win=False)
    ok, reason = t.can_trade("BTC", "m1", TradingMode.SHADOW_LIVE)
    assert not ok and "cooldown" in reason
    clock.advance_ms(int(c.trade_frequency.cooldown_after_loss_seconds * 1000) + 1)
    # cooldown gone but re-entry rule now applies (last was a loss)
    ok2, reason2 = t.can_trade("BTC", "m1", TradingMode.SHADOW_LIVE)
    assert not ok2 and "profit" in reason2


def test_reentry_after_win_allowed_up_to_cap():
    c = cfg()
    clock = SimClock(NOW_MS)
    t = _tfc(clock, c)
    t.record_entry("BTC", "m1")
    t.record_result("BTC", "m1", win=True)
    clock.advance_ms(int(c.trade_frequency.cooldown_after_win_seconds * 1000) + 1)
    ok, reason = t.can_trade("BTC", "m1", TradingMode.SHADOW_LIVE)
    assert ok, reason
    t.record_entry("BTC", "m1")
    t.record_result("BTC", "m1", win=True)
    clock.advance_ms(11_000)
    t.record_entry("BTC", "m1")  # 3rd entry = 2nd re-entry (cap)
    t.record_result("BTC", "m1", win=True)
    clock.advance_ms(11_000)
    ok3, reason3 = t.can_trade("BTC", "m1", TradingMode.SHADOW_LIVE)
    assert not ok3
    assert "re-entries" in reason3


def test_bad_fill_cooldown_longer():
    c = cfg()
    clock = SimClock(NOW_MS)
    t = _tfc(clock, c)
    t.record_entry("BTC", "m1")
    t.record_result("BTC", "m1", win=True, bad_fill=True)
    clock.advance_ms(int(c.trade_frequency.cooldown_after_win_seconds * 1000) + 1)
    ok, _ = t.can_trade("BTC", "m1", TradingMode.SHADOW_LIVE)
    assert not ok  # still in bad-fill cooldown (120s > 10s)
