from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import Outcome, RejectReason, TradingMode
from poly_alpha_sniper.risk.kill_switch import KillSwitch
from poly_alpha_sniper.risk.panic_mode import PanicMode
from poly_alpha_sniper.risk.risk_manager import RiskManager
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg, portfolio_snapshot, position, signal


def _rm(c=None):
    return RiskManager(c or cfg(), SimClock(NOW_MS), KillSwitch(), PanicMode())


def test_happy_path_approves_one_dollar():
    d = _rm().check_entry(signal(), portfolio_snapshot(), TradingMode.SHADOW_LIVE)
    assert d.approved
    assert d.size_usd == 1.0


def test_panic_blocks_entry():
    rm = _rm()
    rm.panic.activate("test")
    d = rm.check_entry(signal(), portfolio_snapshot(), TradingMode.SHADOW_LIVE)
    assert not d.approved
    assert d.reject_reason == RejectReason.PANIC_MODE


def test_kill_switch_blocks_entry():
    rm = _rm()
    rm.kill_switch.activate("test")
    d = rm.check_entry(signal(), portfolio_snapshot(), TradingMode.SHADOW_LIVE)
    assert not d.approved
    assert d.reject_reason == RejectReason.KILL_SWITCH


def test_max_open_positions_blocks():
    snap = portfolio_snapshot(open_positions=2)
    d = _rm().check_entry(signal(), snap, TradingMode.SHADOW_LIVE)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_OPEN_POSITIONS


def test_one_position_per_market_blocks():
    snap = portfolio_snapshot(open_positions=1,
                              positions_by_market={"m1": "YES"},
                              exposure_by_market={"m1": 0.6}, total_exposure_usd=0.6)
    d = _rm().check_entry(signal(), snap, TradingMode.SHADOW_LIVE)
    assert not d.approved
    assert d.reject_reason == RejectReason.ONE_POSITION_PER_MARKET


def test_micro_bankroll_halt_after_two_losses():
    snap = portfolio_snapshot(equity=10, cash=10, consecutive_losses=2)
    d = _rm().check_entry(signal(), snap, TradingMode.SHADOW_LIVE)
    assert not d.approved
    assert d.reject_reason == RejectReason.LOSS_STREAK


def test_daily_loss_cap_blocks():
    snap = portfolio_snapshot(realized_pnl_today_usd=-2.5)
    d = _rm().check_entry(signal(), snap, TradingMode.SHADOW_LIVE)
    assert not d.approved
    assert d.reject_reason == RejectReason.DAILY_LOSS_CAP


def test_sell_insufficient_shares_rejected():
    rm = _rm()
    pos = position(shares=1.0)
    d = rm.check_sell(pos, 2.0, book())
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_SHARES


def test_sell_zero_shares_rejected():
    d = _rm().check_sell(position(shares=1.0), 0.0, book())
    assert not d.approved
    assert d.reject_reason == RejectReason.SELL_SIZE_TOO_SMALL


def test_sell_no_position_rejected():
    d = _rm().check_sell(None, 1.0, book())
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_SHARES


def test_sell_allowed_even_in_panic():
    rm = _rm()
    rm.panic.activate("test")
    d = rm.check_sell(position(shares=1.0), 1.0, book())
    assert d.approved  # closing risk is always allowed


def test_sell_stale_book_rejected():
    stale = book(ts_ms=NOW_MS - 60_000)
    d = _rm().check_sell(position(shares=1.0), 1.0, stale)
    assert not d.approved
    assert d.reject_reason == RejectReason.STALE_ORDERBOOK
