import pytest

from poly_alpha_sniper.core.contracts import RejectReason, TradingMode
from poly_alpha_sniper.risk.position_sizer import compute_position_size
from poly_alpha_sniper.tests.helpers import cfg, market, portfolio_snapshot


def _cfg():
    """These tests exercise the "max_trade_usd" sizing mode specifically --
    pin it explicitly rather than relying on whatever config.yaml's ambient
    default happens to be (WS4 changed the shipped default to
    fixed_min_shares; these tests must stay deterministic either way)."""
    c = cfg()
    c.risk.sizing_mode = "max_trade_usd"
    return c


def test_ten_pct_of_ten_dollars_clamped_to_min_one():
    d = compute_position_size(_cfg(), portfolio_snapshot(equity=10, cash=10),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.approved
    assert d.size_usd == 1.0  # raw $1.00 == min == max


def test_equity_growth_capped_by_max_trade():
    d = compute_position_size(_cfg(), portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.approved
    assert d.size_usd == 1.0  # raw $5 but max_trade_usd=1


def test_compounding_uses_realized_equity_only():
    # unrealized pnl present but equity (realized) unchanged -> same size
    snap = portfolio_snapshot(equity=10, cash=10, unrealized_pnl_usd=50.0)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.size_usd == 1.0


def test_insufficient_cash_rejected():
    d = compute_position_size(_cfg(), portfolio_snapshot(equity=10, cash=0.5),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH


def test_min_order_too_high_rejected():
    m = market()
    m.min_order_size_usd = 5.0
    d = compute_position_size(_cfg(), portfolio_snapshot(), m, TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MIN_ORDER_SIZE_TOO_HIGH


def test_market_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"m1": 0.9}, total_exposure_usd=0.9)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_total_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"other": 2.5}, total_exposure_usd=2.5)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_edge_killed_by_costs_rejected():
    d = compute_position_size(_cfg(), portfolio_snapshot(), market(),
                              TradingMode.SHADOW_LIVE, -0.01)
    assert not d.approved
    assert d.reject_reason == RejectReason.SLIPPAGE_TOO_HIGH


def test_daily_loss_cap():
    snap = portfolio_snapshot(equity=10, cash=10, realized_pnl_today_usd=-2.0)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.DAILY_LOSS_CAP


def test_loss_streak_cap():
    snap = portfolio_snapshot(equity=10, cash=10, consecutive_losses=2)
    d = compute_position_size(_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.LOSS_STREAK


def test_live_micro_cap_applies():
    c = _cfg()
    c.risk.max_trade_usd = 5
    c.risk.live_micro_trade_usd = 1
    d = compute_position_size(c, portfolio_snapshot(equity=100, cash=100),
                              market(), TradingMode.LIVE_MICRO, 0.08)
    assert d.approved
    assert d.size_usd == 1.0


# ---------------------------------------------------------------------------
# WS4: fixed_min_shares sizing mode
# ---------------------------------------------------------------------------

def _fixed_cfg(fixed_order_shares=5.0):
    c = cfg()
    c.risk.sizing_mode = "fixed_min_shares"
    c.risk.fixed_order_shares = fixed_order_shares
    c.risk.use_max_trade_usd = False
    return c


def test_fixed_min_shares_allowed_when_cash_covers_it_ask_065():
    # equity=50 so the 10%/30%-of-equity exposure caps ($5 / $15) have
    # headroom for a $3.25 fixed-size position -- isolates the sizing
    # mechanism from the (separately-tested) exposure caps.
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.65)
    assert d.approved
    assert d.size_usd == pytest.approx(3.25)  # 5 * 0.65


def test_fixed_min_shares_allowed_when_cash_covers_it_ask_095():
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.95)
    assert d.approved
    assert d.size_usd == pytest.approx(4.75)  # 5 * 0.95


def test_fixed_min_shares_rejected_insufficient_cash():
    # cash=4 is below the $4.75 requirement -- fails the cash check before
    # exposure caps are even reached, so equity=10's tighter caps don't matter.
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=10, cash=4),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.95)
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES


def test_fixed_min_shares_ignores_max_trade_usd_of_one_dollar():
    c = _fixed_cfg()
    c.risk.max_trade_usd = 1  # would have blocked ask=0.65 * 5 shares = $3.25 in the old mode
    d = compute_position_size(c, portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.65)
    assert d.approved
    assert d.size_usd == pytest.approx(3.25)


def test_fixed_min_shares_does_not_check_market_min_order_size_usd():
    """fixed sizing IS the min order by construction -- market.min_order_size_usd
    (the discovery-time conservative floor) must not additionally block it."""
    m = market()
    m.min_order_size_usd = 999.0  # would hard-block in max_trade_usd mode
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=50, cash=50),
                              m, TradingMode.SHADOW_LIVE, 0.08, executable_price=0.65)
    assert d.approved


def test_fixed_min_shares_still_enforces_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"other": 2.9}, total_exposure_usd=2.9)
    d = compute_position_size(_fixed_cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.65)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_fixed_min_shares_rejects_data_quality_when_no_price():
    d = compute_position_size(_fixed_cfg(), portfolio_snapshot(equity=10, cash=10),
                              market(), TradingMode.SHADOW_LIVE, 0.08,
                              executable_price=0.0)
    assert not d.approved
    assert d.reject_reason == RejectReason.DATA_QUALITY
