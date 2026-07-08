import pytest

from poly_alpha_sniper.core.contracts import RejectReason, TradingMode
from poly_alpha_sniper.risk.position_sizer import compute_position_size
from poly_alpha_sniper.tests.helpers import cfg, market, portfolio_snapshot


def test_ten_pct_of_ten_dollars_clamped_to_min_one():
    d = compute_position_size(cfg(), portfolio_snapshot(equity=10, cash=10),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.approved
    assert d.size_usd == 1.0  # raw $1.00 == min == max


def test_equity_growth_capped_by_max_trade():
    d = compute_position_size(cfg(), portfolio_snapshot(equity=50, cash=50),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.approved
    assert d.size_usd == 1.0  # raw $5 but max_trade_usd=1


def test_compounding_uses_realized_equity_only():
    # unrealized pnl present but equity (realized) unchanged -> same size
    snap = portfolio_snapshot(equity=10, cash=10, unrealized_pnl_usd=50.0)
    d = compute_position_size(cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert d.size_usd == 1.0


def test_insufficient_cash_rejected():
    d = compute_position_size(cfg(), portfolio_snapshot(equity=10, cash=0.5),
                              market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH


def test_min_order_too_high_rejected():
    m = market()
    m.min_order_size_usd = 5.0
    d = compute_position_size(cfg(), portfolio_snapshot(), m, TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MIN_ORDER_SIZE_TOO_HIGH


def test_market_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"m1": 0.9}, total_exposure_usd=0.9)
    d = compute_position_size(cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_total_exposure_cap():
    snap = portfolio_snapshot(equity=10, cash=10,
                              exposure_by_market={"other": 2.5}, total_exposure_usd=2.5)
    d = compute_position_size(cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_edge_killed_by_costs_rejected():
    d = compute_position_size(cfg(), portfolio_snapshot(), market(),
                              TradingMode.SHADOW_LIVE, -0.01)
    assert not d.approved
    assert d.reject_reason == RejectReason.SLIPPAGE_TOO_HIGH


def test_daily_loss_cap():
    snap = portfolio_snapshot(equity=10, cash=10, realized_pnl_today_usd=-2.0)
    d = compute_position_size(cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.DAILY_LOSS_CAP


def test_loss_streak_cap():
    snap = portfolio_snapshot(equity=10, cash=10, consecutive_losses=2)
    d = compute_position_size(cfg(), snap, market(), TradingMode.SHADOW_LIVE, 0.08)
    assert not d.approved
    assert d.reject_reason == RejectReason.LOSS_STREAK


def test_live_micro_cap_applies():
    c = cfg()
    c.risk.max_trade_usd = 5
    c.risk.live_micro_trade_usd = 1
    d = compute_position_size(c, portfolio_snapshot(equity=100, cash=100),
                              market(), TradingMode.LIVE_MICRO, 0.08)
    assert d.approved
    assert d.size_usd == 1.0
