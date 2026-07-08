"""Regression tests for the phantom-exposure investigation: order #1 (SOL,
market 2816222) rested OPEN with filled_shares=0 indefinitely (root cause
fixed in execution/order_manager.py + storage/order_reconciliation.py). These
tests lock in the separate, pre-existing invariant that made the exposure
math itself safe throughout: Portfolio/RiskManager compute exposure strictly
from confirmed FILLS, never from raw order state, so a resting/never-filled
order cannot inflate exposure_by_market or total_exposure_usd even without
the reconciliation fix."""
from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import FillRecord, OrderSide, RejectReason, TradingMode
from poly_alpha_sniper.portfolio.positions import Portfolio
from poly_alpha_sniper.risk.kill_switch import KillSwitch
from poly_alpha_sniper.risk.panic_mode import PanicMode
from poly_alpha_sniper.risk.risk_manager import RiskManager
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg, portfolio_snapshot, signal


def test_unfilled_order_never_creates_a_position():
    """A submitted order that rests OPEN with zero fill must never appear in
    Portfolio._positions -- exposure is fill-based, not order-based."""
    p = Portfolio(cfg(), SimClock(NOW_MS))
    snap = p.snapshot(NOW_MS)
    assert snap.total_exposure_usd == 0.0
    assert snap.exposure_by_market == {}
    assert snap.open_positions == 0


def test_filled_position_counts_toward_exposure_then_clears_on_exit():
    p = Portfolio(cfg(), SimClock(NOW_MS))
    buy = FillRecord(order_id="o1", token_id="tok", market_id="m1",
                     side=OrderSide.BUY_YES, price=0.19, size_shares=5.26, ts_ms=NOW_MS)
    p.apply_fill(buy)
    snap = p.snapshot(NOW_MS)
    assert snap.total_exposure_usd > 0.0
    assert snap.exposure_by_market["m1"] > 0.0
    assert snap.open_positions == 1

    sell = FillRecord(order_id="o2", token_id="tok", market_id="m1",
                      side=OrderSide.SELL_YES, price=0.16, size_shares=5.26, ts_ms=NOW_MS + 800)
    p.apply_fill(sell)
    snap2 = p.snapshot(NOW_MS + 800)
    assert snap2.total_exposure_usd == 0.0
    assert snap2.exposure_by_market == {}
    assert snap2.open_positions == 0


def test_max_exposure_does_not_fire_when_no_fills_exist():
    """With a portfolio snapshot that correctly reflects zero fills (as it
    would be even while a phantom unfilled order sits in the orders table),
    a normal-sized signal must be approved, not rejected as MAX_EXPOSURE."""
    rm = RiskManager(cfg(), SimClock(NOW_MS), KillSwitch(), PanicMode())
    snap = portfolio_snapshot()  # equity=10, cash=10, no exposure, no positions
    d = rm.check_entry(signal(), snap, TradingMode.SHADOW_LIVE)
    assert d.approved
    assert d.reject_reason != RejectReason.MAX_EXPOSURE
