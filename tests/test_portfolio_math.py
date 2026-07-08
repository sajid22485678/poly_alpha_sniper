import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import FillRecord, OrderSide
from poly_alpha_sniper.portfolio.positions import Portfolio
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg


def _fill(side, price, shares, token="tok_yes", ts=NOW_MS):
    return FillRecord(order_id="o", token_id=token, market_id="m1", side=side,
                      price=price, size_shares=shares, ts_ms=ts)


def _pf(clock=None):
    return Portfolio(cfg(), clock or SimClock(NOW_MS))


def test_buy_yes_fill_updates_position_and_cash():
    pf = _pf()
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 1.6))
    pos = pf.get("tok_yes")
    assert pos.shares == pytest.approx(1.6)
    assert pos.avg_entry_price == pytest.approx(0.60)
    assert pf.cash == pytest.approx(10 - 0.96)


def test_second_buy_averages():
    pf = _pf()
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 1.0))
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.70, 1.0))
    pos = pf.get("tok_yes")
    assert pos.shares == pytest.approx(2.0)
    assert pos.avg_entry_price == pytest.approx(0.65)


def test_partial_sell_reduces_inventory_and_realizes():
    pf = _pf()
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 2.0))
    pf.apply_fill(_fill(OrderSide.SELL_YES, 0.70, 1.0))
    pos = pf.get("tok_yes")
    assert pos.shares == pytest.approx(1.0)
    snap = pf.snapshot(NOW_MS)
    assert snap.realized_pnl_usd == pytest.approx(0.10)
    assert snap.equity_usd == pytest.approx(10.10)


def test_full_sell_closes_position():
    pf = _pf()
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 2.0))
    pf.apply_fill(_fill(OrderSide.SELL_YES, 0.70, 2.0))
    assert pf.get("tok_yes") is None
    assert pf.snapshot(NOW_MS).realized_pnl_usd == pytest.approx(0.20)
    assert pf.cash == pytest.approx(10 - 1.2 + 1.4)


def test_oversell_raises():
    pf = _pf()
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 1.0))
    with pytest.raises(ValueError):
        pf.apply_fill(_fill(OrderSide.SELL_YES, 0.70, 2.0))


def test_sell_without_position_raises():
    pf = _pf()
    with pytest.raises(ValueError):
        pf.apply_fill(_fill(OrderSide.SELL_NO, 0.5, 1.0, token="tok_no"))


def test_equity_ignores_unrealized():
    pf = _pf()
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 2.0))
    pf.mark("tok_yes", bid=0.90, ask=0.92)  # huge unrealized gain
    snap = pf.snapshot(NOW_MS)
    assert snap.equity_usd == pytest.approx(10.0)  # realized only
    assert snap.unrealized_pnl_usd == pytest.approx(0.60)


def test_daily_pnl_resets_across_day_boundary():
    clock = SimClock(NOW_MS)
    pf = _pf(clock)
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 2.0))
    pf.apply_fill(_fill(OrderSide.SELL_YES, 0.70, 2.0))
    assert pf.snapshot(clock.now_ms()).realized_pnl_today_usd == pytest.approx(0.20)
    clock.advance_ms(25 * 3600 * 1000)  # next day
    snap = pf.snapshot(clock.now_ms())
    assert snap.realized_pnl_today_usd == 0.0
    assert snap.realized_pnl_usd == pytest.approx(0.20)  # total persists


def test_consecutive_loss_counter():
    pf = _pf()
    pf.record_trade_result(False)
    pf.record_trade_result(False)
    assert pf.snapshot(NOW_MS).consecutive_losses == 2
    pf.record_trade_result(True)
    assert pf.snapshot(NOW_MS).consecutive_losses == 0


def test_settle_resolution_win_and_loss():
    pf = _pf()
    pf.apply_fill(_fill(OrderSide.BUY_YES, 0.60, 2.0))
    realized = pf.settle_resolution("tok_yes", won=True, ts_ms=NOW_MS)
    assert realized == pytest.approx(0.80)  # 2 * (1 - 0.6)
    assert pf.get("tok_yes") is None
    pf.apply_fill(_fill(OrderSide.BUY_NO, 0.40, 2.0, token="tok_no"))
    realized2 = pf.settle_resolution("tok_no", won=False, ts_ms=NOW_MS)
    assert realized2 == pytest.approx(-0.80)
