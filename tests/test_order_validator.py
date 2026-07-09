import pytest

from poly_alpha_sniper.core.contracts import (
    BookLevel, OrderbookSnapshot, OrderRequest, OrderSide, RejectReason)
from poly_alpha_sniper.execution.order_validator import validate_order
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg, market, portfolio_snapshot


_DEFAULT = object()


def _max_trade_usd_cfg():
    """These tests exercise the legacy "max_trade_usd" min-order checks
    specifically -- pin the mode explicitly rather than relying on
    config.yaml's ambient default (fixed_min_shares as of WS4), which skips
    these checks entirely."""
    c = cfg()
    c.risk.sizing_mode = "max_trade_usd"
    return c


def _req(side=OrderSide.BUY_YES, price=0.50, shares=2.0, usd=None):
    # $1.00 order: exactly the min-order floor AND the 10%-of-$10 market cap
    return OrderRequest(order_id="o1", token_id="tok_yes", market_id="m1",
                        side=side, price=price, size_shares=shares,
                        size_usd=usd if usd is not None else round(price * shares, 4))


def _validate(req=None, bk=_DEFAULT, m=None, snap=None, c=None, owned=0.0, now=NOW_MS):
    default_book = book(bid=0.48, ask=0.50)
    return validate_order(req or _req(), default_book if bk is _DEFAULT else bk,
                          m or market(), snap or portfolio_snapshot(),
                          c or cfg(), now, owned_shares=owned)


def test_happy_path_buy():
    d = _validate()
    assert d.approved, d.reject_reason


def test_stale_orderbook_rejected():
    d = _validate(bk=book(ts_ms=NOW_MS - 10_000))
    assert d.reject_reason == RejectReason.STALE_ORDERBOOK


def test_missing_book_rejected():
    d = _validate(bk=None)
    assert d.reject_reason == RejectReason.STALE_ORDERBOOK


def test_one_sided_book_rejected():
    one_sided = OrderbookSnapshot(token_id="tok_yes", ts_ms=NOW_MS,
                                  asks=[BookLevel(0.6, 100)])
    d = _validate(bk=one_sided)
    assert d.reject_reason == RejectReason.NO_BEST_BID_ASK


def test_invalid_tick_price_rejected():
    d = _validate(req=_req(price=0.5037, usd=1.0))
    assert d.reject_reason == RejectReason.INVALID_PRICE_PRECISION


def test_invalid_tick_size_rejected():
    m = market()
    m.tick_size = 0.0
    d = _validate(m=m)
    assert d.reject_reason == RejectReason.INVALID_TICK_SIZE


def test_min_order_size_rejected():
    m = market()
    m.min_order_size_usd = 5.0
    d = _validate(m=m, c=_max_trade_usd_cfg())
    assert d.reject_reason == RejectReason.MIN_ORDER_SIZE_TOO_HIGH
    assert d.sizing_detail["ask_price"] == pytest.approx(0.50)
    assert d.sizing_detail["min_required_usd"] > 0


def test_min_order_share_floor_rejected_includes_full_sizing_detail():
    """The practically-firing case in max_trade_usd mode: Polymarket's share
    minimum (config.yaml discovery default 5) vs. this bankroll's $1
    max_trade_usd. Every field the small-bankroll diagnostic report needs
    must be present and correct -- this used to surface only as a bare
    REJECTED_MIN_ORDER_SIZE_TOO_HIGH string with no explanation of why."""
    m = market()
    m.raw = {"min_order_shares": 5}
    c = _max_trade_usd_cfg()
    # default _req(): price=0.50, shares=2.0, usd=1.0 -> 2.0 shares < 5 required
    d = _validate(m=m, c=c)
    assert d.reject_reason == RejectReason.MIN_ORDER_SIZE_TOO_HIGH
    detail = d.sizing_detail
    assert detail["min_shares"] == 5
    assert detail["ask_price"] == pytest.approx(0.50)
    assert detail["min_required_usd"] == pytest.approx(2.50)          # 5 * 0.50
    assert detail["configured_max_trade_usd"] == c.risk.max_trade_usd
    assert detail["proposed_usd"] == pytest.approx(1.0)
    assert detail["available_cash_usd"] == pytest.approx(10.0)
    assert detail["shortfall_usd"] == pytest.approx(1.50)             # 2.50 - 1.0


# ---------------------------------------------------------------------------
# WS4 follow-up: fixed_min_shares mode must never re-block an
# already-correctly-sized order through these legacy max_trade_usd-relative
# checks -- position_sizer.py already treats fixed sizing as the min order
# by construction; this validator must agree.
# ---------------------------------------------------------------------------

def _fixed_shares_cfg():
    c = cfg()
    c.risk.sizing_mode = "fixed_min_shares"
    return c


def test_fixed_min_shares_mode_ignores_market_min_order_size_usd():
    m = market()
    m.min_order_size_usd = 999.0  # would hard-block in max_trade_usd mode
    d = _validate(m=m, c=_fixed_shares_cfg())
    assert d.approved, d.reject_reason


def test_fixed_min_shares_mode_ignores_market_min_order_shares():
    m = market()
    m.raw = {"min_order_shares": 5}
    # default _req(): shares=2.0 -- would fail the share-floor check below 5
    d = _validate(m=m, c=_fixed_shares_cfg())
    assert d.approved, d.reject_reason


def test_fixed_min_shares_mode_never_returns_legacy_sizing_detail():
    m = market()
    m.min_order_size_usd = 999.0
    d = _validate(m=m, c=_fixed_shares_cfg())
    assert "configured_max_trade_usd" not in d.sizing_detail


def test_screenshot_regression_sol_five_shares_passes_with_exposure_headroom():
    # Same ask as the reported case, but enough equity that the unrelated
    # 10%-of-equity market exposure cap isn't also in play.
    m = market()
    req = OrderRequest(order_id="o", token_id="tok_yes", market_id="m1",
                       side=OrderSide.BUY_YES, price=0.52, size_shares=5.0,
                       size_usd=2.60)
    snap = portfolio_snapshot(equity=50, cash=50)
    d = _validate(req=req, bk=book(bid=0.50, ask=0.52), m=m, snap=snap,
                 c=_fixed_shares_cfg())
    assert d.approved, d.reject_reason


def test_screenshot_regression_at_reported_equity_blocked_by_exposure_not_min_order():
    """At the ACTUAL reported equity ($12.44), the 10%-of-equity market
    exposure cap ($1.244) is genuinely below the $2.60 fixed order -- real,
    correctly-enforced. What matters here is what it must NOT say: never
    MIN_ORDER_SIZE_TOO_HIGH, never reference max_trade_usd."""
    m = market()
    req = OrderRequest(order_id="o", token_id="tok_yes", market_id="m1",
                       side=OrderSide.BUY_YES, price=0.52, size_shares=5.0,
                       size_usd=2.60)
    snap = portfolio_snapshot(equity=12.44, cash=12.44)
    d = _validate(req=req, bk=book(bid=0.50, ask=0.52), m=m, snap=snap,
                 c=_fixed_shares_cfg())
    assert not d.approved
    assert d.reject_reason == RejectReason.MAX_EXPOSURE
    assert d.reject_reason != RejectReason.MIN_ORDER_SIZE_TOO_HIGH


def test_screenshot_regression_insufficient_cash_variant():
    m = market()
    req = OrderRequest(order_id="o", token_id="tok_yes", market_id="m1",
                       side=OrderSide.BUY_YES, price=0.52, size_shares=5.0,
                       size_usd=2.60)
    snap = portfolio_snapshot(equity=2.59, cash=2.59)
    d = _validate(req=req, bk=book(bid=0.50, ask=0.52), m=m, snap=snap,
                 c=_fixed_shares_cfg())
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH


def test_insufficient_cash_rejected():
    d = _validate(snap=portfolio_snapshot(cash=0.10))
    assert d.reject_reason == RejectReason.INSUFFICIENT_CASH


def test_insufficient_shares_on_sell():
    d = _validate(req=_req(side=OrderSide.SELL_YES, price=0.48), owned=0.5)
    assert d.reject_reason == RejectReason.INSUFFICIENT_SHARES


def test_sell_with_shares_ok():
    d = _validate(req=_req(side=OrderSide.SELL_YES, price=0.48), owned=2.0)
    assert d.approved, d.reject_reason


def test_spread_too_wide_rejected():
    d = _validate(bk=book(bid=0.40, ask=0.50))
    assert d.reject_reason == RejectReason.SPREAD_TOO_WIDE


def test_slippage_too_high_rejected():
    thin = OrderbookSnapshot(
        token_id="tok_yes", ts_ms=NOW_MS,
        bids=[BookLevel(0.58, 100)],
        asks=[BookLevel(0.60, 0.5), BookLevel(0.63, 0.5), BookLevel(0.66, 100)])
    d = _validate(req=_req(shares=5.0, price=0.66), bk=thin,
                  snap=portfolio_snapshot(equity=100, cash=10))
    assert d.reject_reason == RejectReason.SLIPPAGE_TOO_HIGH


def test_exposure_cap_rejected():
    snap = portfolio_snapshot(cash=10, total_exposure_usd=2.9,
                              exposure_by_market={"other": 2.9})
    d = _validate(snap=snap)
    assert d.reject_reason == RejectReason.MAX_EXPOSURE


def test_closed_market_rejected():
    m = market()
    m.closed = True
    d = _validate(m=m)
    assert d.reject_reason == RejectReason.AMBIGUOUS_MARKET
