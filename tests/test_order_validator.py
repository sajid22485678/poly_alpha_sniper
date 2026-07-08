from poly_alpha_sniper.core.contracts import (
    BookLevel, OrderbookSnapshot, OrderRequest, OrderSide, RejectReason)
from poly_alpha_sniper.execution.order_validator import validate_order
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg, market, portfolio_snapshot


_DEFAULT = object()


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
    d = _validate(m=m)
    assert d.reject_reason == RejectReason.MIN_ORDER_SIZE_TOO_HIGH


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
