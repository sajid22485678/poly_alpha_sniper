import pytest

from poly_alpha_sniper.core.contracts import BookLevel, OrderSide, OrderbookSnapshot
from poly_alpha_sniper.strategy.edge_engine import compute_edge, walk_book_avg_price
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg


def test_buy_edge_math():
    e = compute_edge(OrderSide.BUY_YES, fair_p=0.70, book=book(bid=0.58, ask=0.60),
                     cfg=cfg(), size_usd=1.0, confidence=80)
    assert e is not None
    assert e.raw_edge == pytest.approx(0.10)
    assert e.edge_after_spread < e.raw_edge
    assert e.edge_after_slippage <= e.edge_after_spread
    assert e.confidence_adjusted_edge == pytest.approx(e.edge_after_slippage * 0.8)


def test_sell_edge_uses_bid():
    e = compute_edge(OrderSide.SELL_YES, fair_p=0.50, book=book(bid=0.58, ask=0.60),
                     cfg=cfg(), size_usd=1.0, confidence=80)
    assert e is not None
    assert e.market_price == 0.58
    assert e.raw_edge == pytest.approx(0.08)


def test_empty_book_returns_none():
    empty = OrderbookSnapshot(token_id="t", ts_ms=NOW_MS)
    assert compute_edge(OrderSide.BUY_YES, 0.7, empty, cfg(), 1.0, 80) is None


def test_thin_depth_punished():
    thin = OrderbookSnapshot(token_id="t", ts_ms=NOW_MS,
                             bids=[BookLevel(0.58, 0.5)],
                             asks=[BookLevel(0.60, 0.5)])  # $0.30 depth only
    deep = book()
    e_thin = compute_edge(OrderSide.BUY_YES, 0.7, thin, cfg(), 5.0, 80)
    e_deep = compute_edge(OrderSide.BUY_YES, 0.7, deep, cfg(), 5.0, 80)
    assert e_thin.edge_after_slippage < e_deep.edge_after_slippage


def test_walk_book_partial_fill():
    levels = [BookLevel(0.60, 1.0)]  # $0.60 available
    avg, filled = walk_book_avg_price(levels, 5.0)
    assert avg == pytest.approx(0.60)
    assert filled == pytest.approx(0.60)


def test_negative_edge_possible():
    e = compute_edge(OrderSide.BUY_YES, fair_p=0.55, book=book(bid=0.58, ask=0.60),
                     cfg=cfg(), size_usd=1.0, confidence=80)
    assert e.raw_edge < 0
