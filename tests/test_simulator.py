import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import (
    BookLevel, OrderbookSnapshot, OrderRequest, OrderSide, OrderState)
from poly_alpha_sniper.execution.simulator import SimulatedClobClient
from poly_alpha_sniper.tests.helpers import NOW_MS, book


def _client(books: dict, balance: float = 10.0):
    clock = SimClock(NOW_MS)
    c = SimulatedClobClient(clock, lambda t: books.get(t))
    c.set_balance(balance)
    return c


def _req(side=OrderSide.BUY_YES, price=0.60, shares=1.6, token="tok_yes",
         order_id="o1"):
    return OrderRequest(order_id=order_id, token_id=token, market_id="m1",
                        side=side, price=price, size_shares=shares,
                        size_usd=round(price * shares, 4))


async def test_buy_yes_fills_at_ask():
    c = _client({"tok_yes": book(bid=0.58, ask=0.60)})
    rec = await c.place_order(_req())
    assert rec.state == OrderState.MATCHED
    assert rec.avg_fill_price == pytest.approx(0.60)
    assert rec.filled_shares == pytest.approx(1.6)
    assert await c.get_balance_usd() == pytest.approx(10.0 - 0.96)
    positions = await c.get_positions()
    assert len(positions) == 1
    assert positions[0].shares == pytest.approx(1.6)


async def test_depth_limited_partial_fill():
    thin = OrderbookSnapshot(token_id="tok_yes", ts_ms=NOW_MS,
                             bids=[BookLevel(0.58, 100)],
                             asks=[BookLevel(0.60, 1.0)])  # only 1 share
    c = _client({"tok_yes": thin})
    rec = await c.place_order(_req(shares=3.0))
    assert rec.state == OrderState.PARTIAL_FILL
    assert rec.filled_shares == pytest.approx(1.0)
    assert len(await c.get_open_orders()) == 1


async def test_never_better_than_book():
    c = _client({"tok_yes": book(bid=0.58, ask=0.60)})
    rec = await c.place_order(_req(price=0.65, shares=1.0))
    assert rec.avg_fill_price == pytest.approx(0.60)  # book price, not limit


async def test_non_marketable_rests_open():
    c = _client({"tok_yes": book(bid=0.58, ask=0.60)})
    rec = await c.place_order(_req(price=0.55, shares=1.0))
    assert rec.state == OrderState.OPEN
    assert rec.filled_shares == 0.0
    assert await c.cancel_order(rec.order_id)
    assert not await c.get_open_orders()


async def test_sell_yes_realizes_pnl():
    books = {"tok_yes": book(bid=0.58, ask=0.60)}
    c = _client(books)
    await c.place_order(_req(shares=2.0))  # buy 2 @ 0.60
    books["tok_yes"] = book(bid=0.70, ask=0.72)
    rec = await c.place_order(_req(side=OrderSide.SELL_YES, price=0.70, shares=2.0,
                                   order_id="o2"))
    assert rec.state == OrderState.MATCHED
    assert rec.avg_fill_price == pytest.approx(0.70)
    assert await c.get_positions() == []
    # balance: 10 - 1.2 + 1.4 = 10.2
    assert await c.get_balance_usd() == pytest.approx(10.2)


async def test_buy_no_and_sell_no():
    books = {"tok_no": book("tok_no", bid=0.38, ask=0.40)}
    c = _client(books)
    rec = await c.place_order(_req(side=OrderSide.BUY_NO, price=0.40, shares=2.0,
                                   token="tok_no"))
    assert rec.state == OrderState.MATCHED
    books["tok_no"] = book("tok_no", bid=0.45, ask=0.47)
    rec2 = await c.place_order(_req(side=OrderSide.SELL_NO, price=0.45, shares=2.0,
                                    token="tok_no", order_id="o2"))
    assert rec2.filled_shares == pytest.approx(2.0)


async def test_sell_more_than_owned_raises():
    books = {"tok_yes": book()}
    c = _client(books)
    await c.place_order(_req(shares=1.0))
    with pytest.raises(ValueError):
        await c.place_order(_req(side=OrderSide.SELL_YES, price=0.58, shares=5.0,
                                 order_id="o2"))


async def test_sell_with_zero_shares_raises():
    c = _client({"tok_yes": book()})
    with pytest.raises(ValueError):
        await c.place_order(_req(side=OrderSide.SELL_YES, price=0.58, shares=1.0))


async def test_cancel_all():
    c = _client({"tok_yes": book()})
    await c.place_order(_req(price=0.50, shares=1.0, order_id="a"))
    await c.place_order(_req(price=0.51, shares=1.0, order_id="b"))
    assert await c.cancel_all() == 2
