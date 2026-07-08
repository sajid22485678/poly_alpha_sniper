"""End-to-end BUY/SELL flow through OrderManager + SimulatedClobClient,
including the flip rule (sell confirmed BEFORE opposite entry allowed)."""
import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import (
    ExitDecision, ExitReason, OrderRequest, OrderSide, OrderState, Outcome)
from poly_alpha_sniper.execution.close_position import flip_position
from poly_alpha_sniper.execution.order_lifecycle import OrderLifecycle
from poly_alpha_sniper.execution.order_manager import OrderManager
from poly_alpha_sniper.execution.sell_executor import SellExecutor
from poly_alpha_sniper.execution.simulator import SimulatedClobClient
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg, position


def _stack(books: dict):
    clock = SimClock(NOW_MS)
    client = SimulatedClobClient(clock, lambda t: books.get(t))
    client.set_balance(10.0)
    om = OrderManager(cfg(), clock, client, OrderLifecycle())
    sell = SellExecutor(cfg(), clock, om)
    return clock, client, om, sell


def _buy_req(side=OrderSide.BUY_YES, token="tok_yes", price=0.60, shares=1.6):
    return OrderRequest(order_id=f"buy-{token}", token_id=token, market_id="m1",
                        side=side, price=price, size_shares=shares,
                        size_usd=round(price * shares, 4))


async def test_buy_yes_then_sell_yes_full_cycle():
    books = {"tok_yes": book(bid=0.58, ask=0.60)}
    clock, client, om, sell = _stack(books)
    rec = await om.submit(_buy_req(), books["tok_yes"])
    assert rec.state == OrderState.MATCHED
    assert rec.filled_shares == pytest.approx(1.6)

    books["tok_yes"] = book(bid=0.70, ask=0.72)
    pos = position(shares=1.6, entry=0.60)
    decision = ExitDecision.full(ExitReason.TAKE_PROFIT, "tp")
    sell_rec = await sell.execute_exit(pos, decision, books["tok_yes"])
    assert sell_rec.state == OrderState.MATCHED
    assert sell_rec.side == OrderSide.SELL_YES
    assert sell_rec.avg_fill_price >= 0.69


async def test_buy_no_then_sell_no():
    books = {"tok_no": book("tok_no", bid=0.38, ask=0.40)}
    clock, client, om, sell = _stack(books)
    rec = await om.submit(_buy_req(side=OrderSide.BUY_NO, token="tok_no",
                                   price=0.40, shares=2.0), books["tok_no"])
    assert rec.state == OrderState.MATCHED
    pos = position(token_id="tok_no", outcome=Outcome.NO, shares=2.0, entry=0.40)
    books["tok_no"] = book("tok_no", bid=0.45, ask=0.47)
    sell_rec = await sell.execute_exit(pos, ExitDecision.full(ExitReason.TAKE_PROFIT),
                                       books["tok_no"])
    assert sell_rec.side == OrderSide.SELL_NO
    assert sell_rec.filled_shares == pytest.approx(2.0)


async def test_flip_yes_to_no_sells_yes_first():
    books = {"tok_yes": book(bid=0.58, ask=0.60)}
    clock, client, om, sell = _stack(books)
    await om.submit(_buy_req(shares=2.0), books["tok_yes"])
    pos = position(shares=2.0, entry=0.60)
    result = await flip_position(pos, OrderSide.BUY_NO, books["tok_yes"], sell)
    assert result["sold"] is True
    assert result["ready_for_opposite"] is True
    assert result["order"].side == OrderSide.SELL_YES


async def test_flip_blocked_when_sell_not_filled():
    # empty bid side -> sell cannot fill -> opposite entry NOT ready
    from poly_alpha_sniper.core.contracts import BookLevel, OrderbookSnapshot
    no_bids = OrderbookSnapshot(token_id="tok_yes", ts_ms=NOW_MS,
                                bids=[BookLevel(0.30, 100)],  # far below our limit
                                asks=[BookLevel(0.62, 100)])
    books = {"tok_yes": book(bid=0.58, ask=0.60)}
    clock, client, om, sell = _stack(books)
    await om.submit(_buy_req(shares=2.0), books["tok_yes"])
    pos = position(shares=2.0, entry=0.60)
    books["tok_yes"] = no_bids
    # emergency sell hits best bid 0.30... that fills. Use a book with NO bids:
    truly_empty = OrderbookSnapshot(token_id="tok_yes", ts_ms=NOW_MS,
                                    bids=[], asks=[BookLevel(0.62, 100)])
    books["tok_yes"] = truly_empty
    with pytest.raises(ValueError):
        await flip_position(pos, OrderSide.BUY_NO, truly_empty, sell)


async def test_flip_to_same_side_noop():
    books = {"tok_yes": book()}
    clock, client, om, sell = _stack(books)
    pos = position(shares=2.0)
    result = await flip_position(pos, OrderSide.BUY_YES, books["tok_yes"], sell)
    assert result["ready_for_opposite"] is False
    assert "nothing to flip" in result["note"]
