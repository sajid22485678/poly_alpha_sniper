import asyncio

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import OrderRequest, OrderSide, OrderState
from poly_alpha_sniper.execution.order_manager import OrderManager
from poly_alpha_sniper.execution.simulator import SimulatedClobClient
from poly_alpha_sniper.tests.helpers import NOW_MS, book


def _req(order_id="o1", price=0.55, tif_ms=800):
    return OrderRequest(order_id=order_id, token_id="tok_yes", market_id="m1",
                        side=OrderSide.BUY_YES, price=price, size_shares=1.0,
                        size_usd=round(price, 4), tif_ms=tif_ms)


async def test_non_marketable_order_tif_cancel_task_is_referenced():
    """Regression test for the asyncio fire-and-forget bug: a TIF-cancel task
    must be strongly referenced by OrderManager (not just handed to
    create_task and dropped) or it can be garbage-collected mid-sleep,
    leaving the order stuck OPEN forever."""
    clock = SimClock(NOW_MS)
    client = SimulatedClobClient(clock, lambda t: {"tok_yes": book(bid=0.58, ask=0.60)}.get(t))
    client.set_balance(10.0)
    om = OrderManager(cfg=None, clock=clock, client=client)

    rec = await om.submit(_req(price=0.55))  # non-marketable -> rests OPEN

    assert rec.state == OrderState.OPEN
    assert len(om._pending_cancel_tasks) == 1, "TIF-cancel task must be retained"


async def test_tif_cancel_actually_fires_and_persists_via_on_update():
    clock = SimClock(NOW_MS)
    client = SimulatedClobClient(clock, lambda t: {"tok_yes": book(bid=0.58, ask=0.60)}.get(t))
    client.set_balance(10.0)
    updates = []

    async def on_update(record):
        updates.append((record.order_id, record.state))

    om = OrderManager(cfg=None, clock=clock, client=client, on_update=on_update)
    rec = await om.submit(_req(price=0.55, tif_ms=800))
    assert rec.state == OrderState.OPEN

    # SimClock.sleep() advances time instantly but the scheduled task still
    # needs event-loop turns to actually run to completion.
    for _ in range(5):
        await asyncio.sleep(0)

    assert om.orders["o1"].state == OrderState.CANCELLED
    assert om._pending_cancel_tasks == set(), "task must be discarded once done"
    assert ("o1", OrderState.CANCELLED) in updates, "cancellation must reach on_update"


async def test_marketable_order_fills_and_schedules_no_cancel():
    clock = SimClock(NOW_MS)
    client = SimulatedClobClient(clock, lambda t: {"tok_yes": book(bid=0.58, ask=0.60)}.get(t))
    client.set_balance(10.0)
    om = OrderManager(cfg=None, clock=clock, client=client)

    rec = await om.submit(_req(price=0.60))  # marketable -> fills immediately

    assert rec.state == OrderState.MATCHED
    assert om._pending_cancel_tasks == set()
