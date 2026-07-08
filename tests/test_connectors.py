"""Connector imports, parsing, live-gate blocking, mirror staleness."""
import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets
from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg


def test_all_connector_modules_import():
    import poly_alpha_sniper.connectors.binance_ws  # noqa: F401
    import poly_alpha_sniper.connectors.bybit_ws  # noqa: F401
    import poly_alpha_sniper.connectors.okx_ws  # noqa: F401
    import poly_alpha_sniper.connectors.multi_cex_feed  # noqa: F401
    import poly_alpha_sniper.connectors.polymarket_gamma  # noqa: F401
    import poly_alpha_sniper.connectors.polymarket_clob_public  # noqa: F401
    import poly_alpha_sniper.connectors.polymarket_clob_private  # noqa: F401
    import poly_alpha_sniper.connectors.polymarket_ws  # noqa: F401
    import poly_alpha_sniper.data.orderbook_mirror  # noqa: F401


async def test_multi_cex_feed_aggregates_ticks():
    from poly_alpha_sniper.connectors.multi_cex_feed import MultiCexFeed
    from poly_alpha_sniper.data.cex_state import CexState
    c = cfg()
    clock = SimClock(NOW_MS)
    state = CexState(c, clock)
    feed = MultiCexFeed(c, clock, state)
    await feed._on_tick(CexTick("BTC", "binance", 100_000.0, NOW_MS))
    await feed._on_tick(CexTick("BTC", "bybit", 100_010.0, NOW_MS))
    view = state.multi_view("BTC")
    assert view is not None
    assert set(view.per_exchange) == {"binance", "bybit"}
    health = feed.health()
    assert "binance" in health and "bybit" in health


def test_clob_public_book_parsing():
    from poly_alpha_sniper.connectors.polymarket_clob_public import parse_book
    data = {"bids": [{"price": "0.55", "size": "100"}, {"price": "0.54", "size": "50"}],
            "asks": [{"price": "0.58", "size": "80"}, {"price": "0.60", "size": "20"}]}
    snap = parse_book(data, "tok", NOW_MS)
    assert snap.best_bid == 0.55
    assert snap.best_ask == 0.58
    assert snap.spread == pytest.approx(0.03)
    assert snap.bids[0].size == 100


def test_private_client_blocked_without_live_gates():
    from poly_alpha_sniper.connectors.polymarket_clob_private import create_live_client
    secrets = Secrets(env={"POLYMARKET_PRIVATE_KEY": "0x" + "a" * 64})
    with pytest.raises(RuntimeError, match="live gates"):
        create_live_client(cfg(), secrets, live_gates_ok=False)


def test_private_client_requires_key():
    from poly_alpha_sniper.connectors.polymarket_clob_private import create_live_client
    with pytest.raises(RuntimeError, match="PRIVATE_KEY"):
        create_live_client(cfg(), Secrets(env={}), live_gates_ok=True)


async def test_polymarket_ws_book_and_delta_handling():
    from poly_alpha_sniper.connectors.polymarket_ws import PolymarketWS
    received = []

    async def on_book(snap):
        received.append(snap)

    ws = PolymarketWS(cfg(), SimClock(NOW_MS), on_book)
    import json
    await ws._handle(json.dumps({
        "event_type": "book", "asset_id": "tokA",
        "bids": [{"price": "0.50", "size": "10"}],
        "asks": [{"price": "0.55", "size": "10"}]}))
    assert received[-1].best_bid == 0.50
    await ws._handle(json.dumps({
        "event_type": "price_change", "asset_id": "tokA",
        "changes": [{"price": "0.52", "size": "5", "side": "BUY"},
                    {"price": "0.50", "size": "0", "side": "BUY"}]}))
    assert received[-1].best_bid == 0.52  # new level added, old removed


async def test_orderbook_mirror_staleness_and_tracking(tmp_path):
    from poly_alpha_sniper.data.orderbook_mirror import OrderbookMirror
    from poly_alpha_sniper.data.orderbook_state import OrderbookStore
    from poly_alpha_sniper.tests.helpers import book, market
    c = cfg()
    clock = SimClock(NOW_MS)
    store = OrderbookStore(c, clock)

    class FakeRest:
        async def get_book(self, token_id):
            return book(token_id)

    mirror = OrderbookMirror(c, clock, ws=None, rest=FakeRest(), store=store)
    await mirror.track_market(market())
    assert mirror.get("tok_yes") is not None
    assert mirror.is_fresh("tok_yes")
    clock.advance_ms(c.polymarket.max_orderbook_staleness_ms + 500)
    assert not mirror.is_fresh("tok_yes")
    mirror.untrack_market(market())
    assert mirror.get("tok_yes") is None
