import pytest

from poly_alpha_sniper.lite.lite_book import LiteBookQuote
from poly_alpha_sniper.lite.lite_config import LiteConfig
from poly_alpha_sniper.lite.lite_resolver import (
    LiteResolver,
    direct_book_exit_price,
    official_outcome_from_market_row,
    official_resolution_pnl,
)
from poly_alpha_sniper.lite.lite_store import LiteStore


def _row(outcome="YES", **overrides):
    row = {
        "id": "m1",
        "eventId": "e1",
        "slug": "btc-updown-5m-0",
        "conditionId": "c1",
        "closed": True,
        "outcomes": '["Up","Down"]',
        "outcomePrices": '["1","0"]' if outcome == "YES" else '["0","1"]',
    }
    row.update(overrides)
    return row


def _identity(row):
    return official_outcome_from_market_row(
        row, market_id="m1", event_id="e1", slug="btc-updown-5m-0",
        condition_id="c1",
    )


def _entry(**overrides):
    row = {
        "asset": "BTC", "market_id": "m1", "event_id": "e1",
        "slug": "btc-updown-5m-0", "condition_id": "c1",
        "yes_token_id": "yes", "no_token_id": "no", "side": "BUY_YES",
        "shares": 5.0, "entry_price": 0.4, "entry_cost": 2.0,
        "entry_ts": 100_000, "window_close_ts": 300_000, "status": "OPEN",
        "anchor_available": False, "price_to_beat": None,
        "cex_source": "test", "cex_entry_price": 100.0,
        "momentum_pct": 0.001, "strategy_name": "lite_momentum_v1",
    }
    row.update(overrides)
    return row


def test_official_yes_outcome_resolves_buy_yes_win():
    outcome, reason = _identity(_row("YES"))
    pnl, exit_price, won = official_resolution_pnl("BUY_YES", 0.4, 5, outcome)
    assert (outcome, reason) == ("YES", "resolved")
    assert (pnl, exit_price, won) == (3.0, 1.0, True)


def test_official_no_outcome_resolves_buy_yes_loss():
    outcome, _ = _identity(_row("NO"))
    pnl, exit_price, won = official_resolution_pnl("BUY_YES", 0.4, 5, outcome)
    assert (pnl, exit_price, won) == (-2.0, 0.0, False)


def test_official_no_outcome_resolves_buy_no_win():
    outcome, _ = _identity(_row("NO"))
    pnl, exit_price, won = official_resolution_pnl("BUY_NO", 0.6, 5, outcome)
    assert (pnl, exit_price, won) == (2.0, 1.0, True)


@pytest.mark.parametrize(
    "row,reason",
    [
        (_row("YES", id="wrong"), "market_id_mismatch"),
        (_row("YES", eventId="wrong"), "event_id_mismatch"),
        (_row("YES", slug="wrong"), "slug_mismatch"),
        (_row("YES", conditionId="wrong"), "condition_id_mismatch"),
        (_row("YES", closed=False), "market_not_closed_yet"),
        (_row("YES", outcomePrices='["0.7","0.3"]'), "outcome_prices_not_degenerate"),
    ],
)
def test_official_resolution_fails_closed_without_exact_evidence(row, reason):
    assert _identity(row) == (None, reason)


def test_resolution_source_url_is_never_outcome_evidence():
    row = _row("YES", closed=False, resolutionSource="https://example.invalid/yes")
    assert _identity(row) == (None, "market_not_closed_yet")


def test_direct_owned_token_book_exit_is_executable():
    trade = _entry()
    quote = LiteBookQuote("yes", 0.55, 0.56, 10.0, 10.0, 310_000)
    price, reason = direct_book_exit_price(trade, quote, 310_000, 8_000, 0.2, 1.0)
    assert (price, reason) == (0.55, "direct_token_bid")


@pytest.mark.asyncio
async def test_book_exit_has_priority_over_official_outcome(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    trade_id = store.insert_trade(_entry())
    store.mark_pending(trade_id, 300_000, "no_preclose_exit")

    class Book:
        async def get_book(self, token_id):
            return LiteBookQuote(token_id, 0.50, 0.51, 10.0, 10.0, 331_000)

    async def fetch(_params):
        return [_row("YES")]

    cfg = LiteConfig(resolver_retry_seconds=30)
    try:
        result = await LiteResolver(store, fetch, Book(), cfg).resolve_due(331_000)
        trade = store.get_trade(trade_id)
        assert result[0]["action"] == "book_exit"
        assert trade["resolution_source"] == "book_exit"
        assert trade["pnl"] == pytest.approx(0.5)
    finally:
        store.close()


@pytest.mark.asyncio
async def test_unresolved_final_stays_null_after_bounded_retry(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    trade_id = store.insert_trade(_entry())
    store.mark_pending(trade_id, 300_000, "no_preclose_exit")

    class NoBook:
        async def get_book(self, _token_id):
            return None

    async def fetch(_params):
        return [_row("YES", closed=False)]

    cfg = LiteConfig(resolver_retry_seconds=30, resolver_max_retries=1)
    try:
        result = await LiteResolver(store, fetch, NoBook(), cfg).resolve_due(331_000)
        trade = store.get_trade(trade_id)
        assert result[0]["action"] == "unresolved_final"
        assert trade["status"] == "UNRESOLVED_FINAL"
        assert trade["pnl"] is None
        assert store.dashboard_metrics(331_000)["completed_trades"] == 0
    finally:
        store.close()
