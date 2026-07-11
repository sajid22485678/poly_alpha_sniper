import pytest

from poly_alpha_sniper.lite.lite_book import normalize_book
from poly_alpha_sniper.lite.lite_config import LiteConfig
from poly_alpha_sniper.lite.lite_resolver import (
    LiteResolver,
    direct_book_exit,
    event_market_row,
    market_fee_rate,
    official_outcome_from_market_row,
    official_resolution_pnl,
)
from poly_alpha_sniper.lite.lite_store import LiteStore


OPEN_MS = 300_000
CLOSE_MS = 600_000


def _row(outcome="YES", **overrides):
    row = {
        "id": "m1", "slug": "btc-updown-5m-300", "conditionId": "c1",
        "closed": True, "outcomes": '["Up","Down"]',
        "outcomePrices": '["1","0"]' if outcome == "YES" else '["0","1"]',
        "clobTokenIds": '["yes","no"]', "feesEnabled": True,
        "feeSchedule": {"rate": 0.07, "exponent": 1, "takerOnly": True},
    }
    row.update(overrides)
    return row


def _event(outcome="YES", **overrides):
    event = {"id": "e1", "slug": "btc-updown-5m-300",
             "markets": [_row(outcome)]}
    event.update(overrides)
    return event


def _entry(**overrides):
    row = {
        "asset": "BTC", "market_id": "m1", "event_id": "e1",
        "slug": "btc-updown-5m-300", "condition_id": "c1",
        "yes_token_id": "yes", "no_token_id": "no", "side": "BUY_YES",
        "shares": 5.0, "entry_price": 0.4, "entry_cost": 2.0,
        "entry_ts": 400_000, "window_open_ts": OPEN_MS,
        "window_close_ts": CLOSE_MS, "status": "OPEN",
        "anchor_available": False, "price_to_beat": None,
        "cex_source": "test", "cex_entry_price": 100.0,
        "momentum_pct": 0.001, "strategy_name": "lite_direction_sniper_v2",
    }
    row.update(overrides)
    return row


def _identity(row):
    return official_outcome_from_market_row(
        row, market_id="m1", slug="btc-updown-5m-300", condition_id="c1",
        window_open_ts=OPEN_MS, window_close_ts=CLOSE_MS,
        yes_token_id="yes", no_token_id="no")


def _book(token="yes", *, now=590_000, source=589_900, bid_size=5,
          ask_size=5, market="c1"):
    return normalize_book({
        "asset_id": token, "market": market, "timestamp": str(source),
        "hash": "book-hash", "min_order_size": "5",
        "bids": [{"price": "0.55", "size": str(bid_size)}],
        "asks": [{"price": "0.56", "size": str(ask_size)}],
    }, token, now)


def test_official_yes_and_no_resolve_correct_sides_with_fee():
    yes, _ = _identity(_row("YES"))
    no, _ = _identity(_row("NO"))
    assert official_resolution_pnl("BUY_YES", 0.4, 5, yes, 0.05) == (2.95, 1.0, True)
    assert official_resolution_pnl("BUY_YES", 0.4, 5, no, 0.05) == (-2.05, 0.0, False)
    assert official_resolution_pnl("BUY_NO", 0.6, 5, no, 0.05) == (1.95, 1.0, True)


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (_row(id="wrong"), "market_id_mismatch"),
        (_row(slug="btc-updown-5m-600"), "slug_mismatch"),
        (_row(conditionId="wrong"), "condition_id_mismatch"),
        (_row(clobTokenIds='["wrong","no"]'), "yes_token_mismatch"),
        (_row(closed=False), "market_not_closed_yet"),
        (_row(outcomePrices='["0.7","0.3"]'), "outcome_prices_not_degenerate"),
        (_row(outcomes=None), "token_mapping_unparseable"),
    ],
)
def test_official_outcome_requires_exact_identity_mapping_and_degenerate_evidence(row, reason):
    assert _identity(row) == (None, reason)


def test_wrong_event_or_event_market_is_rejected():
    assert event_market_row(
        _event(id="wrong"), event_id="e1", market_id="m1",
        slug="btc-updown-5m-300", condition_id="c1",
        window_open_ts=OPEN_MS, window_close_ts=CLOSE_MS,
        yes_token_id="yes", no_token_id="no")[1] == "event_id_mismatch"
    assert event_market_row(
        _event(markets=[_row(id="wrong")]), event_id="e1", market_id="m1",
        slug="btc-updown-5m-300", condition_id="c1",
        window_open_ts=OPEN_MS, window_close_ts=CLOSE_MS,
        yes_token_id="yes", no_token_id="no")[0] is None


def test_resolution_source_url_is_never_outcome_evidence():
    row = _row("YES", closed=False, resolutionSource="https://example.invalid/yes")
    assert _identity(row) == (None, "market_not_closed_yet")


def test_preclose_exit_uses_owned_token_five_share_bid_sweep():
    sweep, reason = direct_book_exit(_entry(), _book(), 590_000, 8_000, 0.2)
    assert reason == "direct_token_bid_sweep"
    assert sweep.shares == 5.0
    assert sweep.vwap == 0.55


@pytest.mark.parametrize(
    ("quote", "now", "reason"),
    [
        (_book(bid_size=4.99), 590_000, "insufficient_five_share_depth"),
        (_book(source=580_000), 590_000, "stale_book"),
        (_book(now=590_000, source=590_100), 590_000, "no_book"),
        (_book(market="wrong"), 590_000, "invalid_market_book"),
        (_book(), 600_000, "post_close_book_forbidden"),
    ],
)
def test_exit_rejects_insufficient_stale_future_wrong_and_postclose_books(quote, now, reason):
    # Future source timestamps fail normalization and appear as no book.
    sweep, actual = direct_book_exit(_entry(), quote, now, 8_000, 0.2)
    assert sweep is None
    assert actual == reason


@pytest.mark.asyncio
async def test_resolver_uses_exact_market_and_event_and_persists_verified_net_pnl(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    trade_id = store.insert_trade(_entry(entry_fee=0.084))
    store.mark_pending(trade_id, CLOSE_MS, "no_preclose_exit")
    calls = []

    async def market(mid):
        calls.append(("market", mid))
        return _row("YES")

    async def event(eid):
        calls.append(("event", eid))
        return _event("YES")

    try:
        result = await LiteResolver(store, market, event, LiteConfig()).resolve_due(610_000)
        trade = store.get_trade(trade_id)
        assert calls == [("market", "m1"), ("event", "e1")]
        assert result[0]["action"] == "official_outcome"
        assert trade["status"] == "CLOSED_WIN"
        assert trade["resolution_verified"] == 1
        assert trade["pnl"] == pytest.approx(3.0-0.084)
    finally:
        store.close()


@pytest.mark.asyncio
async def test_exact_disabled_fee_reconciles_stored_entry_accounting(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    trade_id = store.insert_trade(_entry(entry_fee=0.084, fee_rate=0.07))
    store.mark_pending(trade_id, CLOSE_MS, "no_preclose_exit")

    async def market(_mid): return _row("YES", feesEnabled=False)
    async def event(_eid):
        return _event("YES", markets=[_row("YES", feesEnabled=False)])

    try:
        result = await LiteResolver(store, market, event, LiteConfig()).resolve_due(610_000)
        trade = store.get_trade(trade_id)
        assert result[0]["action"] == "official_outcome"
        assert trade["entry_fee"] == 0.0
        assert trade["fee_rate"] == 0.0
        assert trade["pnl"] == pytest.approx(3.0)
        assert "fees_disabled" in trade["resolution_reason"]
    finally:
        store.close()


@pytest.mark.asyncio
async def test_wrong_event_never_backfills_and_retry_state_persists(tmp_path):
    path = tmp_path / "lite.db"
    store = LiteStore(str(path))
    trade_id = store.insert_trade(_entry())
    store.mark_pending(trade_id, CLOSE_MS, "no_exit")

    async def market(_mid): return _row("YES")
    async def event(_eid): return _event(id="wrong")
    cfg = LiteConfig(resolver_retry_seconds=1, resolver_retry_cap_seconds=8,
                     resolver_max_retries=3)
    try:
        result = await LiteResolver(store, market, event, cfg).resolve_due(610_000)
        trade = store.get_trade(trade_id)
        assert result[0]["action"] == "retry"
        assert trade["status"] == "UNRESOLVED_RETRYING"
        assert trade["retry_count"] == 1
        assert trade["last_attempt_at"] == 610_000
        assert "event_id_mismatch" in trade["last_error"]
        next_attempt = trade["next_attempt_at"]
    finally:
        store.close()
    reopened = LiteStore(str(path))
    try:
        trade = reopened.get_trade(trade_id)
        assert trade["retry_count"] == 1
        assert trade["next_attempt_at"] == next_attempt
        assert reopened.resolution_trades_due(next_attempt-1) == []
        assert reopened.resolution_trades_due(next_attempt)[0]["id"] == trade_id
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_direct_and_event_outcomes_must_corroborate(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    trade_id = store.insert_trade(_entry())
    store.mark_pending(trade_id, CLOSE_MS, "no_exit")

    async def market(_mid): return _row("YES")
    async def event(_eid): return _event("NO")
    try:
        result = await LiteResolver(store, market, event, LiteConfig()).resolve_due(610_000)
        trade = store.get_trade(trade_id)
        assert result[0]["action"] == "retry"
        assert trade["pnl"] is None
        assert "direct_event_outcome_mismatch" in trade["last_error"]
    finally:
        store.close()


def test_historical_backfill_method_keeps_exact_fee_net_and_null_only_until_evidence(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    trade_id = store.insert_trade(_entry())
    store.mark_pending(trade_id, CLOSE_MS, "no_exit")
    store.mark_unresolved(trade_id, 700_000, "no_official_evidence")
    assert store.get_trade(trade_id)["pnl"] is None
    assert store.apply_verified_official_resolution(
        trade_id, outcome="NO", evidence_ts=700_001, fee_rate=0.07)
    trade = store.get_trade(trade_id)
    assert trade["status"] == "CLOSED_LOSS"
    assert trade["gross_pnl"] == -2.0
    assert trade["pnl"] < -2.0
    store.close()


def test_exact_crypto_fee_schedule_is_used():
    assert market_fee_rate(_row())[0] == 0.07
    assert market_fee_rate(_row(feesEnabled=False)) == (0.0, "fees_disabled")
    assert market_fee_rate(_row(feesEnabled=None))[1] == "fee_default_unparseable_enabled"
    assert market_fee_rate(_row(feeSchedule={"rate": 0.07, "exponent": 2,
                                             "takerOnly": True}))[1] == \
        "fee_default_unsupported_schedule"
