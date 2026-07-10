from dataclasses import replace

import pytest

from poly_alpha_sniper.lite.lite_book import LiteBookQuote
from poly_alpha_sniper.lite.lite_broker import LiteBroker
from poly_alpha_sniper.lite.lite_cex import LiteCexFeed
from poly_alpha_sniper.lite.lite_config import LiteConfig
from poly_alpha_sniper.lite.lite_market import LiteMarket, parse_market_row
from poly_alpha_sniper.lite.lite_strategy import LiteDecision, LiteStrategy

NOW_MS = 100_000


def _market(anchor=False):
    return LiteMarket(
        asset="BTC", slug="btc-updown-5m-0", market_id="m1", event_id="e1",
        condition_id="c1", yes_token_id="yes", no_token_id="no",
        window_start_s=0, window_close_s=300,
        anchor_available=anchor, price_to_beat=100.0 if anchor else None,
    )


def _book(token, bid, ask, *, ts=NOW_MS, bid_depth=20.0, ask_depth=20.0):
    return LiteBookQuote(token, bid, ask, bid_depth, ask_depth, ts)


def _evaluate(*, momentum=0.001, market=None, yes=None, no=None,
              cex_price=100.0, cex_age=100, positions=None, cfg=None):
    market = market or _market()
    yes = yes if yes is not None else _book("yes", 0.39, 0.40)
    no = no if no is not None else _book("no", 0.59, 0.60)
    return LiteStrategy(cfg or LiteConfig()).evaluate(
        market, yes, no, cex_price, cex_age, "test", [momentum], NOW_MS,
        positions or [],
    )


def test_momentum_up_opens_buy_yes_at_direct_yes_ask_for_five_shares():
    decision = _evaluate(momentum=0.001)
    assert decision.accepted is True
    assert decision.side == "BUY_YES"
    assert decision.token_id == "yes"
    assert decision.shares == 5.0
    assert decision.entry_price == 0.40
    assert decision.entry_cost == 5 * 0.40


def test_momentum_down_opens_buy_no_at_direct_no_ask_for_five_shares():
    decision = _evaluate(momentum=-0.001)
    assert decision.accepted is True
    assert decision.side == "BUY_NO"
    assert decision.token_id == "no"
    assert decision.entry_price == 0.60
    assert decision.entry_cost == 5 * 0.60


def test_flat_or_missing_momentum_skips():
    assert _evaluate(momentum=0.00001).reject_reason == "no_momentum"
    decision = LiteStrategy(LiteConfig()).evaluate(
        _market(), _book("yes", 0.4, 0.5), _book("no", 0.4, 0.5),
        100.0, 10, "test", [], NOW_MS, [],
    )
    assert decision.reject_reason == "no_momentum"


def test_missing_anchor_does_not_block_and_available_anchor_is_optional():
    no_anchor = _evaluate(market=_market(anchor=False))
    with_anchor = _evaluate(market=_market(anchor=True))
    assert no_anchor.accepted and with_anchor.accepted


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"market": replace(_market(), yes_token_id="")}, "invalid_token"),
        ({"yes": False}, "no_book"),
        ({"yes": _book("yes", 0.39, 0.40, ts=NOW_MS - 8_001)}, "stale_book"),
        ({"yes": _book("yes", 0.20, 0.50)}, "spread_too_wide"),
        ({"yes": _book("yes", 0.39, 0.40, ask_depth=0.5)}, "depth_too_low"),
        ({"cex_age": 8_001}, "cex_stale"),
    ],
)
def test_hard_data_gates_reject(kwargs, reason):
    if kwargs.get("yes") is False:
        market = kwargs.pop("market", _market())
        decision = LiteStrategy(LiteConfig()).evaluate(
            market, None, _book("no", 0.59, 0.60), 100.0, 100,
            "test", [0.001], NOW_MS, [],
        )
    else:
        decision = _evaluate(**kwargs)
    assert decision.accepted is False
    assert decision.reject_reason == reason


def test_duplicate_same_asset_window_side_rejects_even_after_close():
    prior = {
        "asset": "BTC", "market_id": "m1", "slug": "btc-updown-5m-0",
        "side": "BUY_YES", "window_close_ts": 300_000, "status": "CLOSED_WIN",
    }
    assert _evaluate(positions=[prior]).reject_reason == "duplicate_position"


def test_global_and_per_asset_position_limits_reject():
    global_cfg = LiteConfig(max_open_positions=1, max_open_per_asset=5)
    prior = [{"asset": "ETH", "market_id": "other", "slug": "other",
              "side": "BUY_NO", "status": "OPEN"}]
    assert _evaluate(cfg=global_cfg, positions=prior).reject_reason == "max_open_positions"

    asset_cfg = LiteConfig(max_open_positions=6, max_open_per_asset=1)
    prior = [{"asset": "BTC", "market_id": "other", "slug": "other",
              "side": "BUY_NO", "status": "OPEN"}]
    assert _evaluate(cfg=asset_cfg, positions=prior).reject_reason == "max_open_positions"


def test_broker_reasserts_exact_five_share_size_and_records_anchor():
    class Store:
        row = None
        def insert_trade(self, row):
            self.row = row
            return 7

    store = Store()
    decision = LiteDecision(
        accepted=True, reject_reason="opened", side="BUY_YES", token_id="yes",
        shares=999, entry_price=0.4, entry_cost=999, momentum_pct=0.001,
    )
    result = LiteBroker(store).open_trade(
        _market(anchor=True), decision, NOW_MS,
        cex_source="test", cex_entry_price=100.0,
    )
    assert store.row["shares"] == 5.0
    assert store.row["entry_cost"] == 2.0
    assert store.row["anchor_available"] is True
    assert store.row["no_anchor_trade"] is False
    assert result["id"] == 7


def test_market_parser_maps_reversed_outcome_tokens_and_allows_no_anchor():
    raw = {
        "id": "m", "eventId": "e", "conditionId": "c",
        "slug": "btc-updown-5m-300",
        "clobTokenIds": '["down-token","up-token"]',
        "outcomes": '["Down","Up"]',
        "active": True, "closed": False, "acceptingOrders": True,
    }
    market, reason = parse_market_row(
        "BTC", raw, expected_slug="btc-updown-5m-300",
    )
    assert reason == ""
    assert market.yes_token_id == "up-token"
    assert market.no_token_id == "down-token"
    assert market.anchor_available is False
    assert market.price_to_beat is None


def test_market_parser_rejects_wrong_window_and_closed_market():
    raw = {
        "id": "m", "eventId": "e", "conditionId": "c",
        "slug": "btc-updown-5m-300", "clobTokenIds": '["up","down"]',
        "outcomes": '["Up","Down"]', "active": True,
        "closed": False, "acceptingOrders": True,
    }
    assert parse_market_row("BTC", raw, "btc-updown-5m-600") == (None, "no_market")
    assert parse_market_row("BTC", {**raw, "closed": True}) == (None, "expired_market")


def test_cex_memory_computes_evidenced_short_momentum():
    feed = LiteCexFeed(["BTC"])
    feed.record("BTC", 100.0, 0, "test")
    feed.record("BTC", 101.0, 60_000, "test")
    values = feed.momentum_values("BTC", [10, 30, 60], 60_000)
    assert values == pytest.approx([0.01, 0.01, 0.01])
