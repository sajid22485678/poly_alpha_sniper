from dataclasses import replace

import pytest

from poly_alpha_sniper.lite.lite_book import normalize_book
from poly_alpha_sniper.lite.lite_broker import LiteBroker
from poly_alpha_sniper.lite.lite_cex import LiteCexFeed
from poly_alpha_sniper.lite.lite_config import LiteConfig
from poly_alpha_sniper.lite.lite_market import LiteMarket, LiteMarketFinder, parse_market_row
from poly_alpha_sniper.lite.lite_strategy import LiteStrategy


NOW_MS = 400_000


def _market(anchor=False, start_s=300):
    return LiteMarket(
        asset="BTC", slug=f"btc-updown-5m-{start_s}", market_id="m1", event_id="e1",
        condition_id="c1", yes_token_id="yes", no_token_id="no",
        window_start_s=start_s, window_close_s=start_s + 300,
        anchor_available=anchor, price_to_beat=100.0 if anchor else None)


def _book(token, bid=0.39, ask=0.40, *, source_ts=NOW_MS-50,
          bid_size=10.0, ask_size=10.0, asks=None, market="c1", hash_="h"):
    raw = {
        "asset_id": token, "market": market, "timestamp": str(source_ts),
        "hash": hash_, "min_order_size": "5",
        "bids": [{"price": str(bid), "size": str(bid_size)}],
        "asks": ([{"price": str(ask), "size": str(ask_size)}]
                 if asks is None else [
                     {"price": str(price), "size": str(size)} for price, size in asks]),
    }
    return normalize_book(raw, token, NOW_MS)


def _features(value=0.001, *, tick=0.0001, volatility=0.0001):
    return {
        "returns": {10: value, 30: value, 60: value},
        "return_10s": value, "return_30s": value, "return_60s": value,
        "tick_return": tick, "volatility": volatility,
    }


def _evaluate(*, value=0.001, features=None, market=None, yes=None, no=None,
              positions=None, cfg=None, lock=None, cex_age=100):
    market = market or _market()
    yes = _book("yes") if yes is None else yes
    no = _book("no") if no is None else no
    return LiteStrategy(cfg or LiteConfig()).evaluate(
        market, yes, no, 100.0, cex_age, "test", features or _features(value),
        NOW_MS, positions or [], window_lock=lock)


def test_strong_up_selects_exactly_buy_yes_and_direct_ask_sweep():
    yes = _book("yes", asks=[(0.40, 2), (0.41, 3)])
    decision = _evaluate(yes=yes)
    assert decision.accepted is True
    assert decision.direction_output == decision.side == "BUY_YES"
    assert decision.token_id == "yes"
    assert decision.shares == 5.0
    assert decision.entry_price == pytest.approx((0.40*2 + 0.41*3)/5)
    assert decision.entry_price != pytest.approx((0.39+0.40)/2)
    assert decision.book_evidence["fill_shares"] == 5.0


def test_strong_down_selects_exactly_buy_no():
    decision = _evaluate(value=-0.001)
    assert decision.accepted is True
    assert decision.direction_output == decision.side == "BUY_NO"
    assert decision.token_id == "no"


def test_flat_and_invalid_data_never_create_both_sides():
    strategy = LiteStrategy(LiteConfig())
    flat = strategy.choose_direction(_features(0.00001), cex_price=100.0)
    invalid = strategy.choose_direction({"returns": {}}, cex_price=100.0)
    assert (flat.output, flat.side) == ("NO_TRADE_TRULY_FLAT", None)
    assert (invalid.output, invalid.side) == ("NO_TRADE_DATA_INVALID", None)
    invalid_cfg = LiteStrategy(LiteConfig(momentum_min_pct=float("nan"))).choose_direction(
        _features(), cex_price=100.0)
    assert (invalid_cfg.output, invalid_cfg.side) == ("NO_TRADE_DATA_INVALID", None)


def test_near_equal_scores_request_only_brief_confirmation():
    features = _features(0.00004, volatility=0.001)
    decision = LiteStrategy(LiteConfig()).choose_direction(features, cex_price=100.0)
    assert decision.output == "BRIEF_CONFIRMATION_WAIT"
    assert decision.side is None


def test_missing_anchor_does_not_block_direction_or_entry():
    assert _evaluate(market=_market(anchor=False)).accepted
    assert _evaluate(market=_market(anchor=True)).accepted


@pytest.mark.parametrize(
    ("yes", "reason"),
    [
        (None, "no_book"),
        (_book("yes", source_ts=NOW_MS-8_001), "stale_book"),
        (_book("yes", bid=0.10, ask=0.40), "spread_too_wide"),
        (_book("yes", ask_size=4.99), "insufficient_five_share_depth"),
        (_book("yes", market="wrong"), "invalid_market_book"),
    ],
)
def test_hard_book_gates_fail_closed(yes, reason):
    decision = LiteStrategy(LiteConfig()).evaluate(
        _market(), yes, _book("no"), 100.0, 100, "test", _features(), NOW_MS, [])
    assert decision.accepted is False
    assert decision.reject_reason == reason


def test_cex_staleness_is_a_hard_gate():
    assert _evaluate(cex_age=8_001).reject_reason == "cex_stale"


def test_stale_or_future_cex_never_selects_a_direction_for_locking():
    strategy = LiteStrategy(LiteConfig())
    for age in (-1, 8_001):
        decision = strategy.choose_direction(
            _features(), cex_price=100.0, cex_age_ms=age)
        assert decision.output == "NO_TRADE_DATA_INVALID"
        assert decision.side is None


def test_same_window_blocks_opposite_and_same_side():
    yes_prior = [{"asset": "BTC", "window_close_ts": 600_000,
                  "side": "BUY_YES", "status": "CLOSED_WIN"}]
    no_prior = [{"asset": "BTC", "window_close_ts": 600_000,
                 "side": "BUY_NO", "status": "CLOSED_LOSS"}]
    assert _evaluate(positions=yes_prior).reject_reason == "duplicate_same_side_blocked"
    assert _evaluate(positions=no_prior).reject_reason == "opposite_side_blocked"


def test_next_window_allows_a_new_direction():
    prior = [{"asset": "BTC", "window_close_ts": 300_000,
              "side": "BUY_YES", "status": "CLOSED_WIN"}]
    assert _evaluate(market=_market(start_s=300), positions=prior).accepted


def test_wait_for_pullback_has_strict_deadline_and_locked_side():
    strategy = LiteStrategy(LiteConfig())
    market = _market()
    expensive = _book("yes", bid=0.64, ask=0.65)
    direction = strategy.choose_direction(
        _features(0.001), cex_price=100.0, market=market,
        yes_book=expensive, no_book=_book("no"))
    timing = strategy.optimize_entry(direction, expensive, market, NOW_MS)
    assert timing.action == "WAIT_FOR_PULLBACK"
    assert NOW_MS < timing.deadline_ts <= NOW_MS + 6_000
    assert timing.target_price < 0.65 < timing.max_chase_price
    assert direction.side == "BUY_YES"


def test_pullback_target_enters_at_improved_executable_price():
    strategy = LiteStrategy(LiteConfig())
    direction = strategy.choose_direction(_features(), cex_price=100.0)
    lock = {
        "side": "BUY_YES", "initial_ask": 0.65, "target_price": 0.64,
        "max_chase_price": 0.67, "deadline_ts": NOW_MS+5_000,
        "direction_decision_ts": NOW_MS-1_000,
    }
    timing = strategy.optimize_entry(
        direction, _book("yes", bid=0.63, ask=0.64), _market(), NOW_MS, lock=lock)
    assert timing.action == "ENTER_NOW"
    assert timing.entry_price == 0.64
    assert timing.actual_improvement == pytest.approx(0.01)


def test_missed_pullback_enters_at_deadline_if_chase_still_acceptable():
    strategy = LiteStrategy(LiteConfig())
    direction = strategy.choose_direction(_features(), cex_price=100.0)
    lock = {
        "side": "BUY_YES", "initial_ask": 0.65, "target_price": 0.64,
        "max_chase_price": 0.67, "deadline_ts": NOW_MS,
        "direction_decision_ts": NOW_MS-6_000,
    }
    timing = strategy.optimize_entry(
        direction, _book("yes", bid=0.64, ask=0.66), _market(), NOW_MS, lock=lock)
    assert timing.action == "ENTER_NOW"
    assert timing.missed_opportunity is True
    assert timing.wait_duration_ms == 6_000


def test_sniper_cannot_wait_past_deadline_or_reverse():
    strategy = LiteStrategy(LiteConfig())
    up = strategy.choose_direction(_features(), cex_price=100.0)
    lock = {"side": "BUY_NO", "deadline_ts": NOW_MS-1,
            "target_price": 0.4, "max_chase_price": 0.42}
    timing = strategy.optimize_entry(up, _book("yes"), _market(), NOW_MS, lock=lock)
    assert timing.action == "SKIP"
    assert timing.reason == "thesis_invalidated"


def test_broker_reasserts_five_shares_and_requires_verified_book():
    class Store:
        row = None
        def insert_trade(self, row):
            self.row = row
            return 7
    store = Store()
    decision = _evaluate(market=_market(anchor=True))
    result = LiteBroker(store).open_trade(
        _market(anchor=True), replace(decision, shares=999, entry_cost=999), NOW_MS)
    assert store.row["shares"] == 5.0
    assert store.row["entry_cost"] == pytest.approx(5*decision.entry_price)
    assert store.row["entry_fee"] > 0
    assert store.row["entry_fill_levels"]
    assert store.row["entry_worst_price"] == decision.entry_price
    assert store.row["execution_verified"] is True
    assert result["id"] == 7


def test_broker_records_wait_entry_mode_from_persisted_wait_duration():
    class Store:
        row = None
        def insert_trade(self, row):
            self.row = row
            return 8
    store = Store()
    decision = replace(_evaluate(), wait_duration_ms=6_000)
    LiteBroker(store).open_trade(_market(), decision, NOW_MS)
    assert store.row["entry_mode"] == "WAIT_FOR_PULLBACK"


def test_broker_rejects_unbound_caller_fabricated_fill_evidence():
    class Store:
        def insert_trade(self, row):
            raise AssertionError("unverified evidence must never reach storage")
    decision = _evaluate()
    fabricated = replace(
        decision, entry_price=0.9,
        book_evidence={
            "book_ts": NOW_MS-1, "received_ts": NOW_MS,
            "age_ms": 1, "book_hash": "fabricated", "fill_shares": 5,
            "ask_depth_shares": 5,
        })
    with pytest.raises(ValueError, match="bound five-share"):
        LiteBroker(Store()).open_trade(_market(), fabricated, NOW_MS)


def test_cex_horizons_require_near_target_samples_and_never_cross_sources():
    feed = LiteCexFeed(["BTC"])
    for ts, price in ((0, 100.0), (30_000, 101.0), (50_000, 102.0), (60_000, 103.0)):
        feed.record("BTC", price, ts, "bybit")
    mapping = feed.momentum_map("BTC", [10, 30, 60], 60_000)
    assert mapping[10] == pytest.approx((103-102)/102)
    assert mapping[30] == pytest.approx((103-101)/101)
    assert mapping[60] == pytest.approx(0.03)
    feed.record("BTC", 200.0, 61_000, "okx")
    assert feed.momentum_values("BTC", [10, 30, 60], 61_000) == []


def test_market_parser_keeps_reversed_direct_token_mapping_and_optional_anchor():
    raw = {
        "id": "m", "eventId": "e", "conditionId": "c",
        "slug": "btc-updown-5m-300", "clobTokenIds": '["down","up"]',
        "outcomes": '["Down","Up"]', "active": True, "closed": False,
        "acceptingOrders": True, "archived": False,
    }
    market, reason = parse_market_row("BTC", raw, "btc-updown-5m-300")
    assert reason == ""
    assert (market.yes_token_id, market.no_token_id) == ("up", "down")
    assert market.anchor_available is False


def test_market_parser_fails_closed_when_state_evidence_is_missing():
    raw = {
        "id": "m", "eventId": "e", "conditionId": "c",
        "slug": "btc-updown-5m-300", "clobTokenIds": '["up","down"]',
        "outcomes": '["Up","Down"]', "active": True, "closed": False,
        "acceptingOrders": True,
    }
    market, reason = parse_market_row("BTC", raw, "btc-updown-5m-300")
    assert market is None
    assert reason == "market_state_invalid"


@pytest.mark.asyncio
async def test_market_finder_rejects_duplicate_exact_slug_rows():
    raw = {
        "id": "m", "eventId": "e", "conditionId": "c",
        "slug": "btc-updown-5m-300", "clobTokenIds": '["up","down"]',
        "outcomes": '["Up","Down"]', "active": True, "closed": False,
        "acceptingOrders": True, "archived": False,
    }

    async def fetch(_params):
        return [raw, {**raw, "id": "m2"}]

    market, reason = await LiteMarketFinder(fetch).current_market("BTC", NOW_MS)
    assert market is None
    assert reason == "ambiguous_market"
