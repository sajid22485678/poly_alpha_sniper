from dataclasses import replace
import inspect

import pytest

from poly_alpha_sniper.lite.lite_book import normalize_book
from poly_alpha_sniper.lite.lite_broker import LiteBroker
from poly_alpha_sniper.lite.lite_bot import LiteBot
from poly_alpha_sniper.lite.lite_cex import LiteCexFeed
from poly_alpha_sniper.lite.lite_config import FIXED_SHARES, LiteConfig
from poly_alpha_sniper.lite.lite_market import (
    LiteMarket, LiteMarketFinder, parse_market_row,
)
from poly_alpha_sniper.lite.lite_resolver import direct_book_exit
from poly_alpha_sniper.lite.lite_risk import sweep_taker_fee
from poly_alpha_sniper.lite.lite_strategy import LiteStrategy


NOW_MS = 400_000


def _market(anchor=False, start_s=300):
    return LiteMarket(
        asset="BTC", slug=f"btc-updown-5m-{start_s}", market_id="m1",
        event_id="e1", condition_id="c1", yes_token_id="yes",
        no_token_id="no", window_start_s=start_s,
        window_close_s=start_s + 300, anchor_available=anchor,
        price_to_beat=99.95 if anchor else None,
    )


def _book(token, bid, ask, *, source_ts=NOW_MS-50, receipt_ts=NOW_MS,
          bid_size=10.0, ask_size=10.0, asks=None, market="c1",
          hash_=None, tick_size="0.01"):
    raw = {
        "asset_id": token, "market": market, "timestamp": str(source_ts),
        "hash": hash_ or f"hash-{token}-{source_ts}",
        "min_order_size": "5", "tick_size": tick_size, "neg_risk": False,
        "bids": [{"price": str(bid), "size": str(bid_size)}],
        "asks": ([{"price": str(ask), "size": str(ask_size)}]
                 if asks is None else [
                     {"price": str(price), "size": str(size)}
                     for price, size in asks]),
    }
    return normalize_book(raw, token, receipt_ts)


def _books(*, yes_ask=0.48, no_ask=0.53, source_ts=NOW_MS-50):
    return (
        _book("yes", yes_ask-0.01, yes_ask, source_ts=source_ts),
        _book("no", no_ask-0.01, no_ask, source_ts=source_ts),
    )


def _features(value=0.001, *, age=100, tick=0.0001,
              volatility=0.0001, move_ts=NOW_MS-100):
    return {
        "returns": {5: value, 10: value, 30: value, 60: value},
        "return_5s": value, "return_10s": value,
        "return_30s": value, "return_60s": value,
        "tick_return": tick, "acceleration": tick/2,
        "volatility": volatility, "sample_count": 12,
        "window_return": value*2,
        "provider_ts_ms": NOW_MS-age, "receipt_ts_ms": NOW_MS-age,
        "provider_age_ms": age, "receipt_age_ms": age,
        "latest_move_ts_ms": move_ts,
    }


def _direction(*, value=0.001, features=None, market=None, yes=None, no=None,
               cfg=None, age=100, previous=None):
    market = market or _market()
    default_yes, default_no = _books()
    return LiteStrategy(cfg or LiteConfig()).choose_direction(
        features or _features(value), cex_price=100.0, market=market,
        yes_book=default_yes if yes is None else yes,
        no_book=default_no if no is None else no,
        cex_age_ms=age, previous_observation=previous, now_ms=NOW_MS,
    )


def _cross_decision(*, value=0.001, market=None, yes=None, no=None,
                    cfg=None):
    market = market or _market()
    default_yes, default_no = _books()
    yes = default_yes if yes is None else yes
    no = default_no if no is None else no
    strategy = LiteStrategy(cfg or LiteConfig())
    direction = strategy.choose_direction(
        _features(value), cex_price=100.0, market=market,
        yes_book=yes, no_book=no, cex_age_ms=100, now_ms=NOW_MS)
    assert direction.side in ("BUY_YES", "BUY_NO")
    selected = yes if direction.side == "BUY_YES" else no
    lock = {
        "side": direction.side, "status": "MAKER_WAIT",
        "initial_ask": selected.buy_sweep(FIXED_SHARES).vwap,
        "maker_price": selected.best_bid, "max_chase_price": 0.999,
        "maker_start_ts": NOW_MS-5_000,
        "maker_deadline_ts": NOW_MS-1, "direction_decision_ts": NOW_MS-5_000,
    }
    timing = strategy.optimize_entry(
        direction, selected, market, NOW_MS, lock=lock)
    assert timing.action == "CROSS_SPREAD"
    return strategy.build_entry_decision(
        market, direction, timing, selected, NOW_MS)


def test_paired_book_fair_value_is_finite_bounded_and_coherent():
    decision = _direction()
    assert decision.output == decision.side == "BUY_YES"
    assert 0.0 <= decision.fair_probability_yes <= 1.0
    assert decision.fair_probability_no == pytest.approx(
        1.0-decision.fair_probability_yes)
    assert decision.executable_yes_price == pytest.approx(0.48)
    assert decision.executable_no_price == pytest.approx(0.53)
    assert decision.net_edge_yes > 0 > decision.net_edge_no


def test_strong_down_selects_exactly_one_no_side():
    decision = _direction(value=-0.001)
    assert decision.output == decision.side == "BUY_NO"
    assert decision.net_edge_no > decision.net_edge_yes


@pytest.mark.parametrize(
    ("yes", "no", "reason"),
    [
        (None, _books()[1], "no_book"),
        (_book("yes", 0.47, 0.48, source_ts=NOW_MS-8_001),
         _books()[1], "stale_book"),
        (_book("yes", 0.47, 0.48, market="wrong"),
         _books()[1], "wrong_condition_pairing"),
        (_book("wrong", 0.47, 0.48), _books()[1], "wrong_token_pairing"),
        (_book("yes", 0.47, 0.48, ask_size=4.99),
         _books()[1], "insufficient_five_share_depth"),
    ],
)
def test_paired_book_provenance_and_depth_fail_closed(yes, no, reason):
    actual_yes = yes
    decision = LiteStrategy(LiteConfig()).choose_direction(
        _features(), cex_price=100.0, market=_market(),
        yes_book=actual_yes, no_book=no, cex_age_ms=100, now_ms=NOW_MS)
    assert decision.output == "NO_TRADE_DATA_INVALID"
    assert decision.side is None
    assert decision.reason == reason


def test_paired_book_timestamp_skew_and_reused_hash_are_rejected():
    yes = _book("yes", 0.47, 0.48, source_ts=NOW_MS-50)
    no = _book("no", 0.52, 0.53, source_ts=NOW_MS-2_100)
    assert _direction(yes=yes, no=no).reason == "paired_book_timestamp_skew"
    reused = replace(yes, hash_reused=True)
    assert _direction(yes=reused).reason == "reused_book_snapshot"


def test_cex_adjustment_is_bounded_and_decays_with_age_and_time():
    fresh = _direction(value=0.01, age=0)
    stale_near_limit = _direction(
        value=0.01, age=7_900,
        features=_features(0.01, age=7_900))
    assert abs(fresh.cex_adjustment) <= LiteConfig().fair_value_max_adjustment
    assert abs(stale_near_limit.cex_adjustment) < abs(fresh.cex_adjustment)
    early_market = _market(start_s=100)  # 0 seconds to close at NOW_MS -> invalid
    expired = _direction(market=early_market)
    assert expired.output == "NO_TRADE_DATA_INVALID"


@pytest.mark.parametrize("age", [-1, 8_001])
def test_stale_or_future_cex_is_rejected(age):
    features = _features(age=max(0, age))
    features["receipt_age_ms"] = age
    decision = _direction(features=features, age=age)
    assert decision.output == "NO_TRADE_DATA_INVALID"
    assert decision.side is None


def test_fee_net_negative_edge_and_expensive_negative_ev_are_rejected():
    flat = _direction(value=0.0, features=_features(0.0, tick=0.0))
    assert flat.output == "NO_TRADE_TRULY_NO_EDGE"
    expensive_yes = _book("yes", 0.97, 0.98)
    cheap_no = _book("no", 0.02, 0.03)
    expensive = _direction(value=0.001, yes=expensive_yes, no=cheap_no)
    assert expensive.output in (
        "NO_TRADE_TRULY_NO_EDGE", "BRIEF_CONFIRMATION_WAIT")
    assert expensive.side is None


def test_positive_edge_cross_uses_exact_five_share_sweep_and_fee():
    yes = _book("yes", 0.47, 0.48, asks=[(0.48, 2), (0.49, 3)])
    decision = _cross_decision(yes=yes)
    assert decision.accepted is True
    assert decision.side == "BUY_YES"
    assert decision.shares == FIXED_SHARES
    assert decision.entry_price == pytest.approx((0.48*2+0.49*3)/5)
    assert decision.entry_fee > 0
    assert decision.execution_state == "CROSS_SPREAD"
    assert decision.maker_fill_assumed is False


def test_lead_lag_uses_only_ordered_point_in_time_observations():
    strategy = LiteStrategy(LiteConfig())
    previous = {
        "cex_price": 100.0, "cex_ts_ms": NOW_MS-2_000,
        "market_probability_yes": 0.50, "poly_book_ts": NOW_MS-1_900,
    }
    lead = strategy.detect_lead_lag(
        cex_price=100.05, cex_move_ts=NOW_MS-1_000,
        market_probability_yes=0.501, poly_book_ts=NOW_MS-500,
        previous_observation=previous, now_ms=NOW_MS)
    assert lead.valid and lead.status == "LEAD_DETECTED"
    assert 0 < lead.probability_adjustment <= LiteConfig().lead_lag_max_adjustment
    stale = strategy.detect_lead_lag(
        cex_price=100.05, cex_move_ts=NOW_MS-100,
        market_probability_yes=0.501, poly_book_ts=NOW_MS-200,
        previous_observation=previous, now_ms=NOW_MS)
    assert stale.valid is False
    assert stale.status == "BOOK_PREDATES_MOVE"


def test_unchanged_but_fresh_cex_tick_is_valid_no_new_tick():
    strategy = LiteStrategy(LiteConfig())
    previous = {
        "cex_price": 100.0, "cex_ts_ms": NOW_MS-2_000,
        "market_probability_yes": 0.50, "poly_book_ts": NOW_MS-1_900,
    }
    yes, no = _books()
    decision = strategy.choose_direction(
        _features(age=100, move_ts=NOW_MS-2_000), cex_price=100.0,
        market=_market(), yes_book=yes, no_book=no, cex_age_ms=100,
        previous_observation=previous, now_ms=NOW_MS)
    assert decision.output != "NO_TRADE_DATA_INVALID"
    assert decision.lead_lag_status == "NO_NEW_TICK"

    evidence = strategy.detect_lead_lag(
        cex_price=100.0, cex_move_ts=NOW_MS-2_000,
        market_probability_yes=0.501, poly_book_ts=NOW_MS-500,
        previous_observation=previous, now_ms=NOW_MS)
    assert evidence.valid is True
    assert evidence.status == "NO_NEW_TICK"
    assert evidence.reason == "no_new_cex_tick_but_fresh"


def test_consecutive_unchanged_provider_ticks_keep_the_move_watermark():
    strategy = LiteStrategy(LiteConfig())
    direction = replace(_direction(), poly_book_ts=NOW_MS-500)

    first = strategy.observation(
        direction, cex_price=100.0,
        features={
            "provider_ts_ms": NOW_MS-1_000,
            "latest_move_ts_ms": NOW_MS-2_000,
        })
    second = strategy.observation(
        direction, cex_price=100.0,
        features={
            "provider_ts_ms": NOW_MS-500,
            "latest_move_ts_ms": NOW_MS-2_000,
        })

    assert first["cex_ts_ms"] == second["cex_ts_ms"] == NOW_MS-2_000
    evidence = strategy.detect_lead_lag(
        cex_price=100.0, cex_move_ts=NOW_MS-2_000,
        market_probability_yes=0.501, poly_book_ts=NOW_MS-100,
        previous_observation=second, now_ms=NOW_MS)
    assert evidence.valid is True
    assert evidence.status == "NO_NEW_TICK"


@pytest.mark.parametrize(
    ("move_ts", "poly_book_ts"),
    [
        pytest.param(NOW_MS-2_001, NOW_MS-500, id="regressed-cex"),
        pytest.param(NOW_MS+1, NOW_MS-500, id="future-cex"),
        pytest.param(NOW_MS-1_000, NOW_MS+1, id="future-book"),
        pytest.param(NOW_MS-1_000, NOW_MS-1_901, id="regressed-book"),
    ],
)
def test_future_or_regressed_lead_lag_timestamps_remain_invalid(
        move_ts, poly_book_ts):
    strategy = LiteStrategy(LiteConfig())
    previous = {
        "cex_price": 100.0, "cex_ts_ms": NOW_MS-2_000,
        "market_probability_yes": 0.50, "poly_book_ts": NOW_MS-1_900,
    }
    evidence = strategy.detect_lead_lag(
        cex_price=100.05, cex_move_ts=move_ts,
        market_probability_yes=0.501, poly_book_ts=poly_book_ts,
        previous_observation=previous, now_ms=NOW_MS)
    assert evidence.valid is False
    assert evidence.status == "OUT_OF_ORDER"
    assert evidence.reason == "future_or_out_of_order_lag_data"


def test_stale_cex_with_unchanged_move_timestamp_remains_invalid():
    previous = {
        "cex_price": 100.0, "cex_ts_ms": NOW_MS-9_000,
        "market_probability_yes": 0.50, "poly_book_ts": NOW_MS-1_900,
    }
    features = _features(age=8_001, move_ts=NOW_MS-9_000)
    decision = _direction(
        features=features, age=8_001, previous=previous)
    assert decision.output == "NO_TRADE_DATA_INVALID"
    assert decision.side is None
    assert decision.reason == "stale_or_invalid_cex"


def test_maker_is_observational_touch_never_counts_as_fill():
    strategy = LiteStrategy(LiteConfig())
    direction = _direction()
    yes, _ = _books()
    initial = strategy.optimize_entry(direction, yes, _market(), NOW_MS)
    assert initial.action == "MAKER_WAIT"
    lock = {
        "side": "BUY_YES", "maker_price": 0.47, "initial_ask": 0.48,
        "max_chase_price": 0.55, "maker_start_ts": NOW_MS,
        "maker_deadline_ts": NOW_MS+4_000,
    }
    touched = _book("yes", 0.46, 0.47)
    waiting = strategy.optimize_entry(
        direction, touched, _market(), NOW_MS+1_000, lock=lock)
    assert waiting.action == "MAKER_WAIT"
    assert waiting.maker_fill_assumed is False
    assert "FILLED" not in waiting.execution_state


def test_maker_wait_is_bounded_and_cross_requires_remaining_edge():
    strategy = LiteStrategy(LiteConfig())
    direction = _direction()
    yes, _ = _books()
    lock = {
        "side": "BUY_YES", "maker_price": 0.47, "initial_ask": 0.48,
        "max_chase_price": 0.55, "maker_start_ts": NOW_MS-5_000,
        "maker_deadline_ts": NOW_MS-1,
    }
    crossed = strategy.optimize_entry(direction, yes, _market(), NOW_MS, lock=lock)
    assert crossed.action == "CROSS_SPREAD"
    no_edge = replace(
        direction, net_edge_yes=0.001, selected_net_edge=0.001)
    skipped = strategy.optimize_entry(no_edge, yes, _market(), NOW_MS, lock=lock)
    assert skipped.action == "SKIP"
    assert skipped.reason == "maker_edge_expired"


def test_chase_cap_blocks_cross_and_direction_never_reverses():
    strategy = LiteStrategy(LiteConfig())
    direction = _direction()
    yes, _ = _books()
    lock = {
        "side": "BUY_YES", "maker_price": 0.47, "initial_ask": 0.48,
        "max_chase_price": 0.47, "maker_start_ts": NOW_MS-5_000,
        "maker_deadline_ts": NOW_MS-1,
    }
    chased = strategy.optimize_entry(direction, yes, _market(), NOW_MS, lock=lock)
    assert chased.action == "SKIP"
    assert chased.reason == "chase_price_negative_ev"
    opposite = {**lock, "side": "BUY_NO"}
    blocked = strategy.optimize_entry(
        direction, yes, _market(), NOW_MS, lock=opposite)
    assert blocked.reason == "thesis_invalidated_no_reversal"


def test_exit_now_vs_hold_is_deterministic_and_not_forced_near_close():
    strategy = LiteStrategy(LiteConfig())
    direction = _direction()
    trade = {
        "side": "BUY_YES", "window_close_ts": 600_000,
        "entry_price": 0.48,
    }
    low_bid = _book("yes", 0.20, 0.21).sell_sweep(FIXED_SHARES)
    fee = sweep_taker_fee(low_bid)
    hold = strategy.decide_exit_or_hold(
        trade, direction, low_bid, fee, NOW_MS)
    assert hold.action == "HOLD"
    assert hold.hold_expected_value > hold.exit_now_value
    high_bid = _book("yes", 0.90, 0.91).sell_sweep(FIXED_SHARES)
    exit_now = strategy.decide_exit_or_hold(
        trade, direction, high_bid, sweep_taker_fee(high_bid), NOW_MS)
    assert exit_now.action == "EXIT_NOW"
    assert exit_now.exit_now_value > exit_now.hold_expected_value


def test_runtime_management_removed_forced_countdown_liquidation():
    source = inspect.getsource(LiteBot._manage_open_positions)
    assert "exit_before_close_s" not in source
    assert "decide_exit_or_hold" in source
    assert "update_trade_management" in source


def test_post_close_and_stale_exit_books_are_impossible():
    trade = {
        "side": "BUY_YES", "yes_token_id": "yes", "no_token_id": "no",
        "condition_id": "c1", "window_close_ts": NOW_MS,
    }
    quote = _book("yes", 0.47, 0.48)
    sweep, reason = direct_book_exit(trade, quote, NOW_MS, 8_000, 0.20)
    assert sweep is None and reason == "post_close_book_forbidden"
    open_trade = {**trade, "window_close_ts": 600_000}
    stale = _book("yes", 0.47, 0.48, source_ts=NOW_MS-8_001)
    sweep, reason = direct_book_exit(open_trade, stale, NOW_MS, 8_000, 0.20)
    assert sweep is None and reason == "stale_book"


def test_broker_persists_taker_edge_and_never_false_pullback_or_maker_fill():
    class Store:
        row = None

        def insert_trade(self, row):
            self.row = row
            return 7

    store = Store()
    decision = _cross_decision()
    result = LiteBroker(store).open_trade(
        _market(), replace(decision, shares=999, entry_cost=999), NOW_MS,
        current_commit="a"*40)
    assert store.row["shares"] == 5.0
    assert store.row["entry_mode"] == "CROSS_SPREAD_AFTER_MAKER_WAIT"
    assert store.row["pullback_start_ts"] is None
    assert store.row["pullback_condition"] is None
    assert store.row["maker_fill_assumed"] is False
    assert store.row["final_entry_reason"] == "maker_expired_cross_edge_valid"
    assert store.row["runtime_commit"] == "a"*40
    assert result["id"] == 7


def test_broker_rejects_fabricated_fill_and_incoherent_probability():
    class Store:
        def insert_trade(self, _row):
            raise AssertionError("invalid evidence must not reach storage")

    decision = _cross_decision()
    with pytest.raises(ValueError, match="fair-value edge"):
        LiteBroker(Store()).open_trade(
            _market(), replace(
                decision, fair_probability_no=decision.fair_probability_no+0.1),
            NOW_MS)
    with pytest.raises(ValueError, match="fair-value edge"):
        LiteBroker(Store()).open_trade(
            _market(), replace(decision, maker_fill_assumed=True), NOW_MS)
    with pytest.raises(ValueError, match="bound five-share"):
        LiteBroker(Store()).open_trade(
            _market(), replace(decision, entry_price=0.9), NOW_MS)


def test_cex_features_are_point_in_time_ordered_and_keep_window_reference():
    feed = LiteCexFeed(["BTC"])
    assert feed.record("BTC", 100.0, 300_000, "okx")
    assert feed.record("BTC", 101.0, 330_000, "okx")
    assert feed.record("BTC", 102.0, 390_000, "okx")
    assert feed.record("BTC", 999.0, 380_000, "okx") is False
    snapshot = feed.feature_snapshot(
        "BTC", [5, 10, 30, 60], 390_000, window_start_ms=300_000)
    assert snapshot["window_reference_price"] == 100.0
    assert snapshot["window_return"] == pytest.approx(0.02)
    feed.record("BTC", 200.0, 391_000, "bybit")
    assert feed.momentum_values("BTC", [5, 10, 30, 60], 391_000) == []


def test_market_parser_keeps_reversed_direct_tokens_and_optional_anchor():
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
    assert market is None and reason == "market_state_invalid"


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
    assert market is None and reason == "ambiguous_market"
