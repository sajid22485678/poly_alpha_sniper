"""Discovery of live 5-minute crypto markets — real payload shapes
(fixtures mirror actual Gamma/CLOB responses captured 2026-07-08)."""
from datetime import datetime, timezone

import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import MarketType, OrderRequest, OrderSide
from poly_alpha_sniper.discovery.crypto_market_classifier import is_crypto_5min_candidate
from poly_alpha_sniper.discovery.market_discovery import MarketDiscovery
from poly_alpha_sniper.discovery.market_mapper import map_raw_market, normalize_raw_market
from poly_alpha_sniper.discovery.market_universe_expander import (
    build_gamma_queries, merge_markets)
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg

UTC = timezone.utc


def _iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


def gamma_updown(asset_word="Bitcoin", slug_asset="btc", tte_s=240.0,
                 title_suffix="July 8, 6:00AM-6:05AM ET", accepting=True,
                 condition="0x" + "ab" * 32, mid="gm-1"):
    """Gamma /markets shape, verbatim field names from the live API.
    Token ids/slug derive from `mid` so distinct fixtures never dedupe."""
    suffix = "".join(ch for ch in mid if ch.isalnum())
    return {
        "question": f"{asset_word} Up or Down - {title_suffix}",
        "slug": f"{slug_asset}-updown-5m-{suffix}",
        "id": mid, "conditionId": condition,
        "outcomes": '["Up", "Down"]',
        "clobTokenIds": f'["111000{suffix}", "222000{suffix}"]',
        "orderMinSize": 5, "orderPriceMinTickSize": 0.01, "negRisk": False,
        "liquidityNum": 14645.297, "volume24hr": 62485.29,
        "active": True, "closed": False, "acceptingOrders": accepting,
        "endDate": _iso(NOW_MS + int(tte_s * 1000)),
        "eventStartTime": _iso(NOW_MS + int((tte_s - 300) * 1000)),
        "description": 'This market will resolve to "Up" if the price at the '
                       "end of the range is greater than or equal to the start.",
    }


def gamma_threshold(asset_word="Bitcoin", slug_asset="btc", price="$108,500",
                    tte_s=240.0):
    raw = gamma_updown(asset_word=asset_word, slug_asset=slug_asset, tte_s=tte_s)
    raw["question"] = f"Will {asset_word} be above {price} at 3:45 PM ET?"
    raw["slug"] = f"will-{slug_asset}-be-above-x"
    raw["outcomes"] = '["Yes", "No"]'
    return raw


def clob_shape(condition="0x" + "ab" * 32, tte_s=240.0):
    """CLOB /markets|/sampling-markets shape."""
    return {
        "question": "Bitcoin Up or Down - July 8, 6:00AM-6:05AM ET",
        "market_slug": "btc-updown-5m-1783504800",
        "condition_id": condition,
        "tokens": [{"token_id": "111000111", "outcome": "Up"},
                   {"token_id": "222000222", "outcome": "Down"}],
        "end_date_iso": _iso(NOW_MS + int(tte_s * 1000)),
        "minimum_order_size": 5, "minimum_tick_size": 0.01,
        "active": True, "closed": False, "accepting_orders": True,
    }


# ---------------------------------------------------------------------------
def test_bitcoin_updown_5m_maps_to_btc():
    m = map_raw_market(gamma_updown("Bitcoin", "btc"), NOW_MS)
    assert m.asset == "BTC"
    assert m.market_type == MarketType.UP_DOWN
    assert m.direction_up_means_yes is True
    assert m.yes_token_id == "111000gm1"
    assert m.mapping_confidence >= 95
    assert m.raw["min_order_shares"] == 5
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert ok, reason


def test_ethereum_and_solana_updown_map():
    for word, slug, asset in (("Ethereum", "eth", "ETH"), ("Solana", "sol", "SOL")):
        m = map_raw_market(gamma_updown(word, slug), NOW_MS)
        assert m.asset == asset
        ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
        assert ok, f"{asset}: {reason}"


def test_updown_without_time_in_title_still_maps():
    m = map_raw_market(gamma_updown(title_suffix="5m"), NOW_MS)
    assert m.asset == "BTC"
    assert m.mapping_confidence >= 95
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert ok, reason


def test_threshold_markets_map():
    # asset-plausible strike prices (implausible ones are penalized by design)
    cases = (("Bitcoin", "btc", "BTC", "$108,500", 108500.0),
             ("Ethereum", "eth", "ETH", "$3,450", 3450.0),
             ("Solana", "sol", "SOL", "$150", 150.0))
    for word, slug, asset, price_str, price in cases:
        m = map_raw_market(gamma_threshold(word, slug, price=price_str), NOW_MS)
        assert m.asset == asset
        assert m.market_type == MarketType.THRESHOLD
        assert m.threshold == price
        ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
        assert ok, f"{asset}: {reason}"


def test_old_parent_event_date_does_not_kill_child():
    raw = gamma_updown(tte_s=240)
    raw["events"] = [{"endDate": "2020-01-01T00:00:00Z", "title": "old series"}]
    m = map_raw_market(raw, NOW_MS)
    assert m.seconds_to_expiry(NOW_MS) == pytest.approx(240, abs=1)
    ok, _ = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert ok


def test_live_child_not_marked_expired():
    m = map_raw_market(gamma_updown(tte_s=180), NOW_MS)
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert ok and reason == "ok"


def test_truly_expired_market_rejected():
    m = map_raw_market(gamma_updown(tte_s=-60), NOW_MS)
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert not ok
    assert reason == "expired"


def test_not_accepting_orders_rejected():
    m = map_raw_market(gamma_updown(accepting=False), NOW_MS)
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert not ok


def test_ambiguous_outcomes_still_rejected():
    raw = gamma_updown()
    raw["outcomes"] = '["Moon", "Dust"]'
    m = map_raw_market(raw, NOW_MS)
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert not ok


def test_xrp_market_rejected_no_asset():
    m = map_raw_market(gamma_updown("XRP", "xrp"), NOW_MS)
    assert m.asset == ""
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert not ok and reason == "no_asset"


def test_clob_shape_normalizes_and_maps():
    norm = normalize_raw_market(clob_shape())
    assert norm["outcomes"] == ["Up", "Down"]
    assert norm["clobTokenIds"] == ["111000111", "222000222"]
    m = map_raw_market(clob_shape(), NOW_MS)
    assert m.asset == "BTC"
    assert m.yes_token_id == "111000111"
    ok, reason = is_crypto_5min_candidate(m, NOW_MS, cfg())
    assert ok, reason


def test_gamma_clob_duplicates_dedupe():
    condition = "0x" + "cd" * 32
    merged = merge_markets([[gamma_updown(condition=condition)],
                            [clob_shape(condition=condition)]])
    assert len(merged) == 1


def test_min_order_usd_floor_not_five_dollars():
    """orderMinSize=5 means 5 SHARES — must not be read as $5."""
    m = map_raw_market(gamma_updown(), NOW_MS)
    assert m.min_order_size_usd <= cfg().risk.max_trade_usd
    assert m.raw["min_order_shares"] == 5


def test_validator_enforces_share_minimum():
    from poly_alpha_sniper.execution.order_validator import validate_order
    from poly_alpha_sniper.tests.helpers import book, market, portfolio_snapshot
    m = market()
    m.raw["min_order_shares"] = 5.0
    snap = portfolio_snapshot(equity=100, cash=10)
    small = OrderRequest(order_id="o", token_id="tok_yes", market_id="m1",
                         side=OrderSide.BUY_YES, price=0.50, size_shares=2.0,
                         size_usd=1.0)
    d = validate_order(small, book(bid=0.48, ask=0.50), m, snap, cfg(), NOW_MS)
    assert not d.approved
    assert d.reject_reason == "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"
    big = OrderRequest(order_id="o2", token_id="tok_yes", market_id="m1",
                       side=OrderSide.BUY_YES, price=0.20, size_shares=5.0,
                       size_usd=1.0)
    d2 = validate_order(big, book(bid=0.18, ask=0.20), m, snap, cfg(), NOW_MS)
    assert d2.approved, d2.reject_reason


async def test_discovery_refresh_and_diagnostics():
    rows = [gamma_updown("Bitcoin", "btc", mid="g1", condition="0x" + "11" * 32),
            gamma_updown("Ethereum", "eth", mid="g2", condition="0x" + "22" * 32),
            gamma_updown("Solana", "sol", mid="g3", condition="0x" + "33" * 32),
            gamma_updown("XRP", "xrp", mid="g4", condition="0x" + "44" * 32),
            gamma_updown("Bitcoin", "btc", tte_s=-120, mid="g5",
                         condition="0x" + "55" * 32)]

    async def fake_gamma(params):
        assert "_label" not in params  # label stripped before fetch
        assert "end_date_min" in params
        return rows if "far" not in str(params.get("end_date_max", "")) else []

    disc = MarketDiscovery(cfg(), SimClock(NOW_MS), fake_gamma)
    tradable = await disc.refresh()
    assets = sorted(m.asset for m in tradable)
    assert assets == ["BTC", "ETH", "SOL"]

    report = disc.diagnostic_report()
    assert report["mapped_count"] == 3
    assert report["raw_count"] == 5
    assert "rejected_by_reason" in report
    assert report["rejected_by_reason"].get("no_asset") == 1
    assert report["rejected_by_reason"].get("expired") == 1
    assert "end_window" in report["candidates_by_query"]
    assert len(report["top_raw"]) == 5
    first = report["mapped"][0]
    for key in ("asset", "title", "slug", "expiry", "time_to_expiry_s",
                "yes_token", "direction_up_means_yes", "active_tradable"):
        assert key in first


async def test_clob_fallback_used_when_gamma_empty():
    async def empty_gamma(params):
        return []

    async def clob_rows():
        return [clob_shape()]

    disc = MarketDiscovery(cfg(), SimClock(NOW_MS), empty_gamma, clob_fetcher=clob_rows)
    tradable = await disc.refresh()
    assert len(tradable) == 1
    assert tradable[0].asset == "BTC"
    assert disc.candidates_by_query.get("clob_fallback") == 1


def test_query_plans_use_end_window():
    queries = build_gamma_queries(cfg(), NOW_MS)
    assert all("end_date_min" in q and "end_date_max" in q for q in queries)
    assert any(q["_label"] == "end_window" for q in queries)
