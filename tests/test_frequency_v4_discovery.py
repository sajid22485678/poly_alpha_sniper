from datetime import datetime, timezone

import pytest

from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config
from poly_alpha_sniper.lite_frequency_v4.contracts import AnchorStatus
from poly_alpha_sniper.lite_frequency_v4.discovery import (
    GammaMarketDiscovery,
    build_discovery_requests,
    discover_from_rows,
    exact_slug,
    parse_market_row,
    window_open_ms,
)


OPEN_MS = 1_800_000_000_000
NOW_MS = OPEN_MS + 10_000


def _iso(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z")


def _row(asset="xrp", minutes=5, market_id="m1", **overrides):
    close = OPEN_MS + minutes * 60_000
    row = {
        "id": market_id,
        "eventId": "e1",
        "conditionId": "c1",
        "slug": f"{asset}-updown-{minutes}m-{OPEN_MS // 1000}",
        "endDate": _iso(close),
        "clobTokenIds": '["yes-token","no-token"]',
        "outcomes": '["Up","Down"]',
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
    }
    row.update(overrides)
    return row


def test_generic_exact_five_minute_asset_is_eligible_without_hardcoding():
    result = parse_market_row(_row(asset="xrp"), now_ms=NOW_MS)
    assert result.reason == ""
    assert result.market.asset == "XRP"
    assert result.market.window_close_ms - result.market.window_open_ms == 300_000
    assert result.market.yes_token_id == "yes-token"
    assert result.market.no_token_id == "no-token"
    assert result.market.anchor_status is AnchorStatus.FIELD_MISSING


def test_reversed_direct_tokens_follow_outcome_labels_not_array_assumption():
    result = parse_market_row(_row(
        clobTokenIds='["down-token","up-token"]',
        outcomes='["Down","Up"]'), now_ms=NOW_MS)
    assert (result.market.yes_token_id, result.market.no_token_id) == (
        "up-token", "down-token")


@pytest.mark.parametrize(("extra", "expected"), [
    ({"priceToBeat": "123.45"}, AnchorStatus.ANCHORED),
    ({"priceToBeat": None}, AnchorStatus.NOT_YET_PUBLISHED),
    ({"priceToBeat": "bad"}, AnchorStatus.PARSE_FAILED),
    ({"anchorRequired": False}, AnchorStatus.UNANCHORED),
    ({}, AnchorStatus.FIELD_MISSING),
])
def test_anchor_statuses_are_distinct_and_optional(extra, expected):
    result = parse_market_row(_row(**extra), now_ms=NOW_MS)
    assert result.market is not None
    assert result.market.anchor_status is expected
    assert (result.market.price_to_beat == 123.45) is (
        expected is AnchorStatus.ANCHORED)


def test_wrong_duration_is_reported_separately_and_never_eligible():
    batch = discover_from_rows([_row(minutes=15)], now_ms=NOW_MS)
    assert batch.eligible_markets == ()
    assert batch.ignored_duration_counts == {"15m": 1}
    assert batch.rejected[0].reason == "wrong_duration"


def test_end_date_must_equal_slug_start_plus_exactly_300_seconds():
    result = parse_market_row(
        _row(endDate=_iso(OPEN_MS + 301_000)), now_ms=NOW_MS)
    assert result.market is None
    assert result.reason == "window_close_mismatch"


def test_missing_state_identity_and_ambiguous_event_fail_closed():
    state = _row()
    del state["acceptingOrders"]
    assert parse_market_row(state, now_ms=NOW_MS).reason == "market_state_invalid"
    ambiguous = _row(events=[{"id": "different"}])
    assert parse_market_row(ambiguous, now_ms=NOW_MS).reason == (
        "event_identity_ambiguous")


def test_repeated_same_market_is_deduped_but_distinct_market_is_ambiguous():
    same = _row()
    deduped = discover_from_rows([same, dict(same)], now_ms=NOW_MS)
    assert len(deduped.eligible_markets) == 1
    competing = _row(market_id="m2", conditionId="c2", eventId="e2")
    ambiguous = discover_from_rows([same, competing], now_ms=NOW_MS)
    assert ambiguous.eligible_markets == ()
    assert sum(row.reason == "duplicate_market_identity"
               for row in ambiguous.rejected) == 2


def test_required_direct_current_next_queries_and_independent_broad_query():
    cfg = FrequencyV4Config()
    requests = build_discovery_requests(cfg, NOW_MS)
    direct = [request for request in requests if not request.broad]
    broad = [request for request in requests if request.broad]
    assert len(direct) == 6 and len(broad) == 1
    assert {request.required_asset for request in direct} == {"BTC", "ETH", "SOL"}
    for asset in cfg.required_assets:
        current = window_open_ms(NOW_MS)
        assert any(request.params.get("slug") == exact_slug(asset, current)
                   for request in direct)
    assert broad[0].params["order"] == "endDate"
    assert "end_date_min" in broad[0].params
    assert broad[0].params["limit"] == 500


@pytest.mark.asyncio
async def test_discovery_fanout_accepts_additional_asset_and_dedupes_query_overlap():
    row = _row(asset="doge")

    async def fetch(params):
        return [row] if params.get("slug") is None else []

    batch = await GammaMarketDiscovery(fetch, FrequencyV4Config()).discover(NOW_MS)
    assert batch.assets == ("DOGE",)
    assert batch.request_count == 7
