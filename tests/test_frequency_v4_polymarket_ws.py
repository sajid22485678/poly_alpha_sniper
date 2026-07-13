from __future__ import annotations

import json

import pytest

from lite_frequency_v4.books import NoBookReason, normalize_book
from lite_frequency_v4.events import EventDisposition
from lite_frequency_v4.polymarket_ws import (
    POLYMARKET_HEARTBEAT,
    POLYMARKET_MARKET_WS_URL,
    PolymarketMarketWS,
    dynamic_subscription,
    initial_subscription,
)


TOKEN = "token-yes"
OTHER_TOKEN = "token-no"
CONDITION = "condition-1"


class Clock:
    def __init__(self, now_ms: int = 2_000, mono_ns: int = 2_000_000):
        self.wall = now_ms
        self.mono = mono_ns

    def now_ms(self) -> int:
        return self.wall

    def monotonic_ns(self) -> int:
        return self.mono


class FakeWs:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False

    async def send(self, value: str) -> None:
        self.sent.append(value)

    async def close(self) -> None:
        self.closed = True


def book(
        *, token: str = TOKEN, condition: str = CONDITION,
        ts: int = 1_000, bid: str = "0.55", ask: str = "0.65",
) -> dict[str, object]:
    return {
        "event_type": "book",
        "asset_id": token,
        "market": condition,
        "timestamp": str(ts),
        "hash": f"book-{ts}",
        "bids": [{"price": bid, "size": "10"}],
        "asks": [{"price": ask, "size": "10"}],
    }


def delta(
        *, token: str = TOKEN, condition: str = CONDITION,
        ts: int = 1_010, price: str = "0.60", size: str = "7",
        side: str = "BUY", best_bid: str = "0.60",
        best_ask: str = "0.65", suffix: str = "1",
) -> dict[str, object]:
    return {
        "event_type": "price_change",
        "market": condition,
        "timestamp": str(ts),
        "price_changes": [{
            "asset_id": token,
            "price": price,
            "size": size,
            "side": side,
            "hash": f"delta-{suffix}",
            "best_bid": best_bid,
            "best_ask": best_ask,
        }],
    }


def adapter(**kwargs: object) -> PolymarketMarketWS:
    result = PolymarketMarketWS(
        {TOKEN: CONDITION, OTHER_TOKEN: CONDITION},
        clock=Clock(),
        **kwargs,
    )
    result._new_connection_epoch()
    return result


def test_official_endpoint_and_subscription_wire_shapes() -> None:
    assert POLYMARKET_MARKET_WS_URL == \
        "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    assert initial_subscription(["b", "a", "a"]) == {
        "assets_ids": ["a", "b"],
        "type": "market",
        "custom_feature_enabled": True,
    }
    assert dynamic_subscription(["b", "a"], subscribe=True) == {
        "assets_ids": ["a", "b"],
        "operation": "subscribe",
        "custom_feature_enabled": True,
    }
    assert dynamic_subscription(["a"], subscribe=False) == {
        "assets_ids": ["a"],
        "operation": "unsubscribe",
    }


@pytest.mark.asyncio
async def test_literal_ping_pong_heartbeat_tracks_rtt() -> None:
    clock = Clock(now_ms=2_000, mono_ns=10_000_000)
    ws = FakeWs()
    stream = PolymarketMarketWS({TOKEN: CONDITION}, clock=clock)
    await stream.send_heartbeat_once(ws)
    assert ws.sent == [POLYMARKET_HEARTBEAT]
    clock.wall = 2_006
    clock.mono = 16_000_000
    assert await stream.handle_message("PONG") == []
    assert stream.health_state.last_pong_receipt_ts_ms == 2_006
    assert stream.health_state.heartbeat_rtt_ms == 6.0


@pytest.mark.asyncio
async def test_dynamic_rollover_unsubscribes_old_tokens_and_clears_state() -> None:
    requests: list[tuple[object, ...]] = []
    stream = adapter(on_hydration_request=lambda *args: requests.append(args))
    await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    ws = FakeWs()
    stream._ws = ws
    stream.health_state.connected = True
    await stream.set_subscriptions({"next-yes": "condition-2"})
    messages = [json.loads(value) for value in ws.sent]
    assert messages == [
        dynamic_subscription([TOKEN, OTHER_TOKEN], subscribe=False),
        dynamic_subscription(["next-yes"], subscribe=True),
    ]
    assert stream.book_state(TOKEN) == {}
    assert stream.health_state.state == "HYDRATING"
    assert requests[-1][:3] == (
        "next-yes", "condition-2", "dynamic_subscription")


@pytest.mark.asyncio
async def test_full_snapshot_then_official_nested_delta_updates_book() -> None:
    stream = adapter()
    decisions = await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    assert decisions[0].disposition is EventDisposition.ACCEPT_NEW
    assert stream.book_state(TOKEN)["best_bid"] == 0.55

    decisions = await stream.handle_message(
        json.dumps(delta()), receipt_ts_ms=1_030, receipt_monotonic_ns=20)
    assert decisions[0].accepted
    assert decisions[0].event is not None
    assert decisions[0].event.sequence is None
    assert stream.book_state(TOKEN)["best_bid"] == 0.60


@pytest.mark.asyncio
async def test_zero_size_delta_removes_level_without_fabricating_depth() -> None:
    stream = adapter()
    await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    await stream.handle_message(
        json.dumps(delta()), receipt_ts_ms=1_030, receipt_monotonic_ns=20)
    removal = delta(
        ts=1_020, price="0.60", size="0", best_bid="0.55", suffix="2")
    decisions = await stream.handle_message(
        json.dumps(removal), receipt_ts_ms=1_040, receipt_monotonic_ns=30)
    assert decisions[0].accepted
    assert stream.book_state(TOKEN)["best_bid"] == 0.55


@pytest.mark.asyncio
async def test_current_book_is_sorted_full_and_copy_safe_for_integration() -> None:
    stream = adapter()
    snapshot = book()
    snapshot["bids"] = [
        {"price": "0.40", "size": "4"},
        {"price": "0.55", "size": "10"},
        {"price": "0.50", "size": "6"},
    ]
    snapshot["asks"] = [
        {"price": "0.75", "size": "5"},
        {"price": "0.65", "size": "10"},
        {"price": "0.70", "size": "7"},
    ]
    await stream.handle_message(
        json.dumps(snapshot), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    current = stream.current_book(TOKEN)
    assert current == stream.normalized_book(TOKEN)
    assert current["condition_id"] == CONDITION
    assert current["bids"] == [(0.55, 10.0), (0.50, 6.0), (0.40, 4.0)]
    assert current["asks"] == [(0.65, 10.0), (0.70, 7.0), (0.75, 5.0)]
    assert current["hydrated"] is True
    assert current["connection_epoch"] == 1
    normalized = normalize_book(
        current,
        expected_token_id=TOKEN,
        expected_condition_id=CONDITION,
        receipt_ts_ms=1_020,
        receipt_monotonic_ns=10,
        hydrated=current["hydrated"],
        connection_epoch=current["connection_epoch"],
        now_ms=1_020,
    )
    assert normalized.reason is NoBookReason.OK
    assert normalized.book is not None
    assert normalized.book.best_bid == 0.55

    # Mutating the returned container cannot change adapter-owned levels.
    current["bids"].append((0.99, 999.0))
    assert stream.current_book(TOKEN)["bids"] == [
        (0.55, 10.0), (0.50, 6.0), (0.40, 4.0)]


@pytest.mark.asyncio
async def test_delta_before_hydration_is_bounded_and_rest_snapshot_flushes_it() -> None:
    requests: list[tuple[str, str, str, int]] = []

    async def request(*args: object) -> None:
        requests.append(tuple(args))

    stream = adapter(on_hydration_request=request,
                     max_buffered_deltas_per_token=2)
    buffered = delta(ts=1_020)
    decisions = await stream.handle_message(
        json.dumps(buffered), receipt_ts_ms=1_030,
        receipt_monotonic_ns=10)
    assert decisions[0].disposition is EventDisposition.BUFFER_UNHYDRATED
    assert requests == [(TOKEN, CONDITION, "delta_before_hydration", 1)]
    assert stream.health_state.hydration_requests == 1

    snapshot = book(ts=1_010)
    decision = await stream.accept_rest_book(
        snapshot, receipt_ts_ms=1_040, receipt_monotonic_ns=20)
    assert decision is not None and decision.accepted
    assert stream.book_state(TOKEN)["best_bid"] == 0.60
    assert stream.book_state(TOKEN)["provider_ts_ms"] == 1_020


@pytest.mark.asyncio
async def test_unhydrated_delta_burst_debounces_rest_recovery_requests() -> None:
    requests: list[tuple[object, ...]] = []
    stream = adapter(on_hydration_request=lambda *args: requests.append(args))
    first = delta(ts=1_010, suffix="1")
    second = delta(ts=1_011, suffix="2")
    await stream.handle_message(
        json.dumps(first), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    await stream.handle_message(
        json.dumps(second), receipt_ts_ms=1_021,
        receipt_monotonic_ns=11)
    assert len(requests) == 1
    assert stream.health_state.hydration_requests == 1
    assert stream.health_state.buffered_events == 2


@pytest.mark.asyncio
async def test_future_unhydrated_delta_is_rejected_not_buffered() -> None:
    requests: list[object] = []
    stream = adapter(on_hydration_request=lambda *args: requests.append(args))
    decisions = await stream.handle_message(
        json.dumps(delta(ts=2_001)), receipt_ts_ms=2_000,
        receipt_monotonic_ns=10)
    assert decisions[0].disposition is EventDisposition.REJECT_FUTURE
    assert requests == []
    assert not stream.hydrated_tokens


@pytest.mark.asyncio
async def test_stale_unhydrated_delta_is_rejected_not_recovered() -> None:
    requests: list[object] = []
    stream = adapter(on_hydration_request=lambda *args: requests.append(args))
    decisions = await stream.handle_message(
        json.dumps(delta(ts=1_000)), receipt_ts_ms=3_001,
        receipt_monotonic_ns=10)
    assert decisions[0].disposition is EventDisposition.REJECT_STALE
    assert requests == []
    assert not stream.hydrated_tokens


@pytest.mark.asyncio
async def test_wrong_token_and_condition_are_rejected() -> None:
    stream = adapter()
    wrong_token = await stream.handle_message(
        json.dumps(delta(token="unexpected")), receipt_ts_ms=1_030,
        receipt_monotonic_ns=10)
    wrong_condition = await stream.handle_message(
        json.dumps(book(condition="other")), receipt_ts_ms=1_030,
        receipt_monotonic_ns=20)
    assert wrong_token[0].disposition is EventDisposition.REJECT_IDENTITY
    assert wrong_condition[0].disposition is EventDisposition.REJECT_IDENTITY


@pytest.mark.asyncio
async def test_reported_bbo_mismatch_invalidates_local_book_and_recovers() -> None:
    requests: list[tuple[object, ...]] = []
    stream = adapter(on_hydration_request=lambda *args: requests.append(args))
    await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    mismatch = delta(best_bid="0.59")
    decisions = await stream.handle_message(
        json.dumps(mismatch), receipt_ts_ms=1_030,
        receipt_monotonic_ns=20)
    assert decisions[0].disposition is EventDisposition.REJECT_BBO_MISMATCH
    assert TOKEN not in stream.hydrated_tokens
    assert stream.book_state(TOKEN) == {}
    assert stream.health_state.hydrated_subscriptions == 0
    assert stream.health_state.state == "HYDRATING"
    assert requests[-1][2] == "bbo_mismatch"


@pytest.mark.asyncio
async def test_stale_bbo_event_cannot_invalidate_fresh_local_book() -> None:
    requests: list[tuple[object, ...]] = []
    stream = adapter(on_hydration_request=lambda *args: requests.append(args))
    await stream.handle_message(
        json.dumps(book(ts=3_000)), receipt_ts_ms=3_010,
        receipt_monotonic_ns=10)
    stale_mismatch = {
        "event_type": "best_bid_ask",
        "market": CONDITION,
        "asset_id": TOKEN,
        "best_bid": "0.10",
        "best_ask": "0.20",
        "timestamp": "1000",
    }
    decisions = await stream.handle_message(
        json.dumps(stale_mismatch), receipt_ts_ms=3_011,
        receipt_monotonic_ns=20)
    assert decisions[0].disposition is EventDisposition.REJECT_STALE
    assert stream.book_state(TOKEN)["best_bid"] == 0.55
    assert TOKEN in stream.hydrated_tokens
    assert requests == []


@pytest.mark.asyncio
async def test_crossed_or_boundary_price_books_never_become_executable() -> None:
    stream = adapter()
    crossed = book(bid="0.70", ask="0.60")
    decisions = await stream.handle_message(
        json.dumps(crossed), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    assert decisions[0].disposition is EventDisposition.REJECT_INVALID
    assert stream.book_state(TOKEN) == {}

    boundary = book(bid="0", ask="0.65", ts=1_001)
    decisions = await stream.handle_message(
        json.dumps(boundary), receipt_ts_ms=1_021,
        receipt_monotonic_ns=11)
    assert decisions == []
    assert stream.health_state.parse_errors == 1
    assert stream.book_state(TOKEN) == {}


@pytest.mark.asyncio
async def test_crossed_delta_invalidates_book_and_requests_exact_recovery() -> None:
    requests: list[tuple[object, ...]] = []
    stream = adapter(on_hydration_request=lambda *args: requests.append(args))
    await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    crossing = delta(
        side="SELL", price="0.50", size="10",
        best_bid="0.55", best_ask="0.50")
    decisions = await stream.handle_message(
        json.dumps(crossing), receipt_ts_ms=1_030,
        receipt_monotonic_ns=20)
    assert decisions[0].disposition is EventDisposition.REJECT_INVALID
    assert stream.book_state(TOKEN) == {}
    assert requests[-1][:3] == (TOKEN, CONDITION, "crossed_book")


@pytest.mark.asyncio
async def test_old_rest_snapshot_cannot_overwrite_newer_delta_state() -> None:
    stream = adapter()
    await stream.handle_message(
        json.dumps(book(ts=1_000)), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    await stream.handle_message(
        json.dumps(delta(ts=1_030)), receipt_ts_ms=1_040,
        receipt_monotonic_ns=20)
    decision = await stream.accept_rest_book(
        book(ts=1_010, bid="0.40"), receipt_ts_ms=1_050,
        receipt_monotonic_ns=30)
    assert decision is not None
    assert decision.disposition is EventDisposition.REJECT_TIMESTAMP_REGRESSION
    assert stream.book_state(TOKEN)["best_bid"] == 0.60


@pytest.mark.asyncio
async def test_reconnect_clears_executable_hydration_and_increments_counter() -> None:
    stream = adapter()
    await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    assert TOKEN in stream.hydrated_tokens
    stream._new_connection_epoch()
    assert stream.connection_epoch == 2
    assert stream.health_state.reconnect_count == 1
    assert not stream.hydrated_tokens
    assert stream.book_state(TOKEN) == {}

    replay = await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_030,
        receipt_monotonic_ns=20)
    assert replay[0].disposition is EventDisposition.DROP_DUPLICATE
    assert TOKEN in stream.hydrated_tokens
    assert stream.book_state(TOKEN)["best_bid"] == 0.55


@pytest.mark.asyncio
async def test_stale_duplicate_snapshot_cannot_rehydrate_after_reconnect() -> None:
    stream = adapter()
    await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    stream._new_connection_epoch()
    replay = await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=3_001,
        receipt_monotonic_ns=20)
    assert replay[0].disposition is EventDisposition.REJECT_STALE
    assert TOKEN not in stream.hydrated_tokens


@pytest.mark.asyncio
async def test_supported_auxiliary_and_lifecycle_events_remain_auditable() -> None:
    stream = adapter()
    await stream.handle_message(
        json.dumps(book()), receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    messages = [
        {"event_type": "last_trade_price", "market": CONDITION,
         "asset_id": TOKEN, "price": "0.61", "timestamp": "1010"},
        {"event_type": "tick_size_change", "market": CONDITION,
         "asset_id": TOKEN, "old_tick_size": "0.01", "new_tick_size": "0.001",
         "timestamp": "1011"},
        {"event_type": "best_bid_ask", "market": CONDITION,
         "asset_id": TOKEN, "best_bid": "0.55", "best_ask": "0.65",
         "timestamp": "1012"},
        {"event_type": "new_market", "market": "new-condition",
         "timestamp": "1013"},
        {"event_type": "market_resolved", "market": CONDITION,
         "winning_asset_id": TOKEN, "timestamp": "1014"},
    ]
    for index, message in enumerate(messages, start=1):
        decisions = await stream.handle_message(
            json.dumps(message), receipt_ts_ms=1_050 + index,
            receipt_monotonic_ns=20 + index)
        assert len(decisions) == 1
        assert decisions[0].accepted


def test_adapter_exposes_no_authenticated_or_execution_surface() -> None:
    forbidden = {
        "place_order", "cancel_order", "cancel_all", "sign", "authenticate",
        "api_key", "secret_key", "private_key",
    }
    assert forbidden.isdisjoint(set(dir(PolymarketMarketWS)))
