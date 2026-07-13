from __future__ import annotations

import json
from typing import Any

import pytest

from lite_frequency_v4.cex import (
    CexProvider,
    OKX_PING,
    OKX_PUBLIC_WS_URL,
    OkxPublicProvider,
    instrument_for_asset,
    okx_subscription,
)
from lite_frequency_v4.events import EventDisposition


class Clock:
    def __init__(self, wall: int = 2_000, mono: int = 2_000_000):
        self.wall = wall
        self.mono = mono

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


class FakeResponse:
    def __init__(self, payload: object, status: int = 200):
        self.payload = payload
        self.status = status

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def json(self, *, content_type: object = None) -> object:
        return self.payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = responses
        self.requests: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, *, params: dict[str, str]) -> FakeResponse:
        self.requests.append((url, params))
        return self.responses.pop(0)


def ticker(
        *, instrument: str = "BTC-USDT", ts: int = 1_000,
        price: str = "60000", bid: str = "59999", ask: str = "60001",
) -> dict[str, str]:
    return {
        "instId": instrument,
        "last": price,
        "lastSz": "0.01",
        "bidPx": bid,
        "askPx": ask,
        "ts": str(ts),
    }


def trade(
        *, instrument: str = "BTC-USDT", ts: int = 1_000,
        price: str = "60000", sequence: int = 10,
        trade_id: str = "trade-1", side: str = "buy",
) -> dict[str, str]:
    return {
        "instId": instrument,
        "tradeId": trade_id,
        "px": price,
        "sz": "0.02",
        "side": side,
        "ts": str(ts),
        "seqId": str(sequence),
    }


def frame(channel: str, row: dict[str, str]) -> str:
    return json.dumps({
        "arg": {"channel": channel, "instId": row["instId"]},
        "data": [row],
    })


def provider(**kwargs: Any) -> OkxPublicProvider:
    result = OkxPublicProvider(
        ("BTC",), clock=Clock(), rest_retry_delays_s=(), **kwargs)
    result._new_connection_epoch()
    return result


def test_provider_protocol_and_public_wire_contract() -> None:
    stream = OkxPublicProvider(("BTC",), session=FakeSession([]))
    assert isinstance(stream, CexProvider)
    assert OKX_PUBLIC_WS_URL == "wss://ws.okx.com:8443/ws/v5/public"
    assert instrument_for_asset("btc") == "BTC-USDT"
    assert instrument_for_asset("doge") == "DOGE-USDT"
    with pytest.raises(ValueError):
        instrument_for_asset("BTC/USD")
    with pytest.raises(ValueError):
        instrument_for_asset("BTC", {"BTC": "ETH-USDT"})
    assert okx_subscription(
        ["ETH-USDT", "BTC-USDT"], subscribe=True,
        request_id="v4test") == {
            "id": "v4test",
            "op": "subscribe",
            "args": [
                {"channel": "tickers", "instId": "BTC-USDT"},
                {"channel": "trades", "instId": "BTC-USDT"},
                {"channel": "tickers", "instId": "ETH-USDT"},
                {"channel": "trades", "instId": "ETH-USDT"},
            ],
        }
    with pytest.raises(ValueError):
        okx_subscription(["BTC-USDT"], subscribe=True, request_id="v4-bad")


@pytest.mark.asyncio
async def test_subscribe_acks_are_distinct_from_data_hydration() -> None:
    stream = provider()
    for channel in ("tickers", "trades"):
        await stream.handle_message(json.dumps({
            "event": "subscribe",
            "arg": {"channel": channel, "instId": "BTC-USDT"},
        }))
    assert stream.health_state.acknowledged_subscriptions == 2
    assert stream.health_state.hydrated_subscriptions == 0
    assert stream.health_state.state != "READY"


@pytest.mark.asyncio
async def test_ticker_normalization_preserves_provider_and_receipt_time() -> None:
    observed: list[tuple[object, object]] = []
    stream = provider(on_observation=lambda *args: observed.append(args))
    decisions = await stream.handle_message(
        frame("tickers", ticker()), receipt_ts_ms=1_020,
        receipt_monotonic_ns=15)
    assert decisions[0].disposition is EventDisposition.ACCEPT_NEW
    observation = decisions[0].event
    assert observation is not None
    assert observation.provider_ts_ms == 1_000
    assert observation.receipt_ts_ms == 1_020
    assert observation.receipt_monotonic_ns == 15
    assert observation.event_type == "ticker"
    assert observation.asset == "BTC"
    assert observation.bid == 59_999
    assert observation.ask == 60_001
    assert observed and stream.health_state.hydrated_subscriptions == 1


@pytest.mark.asyncio
async def test_unchanged_ticker_is_fresh_no_new_tick_not_invalid() -> None:
    stream = provider()
    first = await stream.handle_message(
        frame("tickers", ticker(ts=1_000)), receipt_ts_ms=1_020,
        receipt_monotonic_ns=10)
    unchanged = await stream.handle_message(
        frame("tickers", ticker(ts=1_010)), receipt_ts_ms=1_030,
        receipt_monotonic_ns=20)
    assert first[0].disposition is EventDisposition.ACCEPT_NEW
    assert unchanged[0].disposition is EventDisposition.ACCEPT_NO_NEW_TICK
    assert unchanged[0].event is not None
    assert unchanged[0].event.unchanged is True
    assert unchanged[0].event.classification == "NO_NEW_TICK"
    assert stream.gate.latest_move_ts_ms(
        "okx", "cex", "ticker:BTC-USDT") == 1_000


@pytest.mark.asyncio
async def test_duplicate_future_and_regressed_tickers_fail_closed() -> None:
    stream = provider()
    raw = frame("tickers", ticker(ts=1_000))
    first = await stream.handle_message(
        raw, receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    replay = await stream.handle_message(
        raw, receipt_ts_ms=1_025, receipt_monotonic_ns=11)
    older = await stream.handle_message(
        frame("tickers", ticker(ts=999, price="59990")),
        receipt_ts_ms=1_030, receipt_monotonic_ns=20)
    future = await stream.handle_message(
        frame("tickers", ticker(ts=2_001, price="60010")),
        receipt_ts_ms=2_000, receipt_monotonic_ns=30)
    assert first[0].accepted
    assert replay[0].disposition is EventDisposition.DROP_DUPLICATE
    assert older[0].disposition is EventDisposition.REJECT_TIMESTAMP_REGRESSION
    assert future[0].disposition is EventDisposition.REJECT_FUTURE


@pytest.mark.asyncio
async def test_first_seen_stale_ticker_does_not_hydrate_source() -> None:
    stream = provider()
    stale = await stream.handle_message(
        frame("tickers", ticker(ts=1_000)), receipt_ts_ms=3_001,
        receipt_monotonic_ns=10)
    assert stale[0].disposition is EventDisposition.REJECT_STALE
    assert stream.health_state.hydrated_subscriptions == 0


@pytest.mark.asyncio
async def test_trade_side_and_noncontiguous_sequence_semantics() -> None:
    stream = provider()
    rows = [
        trade(ts=1_000, sequence=10, trade_id="a", side="buy"),
        trade(ts=1_001, sequence=10, trade_id="b", side="sell"),
        trade(ts=1_002, sequence=500, trade_id="c", side="buy"),
    ]
    for index, row in enumerate(rows):
        decisions = await stream.handle_message(
            frame("trades", row), receipt_ts_ms=1_020 + index,
            receipt_monotonic_ns=10 + index)
        assert decisions[0].accepted
        assert decisions[0].event is not None
        assert decisions[0].event.event_type == "trade"
        assert decisions[0].event.side == row["side"]
    regressed = await stream.handle_message(
        frame("trades", trade(
            ts=1_003, sequence=499, trade_id="d")),
        receipt_ts_ms=1_030, receipt_monotonic_ns=20)
    assert regressed[0].disposition is EventDisposition.REJECT_SEQUENCE


@pytest.mark.asyncio
async def test_sequence_can_restart_only_after_connection_epoch_changes() -> None:
    stream = provider()
    await stream.handle_message(
        frame("trades", trade(ts=1_000, sequence=100)),
        receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    stream._new_connection_epoch()
    reset = await stream.handle_message(
        frame("trades", trade(
            ts=1_001, sequence=1, trade_id="after-reconnect")),
        receipt_ts_ms=1_030, receipt_monotonic_ns=20)
    assert reset[0].accepted
    assert reset[0].event is not None
    assert reset[0].event.connection_epoch == 2


@pytest.mark.asyncio
async def test_fresh_duplicate_ticker_can_rehydrate_without_new_signal() -> None:
    stream = provider()
    raw = frame("tickers", ticker(ts=1_000))
    await stream.handle_message(
        raw, receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    stream._new_connection_epoch()
    replay = await stream.handle_message(
        raw, receipt_ts_ms=1_030, receipt_monotonic_ns=20)
    assert replay[0].disposition is EventDisposition.DROP_DUPLICATE
    assert stream.health_state.hydrated_subscriptions == 1

    stream._new_connection_epoch()
    stale = await stream.handle_message(
        raw, receipt_ts_ms=3_001, receipt_monotonic_ns=30)
    assert stale[0].disposition is EventDisposition.REJECT_STALE
    assert stream.health_state.hydrated_subscriptions == 0


@pytest.mark.asyncio
async def test_literal_ping_pong_and_service_upgrade_reconnect() -> None:
    clock = Clock(wall=2_000, mono=10_000_000)
    stream = OkxPublicProvider(
        ("BTC",), clock=clock, session=FakeSession([]),
        rest_retry_delays_s=())
    ws = FakeWs()
    stream._ws = ws
    await stream.send_heartbeat_once()
    assert ws.sent == [OKX_PING]
    clock.wall = 2_004
    clock.mono = 14_000_000
    assert await stream.handle_message("pong") == []
    assert stream.health_state.heartbeat_rtt_ms == 4.0

    await stream.handle_message(json.dumps({
        "event": "notice", "code": "64008", "msg": "service upgrade",
    }))
    assert ws.closed
    assert stream.health_state.state == "RECONNECT_REQUESTED"


@pytest.mark.asyncio
async def test_dynamic_assets_send_exact_unsubscribe_subscribe_and_hydrate() -> None:
    stream = provider()
    ws = FakeWs()
    stream._ws = ws
    hydrated: list[str] = []

    async def fake_hydrate(asset: str) -> None:
        hydrated.append(asset)

    stream.hydrate_asset = fake_hydrate  # type: ignore[method-assign]
    await stream.set_assets(("ETH",))
    messages = [json.loads(value) for value in ws.sent]
    assert messages[0]["op"] == "unsubscribe"
    assert {item["instId"] for item in messages[0]["args"]} == {"BTC-USDT"}
    assert messages[1]["op"] == "subscribe"
    assert {item["instId"] for item in messages[1]["args"]} == {"ETH-USDT"}
    assert hydrated == ["ETH"]


@pytest.mark.asyncio
async def test_rest_hydration_shares_watermark_and_cannot_regress_ws() -> None:
    old_rest = {
        "code": "0",
        "data": [ticker(ts=1_010, price="59900")],
    }
    session = FakeSession([FakeResponse(old_rest)])
    clock = Clock(wall=1_060, mono=30)
    stream = OkxPublicProvider(
        ("BTC",), clock=clock, session=session,
        rest_retry_delays_s=())
    stream._new_connection_epoch()
    ws = await stream.handle_message(
        frame("tickers", ticker(ts=1_050, price="60000")),
        receipt_ts_ms=1_055, receipt_monotonic_ns=20)
    decision = await stream.hydrate_asset("BTC")
    assert ws[0].accepted
    assert decision is not None
    assert decision.disposition is EventDisposition.REJECT_TIMESTAMP_REGRESSION
    state = stream.gate.stream_state("okx", "cex", "ticker:BTC-USDT")
    assert state["provider_ts_ms"] == 1_050
    assert state["observation_value"] == 60_000
    assert session.requests[0][1] == {"instId": "BTC-USDT"}


@pytest.mark.asyncio
async def test_rest_transport_failure_is_bounded_and_reported() -> None:
    session = FakeSession([FakeResponse({}, status=503)])
    stream = provider(session=session)
    decision = await stream.hydrate_asset("BTC")
    assert decision is not None
    assert decision.disposition is EventDisposition.REQUEST_HYDRATION
    assert stream.health_state.hydration_requests == 1
    assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_wrong_instrument_and_malformed_trade_are_rejected() -> None:
    stream = provider()
    wrong = await stream.handle_message(
        frame("tickers", ticker(instrument="ETH-USDT")),
        receipt_ts_ms=1_020, receipt_monotonic_ns=10)
    invalid_row = trade(side="unknown")
    invalid = await stream.handle_message(
        frame("trades", invalid_row), receipt_ts_ms=1_020,
        receipt_monotonic_ns=20)
    assert wrong[0].disposition is EventDisposition.REJECT_IDENTITY
    assert invalid[0].disposition is EventDisposition.REJECT_INVALID


def test_provider_has_no_auth_or_execution_api() -> None:
    forbidden = {
        "place_order", "cancel_order", "cancel_all", "sign", "authenticate",
        "api_key", "secret_key", "private_key", "passphrase",
    }
    assert forbidden.isdisjoint(set(dir(OkxPublicProvider)))
