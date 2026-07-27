"""Public Polymarket CLOB market-channel adapter for Frequency V4 shadow.

Only unauthenticated market data is exposed.  The implementation follows the
documented market-channel wire schema, including nested ``price_changes``,
literal ``PING``/``PONG`` heartbeats, dynamic subscription messages, and
custom lifecycle events.  Polymarket publishes no sequence/previous-sequence
contract on this channel, so hashes remain opaque change identifiers and are
never promoted into fictional sequence guarantees.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping, Optional

import websockets

from .contracts import SourceEvent
from .events import (
    EventDecision,
    EventDisposition,
    EventGate,
    SequencePolicy,
    SourceHealth,
    SystemClock,
    backoff_seconds,
    canonical_json,
    canonical_payload_hash,
    invoke_callback,
    monotonic_ns,
    parse_positive_millis,
    stable_event_id,
    wall_ms,
)


POLYMARKET_MARKET_WS_URL = (
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)
POLYMARKET_HEARTBEAT = "PING"
POLYMARKET_HEARTBEAT_REPLY = "PONG"
SUPPORTED_EVENT_TYPES = frozenset({
    "book",
    "price_change",
    "tick_size_change",
    "last_trade_price",
    "best_bid_ask",
    "new_market",
    "market_resolved",
})


def initial_subscription(token_ids: Iterable[str]) -> dict[str, Any]:
    return {
        "assets_ids": sorted({str(token) for token in token_ids if str(token)}),
        "type": "market",
        "custom_feature_enabled": True,
    }


def dynamic_subscription(token_ids: Iterable[str], *, subscribe: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "assets_ids": sorted({str(token) for token in token_ids if str(token)}),
        "operation": "subscribe" if subscribe else "unsubscribe",
    }
    if subscribe:
        payload["custom_feature_enabled"] = True
    return payload


def _decimal(value: object, *, positive: bool = False,
             probability: bool = False) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    if positive and parsed <= 0:
        return None
    if probability and not (Decimal("0") <= parsed <= Decimal("1")):
        return None
    return parsed


@dataclass
class _BookState:
    condition_id: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    provider_ts_ms: int
    book_hash: str = ""

    @property
    def best_bid(self) -> Optional[Decimal]:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return min(self.asks) if self.asks else None


class PolymarketMarketWS:
    """Reconnectable, hydration-aware public market data adapter."""

    source = "polymarket"

    def __init__(
            self,
            token_conditions: Optional[Mapping[str, str]] = None,
            *,
            url: str = POLYMARKET_MARKET_WS_URL,
            clock: object | None = None,
            event_gate: EventGate | None = None,
            on_event: Optional[Callable[..., Any]] = None,
            on_health: Optional[Callable[..., Any]] = None,
            on_hydration_request: Optional[Callable[..., Any]] = None,
            websocket_factory: Optional[Callable[..., Any]] = None,
            heartbeat_interval_s: float = 10.0,
            pong_timeout_s: float = 10.0,
            max_buffered_deltas_per_token: int = 64,
            hydration_request_cooldown_s: float = 0.25,
            max_event_age_ms: int = 2_000,
    ):
        self.url = str(url)
        self.clock = clock or SystemClock()
        self.gate = event_gate or EventGate()
        self.on_event = on_event
        self.on_health = on_health
        self.on_hydration_request = on_hydration_request
        self.websocket_factory = websocket_factory or websockets.connect
        self.heartbeat_interval_s = float(heartbeat_interval_s)
        self.pong_timeout_s = float(pong_timeout_s)
        self.max_buffered_deltas_per_token = int(max_buffered_deltas_per_token)
        self.hydration_request_cooldown_s = float(hydration_request_cooldown_s)
        self.max_event_age_ms = int(max_event_age_ms)
        if self.heartbeat_interval_s <= 0 or self.pong_timeout_s <= 0:
            raise ValueError("heartbeat intervals must be positive")
        if self.max_buffered_deltas_per_token <= 0:
            raise ValueError("max buffered deltas must be positive")
        if self.hydration_request_cooldown_s <= 0:
            raise ValueError("hydration request cooldown must be positive")
        if self.max_event_age_ms <= 0:
            raise ValueError("maximum event age must be positive")

        self._desired: dict[str, str] = {
            str(token): str(condition)
            for token, condition in (token_conditions or {}).items()
            if str(token) and str(condition)
        }
        self._books: dict[str, _BookState] = {}
        self._hydrated: set[str] = set()
        self._buffers: dict[str, deque[tuple[SourceEvent, dict[str, Any]]]] = (
            defaultdict(deque))
        self._ws: Any = None
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_ping_mono_ns = 0
        self._last_pong_mono_ns = 0
        self._last_hydration_request_mono_ns: dict[str, int] = {}
        self.health_state = SourceHealth(source=self.source)
        self.health_state.desired_subscriptions = len(self._desired)

    @property
    def connection_epoch(self) -> int:
        return self.health_state.connection_epoch

    @property
    def hydrated_tokens(self) -> frozenset[str]:
        return frozenset(self._hydrated)

    def health(self) -> dict[str, Any]:
        self.health_state.desired_subscriptions = len(self._desired)
        self.health_state.hydrated_subscriptions = len(self._hydrated)
        return self.health_state.to_dict()

    def book_state(self, token_id: str) -> dict[str, Any]:
        book = self._books.get(str(token_id))
        if book is None:
            return {}
        return {
            "condition_id": book.condition_id,
            "best_bid": float(book.best_bid) if book.best_bid is not None else None,
            "best_ask": float(book.best_ask) if book.best_ask is not None else None,
            "provider_ts_ms": book.provider_ts_ms,
            "hash": book.book_hash,
            "hydrated": str(token_id) in self._hydrated,
        }

    def current_book(self, token_id: str) -> dict[str, Any]:
        """Return a normalized copy of the current full in-memory book.

        Level entries are immutable tuples ordered best-price first and each
        containing only primitive values.  The level lists and mapping are
        new objects, so callers can pass them directly to ``normalize_book``
        or decorate them without mutating the adapter's executable state.
        """

        normalized_token = str(token_id)
        book = self._books.get(normalized_token)
        if book is None:
            return {}
        bids = [
            (float(price), float(book.bids[price]))
            for price in sorted(book.bids, reverse=True)
        ]
        asks = [
            (float(price), float(book.asks[price]))
            for price in sorted(book.asks)
        ]
        return {
            "token_id": normalized_token,
            "condition_id": book.condition_id,
            "bids": bids,
            "asks": asks,
            "provider_ts_ms": book.provider_ts_ms,
            "hash": book.book_hash,
            "hydrated": normalized_token in self._hydrated,
            "connection_epoch": self.connection_epoch,
        }

    # Explicit descriptive alias for callers that prefer the longer name.
    normalized_book = current_book

    async def _publish_health(self) -> None:
        await invoke_callback(self.on_health, self.health())

    async def _publish(self, event: SourceEvent,
                       decision: EventDecision) -> EventDecision:
        self.health_state.record(decision)
        await invoke_callback(self.on_event, event, decision)
        return decision

    async def _request_hydration(self, token: str, condition: str,
                                 reason: str, *, force: bool = False) -> bool:
        now_mono = monotonic_ns(self.clock)
        last_mono = self._last_hydration_request_mono_ns.get(str(token))
        cooldown_ns = int(self.hydration_request_cooldown_s * 1_000_000_000)
        if (not force and last_mono is not None
                and now_mono - last_mono < cooldown_ns):
            return False
        self._last_hydration_request_mono_ns[str(token)] = now_mono
        self.health_state.hydration_requests += 1
        await invoke_callback(
            self.on_hydration_request,
            str(token), str(condition), str(reason), self.connection_epoch,
        )
        return True

    def _new_connection_epoch(self) -> None:
        previous = self.health_state.connection_epoch
        self.health_state.connection_epoch = previous + 1
        if previous > 0:
            self.health_state.reconnect_count += 1
        # Old state remains historical in downstream storage but is never
        # executable in a new transport epoch.
        self._books.clear()
        self._hydrated.clear()
        self._buffers.clear()
        self._last_hydration_request_mono_ns.clear()
        # Heartbeat accounting is per transport epoch.  A stale unanswered
        # PING inherited from a previous connection otherwise trips the
        # pong-timeout check on the FIRST poll of the new epoch's heartbeat
        # loop - its age already exceeds pong_timeout_s - closing the fresh
        # socket before it ever sends its own first PING.  One genuine pong
        # loss then poisons every later epoch into a perpetual reconnect
        # loop with frozen heartbeat/pong timestamps.
        self._last_ping_mono_ns = 0
        self._last_pong_mono_ns = 0
        self.health_state.hydrated_subscriptions = 0
        self.health_state.state = "SUBSCRIBING"

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="frequency_v4_poly_ws")

    async def stop(self) -> None:
        self._stop.set()
        for task in (self._heartbeat_task, self._task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._heartbeat_task, self._task):
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:  # noqa: BLE001 - see below
                    # A socket that ended without a close handshake is what
                    # stopping *observes*, not a reason stopping failed.  If
                    # this propagated it would abort the caller's shutdown
                    # sequence mid-way -- and in this engine that sequence is
                    # what durably ends the runtime session, so a peer
                    # disconnecting rudely would leave the session row open
                    # forever.  Record it and finish stopping.
                    self.health_state.last_error = (
                        f"stop:{type(exc).__name__}:{exc}")[:240]
        self._heartbeat_task = None
        self._task = None
        self._ws = None
        self.health_state.connected = False
        self.health_state.state = "STOPPED"
        await self._publish_health()

    async def set_subscriptions(self, token_conditions: Mapping[str, str]) -> None:
        wanted = {
            str(token): str(condition)
            for token, condition in token_conditions.items()
            if str(token) and str(condition)
        }
        old_tokens, new_tokens = set(self._desired), set(wanted)
        removed = sorted(old_tokens - new_tokens)
        added = sorted(new_tokens - old_tokens)
        changed = sorted(token for token in old_tokens & new_tokens
                         if self._desired[token] != wanted[token])
        removed = sorted(set(removed + changed))
        added = sorted(set(added + changed))
        self._desired = wanted
        for token in removed:
            self._books.pop(token, None)
            self._hydrated.discard(token)
            self._buffers.pop(token, None)
            self._last_hydration_request_mono_ns.pop(token, None)
        self.health_state.desired_subscriptions = len(wanted)
        if self._ws is not None and self.health_state.connected:
            if removed:
                await self._ws.send(canonical_json(
                    dynamic_subscription(removed, subscribe=False)))
            if added:
                await self._ws.send(canonical_json(
                    dynamic_subscription(added, subscribe=True)))
                await asyncio.gather(*(
                    self._request_hydration(
                        token, self._desired[token], "dynamic_subscription")
                    for token in added
                ))
        if not wanted:
            self.health_state.state = "WAITING_FOR_SUBSCRIPTIONS"
        elif self.health_state.connected:
            self.health_state.state = (
                "READY" if self._hydrated == set(wanted) else "HYDRATING")
        self.health_state.hydrated_subscriptions = len(self._hydrated)
        await self._publish_health()

    async def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            if not self._desired:
                self.health_state.state = "WAITING_FOR_SUBSCRIPTIONS"
                await asyncio.sleep(0.25)
                continue
            connected_started_ns: Optional[int] = None
            try:
                self.health_state.state = "CONNECTING"
                await self._publish_health()
                async with self.websocket_factory(
                        self.url, ping_interval=None, close_timeout=3) as ws:
                    self._ws = ws
                    self.health_state.connected = True
                    connected_started_ns = monotonic_ns(self.clock)
                    self._new_connection_epoch()
                    await ws.send(canonical_json(initial_subscription(self._desired)))
                    self.health_state.state = "HYDRATING"
                    self.health_state.backoff_seconds = 0.0
                    await asyncio.gather(*(
                        self._request_hydration(token, condition, "reconnect")
                        for token, condition in sorted(self._desired.items())
                    ))
                    await self._publish_health()
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(ws),
                        name="frequency_v4_poly_heartbeat")
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self.handle_message(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 - transport must reconnect
                self.health_state.last_error = repr(exc)[:240]
            finally:
                if self._heartbeat_task is not None:
                    self._heartbeat_task.cancel()
                    try:
                        await self._heartbeat_task
                    except asyncio.CancelledError:
                        pass
                    self._heartbeat_task = None
                self._ws = None
                self.health_state.connected = False
            if self._stop.is_set():
                break
            if (connected_started_ns is not None
                    and monotonic_ns(self.clock) - connected_started_ns
                    >= 30_000_000_000):
                # A genuinely stable 30-second session clears failure history;
                # merely completing a handshake does not.
                attempt = 0
            delay = backoff_seconds(attempt)
            attempt += 1
            self.health_state.state = "BACKOFF"
            self.health_state.backoff_seconds = delay
            await self._publish_health()
            await asyncio.sleep(delay)
        self.health_state.connected = False

    async def _heartbeat_loop(self, ws: Any) -> None:
        interval_ns = int(self.heartbeat_interval_s * 1_000_000_000)
        timeout_ns = int(self.pong_timeout_s * 1_000_000_000)
        poll_s = min(1.0, self.heartbeat_interval_s, self.pong_timeout_s)
        started_ns = monotonic_ns(self.clock)
        while not self._stop.is_set() and self.health_state.connected:
            await asyncio.sleep(poll_s)
            now_mono = monotonic_ns(self.clock)
            if (self._last_ping_mono_ns > self._last_pong_mono_ns
                    and now_mono - self._last_ping_mono_ns >= timeout_ns):
                self.health_state.last_error = "polymarket_pong_timeout"
                await ws.close()
                return
            last_sent = self._last_ping_mono_ns or started_ns
            if now_mono - last_sent < interval_ns:
                continue
            self._last_ping_mono_ns = now_mono
            self.health_state.last_heartbeat_sent_ts_ms = wall_ms(self.clock)
            await ws.send(POLYMARKET_HEARTBEAT)

    async def send_heartbeat_once(self, ws: Any | None = None) -> None:
        target = ws or self._ws
        if target is None:
            raise RuntimeError("websocket is not connected")
        self._last_ping_mono_ns = monotonic_ns(self.clock)
        self.health_state.last_heartbeat_sent_ts_ms = wall_ms(self.clock)
        await target.send(POLYMARKET_HEARTBEAT)

    async def handle_message(
            self, raw: str | bytes, *, receipt_ts_ms: Optional[int] = None,
            receipt_monotonic_ns: Optional[int] = None,
    ) -> list[EventDecision]:
        received_ms = int(receipt_ts_ms if receipt_ts_ms is not None
                          else wall_ms(self.clock))
        received_mono = int(
            receipt_monotonic_ns if receipt_monotonic_ns is not None
            else monotonic_ns(self.clock))
        self.health_state.last_frame_receipt_ts_ms = received_ms
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                self.health_state.parse_errors += 1
                return []
        if str(raw).strip() == POLYMARKET_HEARTBEAT_REPLY:
            self._last_pong_mono_ns = received_mono
            self.health_state.last_pong_receipt_ts_ms = received_ms
            if self._last_ping_mono_ns:
                self.health_state.heartbeat_rtt_ms = max(
                    0.0, (received_mono - self._last_ping_mono_ns) / 1_000_000)
            await self._publish_health()
            return []
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            self.health_state.parse_errors += 1
            self.health_state.last_error = "invalid_polymarket_json"
            return []
        messages = payload if isinstance(payload, list) else [payload]
        decisions: list[EventDecision] = []
        for message in messages:
            if not isinstance(message, dict):
                self.health_state.parse_errors += 1
                continue
            event_type = str(message.get("event_type") or message.get("type") or "")
            if event_type not in SUPPORTED_EVENT_TYPES:
                continue
            if event_type == "book":
                decision = await self._handle_book(
                    message, received_ms, received_mono, channel="market")
                if decision is not None:
                    decisions.append(decision)
            elif event_type == "price_change":
                decisions.extend(await self._handle_price_change(
                    message, received_ms, received_mono))
            else:
                decision = await self._handle_auxiliary(
                    event_type, message, received_ms, received_mono)
                if decision is not None:
                    decisions.append(decision)
        return decisions

    def _identity_ok(self, token: str, condition: str) -> bool:
        return bool(token in self._desired
                    and condition
                    and self._desired[token] == condition)

    def _source_event(self, *, event_type: str, payload: dict[str, Any],
                      provider_ts_ms: int, receipt_ts_ms: int,
                      receipt_monotonic_ns: int, token_id: str = "",
                      condition_id: str = "", channel: str = "market") -> SourceEvent:
        payload_json = canonical_json(payload)
        payload_hash = canonical_payload_hash(payload)
        identity = token_id or condition_id or str(payload.get("market") or "")
        event_key = stable_event_id(
            self.source, channel, event_type, identity, provider_ts_ms,
            payload_hash,
        )
        return SourceEvent(
            source=self.source,
            channel=channel,
            event_type=event_type,
            event_key=event_key,
            payload_hash=payload_hash,
            provider_ts_ms=provider_ts_ms,
            receipt_ts_ms=receipt_ts_ms,
            receipt_monotonic_ns=receipt_monotonic_ns,
            sequence=None,
            asset="",
            market_id=condition_id,
            condition_id=condition_id,
            token_id=token_id,
            window_open_ms=None,
            payload_json=payload_json,
            connection_epoch=self.connection_epoch,
        )

    @staticmethod
    def _parse_levels(raw: object) -> Optional[dict[Decimal, Decimal]]:
        if not isinstance(raw, list):
            return None
        levels: dict[Decimal, Decimal] = {}
        for item in raw:
            if not isinstance(item, dict):
                return None
            price = _decimal(item.get("price"), probability=True)
            size = _decimal(item.get("size"), positive=True)
            if (price is None or size is None
                    or not Decimal("0") < price < Decimal("1")
                    or price in levels):
                return None
            levels[price] = size
        return levels

    async def _handle_book(
            self, payload: dict[str, Any], receipt_ms: int, receipt_mono: int,
            *, channel: str,
    ) -> Optional[EventDecision]:
        token = str(payload.get("asset_id") or "")
        condition = str(payload.get("market") or payload.get("condition_id") or "")
        provider_ms = parse_positive_millis(payload.get("timestamp"))
        bids = self._parse_levels(payload.get("bids"))
        asks = self._parse_levels(payload.get("asks"))
        if (not token or not condition or provider_ms is None
                or bids is None or asks is None):
            self.health_state.parse_errors += 1
            return None
        event = self._source_event(
            event_type="book", payload=payload,
            provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
            receipt_monotonic_ns=receipt_mono, token_id=token,
            condition_id=condition, channel=channel)
        if not self._identity_ok(token, condition):
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_IDENTITY, event,
                "book_token_or_condition_mismatch"))
        if provider_ms > receipt_ms:
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_FUTURE, event,
                "book_timestamp_after_receipt"))
        if receipt_ms - provider_ms > self.max_event_age_ms:
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_STALE, event,
                "book_too_old_at_receipt"))
        if bids and asks and max(bids) > min(asks):
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_INVALID, event,
                "crossed_book_snapshot"))
        current = self._books.get(token)
        if current is not None and provider_ms < current.provider_ts_ms:
            return await self._publish(event, EventDecision(
                EventDisposition.REJECT_TIMESTAMP_REGRESSION, event,
                "book_snapshot_older_than_local_book"))
        decision = self.gate.evaluate(
            event, stream_key=f"book_snapshot:{token}",
            sequence_policy=SequencePolicy.NONE,
            allow_equal_timestamp_distinct=True,
            max_age_ms=self.max_event_age_ms)
        await self._publish(event, decision)
        # A newly fetched/subscribed snapshot may be byte-identical to the
        # prior transport epoch.  It remains a deduplicated event (and cannot
        # retrigger strategy evidence), but a fresh exact snapshot may safely
        # re-establish local hydration after reconnect.
        rehydrate_duplicate = (
            decision.duplicate and token not in self._hydrated)
        if not decision.accepted and not rehydrate_duplicate:
            return decision
        self._books[token] = _BookState(
            condition_id=condition, bids=bids, asks=asks,
            provider_ts_ms=provider_ms,
            book_hash=str(payload.get("hash") or ""))
        self._hydrated.add(token)
        await self._flush_buffer(token)
        self.health_state.hydrated_subscriptions = len(self._hydrated)
        # Hydration progress promotes readiness only while the transport is
        # actually connected.  REST snapshots also flow through here and can
        # land during BACKOFF; a disconnected source must never report READY
        # merely because REST data kept its books fresh.
        if self.health_state.connected:
            self.health_state.state = (
                "READY" if self._desired and self._hydrated == set(self._desired)
                else "HYDRATING")
        await self._publish_health()
        return decision

    async def accept_rest_book(
            self, payload: dict[str, Any], *, receipt_ts_ms: Optional[int] = None,
            receipt_monotonic_ns: Optional[int] = None,
    ) -> Optional[EventDecision]:
        """Admit an exact-token public REST snapshot without regressing WS state."""

        return await self._handle_book(
            payload,
            int(receipt_ts_ms if receipt_ts_ms is not None else wall_ms(self.clock)),
            int(receipt_monotonic_ns if receipt_monotonic_ns is not None
                else monotonic_ns(self.clock)),
            channel="market",
        )

    async def _handle_price_change(
            self, payload: dict[str, Any], receipt_ms: int, receipt_mono: int,
    ) -> list[EventDecision]:
        condition = str(payload.get("market") or "")
        provider_ms = parse_positive_millis(payload.get("timestamp"))
        changes = payload.get("price_changes")
        if not condition or provider_ms is None or not isinstance(changes, list):
            self.health_state.parse_errors += 1
            return []
        decisions: list[EventDecision] = []
        for index, change in enumerate(changes):
            if not isinstance(change, dict):
                self.health_state.parse_errors += 1
                continue
            token = str(change.get("asset_id") or "")
            normalized = {
                "market": condition,
                "timestamp": str(provider_ms),
                "event_type": "price_change",
                "change_index": index,
                "change": change,
            }
            event = self._source_event(
                event_type="price_change", payload=normalized,
                provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
                receipt_monotonic_ns=receipt_mono, token_id=token,
                condition_id=condition)
            if not self._identity_ok(token, condition):
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_IDENTITY, event,
                    "delta_token_or_condition_mismatch")))
                continue
            if not self._valid_change(change):
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_INVALID, event,
                    "invalid_price_change")))
                continue
            if provider_ms > receipt_ms:
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_FUTURE, event,
                    "price_change_timestamp_after_receipt")))
                continue
            if receipt_ms - provider_ms > self.max_event_age_ms:
                decisions.append(await self._publish(event, EventDecision(
                    EventDisposition.REJECT_STALE, event,
                    "price_change_too_old_at_receipt")))
                continue
            if token not in self._hydrated:
                queue = self._buffers[token]
                if len(queue) >= self.max_buffered_deltas_per_token:
                    queue.popleft()
                queue.append((event, change))
                decision = EventDecision(
                    EventDisposition.BUFFER_UNHYDRATED, event,
                    "price_change_before_full_snapshot", request_hydration=True)
                decisions.append(await self._publish(event, decision))
                await self._request_hydration(token, condition, "delta_before_hydration")
                continue
            decisions.append(await self._apply_delta(event, change))
        return decisions

    @staticmethod
    def _valid_change(change: dict[str, Any]) -> bool:
        price = _decimal(change.get("price"), probability=True)
        size = _decimal(change.get("size"), probability=False)
        best_bid = _decimal(change.get("best_bid"), probability=True)
        best_ask = _decimal(change.get("best_ask"), probability=True)
        return bool(
            str(change.get("side") or "").upper() in {"BUY", "SELL"}
            and price is not None and Decimal("0") < price < Decimal("1")
            and size is not None and size >= 0
            and best_bid is not None and best_ask is not None
        )

    @staticmethod
    def _reported_bbo_matches(book: _BookState,
                              change: dict[str, Any]) -> bool:
        reported_bid = _decimal(change.get("best_bid"), probability=True)
        reported_ask = _decimal(change.get("best_ask"), probability=True)
        bid_ok = (reported_bid == book.best_bid
                  or (book.best_bid is None and reported_bid == Decimal("0")))
        ask_ok = (reported_ask == book.best_ask
                  or (book.best_ask is None and reported_ask == Decimal("1")))
        return bool(bid_ok and ask_ok)

    async def _apply_delta(self, event: SourceEvent,
                           change: dict[str, Any]) -> EventDecision:
        token = event.token_id
        current = self._books.get(token)
        if current is None:
            decision = EventDecision(
                EventDisposition.BUFFER_UNHYDRATED, event,
                "missing_local_book", request_hydration=True)
            await self._publish(event, decision)
            await self._request_hydration(token, event.condition_id, "missing_local_book")
            return decision
        bids, asks = dict(current.bids), dict(current.asks)
        price = _decimal(change.get("price"), probability=True)
        size = _decimal(change.get("size"))
        assert price is not None and size is not None
        if event.provider_ts_ms < current.provider_ts_ms:
            decision = EventDecision(
                EventDisposition.REJECT_TIMESTAMP_REGRESSION, event,
                "price_change_older_than_local_book")
            return await self._publish(event, decision)
        levels = bids if str(change.get("side")).upper() == "BUY" else asks
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        proposed = _BookState(
            condition_id=current.condition_id, bids=bids, asks=asks,
            provider_ts_ms=event.provider_ts_ms,
            book_hash=str(change.get("hash") or ""))
        if (proposed.best_bid is not None and proposed.best_ask is not None
                and proposed.best_bid > proposed.best_ask):
            self._books.pop(token, None)
            self._hydrated.discard(token)
            decision = EventDecision(
                EventDisposition.REJECT_INVALID, event,
                "crossed_book_after_delta", request_hydration=True)
            await self._publish(event, decision)
            self.health_state.hydrated_subscriptions = len(self._hydrated)
            self.health_state.state = "HYDRATING"
            await self._publish_health()
            await self._request_hydration(
                token, event.condition_id, "crossed_book", force=True)
            return decision
        if not self._reported_bbo_matches(proposed, change):
            self._books.pop(token, None)
            self._hydrated.discard(token)
            decision = EventDecision(
                EventDisposition.REJECT_BBO_MISMATCH, event,
                "reported_bbo_disagrees_with_local_delta", request_hydration=True)
            await self._publish(event, decision)
            self.health_state.hydrated_subscriptions = len(self._hydrated)
            self.health_state.state = "HYDRATING"
            await self._publish_health()
            await self._request_hydration(
                token, event.condition_id, "bbo_mismatch", force=True)
            return decision
        decision = self.gate.evaluate(
            event, stream_key=f"book_delta:{token}",
            sequence_policy=SequencePolicy.NONE,
            allow_equal_timestamp_distinct=True,
            max_age_ms=self.max_event_age_ms)
        await self._publish(event, decision)
        if decision.accepted:
            self._books[token] = proposed
        return decision

    async def _flush_buffer(self, token: str) -> None:
        queue = self._buffers.pop(token, deque())
        if not queue:
            return
        # Preserve receipt order.  Sorting by provider time would make a late,
        # out-of-order provider event appear valid and would falsify arrival
        # semantics.  Local provider watermarks reject such regressions.
        ordered = sorted(queue, key=lambda item: (
            item[0].receipt_monotonic_ns, item[0].event_key))
        snapshot_ts = self._books[token].provider_ts_ms
        for event, change in ordered:
            if event.provider_ts_ms <= snapshot_ts:
                continue
            if token not in self._hydrated:
                break
            await self._apply_delta(event, change)

    async def _handle_auxiliary(
            self, event_type: str, payload: dict[str, Any],
            receipt_ms: int, receipt_mono: int,
    ) -> Optional[EventDecision]:
        provider_ms = parse_positive_millis(payload.get("timestamp"))
        if provider_ms is None:
            self.health_state.parse_errors += 1
            return None
        condition = str(payload.get("market") or payload.get("condition_id") or "")
        token = str(payload.get("asset_id") or "")
        if event_type == "market_resolved":
            winning = str(payload.get("winning_asset_id") or "")
            known = {tok for tok, cond in self._desired.items() if cond == condition}
            if known and winning not in known:
                token = winning
                event = self._source_event(
                    event_type=event_type, payload=payload,
                    provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
                    receipt_monotonic_ns=receipt_mono, token_id=token,
                    condition_id=condition)
                return await self._publish(event, EventDecision(
                    EventDisposition.REJECT_IDENTITY, event,
                    "resolved_winner_not_in_verified_pair"))
        event = self._source_event(
            event_type=event_type, payload=payload,
            provider_ts_ms=provider_ms, receipt_ts_ms=receipt_ms,
            receipt_monotonic_ns=receipt_mono, token_id=token,
            condition_id=condition)
        if event_type in {"last_trade_price", "tick_size_change", "best_bid_ask"}:
            if not self._identity_ok(token, condition):
                return await self._publish(event, EventDecision(
                    EventDisposition.REJECT_IDENTITY, event,
                    "auxiliary_token_or_condition_mismatch"))
        stream_identity = token or condition or event.event_key
        admitted = self.gate.evaluate(
            event, stream_key=f"{event_type}:{stream_identity}",
            sequence_policy=SequencePolicy.NONE,
            allow_equal_timestamp_distinct=True,
            max_age_ms=self.max_event_age_ms)
        if not admitted.accepted:
            return await self._publish(event, admitted)
        if event_type == "best_bid_ask" and token in self._hydrated:
            book = self._books.get(token)
            if book is not None and not self._reported_bbo_matches(book, payload):
                self._books.pop(token, None)
                self._hydrated.discard(token)
                decision = EventDecision(
                    EventDisposition.REJECT_BBO_MISMATCH, event,
                    "best_bid_ask_disagrees_with_local_book",
                    request_hydration=True)
                await self._publish(event, decision)
                self.health_state.hydrated_subscriptions = len(self._hydrated)
                self.health_state.state = "HYDRATING"
                await self._publish_health()
                await self._request_hydration(
                    token, condition, "bbo_event_mismatch", force=True)
                return decision
        return await self._publish(event, admitted)


# Clear, compatibility-friendly name for the v4 runtime wiring.
PolymarketWS = PolymarketMarketWS
