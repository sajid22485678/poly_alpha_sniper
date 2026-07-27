"""Unauthenticated CEX market-data providers for Frequency V4 shadow.

The strategy consumes this module through :class:`CexProvider`; no trading,
account, credential, signing, order, or cancellation surface exists here.
OKX is the initial public-data implementation.  WebSocket ticker/trade events
are primary and the public REST ticker is used only for bounded hydration and
recovery.  Both paths share one admission watermark so an older cached REST
response can never overwrite newer WebSocket evidence.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import replace
from typing import Any, Callable, Iterable, Mapping, Optional, Protocol, runtime_checkable

import aiohttp
import websockets

from .contracts import CexObservation
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
    finite_number,
    invoke_callback,
    monotonic_ns,
    parse_positive_millis,
    stable_event_id,
    wall_ms,
)


OKX_PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_PUBLIC_REST_BASE = "https://www.okx.com"
OKX_PING = "ping"
OKX_PONG = "pong"
OKX_CHANNELS = ("tickers", "trades")
OKX_SERVICE_UPGRADE_CODE = "64008"
DEFAULT_INSTRUMENTS: dict[str, str] = {
    "BTC": "BTC-USDT",
    "ETH": "ETH-USDT",
    "SOL": "SOL-USDT",
}
_ASSET_RE = re.compile(r"^[A-Z0-9]{2,16}$")


@runtime_checkable
class CexProvider(Protocol):
    """Provider-neutral public market-data lifecycle used by the runtime."""

    source: str

    @property
    def health(self) -> Mapping[str, Any]: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def set_assets(self, assets: Iterable[str]) -> None: ...

    async def hydrate_asset(self, asset: str) -> Optional[EventDecision]: ...

    async def hydrate_all(self) -> list[EventDecision]: ...


def instrument_for_asset(
        asset: str, instruments: Optional[Mapping[str, str]] = None) -> str:
    normalized = str(asset).strip().upper()
    if not _ASSET_RE.fullmatch(normalized):
        raise ValueError(f"invalid CEX asset: {asset!r}")
    mapping = {**DEFAULT_INSTRUMENTS,
               **{str(key).upper(): str(value).upper()
                  for key, value in (instruments or {}).items()}}
    instrument = mapping.get(normalized, f"{normalized}-USDT")
    if instrument != f"{normalized}-USDT" or not _ASSET_RE.fullmatch(normalized):
        # Explicit mappings may use a non-USDT public instrument, but must
        # remain plain OKX instrument identifiers.
        if not re.fullmatch(r"[A-Z0-9]{2,16}-[A-Z0-9]{2,16}", instrument):
            raise ValueError(f"invalid OKX instrument mapping: {instrument!r}")
    if instrument.split("-", 1)[0] != normalized:
        raise ValueError("OKX instrument base must match the requested asset")
    return instrument


def okx_subscription(
        instruments: Iterable[str], *, subscribe: bool,
        request_id: str = "frequencyv4",
) -> dict[str, Any]:
    """Build the documented public subscription/unsubscription frame."""

    unique = sorted({str(value).strip().upper()
                     for value in instruments if str(value).strip()})
    operation = "subscribe" if subscribe else "unsubscribe"
    normalized_id = str(request_id)
    # OKX rejects punctuation in this field (code 60033); its documented
    # request identifier is 1-32 alphanumeric characters.
    if not re.fullmatch(r"[A-Za-z0-9]{1,32}", normalized_id):
        raise ValueError("OKX request id must be 1-32 alphanumeric characters")
    return {
        "id": normalized_id,
        "op": operation,
        "args": [
            {"channel": channel, "instId": instrument}
            for instrument in unique
            for channel in OKX_CHANNELS
        ],
    }


def _optional_positive(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    return finite_number(value, positive=True)


def _optional_sequence(value: object) -> Optional[int]:
    if value in (None, "") or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text.isascii() or not text.isdecimal():
        return None
    parsed = int(text)
    return parsed if parsed >= 0 else None


class OkxPublicProvider:
    """Reconnectable OKX tickers/trades public-data implementation."""

    source = "okx"

    def __init__(
            self,
            assets: Iterable[str] = ("BTC", "ETH", "SOL"),
            *,
            instruments: Optional[Mapping[str, str]] = None,
            ws_url: str = OKX_PUBLIC_WS_URL,
            rest_base_url: str = OKX_PUBLIC_REST_BASE,
            clock: object | None = None,
            event_gate: EventGate | None = None,
            on_observation: Optional[Callable[..., Any]] = None,
            on_health: Optional[Callable[..., Any]] = None,
            on_hydration: Optional[Callable[..., Any]] = None,
            websocket_factory: Optional[Callable[..., Any]] = None,
            session: Any | None = None,
            heartbeat_inactivity_s: float = 20.0,
            pong_timeout_s: float = 10.0,
            rest_timeout_s: float = 4.0,
            rest_retry_delays_s: Iterable[float] = (0.25, 0.5),
            max_event_age_ms: int = 2_000,
    ):
        if heartbeat_inactivity_s <= 0 or heartbeat_inactivity_s >= 30:
            raise ValueError("OKX heartbeat inactivity must be in (0,30) seconds")
        if pong_timeout_s <= 0 or rest_timeout_s <= 0:
            raise ValueError("timeouts must be positive")
        self.ws_url = str(ws_url)
        self.rest_base_url = str(rest_base_url).rstrip("/")
        self.clock = clock or SystemClock()
        self.gate = event_gate or EventGate()
        self.on_observation = on_observation
        self.on_health = on_health
        self.on_hydration = on_hydration
        self.websocket_factory = websocket_factory or websockets.connect
        self._session = session
        self._owns_session = session is None
        self.heartbeat_inactivity_s = float(heartbeat_inactivity_s)
        self.pong_timeout_s = float(pong_timeout_s)
        self.rest_timeout_s = float(rest_timeout_s)
        self.max_event_age_ms = int(max_event_age_ms)
        if self.max_event_age_ms <= 0:
            raise ValueError("maximum event age must be positive")
        delays = tuple(float(value) for value in rest_retry_delays_s)
        if any(value < 0 for value in delays):
            raise ValueError("REST retry delays cannot be negative")
        self.rest_retry_delays_s = delays
        self._instrument_overrides = dict(instruments or {})
        self._desired: dict[str, str] = {}
        for asset in assets:
            normalized = str(asset).strip().upper()
            self._desired[normalized] = instrument_for_asset(
                normalized, self._instrument_overrides)
        if len(set(self._desired.values())) != len(self._desired):
            raise ValueError("multiple CEX assets cannot own one instrument")
        self._instrument_to_asset = {
            instrument: asset for asset, instrument in self._desired.items()
        }
        self.health_state = SourceHealth(source=self.source)
        self.health_state.desired_subscriptions = len(self._desired) * len(OKX_CHANNELS)
        self._acknowledged: set[tuple[str, str]] = set()
        self._hydrated: set[str] = set()
        self._connection_epoch = 0
        self._ws: Any | None = None
        self._task: Optional[asyncio.Task[Any]] = None
        self._heartbeat_task: Optional[asyncio.Task[Any]] = None
        self._stop = asyncio.Event()
        self._last_frame_mono_ns = 0
        self._last_ping_mono_ns = 0
        self._last_pong_mono_ns = 0
        self._request_counter = 0

    @property
    def connection_epoch(self) -> int:
        return self._connection_epoch

    @property
    def assets(self) -> tuple[str, ...]:
        return tuple(sorted(self._desired))

    @property
    def health(self) -> dict[str, Any]:
        result = self.health_state.to_dict()
        result.update({
            "assets": list(self.assets),
            "instruments": [self._desired[asset] for asset in self.assets],
            "hydrated_assets": sorted(self._hydrated),
            "transport_primary": "websocket",
            "rest_role": "hydration_and_recovery_only",
        })
        return result

    async def _publish_health(self) -> None:
        await invoke_callback(self.on_health, self.health)

    async def _publish(
            self, observation: CexObservation, decision: EventDecision,
    ) -> EventDecision:
        self.health_state.record(decision)
        await invoke_callback(self.on_observation, observation, decision)
        return decision

    async def _reject(
            self, disposition: EventDisposition, reason: str,
            observation: CexObservation | None = None,
    ) -> EventDecision:
        decision = EventDecision(disposition, observation, reason)
        self.health_state.record(decision)
        if observation is not None:
            await invoke_callback(self.on_observation, observation, decision)
        return decision

    def _next_request_id(self, operation: str) -> str:
        self._request_counter += 1
        label = "sub" if operation == "sub" else "unsub"
        return f"v4{label}{self._connection_epoch}{self._request_counter}"[:32]

    def _refresh_ready_state(self) -> None:
        desired_acks = {
            (channel, instrument)
            for instrument in self._desired.values()
            for channel in OKX_CHANNELS
        }
        all_hydrated = bool(self._desired) and self._hydrated >= set(self._desired)
        all_acked = bool(desired_acks) and self._acknowledged >= desired_acks
        if self.health_state.connected:
            self.health_state.state = "READY" if all_hydrated and all_acked else "HYDRATING"
        self.health_state.acknowledged_subscriptions = len(
            self._acknowledged & desired_acks)
        self.health_state.hydrated_subscriptions = len(self._hydrated)

    def _new_connection_epoch(self) -> None:
        self._connection_epoch += 1
        self.health_state.connection_epoch = self._connection_epoch
        if self._connection_epoch > 1:
            self.health_state.reconnect_count += 1
        self._acknowledged.clear()
        self._hydrated.clear()
        self._last_frame_mono_ns = monotonic_ns(self.clock)
        self._last_ping_mono_ns = 0
        self._last_pong_mono_ns = 0
        self._refresh_ready_state()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        if self._owns_session and self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.rest_timeout_s))
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="frequency_v4_okx_public")

    async def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception as exc:  # noqa: BLE001 - see the task loop below
                self.health_state.last_error = (
                    f"stop_close:{type(exc).__name__}:{exc}")[:240]
        for task in (self._heartbeat_task, self._task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._heartbeat_task, self._task):
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:  # noqa: BLE001
                    # A transport that already died is what stopping expects
                    # to find.  Letting it propagate would abort the engine's
                    # shutdown sequence before the runtime session is durably
                    # ended, which is far more damaging than a lost close
                    # frame.  Record it and finish stopping.
                    self.health_state.last_error = (
                        f"stop:{type(exc).__name__}:{exc}")[:240]
        self._heartbeat_task = None
        self._task = None
        if self._owns_session and self._session is not None:
            try:
                await self._session.close()
            except Exception as exc:  # noqa: BLE001
                self.health_state.last_error = (
                    f"stop_session:{type(exc).__name__}:{exc}")[:240]
            self._session = None
        self.health_state.connected = False
        self.health_state.state = "STOPPED"
        await self._publish_health()

    async def set_assets(self, assets: Iterable[str]) -> None:
        desired: dict[str, str] = {}
        for asset in assets:
            normalized = str(asset).strip().upper()
            desired[normalized] = instrument_for_asset(
                normalized, self._instrument_overrides)
        if len(set(desired.values())) != len(desired):
            raise ValueError("multiple CEX assets cannot own one instrument")
        previous_instruments = set(self._desired.values())
        next_instruments = set(desired.values())
        removed = previous_instruments - next_instruments
        added = next_instruments - previous_instruments
        self._desired = desired
        self._instrument_to_asset = {
            instrument: asset for asset, instrument in self._desired.items()
        }
        self._hydrated.intersection_update(self._desired)
        self._acknowledged = {
            key for key in self._acknowledged if key[1] in next_instruments
        }
        self.health_state.desired_subscriptions = len(self._desired) * len(OKX_CHANNELS)
        if self._ws is not None:
            if removed:
                await self._ws.send(canonical_json(okx_subscription(
                    removed, subscribe=False,
                    request_id=self._next_request_id("unsub"))))
            if added:
                await self._ws.send(canonical_json(okx_subscription(
                    added, subscribe=True,
                    request_id=self._next_request_id("sub"))))
                await asyncio.gather(*(
                    self.hydrate_asset(self._instrument_to_asset[instrument])
                    for instrument in sorted(added)
                ))
        self._refresh_ready_state()
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
                        self.ws_url, ping_interval=None, close_timeout=3) as ws:
                    self._ws = ws
                    self.health_state.connected = True
                    connected_started_ns = monotonic_ns(self.clock)
                    self._new_connection_epoch()
                    await ws.send(canonical_json(okx_subscription(
                        self._desired.values(), subscribe=True,
                        request_id=self._next_request_id("sub"))))
                    self.health_state.backoff_seconds = 0.0
                    await self._publish_health()
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(ws), name="frequency_v4_okx_heartbeat")
                    # Public REST is initial state only.  Any cache regression
                    # loses at the same EventGate watermark as WebSocket data.
                    await self.hydrate_all()
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self.handle_message(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 - transport boundary
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
                attempt = 0
            delay = backoff_seconds(attempt)
            attempt += 1
            self.health_state.state = "BACKOFF"
            self.health_state.backoff_seconds = delay
            await self._publish_health()
            await asyncio.sleep(delay)

    async def _heartbeat_loop(self, ws: Any) -> None:
        while not self._stop.is_set() and self.health_state.connected:
            await asyncio.sleep(min(1.0, self.heartbeat_inactivity_s / 2.0))
            now_mono = monotonic_ns(self.clock)
            inactive_ns = now_mono - self._last_frame_mono_ns
            if inactive_ns < int(self.heartbeat_inactivity_s * 1_000_000_000):
                continue
            sent_mono = monotonic_ns(self.clock)
            self._last_ping_mono_ns = sent_mono
            self.health_state.last_heartbeat_sent_ts_ms = wall_ms(self.clock)
            await ws.send(OKX_PING)
            await asyncio.sleep(self.pong_timeout_s)
            if self._last_pong_mono_ns < sent_mono:
                self.health_state.last_error = "okx_pong_timeout"
                await ws.close()
                return

    async def send_heartbeat_once(self, ws: Any | None = None) -> None:
        target = ws or self._ws
        if target is None:
            raise RuntimeError("websocket is not connected")
        self._last_ping_mono_ns = monotonic_ns(self.clock)
        self.health_state.last_heartbeat_sent_ts_ms = wall_ms(self.clock)
        await target.send(OKX_PING)

    async def handle_message(
            self, raw: str | bytes, *, receipt_ts_ms: Optional[int] = None,
            receipt_monotonic_ns: Optional[int] = None,
    ) -> list[EventDecision]:
        received_ms = int(receipt_ts_ms if receipt_ts_ms is not None
                          else wall_ms(self.clock))
        received_mono = int(
            receipt_monotonic_ns if receipt_monotonic_ns is not None
            else monotonic_ns(self.clock))
        self._last_frame_mono_ns = received_mono
        self.health_state.last_frame_receipt_ts_ms = received_ms
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                self.health_state.parse_errors += 1
                return []
        if str(raw).strip() == OKX_PONG:
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
            self.health_state.last_error = "invalid_okx_json"
            return []
        if not isinstance(payload, dict):
            self.health_state.parse_errors += 1
            return []
        event = str(payload.get("event") or "")
        if event:
            await self._handle_control(event, payload)
            return []
        arg = payload.get("arg")
        rows = payload.get("data")
        if not isinstance(arg, dict) or not isinstance(rows, list):
            self.health_state.parse_errors += 1
            return []
        channel = str(arg.get("channel") or "")
        instrument = str(arg.get("instId") or "").upper()
        if channel not in OKX_CHANNELS or not instrument:
            self.health_state.parse_errors += 1
            return []
        decisions: list[EventDecision] = []
        for row in rows:
            if not isinstance(row, dict):
                self.health_state.parse_errors += 1
                continue
            if channel == "tickers":
                decision = await self._handle_ticker_row(
                    instrument, row, received_ms, received_mono)
            else:
                decision = await self._handle_trade_row(
                    instrument, row, received_ms, received_mono)
            decisions.append(decision)
        return decisions

    async def _handle_control(self, event: str, payload: dict[str, Any]) -> None:
        arg = payload.get("arg") if isinstance(payload.get("arg"), dict) else {}
        channel = str(arg.get("channel") or "")
        instrument = str(arg.get("instId") or "").upper()
        if event == "subscribe" and channel in OKX_CHANNELS and instrument:
            if instrument in self._instrument_to_asset:
                self._acknowledged.add((channel, instrument))
        elif event == "unsubscribe" and channel and instrument:
            self._acknowledged.discard((channel, instrument))
        elif event in {"error", "notice"}:
            code = str(payload.get("code") or "")
            message = str(payload.get("msg") or "")
            self.health_state.last_error = f"okx_{event}:{code}:{message}"[:240]
            if event == "notice" and code == OKX_SERVICE_UPGRADE_CODE:
                self.health_state.state = "RECONNECT_REQUESTED"
                if self._ws is not None:
                    await self._ws.close()
        self._refresh_ready_state()
        await self._publish_health()

    def _asset_for_instrument(self, instrument: str) -> Optional[str]:
        return self._instrument_to_asset.get(str(instrument).upper())

    @staticmethod
    def _payload_event_id(event_type: str, instrument: str,
                          provider_ms: int, payload: dict[str, Any]) -> str:
        return stable_event_id(
            "okx", event_type, instrument, provider_ms,
            canonical_payload_hash(payload),
        )

    async def _handle_ticker_row(
            self, instrument: str, row: dict[str, Any], receipt_ms: int,
            receipt_mono: int,
    ) -> EventDecision:
        row_instrument = str(row.get("instId") or instrument).upper()
        asset = self._asset_for_instrument(instrument)
        provider_ms = parse_positive_millis(row.get("ts"))
        price = finite_number(row.get("last"), positive=True)
        bid = _optional_positive(row.get("bidPx"))
        ask = _optional_positive(row.get("askPx"))
        size = _optional_positive(row.get("lastSz"))
        if asset is None or row_instrument != instrument:
            return await self._reject(
                EventDisposition.REJECT_IDENTITY, "ticker_instrument_mismatch")
        if (provider_ms is None or price is None
                or (bid is not None and ask is not None and bid > ask)):
            return await self._reject(
                EventDisposition.REJECT_INVALID, "invalid_okx_ticker")
        observation = CexObservation(
            provider=self.source,
            asset=asset,
            instrument=instrument,
            price=price,
            provider_ts_ms=provider_ms,
            receipt_ts_ms=receipt_ms,
            receipt_monotonic_ns=receipt_mono,
            event_id=self._payload_event_id(
                "ticker", instrument, provider_ms, row),
            event_type="ticker",
            side="",
            sequence=None,
            connection_epoch=self.connection_epoch,
            bid=bid,
            ask=ask,
            size=size,
        )
        if provider_ms > receipt_ms:
            decision = EventDecision(
                EventDisposition.REJECT_FUTURE, observation,
                "provider_timestamp_after_receipt")
        elif receipt_ms - provider_ms > self.max_event_age_ms:
            decision = EventDecision(
                EventDisposition.REJECT_STALE, observation,
                "provider_evidence_too_old_at_receipt")
        else:
            decision = self.gate.evaluate(
                observation, stream_key=f"ticker:{instrument}",
                sequence_policy=SequencePolicy.NONE,
                observation_value=price,
                max_age_ms=self.max_event_age_ms)
        classification = (
            "NO_NEW_TICK"
            if decision.disposition is EventDisposition.ACCEPT_NO_NEW_TICK
            else "NEW_TICK")
        observation = replace(
            observation,
            unchanged=(decision.disposition is EventDisposition.ACCEPT_NO_NEW_TICK),
            classification=classification,
        )
        decision = replace(decision, event=observation)
        if decision.accepted or decision.duplicate:
            self._hydrated.add(asset)
            self._refresh_ready_state()
        return await self._publish(observation, decision)

    async def _handle_trade_row(
            self, instrument: str, row: dict[str, Any], receipt_ms: int,
            receipt_mono: int,
    ) -> EventDecision:
        row_instrument = str(row.get("instId") or instrument).upper()
        asset = self._asset_for_instrument(instrument)
        provider_ms = parse_positive_millis(row.get("ts"))
        price = finite_number(row.get("px"), positive=True)
        size = finite_number(row.get("sz"), positive=True)
        side = str(row.get("side") or "").lower()
        sequence = _optional_sequence(row.get("seqId"))
        if asset is None or row_instrument != instrument:
            return await self._reject(
                EventDisposition.REJECT_IDENTITY, "trade_instrument_mismatch")
        if (provider_ms is None or price is None or size is None
                or side not in {"buy", "sell"}
                or (row.get("seqId") not in (None, "") and sequence is None)):
            return await self._reject(
                EventDisposition.REJECT_INVALID, "invalid_okx_trade")
        observation = CexObservation(
            provider=self.source,
            asset=asset,
            instrument=instrument,
            price=price,
            provider_ts_ms=provider_ms,
            receipt_ts_ms=receipt_ms,
            receipt_monotonic_ns=receipt_mono,
            event_id=self._payload_event_id(
                "trade", instrument, provider_ms, row),
            event_type="trade",
            side=side,
            sequence=sequence,
            connection_epoch=self.connection_epoch,
            size=size,
        )
        if provider_ms > receipt_ms:
            decision = EventDecision(
                EventDisposition.REJECT_FUTURE, observation,
                "provider_timestamp_after_receipt")
        elif receipt_ms - provider_ms > self.max_event_age_ms:
            decision = EventDecision(
                EventDisposition.REJECT_STALE, observation,
                "provider_evidence_too_old_at_receipt")
        else:
            decision = self.gate.evaluate(
                observation, stream_key=f"trades:{instrument}",
                sequence_policy=SequencePolicy.NONDECREASING,
                observation_value=price,
                allow_equal_timestamp_distinct=True,
                max_age_ms=self.max_event_age_ms)
        classification = (
            "NO_NEW_TICK"
            if decision.disposition is EventDisposition.ACCEPT_NO_NEW_TICK
            else "NEW_TICK")
        observation = replace(
            observation,
            unchanged=(decision.disposition is EventDisposition.ACCEPT_NO_NEW_TICK),
            classification=classification,
        )
        decision = replace(decision, event=observation)
        return await self._publish(observation, decision)

    async def hydrate_all(self) -> list[EventDecision]:
        results = await asyncio.gather(*(
            self.hydrate_asset(asset) for asset in self.assets
        ))
        return [decision for decision in results if decision is not None]

    async def hydrate_asset(self, asset: str) -> Optional[EventDecision]:
        """Bounded public REST recovery for the exact configured instrument."""

        normalized = str(asset).strip().upper()
        instrument = self._desired.get(normalized)
        if instrument is None:
            decision = await self._reject(
                EventDisposition.REJECT_IDENTITY,
                "REST hydration requested for unknown asset")
            await invoke_callback(self.on_hydration, normalized, decision)
            return decision
        if self._session is None:
            if self._owns_session:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=self.rest_timeout_s))
            else:
                return None
        attempts = len(self.rest_retry_delays_s) + 1
        last_decision: Optional[EventDecision] = None
        for attempt in range(attempts):
            self.health_state.hydration_requests += 1
            try:
                async with self._session.get(
                        f"{self.rest_base_url}/api/v5/market/ticker",
                        params={"instId": instrument}) as response:
                    if int(response.status) != 200:
                        raise RuntimeError(f"OKX public REST HTTP {response.status}")
                    payload = await response.json(content_type=None)
                if (not isinstance(payload, dict)
                        or str(payload.get("code")) != "0"
                        or not isinstance(payload.get("data"), list)
                        or len(payload["data"]) != 1
                        or not isinstance(payload["data"][0], dict)):
                    raise ValueError("invalid OKX public REST payload")
                row = payload["data"][0]
                receipt_ms = wall_ms(self.clock)
                receipt_mono = monotonic_ns(self.clock)
                last_decision = await self._handle_ticker_row(
                    instrument, row, receipt_ms, receipt_mono)
                # A valid replay/regression is final evidence, not a transport
                # failure to retry until it appears fresh.
                break
            except (aiohttp.ClientError, asyncio.TimeoutError,
                    RuntimeError, ValueError, TypeError) as exc:
                self.health_state.last_error = f"okx_rest:{exc!r}"[:240]
                if attempt >= len(self.rest_retry_delays_s):
                    last_decision = await self._reject(
                        EventDisposition.REQUEST_HYDRATION,
                        "okx_public_rest_recovery_exhausted")
                    break
                await asyncio.sleep(self.rest_retry_delays_s[attempt])
        await invoke_callback(self.on_hydration, normalized, last_decision)
        self._refresh_ready_state()
        await self._publish_health()
        return last_decision

    async def recover_asset(self, asset: str) -> Optional[EventDecision]:
        return await self.hydrate_asset(asset)


# Concise runtime-facing alias.
OkxProvider = OkxPublicProvider
