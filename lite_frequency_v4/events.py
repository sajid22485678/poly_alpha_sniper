"""Strict, deterministic source-event admission for Frequency V4 shadow.

This module is deliberately transport- and persistence-agnostic.  Public
adapters construct :class:`SourceEvent` or :class:`CexObservation` records and
pass them through :class:`EventGate` before any strategy state can observe
them.  Provider time, wall-clock receipt time, and monotonic receipt time are
kept separate; receiving a replayed frame never makes its evidence fresh.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import math
import time
from collections import OrderedDict, Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Optional

from .contracts import CexObservation, SourceEvent


class EventDisposition(str, Enum):
    """Outcome of validating one normalized public-data observation."""

    ACCEPT_NEW = "ACCEPT_NEW"
    ACCEPT_NO_NEW_TICK = "ACCEPT_NO_NEW_TICK"
    DROP_DUPLICATE = "DROP_DUPLICATE"
    REJECT_INVALID = "REJECT_INVALID"
    REJECT_FUTURE = "REJECT_FUTURE"
    REJECT_STALE = "REJECT_STALE"
    REJECT_TIMESTAMP_REGRESSION = "REJECT_TIMESTAMP_REGRESSION"
    REJECT_TIMESTAMP_CONFLICT = "REJECT_TIMESTAMP_CONFLICT"
    REJECT_RECEIPT_REGRESSION = "REJECT_RECEIPT_REGRESSION"
    REJECT_SEQUENCE = "REJECT_SEQUENCE"
    REJECT_IDENTITY = "REJECT_IDENTITY"
    REJECT_BBO_MISMATCH = "REJECT_BBO_MISMATCH"
    BUFFER_UNHYDRATED = "BUFFER_UNHYDRATED"
    REQUEST_HYDRATION = "REQUEST_HYDRATION"


class SequencePolicy(str, Enum):
    """Documented sequence guarantees for a stream.

    ``NONDECREASING`` intentionally does not require contiguity.  OKX's
    public trades channel exposes only a current ``seqId``, permits equal IDs,
    and does not provide ``prevSeqId``.  A numeric jump therefore is not proof
    of a dropped event.
    """

    NONE = "NONE"
    NONDECREASING = "NONDECREASING"


@dataclass(frozen=True)
class EventDecision:
    disposition: EventDisposition
    event: SourceEvent | CexObservation | None = None
    reason: str = ""
    request_hydration: bool = False

    @property
    def accepted(self) -> bool:
        return self.disposition in {
            EventDisposition.ACCEPT_NEW,
            EventDisposition.ACCEPT_NO_NEW_TICK,
        }

    @property
    def duplicate(self) -> bool:
        return self.disposition is EventDisposition.DROP_DUPLICATE


@dataclass
class SourceHealth:
    """Transport health kept distinct from market-evidence freshness."""

    source: str
    state: str = "DISCONNECTED"
    connected: bool = False
    connection_epoch: int = 0
    reconnect_count: int = 0
    desired_subscriptions: int = 0
    acknowledged_subscriptions: int = 0
    hydrated_subscriptions: int = 0
    last_frame_receipt_ts_ms: int = 0
    last_data_provider_ts_ms: int = 0
    last_data_receipt_ts_ms: int = 0
    last_heartbeat_sent_ts_ms: int = 0
    last_pong_receipt_ts_ms: int = 0
    heartbeat_rtt_ms: Optional[float] = None
    backoff_seconds: float = 0.0
    hydration_requests: int = 0
    buffered_events: int = 0
    duplicate_events: int = 0
    rejected_events: int = 0
    accepted_events: int = 0
    parse_errors: int = 0
    last_error: str = ""
    disposition_counts: Counter[str] = field(default_factory=Counter)

    def record(self, decision: EventDecision) -> None:
        value = decision.disposition.value
        self.disposition_counts[value] += 1
        if decision.accepted:
            self.accepted_events += 1
            if decision.event is not None:
                self.last_data_provider_ts_ms = int(
                    max(self.last_data_provider_ts_ms,
                        int(getattr(decision.event, "provider_ts_ms", 0) or 0)))
                self.last_data_receipt_ts_ms = int(
                    max(self.last_data_receipt_ts_ms,
                        int(getattr(decision.event, "receipt_ts_ms", 0) or 0)))
        elif decision.duplicate:
            self.duplicate_events += 1
        elif decision.disposition is EventDisposition.BUFFER_UNHYDRATED:
            self.buffered_events += 1
        else:
            self.rejected_events += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "state": self.state,
            "connected": self.connected,
            "connection_epoch": self.connection_epoch,
            "reconnect_count": self.reconnect_count,
            "desired_subscriptions": self.desired_subscriptions,
            "acknowledged_subscriptions": self.acknowledged_subscriptions,
            "hydrated_subscriptions": self.hydrated_subscriptions,
            "last_frame_receipt_ts_ms": self.last_frame_receipt_ts_ms,
            "last_data_provider_ts_ms": self.last_data_provider_ts_ms,
            "last_data_receipt_ts_ms": self.last_data_receipt_ts_ms,
            "last_heartbeat_sent_ts_ms": self.last_heartbeat_sent_ts_ms,
            "last_pong_receipt_ts_ms": self.last_pong_receipt_ts_ms,
            "heartbeat_rtt_ms": self.heartbeat_rtt_ms,
            "backoff_seconds": self.backoff_seconds,
            "hydration_requests": self.hydration_requests,
            "buffered_events": self.buffered_events,
            "duplicate_events": self.duplicate_events,
            "rejected_events": self.rejected_events,
            "accepted_events": self.accepted_events,
            "parse_errors": self.parse_errors,
            "last_error": self.last_error,
            "disposition_counts": dict(self.disposition_counts),
        }


@dataclass
class _StreamState:
    provider_ts_ms: int = 0
    receipt_monotonic_ns: int = 0
    payload_hash: str = ""
    connection_epoch: int = -1
    sequence: Optional[int] = None
    observation_value: Optional[float] = None
    latest_move_ts_ms: Optional[int] = None


_MISSING = object()


def canonical_json(payload: Mapping[str, Any] | list[Any]) -> str:
    """Stable JSON for event IDs, persistence, and deterministic replay."""

    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def canonical_payload_hash(payload: Mapping[str, Any] | list[Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def stable_event_id(*parts: object) -> str:
    raw = "\x1f".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_positive_millis(value: object) -> Optional[int]:
    """Parse an explicit positive integer timestamp without float coercion."""

    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text or not text.isascii() or not text.isdecimal():
        return None
    try:
        parsed = int(text)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def finite_number(value: object, *, positive: bool = False,
                  minimum: Optional[float] = None,
                  maximum: Optional[float] = None) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    if positive and parsed <= 0.0:
        return None
    if minimum is not None and parsed < minimum:
        return None
    if maximum is not None and parsed > maximum:
        return None
    return parsed


class EventGate:
    """Stateful admission gate with bounded, deterministic deduplication."""

    def __init__(self, *, dedupe_capacity: int = 4096,
                 stream_capacity: int = 4096):
        if int(dedupe_capacity) <= 0 or int(stream_capacity) <= 0:
            raise ValueError("event gate capacities must be positive")
        self.dedupe_capacity = int(dedupe_capacity)
        self.stream_capacity = int(stream_capacity)
        self._seen: OrderedDict[tuple[str, str, str], None] = OrderedDict()
        self._streams: OrderedDict[
            tuple[str, str, str], _StreamState] = OrderedDict()

    @staticmethod
    def _fingerprint(event: SourceEvent | CexObservation) -> str:
        explicit = str(getattr(event, "payload_hash", "") or "")
        if explicit:
            return explicit
        event_id = str(getattr(event, "event_id", "") or "")
        if event_id:
            return event_id
        try:
            data = event.to_dict()
        except AttributeError:
            data = vars(event)
        return canonical_payload_hash(data)

    @staticmethod
    def _source(event: SourceEvent | CexObservation) -> str:
        return str(getattr(event, "source", "")
                   or getattr(event, "provider", "") or "unknown")

    @staticmethod
    def _channel(event: SourceEvent | CexObservation) -> str:
        return str(getattr(event, "channel", "") or "cex")

    def evaluate(
            self,
            event: SourceEvent | CexObservation,
            *,
            stream_key: str,
            sequence_policy: SequencePolicy = SequencePolicy.NONE,
            observation_value: object = _MISSING,
            allow_equal_timestamp_distinct: bool = False,
            max_age_ms: Optional[int] = None,
    ) -> EventDecision:
        source = self._source(event)
        channel = self._channel(event)
        key = (source, channel, str(stream_key))
        fingerprint = self._fingerprint(event)
        seen_key = (source, str(stream_key), fingerprint)

        try:
            provider_ms = int(getattr(event, "provider_ts_ms"))
            receipt_ms = int(getattr(event, "receipt_ts_ms"))
            receipt_mono = int(getattr(event, "receipt_monotonic_ns"))
            epoch = int(getattr(event, "connection_epoch", 0))
        except (TypeError, ValueError, AttributeError, OverflowError):
            return EventDecision(EventDisposition.REJECT_INVALID, event,
                                 "invalid_event_timestamps")
        if min(provider_ms, receipt_ms, receipt_mono) <= 0 or epoch < 0:
            return EventDecision(EventDisposition.REJECT_INVALID, event,
                                 "non_positive_event_timestamps_or_epoch")
        if provider_ms > receipt_ms:
            return EventDecision(EventDisposition.REJECT_FUTURE, event,
                                 "provider_timestamp_after_receipt")

        # An exact replay is harmless even when it arrives after newer data.
        if seen_key in self._seen:
            self._seen.move_to_end(seen_key)
            return EventDecision(EventDisposition.DROP_DUPLICATE, event,
                                 "identical_event_replayed")
        if max_age_ms is not None:
            try:
                maximum_age = int(max_age_ms)
            except (TypeError, ValueError, OverflowError):
                return EventDecision(EventDisposition.REJECT_INVALID, event,
                                     "invalid_maximum_event_age")
            if maximum_age < 0:
                return EventDecision(EventDisposition.REJECT_INVALID, event,
                                     "invalid_maximum_event_age")
            if receipt_ms - provider_ms > maximum_age:
                return EventDecision(EventDisposition.REJECT_STALE, event,
                                     "provider_evidence_too_old_at_receipt")

        state = self._streams.get(key)
        if state is not None:
            if epoch < state.connection_epoch:
                return EventDecision(EventDisposition.REJECT_RECEIPT_REGRESSION,
                                     event, "connection_epoch_regressed")
            if (epoch == state.connection_epoch
                    and receipt_mono < state.receipt_monotonic_ns):
                return EventDecision(EventDisposition.REJECT_RECEIPT_REGRESSION,
                                     event, "monotonic_receipt_regressed")
            if provider_ms < state.provider_ts_ms:
                return EventDecision(
                    EventDisposition.REJECT_TIMESTAMP_REGRESSION, event,
                    "provider_timestamp_regressed")
            if (provider_ms == state.provider_ts_ms
                    and fingerprint != state.payload_hash
                    and not allow_equal_timestamp_distinct):
                return EventDecision(EventDisposition.REJECT_TIMESTAMP_CONFLICT,
                                     event, "same_timestamp_conflicting_payload")

            sequence = getattr(event, "sequence", None)
            if sequence_policy is SequencePolicy.NONDECREASING and sequence is not None:
                try:
                    parsed_sequence = int(sequence)
                except (TypeError, ValueError, OverflowError):
                    return EventDecision(EventDisposition.REJECT_SEQUENCE, event,
                                         "invalid_sequence")
                # Sequence numbering restarts at a new connection boundary.
                if (epoch == state.connection_epoch and state.sequence is not None
                        and parsed_sequence < state.sequence):
                    return EventDecision(EventDisposition.REJECT_SEQUENCE, event,
                                         "sequence_regressed")
        else:
            state = _StreamState()

        disposition = EventDisposition.ACCEPT_NEW
        parsed_value: Optional[float] = None
        if observation_value is not _MISSING:
            parsed_value = finite_number(observation_value)
            if parsed_value is None:
                return EventDecision(EventDisposition.REJECT_INVALID, event,
                                     "invalid_observation_value")
            if state.observation_value is not None and parsed_value == state.observation_value:
                disposition = EventDisposition.ACCEPT_NO_NEW_TICK

        sequence = getattr(event, "sequence", None)
        parsed_sequence = int(sequence) if sequence is not None else None
        latest_move = state.latest_move_ts_ms
        if parsed_value is not None:
            if state.observation_value is None or parsed_value != state.observation_value:
                latest_move = provider_ms

        self._streams[key] = _StreamState(
            provider_ts_ms=provider_ms,
            receipt_monotonic_ns=receipt_mono,
            payload_hash=fingerprint,
            connection_epoch=max(epoch, state.connection_epoch),
            sequence=parsed_sequence,
            observation_value=(parsed_value if parsed_value is not None
                               else state.observation_value),
            latest_move_ts_ms=latest_move,
        )
        self._streams.move_to_end(key)
        while len(self._streams) > self.stream_capacity:
            self._streams.popitem(last=False)
        self._seen[seen_key] = None
        self._seen.move_to_end(seen_key)
        while len(self._seen) > self.dedupe_capacity:
            self._seen.popitem(last=False)
        reason = ("fresh_observation_without_price_move"
                  if disposition is EventDisposition.ACCEPT_NO_NEW_TICK
                  else "new_valid_event")
        return EventDecision(disposition, event, reason)

    def latest_move_ts_ms(self, source: str, channel: str,
                          stream_key: str) -> Optional[int]:
        state = self._streams.get((str(source), str(channel), str(stream_key)))
        return state.latest_move_ts_ms if state is not None else None

    def stream_state(self, source: str, channel: str,
                     stream_key: str) -> dict[str, Any]:
        state = self._streams.get((str(source), str(channel), str(stream_key)))
        if state is None:
            return {}
        return {
            "provider_ts_ms": state.provider_ts_ms,
            "receipt_monotonic_ns": state.receipt_monotonic_ns,
            "payload_hash": state.payload_hash,
            "connection_epoch": state.connection_epoch,
            "sequence": state.sequence,
            "observation_value": state.observation_value,
            "latest_move_ts_ms": state.latest_move_ts_ms,
        }


class SystemClock:
    """Default wall and monotonic clocks, replaceable by deterministic tests."""

    @staticmethod
    def now_ms() -> int:
        return time.time_ns() // 1_000_000

    @staticmethod
    def monotonic_ns() -> int:
        return time.monotonic_ns()


def wall_ms(clock: object) -> int:
    value = getattr(clock, "now_ms", None)
    if callable(value):
        return int(value())
    if callable(clock):
        return int(clock())
    raise TypeError("clock must expose now_ms()")


def monotonic_ns(clock: object) -> int:
    value = getattr(clock, "monotonic_ns", None)
    if callable(value):
        return int(value())
    # Existing repository clocks expose only now_ms().  Keep a monotonic unit
    # for deterministic tests without conflating it with the persisted wall
    # receipt timestamp.
    return time.monotonic_ns()


async def maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def invoke_callback(callback: Optional[Callable[..., Any]],
                          *args: Any) -> Any:
    if callback is None:
        return None
    return await maybe_await(callback(*args))


BACKOFF_SECONDS: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)


def backoff_seconds(attempt: int) -> float:
    index = max(0, min(int(attempt), len(BACKOFF_SECONDS) - 1))
    return BACKOFF_SECONDS[index]
