"""Bounded, connection-free telemetry aggregation for Frequency V4.

This module deliberately does not import sqlite3 or :mod:`.store`.  The
telemetry worker owns only an in-memory queue.  It coalesces non-critical
observations and submits short, low-priority batches to the physical
persistence writer through a small duck-typed API::

    submit_telemetry_batch(commands, timeout_s=...)

Each command is a mapping containing ``method``, ``args`` and ``kwargs``.  The
physical writer remains the sole owner of the SQLite writer connection and is
responsible for executing a submitted batch atomically.  Actual trade evidence
must use the critical persistence lane, never this lossy telemetry lane.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict, deque
from concurrent.futures import Future as ConcurrentFuture
import copy
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import inspect
import json
import math
import threading
import time
from typing import Any, Callable, Hashable, Mapping, Optional, Protocol, Sequence


class TelemetrySink(Protocol):
    """Minimal API implemented by the physical persistence writer."""

    def submit_telemetry_batch(
        self, commands: Sequence[Mapping[str, Any]], *, timeout_s: float,
    ) -> Any:
        """Submit one low-priority batch and return a result or future."""


class TelemetryDisposition(str, Enum):
    """Outcome of a non-blocking telemetry admission attempt."""

    ACCEPTED = "ACCEPTED"
    COALESCED = "COALESCED"
    DROPPED = "DROPPED"


@dataclass(frozen=True, slots=True)
class TelemetryCommand:
    """One method invocation to execute inside a physical writer batch."""

    method: str
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method.strip():
            raise ValueError("telemetry command method must be a non-empty string")
        object.__setattr__(self, "args", tuple(copy.deepcopy(tuple(self.args))))
        if not isinstance(self.kwargs, Mapping):
            raise TypeError("telemetry command kwargs must be a mapping")
        object.__setattr__(self, "kwargs", copy.deepcopy(dict(self.kwargs)))

    def payload(self) -> dict[str, Any]:
        """Return the intentionally small duck-typed writer payload."""

        return {
            "method": self.method,
            "args": tuple(copy.deepcopy(self.args)),
            "kwargs": copy.deepcopy(dict(self.kwargs)),
        }


MergeHook = Callable[[TelemetryCommand, TelemetryCommand], TelemetryCommand]


def sum_kwargs(*fields: str) -> MergeHook:
    """Build a deterministic time-bucket merger for numeric keyword fields.

    Non-summed fields take their latest value.  Callers must opt fields in so a
    timestamp or identifier is never accidentally added merely because it is
    numeric.
    """

    names = tuple(str(name) for name in fields)
    if not names or any(not name for name in names):
        raise ValueError("sum_kwargs requires at least one non-empty field")

    def merge(existing: TelemetryCommand,
              incoming: TelemetryCommand) -> TelemetryCommand:
        if existing.method != incoming.method or existing.args != incoming.args:
            raise ValueError("aggregated telemetry commands must share method and args")
        combined = dict(existing.kwargs)
        combined.update(incoming.kwargs)
        for name in names:
            left = existing.kwargs.get(name, 0)
            right = incoming.kwargs.get(name, 0)
            if (isinstance(left, bool) or isinstance(right, bool)
                    or not isinstance(left, (int, float))
                    or not isinstance(right, (int, float))):
                raise ValueError(f"aggregated field {name!r} must be numeric")
            total = left + right
            if isinstance(total, float) and not math.isfinite(total):
                raise ValueError(f"aggregated field {name!r} became non-finite")
            combined[name] = total
        return TelemetryCommand(existing.method, existing.args, combined)

    return merge


@dataclass(slots=True)
class _Pending:
    token: int
    command: TelemetryCommand
    admitted_monotonic: float
    logical_count: int = 1
    dedupe_keys: set[str] = field(default_factory=set)
    state_key: Optional[str] = None
    state_signature: Optional[str] = None
    state_admitted_monotonic: Optional[float] = None
    aggregate_key: Optional[tuple[str, str, int, int]] = None
    merge_hook: Optional[MergeHook] = None


def _canonical_digest(value: Any) -> str:
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False, default=str,
        ).encode("utf-8")
    except (TypeError, ValueError):
        encoded = repr(value).encode("utf-8", errors="backslashreplace")
    return hashlib.sha256(encoded).hexdigest()


class V4TelemetryWriter:
    """A bounded telemetry aggregator with no database connection.

    Admission is always non-blocking.  Repeated state, duplicate event, and
    time-bucket aggregation are performed under a short in-memory lock.  A
    dedicated daemon thread flushes batches to the physical writer.  Failures
    are accounted for and contained here; they never propagate into the
    critical persistence lane.
    """

    def __init__(
        self,
        persistence_writer: TelemetrySink,
        *,
        capacity: int = 32_768,
        batch_size: int = 256,
        physical_batch_size: int = 32,
        flush_interval_s: float = 0.250,
        coalescing_interval_s: float = 5.0,
        submit_timeout_s: float = 5.0,
        heartbeat_interval_s: float = 1.0,
        dedupe_capacity: int = 50_000,
        state_capacity: int = 10_000,
        thread_name: str = "v4-telemetry-writer",
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("telemetry capacity must be a positive integer")
        if (isinstance(batch_size, bool) or not isinstance(batch_size, int)
                or batch_size < 1 or batch_size > capacity):
            raise ValueError("telemetry batch size must be within queue capacity")
        if (isinstance(physical_batch_size, bool)
                or not isinstance(physical_batch_size, int)
                or not 1 <= physical_batch_size <= 512):
            raise ValueError("physical_batch_size must be in [1,512]")
        for name, value in (
            ("flush_interval_s", flush_interval_s),
            ("coalescing_interval_s", coalescing_interval_s),
            ("submit_timeout_s", submit_timeout_s),
            ("heartbeat_interval_s", heartbeat_interval_s),
        ):
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(float(value)) or float(value) <= 0):
                raise ValueError(f"{name} must be finite and positive")
        for name, value in (
            ("dedupe_capacity", dedupe_capacity),
            ("state_capacity", state_capacity),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        submitter = getattr(persistence_writer, "submit_telemetry_batch", None)
        if not callable(submitter):
            raise TypeError(
                "persistence writer must expose submit_telemetry_batch(commands, timeout_s=...)"
            )

        self._persistence_writer = persistence_writer
        self.capacity = capacity
        self.batch_size = batch_size
        self.physical_batch_size = min(batch_size, physical_batch_size)
        self.flush_interval_s = float(flush_interval_s)
        self.coalescing_interval_s = float(coalescing_interval_s)
        self.submit_timeout_s = float(submit_timeout_s)
        self.heartbeat_interval_s = float(heartbeat_interval_s)
        self.dedupe_capacity = dedupe_capacity
        self.state_capacity = state_capacity
        self.thread_name = str(thread_name)

        self._condition = threading.Condition(threading.RLock())
        self._queue: deque[int] = deque()
        self._pending: dict[int, _Pending] = {}
        self._aggregate_tokens: dict[tuple[str, str, int, int], int] = {}
        self._pending_dedupe: dict[str, int] = {}
        self._dedupe_seen: OrderedDict[str, None] = OrderedDict()
        self._state_seen: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._next_token = 1
        self._thread: Optional[threading.Thread] = None
        self._accepting = True
        self._stop_requested = False
        self._drain_on_stop = True
        self._flush_requested = False
        self._inflight_batches = 0

        now_wall = int(time.time() * 1_000)
        self._health = "CREATED"
        self._last_error = ""
        self._last_heartbeat_ts_ms = now_wall
        self._last_success_ts_ms = 0
        self._last_failure_ts_ms = 0
        self._last_overflow_ts_ms = 0
        self._submitted = 0
        self._coalesced = 0
        self._deduplicated = 0
        self._dropped = 0
        self._written = 0
        self._logical_written = 0
        self._batches = 0
        self._batch_attempts = 0
        self._failed_batches = 0
        self._priority_skipped_batches = 0
        self._priority_skipped_rows = 0
        self._deadline_exceeded_batches = 0
        self._deadline_exceeded_rows = 0
        self._physical_batch_high_water = 0
        self._overflow_count = 0
        self._high_water = 0
        self._batch_latencies_ms: deque[float] = deque(maxlen=2_048)
        self._flush_latencies_ms: deque[float] = deque(maxlen=2_048)
        # Cooperative deadline backoff: when the physical sink repeatedly
        # exceeds its short transaction budget (the WAL-pinned slow-commit
        # case), resubmitting the next batch immediately just produces another
        # deadline miss and more dropped rows.  We pause admission-to-dispatch
        # until this monotonic deadline expires so a recovering writer is not
        # hammered while it is still drained.  The queue still accepts items;
        # only the flush is deferred.
        self._deadline_backoff_until: float = 0.0
        self._deadline_backoff_s: float = 0.0

    @staticmethod
    def _command(
        command: TelemetryCommand | Mapping[str, Any] | str,
        args: tuple[Any, ...], kwargs: Optional[Mapping[str, Any]],
    ) -> TelemetryCommand:
        if isinstance(command, TelemetryCommand):
            if args or kwargs is not None:
                raise ValueError("args/kwargs cannot accompany TelemetryCommand")
            # The pending queue must never share caller-owned nested payloads.
            return copy.deepcopy(command)
        if isinstance(command, Mapping):
            if args or kwargs is not None:
                raise ValueError("args/kwargs cannot accompany command mapping")
            allowed = {"method", "args", "kwargs"}
            if set(command) - allowed or "method" not in command:
                raise ValueError("command mapping accepts only method/args/kwargs")
            return TelemetryCommand(
                str(command["method"]), tuple(command.get("args") or ()),
                dict(command.get("kwargs") or {}),
            )
        return TelemetryCommand(str(command), tuple(args), dict(kwargs or {}))

    @staticmethod
    def _cache_put(cache: OrderedDict[str, Any], key: str, value: Any,
                   maximum: int) -> None:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > maximum:
            cache.popitem(last=False)

    def start(self) -> None:
        """Start the dedicated aggregation thread; safe to call once."""

        with self._condition:
            if self._thread is not None:
                if self._thread.is_alive():
                    return
                raise RuntimeError("telemetry writer cannot be restarted after stop")
            if self._stop_requested:
                raise RuntimeError("telemetry writer is already stopping")
            self._health = "HEALTHY"
            self._thread = threading.Thread(
                target=self._run, name=self.thread_name, daemon=True)
            self._thread.start()

    def submit(
        self,
        command: TelemetryCommand | Mapping[str, Any] | str,
        *args: Any,
        kwargs: Optional[Mapping[str, Any]] = None,
        dedupe_key: Optional[Hashable] = None,
        state_key: Optional[Hashable] = None,
        state_value: Any = None,
        bucket_key: Optional[Hashable] = None,
        event_ts_ms: Optional[int] = None,
        bucket_ms: int = 1_000,
        merge_hook: Optional[MergeHook] = None,
    ) -> TelemetryDisposition:
        """Admit telemetry without waiting for persistence.

        ``state_key`` stores the first state and every material state change,
        while identical state is sampled only after ``coalescing_interval_s``.
        ``bucket_key`` aggregates commands within an event-time bucket using a
        caller-supplied deterministic ``merge_hook``.  The two policies are
        intentionally mutually exclusive.
        """

        item = self._command(command, tuple(args), kwargs)
        if state_key is not None and bucket_key is not None:
            raise ValueError("state and bucket coalescing are mutually exclusive")
        if merge_hook is not None and bucket_key is None:
            raise ValueError("merge_hook requires bucket_key")
        if bucket_key is not None:
            if isinstance(bucket_ms, bool) or not isinstance(bucket_ms, int) or bucket_ms < 1:
                raise ValueError("bucket_ms must be a positive integer")
            if (event_ts_ms is None or isinstance(event_ts_ms, bool)
                    or not isinstance(event_ts_ms, int) or event_ts_ms < 0):
                raise ValueError("bucket aggregation requires a non-negative event_ts_ms")

        now_mono = time.monotonic()
        dedupe = _canonical_digest(dedupe_key) if dedupe_key is not None else None
        state = _canonical_digest(state_key) if state_key is not None else None
        signature = _canonical_digest(state_value) if state_key is not None else None
        aggregate: Optional[tuple[str, str, int, int]] = None
        if bucket_key is not None and event_ts_ms is not None:
            aggregate = (
                item.method, _canonical_digest(bucket_key),
                event_ts_ms // bucket_ms * bucket_ms, bucket_ms,
            )

        with self._condition:
            self._submitted += 1
            if not self._accepting or self._stop_requested:
                self._drop_locked(1, "telemetry_submit_after_stop")
                return TelemetryDisposition.DROPPED

            if dedupe is not None and (
                    dedupe in self._dedupe_seen or dedupe in self._pending_dedupe):
                self._coalesced += 1
                self._deduplicated += 1
                return TelemetryDisposition.COALESCED

            if state is not None and signature is not None:
                prior = self._state_seen.get(state)
                if (prior is not None and prior[0] == signature
                        and now_mono - prior[1] < self.coalescing_interval_s):
                    self._state_seen.move_to_end(state)
                    self._coalesced += 1
                    return TelemetryDisposition.COALESCED

            if aggregate is not None and aggregate in self._aggregate_tokens:
                token = self._aggregate_tokens[aggregate]
                pending = self._pending.get(token)
                if pending is not None:
                    try:
                        pending.command = (
                            merge_hook(pending.command, item)
                            if merge_hook is not None else item
                        )
                    except Exception:
                        self._drop_locked(1, "telemetry_merge_failed")
                        return TelemetryDisposition.DROPPED
                    pending.logical_count += 1
                    self._coalesced += 1
                    if dedupe is not None:
                        pending.dedupe_keys.add(dedupe)
                        self._pending_dedupe[dedupe] = token
                        self._cache_put(
                            self._dedupe_seen, dedupe, None, self.dedupe_capacity)
                    return TelemetryDisposition.COALESCED

            if len(self._queue) >= self.capacity:
                self._overflow_count += 1
                self._last_overflow_ts_ms = int(time.time() * 1_000)
                self._drop_locked(1, "telemetry_queue_full", health="DEGRADED_OVERFLOW")
                return TelemetryDisposition.DROPPED

            token = self._next_token
            self._next_token += 1
            pending = _Pending(
                token=token, command=item, admitted_monotonic=now_mono,
                dedupe_keys={dedupe} if dedupe is not None else set(),
                state_key=state, state_signature=signature,
                state_admitted_monotonic=now_mono if state is not None else None,
                aggregate_key=aggregate, merge_hook=merge_hook,
            )
            self._pending[token] = pending
            self._queue.append(token)
            if aggregate is not None:
                self._aggregate_tokens[aggregate] = token
            if dedupe is not None:
                self._pending_dedupe[dedupe] = token
                self._cache_put(
                    self._dedupe_seen, dedupe, None, self.dedupe_capacity)
            if state is not None and signature is not None:
                self._cache_put(
                    self._state_seen, state, (signature, now_mono),
                    self.state_capacity,
                )
            self._high_water = max(self._high_water, len(self._queue))
            self._condition.notify_all()
            return TelemetryDisposition.ACCEPTED

    def _drop_locked(self, logical_count: int, reason: str,
                     *, health: str = "DEGRADED_TELEMETRY") -> None:
        self._dropped += int(logical_count)
        self._last_failure_ts_ms = int(time.time() * 1_000)
        self._last_error = str(reason)[:240]
        self._health = health

    def _rollback_admission_locked(self, pending: _Pending) -> None:
        for dedupe_key in pending.dedupe_keys:
            self._dedupe_seen.pop(dedupe_key, None)
            self._pending_dedupe.pop(dedupe_key, None)
        if pending.state_key is not None and pending.state_signature is not None:
            current = self._state_seen.get(pending.state_key)
            if (current is not None and current[0] == pending.state_signature
                    and current[1] == pending.state_admitted_monotonic):
                self._state_seen.pop(pending.state_key, None)

    def _take_batch_locked(self) -> list[_Pending]:
        batch: list[_Pending] = []
        while self._queue and len(batch) < self.batch_size:
            token = self._queue.popleft()
            pending = self._pending.pop(token, None)
            if pending is None:
                continue
            if (pending.aggregate_key is not None
                    and self._aggregate_tokens.get(pending.aggregate_key) == token):
                self._aggregate_tokens.pop(pending.aggregate_key, None)
            for dedupe_key in pending.dedupe_keys:
                if self._pending_dedupe.get(dedupe_key) == token:
                    self._pending_dedupe.pop(dedupe_key, None)
            batch.append(pending)
        if batch:
            self._inflight_batches += 1
        return batch

    def _run(self) -> None:
        next_heartbeat = time.monotonic()
        while True:
            with self._condition:
                now = time.monotonic()
                if now >= next_heartbeat:
                    self._last_heartbeat_ts_ms = int(time.time() * 1_000)
                    next_heartbeat = now + self.heartbeat_interval_s

                while not self._queue and not self._stop_requested:
                    wait_s = max(0.001, min(
                        self.heartbeat_interval_s,
                        next_heartbeat - time.monotonic(),
                    ))
                    self._condition.wait(wait_s)
                    now = time.monotonic()
                    if now >= next_heartbeat:
                        self._last_heartbeat_ts_ms = int(time.time() * 1_000)
                        next_heartbeat = now + self.heartbeat_interval_s

                if self._stop_requested and (
                        not self._drain_on_stop or not self._queue):
                    break

                # Cooperative deadline backoff: if the previous dispatch hit a
                # deadline-exceeded failure, defer the next flush until the
                # backoff window expires rather than resubmitting into a still-
                # stuck writer (which would only produce another miss + drops).
                # A stop request or an explicit flush always overrides it.
                if (self._deadline_backoff_until > 0.0
                        and not self._stop_requested
                        and not self._flush_requested):
                    remaining_backoff = self._deadline_backoff_until - time.monotonic()
                    if remaining_backoff > 0.0:
                        self._condition.wait(min(
                            remaining_backoff, self.heartbeat_interval_s))
                        if time.monotonic() >= next_heartbeat:
                            self._last_heartbeat_ts_ms = int(time.time() * 1_000)
                            next_heartbeat = time.monotonic() + self.heartbeat_interval_s
                        self._flush_requested = False
                        batch = self._take_batch_locked()
                        if batch:
                            self._dispatch(batch)
                        continue
                    self._deadline_backoff_until = 0.0
                    self._deadline_backoff_s = 0.0

                if self._queue and not self._stop_requested and not self._flush_requested:
                    first = self._pending.get(self._queue[0])
                    deadline = (
                        first.admitted_monotonic + self.flush_interval_s
                        if first is not None else time.monotonic()
                    )
                    while (len(self._queue) < self.batch_size
                           and not self._stop_requested
                           and not self._flush_requested):
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self._condition.wait(min(remaining, self.heartbeat_interval_s))
                        if time.monotonic() >= next_heartbeat:
                            self._last_heartbeat_ts_ms = int(time.time() * 1_000)
                            next_heartbeat = time.monotonic() + self.heartbeat_interval_s

                self._flush_requested = False
                batch = self._take_batch_locked()

            if batch:
                self._dispatch(batch)

        # A physical telemetry sink may lazily create its SQLite connection on
        # this aggregation thread.  Close it here, on the same owner thread,
        # before advertising STOPPED; never leak or cross-close that connection.
        close_sink = getattr(
            self._persistence_writer, "close_telemetry_sink", None)
        if callable(close_sink):
            try:
                close_sink()
            except Exception as exc:  # noqa: BLE001 - shutdown remains observable
                with self._condition:
                    self._last_error = (
                        f"telemetry_sink_close:{type(exc).__name__}:{exc}"
                    )[:240]
                    self._last_failure_ts_ms = int(time.time() * 1_000)
                    self._failed_batches += 1

        with self._condition:
            self._health = "STOPPED"
            self._last_heartbeat_ts_ms = int(time.time() * 1_000)
            self._condition.notify_all()

    def _dispatch(self, batch: list[_Pending]) -> None:
        started = time.monotonic()
        written = 0
        logical_written = 0
        successful_batches = 0
        error = ""
        priority_skip = False
        deadline_exceeded = False
        failed_rows: list[_Pending] = []
        cursor = 0
        while cursor < len(batch):
            chunk = batch[cursor:cursor + self.physical_batch_size]
            payload = [pending.command.payload() for pending in chunk]
            self._physical_batch_high_water = max(
                self._physical_batch_high_water, len(payload))
            try:
                self._batch_attempts += 1
                result = self._persistence_writer.submit_telemetry_batch(
                    payload, timeout_s=self.submit_timeout_s)
                result = self._resolve_result(result)
                chunk_written = self._written_from_result(result, len(payload))
                if chunk_written != len(payload):
                    raise RuntimeError(
                        "physical telemetry writer returned a partial success")
            except Exception as exc:  # noqa: BLE001 - lossy lane is contained
                error = f"{type(exc).__name__}:{exc}"[:240]
                priority_skip = bool(
                    getattr(exc, "telemetry_priority_skip", False))
                deadline_exceeded = bool(
                    getattr(exc, "telemetry_deadline_exceeded", False))
                # The failing sink transaction rolled this chunk back.  Stop
                # immediately; all later chunks remain unwritten and are
                # explicitly accounted as dropped below.
                failed_rows = batch[cursor:]
                break
            written += chunk_written
            logical_written += sum(row.logical_count for row in chunk)
            successful_batches += 1
            cursor += len(chunk)
        success = not failed_rows and cursor == len(batch)
        completed = time.monotonic()
        batch_ms = max(0.0, (completed - started) * 1_000.0)
        flush_ms = max(
            0.0,
            (completed - min(row.admitted_monotonic for row in batch)) * 1_000.0,
        )
        logical_dropped = sum(row.logical_count for row in failed_rows)
        with self._condition:
            self._batch_latencies_ms.append(batch_ms)
            self._flush_latencies_ms.append(flush_ms)
            self._inflight_batches = max(0, self._inflight_batches - 1)
            self._batches += successful_batches
            self._written += written
            self._logical_written += logical_written
            if successful_batches:
                self._last_success_ts_ms = int(time.time() * 1_000)
            if success:
                if self._health not in {"STOPPING", "STOPPED"}:
                    self._health = "HEALTHY"
                    self._last_error = ""
                # A clean commit clears any prior deadline backoff: the writer
                # has recovered and the lossy lane can resume normal cadence.
                self._deadline_backoff_until = 0.0
                self._deadline_backoff_s = 0.0
            else:
                self._failed_batches += 1
                if priority_skip:
                    self._priority_skipped_batches += 1
                    self._priority_skipped_rows += logical_dropped
                    health = "DEGRADED_CRITICAL_PRIORITY"
                elif deadline_exceeded:
                    self._deadline_exceeded_batches += 1
                    self._deadline_exceeded_rows += logical_dropped
                    health = "DEGRADED_TELEMETRY_DEADLINE"
                    # Exponential backoff (capped at 2 s) so a WAL-pinned slow
                    # commit does not cause a tight resubmit-and-miss loop.
                    # The queue keeps accepting; only the next flush is deferred.
                    previous = self._deadline_backoff_s
                    self._deadline_backoff_s = min(
                        2.0, max(self.flush_interval_s, previous * 2.0
                                 if previous > 0.0 else self.flush_interval_s))
                    self._deadline_backoff_until = (
                        time.monotonic() + self._deadline_backoff_s)
                else:
                    health = "DEGRADED_WRITER"
                self._drop_locked(
                    logical_dropped, error or "telemetry_batch_failed", health=health)
                for pending in failed_rows:
                    self._rollback_admission_locked(pending)
            self._last_heartbeat_ts_ms = int(time.time() * 1_000)
            self._condition.notify_all()

    def _resolve_result(self, result: Any) -> Any:
        if isinstance(result, ConcurrentFuture):
            return result.result(timeout=self.submit_timeout_s)
        if isinstance(result, asyncio.Future):
            if not result.done():
                raise TimeoutError("asyncio telemetry future is not complete")
            return result.result()
        if inspect.isawaitable(result):
            async def wait() -> Any:
                return await asyncio.wait_for(result, timeout=self.submit_timeout_s)

            return asyncio.run(wait())
        resolver = getattr(result, "result", None)
        if callable(resolver) and not isinstance(result, Mapping):
            return resolver(timeout=self.submit_timeout_s)
        return result

    @staticmethod
    def _written_from_result(result: Any, default: int) -> int:
        if result is False:
            raise RuntimeError("physical telemetry writer rejected batch")
        if isinstance(result, Mapping):
            if result.get("ok") is False or result.get("success") is False:
                raise RuntimeError(str(result.get("error") or "telemetry batch failed"))
            failed = result.get("failed_count", 0)
            if (isinstance(failed, bool) or not isinstance(failed, int)
                    or failed < 0):
                raise ValueError("physical writer returned invalid failed count")
            if failed:
                raise RuntimeError(f"physical telemetry writer failed {failed} commands")
            for key in ("rows_written", "written", "count"):
                if key in result:
                    value = result[key]
                    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                        raise ValueError("physical writer returned invalid written count")
                    return value
        if isinstance(result, bool) or result is None:
            return default
        if isinstance(result, int):
            if result < 0:
                raise ValueError("physical writer returned negative written count")
            return result
        value = getattr(result, "rows_written", default)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("physical writer returned invalid written count")
        return value

    def flush(self, timeout_s: float = 5.0) -> bool:
        """Request an immediate batch and wait for the telemetry lane to idle."""

        if not math.isfinite(float(timeout_s)) or float(timeout_s) < 0:
            raise ValueError("flush timeout must be finite and non-negative")
        deadline = time.monotonic() + float(timeout_s)
        with self._condition:
            self._flush_requested = True
            self._condition.notify_all()
            while self._queue or self._inflight_batches:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def stop(self, *, drain: bool = True, timeout_s: float = 10.0) -> bool:
        """Stop admission and optionally drain every accepted telemetry command."""

        if type(drain) is not bool:
            raise ValueError("drain must be a strict boolean")
        if not math.isfinite(float(timeout_s)) or float(timeout_s) < 0:
            raise ValueError("stop timeout must be finite and non-negative")
        with self._condition:
            thread = self._thread
            if thread is not None and not thread.is_alive():
                # Idempotent post-stop call: do not overwrite STOPPED with
                # STOPPING after the owner thread has closed its SQLite sink.
                return self._health == "STOPPED" and not self._queue
            if thread is None and drain and self._queue:
                # Prefilled queues still drain on the dedicated worker; the
                # caller thread never invokes the physical persistence writer.
                thread = threading.Thread(
                    target=self._run, name=self.thread_name, daemon=True)
                self._thread = thread
                thread.start()
            self._accepting = False
            if not self._stop_requested:
                self._stop_requested = True
                self._drain_on_stop = drain
            self._health = "STOPPING"
            if not self._drain_on_stop:
                discarded = sum(row.logical_count for row in self._pending.values())
                for pending in self._pending.values():
                    self._rollback_admission_locked(pending)
                self._queue.clear()
                self._pending.clear()
                self._aggregate_tokens.clear()
                self._pending_dedupe.clear()
                if discarded:
                    self._drop_locked(
                        discarded, "telemetry_shutdown_discard",
                        health="STOPPING",
                    )
            self._condition.notify_all()
        if thread is None:
            with self._condition:
                self._health = "STOPPED"
            return not self._queue
        thread.join(float(timeout_s))
        if thread.is_alive():
            with self._condition:
                self._health = "STOPPING_TIMEOUT"
                self._last_error = "telemetry_shutdown_timeout"
            return False
        with self._condition:
            return self._health == "STOPPED" and not self._queue

    def snapshot(self) -> dict[str, Any]:
        """Return a sanitized, internally consistent telemetry health snapshot."""

        with self._condition:
            now_ms = int(time.time() * 1_000)
            batch_values = tuple(self._batch_latencies_ms)
            flush_values = tuple(self._flush_latencies_ms)
            return {
                "health": self._health,
                "queue_depth": len(self._queue),
                "queue_capacity": self.capacity,
                "queue_high_water": self._high_water,
                "inflight_batches": self._inflight_batches,
                "submitted": self._submitted,
                "rows_submitted": self._submitted,
                "coalesced": self._coalesced,
                "rows_coalesced": self._coalesced,
                "deduplicated": self._deduplicated,
                "dropped": self._dropped,
                "rows_dropped": self._dropped,
                "written": self._written,
                "rows_written": self._written,
                "logical_written": self._logical_written,
                "batches": self._batches,
                "batch_attempts": self._batch_attempts,
                "failed_batches": self._failed_batches,
                "priority_skipped_batches": self._priority_skipped_batches,
                "priority_skipped_rows": self._priority_skipped_rows,
                "deadline_exceeded_batches": self._deadline_exceeded_batches,
                "deadline_exceeded_rows": self._deadline_exceeded_rows,
                "physical_batch_size": self.physical_batch_size,
                "physical_batch_high_water": self._physical_batch_high_water,
                "overflow_count": self._overflow_count,
                "batch_latency_avg_ms": self._average(batch_values),
                "batch_latency_p95_ms": self._percentile(batch_values, 0.95),
                "batch_latency_max_ms": max(batch_values, default=0.0),
                "flush_latency_avg_ms": self._average(flush_values),
                "flush_latency_p95_ms": self._percentile(flush_values, 0.95),
                "flush_latency_max_ms": max(flush_values, default=0.0),
                "heartbeat_ts_ms": self._last_heartbeat_ts_ms,
                "heartbeat_age_ms": max(0, now_ms - self._last_heartbeat_ts_ms),
                "last_success_ts_ms": self._last_success_ts_ms or None,
                "last_failure_ts_ms": self._last_failure_ts_ms or None,
                "last_overflow_ts_ms": self._last_overflow_ts_ms or None,
                "deadline_backoff_active": self._deadline_backoff_until > 0.0,
                "deadline_backoff_s": round(self._deadline_backoff_s, 3),
                "last_error": self._last_error or None,
            }

    @staticmethod
    def _average(values: Sequence[float]) -> float:
        return round(sum(values) / len(values), 3) if values else 0.0

    @staticmethod
    def _percentile(values: Sequence[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1)
        return round(float(ordered[max(0, index)]), 3)

    def __enter__(self) -> "V4TelemetryWriter":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop(drain=True)


__all__ = [
    "MergeHook",
    "TelemetryCommand",
    "TelemetryDisposition",
    "TelemetrySink",
    "V4TelemetryWriter",
    "sum_kwargs",
]
