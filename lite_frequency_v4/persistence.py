"""Single-owner, journaled SQLite persistence for Frequency V4.

The writer owns exactly one writable :class:`V4Store` connection on one OS
thread.  Callers exchange immutable commands and concurrent futures; a future
is completed only after the command's database transaction commits.
"""
from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
import copy
from dataclasses import asdict, dataclass, field, is_dataclass
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Iterable, Mapping, Optional
import uuid

from .store import V4Store, V4StoreError


_SECRET_KEY_RE = re.compile(
    r"(?i)(private_key|api_secret|api_key|passphrase|password|bot_token|"
    r"auth_header|signed_payload|wallet|cookie)"
)

TELEMETRY_BATCH_METHOD = "__telemetry_batch__"
TERMINAL_METHODS = frozenset({
    "close_position", "management_bundle", "resolution_bundle",
    "mark_unresolved_final", "end_runtime_session",
})
TELEMETRY_METHODS = frozenset({
    "record_event_count", "record_event_count_batch", "record_reject",
    "record_latency", "record_source_health", "record_runtime_health",
    "update_window_funnel", "record_source_event", "record_cex_observation",
    "record_book_snapshot", "record_model_contribution",
})
ALLOWED_STORE_METHODS = frozenset({
    "record_runtime_session", "end_runtime_session", "ensure_cohort",
    "upsert_market",
    "record_market_identity", "ensure_asset_window", "link_window_market",
    "record_anchor_observation", "update_window_funnel",
    "record_event_count", "record_event_count_batch", "record_source_event",
    "record_book_snapshot", "record_cex_observation", "record_candidate",
    "link_candidate_book", "link_candidate_cex", "record_model_contribution",
    "record_fair_value", "record_decision", "reserve_window",
    "release_window_reservation", "create_entry",
    "reserve_and_create_entry_bundle", "record_maker_observation",
    "record_maker_update", "finish_maker_observation",
    "record_management_decision", "management_bundle",
    "record_resolution_attempt", "resolution_bundle", "close_position",
    "mark_unresolved_final", "record_reject", "record_latency",
    "record_source_health", "record_runtime_health",
    "record_compounding_preview", "pin_source_event",
    "persist_market_bundle", "persist_evaluation_bundle",
    "reconcile_startup_state",
    "compact_raw_evidence", "enforce_raw_row_cap", "compact_event_buckets",
})
# Methods whose unconfirmed state makes new trade execution unsafe: locks and
# reservations, idempotency-bearing entries, position transitions, exits,
# resolution/accounting, maker execution evidence, and session accounting.
# Evidence-only bundles (evaluations, market discovery, telemetry) are fully
# materialized in their authoritative tables and must not gate entries.
TRADE_CRITICAL_METHODS = TERMINAL_METHODS | frozenset({
    "reserve_window", "release_window_reservation", "create_entry",
    "reserve_and_create_entry_bundle", "ensure_asset_window",
    "record_maker_observation", "record_maker_update",
    "finish_maker_observation", "record_management_decision",
    "record_resolution_attempt", "record_runtime_session",
    "reconcile_startup_state",
})
# High-volume evidence command types whose journal payload is a redundant
# serialization of rows the same transaction writes to authoritative evidence
# tables.  Their payloads are tombstoned atomically WITH the durable commit;
# pending/failed payloads are never touched, and idempotency (payload_hash),
# identity, status, timestamps, references, and error metadata all remain.
COMPACT_ON_COMMIT_COMMAND_TYPES = frozenset({
    "ENTRY_DECISION_EVIDENCE", "MARKET_DISCOVERY",
})


def _is_trade_critical(command: "V4PersistenceCommand") -> bool:
    return bool(command.terminal) or command.method in TRADE_CRITICAL_METHODS


class V4PersistenceError(V4StoreError):
    """Base class for fail-closed writer errors."""


class V4PersistenceQueueFull(V4PersistenceError):
    """The bounded writer queue refused a command."""


class V4PersistenceTimeout(V4PersistenceError):
    """The caller's acknowledgement deadline expired."""


class V4PersistenceIdempotencyConflict(V4PersistenceError):
    """One command/idempotency key was reused for different content."""


class V4PersistenceCommandFailed(V4PersistenceError):
    """A command failed without committing its target mutation."""


class V4TelemetryPrioritySkip(V4PersistenceError):
    """Lossy telemetry was skipped because critical persistence had priority."""

    telemetry_priority_skip = True


class V4TelemetryDeadlineExceeded(V4PersistenceError):
    """A lossy telemetry transaction exceeded its short cooperative budget."""

    telemetry_deadline_exceeded = True


class V4TelemetryWriteContention(V4PersistenceError):
    """Telemetry could not take the write lock within its short busy timeout.

    Another writer -- normally the maintenance worker holding the lock for a
    WAL checkpoint -- owned it.  Nothing was written and the transaction never
    opened, so this is a deferral exactly like a priority skip, not a fault.
    It carries ``telemetry_priority_skip`` so the aggregator requeues the rows
    instead of destroying them and counting a batch failure; a bounded
    checkpoint otherwise showed up as a telemetry outage on every cycle.
    """

    telemetry_priority_skip = True


class V4TelemetryMaintenanceDeferral(V4TelemetryPrioritySkip):
    """Telemetry yielded before opening a transaction so maintenance can run."""

    telemetry_priority_skip = True
    telemetry_checkpoint_deferral = True


class V4TelemetryBatchTooLarge(V4PersistenceError):
    """The physical telemetry sink refused an oversized transaction."""

    telemetry_batch_rejected = True


def _is_lock_contention(exc: BaseException) -> bool:
    """True for a SQLite busy/locked error, i.e. another writer held the lock."""

    text = str(exc).lower()
    return "locked" in text or "busy" in text


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _jsonable(value: Any, path: str = "") -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value), path)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, nested in value.items():
            name = str(key)
            field_path = f"{path}.{name}" if path else name
            if _SECRET_KEY_RE.search(name):
                raise ValueError(f"refusing secret-like persistence field {field_path!r}")
            result[name] = _jsonable(nested, field_path)
        return result
    if isinstance(value, (tuple, list)):
        return [_jsonable(item, f"{path}[]") for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "value"):
        return _jsonable(value.value, path)
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite persistence command value")
        return value
    raise TypeError(f"unsupported persistence command value: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def _hash_payload(payload_json: str) -> str:
    import hashlib
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class V4PersistenceCommand:
    """One allowlisted persistence operation.

    ``ordering_key`` should be the exact asset/window key for stateful strategy
    commands.  The scheduler never changes FIFO order within that key.
    """

    command_id: str
    method: str
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    ordering_key: str = "global"
    command_type: str = "STORE_CALL"
    idempotency_key: Optional[str] = None
    priority: int = 50
    terminal: bool = False
    associated_asset: Optional[str] = None
    associated_window_id: Optional[int] = None
    associated_trade_id: Optional[int] = None
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if not self.command_id or not self.method or not self.ordering_key:
            raise ValueError("persistence command identity is required")
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", self.command_id):
            raise ValueError("invalid persistence command_id")
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,240}", self.ordering_key):
            raise ValueError("invalid persistence ordering_key")
        if isinstance(self.priority, bool) or not 0 <= int(self.priority) <= 10_000:
            raise ValueError("invalid persistence priority")
        if isinstance(self.max_attempts, bool) or not 1 <= int(self.max_attempts) <= 10:
            raise ValueError("max_attempts must be in [1,10]")
        # A frozen dataclass is only shallowly immutable.  Snapshot every
        # nested value here, then the writer snapshots once more at admission,
        # so caller-owned dictionaries/lists can never race journal hashing or
        # dispatch after ``submit`` returns.
        object.__setattr__(self, "args", tuple(copy.deepcopy(tuple(self.args))))
        object.__setattr__(self, "kwargs", copy.deepcopy(dict(self.kwargs)))
        object.__setattr__(self, "idempotency_key",
                           str(self.idempotency_key or self.command_id))
        if self.terminal or self.method in TERMINAL_METHODS:
            object.__setattr__(self, "terminal", True)
            object.__setattr__(self, "priority", min(int(self.priority), 10))
        # Validate journal serialization and secret rejection at construction.
        _canonical_json(self.payload)

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "args": self.args,
            "kwargs": self.kwargs,
        }


@dataclass(slots=True)
class _Envelope:
    command: V4PersistenceCommand
    future: Future[Any]
    sequence: int
    enqueued_ts_ms: int


class _BoundedPriorityScheduler:
    """Priority across keys, strict FIFO within every ordering key."""

    def __init__(self, capacity: int):
        if isinstance(capacity, bool) or not 1 <= int(capacity) <= 1_000_000:
            raise ValueError("writer queue capacity must be in [1,1000000]")
        self.capacity = int(capacity)
        self._condition = threading.Condition()
        self._by_key: dict[str, deque[_Envelope]] = {}
        self._size = 0
        self._closing = False
        self.high_water = 0

    def put(self, envelope: _Envelope) -> None:
        with self._condition:
            if self._closing:
                raise V4PersistenceError("persistence writer is closing")
            if self._size >= self.capacity:
                raise V4PersistenceQueueFull("persistence writer queue is full")
            queue = self._by_key.setdefault(envelope.command.ordering_key, deque())
            queue.append(envelope)
            self._size += 1
            self.high_water = max(self.high_water, self._size)
            self._condition.notify()

    def get(self, timeout_s: float = 0.5) -> Optional[_Envelope | object]:
        with self._condition:
            if self._size == 0 and not self._closing:
                self._condition.wait(timeout=max(0.01, float(timeout_s)))
                if self._size == 0 and not self._closing:
                    return _IDLE
            if self._size == 0 and self._closing:
                return None
            # Only each ordering key's head is eligible.  A later terminal
            # command can overtake other keys, never its own earlier mutation.
            selected = min(
                (rows[0] for rows in self._by_key.values() if rows),
                key=lambda envelope: (
                    int(envelope.command.priority), envelope.sequence),
            )
            rows = self._by_key[selected.command.ordering_key]
            assert rows.popleft() is selected
            if not rows:
                del self._by_key[selected.command.ordering_key]
            self._size -= 1
            return selected

    def fail_all(self, exc: BaseException) -> None:
        with self._condition:
            rows = [row for queue in self._by_key.values() for row in queue]
            self._by_key.clear()
            self._size = 0
            self._closing = True
            self._condition.notify_all()
        for row in rows:
            if not row.future.done():
                row.future.set_exception(exc)

    def close(self) -> None:
        with self._condition:
            self._closing = True
            self._condition.notify_all()

    @property
    def depth(self) -> int:
        with self._condition:
            return self._size

    @property
    def oldest_age_ms(self) -> int:
        with self._condition:
            if not self._size:
                return 0
            oldest = min(row.enqueued_ts_ms for rows in self._by_key.values() for row in rows)
        return max(0, _now_ms() - oldest)


_IDLE = object()


class _CriticalFirstWriteGate:
    """Coordinate separate SQLite writers without sacrificing critical priority.

    Critical commands register before entering the scheduler.  Telemetry is
    admitted only when no critical command is queued or active and acquires the
    gate without waiting.  A critical command that arrives during an already
    admitted short telemetry transaction prevents every subsequent telemetry
    batch, so telemetry cannot continually overtake or starve it.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.Lock())
        self._critical_pending = 0
        self._critical_active = False
        self._telemetry_active = False
        self._maintenance_waiting = 0
        self._maintenance_active = False
        self._telemetry_priority_skips = 0
        self._telemetry_maintenance_deferrals = 0
        self._maintenance_deferrals = 0

    def register_critical(self) -> None:
        with self._condition:
            self._critical_pending += 1
            self._condition.notify_all()

    def cancel_critical(self) -> None:
        with self._condition:
            self._critical_pending = max(0, self._critical_pending - 1)
            self._condition.notify_all()

    def acquire_critical(self) -> None:
        with self._condition:
            # Maintenance is admitted only when no critical work exists, but
            # once a PASSIVE checkpoint has started it must not invert
            # priority and hold a later critical command behind a potentially
            # multi-second scan.  SQLite coordinates the separate connections;
            # telemetry remains mutually exclusive because it owns an actual
            # write transaction.
            while self._critical_active or self._telemetry_active:
                self._condition.wait()
            self._critical_active = True

    def release_critical(self) -> None:
        with self._condition:
            if not self._critical_active:
                raise RuntimeError("critical write gate released while inactive")
            self._critical_active = False
            self._critical_pending = max(0, self._critical_pending - 1)
            self._condition.notify_all()

    def try_acquire_telemetry_reason(self) -> Optional[str]:
        with self._condition:
            if (self._critical_pending > 0 or self._critical_active
                    or self._telemetry_active):
                self._telemetry_priority_skips += 1
                return "critical_persistence_pending"
            if self._maintenance_waiting > 0 or self._maintenance_active:
                self._telemetry_priority_skips += 1
                self._telemetry_maintenance_deferrals += 1
                return "maintenance_pending"
            self._telemetry_active = True
            return None

    def try_acquire_telemetry(self) -> bool:
        return self.try_acquire_telemetry_reason() is None

    def critical_pending(self) -> bool:
        with self._condition:
            return self._critical_pending > 0 or self._critical_active

    def release_telemetry(self) -> None:
        with self._condition:
            if not self._telemetry_active:
                raise RuntimeError("telemetry write gate released while inactive")
            self._telemetry_active = False
            self._condition.notify_all()

    def acquire_maintenance(self, timeout_s: float = 0.250) -> bool:
        """Boundedly wait for admitted telemetry, while never delaying critical."""

        timeout = max(0.0, float(timeout_s))
        deadline = time.monotonic() + timeout
        with self._condition:
            if (self._critical_pending > 0 or self._critical_active
                    or self._maintenance_active):
                self._maintenance_deferrals += 1
                # Retain the historical aggregate low-priority skip counter
                # while exposing maintenance-specific accounting separately.
                self._telemetry_priority_skips += 1
                return False
            self._maintenance_waiting += 1
            try:
                while self._telemetry_active:
                    if self._critical_pending > 0 or self._critical_active:
                        self._maintenance_deferrals += 1
                        self._telemetry_priority_skips += 1
                        return False
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._maintenance_deferrals += 1
                        self._telemetry_priority_skips += 1
                        return False
                    self._condition.wait(remaining)
                if self._critical_pending > 0 or self._critical_active:
                    self._maintenance_deferrals += 1
                    self._telemetry_priority_skips += 1
                    return False
                self._maintenance_active = True
                return True
            finally:
                self._maintenance_waiting = max(
                    0, self._maintenance_waiting - 1)
                self._condition.notify_all()

    def release_maintenance(self) -> None:
        with self._condition:
            if not self._maintenance_active:
                raise RuntimeError(
                    "maintenance write gate released while inactive")
            self._maintenance_active = False
            self._condition.notify_all()

    def reset_critical(self) -> None:
        """Release fail-closed accounting when the critical worker terminates."""

        with self._condition:
            self._critical_pending = 0
            self._critical_active = False
            self._condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            return {
                "critical_pending": self._critical_pending,
                "critical_inflight": int(self._critical_active),
                "telemetry_inflight": int(self._telemetry_active),
                "maintenance_waiting": self._maintenance_waiting,
                "maintenance_inflight": int(self._maintenance_active),
                "telemetry_priority_skips": self._telemetry_priority_skips,
                "telemetry_maintenance_deferrals": (
                    self._telemetry_maintenance_deferrals),
                "maintenance_deferrals": self._maintenance_deferrals,
            }


class V4PersistenceWriter:
    """Dedicated, bounded, journaled V4 SQLite writer."""

    def __init__(self, db_path: str | Path, *, queue_capacity: int = 10_000,
                 busy_timeout_ms: int = 10_000, sample_interval_s: float = 1.0,
                 checkpoint_on_close: bool = True,
                 current_launch_nonce: Optional[str] = None,
                 proven_absent_launch_nonces: Iterable[str] = ()):
        self.db_path = Path(db_path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        self.sample_interval_s = max(0.0, float(sample_interval_s))
        if type(checkpoint_on_close) is not bool:
            raise ValueError("checkpoint_on_close must be a strict boolean")
        self.checkpoint_on_close = checkpoint_on_close
        self.current_launch_nonce = str(current_launch_nonce or "") or None
        self.proven_absent_launch_nonces = tuple(sorted({
            str(value) for value in proven_absent_launch_nonces if str(value)
        }))
        if (self.current_launch_nonce is not None
                and self.current_launch_nonce in self.proven_absent_launch_nonces):
            raise ValueError("current launch nonce cannot be proven absent")
        self._scheduler = _BoundedPriorityScheduler(queue_capacity)
        self._write_gate = _CriticalFirstWriteGate()
        self._thread: Optional[threading.Thread] = None
        self._ready: Future[bool] = Future()
        self._lifecycle_lock = threading.Lock()
        self._sequence = 0
        self._sequence_lock = threading.Lock()
        # Set only for the duration of an explicitly requested quiescent window
        # (the audit snapshot copy).  It suppresses the periodic diagnostic
        # sample and nothing else; critical evidence is never affected.
        self._quiescent_window = threading.Event()
        self._metrics_lock = threading.Lock()
        self._metrics: dict[str, Any] = {
            "state": "NEW", "worker_thread_id": 0,
            "commands_submitted": 0, "commands_committed": 0,
            "commands_failed": 0, "commands_retried": 0,
            "idempotent_replays": 0, "recovered_abandoned": 0,
            "queue_full_count": 0, "timeout_count": 0,
            "terminal_submitted": 0, "terminal_committed": 0,
            "last_commit_ts_ms": 0, "last_committed_command_id": "",
            "heartbeat_ts_ms": 0, "transaction_rate_per_min": 0.0,
            "unconfirmed_command_count": 0,
            "unconfirmed_trade_critical_count": 0,
            "journal_payloads_compacted_on_commit": 0,
            "last_error": "",
            "journal_finalization_failures": 0,
            "unfinished_maker_observations": 0,
            "reconciled_abandoned_maker_observations": 0,
            "unfinished_makers_left_fail_closed": 0,
            "recovery_reconciliation": {},
        }
        self._started_mono = time.monotonic()
        self._latency_samples: dict[str, deque[float]] = {
            "commit": deque(maxlen=1_024), "queue": deque(maxlen=1_024),
            "ack": deque(maxlen=1_024),
        }
        self._critical_submit_times: deque[float] = deque(maxlen=4_096)
        self._critical_commit_times: deque[float] = deque(maxlen=4_096)
        self._trade_critical_submit_times: deque[float] = deque(maxlen=4_096)
        self._trade_critical_commit_times: deque[float] = deque(maxlen=4_096)
        self._telemetry_sink: Optional[V4TelemetryStoreSink] = None
        self._telemetry_sink_lock = threading.Lock()
        self.telemetry_max_transaction_ms = 250.0
        # Trade-critical commands accepted by the scheduler but not yet
        # journal-admitted.  Guarded by _metrics_lock; the exposed
        # unconfirmed_trade_critical_count is this queue-side hold plus the
        # journal-side counter, so the execution gate never sees a gap while
        # a trade-critical command is anywhere in flight.
        self._queued_trade_critical = 0
        # command_id -> (enqueued_ts_ms, trade_critical) for every command that
        # has been accepted but not yet resolved (committed, idempotently
        # replayed, or finalized as FAILED).  Insertion order is submission
        # order, so the exposed "oldest unconfirmed age" is exact.  A command
        # that is merely in flight is not evidence of a persistence fault; a
        # command still unconfirmed past its acknowledgement deadline is.  The
        # execution gate needs that distinction to stay fail-closed on genuine
        # faults without latching on ordinary pipelining.
        self._inflight_commands: dict[str, tuple[int, bool]] = {}

    def start(self, timeout_s: float = 10.0) -> None:
        with self._lifecycle_lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, name="lite-frequency-v4-persistence",
                    daemon=False)
                self._thread.start()
        self._ready.result(timeout=timeout_s)

    def submit(self, command: V4PersistenceCommand) -> Future[Any]:
        if not isinstance(command, V4PersistenceCommand):
            raise TypeError("V4PersistenceCommand is required")
        if command.method not in ALLOWED_STORE_METHODS | {TELEMETRY_BATCH_METHOD}:
            raise V4PersistenceError(f"persistence method is not allowlisted: {command.method}")
        # Freeze immediately on entry, before a potentially slow first worker
        # startup, so even concurrent caller mutation has the smallest possible
        # race window and can never occur after admission returns.
        command_snapshot = copy.deepcopy(command)
        _canonical_json(command_snapshot.payload)
        if self._thread is None:
            self.start()
        self._ready.result(timeout=10.0)
        future: Future[Any] = Future()
        with self._sequence_lock:
            sequence = self._sequence
            self._sequence += 1
        envelope = _Envelope(command_snapshot, future, sequence, _now_ms())
        self._write_gate.register_critical()
        try:
            self._scheduler.put(envelope)
        except V4PersistenceQueueFull:
            self._write_gate.cancel_critical()
            with self._metrics_lock:
                self._metrics["queue_full_count"] += 1
            raise
        except Exception:
            self._write_gate.cancel_critical()
            raise
        trade_critical = _is_trade_critical(command_snapshot)
        with self._metrics_lock:
            self._metrics["commands_submitted"] += 1
            self._metrics["terminal_submitted"] += int(command_snapshot.terminal)
            submitted_mono = time.monotonic()
            self._critical_submit_times.append(submitted_mono)
            if trade_critical:
                self._queued_trade_critical += 1
                self._trade_critical_submit_times.append(submitted_mono)
            self._inflight_commands[command_snapshot.command_id] = (
                envelope.enqueued_ts_ms, trade_critical)
        return future

    async def execute(self, command: V4PersistenceCommand,
                      timeout_s: Optional[float] = None) -> Any:
        future = self.submit(command)
        wrapped = asyncio.wrap_future(future)
        try:
            if timeout_s is None:
                return await asyncio.shield(wrapped)
            return await asyncio.wait_for(asyncio.shield(wrapped), timeout=float(timeout_s))
        except asyncio.TimeoutError as exc:
            with self._metrics_lock:
                self._metrics["timeout_count"] += 1
            # The shielded command deliberately continues so the journal can
            # establish its durable outcome.  Consume that late asyncio result
            # (the writer records exact finalized loss independently) to avoid
            # an unobserved-future warning without pretending the timed-out
            # caller received an acknowledgement.
            wrapped.add_done_callback(
                lambda completed: completed.exception()
                if not completed.cancelled() else None)
            timeout_error = V4PersistenceTimeout(
                f"persistence acknowledgement timed out: {command.command_id}")
            timeout_error.persistence_late_future = wrapped
            raise timeout_error from exc

    def execute_sync(self, command: V4PersistenceCommand,
                     timeout_s: Optional[float] = None) -> Any:
        future = self.submit(command)
        try:
            return future.result(timeout=timeout_s)
        except FutureTimeoutError as exc:
            with self._metrics_lock:
                self._metrics["timeout_count"] += 1
            raise V4PersistenceTimeout(
                f"persistence acknowledgement timed out: {command.command_id}") from exc

    def submit_telemetry_batch(
        self, calls: Iterable[V4PersistenceCommand | Mapping[str, Any]], *,
        command_id: Optional[str] = None,
        timeout_s: Optional[float] = None,
    ) -> list[Any]:
        """Synchronously submit low-priority telemetry as one DB transaction."""

        # The telemetry aggregation thread owns this second physical connection;
        # telemetry can therefore never wait behind or checkpoint the critical
        # lifecycle writer connection.
        with self._telemetry_sink_lock:
            if self._telemetry_sink is None:
                self._telemetry_sink = V4TelemetryStoreSink(
                    self.db_path,
                    # Telemetry is intentionally lossy; a bounded short busy
                    # timeout prevents an unrelated external lock from making
                    # a newly-arrived critical command wait behind telemetry.
                    busy_timeout_ms=100,
                    max_transaction_ms=self.telemetry_max_transaction_ms,
                    write_gate=self._write_gate)
            sink = self._telemetry_sink
        return sink.submit_telemetry_batch(
            calls, command_id=command_id, timeout_s=timeout_s)

    submit_telemetry_batch_sync = submit_telemetry_batch

    def telemetry_transaction_budget_ms(self) -> float:
        """Expose the physical sink's cooperative transaction budget."""

        return self.telemetry_max_transaction_ms

    def telemetry_commit_metrics(self) -> dict[str, Any]:
        """Return the latest sink-owned transaction timing sample."""

        with self._telemetry_sink_lock:
            if self._telemetry_sink is None:
                return {
                    "last_transaction_duration_ms": 0.0,
                    "last_duration_ms": 0.0,
                }
            return self._telemetry_sink.health()

    def critical_write_pending(self) -> bool:
        """Thread-safe callback for non-critical worker admission checks."""

        return self._write_gate.critical_pending()

    def try_acquire_background_write(self) -> bool:
        """Bounded maintenance/background admission on the shared gate.

        A maintenance owner thread should call this immediately before opening
        a write transaction and call :meth:`release_background_write` in a
        ``finally`` block.  ``False`` means it must skip/defer the pass.
        Maintenance may wait briefly for an already-admitted short telemetry
        transaction, but never waits when critical work is queued or active.
        """

        return self._write_gate.acquire_maintenance(timeout_s=0.250)

    def release_background_write(self) -> None:
        """Release a successful background-write admission."""

        self._write_gate.release_maintenance()

    def close_telemetry_sink(self) -> bool:
        """Close the lazy telemetry connection on its aggregation thread.

        The critical writer thread and the telemetry aggregation thread own
        different physical SQLite connections.  Consequently the critical
        writer's :meth:`close` must never attempt to cross-close this sink;
        ``V4TelemetryWriter`` invokes this hook before its own thread exits.
        """

        with self._telemetry_sink_lock:
            sink = self._telemetry_sink
            if sink is None:
                return False
            sink.close()
            self._telemetry_sink = None
            return True

    @staticmethod
    def _latency_summary(rows: deque[float]) -> dict[str, float]:
        if not rows:
            return {"avg": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0}
        ordered = sorted(rows)
        def percentile(fraction: float) -> float:
            index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * fraction)))
            return float(ordered[index])
        return {
            "avg": sum(ordered) / len(ordered), "p50": percentile(0.50),
            "p95": percentile(0.95), "max": float(ordered[-1]),
        }

    def begin_quiescent_window(self) -> None:
        """Suppress the periodic diagnostic sample until the window ends.

        Requested only around the audit snapshot copy, which cannot converge
        while any connection writes the source.  Queued commands continue to be
        processed and committed; only the 1 Hz ``persistence_worker_samples``
        row is withheld, and the count withheld is published.
        """

        self._quiescent_window.set()

    def end_quiescent_window(self) -> None:
        self._quiescent_window.clear()

    @property
    def quiescent_window_active(self) -> bool:
        return self._quiescent_window.is_set()

    def metrics(self) -> dict[str, Any]:
        with self._metrics_lock:
            current_mono = time.monotonic()
            rate_cutoff = current_mono - 15.0
            for rows in (
                self._critical_submit_times, self._critical_commit_times,
                self._trade_critical_submit_times,
                self._trade_critical_commit_times,
            ):
                while rows and rows[0] < rate_cutoff:
                    rows.popleft()
            result = dict(self._metrics)
            latency = {
                name: self._latency_summary(rows)
                for name, rows in self._latency_samples.items()
            }
            rate_elapsed_s = min(
                15.0, max(1.0, current_mono - self._started_mono))
            current_rates = {
                "critical_rows_submitted_per_second": round(
                    len(self._critical_submit_times) / rate_elapsed_s, 4),
                "critical_rows_committed_per_second": round(
                    len(self._critical_commit_times) / rate_elapsed_s, 4),
                "trade_critical_rows_submitted_per_second": round(
                    len(self._trade_critical_submit_times) / rate_elapsed_s, 4),
                "trade_critical_rows_committed_per_second": round(
                    len(self._trade_critical_commit_times) / rate_elapsed_s, 4),
                "critical_rate_window_seconds": 15,
            }
        queue_depth = self._scheduler.depth
        gate = self._write_gate.snapshot()
        now = _now_ms()
        with self._metrics_lock:
            queued_trade_critical = self._queued_trade_critical
            oldest_any: Optional[int] = None
            oldest_critical: Optional[int] = None
            for enqueued_ts_ms, trade_critical in self._inflight_commands.values():
                age = max(0, now - int(enqueued_ts_ms))
                if oldest_any is None or age > oldest_any:
                    oldest_any = age
                if trade_critical and (
                        oldest_critical is None or age > oldest_critical):
                    oldest_critical = age
            inflight_registered = len(self._inflight_commands)
        result.update({
            "queue_depth": queue_depth,
            "queue_capacity": self._scheduler.capacity,
            "queue_high_water": self._scheduler.high_water,
            "oldest_queue_age_ms": self._scheduler.oldest_age_ms,
            "commit_latency_ms": latency["commit"],
            "queue_latency_ms": latency["queue"],
            "ack_latency_ms": latency["ack"],
            "unconfirmed_command_count": (
                int(result.get("unconfirmed_command_count") or 0) + queue_depth),
            "unconfirmed_trade_critical_count": (
                int(result.get("unconfirmed_trade_critical_count") or 0)
                + queued_trade_critical),
            # Age of the oldest command that is accepted but not yet resolved.
            # ``None`` means nothing is in flight.  Consumers gate on the age,
            # not the bare count: an in-flight command is normal pipelining,
            # an overdue one is an unresolved critical command.
            "unconfirmed_command_oldest_age_ms": oldest_any,
            "unconfirmed_trade_critical_oldest_age_ms": oldest_critical,
            "inflight_registered_commands": inflight_registered,
            **current_rates,
            **gate,
        })
        return result

    def health(self, *, stale_after_ms: int = 5_000) -> dict[str, Any]:
        result = self.metrics()
        heartbeat = int(result.get("heartbeat_ts_ms") or 0)
        age = max(0, _now_ms() - heartbeat) if heartbeat else 0
        raw_state = str(result.get("state") or "NEW")
        if raw_state == "RUNNING":
            healthy = bool(self._thread and self._thread.is_alive() and age <= stale_after_ms)
            state = "HEALTHY" if healthy else "FAILED"
        else:
            state = raw_state
            healthy = False
        result.update({
            "state": state, "raw_state": raw_state, "healthy": healthy,
            "heartbeat_age_ms": age,
        })
        return result

    def close(self, timeout_s: float = 15.0) -> None:
        if self._thread is None:
            return
        self._scheduler.close()
        self._thread.join(timeout=float(timeout_s))
        if self._thread.is_alive():
            raise V4PersistenceTimeout("persistence writer did not stop after draining")

    async def aclose(self, timeout_s: float = 15.0) -> None:
        await asyncio.to_thread(self.close, timeout_s)

    def __enter__(self) -> "V4PersistenceWriter":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def _command_payload(command: V4PersistenceCommand) -> tuple[str, str]:
        payload_json = _canonical_json(command.payload)
        return payload_json, _hash_payload(payload_json)

    def _recover_incomplete(self, store: V4Store) -> None:
        """Fail closed on crash-left commands; never synthesize an old entry."""

        current = _now_ms()
        with store.transaction(immediate=True) as conn:
            rows = conn.execute(
                "SELECT command_id FROM persistence_commands "
                "WHERE status IN ('SUBMITTED','EXECUTING')"
            ).fetchall()
            if rows:
                conn.execute(
                    """UPDATE persistence_commands SET status='FAILED',
                       completed_ts_ms=CASE WHEN submitted_ts_ms > ?
                           THEN submitted_ts_ms ELSE ? END,
                       error_type='CrashRecovery',
                       error='uncommitted command not replayed after process restart'
                       WHERE status IN ('SUBMITTED','EXECUTING')""",
                    (current, current),
                )
        with self._metrics_lock:
            self._metrics["recovered_abandoned"] += len(rows)
            self._metrics["unconfirmed_command_count"] = 0
            self._metrics["unconfirmed_trade_critical_count"] = 0

    def _existing_result(self, store: V4Store, command: V4PersistenceCommand,
                         payload_hash: str) -> tuple[bool, Any]:
        row = store.query_one(
            "SELECT * FROM persistence_commands WHERE command_id=? OR idempotency_key=?",
            (command.command_id, command.idempotency_key),
        )
        if row is None:
            return False, None
        if (str(row["payload_hash"]) != payload_hash
                or str(row["method"]) != command.method):
            raise V4PersistenceIdempotencyConflict(
                f"persistence idempotency conflict: {command.command_id}")
        if row["status"] == "COMMITTED":
            return True, json.loads(row["result_json"] or "null")
        if row["status"] == "FAILED":
            raise V4PersistenceCommandFailed(
                f"previous persistence command failed: {row.get('error') or command.command_id}")
        raise V4PersistenceCommandFailed(
            f"incomplete persistence command is fail-closed: {command.command_id}")

    def _journal_submitted(self, store: V4Store, envelope: _Envelope,
                           payload_json: str, payload_hash: str) -> tuple[bool, Any]:
        command = envelope.command
        existing, result = self._existing_result(store, command, payload_hash)
        if existing:
            return True, result
        with store.transaction(immediate=True) as conn:
            conn.execute(
                """INSERT INTO persistence_commands(
                   command_id,command_type,method,idempotency_key,ordering_key,
                   priority,terminal,associated_asset,associated_window_id,
                   associated_trade_id,payload_hash,payload_json,status,
                   attempt_count,submitted_ts_ms)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?, 'SUBMITTED',0,?)""",
                (command.command_id, command.command_type, command.method,
                 command.idempotency_key, command.ordering_key, int(command.priority),
                 int(command.terminal), command.associated_asset,
                 command.associated_window_id, command.associated_trade_id,
                 payload_hash, payload_json, envelope.enqueued_ts_ms),
            )
        with self._metrics_lock:
            self._metrics["unconfirmed_command_count"] += 1
            if _is_trade_critical(command):
                self._metrics["unconfirmed_trade_critical_count"] += 1
        return False, None

    @staticmethod
    def _mark_executing(store: V4Store, command: V4PersistenceCommand,
                        attempt: int, thread_id: int) -> None:
        with store.transaction(immediate=True) as conn:
            cursor = conn.execute(
                """UPDATE persistence_commands SET status='EXECUTING',
                   started_ts_ms=COALESCE(
                       started_ts_ms,MAX(submitted_ts_ms,?)),attempt_count=?,
                   worker_thread_id=?,error_type=NULL,error=NULL WHERE command_id=?
                   AND status IN ('SUBMITTED','EXECUTING')""",
                (_now_ms(), int(attempt), int(thread_id), command.command_id),
            )
            if cursor.rowcount != 1:
                raise V4PersistenceError(
                    "journal command is no longer executable")

    @staticmethod
    def _dispatch(store: V4Store, command: V4PersistenceCommand) -> Any:
        if command.method == TELEMETRY_BATCH_METHOD:
            calls = tuple(command.args[0])
            results = []
            for call in calls:
                if call.method not in TELEMETRY_METHODS:
                    raise V4PersistenceError(
                        f"non-telemetry method in batch: {call.method}")
                results.append(getattr(store, call.method)(*call.args, **dict(call.kwargs)))
            return results
        if command.method not in ALLOWED_STORE_METHODS:
            raise V4PersistenceError(
                f"persistence method is not allowlisted: {command.method}")
        return getattr(store, command.method)(*command.args, **dict(command.kwargs))

    def _commit_command(self, store: V4Store, command: V4PersistenceCommand,
                        attempt: int, payload_hash: str) -> Any:
        reference = f"v4tx:{command.command_id}:{attempt}"
        # Evidence-only payloads are tombstoned atomically WITH the durable
        # commit: the same transaction has just written the authoritative
        # evidence rows, recovery never reads a COMMITTED payload back, and
        # idempotency continues to verify against the retained payload_hash.
        # Pending/failed rows and every trade-critical payload keep their
        # full payload_json.
        compact = (
            command.command_type in COMPACT_ON_COMMIT_COMMAND_TYPES
            and not command.terminal
            and not _is_trade_critical(command)
            and command.associated_trade_id is None
        )
        payload_clause = ",payload_json=?" if compact else ""
        parameters: list[Any] = []
        with store.transaction(immediate=True) as conn:
            result = self._dispatch(store, command)
            result_json = _canonical_json(result)
            committed = _now_ms()
            if compact:
                parameters.append(json.dumps(
                    {"compacted": True, "payload_hash": payload_hash},
                    sort_keys=True, separators=(",", ":"), allow_nan=False,
                ))
            cursor = conn.execute(
                f"""UPDATE persistence_commands SET status='COMMITTED',
                   committed_ts_ms=MAX(
                       submitted_ts_ms,COALESCE(started_ts_ms,submitted_ts_ms),?),
                   completed_ts_ms=MAX(
                       submitted_ts_ms,COALESCE(started_ts_ms,submitted_ts_ms),?),
                   result_json=?{payload_clause},
                   transaction_reference=?,error_type=NULL,error=NULL
                   WHERE command_id=? AND status='EXECUTING'""",
                (committed, committed, result_json, *parameters, reference,
                 command.command_id),
            )
            if cursor.rowcount != 1:
                raise V4PersistenceError("journal lost executing command ownership")
        if compact:
            with self._metrics_lock:
                self._metrics["journal_payloads_compacted_on_commit"] += 1
        return result

    @staticmethod
    def _mark_failed(store: V4Store, command: V4PersistenceCommand,
                     attempt: int, exc: BaseException) -> bool:
        """Durably finalize a failed command, returning whether it was stored.

        A secondary journal failure is deliberately not raised over the
        original command exception, but it is never treated as confirmation.
        The caller retains the unconfirmed count and degrades writer health.
        """

        try:
            with store.transaction(immediate=True) as conn:
                cursor = conn.execute(
                    """UPDATE persistence_commands SET status='FAILED',attempt_count=?,
                       completed_ts_ms=MAX(submitted_ts_ms,?),error_type=?,error=?
                       WHERE command_id=?
                       AND status<>'COMMITTED'""",
                    (int(attempt), _now_ms(), type(exc).__name__, str(exc)[:500],
                     command.command_id),
                )
                if cursor.rowcount != 1:
                    return False
            return True
        except Exception:
            # The original failure is authoritative; a locked/corrupt database
            # must not be hidden by a secondary journal failure.
            return False

    @staticmethod
    def _retryable_sqlite_error(exc: sqlite3.OperationalError) -> bool:
        return any(token in str(exc).lower() for token in ("locked", "busy"))

    def _ack_committed(
        self, envelope: _Envelope, result: Any, *, queue_ms: float,
        process_started: float, commit_ms: float,
    ) -> None:
        command = envelope.command
        ack_ms = max(
            0.0, (time.monotonic() - process_started) * 1_000.0 + queue_ms)
        with self._metrics_lock:
            self._metrics["commands_committed"] += 1
            self._metrics["terminal_committed"] += int(command.terminal)
            self._metrics["last_commit_ts_ms"] = _now_ms()
            self._metrics["last_committed_command_id"] = command.command_id
            committed_mono = time.monotonic()
            self._critical_commit_times.append(committed_mono)
            self._inflight_commands.pop(command.command_id, None)
            self._metrics["unconfirmed_command_count"] = max(
                0, int(self._metrics["unconfirmed_command_count"]) - 1)
            if _is_trade_critical(command):
                self._trade_critical_commit_times.append(committed_mono)
                self._metrics["unconfirmed_trade_critical_count"] = max(
                    0, int(self._metrics["unconfirmed_trade_critical_count"]) - 1)
            self._metrics["heartbeat_ts_ms"] = _now_ms()
            self._latency_samples["queue"].append(queue_ms)
            self._latency_samples["commit"].append(max(0.0, commit_ms))
            self._latency_samples["ack"].append(ack_ms)
            if self._metrics.get("state") != "DEGRADED":
                self._metrics["last_error"] = ""
        if not envelope.future.done():
            envelope.future.set_result(result)

    def _process(self, store: V4Store, envelope: _Envelope) -> None:
        command = envelope.command
        payload_json, payload_hash = self._command_payload(command)
        process_started = time.monotonic()
        queue_ms = max(0.0, float(_now_ms() - envelope.enqueued_ts_ms))
        journal_open = False
        # The queue-side hold transfers to the journal-side counter at
        # admission; releasing it only after _journal_submitted returns keeps
        # the trade-critical gate free of any in-flight visibility gap.
        queue_hold = _is_trade_critical(command)

        def release_queue_hold() -> None:
            nonlocal queue_hold
            if queue_hold:
                queue_hold = False
                with self._metrics_lock:
                    self._queued_trade_critical = max(
                        0, self._queued_trade_critical - 1)

        try:
            for journal_attempt in range(1, command.max_attempts + 1):
                try:
                    replay, result = self._journal_submitted(
                        store, envelope, payload_json, payload_hash)
                    journal_open = not replay
                    break
                except sqlite3.OperationalError as exc:
                    if (not self._retryable_sqlite_error(exc)
                            or journal_attempt >= command.max_attempts):
                        raise
                    with self._metrics_lock:
                        self._metrics["commands_retried"] += 1
                    time.sleep(min(0.1, 0.01 * (2 ** (journal_attempt - 1))))
            release_queue_hold()
            if replay:
                with self._metrics_lock:
                    self._metrics["idempotent_replays"] += 1
                    # An idempotent replay is a confirmed durable outcome: the
                    # journal already holds a COMMITTED row for it.
                    self._inflight_commands.pop(command.command_id, None)
                if not envelope.future.done():
                    envelope.future.set_result(result)
                return
            for attempt in range(1, command.max_attempts + 1):
                try:
                    # If a prior COMMIT acknowledgement was ambiguous, consult
                    # the journal before issuing any operation again.  A
                    # committed mutation is acknowledged, never replayed.
                    journal = store.query_one(
                        "SELECT status,result_json FROM persistence_commands "
                        "WHERE command_id=?", (command.command_id,))
                    if journal is not None and journal["status"] == "COMMITTED":
                        self._ack_committed(
                            envelope, json.loads(journal["result_json"] or "null"),
                            queue_ms=queue_ms, process_started=process_started,
                            commit_ms=0.0)
                        return
                    self._mark_executing(
                        store, command, attempt, threading.get_ident())
                    commit_started = time.monotonic()
                    result = self._commit_command(
                        store, command, attempt, payload_hash)
                    commit_ms = max(0.0, (time.monotonic() - commit_started) * 1_000.0)
                    self._ack_committed(
                        envelope, result, queue_ms=queue_ms,
                        process_started=process_started, commit_ms=commit_ms)
                    return
                except sqlite3.OperationalError as exc:
                    if (self._retryable_sqlite_error(exc)
                            and attempt < command.max_attempts):
                        with self._metrics_lock:
                            self._metrics["commands_retried"] += 1
                        time.sleep(min(0.1, 0.01 * (2 ** (attempt - 1))))
                        continue
                    raise
        except Exception as exc:
            known_prior_journal = (
                not journal_open
                and isinstance(exc, (
                    V4PersistenceIdempotencyConflict,
                    V4PersistenceCommandFailed,
                ))
            )
            failure_finalized = known_prior_journal or self._mark_failed(
                store, command, command.max_attempts, exc)
            release_queue_hold()
            trade_critical = _is_trade_critical(command)
            with self._metrics_lock:
                self._metrics["commands_failed"] += 1
                if failure_finalized:
                    # Durably finalized as FAILED: resolved, not unconfirmed.
                    # An unfinalized failure stays registered so its age keeps
                    # growing and the execution gate stays fail-closed.
                    self._inflight_commands.pop(command.command_id, None)
                    self._metrics["unconfirmed_command_count"] = max(
                        0, int(self._metrics["unconfirmed_command_count"]) - 1)
                    if trade_critical:
                        self._metrics["unconfirmed_trade_critical_count"] = max(
                            0,
                            int(self._metrics["unconfirmed_trade_critical_count"]) - 1,
                        )
                    self._metrics["last_error"] = (
                        f"{type(exc).__name__}:{exc}")[:500]
                else:
                    # If journal admission itself was ambiguous, conservatively
                    # add one outstanding command.  If it was known open, retain
                    # the count established by _journal_submitted.
                    if not journal_open:
                        self._metrics["unconfirmed_command_count"] += 1
                        if trade_critical:
                            self._metrics["unconfirmed_trade_critical_count"] += 1
                    self._metrics["journal_finalization_failures"] += 1
                    self._metrics["state"] = "DEGRADED"
                    self._metrics["last_error"] = (
                        "JournalFinalizationFailed:"
                        f"{type(exc).__name__}:{exc}")[:500]
                self._metrics["heartbeat_ts_ms"] = _now_ms()
            # The engine must distinguish a durably FAILED command (known
            # logical evidence loss) from an acknowledgement/journal failure
            # whose durable outcome is still unknown.  Preserve that exact
            # classification on the original exception propagated to its
            # awaiting caller.
            try:
                setattr(
                    exc, "persistence_failure_finalized",
                    bool(failure_finalized),
                )
            except Exception:
                pass
            if not envelope.future.done():
                envelope.future.set_exception(exc)

    def _record_sample(self, store: V4Store, state: str) -> None:
        metrics = self.metrics()
        counters = store.transaction_counters
        elapsed_minutes = max((time.monotonic() - self._started_mono) / 60.0, 1 / 60_000)
        with self._metrics_lock:
            self._metrics["transaction_rate_per_min"] = (
                float(counters["committed"]) / elapsed_minutes)
        store.record_persistence_worker_sample({
            "sample_ts_ms": _now_ms(),
            "worker_thread_id": threading.get_ident(),
            "state": state,
            "queue_depth": metrics["queue_depth"],
            "queue_capacity": metrics["queue_capacity"],
            "queue_high_water": metrics["queue_high_water"],
            "oldest_queue_age_ms": metrics["oldest_queue_age_ms"],
            "commands_submitted": metrics["commands_submitted"],
            "commands_committed": metrics["commands_committed"],
            "commands_failed": metrics["commands_failed"],
            "commands_retried": metrics["commands_retried"],
            "idempotent_replays": metrics["idempotent_replays"],
            "queue_full_count": metrics["queue_full_count"],
            "timeout_count": metrics["timeout_count"],
            "transactions_started": counters["started"],
            "transactions_committed": counters["committed"],
            "transactions_rolled_back": counters["rolled_back"],
            "last_commit_ts_ms": metrics["last_commit_ts_ms"] or None,
            "last_error": metrics["last_error"] or None,
        })

    def _run(self) -> None:
        store: Optional[V4Store] = None
        try:
            store = V4Store(
                self.db_path, busy_timeout_ms=self.busy_timeout_ms,
                enforce_thread_ownership=True)
            with self._metrics_lock:
                self._metrics["state"] = "RUNNING"
                self._metrics["worker_thread_id"] = threading.get_ident()
                self._metrics["heartbeat_ts_ms"] = _now_ms()
            self._recover_incomplete(store)
            reconciliation = store.reconcile_startup_state(
                current_launch_nonce=self.current_launch_nonce,
                proven_absent_launch_nonces=self.proven_absent_launch_nonces,
            )
            with self._metrics_lock:
                self._metrics["recovery_reconciliation"] = reconciliation
                for key in (
                    "unfinished_maker_observations",
                    "reconciled_abandoned_maker_observations",
                    "unfinished_makers_left_fail_closed",
                ):
                    self._metrics[key] = int(reconciliation.get(key, 0) or 0)
            if int(reconciliation.get("consistency_errors") or 0):
                raise V4PersistenceError(
                    "startup persistence reconciliation found lifecycle inconsistencies")
            self._record_sample(store, "STARTED")
            self._ready.set_result(True)
            last_sample = time.monotonic()
            while True:
                envelope = self._scheduler.get(timeout_s=0.25)
                if envelope is None:
                    break
                if envelope is not _IDLE:
                    assert isinstance(envelope, _Envelope)
                    self._write_gate.acquire_critical()
                    try:
                        self._process(store, envelope)
                    finally:
                        self._write_gate.release_critical()
                with self._metrics_lock:
                    self._metrics["heartbeat_ts_ms"] = _now_ms()
                if (self.sample_interval_s == 0.0
                        or time.monotonic() - last_sample >= self.sample_interval_s):
                    # The periodic worker sample is diagnostics, not evidence,
                    # and it is the only writer that runs when the lane is
                    # otherwise idle.  SQLite's online backup restarts from page
                    # one on any external write, so a 1 Hz diagnostic row is
                    # enough to stop an audit snapshot converging forever.  A
                    # bounded, explicitly requested quiescent window suppresses
                    # it -- and only it; every queued command is still processed
                    # and committed exactly as usual.
                    if self._quiescent_window.is_set():
                        with self._metrics_lock:
                            self._metrics["quiescent_samples_skipped"] = int(
                                self._metrics.get("quiescent_samples_skipped")
                                or 0) + 1
                    else:
                        self._record_sample(store, "RUNNING")
                        last_sample = time.monotonic()
            self._record_sample(store, "STOPPING")
            if self.checkpoint_on_close:
                store.checkpoint(mode="PASSIVE", reason="persistence_writer_stop")
            with self._metrics_lock:
                self._metrics["state"] = "STOPPED"
                self._metrics["heartbeat_ts_ms"] = _now_ms()
        except Exception as exc:
            with self._metrics_lock:
                self._metrics["state"] = "FAILED"
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"[:500]
                self._metrics["heartbeat_ts_ms"] = _now_ms()
            if not self._ready.done():
                self._ready.set_exception(exc)
            self._scheduler.fail_all(
                V4PersistenceError(f"persistence worker failed: {type(exc).__name__}:{exc}"))
            self._write_gate.reset_critical()
        finally:
            if store is not None:
                store.close()


class V4TelemetryStoreSink:
    """Short-transaction telemetry sink owned by its aggregation thread.

    This is intentionally a second SQLite connection.  It never enters the
    critical writer scheduler, so lossy event counters and health rows cannot
    create head-of-line blocking for entries or terminal position transitions.
    """

    def __init__(self, db_path: str | Path, *, busy_timeout_ms: int = 10_000,
                 write_gate: Optional[_CriticalFirstWriteGate] = None,
                 max_batch_calls: int = 32,
                 # The budget bounds how long telemetry may hold the shared
                 # write lock while nothing critical is waiting.  A newly
                 # arrived critical command still preempts at the next row
                 # boundary, so the real contention a critical write can see is
                 # one row, not this budget.  100 ms left no headroom at all on
                 # a multi-GB database: a single row could exceed it, making
                 # deadline misses unavoidable even at a chunk size of one.
                 max_transaction_ms: float = 250.0):
        if (isinstance(max_batch_calls, bool)
                or not isinstance(max_batch_calls, int)
                or not 1 <= max_batch_calls <= 512):
            raise ValueError("max_batch_calls must be in [1,512]")
        if (isinstance(max_transaction_ms, bool)
                or not isinstance(max_transaction_ms, (int, float))
                or not math.isfinite(float(max_transaction_ms))
                or not 1.0 <= float(max_transaction_ms) <= 1_000.0):
            raise ValueError("max_transaction_ms must be within [1,1000]")
        self.db_path = Path(db_path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        self._write_gate = write_gate or _CriticalFirstWriteGate()
        self.max_batch_calls = int(max_batch_calls)
        self.max_transaction_ms = float(max_transaction_ms)
        self._store: Optional[V4Store] = None
        self._owner_thread_id: Optional[int] = None
        self._metrics: dict[str, Any] = {
            "state": "NEW", "owner_thread_id": 0, "batches": 0,
            "calls": 0, "failures": 0, "last_commit_ts_ms": 0,
            "last_duration_ms": 0.0, "max_duration_ms": 0.0,
            "last_transaction_duration_ms": 0.0,
            "max_transaction_duration_ms": 0.0,
            "last_transaction_fixed_overhead_ms": 0.0,
            "last_row_work_duration_ms": 0.0,
            "last_row_call_p95_ms": 0.0,
            "last_row_call_max_ms": 0.0,
            "last_outer_transactions": 0, "last_error": "",
            "priority_skipped_batches": 0,
            "deadline_exceeded_batches": 0, "batch_rejected_count": 0,
            "max_batch_calls": self.max_batch_calls,
            "max_transaction_ms": self.max_transaction_ms,
        }

    def _ensure_store(self) -> V4Store:
        current = threading.get_ident()
        if self._store is None:
            self._owner_thread_id = current
            self._store = V4Store(
                self.db_path, busy_timeout_ms=self.busy_timeout_ms,
                enforce_thread_ownership=True)
            self._metrics.update({
                "state": "HEALTHY", "owner_thread_id": current,
            })
        elif self._owner_thread_id != current:
            raise V4PersistenceError(
                "telemetry SQLite sink used outside its aggregation thread")
        return self._store

    @staticmethod
    def _normalize_calls(
        calls: Iterable[V4PersistenceCommand | Mapping[str, Any]],
        *, max_calls: int,
    ) -> tuple[V4PersistenceCommand, ...]:
        normalized: list[V4PersistenceCommand] = []
        for index, raw in enumerate(calls):
            if index >= max_calls:
                raise V4TelemetryBatchTooLarge(
                    f"telemetry physical batch exceeds {max_calls} calls")
            if isinstance(raw, V4PersistenceCommand):
                command = raw
            elif isinstance(raw, Mapping):
                unknown = set(raw) - {"method", "args", "kwargs", "command_id"}
                if unknown:
                    raise V4PersistenceError(
                        f"unsupported telemetry batch fields: {sorted(unknown)}")
                command = V4PersistenceCommand(
                    command_id=str(raw.get("command_id") or
                                   f"telemetry-call:{uuid.uuid4().hex}:{index}"),
                    method=str(raw.get("method") or ""),
                    args=tuple(raw.get("args") or ()),
                    kwargs=dict(raw.get("kwargs") or {}),
                    ordering_key="telemetry", command_type="TELEMETRY_CALL",
                    priority=1_000,
                )
            else:
                raise V4PersistenceError("invalid telemetry batch call")
            if command.method not in TELEMETRY_METHODS:
                raise V4PersistenceError(
                    f"telemetry method is not allowlisted: {command.method}")
            normalized.append(command)
        return tuple(normalized)

    def submit_telemetry_batch(
        self, calls: Iterable[V4PersistenceCommand | Mapping[str, Any]], *,
        command_id: Optional[str] = None,
        timeout_s: Optional[float] = None,
    ) -> list[Any]:
        del command_id  # Telemetry is intentionally aggregated, not journaled.
        if timeout_s is not None and (
                isinstance(timeout_s, bool)
                or not isinstance(timeout_s, (int, float))
                or not math.isfinite(float(timeout_s))
                or float(timeout_s) <= 0):
            raise V4PersistenceTimeout("telemetry timeout must be finite and positive")
        try:
            rows = self._normalize_calls(calls, max_calls=self.max_batch_calls)
        except V4TelemetryBatchTooLarge:
            self._metrics["batch_rejected_count"] += 1
            self._metrics["state"] = "DEGRADED_BATCH_BOUND"
            self._metrics["last_error"] = "telemetry_physical_batch_too_large"
            raise
        if not rows:
            return []
        deferral_reason = self._write_gate.try_acquire_telemetry_reason()
        if deferral_reason is not None:
            self._metrics["priority_skipped_batches"] += 1
            if deferral_reason == "maintenance_pending":
                self._metrics["state"] = "DEFERRED_MAINTENANCE"
                self._metrics["last_error"] = "maintenance_pending"
                raise V4TelemetryMaintenanceDeferral(
                    "telemetry deferred so bounded maintenance can run")
            self._metrics["state"] = "DEGRADED_CRITICAL_PRIORITY"
            self._metrics["last_error"] = "critical_persistence_pending"
            raise V4TelemetryPrioritySkip(
                "telemetry skipped because critical persistence is pending")
        started_clock = time.perf_counter()
        allowed_s = self.max_transaction_ms / 1_000.0
        if timeout_s is not None:
            allowed_s = min(allowed_s, float(timeout_s))
        # The budget bounds how long telemetry may *hold* the shared SQLite
        # write lock, so it is armed when BEGIN IMMEDIATE actually acquires it.
        # Arming at call entry instead charged the contended lock wait -- during
        # which telemetry holds nothing and blocks nobody -- against the row
        # budget, so a busy writer alone could exhaust the deadline before a
        # single row was written and roll the whole chunk back.
        deadline: Optional[float] = None
        transaction_started_clock: Optional[float] = None
        row_call_durations_ms: list[float] = []

        def arm_deadline() -> None:
            nonlocal deadline, transaction_started_clock
            transaction_started_clock = time.perf_counter()
            deadline = time.monotonic() + allowed_s

        def cooperative_check() -> None:
            if self._write_gate.critical_pending():
                raise V4TelemetryPrioritySkip(
                    "telemetry transaction yielded to newly pending critical persistence")
            if deadline is not None and time.monotonic() >= deadline:
                exc = V4TelemetryDeadlineExceeded(
                    "telemetry transaction exceeded cooperative deadline")
                exc.telemetry_rows_attempted = len(rows)
                exc.telemetry_deadline_ms = allowed_s * 1_000.0
                raise exc

        try:
            # Connection creation is also inside the gate because V4Store
            # performs additive schema hydration on first open.
            store = self._ensure_store()
            cooperative_check()
            before_transactions = store.transaction_counters
            with store.transaction(immediate=True):
                arm_deadline()
                results = []
                for row in rows:
                    cooperative_check()
                    row_started_clock = time.perf_counter()
                    results.append(getattr(store, row.method)(
                        *row.args, **dict(row.kwargs)))
                    row_call_durations_ms.append(max(
                        0.0,
                        (time.perf_counter() - row_started_clock) * 1_000.0,
                    ))
                    # Check after every nested Store call, including the last,
                    # so a critical arrival rolls back this telemetry chunk
                    # instead of waiting for its commit.
                    cooperative_check()
            completed_clock = time.perf_counter()
            duration = max(
                0.0, (completed_clock - started_clock) * 1_000.0)
            transaction_duration = max(
                0.0,
                (completed_clock - (
                    transaction_started_clock
                    if transaction_started_clock is not None
                    else started_clock
                )) * 1_000.0,
            )
            self._metrics["batches"] += 1
            self._metrics["calls"] += len(rows)
            self._metrics["state"] = "HEALTHY"
            self._metrics["last_commit_ts_ms"] = _now_ms()
            self._metrics["last_duration_ms"] = duration
            self._metrics["max_duration_ms"] = max(
                float(self._metrics["max_duration_ms"]), duration)
            self._metrics["last_transaction_duration_ms"] = (
                transaction_duration)
            self._metrics["max_transaction_duration_ms"] = max(
                float(self._metrics["max_transaction_duration_ms"]),
                transaction_duration,
            )
            row_work_duration = sum(row_call_durations_ms)
            ordered_row_calls = sorted(row_call_durations_ms)
            row_p95_index = max(
                0,
                math.ceil(0.95 * len(ordered_row_calls)) - 1,
            )
            self._metrics["last_row_work_duration_ms"] = row_work_duration
            self._metrics["last_row_call_p95_ms"] = (
                ordered_row_calls[row_p95_index]
                if ordered_row_calls else 0.0)
            self._metrics["last_row_call_max_ms"] = max(
                ordered_row_calls, default=0.0)
            self._metrics["last_transaction_fixed_overhead_ms"] = max(
                0.0, transaction_duration - row_work_duration)
            after_transactions = store.transaction_counters
            self._metrics["last_outer_transactions"] = (
                after_transactions["committed"] - before_transactions["committed"])
            self._metrics["last_error"] = ""
            return results
        except V4TelemetryPrioritySkip as exc:
            self._metrics["failures"] += 1
            self._metrics["priority_skipped_batches"] += 1
            self._metrics["state"] = "DEGRADED_CRITICAL_PRIORITY"
            self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"[:500]
            raise
        except V4TelemetryDeadlineExceeded as exc:
            self._metrics["failures"] += 1
            self._metrics["deadline_exceeded_batches"] += 1
            self._metrics["state"] = "DEGRADED_TELEMETRY_DEADLINE"
            self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"[:500]
            raise
        except sqlite3.OperationalError as exc:
            if not _is_lock_contention(exc):
                self._metrics["failures"] += 1
                self._metrics["state"] = "FAILED"
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"[:500]
                raise
            # The lock was held by another writer, so nothing was written and
            # no transaction opened.  Defer rather than lose the rows.
            self._metrics["priority_skipped_batches"] += 1
            self._metrics["state"] = "DEGRADED_WRITE_CONTENTION"
            self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"[:500]
            raise V4TelemetryWriteContention(
                "telemetry yielded the write lock to another writer") from exc
        except Exception as exc:
            self._metrics["failures"] += 1
            self._metrics["state"] = "FAILED"
            self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"[:500]
            raise
        finally:
            self._write_gate.release_telemetry()

    submit_telemetry_batch_sync = submit_telemetry_batch

    def telemetry_transaction_budget_ms(self) -> float:
        """Cooperative transaction budget one physical batch must fit inside.

        Published so the aggregator can size its chunks from measured per-row
        cost instead of discovering the limit by losing a batch to it.
        """

        return self.max_transaction_ms

    def health(self) -> dict[str, Any]:
        return dict(self._metrics)

    metrics = health

    def close(self) -> None:
        if self._store is None:
            self._metrics["state"] = "STOPPED"
            return
        if threading.get_ident() != self._owner_thread_id:
            raise V4PersistenceError(
                "telemetry SQLite sink must close on its aggregation thread")
        self._store.close()
        self._store = None
        self._metrics["state"] = "STOPPED"


__all__ = [
    "ALLOWED_STORE_METHODS", "COMPACT_ON_COMMIT_COMMAND_TYPES",
    "TRADE_CRITICAL_METHODS",
    "TELEMETRY_BATCH_METHOD", "TELEMETRY_METHODS",
    "V4PersistenceCommand", "V4PersistenceCommandFailed",
    "V4PersistenceError", "V4PersistenceIdempotencyConflict",
    "V4PersistenceQueueFull", "V4PersistenceTimeout", "V4PersistenceWriter",
    "V4TelemetryBatchTooLarge", "V4TelemetryDeadlineExceeded",
    "V4TelemetryMaintenanceDeferral",
    "V4TelemetryPrioritySkip", "V4TelemetryStoreSink",
    "V4TelemetryWriteContention",
]
