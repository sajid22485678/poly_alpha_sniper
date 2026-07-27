"""Deterministic, off-loop maintenance policy for Frequency V4.

The functions in this module are synchronous by design: pass
``run_bounded_maintenance_pass`` to ``V4MaintenanceWorker.run_maintenance`` so
all database and filesystem work remains on that worker's dedicated thread.
The module depends only on small, duck-typed Store contracts and never imports
the V4 engine or a legacy lane.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field, replace
from enum import Enum
import inspect
import math
from pathlib import Path
import time
from typing import Any, Callable, Mapping, Optional


class CheckpointMode(str, Enum):
    """The only checkpoint modes the V4 policy may issue."""

    PASSIVE = "PASSIVE"
    RESTART = "RESTART"
    TRUNCATE = "TRUNCATE"


class CheckpointStatus(str, Enum):
    SKIPPED = "SKIPPED"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    BUSY = "BUSY"
    FAILED = "FAILED"


class MaintenanceStatus(str, Enum):
    SKIPPED = "SKIPPED"
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


@dataclass(frozen=True)
class MaintenancePolicy:
    """Validated limits for one checkpoint/retention pass."""

    wal_trigger_bytes: int = 32 * 1024 * 1024
    restart_trigger_bytes: int = 128 * 1024 * 1024
    truncate_trigger_bytes: int = 32 * 1024 * 1024
    checkpoint_min_interval_ms: int = 60_000
    max_commit_p95_ms: float = 100.0
    restart_max_commit_p95_ms: float = 25.0
    telemetry_queue_gate: int = 256
    window_guard_ms: int = 3_000
    restart_window_guard_ms: int = 10_000
    retention_ms: int = 24 * 60 * 60 * 1_000
    retention_chunk_rows: int = 250
    retention_row_budget: int = 4_000
    retention_time_budget_ms: int = 1_000
    raw_event_max_rows: int = 250_000
    event_bucket_detail_retention_ms: int = 15 * 60 * 1_000
    metadata_retention_ms: int = 24 * 60 * 60 * 1_000
    metadata_max_rows: int = 100_000
    journal_payload_retention_ms: int = 24 * 60 * 60 * 1_000
    # Consecutive PASSIVE checkpoints that reclaimed zero WAL bytes before the
    # policy escalates to a stronger, live-safe mode.
    no_progress_escalation_threshold: int = 2
    # Loop lag at or above which a live TRUNCATE reclaim is deferred, and the
    # WAL size above which it proceeds anyway so the WAL cannot grow without
    # bound.  The ceiling is deliberately well above the escalation trigger:
    # deferral buys responsiveness for a cycle or two, it never disables
    # reclamation.
    lag_defer_threshold_ms: float = 250.0
    lag_defer_max_wal_bytes: int = 512 * 1024 * 1024
    # Rate limit for checkpoints taken *because the WAL is over its trigger*.
    # ``checkpoint_min_interval_ms`` paces routine passes, but applying it to
    # pressure passes as well is what let the WAL reach 450 MB: measured under
    # load the WAL grows ~110 MB/min, so one attempt per minute means every
    # attempt lands on a WAL an order of magnitude past the 32 MB trigger this
    # policy already documents.  A short pressure floor makes that existing
    # trigger enforceable instead of nominal.
    pressure_checkpoint_min_interval_ms: int = 5_000
    # Hard bound on how long an escalated live checkpoint may wait on the
    # writer/reader locks.  It keeps a reclaim attempt from becoming a long
    # global database stall; a contended attempt returns BUSY and retries on
    # the next maintenance cycle instead.
    live_reclaim_busy_timeout_ms: int = 2_000
    # How long an escalated reclamation may keep retrying a refused
    # background-write gate before giving up for this cycle.  The gate refuses
    # whenever a critical command is in flight, so a single non-blocking
    # attempt is refused almost every cycle under continuous load and the
    # reclamation the policy decided on never actually runs.
    live_reclaim_gate_wait_ms: int = 4_000

    def __post_init__(self) -> None:
        positive_ints = (
            "wal_trigger_bytes",
            "restart_trigger_bytes",
            "truncate_trigger_bytes",
            "checkpoint_min_interval_ms",
            "retention_ms",
            "retention_chunk_rows",
            "retention_row_budget",
            "retention_time_budget_ms",
            "raw_event_max_rows",
            "event_bucket_detail_retention_ms",
            "metadata_retention_ms",
            "metadata_max_rows",
            "journal_payload_retention_ms",
        )
        for name in positive_ints:
            _require_int(name, getattr(self, name), minimum=1)
        _require_int(
            "no_progress_escalation_threshold",
            self.no_progress_escalation_threshold, minimum=1)
        _require_int(
            "live_reclaim_busy_timeout_ms",
            self.live_reclaim_busy_timeout_ms, minimum=1)
        _require_int(
            "live_reclaim_gate_wait_ms",
            self.live_reclaim_gate_wait_ms, minimum=0)
        _require_int("telemetry_queue_gate", self.telemetry_queue_gate, minimum=0)
        _require_int("window_guard_ms", self.window_guard_ms, minimum=0)
        _require_int("restart_window_guard_ms", self.restart_window_guard_ms, minimum=0)
        _require_finite_positive("max_commit_p95_ms", self.max_commit_p95_ms)
        _require_finite_positive(
            "restart_max_commit_p95_ms", self.restart_max_commit_p95_ms
        )
        if self.restart_trigger_bytes < self.wal_trigger_bytes:
            raise ValueError("restart_trigger_bytes must be >= wal_trigger_bytes")
        if self.truncate_trigger_bytes < self.wal_trigger_bytes:
            raise ValueError("truncate_trigger_bytes must be >= wal_trigger_bytes")
        if self.restart_max_commit_p95_ms > self.max_commit_p95_ms:
            raise ValueError("restart_max_commit_p95_ms must be <= max_commit_p95_ms")
        if self.retention_chunk_rows > self.retention_row_budget:
            raise ValueError("retention_chunk_rows must be <= retention_row_budget")
        if self.raw_event_max_rows < 1_000:
            raise ValueError("raw_event_max_rows must be >= 1000")
        if self.metadata_max_rows < 1_000:
            raise ValueError("metadata_max_rows must be >= 1000")


@dataclass(frozen=True)
class MaintenanceSnapshot:
    """A point-in-time, immutable input to the deterministic policy."""

    now_ms: int
    wal_bytes: int
    critical_queue_depth: int
    telemetry_queue_depth: int
    runtime_active: bool
    runtime_health: str
    writer_healthy: bool
    open_positions: int
    active_readers: int = 0
    long_reader_count: int = 0
    critical_commit_p95_ms: Optional[float] = None
    time_to_window_boundary_ms: Optional[int] = None
    last_checkpoint_attempt_ts_ms: Optional[int] = None
    last_successful_checkpoint_ts_ms: Optional[int] = None
    # When the last RESTART/TRUNCATE reclamation was last *attempted* (any
    # outcome).  The expensive escalation is paced against this, not against
    # the last checkpoint of any mode: under WAL pressure a cheap PASSIVE
    # backfill runs every few seconds, so pacing the escalation by the last
    # attempt of any mode makes it permanently unreachable exactly when it is
    # needed -- measured live, consecutive_no_progress_passive reached 112
    # with zero escalations while the WAL grew to 1.28 GB.  ``None`` falls
    # back to ``last_checkpoint_attempt_ts_ms``, preserving the historical
    # single-cadence behaviour for callers that do not track escalations.
    last_escalation_attempt_ts_ms: Optional[int] = None
    # Number of consecutive recent PASSIVE checkpoints that completed without
    # reclaiming any WAL bytes (``after_wal_bytes >= before_wal_bytes``).  A
    # reader-pinned WAL can report SUCCESS + frames checkpointed yet never
    # shrink; this counter lets the policy escalate to RESTART, which briefly
    # waits for readers to drain, instead of looping on PASSIVE forever.
    consecutive_no_progress_passive: int = 0
    # Current event-loop lag.  A large live reclaim saturates the disk the
    # runtime also publishes its heartbeat/export to, so an already-lagging loop
    # defers the reclaim rather than compounding the stall.  Defaults to 0 so an
    # unaware caller behaves exactly as before.
    event_loop_lag_ms: float = 0.0
    # Whether the last database integrity scan passed.  This is the only
    # health signal WAL reclamation consults, because it is the only one that
    # describes the database.  ``runtime_health`` folds in trade-gating
    # conditions (unreconciled maker evidence, exposure limits, market-data
    # freshness) that say nothing about whether a checkpoint is safe, and
    # gating on it suspended reclamation indefinitely in a live soak.
    integrity_ok: bool = True

    def __post_init__(self) -> None:
        for name in (
            "now_ms",
            "wal_bytes",
            "critical_queue_depth",
            "telemetry_queue_depth",
            "open_positions",
            "active_readers",
            "long_reader_count",
            "consecutive_no_progress_passive",
        ):
            _require_int(name, getattr(self, name), minimum=0)
        if not isinstance(self.runtime_active, bool):
            raise TypeError("runtime_active must be bool")
        if not isinstance(self.writer_healthy, bool):
            raise TypeError("writer_healthy must be bool")
        if not isinstance(self.integrity_ok, bool):
            raise TypeError("integrity_ok must be bool")
        if not isinstance(self.runtime_health, str) or not self.runtime_health.strip():
            raise ValueError("runtime_health must be a non-empty string")
        for name in (
            "time_to_window_boundary_ms",
            "last_checkpoint_attempt_ts_ms",
            "last_successful_checkpoint_ts_ms",
            "last_escalation_attempt_ts_ms",
        ):
            value = getattr(self, name)
            if value is not None:
                _require_int(name, value, minimum=0)
        latency = self.critical_commit_p95_ms
        if latency is not None:
            if isinstance(latency, bool) or not isinstance(latency, (int, float)):
                raise TypeError("critical_commit_p95_ms must be numeric or None")
            if not math.isfinite(float(latency)) or float(latency) < 0:
                raise ValueError("critical_commit_p95_ms must be finite and >= 0")

    @property
    def normalized_health(self) -> str:
        return self.runtime_health.strip().upper()


@dataclass(frozen=True)
class CheckpointDecision:
    should_run: bool
    mode: Optional[CheckpointMode]
    reason: str
    snapshot: MaintenanceSnapshot

    def __post_init__(self) -> None:
        if self.should_run != (self.mode is not None):
            raise ValueError("checkpoint decision mode/run state is inconsistent")
        if not self.reason:
            raise ValueError("checkpoint decision reason is required")


@dataclass(frozen=True)
class CheckpointResult:
    status: CheckpointStatus
    mode: Optional[CheckpointMode]
    reason: str
    started_ts_ms: int
    completed_ts_ms: int
    duration_ms: float
    before_wal_bytes: int
    after_wal_bytes: int
    busy_result: Optional[int] = None
    frames_total: Optional[int] = None
    frames_checkpointed: Optional[int] = None
    database_bytes: int = 0
    failure_reason: Optional[str] = None
    record_attempted: bool = False
    recorded: bool = False
    record_error: Optional[str] = None
    # Active-reader correlation, captured on the maintenance worker at the
    # instant the checkpoint began and again when it finished.  This is what
    # ties a zero-progress checkpoint to the concrete reader that pinned it.
    # Optional and excluded from the persisted checkpoint_runs record so the
    # database schema is untouched.
    readers_at_start: Optional[Mapping[str, Any]] = None
    readers_at_end: Optional[Mapping[str, Any]] = None

    @property
    def successful(self) -> bool:
        return self.status in {CheckpointStatus.SUCCESS, CheckpointStatus.PARTIAL}

    @property
    def bytes_reclaimed(self) -> int:
        """WAL bytes actually returned to the filesystem (never negative)."""

        return max(0, int(self.before_wal_bytes) - int(self.after_wal_bytes))

    @property
    def frames_progress(self) -> bool:
        """True when SQLite copied at least one frame into the database."""

        return bool(self.frames_checkpointed and self.frames_checkpointed > 0)

    @property
    def made_progress(self) -> bool:
        """True only when the WAL file actually shrank.

        A PASSIVE checkpoint routinely reports ``frames_checkpointed ==
        frames_total`` and still leaves ``after_wal_bytes >= before_wal_bytes``:
        the frames were backfilled into the database, but the WAL could not be
        restarted (a reader held a read-mark, or the writer appended new frames
        during the backfill), so the file kept growing.  Treating copied frames
        as progress made the no-progress escalation counter unreachable and let
        the WAL grow without bound.  Progress is reclaimed bytes.
        """

        return self.bytes_reclaimed > 0

    @property
    def last_successful_checkpoint_ts_ms(self) -> Optional[int]:
        return self.completed_ts_ms if self.successful else None

    def checkpoint_record(self) -> dict[str, Any]:
        if self.mode is None:
            raise ValueError("a skipped checkpoint has no persistence record")
        return {
            "started_ts_ms": self.started_ts_ms,
            "completed_ts_ms": self.completed_ts_ms,
            "mode": self.mode.value,
            "reason": self.reason,
            "before_wal_bytes": self.before_wal_bytes,
            "after_wal_bytes": self.after_wal_bytes,
            "duration_ms": self.duration_ms,
            "busy_result": self.busy_result,
            "frames_total": self.frames_total,
            "frames_checkpointed": self.frames_checkpointed,
            "database_bytes": self.database_bytes,
            "success": int(self.successful),
            "failure_reason": self.failure_reason,
        }

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["status"] = self.status.value
        result["mode"] = self.mode.value if self.mode is not None else None
        result["successful"] = self.successful
        result["made_progress"] = self.made_progress
        result["frames_progress"] = self.frames_progress
        result["bytes_reclaimed"] = self.bytes_reclaimed
        return result


@dataclass(frozen=True)
class MaintenancePassResult:
    status: MaintenanceStatus
    reason: str
    started_ts_ms: int
    completed_ts_ms: int
    duration_ms: float
    rows_deleted: int
    protected_rows_skipped: int
    chunks_completed: int
    row_budget_exhausted: bool
    time_budget_exhausted: bool
    rows_compacted: int = 0
    budget_units_consumed: int = 0
    metrics: Mapping[str, int] = field(default_factory=dict)
    checkpoint: Optional[CheckpointResult] = None
    failure_reason: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["status"] = self.status.value
        result["checkpoint"] = (
            self.checkpoint.as_dict() if self.checkpoint is not None else None
        )
        return result


_ACTIVE_HEALTH = frozenset({"HEALTHY", "READY", "RUNNING"})
_STOPPED_HEALTH = frozenset({"OFFLINE", "STOPPED"})
# Decision reason for the live, byte-reclaiming escalation.  Carried through to
# perform_checkpoint so the hard-safety recheck knows this TRUNCATE was
# authorized by the live invariants rather than the stopped-runtime ones.
LIVE_RECLAIM_REASON = "passive_no_progress_live_reclaim"
EMERGENCY_WAL_REASON = "emergency_wal_pressure"
# Checkpoint reasons urgent enough to retry a refused background-write gate.
# Both mean the WAL is already past its restart trigger.  A routine pass has no
# urgency and keeps yielding immediately; these do, because yielding is exactly
# what let the WAL grow -- a deferred PASSIVE never runs, so the no-progress
# counter never advances, so the reclamation escalation never arms.
_GATE_RETRY_REASONS = frozenset({LIVE_RECLAIM_REASON, EMERGENCY_WAL_REASON})


def decide_checkpoint(
    snapshot: MaintenanceSnapshot, policy: MaintenancePolicy
) -> CheckpointDecision:
    """Return a deterministic checkpoint decision with a precise reason."""

    if snapshot.wal_bytes < policy.wal_trigger_bytes:
        return _skip(snapshot, "wal_below_trigger")
    last_attempt = snapshot.last_checkpoint_attempt_ts_ms
    if last_attempt is not None:
        if last_attempt > snapshot.now_ms:
            return _skip(snapshot, "checkpoint_timestamp_in_future")
        since_last = snapshot.now_ms - last_attempt
        floor_ms = min(
            policy.checkpoint_min_interval_ms,
            policy.pressure_checkpoint_min_interval_ms,
        )
        if since_last < floor_ms:
            return _skip(snapshot, "checkpoint_min_interval")
        if since_last < policy.checkpoint_min_interval_ms:
            # Between the pressure floor and the routine interval only the cheap
            # PASSIVE backfill may run, never an escalation.  The WAL is already
            # past ``wal_trigger_bytes`` (checked above), and pacing this backfill
            # at the routine interval is what let a WAL growing ~110 MB/min reach
            # 450 MB before anything touched it -- so the only thing that ever
            # reclaimed bytes was a disk-saturating one-shot TRUNCATE.  Frequent
            # small backfills keep the file near its configured trigger, which is
            # what makes the eventual reclaim small.  PASSIVE runs on the
            # dedicated maintenance connection and never blocks the critical
            # writer, so it is safe at this cadence.
            #
            # The reclamation escalation keeps the full routine interval, but it
            # is paced against the last *escalation* attempt, not the last
            # attempt of any mode: this branch runs every few seconds while the
            # WAL is over its trigger, so measuring the escalation against it
            # made the escalation unreachable exactly when it was armed --
            # measured live, consecutive_no_progress_passive reached 112 with
            # zero escalations while the WAL grew to 1.28 GB and only one
            # accidental reader-gap reset occurred in 28 minutes.  A backfilled
            # WAL makes the reclaim cheap: TRUNCATE only has to reset the log
            # and release the file, never copy hundreds of megabytes of frames.
            if (_escalation_due(snapshot, policy)
                    and _live_reclaim_is_safe(snapshot, policy)):
                return CheckpointDecision(
                    True, CheckpointMode.TRUNCATE, LIVE_RECLAIM_REASON, snapshot
                )
            return CheckpointDecision(
                True, CheckpointMode.PASSIVE, EMERGENCY_WAL_REASON, snapshot
            )

    # Live WAL reclamation, evaluated BEFORE the general maintenance gate.
    #
    # Under continuous write load a PASSIVE checkpoint backfills every frame
    # into the database and still cannot reset the WAL: a reader holds a
    # read-mark, or the writer appends new frames while the backfill runs.  It
    # therefore reports SUCCESS with frames_checkpointed == frames_total and
    # reclaims zero bytes, forever, while the file keeps growing.
    #
    # The old escalation sat after the gate, but an oversized WAL is exactly
    # what closes that gate (commit latency rises), so the emergency branch
    # below returned PASSIVE every cycle and the escalation was unreachable in
    # production.  It also required full business quiescence (no open
    # positions), which never holds in a live shadow run and has nothing to do
    # with SQLite reader safety.  Reclamation is gated on database-level
    # safety only, and uses TRUNCATE because that is the only mode that
    # returns bytes to the filesystem.
    if _live_reclaim_is_safe(snapshot, policy):
        return CheckpointDecision(
            True, CheckpointMode.TRUNCATE, LIVE_RECLAIM_REASON, snapshot
        )

    gate_reason = maintenance_gate(snapshot, policy)
    if gate_reason is not None:
        if snapshot.wal_bytes >= policy.restart_trigger_bytes:
            # WAL-pressure liveness: a PASSIVE checkpoint on the dedicated
            # maintenance connection never blocks the critical writer, so a
            # busy or degraded runtime must not be able to defer it
            # indefinitely.  Only the bounded min-interval above rate-limits
            # this branch; retention still honors the gate.
            return CheckpointDecision(
                True, CheckpointMode.PASSIVE, EMERGENCY_WAL_REASON, snapshot
            )
        return _skip(snapshot, gate_reason)

    # Quiescent-runtime escalation (kept for the stopped/idle paths, where the
    # stricter quiescence invariants below can actually be satisfied).
    if (snapshot.consecutive_no_progress_passive
            >= policy.no_progress_escalation_threshold
            and snapshot.wal_bytes >= policy.wal_trigger_bytes
            and snapshot.long_reader_count == 0
            and _restart_is_safe(snapshot, policy)):
        return CheckpointDecision(
            True, CheckpointMode.RESTART,
            "passive_no_progress_escalation", snapshot
        )

    if _truncate_is_safe(snapshot, policy):
        return CheckpointDecision(
            True, CheckpointMode.TRUNCATE, "offline_stopped_and_quiescent", snapshot
        )
    if _restart_is_safe(snapshot, policy):
        return CheckpointDecision(
            True, CheckpointMode.RESTART, "high_wal_runtime_quiescent", snapshot
        )
    return CheckpointDecision(
        True, CheckpointMode.PASSIVE, "normal_bounded_checkpoint", snapshot
    )


def maintenance_gate(
    snapshot: MaintenanceSnapshot, policy: MaintenancePolicy
) -> Optional[str]:
    """Return why all maintenance must yield, or ``None`` when it may run."""

    if snapshot.critical_queue_depth > 0:
        return "critical_queue_not_empty"
    if snapshot.telemetry_queue_depth > policy.telemetry_queue_gate:
        return "telemetry_queue_above_gate"
    if snapshot.runtime_active:
        if not snapshot.writer_healthy:
            return "critical_writer_unhealthy"
        if snapshot.normalized_health not in _ACTIVE_HEALTH:
            return "runtime_not_healthy"
        latency = snapshot.critical_commit_p95_ms
        if latency is not None and latency > policy.max_commit_p95_ms:
            return "critical_commit_latency_high"
        boundary = snapshot.time_to_window_boundary_ms
        if boundary is not None and boundary <= policy.window_guard_ms:
            return "market_window_guard"
    return None


def perform_checkpoint(
    store: Any,
    decision: CheckpointDecision,
    policy: MaintenancePolicy,
    *,
    wall_clock_ms: Optional[Callable[[], int]] = None,
    monotonic: Optional[Callable[[], float]] = None,
    wal_size_reader: Optional[Callable[[], int]] = None,
    reader_snapshot_provider: Optional[Callable[[], Mapping[str, Any]]] = None,
) -> CheckpointResult:
    """Execute and account for one policy-approved checkpoint.

    This function catches operation and recording failures so a maintenance
    fault cannot escape into source tasks. It must be called on the dedicated
    maintenance worker, never from an active asyncio loop.

    ``reader_snapshot_provider`` supplies a bounded view of the currently
    active SQLite readers; when given, it is sampled immediately before and
    after the checkpoint so a zero-progress outcome can be attributed to the
    concrete reader that held the WAL read-mark.
    """

    clock = wall_clock_ms or _wall_clock_ms
    monotonic_clock = monotonic or time.monotonic
    started_ts_ms = max(decision.snapshot.now_ms, int(clock()))
    started_mono = monotonic_clock()
    before_wal = decision.snapshot.wal_bytes

    if not decision.should_run:
        return CheckpointResult(
            status=CheckpointStatus.SKIPPED,
            mode=None,
            reason=decision.reason,
            started_ts_ms=started_ts_ms,
            completed_ts_ms=started_ts_ms,
            duration_ms=0.0,
            before_wal_bytes=before_wal,
            after_wal_bytes=before_wal,
        )
    assert decision.mode is not None
    off_loop_reason = _active_event_loop_reason()
    unsafe_reason = _unsafe_mode_reason(
        decision.mode, decision.snapshot, policy, decision.reason)
    if off_loop_reason or unsafe_reason:
        return CheckpointResult(
            status=CheckpointStatus.SKIPPED,
            mode=decision.mode,
            reason=off_loop_reason or unsafe_reason or "unsafe_checkpoint",
            started_ts_ms=started_ts_ms,
            completed_ts_ms=started_ts_ms,
            duration_ms=0.0,
            before_wal_bytes=before_wal,
            after_wal_bytes=before_wal,
            failure_reason=off_loop_reason or unsafe_reason,
        )

    status = CheckpointStatus.FAILED
    busy: Optional[int] = None
    total: Optional[int] = None
    checkpointed: Optional[int] = None
    failure: Optional[str] = None
    background_deferred = False
    already_recorded = False
    operation_duration_ms: Optional[float] = None
    readers_at_start = _safe_reader_snapshot(reader_snapshot_provider)
    try:
        before_wal = _read_wal_bytes(
            store, fallback=snapshot_wal(decision), reader=wal_size_reader
        )
        response = _invoke_reclaiming_checkpoint(
            store, decision, policy, monotonic_clock)
        busy, total, checkpointed = _normalize_checkpoint_response(response)
        if isinstance(response, Mapping) and "before_wal_bytes" in response:
            before_wal = _coerce_nonnegative_int(
                "before_wal_bytes", response["before_wal_bytes"]
            )
        if isinstance(response, Mapping) and "after_wal_bytes" in response:
            after_wal = _coerce_nonnegative_int(
                "after_wal_bytes", response["after_wal_bytes"]
            )
        else:
            after_wal = _read_wal_bytes(
                store, fallback=before_wal, reader=wal_size_reader
            )
        if isinstance(response, Mapping):
            already_recorded = bool(
                response.get("checkpoint_run_id") or response.get("recorded")
            )
            if response.get("duration_ms") is not None:
                operation_duration_ms = _coerce_nonnegative_float(
                    "checkpoint duration", response["duration_ms"]
                )
            response_failure = response.get("failure_reason")
            if response_failure:
                failure = str(response_failure)[:512]
        if busy > 0:
            status = CheckpointStatus.BUSY
        elif checkpointed < total:
            status = CheckpointStatus.PARTIAL
        elif isinstance(response, Mapping) and response.get("success") is False:
            status = CheckpointStatus.FAILED
            failure = failure or "checkpoint Store reported failure"
        else:
            status = CheckpointStatus.SUCCESS
    except Exception as exc:  # noqa: BLE001 - failure is structured, not fatal
        after_wal = _safe_wal_bytes(store, fallback=before_wal, reader=wal_size_reader)
        background_deferred = _is_background_write_deferred(exc)
        if background_deferred:
            status = CheckpointStatus.SKIPPED
            failure = None
        else:
            failure = _error_text(exc)

    completed_ts_ms = max(started_ts_ms, int(clock()))
    measured_duration_ms = max(0.0, (monotonic_clock() - started_mono) * 1_000.0)
    duration_ms = (
        operation_duration_ms
        if operation_duration_ms is not None
        else measured_duration_ms
    )
    result = CheckpointResult(
        status=status,
        mode=decision.mode,
        reason=("critical_write_pending" if background_deferred
                else decision.reason),
        started_ts_ms=started_ts_ms,
        completed_ts_ms=completed_ts_ms,
        duration_ms=duration_ms,
        before_wal_bytes=before_wal,
        after_wal_bytes=after_wal,
        busy_result=busy,
        frames_total=total,
        frames_checkpointed=checkpointed,
        database_bytes=_database_bytes(store),
        failure_reason=failure,
        readers_at_start=readers_at_start,
        readers_at_end=_safe_reader_snapshot(reader_snapshot_provider),
    )
    if background_deferred:
        return result
    if already_recorded:
        return replace(result, record_attempted=True, recorded=True)
    return _record_checkpoint_result(store, result)


def run_bounded_maintenance_pass(
    store: Any,
    *,
    snapshot: MaintenanceSnapshot,
    policy: MaintenancePolicy,
    wall_clock_ms: Optional[Callable[[], int]] = None,
    monotonic: Optional[Callable[[], float]] = None,
    wal_size_reader: Optional[Callable[[], int]] = None,
    reader_snapshot_provider: Optional[Callable[[], Mapping[str, Any]]] = None,
) -> MaintenancePassResult:
    """Run one incremental, row/time-budgeted pass.

    The callable shape is compatible with ``V4MaintenanceWorker``. The
    critical queue is an absolute gate. A busy/partial checkpoint or known
    long reader prevents retention writes in the same pass. Existing Store
    retention methods are required to preserve pinned and trade-linked rows.
    """

    clock = wall_clock_ms or _wall_clock_ms
    monotonic_clock = monotonic or time.monotonic
    started_ts_ms = max(snapshot.now_ms, int(clock()))
    started_mono = monotonic_clock()

    def finish(
        status: MaintenanceStatus,
        reason: str,
        *,
        rows: int = 0,
        protected: int = 0,
        chunks: int = 0,
        compacted: int = 0,
        budget_units: int = 0,
        metrics: Optional[Mapping[str, int]] = None,
        row_exhausted: bool = False,
        time_exhausted: bool = False,
        checkpoint: Optional[CheckpointResult] = None,
        failure: Optional[str] = None,
    ) -> MaintenancePassResult:
        completed = max(started_ts_ms, int(clock()))
        duration = max(0.0, (monotonic_clock() - started_mono) * 1_000.0)
        return MaintenancePassResult(
            status=status,
            reason=reason,
            started_ts_ms=started_ts_ms,
            completed_ts_ms=completed,
            duration_ms=duration,
            rows_deleted=rows,
            protected_rows_skipped=protected,
            chunks_completed=chunks,
            row_budget_exhausted=row_exhausted,
            time_budget_exhausted=time_exhausted,
            rows_compacted=compacted,
            budget_units_consumed=budget_units,
            metrics=dict(metrics or {}),
            checkpoint=checkpoint,
            failure_reason=failure,
        )

    off_loop = _active_event_loop_reason()
    if off_loop:
        return finish(MaintenanceStatus.SKIPPED, off_loop, failure=off_loop)

    # Checkpoint gating is decided SOLELY by decide_checkpoint, which
    # consults maintenance_gate internally and applies the WAL-pressure
    # emergency override when the gate is closed.  It must run BEFORE the
    # retention gate short-circuit below: a closed maintenance gate
    # previously returned SKIPPED here first, making the emergency override
    # unreachable and starving checkpoints while the WAL grew unbounded.
    decision = decide_checkpoint(snapshot, policy)
    checkpoint: Optional[CheckpointResult] = None
    if decision.should_run:
        checkpoint = perform_checkpoint(
            store,
            decision,
            policy,
            wall_clock_ms=clock,
            monotonic=monotonic_clock,
            wal_size_reader=wal_size_reader,
            reader_snapshot_provider=reader_snapshot_provider,
        )
        if checkpoint.status in {
            CheckpointStatus.BUSY,
            CheckpointStatus.FAILED,
            CheckpointStatus.PARTIAL,
        }:
            reason = f"checkpoint_{checkpoint.status.value.lower()}"
            failure = checkpoint.failure_reason
            return finish(
                MaintenanceStatus.PARTIAL,
                reason,
                checkpoint=checkpoint,
                failure=failure,
            )

    # Retention (and only retention) honors the plain maintenance gate.
    gate_reason = maintenance_gate(snapshot, policy)
    if gate_reason:
        if checkpoint is not None:
            # An emergency checkpoint ran even though normal maintenance is
            # gated; report the pass as PARTIAL so its result is recorded.
            return finish(
                MaintenanceStatus.PARTIAL, gate_reason, checkpoint=checkpoint)
        return finish(MaintenanceStatus.SKIPPED, gate_reason)
    if snapshot.active_readers > 0 or snapshot.long_reader_count > 0:
        return finish(
            MaintenanceStatus.SKIPPED,
            "active_or_long_reader",
            checkpoint=checkpoint,
        )

    deadline = started_mono + policy.retention_time_budget_ms / 1_000.0
    rows_deleted = 0
    rows_compacted = 0
    budget_units = 0
    protected_rows = 0
    chunks = 0
    retention_metrics: dict[str, int] = {}
    store_deadline_exhausted = False
    failure: Optional[str] = None
    while budget_units < policy.retention_row_budget:
        if monotonic_clock() >= deadline:
            break
        remaining = policy.retention_row_budget - budget_units
        requested = min(policy.retention_chunk_rows, remaining)
        try:
            step = _run_retention_step(
                store,
                now_ms=snapshot.now_ms,
                retention_ms=policy.retention_ms,
                max_rows=requested,
                deadline_monotonic=deadline,
                policy=policy,
            )
            consumed, deleted, compacted, protected = _retention_counts(step)
            if consumed > requested:
                raise RuntimeError("retention Store exceeded the requested row budget")
            if _protected_deletion_count(step) > 0:
                raise RuntimeError("retention Store reported protected-row deletion")
        except Exception as exc:  # noqa: BLE001 - structured worker result
            if _is_background_write_deferred(exc):
                return finish(
                    MaintenanceStatus.SKIPPED,
                    "critical_write_pending",
                    rows=rows_deleted,
                    protected=protected_rows,
                    chunks=chunks,
                    compacted=rows_compacted,
                    budget_units=budget_units,
                    metrics=retention_metrics,
                    checkpoint=checkpoint,
                )
            failure = _error_text(exc)
            break
        chunks += 1
        rows_deleted += deleted
        rows_compacted += compacted
        budget_units += consumed
        protected_rows += protected
        if isinstance(step, Mapping):
            raw_metrics = step.get("metrics", {})
            if isinstance(raw_metrics, Mapping):
                for key, value in raw_metrics.items():
                    normalized = _coerce_nonnegative_int(str(key), value)
                    retention_metrics[str(key)] = (
                        retention_metrics.get(str(key), 0) + normalized)
            store_deadline_exhausted = bool(step.get("deadline_exhausted", False))
        if consumed == 0 or store_deadline_exhausted:
            break

    row_exhausted = budget_units >= policy.retention_row_budget
    time_exhausted = store_deadline_exhausted or monotonic_clock() >= deadline
    if failure:
        return finish(
            MaintenanceStatus.FAILED,
            "retention_failed",
            rows=rows_deleted,
            protected=protected_rows,
            chunks=chunks,
            compacted=rows_compacted,
            budget_units=budget_units,
            metrics=retention_metrics,
            row_exhausted=row_exhausted,
            time_exhausted=time_exhausted,
            checkpoint=checkpoint,
            failure=failure,
        )
    status = (
        MaintenanceStatus.PARTIAL
        if row_exhausted or time_exhausted
        else MaintenanceStatus.COMPLETE
    )
    reason = (
        "row_budget_exhausted"
        if row_exhausted
        else "time_budget_exhausted"
        if time_exhausted
        else "bounded_pass_complete"
    )
    return finish(
        status,
        reason,
        rows=rows_deleted,
        protected=protected_rows,
        chunks=chunks,
        compacted=rows_compacted,
        budget_units=budget_units,
        metrics=retention_metrics,
        row_exhausted=row_exhausted,
        time_exhausted=time_exhausted,
        checkpoint=checkpoint,
    )


def _skip(snapshot: MaintenanceSnapshot, reason: str) -> CheckpointDecision:
    return CheckpointDecision(False, None, reason, snapshot)


def _escalation_due(
    snapshot: MaintenanceSnapshot, policy: MaintenancePolicy
) -> bool:
    """Whether the routine interval has elapsed since the last escalation.

    The reference is the last RESTART/TRUNCATE *attempt* of any outcome, so a
    BUSY or deferred reclaim still consumes its cadence slot and cannot spin.
    A caller that does not track escalations separately falls back to the last
    checkpoint attempt of any mode, which is exactly the historical behaviour.
    """

    reference = snapshot.last_escalation_attempt_ts_ms
    if reference is None:
        reference = snapshot.last_checkpoint_attempt_ts_ms
    if reference is None:
        return True
    if reference > snapshot.now_ms:
        return False
    return snapshot.now_ms - reference >= policy.checkpoint_min_interval_ms


def _safe_reader_snapshot(
    provider: Optional[Callable[[], Mapping[str, Any]]],
) -> Optional[dict[str, Any]]:
    """One bounded reader observation; a diagnostics fault must stay silent."""

    if provider is None:
        return None
    try:
        observed = provider()
    except Exception:  # noqa: BLE001 - diagnostics never fail a checkpoint
        return None
    return dict(observed) if isinstance(observed, Mapping) else None


def _live_reclaim_is_safe(
    snapshot: MaintenanceSnapshot, policy: MaintenancePolicy
) -> bool:
    """Whether a live TRUNCATE may run to actually reclaim WAL bytes.

    Every condition here is a database-level safety property, never a business
    one.  Open positions, exposure, and market windows do not make a WAL
    checkpoint unsafe; readers, a pending critical write, and an unhealthy
    writer do.
    """

    if snapshot.consecutive_no_progress_passive < (
            policy.no_progress_escalation_threshold):
        return False
    if snapshot.wal_bytes < policy.restart_trigger_bytes:
        return False
    # Event-loop responsiveness gate.  A live TRUNCATE rewrites the whole WAL
    # into the database; on a several-hundred-megabyte WAL that saturates the
    # same disk the runtime publishes its heartbeat and export to.  Measured on
    # a 64.7-minute soak, every heartbeat/export latency breach (up to 19.6 s
    # and 31.7 s) coincided with a reclaim against a 266-457 MB WAL.  When the
    # loop is already lagging, defer to the next cycle rather than compound it.
    #
    # This is a deferral, never an abandonment: once the WAL passes
    # ``lag_defer_max_wal_bytes`` the reclaim proceeds regardless, so WAL stays
    # bounded and reclaim stays reachable even under sustained lag.
    if (snapshot.event_loop_lag_ms >= policy.lag_defer_threshold_ms
            and snapshot.wal_bytes < policy.lag_defer_max_wal_bytes):
        return False
    # Reclaim size is bounded by keeping the WAL small in the first place --
    # ``pressure_checkpoint_min_interval_ms`` above -- rather than by a cap here.
    # A cap would contradict ``restart_trigger_bytes``, which is this policy's
    # documented escalation point, and would leave an oversized WAL unreclaimed.
    # A LONG reader (an integrity scan, a heavy report) holds its read-mark for
    # far longer than the bounded lock wait, so reclamation would stall behind
    # it: that is prohibited.  A short in-flight read is not, and must not be:
    # an oversized WAL slows every read, which keeps a reader in flight, which
    # would block reclamation forever.  A brief overlap simply returns BUSY
    # within live_reclaim_busy_timeout_ms and retries on the next cycle.
    if snapshot.long_reader_count:
        return False
    # Critical persistence must never queue behind reclamation.
    if snapshot.critical_queue_depth:
        return False
    if not snapshot.runtime_active:
        # The stopped path keeps its own stricter invariants below.
        return False
    # Database health only.  ``runtime_health`` is deliberately NOT consulted:
    # it folds in trade-gating conditions -- a partial market-data feed,
    # unreconciled maker evidence, exposure limits -- none of which describe
    # the database.  Every previous attempt to gate reclamation on it made
    # reclamation unreachable in a live run, because a live runtime is almost
    # never in a pristine overall health state.
    return snapshot.writer_healthy and snapshot.integrity_ok


def _restart_is_safe(snapshot: MaintenanceSnapshot, policy: MaintenancePolicy) -> bool:
    if snapshot.wal_bytes < policy.restart_trigger_bytes:
        return False
    if (
        snapshot.critical_queue_depth
        or snapshot.telemetry_queue_depth
        or snapshot.open_positions
        or snapshot.active_readers
        or snapshot.long_reader_count
    ):
        return False
    latency = snapshot.critical_commit_p95_ms
    if latency is None or latency > policy.restart_max_commit_p95_ms:
        return False
    if snapshot.runtime_active:
        if (
            not snapshot.writer_healthy
            or snapshot.normalized_health not in _ACTIVE_HEALTH
        ):
            return False
        boundary = snapshot.time_to_window_boundary_ms
        return boundary is not None and boundary > policy.restart_window_guard_ms
    return snapshot.normalized_health in _STOPPED_HEALTH


def _truncate_is_safe(snapshot: MaintenanceSnapshot, policy: MaintenancePolicy) -> bool:
    return (
        snapshot.wal_bytes >= policy.truncate_trigger_bytes
        and not snapshot.runtime_active
        and snapshot.normalized_health in _STOPPED_HEALTH
        and snapshot.critical_queue_depth == 0
        and snapshot.telemetry_queue_depth == 0
        and snapshot.open_positions == 0
        and snapshot.active_readers == 0
        and snapshot.long_reader_count == 0
    )


def _unsafe_mode_reason(
    mode: CheckpointMode,
    snapshot: MaintenanceSnapshot,
    policy: MaintenancePolicy,
    reason: str = "",
) -> Optional[str]:
    """Execution-time revalidation of mode-specific HARD safety only.

    General gating (queue depth, runtime health, latency, window guard) is
    the sole responsibility of decide_checkpoint, which also applies the
    WAL-pressure emergency override.  Re-running maintenance_gate here for
    PASSIVE mode contradicted that decision and vetoed every emergency
    checkpoint; PASSIVE never blocks the critical writer and needs no
    quiescence, so only TRUNCATE/RESTART invariants are rechecked.

    A TRUNCATE authorized as the live reclamation escalation is revalidated
    against the live invariants that authorized it, not the stopped-runtime
    ones -- rechecking the wrong contract would veto it unconditionally.
    """

    if mode is CheckpointMode.TRUNCATE and reason == LIVE_RECLAIM_REASON:
        if not _live_reclaim_is_safe(snapshot, policy):
            return "live_reclaim_requires_reader_free_runtime"
        return None
    if mode is CheckpointMode.TRUNCATE and not _truncate_is_safe(snapshot, policy):
        return "truncate_requires_verified_stopped_runtime"
    if mode is CheckpointMode.RESTART and not _restart_is_safe(snapshot, policy):
        return "restart_requires_quiescent_runtime"
    return None


def _invoke_reclaiming_checkpoint(
    store: Any,
    decision: CheckpointDecision,
    policy: MaintenancePolicy,
    monotonic_clock: Callable[[], float],
    sleep: Optional[Callable[[float], None]] = None,
) -> Any:
    """Invoke one checkpoint, retrying only a refused background-write gate.

    The Store takes the shared background-write gate before checkpointing, and
    that gate refuses whenever a critical command is in flight.  Under
    continuous critical writes one non-blocking attempt per maintenance cycle
    is refused essentially every time, so an escalated reclamation is decided
    but never executed and the WAL keeps growing -- the exact failure this
    escalation exists to prevent.

    Retrying inside a short bounded window slips the checkpoint into an
    ordinary gap between critical commands.  It never makes a critical write
    wait: a refusal means we did not start, and we simply try again.  Only the
    escalated reclamation retries; ordinary PASSIVE passes keep the original
    single-attempt behaviour and yield immediately.
    """

    assert decision.mode is not None
    if decision.reason not in _GATE_RETRY_REASONS:
        return _invoke_checkpoint(store, decision.mode, decision.reason)
    pause = sleep or time.sleep
    deadline = monotonic_clock() + policy.live_reclaim_gate_wait_ms / 1_000.0
    while True:
        try:
            return _invoke_checkpoint(
                store, decision.mode, decision.reason,
                busy_timeout_ms=(
                    policy.live_reclaim_busy_timeout_ms
                    if decision.reason == LIVE_RECLAIM_REASON else None
                ),
            )
        except Exception as exc:  # noqa: BLE001 - only gate refusals retry
            if not _is_background_write_deferred(exc):
                raise
            if monotonic_clock() >= deadline:
                raise
            pause(0.02)


def _invoke_checkpoint(
    store: Any, mode: CheckpointMode, reason: str,
    busy_timeout_ms: Optional[int] = None,
) -> Any:
    for name in ("run_wal_checkpoint", "checkpoint"):
        method = getattr(store, name, None)
        if callable(method):
            if name == "checkpoint":
                parameters = inspect.signature(method).parameters
                kwargs: dict[str, Any] = {}
                if "reason" in parameters:
                    kwargs["reason"] = reason
                # A bounded lock wait is only meaningful for a Store that
                # supports it; older/duck-typed Stores keep their own default.
                if busy_timeout_ms is not None and "busy_timeout_ms" in parameters:
                    kwargs["busy_timeout_ms"] = int(busy_timeout_ms)
                if "mode" in parameters:
                    response = method(mode=mode.value, **kwargs)
                elif kwargs:
                    response = method(mode.value, **kwargs)
                else:
                    response = method(mode.value)
            else:
                response = method(mode.value)
            if inspect.isawaitable(response):
                raise TypeError("maintenance Store checkpoint API must be synchronous")
            return response
    connection = getattr(store, "connection", None)
    if connection is None:
        raise TypeError("maintenance Store has no checkpoint API or connection")
    row = connection.execute(f"PRAGMA wal_checkpoint({mode.value})").fetchone()
    return row


def _normalize_checkpoint_response(response: Any) -> tuple[int, int, int]:
    if isinstance(response, Mapping):
        busy = response.get("busy_result", response.get("busy"))
        total = response.get("frames_total", response.get("log_frames"))
        checkpointed = response.get(
            "frames_checkpointed", response.get("checkpointed_frames")
        )
        values = (busy, total, checkpointed)
    else:
        try:
            values = (response[0], response[1], response[2])
        except (IndexError, KeyError, TypeError) as exc:
            raise ValueError("unexpected checkpoint response") from exc
    normalized = tuple(
        _coerce_nonnegative_int("checkpoint response", x) for x in values
    )
    busy, total, checkpointed = normalized
    if checkpointed > total:
        raise ValueError("checkpointed frames exceed total frames")
    return busy, total, checkpointed


def _record_checkpoint_result(store: Any, result: CheckpointResult) -> CheckpointResult:
    record = result.checkpoint_record()
    recorder = None
    for name in (
        "record_checkpoint_run",
        "record_checkpoint",
        "insert_checkpoint_run",
    ):
        candidate = getattr(store, name, None)
        if callable(candidate):
            recorder = candidate
            break
    if recorder is not None:
        try:
            response = recorder(record)
            if inspect.isawaitable(response):
                raise TypeError("checkpoint recorder must be synchronous")
            return replace(result, record_attempted=True, recorded=True)
        except Exception as exc:  # noqa: BLE001 - do not hide checkpoint result
            return replace(
                result,
                record_attempted=True,
                recorded=False,
                record_error=_error_text(exc),
            )

    inserter = getattr(store, "_insert", None)
    transaction = getattr(store, "transaction", None)
    if callable(inserter) and callable(transaction):
        try:
            with transaction(immediate=True) as connection:
                inserter("checkpoint_runs", record, conn=connection)
            return replace(result, record_attempted=True, recorded=True)
        except Exception as exc:  # noqa: BLE001 - preserve operation result
            return replace(
                result,
                record_attempted=True,
                recorded=False,
                record_error=_error_text(exc),
            )
    return result


def _run_retention_step(
    store: Any,
    *,
    now_ms: int,
    retention_ms: int,
    max_rows: int,
    deadline_monotonic: float,
    policy: MaintenancePolicy,
) -> Any:
    bounded = getattr(store, "bounded_retention_step", None)
    if callable(bounded):
        response = bounded(
            cutoff_ts_ms=max(0, now_ms - retention_ms),
            max_rows=max_rows,
            deadline_monotonic=deadline_monotonic,
            protect_trade_evidence=True,
            raw_event_max_rows=policy.raw_event_max_rows,
            event_bucket_detail_retention_ms=(
                policy.event_bucket_detail_retention_ms),
            metadata_retention_ms=policy.metadata_retention_ms,
            metadata_max_rows=policy.metadata_max_rows,
            journal_payload_retention_ms=(
                policy.journal_payload_retention_ms),
            now_ms=now_ms,
        )
        if inspect.isawaitable(response):
            raise TypeError("maintenance Store retention API must be synchronous")
        return response

    compact = getattr(store, "compact_raw_evidence", None)
    if not callable(compact):
        raise TypeError("maintenance Store has no bounded retention API")
    # The current Store contract applies the batch limit independently to three
    # raw tables. Keep their combined theoretical maximum within this pass's
    # remaining row budget.
    per_table = max_rows // 3
    if per_table < 1:
        return {"rows_deleted": 0, "pinned_rows_skipped": 0}
    response = compact(
        now_ms,
        retention_ms=retention_ms,
        batch_size=per_table,
        run_integrity=False,
    )
    if inspect.isawaitable(response):
        raise TypeError("maintenance Store retention API must be synchronous")
    return response


def _retention_counts(response: Any) -> tuple[int, int, int, int]:
    if isinstance(response, int) and not isinstance(response, bool):
        deleted = _coerce_nonnegative_int("rows_deleted", response)
        return deleted, deleted, 0, 0
    if not isinstance(response, Mapping):
        raise TypeError("bounded retention response must be a mapping or int")
    if "rows_deleted" in response:
        deleted = _coerce_nonnegative_int("rows_deleted", response["rows_deleted"])
    else:
        deleted = sum(
            _coerce_nonnegative_int(name, response.get(name, 0))
            for name in (
                "source_events",
                "book_snapshots",
                "cex_observations",
                "event_buckets",
            )
        )
    protected = _coerce_nonnegative_int(
        "protected_rows_skipped",
        response.get("protected_rows_skipped", response.get("pinned_rows_skipped", 0)),
    )
    compacted = _coerce_nonnegative_int(
        "rows_compacted", response.get("rows_compacted", 0))
    consumed = _coerce_nonnegative_int(
        "budget_units", response.get("budget_units", deleted + compacted))
    if consumed < deleted + compacted:
        raise ValueError("retention budget units omit deleted/compacted rows")
    return consumed, deleted, compacted, protected


def _protected_deletion_count(response: Any) -> int:
    if not isinstance(response, Mapping):
        return 0
    return sum(
        _coerce_nonnegative_int(name, response.get(name, 0))
        for name in (
            "trade_evidence_deleted",
            "permanent_rows_deleted",
            "protected_rows_deleted",
        )
    )


def _read_wal_bytes(
    store: Any, *, fallback: int, reader: Optional[Callable[[], int]]
) -> int:
    if reader is not None:
        return _coerce_nonnegative_int("WAL size", reader())
    for name in ("wal_size_bytes", "get_wal_size_bytes"):
        method = getattr(store, name, None)
        if callable(method):
            return _coerce_nonnegative_int("WAL size", method())
    path = getattr(store, "path", None)
    if path is None:
        return _coerce_nonnegative_int("WAL size fallback", fallback)
    wal_path = Path(f"{Path(path)}-wal")
    return wal_path.stat().st_size if wal_path.exists() else 0


def _safe_wal_bytes(
    store: Any, *, fallback: int, reader: Optional[Callable[[], int]]
) -> int:
    try:
        return _read_wal_bytes(store, fallback=fallback, reader=reader)
    except Exception:  # noqa: BLE001 - error is already represented by caller
        return fallback


def _database_bytes(store: Any) -> int:
    method = getattr(store, "database_file_size_bytes", None)
    if callable(method):
        try:
            return _coerce_nonnegative_int("database size", method())
        except Exception:  # noqa: BLE001 - accounting remains valid with zero
            return 0
    path = getattr(store, "path", None)
    if path is None:
        return 0
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0


def _active_event_loop_reason() -> Optional[str]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return None
    return "dedicated_maintenance_worker_required"


def _wall_clock_ms() -> int:
    return int(time.time() * 1_000)


def snapshot_wal(decision: CheckpointDecision) -> int:
    return decision.snapshot.wal_bytes


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}:{exc}"[:512]


def _is_background_write_deferred(exc: BaseException) -> bool:
    return type(exc).__name__ == "V4BackgroundWriteDeferred"


def _require_int(name: str, value: Any, *, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")


def _coerce_nonnegative_int(name: str, value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{name} must not be bool")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{name} must be integer-like") from exc
    if normalized < 0 or (isinstance(value, float) and not value.is_integer()):
        raise ValueError(f"{name} must be a non-negative integer")
    return normalized


def _coerce_nonnegative_float(name: str, value: Any) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{name} must not be bool")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{name} must be numeric") from exc
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return normalized


def _require_finite_positive(name: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    if not math.isfinite(float(value)) or float(value) <= 0:
        raise ValueError(f"{name} must be finite and > 0")


__all__ = [
    "CheckpointDecision",
    "CheckpointMode",
    "CheckpointResult",
    "CheckpointStatus",
    "MaintenancePassResult",
    "MaintenancePolicy",
    "MaintenanceSnapshot",
    "MaintenanceStatus",
    "decide_checkpoint",
    "maintenance_gate",
    "perform_checkpoint",
    "run_bounded_maintenance_pass",
]
