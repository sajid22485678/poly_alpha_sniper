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

Non-critical loss policy (explicit and bounded)
-----------------------------------------------
This lane may coalesce, deduplicate, or drop rows.  It does so only in these
documented cases, each with its own counter:

``coalesced``/``deduplicated``
    Repeated identical state within ``coalescing_interval_s``, an explicit
    duplicate ``dedupe_key``, or a deterministic ``bucket_key`` merge.
``sampled``/``deferred``
    An explicitly classified non-critical row was sampled to measured sink
    capacity, or an aggregate was deferred for caller retry.  Neither is
    reported as unknown loss.
``dropped`` (``telemetry_queue_full``)
    Admission overflow once ``capacity`` rows are already pending.
``dropped`` (batch failure)
    Exactly the rows of the one physical chunk whose sink transaction rolled
    back.  A dispatch never carries more than one physical chunk, so a single
    failure can never destroy rows it did not attempt.
``dropped`` (``telemetry_submit_after_stop``/``telemetry_shutdown_discard``)
    Admission or drain after ``stop()``.

A cooperative *priority skip* is explicitly not loss: the sink yields to
critical persistence before writing anything, so those rows are requeued and
only counted in ``priority_skipped_batches``/``priority_skipped_rows``.  It is
designed backpressure and never marks the lane unhealthy.

Two bounded mechanisms keep repeated sink pressure from becoming an unbounded
retry loop:

*Deadline backoff* — after a deadline-exceeded chunk the next flush is deferred
for an exponentially growing window (capped at 2 s).  Admission keeps running;
only dispatch waits.  A clean commit clears it.

*Throughput-aware physical-chunk sizing* — a rolling current window separates
the deadline-safe chunk, the service floor needed to stabilize/drain the queue,
and the physical maximum.  Transaction hold-time tails bound the safe side;
admitted/committed rates and queue slope drive the service side.  In-range
growth is gradual, deadline pressure shrinks immediately, overload uses only
caller-selected non-critical policies, and a bounded healthy window restores
operational health without erasing lifetime audit counters.
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
    SAMPLED = "SAMPLED"
    DEFERRED = "DEFERRED"
    DROPPED = "DROPPED"


class TelemetryLossCategory(str, Enum):
    """Terminal outcome for a logical row that is not committed or buffered.

    The categories are mutually exclusive and jointly exhaustive: every row the
    lane stops carrying is attributed to exactly one, so the conservation
    identity in :meth:`V4TelemetryWriter.reconcile` closes exactly.

    The first four are *approved policy* outcomes.  They are deliberate,
    deterministic, and individually accounted, so they do not by themselves make
    the lane unsafe -- but they are always reported, never hidden.  The last
    group is unexpected loss and stays operationally blocking.
    """

    # --- approved policy outcomes (noncritical only) ----------------------
    POLICY_SAMPLED = "POLICY_SAMPLED"
    POLICY_COALESCED = "POLICY_COALESCED"
    POLICY_DEDUPLICATED = "POLICY_DEDUPLICATED"
    # --- unexpected loss (blocking) ---------------------------------------
    QUEUE_OVERFLOW = "QUEUE_OVERFLOW"
    DEADLINE_EXPIRED = "DEADLINE_EXPIRED"
    SINK_FAILURE = "SINK_FAILURE"
    ACKNOWLEDGEMENT_FAILURE = "ACKNOWLEDGEMENT_FAILURE"
    MALFORMED_ROW = "MALFORMED_ROW"
    SHUTDOWN_ABANDONED = "SHUTDOWN_ABANDONED"


#: Categories that represent an approved, deterministic policy decision rather
#: than loss of evidence the lane was expected to carry.
POLICY_LOSS_CATEGORIES: frozenset[str] = frozenset({
    TelemetryLossCategory.POLICY_SAMPLED.value,
    TelemetryLossCategory.POLICY_COALESCED.value,
    TelemetryLossCategory.POLICY_DEDUPLICATED.value,
})

#: Categories that remain operationally blocking.
UNEXPECTED_LOSS_CATEGORIES: frozenset[str] = frozenset(
    category.value for category in TelemetryLossCategory
) - POLICY_LOSS_CATEGORIES


class TelemetryCapacityState(str, Enum):
    """How the lane is currently coping with offered load.

    Capacity is reported separately from data safety.  A lane that is sampling
    noncritical rows under an approved policy is honestly *not* within capacity,
    but it is not unsafe either; conflating the two is what previously pinned
    ``operational_ready`` false forever under sustained load.
    """

    WITHIN_CAPACITY = "WITHIN_CAPACITY"
    POLICY_SAMPLING_ACTIVE = "POLICY_SAMPLING_ACTIVE"
    POLICY_COALESCING_ACTIVE = "POLICY_COALESCING_ACTIVE"
    POLICY_DEFER_ACTIVE = "POLICY_DEFER_ACTIVE"
    HARD_OVERLOAD = "HARD_OVERLOAD"
    UNKNOWN = "UNKNOWN"


class TelemetryOverloadPolicy(str, Enum):
    """Explicit non-critical behavior once service pressure is detected.

    The telemetry lane never carries authoritative trade evidence.  Callers
    still opt into the exact overload behavior so pressure cannot silently
    change the retention contract:

    ``ADMIT``
        Preserve the legacy behavior and admit until the hard queue bound.
    ``LATEST``
        Under pressure, replace the newest still-pending row for the same key.
    ``SAMPLE``
        Under pressure, retain a deterministic capacity-sized sample.
    ``DEFER``
        Refuse admission without loss so the caller can keep its aggregate and
        retry later.
    """

    ADMIT = "ADMIT"
    LATEST = "LATEST"
    SAMPLE = "SAMPLE"
    DEFER = "DEFER"


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
    overload_key: Optional[str] = None
    requeue_attempts: int = 0


# Controller constants are intentionally module-local and deterministic.  The
# queue thresholds are derived from the physical batch cap below so production
# pressure is handled before the maintenance policy's 256-row gate.
_RATE_WINDOW_S = 15
_COST_WINDOW_S = 20.0
# The cost model is bounded by both wall time and recent transactions.  Under
# sustained high throughput, transaction count is the faster clock: retaining
# thousands of obsolete slow-size samples can pin the inferred marginal cost
# after the sink has demonstrably recovered.
_MAX_COST_SAMPLES = 256
_DEADLINE_BUDGET_FRACTION = 0.80
_THROUGHPUT_MARGIN = 1.20
_DRAIN_HORIZON_S = 10.0
_UP_STEP_FRACTION = 0.25
_UP_HEADROOM_WINDOWS = 5
_OVERLOAD_ENTER_WINDOWS = 2
_OVERLOAD_EXIT_WINDOWS = 5
_RECOVERY_HEALTHY_WINDOWS = 10
_RECOVERY_SETTLE_S = max(float(_RATE_WINDOW_S), _COST_WINDOW_S)
_MIN_SAMPLING_KEEP_RATIO = 0.05
_SAMPLING_CAPACITY_RESERVE = 0.90


@dataclass(slots=True)
class _RateBucket:
    tick: int = -1
    incoming: int = 0
    offered: int = 0
    admitted: int = 0
    committed: int = 0
    logical_committed: int = 0
    coalesced: int = 0
    sampled: int = 0
    deferred: int = 0
    overload_handled: int = 0
    lost: int = 0
    # Unexpected loss of rows that had already been counted in ``admitted``.
    # ``lost`` is the whole of it -- including rows refused *before* admission
    # (submit-after-stop, a failed merge, hard queue overflow) -- and stays the
    # health signal.  The conservation identity is over the admitted cohort, so
    # it may only subtract the rows that actually entered that cohort; charging
    # a pre-admission refusal against it reports a shortfall for a row the
    # window never counted as inflow.
    lost_admitted: int = 0
    # Logical rows that were *admitted* and then left the lane under an approved
    # policy.  Deliberately distinct from ``overload_handled``, which also counts
    # pre-admission policy decisions: only rows counted in ``admitted`` may
    # appear here, so this term can never mask a genuine conservation gap.
    policy_resolved: int = 0
    # Logical rows merged into an *already-queued* pending.  They raise that
    # pending's logical count -- so they enter the lane's inventory and are
    # committed with it -- but they are never counted in ``admitted``.  They are
    # therefore a second, independent inflow to the identity, and the only one
    # of the four ``coalesced`` paths that touches inventory at all: the dedupe,
    # state and LATEST paths all resolve the row without it ever being held.
    aggregated: int = 0
    admission_overflow: int = 0
    dispatch_attempts: int = 0
    dispatch_successes: int = 0
    # Wall time the aggregation thread actually spent inside a dispatch, in
    # milliseconds.  This is service time, not elapsed time: it excludes every
    # interval the lane was merely holding queued rows while deliberately
    # waiting for a batch to fill.
    dispatch_busy_ms: float = 0.0
    failed_batches: int = 0
    deadline_failures: int = 0
    priority_deferrals: int = 0
    checkpoint_deferrals: int = 0
    first_depth: Optional[int] = None
    last_depth: Optional[int] = None
    max_depth: int = 0
    min_depth: Optional[int] = None


@dataclass(frozen=True, slots=True)
class _WindowView:
    elapsed_s: float
    incoming: int
    offered: int
    admitted: int
    committed: int
    logical_committed: int
    coalesced: int
    sampled: int
    deferred: int
    overload_handled: int
    lost: int
    lost_admitted: int
    policy_resolved: int
    aggregated: int
    admission_overflow: int
    dispatch_attempts: int
    dispatch_successes: int
    failed_batches: int
    deadline_failures: int
    priority_deferrals: int
    checkpoint_deferrals: int
    incoming_rps: float
    offered_rps: float
    admitted_rps: float
    committed_rps: float
    logical_committed_rps: float
    dispatch_attempt_rps: float
    dispatch_success_rps: float
    queue_slope_rps: float
    backlogged_seconds: float
    # Seconds of real dispatch service time in the window.  ``backlogged_seconds``
    # answers "how long did the lane hold rows?"; this answers "how long was the
    # lane actually working?", which is the only one of the two that bounds
    # throughput.
    dispatch_busy_s: float
    first_depth: Optional[int]
    last_depth: Optional[int]
    max_depth: int
    # Drainage floor: the shallowest depth reached in each half of the window.
    # A queue that keeps draining has a flat floor however spiky its peaks; a
    # queue that is genuinely accumulating has a *rising* floor.  That is the
    # signal which separates bounded oscillation from real backlog growth.
    first_half_min_depth: Optional[int]
    second_half_min_depth: Optional[int]


@dataclass(frozen=True, slots=True)
class _ConservationSnapshot:
    """Running identity totals plus observed inventory, at one instant.

    The five counters are lifetime running totals maintained by the window; the
    inventory is what the lane independently believes it is still holding.  The
    two are maintained by completely separate code paths, which is exactly why
    comparing them detects drift: a counter that is not matched by an inventory
    move (or the reverse) is a row the lane cannot account for.

    Captured atomically under the writer's lock, so every field describes the
    same instant.  That is what makes a difference between two snapshots an
    exact interval measurement rather than a comparison of things sampled at
    different times.
    """

    tick: int
    admitted: int
    aggregated: int
    logical_committed: int
    lost_admitted: int
    policy_resolved: int
    inventory: int

    @property
    def gap(self) -> int:
        """Rows admitted (or merged in) that the lane can no longer account for.

        Zero in a correct lane at every instant: an admitted row is committed,
        lost, resolved by policy, or still held.  Non-zero means the counters
        and the inventory disagree, which is indistinguishable from silent loss.
        """

        return (
            self.admitted + self.aggregated
            - self.logical_committed - self.lost_admitted
            - self.policy_resolved
            - self.inventory
        )


#: The lane's state before it did anything: nothing admitted, nothing held.  It
#: is the exact boundary for a window that reaches back past the first recorded
#: observation, so a young lane needs no special case.
_CONSERVATION_ORIGIN = _ConservationSnapshot(
    tick=-1, admitted=0, aggregated=0, logical_committed=0,
    lost_admitted=0, policy_resolved=0, inventory=0,
)


@dataclass(frozen=True, slots=True)
class _ConservationResult:
    """One evaluation of the admitted-cohort conservation identity.

    ``residual`` is the mission-critical number:

        admitted_window + aggregated_window
          - committed_window - lost_admitted_window - policy_resolved_window
          - (ending_inventory - starting_inventory)

    Every term is measured across the *same* boundaries -- the opening snapshot
    and now -- so there is no cohort mismatch to hide behind.  It must be
    exactly zero.  A positive residual is missing rows; a negative residual is
    rows accounted for twice, or inventory that grew without an inflow.  Both
    are accounting failures and both fail.
    """

    window_start_tick: int
    window_end_tick: int
    boundary_observed: bool
    admitted_window: int
    aggregated_window: int
    committed_window: int
    lost_admitted_window: int
    policy_resolved_window: int
    starting_inventory: int
    ending_inventory: int
    residual: int
    absolute_residual: int

    @property
    def balanced(self) -> bool:
        """Exact on both counts: no gap in this window, and none inherited.

        The runtime predicate uses only ``residual`` -- see
        :func:`_service_balanced` for why.  This is the stronger statement the
        certification contract proves against a real run.
        """

        return self.residual == 0 and self.absolute_residual == 0


def _half_min(rows: Sequence[_RateBucket], *, first: bool) -> Optional[int]:
    """Shallowest observed depth in one half of the window, or None.

    Only buckets that actually observed a depth contribute; a silent second
    carries no information about the floor.
    """

    observed = [bucket for bucket in rows if bucket.min_depth is not None]
    if len(observed) < 2:
        return None
    midpoint = len(observed) // 2
    half = observed[:midpoint] if first else observed[midpoint:]
    if not half:
        return None
    return min(int(bucket.min_depth) for bucket in half)


class _RollingTelemetryWindow:
    """Fixed one-second ring used by the live controller.

    Admission stays O(1), memory is fixed, and current decisions never depend
    on lifetime averages.  The class accepts explicit monotonic timestamps so
    its math can be tested without sleeping.
    """

    _COUNTER_FIELDS = frozenset({
        "incoming", "offered", "admitted", "committed", "logical_committed",
        "coalesced", "sampled", "deferred", "lost", "lost_admitted",
        "overload_handled", "policy_resolved", "aggregated",
        "admission_overflow", "dispatch_attempts", "dispatch_successes",
        "failed_batches", "deadline_failures", "priority_deferrals",
        "checkpoint_deferrals",
    })

    #: The five counters of the admitted-cohort conservation identity, in the
    #: order they appear in it.  Two inflows to the lane's inventory, three
    #: outflows.  Every one is maintained by the same ``add`` path, so a
    #: snapshot of their running totals is internally consistent by
    #: construction.
    _IDENTITY_FIELDS = (
        "admitted", "aggregated",
        "logical_committed", "lost_admitted", "policy_resolved",
    )

    def __init__(self, *, window_s: int = _RATE_WINDOW_S,
                 started_monotonic: Optional[float] = None) -> None:
        if isinstance(window_s, bool) or not isinstance(window_s, int) or window_s < 3:
            raise ValueError("telemetry rate window must be an integer >= 3")
        self.window_s = int(window_s)
        self.started_monotonic = (
            time.monotonic() if started_monotonic is None
            else float(started_monotonic)
        )
        self._buckets = [_RateBucket() for _ in range(self.window_s)]
        # Lifetime running totals of the identity counters.  The ring only
        # remembers ``window_s`` seconds, but the identity needs differences
        # taken against an arbitrary earlier instant, so these are kept whole.
        # Python integers do not wrap, so there is no overflow to reason about.
        self._identity_totals: dict[str, int] = {
            name: 0 for name in self._IDENTITY_FIELDS}
        # Opening conservation snapshot per second, keyed by tick.  One entry
        # per second of the window plus a small margin, pruned on every record,
        # so memory is fixed.
        self._conservation: dict[int, _ConservationSnapshot] = {}
        # ``view`` is a pure function of the ring and the tick asked for, and
        # the controller asks for it twice on every submission.  Each call sorts
        # the ring, sums seventeen counters across it and fits a regression, so
        # the repetition is measurable on the event loop.  Revision + tick
        # identify the exact ring state a view was built from.
        self._revision = 0
        self._view_cache: dict[bool, tuple[tuple[int, int], _WindowView]] = {}

    def _bucket(self, now: float) -> _RateBucket:
        tick = math.floor(float(now))
        index = tick % self.window_s
        bucket = self._buckets[index]
        if bucket.tick != tick:
            bucket = _RateBucket(tick=tick)
            self._buckets[index] = bucket
            # Recycling a stale slot changes which seconds the window covers.
            self._revision += 1
        return bucket

    def add(self, now: float, **deltas: int) -> None:
        unknown = set(deltas) - self._COUNTER_FIELDS
        if unknown:
            raise ValueError(f"unknown telemetry rate fields: {sorted(unknown)}")
        bucket = self._bucket(now)
        for name, value in deltas.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"telemetry rate delta {name} must be non-negative")
            if value:
                setattr(bucket, name, int(getattr(bucket, name)) + int(value))
                if name in self._identity_totals:
                    self._identity_totals[name] += int(value)
                self._revision += 1

    def conservation_now(
        self, now: float, *, queued_logical: int, inflight_logical: int,
    ) -> _ConservationSnapshot:
        """The identity totals and the observed inventory, right now."""

        return _ConservationSnapshot(
            tick=math.floor(float(now)),
            inventory=(
                max(0, int(queued_logical)) + max(0, int(inflight_logical))),
            **self._identity_totals,
        )

    def record_conservation(
        self, now: float, *, queued_logical: int, inflight_logical: int,
    ) -> _ConservationSnapshot:
        """Remember where this second *opened*, and return the live state.

        Only the first observation in a second is kept.  That is deliberate: the
        boundary of a rolling window is the moment the second began, and the
        first observation in it is the closest instant to that moment at which
        the lane is known to be consistent.  Every later observation in the same
        second is then measured *against* that boundary rather than replacing
        it, which is what stops the identity collapsing into a comparison of
        now against now.
        """

        live = self.conservation_now(
            now, queued_logical=queued_logical,
            inflight_logical=inflight_logical)
        self._conservation.setdefault(live.tick, live)
        if len(self._conservation) > self.window_s + 2:
            horizon = live.tick - self.window_s - 1
            for tick in [t for t in self._conservation if t < horizon]:
                del self._conservation[tick]
        return live

    def conservation(
        self, now: float, *, queued_logical: int, inflight_logical: int,
    ) -> _ConservationResult:
        """Evaluate the admitted-cohort conservation identity exactly.

        The window's boundary is the oldest opening snapshot still inside it.
        Every term is then a difference between that snapshot and the live one,
        so the counters and the inventory are read across identical boundaries:

            admitted + aggregated
              == committed + lost_admitted + policy_resolved
                 + ending_inventory - starting_inventory

        Rearranged, the residual must be exactly zero.  Both signs fail.  A
        surplus of pre-window inventory cannot mask a lost current-window row,
        because that inventory appears in ``starting_inventory`` and
        ``ending_inventory`` alike and cancels; only what the window itself did
        survives the subtraction.

        ``absolute_residual`` is the same identity taken from the lane's origin
        instead of the window boundary.  It answers a different question -- "is
        there any unaccounted row at all, however old?" -- and is reported
        separately so an operator can tell a fresh gap from an inherited one.
        """

        live = self.conservation_now(
            now, queued_logical=queued_logical,
            inflight_logical=inflight_logical)
        earliest = live.tick - self.window_s + 1
        opening = _CONSERVATION_ORIGIN
        observed = False
        for tick in range(earliest, live.tick + 1):
            candidate = self._conservation.get(tick)
            if candidate is not None:
                opening, observed = candidate, True
                break
        admitted = live.admitted - opening.admitted
        aggregated = live.aggregated - opening.aggregated
        committed = live.logical_committed - opening.logical_committed
        lost_admitted = live.lost_admitted - opening.lost_admitted
        policy_resolved = live.policy_resolved - opening.policy_resolved
        return _ConservationResult(
            window_start_tick=opening.tick,
            window_end_tick=live.tick,
            boundary_observed=observed,
            admitted_window=admitted,
            aggregated_window=aggregated,
            committed_window=committed,
            lost_admitted_window=lost_admitted,
            policy_resolved_window=policy_resolved,
            starting_inventory=opening.inventory,
            ending_inventory=live.inventory,
            residual=(
                admitted + aggregated
                - committed - lost_admitted - policy_resolved
                - (live.inventory - opening.inventory)
            ),
            absolute_residual=live.gap,
        )

    def observe_dispatch_busy(self, now: float, duration_ms: float) -> None:
        """Accumulate real dispatch service time into the current second."""

        if (isinstance(duration_ms, bool)
                or not isinstance(duration_ms, (int, float))
                or not math.isfinite(float(duration_ms))
                or float(duration_ms) < 0):
            raise ValueError("dispatch busy duration must be finite and non-negative")
        value = float(duration_ms)
        if value <= 0.0:
            return
        bucket = self._bucket(now)
        bucket.dispatch_busy_ms += value
        self._revision += 1

    def observe_depth(self, now: float, depth: int) -> None:
        value = max(0, int(depth))
        bucket = self._bucket(now)
        # Only a depth that actually moves one of the four tracked statistics
        # changes any view built from this ring.  A burst of submissions
        # observing the same depth within one second is the common case, and
        # treating those as changes would defeat the view cache entirely.
        if bucket.first_depth is None:
            bucket.first_depth = value
            self._revision += 1
        if bucket.last_depth != value:
            bucket.last_depth = value
            self._revision += 1
        if value > bucket.max_depth:
            bucket.max_depth = value
            self._revision += 1
        if bucket.min_depth is None or value < bucket.min_depth:
            bucket.min_depth = value
            self._revision += 1

    def view(self, now: float, *, include_current: bool = True) -> _WindowView:
        current_tick = math.floor(float(now))
        latest = current_tick if include_current else current_tick - 1
        cached = self._view_cache.get(include_current)
        if cached is not None and cached[0] == (self._revision, latest):
            return cached[1]
        earliest = latest - self.window_s + 1
        rows = sorted(
            (bucket for bucket in self._buckets
             if earliest <= bucket.tick <= latest),
            key=lambda bucket: bucket.tick,
        )
        elapsed = min(float(self.window_s), max(
            1.0,
            float(latest - math.floor(self.started_monotonic) + 1),
        ))
        totals = {
            name: sum(int(getattr(bucket, name)) for bucket in rows)
            for name in self._COUNTER_FIELDS
        }
        # Depth is observed on admission/commit events, not on a timer, so the
        # raw series is unevenly sampled: a busy second contributes a mid-burst
        # depth while a quiet second contributes nothing at all.  Regressing over
        # only the buckets that happen to carry an observation reported steep
        # "growth" for a queue that provably returned to empty every second.
        # Carry the last known depth forward across silent buckets so the series
        # is uniform in time and the slope means what it claims to mean.
        points: list[tuple[float, float]] = []
        carried: Optional[float] = None
        for bucket in rows:
            if bucket.last_depth is not None:
                carried = float(bucket.last_depth)
            if carried is not None:
                points.append((float(bucket.tick), carried))
        slope = 0.0
        if len(points) >= 3:
            mean_x = sum(point[0] for point in points) / len(points)
            mean_y = sum(point[1] for point in points) / len(points)
            denominator = sum((point[0] - mean_x) ** 2 for point in points)
            if denominator > 0:
                slope = sum(
                    (point[0] - mean_x) * (point[1] - mean_y)
                    for point in points
                ) / denominator
        backlogged = float(sum(bucket.max_depth > 0 for bucket in rows))
        busy_s = sum(
            max(0.0, float(bucket.dispatch_busy_ms)) for bucket in rows
        ) / 1_000.0
        first_depth = next(
            (bucket.first_depth for bucket in rows
             if bucket.first_depth is not None),
            None,
        )
        last_depth = next(
            (bucket.last_depth for bucket in reversed(rows)
             if bucket.last_depth is not None),
            None,
        )
        built = _WindowView(
            elapsed_s=elapsed,
            incoming=totals["incoming"],
            offered=totals["offered"],
            admitted=totals["admitted"],
            committed=totals["committed"],
            logical_committed=totals["logical_committed"],
            coalesced=totals["coalesced"],
            sampled=totals["sampled"],
            deferred=totals["deferred"],
            overload_handled=totals["overload_handled"],
            lost=totals["lost"],
            lost_admitted=totals["lost_admitted"],
            policy_resolved=totals["policy_resolved"],
            aggregated=totals["aggregated"],
            admission_overflow=totals["admission_overflow"],
            dispatch_attempts=totals["dispatch_attempts"],
            dispatch_successes=totals["dispatch_successes"],
            failed_batches=totals["failed_batches"],
            deadline_failures=totals["deadline_failures"],
            priority_deferrals=totals["priority_deferrals"],
            checkpoint_deferrals=totals["checkpoint_deferrals"],
            incoming_rps=totals["incoming"] / elapsed,
            offered_rps=totals["offered"] / elapsed,
            admitted_rps=totals["admitted"] / elapsed,
            committed_rps=totals["committed"] / elapsed,
            logical_committed_rps=totals["logical_committed"] / elapsed,
            dispatch_attempt_rps=totals["dispatch_attempts"] / elapsed,
            dispatch_success_rps=totals["dispatch_successes"] / elapsed,
            queue_slope_rps=float(slope),
            backlogged_seconds=backlogged,
            dispatch_busy_s=busy_s,
            first_depth=first_depth,
            last_depth=last_depth,
            max_depth=max((bucket.max_depth for bucket in rows), default=0),
            first_half_min_depth=_half_min(rows, first=True),
            second_half_min_depth=_half_min(rows, first=False),
        )
        self._view_cache[include_current] = ((self._revision, latest), built)
        return built


def _capacity_state(
    decision: "_ControlDecision", *, queue_depth: int, capacity: int,
) -> str:
    """Report how the lane is coping with offered load, honestly.

    Policy sampling is never reported as "within capacity" -- the lane really is
    shedding noncritical rows and an operator must be able to see that.  It is
    also not reported as hard overload, because the shedding is deliberate,
    bounded and fully accounted.
    """

    view = decision.controller_view
    # Hard overload means the lane is genuinely out of control -- not merely
    # that it is shedding.  Shedding under an approved policy is the lane
    # working as designed, and reporting it as hard overload is what made a
    # correctly-behaving runtime permanently unready.
    # Depth alone is not the signal.  Crossing the high-water mark is what
    # *triggers* the shedding policy, and even sitting at the hard bound is the
    # bound holding, so long as the policy keeps resolving admissions without
    # loss.  The lane is out of control exactly when that stops being true:
    # rows are lost, admissions overflow outside policy, or the sink misses its
    # cooperative deadline.  Each of those fires precisely when the bound fails.
    # Hard overload means control was *lost*, which is evidenced by rows the
    # lane could not keep: unexpected loss, or an admission that overflowed
    # outside policy.
    #
    # A cooperative deadline miss on its own is deliberately not enough.
    # Measured on the 64.7-minute soak, all three HARD_OVERLOAD samples had a
    # queue depth of 2-9 rows out of 20 000, zero unexpected loss and zero
    # reconciliation mismatch: the sink missed a deadline, the controller shrank
    # the chunk and requeued the batch, and every row still committed.  That is
    # the bounded-retry contract working, not a lane out of control -- and
    # reporting it as hard overload made a healthy runtime unready.
    # A deadline miss that actually costs rows still shows up through ``lost``.
    _ = queue_depth, capacity
    out_of_control = (
        int(view.lost) > 0                       # unexpected loss
        or int(view.admission_overflow) > 0      # overflow outside policy
    )
    if out_of_control:
        return TelemetryCapacityState.HARD_OVERLOAD.value
    if decision.overload_active or decision.sampling_keep_ratio < 1.0:
        if decision.sampling_keep_ratio < 1.0:
            return TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value
        if int(view.deferred) > 0:
            return TelemetryCapacityState.POLICY_DEFER_ACTIVE.value
        if int(view.coalesced) > 0:
            return TelemetryCapacityState.POLICY_COALESCING_ACTIVE.value
        return TelemetryCapacityState.POLICY_SAMPLING_ACTIVE.value
    return TelemetryCapacityState.WITHIN_CAPACITY.value


def _service_balanced(result: _ConservationResult) -> bool:
    """Is every logical row the window took in accounted for, exactly?

    The predicate this replaced compared window-scoped inflow against
    window-scoped outflow *plus instantaneous inventory*, and accepted any
    surplus::

        committed_w + lost_w + policy_resolved_w + queued_now + inflight_now
            >= admitted_w

    Two independent defects, either of which is enough to hide real loss.

    **Cohort mismatch.**  ``queued_now`` and ``inflight_now`` count every row
    the lane is holding, including rows admitted long before the window opened.
    That backlog is inflow the window never counted, so it inflated the
    left-hand side by an amount unrelated to anything the window did.

    **One-sidedness.**  Only a positive deficit failed.  Since the mismatched
    backlog can only ever make the left side bigger, the two defects compose:
    with a hundred rows of pre-window backlog, ten rows admitted and all ten
    silently lost inside the window, the comparison is ``0 + 0 + 0 + 100 >= 10``
    -- true -- and the computed deficit is ``-90``, which the evaluator then
    reported as "deficit = 0".  Measured on the preserved final evidence, 19,168
    of the definitive soak's 20,312 in-window ticks were negative surplus of
    exactly this kind, so "conservation closed exactly" was never actually
    demonstrated on a single one of them.

    The identity here is cohort-matched on both sides.  Inventory enters it as a
    *difference* between the window's boundaries, so a constant backlog cancels
    and cannot mask anything; and the residual must be exactly zero, so rows
    counted twice fail just as loudly as rows lost.  See
    :meth:`_RollingTelemetryWindow.conservation` for the identity itself.

    ``absolute_residual`` -- the same identity taken from the lane's origin --
    is deliberately *not* part of this predicate, though it is recorded on every
    tick and the certification contract requires it to be zero throughout a run.
    The distinction is about who owns the two sides.  Inventory is maintained by
    the writer; the counters are maintained here.  Only when the same writer
    owns both is their lifetime difference meaningful, and the controller is
    also driven directly -- by tests, and by callers that pass a documented
    lower bound for ``queued_logical`` -- where a fabricated inventory would
    make the lifetime figure say nothing about correctness.  The window residual
    has no such weakness: a fabricated inventory that is merely *stable* cancels
    out, and one that moves without a matching counter is caught.  So the
    runtime predicate is the window identity, and the lifetime identity is
    proved against real runs, where it means something.
    """

    return result.residual == 0


def _legacy_service_balanced(view: _WindowView, *, queued_logical: int,
                             inflight_logical: int = 0) -> bool:
    """The superseded one-sided predicate, retained only to prove it was wrong.

    Not called by any production path.  It exists so the regression that
    demonstrates the defect can assert both halves of the claim in one place:
    that the old comparison accepts a window in which rows were lost, and that
    the corrected identity rejects the very same window.

    It reached this shape by closing real defects one at a time -- the units
    were reconciled to logical rows, the current second was included, in-flight
    rows were given a term, and post-admission policy resolution was given a
    fifth exit.  Each of those was right, and each survives in the corrected
    identity.  What none of them addressed is that the inventory terms were
    still read instantaneously while everything else was read over a window, and
    that the comparison was ``>=`` rather than ``==``.  Those two are the defect;
    see :func:`_service_balanced`.
    """

    return (
        view.logical_committed + view.lost + view.policy_resolved
        + max(0, int(queued_logical)) + max(0, int(inflight_logical))
        >= view.admitted
    )


#: Fraction of hard queue capacity at which backlog is dangerous regardless of
#: trend.  The observed production peak was 150 of 20 000 (0.75%); half the hard
#: bound is real danger, not the pressure threshold that merely starts shedding.
_QUEUE_DANGER_FRACTION = 0.5
#: Longest a row may sit queued before the backlog is treated as real, however
#: shallow it looks.  Depth alone hides a small queue that never drains.
_QUEUE_MAX_RESIDENCE_S = 30.0


def _queue_not_accumulating(
    view: _WindowView, *, queue_depth: int, low_water: int, high_water: int,
    capacity: int, oldest_age_s: float,
) -> bool:
    """Is the queue free of genuine, unsafe accumulation?

    The previous test blocked on *either* crossing the high-water mark or a
    single positive regression slope.  Measured against a real 64.7-minute soak
    both proved to be noise detectors rather than backlog detectors: the queue
    peaked at 150 of 20 000 (0.75% of capacity), 65 of 87 "accumulating" samples
    were below the low-water mark -- one at depth 3 -- and every single one of
    those 87 samples had zero unexpected loss, zero reconciliation mismatch and
    a bounded queue.  Crossing high water is precisely what *starts* the
    shedding policy, so treating it as failure made a correctly-shedding lane
    permanently unable to certify.

    Accumulation is now judged on evidence of backlog that does not clear:

    * **Danger** -- the queue reached a material fraction of its hard capacity.
    * **Residence** -- the oldest queued row has been waiting too long, which
      catches a small queue that never drains (depth alone would hide it).
    * **Rising floor** -- the *shallowest* depth in the window's second half is
      higher than in its first half, and materially deep.  A queue that keeps
      draining has a flat floor no matter how spiky its peaks; a queue that is
      genuinely accumulating has a floor that climbs.  This is what separates
      bounded oscillation (0 -> 5 -> 2 -> 7 -> 1, or 5 -> 96 -> 5 with a
      successful drain) from sustained monotonic growth toward capacity.

    Peaks, instantaneous depth and a momentarily positive slope are deliberately
    *not* sufficient on their own -- they are what shedding is for.
    """

    _ = queue_depth
    limit = max(1, int(capacity))
    if view.max_depth >= limit * _QUEUE_DANGER_FRACTION:
        return False
    if float(oldest_age_s) > _QUEUE_MAX_RESIDENCE_S:
        return False
    floor_before = view.first_half_min_depth
    floor_after = view.second_half_min_depth
    if floor_before is None or floor_after is None:
        # Not enough observations to judge a trend; fall back to the endpoints,
        # which still catch a monotonic climb.
        return (
            view.first_depth is None
            or view.last_depth is None
            or view.last_depth <= view.first_depth
        )
    # A floor that is rising *and* already meaningful relative to the pressure
    # threshold is real backlog.  Below that the queue is draining fully between
    # bursts and the movement is noise.
    material_floor = max(1, int(low_water) // 4)
    return not (floor_after > floor_before and floor_after >= material_floor)


def required_rows_per_dispatch(
    *,
    admitted_rows_per_second: float,
    sustainable_dispatches_per_second: float,
    queue_depth: int,
    queue_target: int,
    drain_horizon_s: float = _DRAIN_HORIZON_S,
    safety_margin: float = _THROUGHPUT_MARGIN,
) -> int:
    """Return the service floor needed to stabilize and drain the queue."""

    values = (
        admitted_rows_per_second, sustainable_dispatches_per_second,
        drain_horizon_s, safety_margin,
    )
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float))
        or not math.isfinite(float(value)) or float(value) < 0
        for value in values
    ):
        raise ValueError("throughput inputs must be finite and non-negative")
    if float(drain_horizon_s) <= 0 or float(safety_margin) <= 0:
        raise ValueError("drain horizon and safety margin must be positive")
    demand = max(0.0, float(admitted_rows_per_second))
    demand += max(0, int(queue_depth) - max(0, int(queue_target))) / float(
        drain_horizon_s)
    if demand <= 0:
        return 1
    if float(sustainable_dispatches_per_second) <= 0:
        # Explicit overload sentinel.  The caller compares this floor with the
        # physical/deadline-safe capacity and never dispatches this many rows.
        return max(2, int(queue_depth) + 1)
    return max(
        1,
        math.ceil(
            demand * float(safety_margin)
            / float(sustainable_dispatches_per_second)
        ),
    )


def deadline_safe_capacity(
    *,
    transaction_budget_ms: Optional[float],
    tail_ms_per_row: Optional[float],
    physical_max_chunk: int,
    fixed_overhead_ms: float = 0.0,
) -> int:
    """Bound a chunk by a conservative affine transaction-cost estimate.

    ``fixed_overhead_ms`` prevents transaction setup/commit cost from being
    multiplied once per row.  The remaining 20 percent of the physical budget
    is reserved for scheduling and tail jitter.
    """

    if isinstance(physical_max_chunk, bool) or int(physical_max_chunk) < 1:
        raise ValueError("physical_max_chunk must be positive")
    maximum = int(physical_max_chunk)
    if transaction_budget_ms is None:
        return maximum
    if (isinstance(transaction_budget_ms, bool)
            or not isinstance(transaction_budget_ms, (int, float))
            or not math.isfinite(float(transaction_budget_ms))
            or float(transaction_budget_ms) <= 0):
        raise ValueError("transaction budget must be finite and positive")
    if tail_ms_per_row is None:
        return 1
    if (isinstance(tail_ms_per_row, bool)
            or not isinstance(tail_ms_per_row, (int, float))
            or not math.isfinite(float(tail_ms_per_row))
            or float(tail_ms_per_row) <= 0):
        raise ValueError("tail cost must be finite and positive")
    if (isinstance(fixed_overhead_ms, bool)
            or not isinstance(fixed_overhead_ms, (int, float))
            or not math.isfinite(float(fixed_overhead_ms))
            or float(fixed_overhead_ms) < 0):
        raise ValueError("fixed overhead must be finite and non-negative")
    usable = (
        float(transaction_budget_ms) * _DEADLINE_BUDGET_FRACTION
        - float(fixed_overhead_ms)
    )
    raw = math.floor(
        usable / max(float(tail_ms_per_row), 1e-3)
    )
    return max(1, min(maximum, raw))


@dataclass(frozen=True, slots=True)
class _TxnObservation:
    ts: float
    rows: int
    transaction_ms: float
    total_ms: float
    deadline_miss: bool = False
    fixed_overhead_ms: Optional[float] = None
    marginal_ms_per_row: Optional[float] = None


@dataclass(frozen=True, slots=True)
class _ControlDecision:
    physical_max_chunk: int
    deadline_safe_chunk: int
    throughput_required_chunk: int
    offered_required_chunk: int
    selected_chunk: int
    sustainable_dispatches_per_second: float
    estimated_sink_capacity_rows_per_second: float
    overload_active: bool
    controlled_overload: bool
    overload_reason: Optional[str]
    sampling_keep_ratio: float
    controller_state: str
    current_operational_healthy: bool
    recovery_healthy_windows: int
    view: _WindowView
    controller_view: _WindowView
    transaction_duration_avg_ms: float
    transaction_duration_p95_ms: float
    transaction_duration_p99_ms: float
    transaction_fixed_overhead_ms: float
    tail_ms_per_row: Optional[float]
    # Named conditions currently preventing a healthy recovery window.  Empty
    # means healthy.  Exposed so "why is this lane not recovering?" is answered
    # by the runtime rather than inferred by an operator.
    recovery_blockers: tuple[str, ...] = ()


class _AdaptiveTelemetryController:
    """Deadline-safe, throughput-aware controller with bounded recovery."""

    def __init__(
        self, *, physical_max_chunk: int, queue_capacity: int,
        flush_interval_s: float, budgeted_sink: bool,
        started_monotonic: Optional[float] = None,
    ) -> None:
        self.physical_max_chunk = max(1, int(physical_max_chunk))
        self.queue_capacity = max(1, int(queue_capacity))
        self.flush_interval_s = max(1e-3, float(flush_interval_s))
        # Upward adaptation is evaluated in real flush intervals.  This keeps
        # the controller responsive while a fast sink is continuously draining
        # a backlog, without letting a same-timestamp burst count as multiple
        # healthy windows.
        self._control_interval_s = self.flush_interval_s
        self._up_min_interval_s = (
            _UP_HEADROOM_WINDOWS * self._control_interval_s)
        # A one-second sampling cadence still represents sustained telemetry,
        # but a longer unobserved gap must break the consecutive-headroom
        # streak.  Expressing this in control ticks keeps fast sinks responsive.
        self._up_max_headroom_gap_ticks = max(
            1, math.ceil(1.0 / self._control_interval_s))
        started = time.monotonic() if started_monotonic is None else float(
            started_monotonic)
        self.window = _RollingTelemetryWindow(
            window_s=_RATE_WINDOW_S, started_monotonic=started)
        self._transactions: deque[_TxnObservation] = deque(
            maxlen=_MAX_COST_SAMPLES)
        # Bumped on every mutation of ``_transactions``; with the head timestamp
        # and length it identifies the exact sample set ``_transaction_stats``
        # last summarized.
        self._transaction_revision = 0
        self._transaction_stats_signature: Optional[
            tuple[int, Optional[float], int]] = None
        self._transaction_stats_cache: tuple[
            float, float, float, float, Optional[float], dict[int, float]
        ] = (0.0, 0.0, 0.0, 0.0, None, {})
        high_candidate = max(64, self.physical_max_chunk * 4)
        self.high_water = max(
            1, min(max(1, self.queue_capacity - 1), high_candidate))
        self.low_water = max(
            0, min(self.high_water - 1, max(16, self.physical_max_chunk * 2)))
        self.queue_target = self.low_water
        # A four-row bootstrap is small enough to remain conservative before a
        # transaction-cost sample exists, while avoiding a self-fulfilling
        # one-row ceiling caused by measuring only per-transaction overhead.
        bootstrap = min(4, self.physical_max_chunk)
        self.selected_chunk = bootstrap if budgeted_sink else self.physical_max_chunk
        self.deadline_safe_chunk = (
            bootstrap if budgeted_sink else self.physical_max_chunk)
        self.throughput_required_chunk = 1
        self.offered_required_chunk = 1
        self._active_deadline_cap: Optional[int] = None
        self._deadline_cap_until = 0.0
        self._headroom_streak = 0
        self._last_increase_ts = started - self._up_min_interval_s
        self._successes_at_selected = 0
        self._overload_active = False
        self._overload_reason: Optional[str] = None
        self._overload_enter_streak = 0
        self._overload_exit_streak = 0
        self._healthy_streak = 0
        self._recovery_blockers: tuple[str, ...] = ("settling",)
        self._settled_since = started
        self._last_control_tick = (
            math.floor(started / self._control_interval_s) - 1)
        self._last_decision: Optional[_ControlDecision] = None
        # Dense readiness observation.  Strictly an observer: it is written only
        # on a control tick that has already been decided, it reads no state it
        # does not receive, and it changes nothing.  ``None`` is the default and
        # costs one identity test per tick.
        self._trace: Optional[deque[dict[str, Any]]] = None
        self._trace_seq = 0
        self._trace_dropped = 0

    def enable_readiness_trace(self, *, capacity: int = 4096) -> None:
        """Begin recording one bounded observation per control tick.

        The ring is fixed-size, so a consumer that stops draining costs bounded
        memory and loses the *oldest* samples, counted in ``trace_dropped``.
        Recording never blocks and never touches the filesystem.
        """

        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("readiness trace capacity must be a positive int")
        if self._trace is None:
            self._trace = deque(maxlen=int(capacity))

    def drain_readiness_trace(self) -> list[dict[str, Any]]:
        """Remove and return every observation recorded since the last drain."""

        if self._trace is None:
            return []
        drained = list(self._trace)
        self._trace.clear()
        return drained

    @staticmethod
    def _percentile(values: Sequence[float], fraction: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(float(value) for value in values)
        index = min(
            len(ordered) - 1,
            max(0, math.ceil(float(fraction) * len(ordered)) - 1),
        )
        return ordered[index]

    def _transaction_stats(
        self, now: float,
    ) -> tuple[float, float, float, float, Optional[float], dict[int, float]]:
        cutoff = float(now) - _COST_WINDOW_S
        while self._transactions and self._transactions[0].ts < cutoff:
            self._transactions.popleft()
        # ``decide`` runs on every submission, but this is a pure function of
        # the retained cost samples: with no new transaction observed and none
        # newly evictable, recomputing it cannot change the answer.  The body
        # below is superlinear in the number of distinct chunk sizes -- the
        # slope estimate is a full pairwise scan over them, with a fresh slice
        # per step -- over a window holding up to _MAX_COST_SAMPLES rows, so on
        # the event loop that repetition is pure waste.  The cached value is
        # returned only when the exact same rows would be summarized again.
        head_ts = self._transactions[0].ts if self._transactions else None
        signature = (self._transaction_revision, head_ts, len(self._transactions))
        if self._transaction_stats_signature == signature:
            return self._transaction_stats_cache
        rows = tuple(self._transactions)
        if not rows:
            stats = (0.0, 0.0, 0.0, 0.0, None, {})
            self._transaction_stats_signature = signature
            self._transaction_stats_cache = stats
            return stats
        total = tuple(row.total_ms for row in rows)
        by_size: dict[int, list[float]] = {}
        for row in rows:
            by_size.setdefault(max(1, row.rows), []).append(row.transaction_ms)
        chunk_p95 = {
            size: (
                max(values) if len(values) < 5
                else self._percentile(values, 0.95)
            )
            for size, values in by_size.items()
        }
        ordered = sorted(chunk_p95.items())
        direct_fixed = [
            float(row.fixed_overhead_ms)
            for row in rows if row.fixed_overhead_ms is not None
        ]
        direct_marginal = [
            float(row.marginal_ms_per_row)
            for row in rows if row.marginal_ms_per_row is not None
        ]
        # The sink's own split of transaction time into fixed overhead and row
        # work is measured, not inferred, so it is resolved first and the
        # size-based estimates below are computed *net* of it.
        measured_fixed = (
            self._percentile(direct_fixed, 0.95) if direct_fixed else 0.0)
        slopes = [
            (right_ms - left_ms) / (right_rows - left_rows)
            for left_index, (left_rows, left_ms) in enumerate(ordered)
            for right_rows, right_ms in ordered[left_index + 1:]
            if right_ms > left_ms
        ]
        if slopes:
            marginal = max(1e-3, self._percentile(slopes, 0.95))
            intercepts = [
                max(0.0, duration - marginal * size)
                for size, duration in ordered
            ]
            fixed = self._percentile(intercepts, 0.95)
        else:
            # One observed size cannot separate fixed from marginal cost on its
            # own -- but the sink already measured the fixed part, so charging
            # the whole transaction to per-row cost is not the only option and
            # was actively harmful.  Under sustained priority starvation the
            # chunk collapses to a single size, this branch is the one that
            # runs, and attributing all of a contended transaction to marginal
            # cost ratcheted the deadline-safe chunk down and kept it there.
            # Net out the measured fixed overhead first; what remains is the
            # only part that actually scales with the row count.
            fixed = measured_fixed
            marginal = max(
                1e-3,
                max(
                    max(0.0, duration - measured_fixed) / max(1, size)
                    for size, duration in ordered
                ),
            )
        if direct_fixed:
            fixed = max(fixed, measured_fixed)
        if direct_marginal:
            marginal = max(
                1e-3, self._percentile(direct_marginal, 0.95))
        miss_floor = max(
            (
                row.transaction_ms / max(1, row.rows)
                for row in rows if row.deadline_miss
            ),
            default=0.0,
        )
        marginal = max(marginal, miss_floor)
        stats = (
            sum(total) / len(total),
            self._percentile(total, 0.95),
            self._percentile(total, 0.99),
            fixed,
            marginal,
            chunk_p95,
        )
        self._transaction_stats_signature = signature
        self._transaction_stats_cache = stats
        return stats

    def _reset_settle(self, now: float) -> None:
        self._settled_since = float(now)
        self._healthy_streak = 0

    def _policy_consistent(self, *, keep_ratio: Optional[float] = None) -> bool:
        """Is the overload policy's own state internally coherent?

        This replaced ``queue_depth <= low_water`` as the test for a *controlled*
        overload.  Depth was never the right question: crossing low water is how
        the shedding policy engages, so requiring the queue to fall back below it
        made sustained-but-safe policy sampling permanently uncertifiable.  What
        actually distinguishes controlled shedding from a lane out of control is
        whether the policy is explicit and its own state agrees with itself:

        * an active overload names its reason, and an inactive one names none;
        * the sampling keep-ratio is a real fraction.

        Loss, overflow, queue growth and conservation are checked separately and
        remain blocking; this is only the policy-coherence conjunct.
        """

        if self._overload_active and not self._overload_reason:
            return False
        if not self._overload_active and self._overload_reason:
            return False
        ratio = keep_ratio
        if ratio is None:
            decision = self._last_decision
            ratio = None if decision is None else decision.sampling_keep_ratio
        if ratio is None:
            return True
        if isinstance(ratio, bool) or not isinstance(ratio, (int, float)):
            return False
        return math.isfinite(float(ratio)) and 0.0 <= float(ratio) <= 1.0

    def add(self, now: float, *, queue_depth: int, **deltas: int) -> None:
        self.window.add(now, **deltas)
        self.window.observe_depth(now, queue_depth)
        # Only *terminal* evidence loss restarts the settle clock.  A failed or
        # deadline-missed batch whose rows were requeued and later committed
        # cost nothing: measured across a 64.7-minute soak, unexpected loss
        # stayed at exactly zero while these counters advanced, yet each one
        # restarted a 20 s settle and blocked a 15 s window, so recovery could
        # not accumulate.  Loss and admissions overflowing outside policy are
        # the terminal signals, and the bounded retry budget guarantees a
        # genuinely stuck batch eventually becomes one of them.
        if any(int(deltas.get(name) or 0) > 0 for name in (
            "lost", "admission_overflow",
        )):
            self._reset_settle(now)

    def observe_dispatch_busy(self, now: float, duration_ms: float) -> None:
        """Record real dispatch service time, whatever the dispatch returned.

        ``duration_ms`` is forwarded unconverted so the window's strict type
        check sees what the caller actually passed; coercing it here would
        launder a bool into a float and defeat that check.
        """

        self.window.observe_dispatch_busy(float(now), duration_ms)

    def observe_commit(
        self, *, now: float, rows: int, logical_rows: int,
        transaction_ms: float, total_ms: float, queue_depth: int,
        fixed_overhead_ms: Optional[float] = None,
        marginal_ms_per_row: Optional[float] = None,
    ) -> None:
        transaction = max(0.0, float(transaction_ms))
        total = max(transaction, float(total_ms), 1e-3)
        # ``total_ms`` is the wall time this dispatch occupied the lane, so the
        # commit observation is itself the service-time sample.  Recorded before
        # the empty-batch guard: a dispatch that wrote nothing still consumed
        # the lane, and capacity that ignores it is overstated.
        self.window.observe_dispatch_busy(float(now), total)
        count = max(0, int(rows))
        if count <= 0:
            return
        fixed = (
            None if fixed_overhead_ms is None
            else max(0.0, float(fixed_overhead_ms))
        )
        marginal = (
            None if marginal_ms_per_row is None
            else max(1e-3, float(marginal_ms_per_row))
        )
        self._transactions.append(_TxnObservation(
            ts=float(now), rows=count,
            transaction_ms=max(transaction, 1e-3), total_ms=total,
            fixed_overhead_ms=fixed,
            marginal_ms_per_row=marginal,
        ))
        self._transaction_revision += 1
        if count >= self.selected_chunk:
            self._successes_at_selected += 1
        self.add(
            now, queue_depth=queue_depth, committed=count,
            logical_committed=max(0, int(logical_rows)),
            dispatch_successes=1,
        )

    def observe_deadline_miss(
        self, *, now: float, failed_rows: int,
        transaction_budget_ms: Optional[float], queue_depth: int,
    ) -> None:
        rows = max(1, int(failed_rows))
        current = max(1, min(self.selected_chunk, rows))
        cap = max(1, current // 2)
        self._active_deadline_cap = cap
        self._deadline_cap_until = float(now) + _COST_WINDOW_S
        self.selected_chunk = min(self.selected_chunk, cap)
        self.deadline_safe_chunk = min(self.deadline_safe_chunk, cap)
        self._headroom_streak = 0
        self._successes_at_selected = 0
        # Shrinking the chunk is the adaptation a deadline miss calls for.
        # Restarting the settle clock is not: the rows are requeued under the
        # bounded retry budget and normally commit, so punishing recovery for a
        # miss that cost nothing is what kept ``settling`` a standing blocker.
        # If the retry budget is exhausted the rows are dropped, ``lost``
        # advances, and the settle clock restarts through that path.
        if transaction_budget_ms is not None:
            budget = max(1e-3, float(transaction_budget_ms))
            self._transactions.append(_TxnObservation(
                ts=float(now), rows=rows,
                transaction_ms=budget, total_ms=budget,
                deadline_miss=True,
            ))
            self._transaction_revision += 1
        self.add(
            now, queue_depth=queue_depth,
            deadline_failures=1, failed_batches=1,
        )

    def _controller_safe(self) -> bool:
        """Is the selected physical chunk both safe and the best available?

        Two requirements, and the second is conditional on being achievable:

        * The chunk must never exceed the deadline-safe size.  This is absolute:
          a larger batch would blow the cooperative transaction budget.
        * It should meet the throughput floor needed to drain the queue -- *when
          that floor is reachable within the deadline budget*.

        When demand exceeds physical sink capacity the floor is by definition
        unreachable: the controller clamps to ``deadline_safe_chunk`` and the
        shortfall is what the shedding policy exists to absorb.  Requiring
        ``selected >= throughput_required`` unconditionally therefore made the
        controller permanently "unsafe" in exactly the steady state it was
        designed to handle, which pinned the recovery streak at zero and
        ``operational_ready`` at false for as long as load stayed high.

        Being pinned at the deadline-safe maximum while shedding is the correct
        response to that situation, so it counts as safe.  The unmet demand
        stays visible through ``throughput_required_chunk`` and the capacity
        state; it is reported, not hidden.
        """

        if self.selected_chunk > self.deadline_safe_chunk:
            return False
        reachable_floor = min(
            self.throughput_required_chunk, self.deadline_safe_chunk)
        return self.selected_chunk >= reachable_floor

    def _sustainable_dispatch_rate(
        self, view: _WindowView, total_p95_ms: float,
    ) -> float:
        """Dispatches per second the sink can actually sustain.

        The observed term is deliberately *service* rate -- successes divided by
        the time the lane spent inside a dispatch -- and not successes divided by
        the time the lane merely held rows.

        Those two are not the same number, and using the second one closed a
        feedback loop that shed a fifth of all telemetry.  The aggregator waits
        up to one flush interval for a batch to fill before dispatching; while
        it waits, the queue is non-empty, so every one of those seconds counted
        as "backlogged".  Dividing by them measured the *batching cadence*
        (~1/flush_interval), never the sink.  The controller then adopted that
        cadence as capacity, concluded it needed more rows per dispatch than the
        physical chunk allows, declared ``throughput_exceeds_deadline_safe_
        capacity``, and shed the excess -- which kept admitted load at the
        cadence ceiling, which kept the observed rate low.

        Measured over a 22-minute real-ingest run at 10f23e9: the clamp fired on
        99.9% of control ticks and held the estimate at 3.47 dispatches/s while
        the same run reached 12/s at p90 and 140/s at peak, and while the
        sink's own p95 transaction time permitted 8/s.  28.0% of offered rows
        were shed by a lane whose physical capacity was never the constraint.

        Service time keeps every safety property the old term was reaching for.
        A sink that is genuinely slow raises ``total_p95_ms``, so
        ``latency_capacity`` clamps.  A lane that is burning dispatches on
        skips, deadline misses or failures accrues busy time without successes,
        so the observed term clamps.  Only the deliberate idle wait -- which
        bounds nothing -- stops being counted as incapacity.
        """

        latency_capacity = (
            1_000.0 / max(1.0, total_p95_ms)
            if total_p95_ms > 0 else 1.0 / self.flush_interval_s
        )
        if view.dispatch_busy_s > 0.0 and view.dispatch_successes > 0:
            observed_service = view.dispatch_successes / view.dispatch_busy_s
            return max(1e-6, min(latency_capacity, observed_service))
        return max(1e-6, latency_capacity)

    def decide(
        self, *, now: float, queue_depth: int,
        transaction_budget_ms: Optional[float],
        advance_state: bool = True,
        queued_logical: Optional[int] = None,
        oldest_age_s: float = 0.0,
        inflight_logical: int = 0,
        trace_context: Optional[Callable[[], Mapping[str, Any]]] = None,
    ) -> _ControlDecision:
        # Logical rows still held by the lane.  Defaults to the pending count,
        # which is a lower bound (one pending can carry several aggregated
        # logical rows), so conservation stays conservative when a caller does
        # not supply it.
        queued_logical = (
            int(queue_depth) if queued_logical is None else int(queued_logical))
        self.window.observe_depth(now, queue_depth)
        # Take the conservation reading before anything else in the tick.  The
        # caller holds the writer's lock across this whole call, so the counters
        # and the inventory it passed describe one instant; recording the
        # window's opening boundary here keeps every later evaluation in this
        # second measured against that same instant.
        self.window.record_conservation(
            now, queued_logical=queued_logical,
            inflight_logical=inflight_logical)
        conservation_result = self.window.conservation(
            now, queued_logical=queued_logical,
            inflight_logical=inflight_logical)
        view = self.window.view(now, include_current=True)
        recovery_view = self.window.view(now, include_current=False)
        (
            avg_ms, p95_ms, p99_ms, fixed_ms, tail_ms, chunk_p95,
        ) = self._transaction_stats(now)
        safe = (
            self.deadline_safe_chunk
            if transaction_budget_ms is not None and tail_ms is None
            else deadline_safe_capacity(
                transaction_budget_ms=transaction_budget_ms,
                tail_ms_per_row=tail_ms,
                physical_max_chunk=self.physical_max_chunk,
                fixed_overhead_ms=fixed_ms,
            )
        )
        if (self._active_deadline_cap is not None
                and float(now) < self._deadline_cap_until):
            safe = min(safe, self._active_deadline_cap)
        elif self._active_deadline_cap is not None:
            self._active_deadline_cap = None
        sustainable = self._sustainable_dispatch_rate(view, p95_ms)
        required = required_rows_per_dispatch(
            admitted_rows_per_second=view.admitted_rps,
            sustainable_dispatches_per_second=sustainable,
            queue_depth=queue_depth,
            queue_target=self.queue_target,
        )
        offered_required = required_rows_per_dispatch(
            admitted_rows_per_second=view.offered_rps,
            sustainable_dispatches_per_second=sustainable,
            queue_depth=queue_depth,
            queue_target=self.queue_target,
        )
        prior_safe_chunk = self.deadline_safe_chunk
        self.deadline_safe_chunk = max(
            1, min(self.physical_max_chunk, safe))
        self.throughput_required_chunk = max(1, int(required))
        self.offered_required_chunk = max(1, int(offered_required))

        capacity_pressure = (
            self.offered_required_chunk > self.deadline_safe_chunk)
        high_pressure = int(queue_depth) >= self.high_water
        current_tick = math.floor(
            float(now) / self._control_interval_s)
        tick_advanced = False
        streak_before = self._healthy_streak
        tick_predicates: dict[str, Any] = {}
        if advance_state and current_tick != self._last_control_tick:
            tick_advanced = True
            control_gap = current_tick - self._last_control_tick
            self._last_control_tick = current_tick
            if (
                control_gap <= 0
                or control_gap > self._up_max_headroom_gap_ticks
            ):
                self._headroom_streak = 0
            prior_selected = self.selected_chunk
            if self.deadline_safe_chunk < self.selected_chunk:
                self.selected_chunk = self.deadline_safe_chunk
                self._headroom_streak = 0
                self._successes_at_selected = 0
            else:
                current_tail = chunk_p95.get(self.selected_chunk, p95_ms)
                cost_headroom = (
                    transaction_budget_ms is None
                    or (
                        current_tail > 0
                        and current_tail
                        <= float(transaction_budget_ms)
                        * _DEADLINE_BUDGET_FRACTION
                    )
                )
                clean_probe_window = (
                    self._successes_at_selected > 0
                    and view.lost == 0
                    and view.admission_overflow == 0
                    and view.failed_batches == 0
                    and view.deadline_failures == 0
                    and view.priority_deferrals == 0
                    and view.checkpoint_deferrals == 0
                )
                if (self.selected_chunk < self.deadline_safe_chunk
                        and cost_headroom and clean_probe_window):
                    self._headroom_streak += 1
                else:
                    self._headroom_streak = 0
                # A successful batch can prove headroom for only one control
                # interval.  Requiring fresh work in every interval prevents a
                # completed burst from ratcheting the size upward while idle.
                self._successes_at_selected = 0
                if (
                    self._headroom_streak >= _UP_HEADROOM_WINDOWS
                    and float(now) - self._last_increase_ts
                    >= self._up_min_interval_s
                ):
                    step = max(
                        1, math.ceil(
                            self.selected_chunk * _UP_STEP_FRACTION))
                    self.selected_chunk = min(
                        self.deadline_safe_chunk,
                        self.selected_chunk + step,
                    )
                    self._last_increase_ts = float(now)
                    self._headroom_streak = 0
                    self._successes_at_selected = 0
            # Honour the throughput floor.  Unlike a growth probe this is not
            # speculative: it is the minimum rows per dispatch needed for
            # committed throughput to keep pace with admitted load plus a
            # bounded backlog drain, and it is clamped by the deadline-safe
            # capacity, so meeting it can never risk a deadline miss.
            # Computing the floor without ever applying it left the controller
            # selecting below its own stated requirement -- the queue then grew
            # under sustained load and current health could never certify,
            # because controller_safe requires selected >= required.
            required_floor = min(
                self.throughput_required_chunk, self.deadline_safe_chunk)
            if self.selected_chunk < required_floor:
                self.selected_chunk = required_floor
                self._headroom_streak = 0
                self._successes_at_selected = 0
            # The settle timer is reset only by a genuinely unsafe transition,
            # not by routine controller adaptation.  Chunk-size changes that
            # follow from a fresh shrink of the deadline-safe capacity mean
            # the sink measured slower (a real capacity change), so that path
            # re-arms recovery.  A chunk change driven by the throughput floor
            # or a healthy upward growth probe is exactly the policy-sampling
            # adaptation recovery must tolerate: resetting settle on every
            # such change made the 20s interval unreachable under fluctuating
            # but safe load.
            #
            # A deadline miss shrinks both chunks, so gating only on the shrink
            # re-punished the very event whose direct reset was removed -- the
            # same setback coming through a different door.  Shrinking is the
            # adaptation, not the setback: it is a genuine capacity regression
            # only when rows were actually lost, which is the same terminal test
            # used everywhere else in this model.
            if (self.selected_chunk < prior_selected
                    and self.deadline_safe_chunk < prior_safe_chunk
                    and (recovery_view.lost
                         or recovery_view.admission_overflow)):
                self._reset_settle(now)

            if capacity_pressure:
                self._overload_enter_streak += 1
            else:
                self._overload_enter_streak = 0
            prior_overload = self._overload_active
            if high_pressure or (
                    self._overload_enter_streak >= _OVERLOAD_ENTER_WINDOWS):
                self._overload_active = True
                self._overload_reason = (
                    "queue_high_water" if high_pressure
                    else "throughput_exceeds_deadline_safe_capacity"
                )
                self._overload_exit_streak = 0
            elif self._overload_active:
                recovered = (
                    int(queue_depth) <= self.low_water
                    and recovery_view.queue_slope_rps <= 0.0
                    and not capacity_pressure
                    and recovery_view.lost == 0
                    and recovery_view.failed_batches == 0
                    and recovery_view.deadline_failures == 0
                )
                self._overload_exit_streak = (
                    self._overload_exit_streak + 1 if recovered else 0)
                if self._overload_exit_streak >= _OVERLOAD_EXIT_WINDOWS:
                    self._overload_active = False
                    self._overload_reason = None
                    self._overload_exit_streak = 0
            # A toggle of ``overload_active`` is not inherently unsafe.
            # HEALTHY_WITH_POLICY_SAMPLING -- overload active while the policy
            # is sampling, queues are bounded, and loss/failures are zero -- is
            # a valid recoverable state.  Resetting settle on every toggle made
            # the recovery window unreachable whenever load fluctuated around
            # the capacity boundary even while every safety invariant held.
            # Only a transition *into* overload via the hard queue-capacity
            # path (queue_high_water) is a genuine capacity setback; the
            # throughput-exceeds-deadline-safe-capacity path is the steady
            # state the shedding policy exists to absorb, and exiting overload
            # is recovery progress, not a setback.
            # ...and even that path is only a setback when it actually cost
            # something.  Crossing the high-water mark is how the shedding
            # policy engages; if nothing was lost, no admission overflowed and
            # the queue is not accumulating, the lane absorbed the burst exactly
            # as designed and the healthy streak must survive it.  Measured on
            # the soak, resetting here unconditionally left ``settling`` as a
            # standing blocker under normal fluctuating load.
            if (self._overload_active and not prior_overload
                    and self._overload_reason == "queue_high_water"
                    and (recovery_view.lost
                         or recovery_view.admission_overflow)):
                self._reset_settle(now)

            service_balanced = _service_balanced(conservation_result)
            depth_nonincreasing = _queue_not_accumulating(
                recovery_view, queue_depth=queue_depth,
                low_water=self.low_water, high_water=self.high_water,
                capacity=self.queue_capacity, oldest_age_s=oldest_age_s)
            controller_safe = self._controller_safe()
            within_hard_bound = int(queue_depth) < self.queue_capacity
            policy_consistent = self._policy_consistent(keep_ratio=None)
            controlled_overload = (
                self._overload_active
                and policy_consistent
                and controller_safe
                and service_balanced
                and depth_nonincreasing
                and within_hard_bound
            )
            # Named conjuncts so an operator (and the soak evidence) can see
            # exactly which condition is holding recovery back, instead of
            # inferring it from a single opaque boolean.
            blockers = [
                name for name, blocked in (
                    ("settling",
                     float(now) - self._settled_since < _RECOVERY_SETTLE_S),
                    ("unexpected_loss", recovery_view.lost != 0),
                    ("admission_overflow",
                     recovery_view.admission_overflow != 0),
                    # Failed and deadline-missed batches are reported, and they
                    # drive the controller's chunk adaptation, but they do not
                    # block recovery on their own: their rows are requeued under
                    # a bounded retry budget and normally commit.  A batch that
                    # genuinely cannot be delivered exhausts that budget, its
                    # rows are dropped, and ``unexpected_loss`` above blocks.
                    ("unresolved_batch_backlog",
                     recovery_view.failed_batches != 0
                     and recovery_view.lost != 0),
                    ("queue_accumulating", not depth_nonincreasing),
                    ("service_imbalance", not service_balanced),
                    ("controller_chunk_unsafe", not controller_safe),
                    # The hard bound, not the pressure threshold.  Crossing
                    # low water is what *starts* the shedding policy; measured
                    # on the 34.6-minute gate the lane sat above a low water of
                    # ~1,024 in a 20,000-row queue for all 70 samples with zero
                    # unexpected loss and zero reconciliation mismatch, so this
                    # blocker alone held ``operational_ready`` false while every
                    # safety invariant was intact.  Recovery is gated on the
                    # queue being *bounded*, which is the invariant that matters.
                    ("queue_hard_cap_breached", not within_hard_bound),
                    ("policy_inconsistent", not policy_consistent),
                    # Overload is uncontrolled when the policy is not resolving
                    # it, not merely when it is engaged.  Sustained
                    # POLICY_SAMPLING_ACTIVE with a bounded, non-accumulating
                    # queue and closed accounting is the shedding policy working
                    # exactly as designed.
                    ("uncontrolled_overload",
                     self._overload_active and not controlled_overload),
                ) if blocked
            ]
            self._recovery_blockers = tuple(blockers)
            healthy = not blockers
            self._healthy_streak = self._healthy_streak + 1 if healthy else 0
            if self._trace is not None:
                # Every conjunct exactly as it was evaluated for *this* tick,
                # plus the raw terms each one was computed from, so an operator
                # never has to infer a predicate from its name.
                tick_predicates = {
                    "blockers": list(blockers),
                    "settle_age_s": round(
                        float(now) - self._settled_since, 4),
                    "service_balanced": bool(service_balanced),
                    "depth_nonincreasing": bool(depth_nonincreasing),
                    "controller_safe": bool(controller_safe),
                    "within_hard_bound": bool(within_hard_bound),
                    "policy_consistent": bool(policy_consistent),
                    "controlled_overload": bool(controlled_overload),
                    "overload_active": bool(self._overload_active),
                    "overload_reason": self._overload_reason,
                    "overload_enter_streak": int(self._overload_enter_streak),
                    "overload_exit_streak": int(self._overload_exit_streak),
                    "capacity_pressure": bool(capacity_pressure),
                    "high_pressure": bool(high_pressure),
                    # Every term of the conservation identity, measured across
                    # the identical boundaries the predicate compares them over,
                    # so an operator can recompute the residual by hand from
                    # this record alone and get the same number.
                    "conservation": {
                        "window_start_tick": int(
                            conservation_result.window_start_tick),
                        "window_end_tick": int(
                            conservation_result.window_end_tick),
                        "boundary_observed": bool(
                            conservation_result.boundary_observed),
                        "admitted_window": int(
                            conservation_result.admitted_window),
                        "aggregated_window": int(
                            conservation_result.aggregated_window),
                        "logical_committed_window": int(
                            conservation_result.committed_window),
                        "lost_admitted_window": int(
                            conservation_result.lost_admitted_window),
                        "policy_resolved_window": int(
                            conservation_result.policy_resolved_window),
                        "starting_inventory": int(
                            conservation_result.starting_inventory),
                        "ending_inventory": int(
                            conservation_result.ending_inventory),
                        # Exactly zero is the contract.  Both signs fail.
                        "residual": int(conservation_result.residual),
                        # The same identity taken from the lane's origin: is
                        # there any unaccounted row at all, however old?
                        "absolute_residual": int(
                            conservation_result.absolute_residual),
                        # Context, not identity terms.  ``lost_window`` is the
                        # whole of unexpected loss including pre-admission
                        # refusals, which is the health signal rather than a
                        # term of the admitted-cohort identity.
                        "lost_window": int(view.lost),
                        "queued_logical_now": int(queued_logical),
                        "inflight_logical_now": int(inflight_logical),
                        "overload_handled_window": int(
                            view.overload_handled),
                        "coalesced_window": int(view.coalesced),
                        "sampled_window": int(view.sampled),
                        "deferred_window": int(view.deferred),
                    },
                    "queue_trend": {
                        "max_depth": int(view.max_depth),
                        "first_half_min_depth": view.first_half_min_depth,
                        "second_half_min_depth": view.second_half_min_depth,
                        "slope_rps": round(
                            float(recovery_view.queue_slope_rps), 4),
                        "oldest_age_s": round(float(oldest_age_s), 4),
                    },
                    "recovery_view": {
                        "lost": int(recovery_view.lost),
                        "admission_overflow": int(
                            recovery_view.admission_overflow),
                        "failed_batches": int(recovery_view.failed_batches),
                        "deadline_failures": int(
                            recovery_view.deadline_failures),
                        "priority_deferrals": int(
                            recovery_view.priority_deferrals),
                        "checkpoint_deferrals": int(
                            recovery_view.checkpoint_deferrals),
                    },
                }

        service_balanced = _service_balanced(conservation_result)
        depth_nonincreasing = _queue_not_accumulating(
            recovery_view, queue_depth=queue_depth,
            low_water=self.low_water, high_water=self.high_water,
            capacity=self.queue_capacity, oldest_age_s=oldest_age_s)
        controller_safe = self._controller_safe()
        controlled_overload = (
            self._overload_active
            and self._policy_consistent(keep_ratio=None)
            and controller_safe
            and service_balanced
            and depth_nonincreasing
            and int(queue_depth) < self.queue_capacity
        )
        # Capacity is estimated only at the currently selected physical size;
        # it is never extrapolated from a smaller observed size to ``safe``.
        estimated_capacity = self.selected_chunk * sustainable
        backlog_drain_rps = (
            max(0, int(queue_depth) - self.queue_target)
            / _DRAIN_HORIZON_S
        )
        admissible_offered_rps = max(
            0.0,
            _SAMPLING_CAPACITY_RESERVE * estimated_capacity
            - backlog_drain_rps,
        )
        keep_ratio = min(
            1.0,
            max(
                _MIN_SAMPLING_KEEP_RATIO,
                admissible_offered_rps / max(view.offered_rps, 1e-9),
            ),
        )
        if high_pressure:
            keep_ratio = min(keep_ratio, 0.25)
        if int(queue_depth) >= self.high_water + self.physical_max_chunk:
            keep_ratio = 0.0
        state = (
            "OVERLOAD_NONCRITICAL_SAMPLING" if self._overload_active
            else "RECOVERING" if (
                self.selected_chunk < self.deadline_safe_chunk
                or self._healthy_streak < _RECOVERY_HEALTHY_WINDOWS
            )
            else "STABLE"
        )
        if tick_advanced and self._trace is not None:
            self._trace_seq += 1
            if len(self._trace) == self._trace.maxlen:
                self._trace_dropped += 1
            sample: dict[str, Any] = {
                "seq": self._trace_seq,
                "mono": round(float(now), 4),
                "wall_ms": int(time.time() * 1_000),
                "control_tick": int(current_tick),
                "controller_state": state,
                "healthy_streak_before": int(streak_before),
                "healthy_streak_after": int(self._healthy_streak),
                "streak_reset": bool(
                    streak_before > 0 and self._healthy_streak == 0),
                "required_healthy_windows": _RECOVERY_HEALTHY_WINDOWS,
                "queue_depth": int(queue_depth),
                "queue_capacity": int(self.queue_capacity),
                "queue_low_water": int(self.low_water),
                "queue_high_water": int(self.high_water),
                "selected_chunk": int(self.selected_chunk),
                "deadline_safe_chunk": int(self.deadline_safe_chunk),
                "throughput_required_chunk": int(
                    self.throughput_required_chunk),
                "offered_required_chunk": int(self.offered_required_chunk),
                "sampling_keep_ratio": round(float(keep_ratio), 6),
                "sustainable_dispatches_per_second": round(
                    float(sustainable), 4),
                "estimated_sink_capacity_rps": round(
                    float(estimated_capacity), 4),
                "rates": {
                    "incoming_rps": round(float(view.incoming_rps), 4),
                    "offered_rps": round(float(view.offered_rps), 4),
                    "admitted_rps": round(float(view.admitted_rps), 4),
                    "logical_committed_rps": round(
                        float(view.logical_committed_rps), 4),
                    "committed_rps": round(float(view.committed_rps), 4),
                    "dispatch_attempt_rps": round(
                        float(view.dispatch_attempt_rps), 4),
                    "dispatch_success_rps": round(
                        float(view.dispatch_success_rps), 4),
                    # Both denominators, so an operator can see directly why
                    # the capacity estimate is what it is.
                    "dispatch_busy_s": round(float(view.dispatch_busy_s), 4),
                    "backlogged_s": round(float(view.backlogged_seconds), 4),
                },
                "transaction_ms": {
                    "avg": round(float(avg_ms), 4),
                    "p95": round(float(p95_ms), 4),
                    "p99": round(float(p99_ms), 4),
                    "fixed_overhead": round(float(fixed_ms), 4),
                    "tail_per_row": (
                        None if tail_ms is None else round(float(tail_ms), 6)),
                },
                **tick_predicates,
            }
            if trace_context is not None:
                try:
                    sample["writer"] = dict(trace_context())
                except Exception:  # noqa: BLE001 - observation is never fatal
                    sample["writer"] = {"error": "trace_context_failed"}
            self._trace.append(sample)
        decision = _ControlDecision(
            physical_max_chunk=self.physical_max_chunk,
            deadline_safe_chunk=self.deadline_safe_chunk,
            throughput_required_chunk=self.throughput_required_chunk,
            offered_required_chunk=self.offered_required_chunk,
            selected_chunk=max(1, min(
                self.physical_max_chunk, self.selected_chunk)),
            sustainable_dispatches_per_second=sustainable,
            estimated_sink_capacity_rows_per_second=estimated_capacity,
            overload_active=self._overload_active,
            controlled_overload=controlled_overload,
            overload_reason=self._overload_reason,
            sampling_keep_ratio=keep_ratio,
            controller_state=state,
            current_operational_healthy=(
                self._healthy_streak >= _RECOVERY_HEALTHY_WINDOWS),
            recovery_healthy_windows=self._healthy_streak,
            recovery_blockers=self._recovery_blockers,
            view=recovery_view,
            controller_view=view,
            transaction_duration_avg_ms=avg_ms,
            transaction_duration_p95_ms=p95_ms,
            transaction_duration_p99_ms=p99_ms,
            transaction_fixed_overhead_ms=fixed_ms,
            tail_ms_per_row=tail_ms,
        )
        self._last_decision = decision
        return decision


# How often one row may be requeued after a cooperative priority skip before it
# is accounted as a genuine drop.  The 250 ms minimum retry cadence below makes
# ordinary critical/write-contention retries an eight-second bound.  Explicit
# maintenance/checkpoint deferrals do not consume this budget: that worker has
# its own bounded lifetime and shutdown drains it before telemetry.
_MAX_PRIORITY_REQUEUE_ATTEMPTS = 32


def _classify_batch_failure(
    error: Optional[str], *, deadline_exceeded: bool,
    verified_duplicate: bool = False,
) -> TelemetryLossCategory:
    """Attribute one failed physical batch to exactly one terminal category.

    A cooperative deadline miss is its own cause.  Deduplication is claimed only
    when the sink has *proven* it: the previous implementation read the
    exception text, and accounted any UNIQUE violation naming
    ``book_snapshots.state_hash`` as an already-stored byte-identical row.

    That inference does not hold.  The constraint is over
    ``(market_identity_id, token_id, state_hash, receipt_ts_ms)``, and the hash
    covers only token, condition, provider timestamp, connection epoch and the
    two ladders.  Provenance, sequence number, monotonic receipt, the derived
    top-of-book, hydration and staleness are all outside both -- so two rows can
    collide while disagreeing about what was observed, and the differing row was
    discarded as "already stored".  A re-offer whose ``stale`` flag has flipped
    is exactly that case, and it is not hypothetical: it is the shape the state
    coalescer deliberately lets through.

    So the sink now returns a typed verdict after reading the stored row and
    comparing every evidence-bearing column, and this function reads that
    verdict.  Nothing here parses a message.  An unverified collision is a sink
    failure and stays blocking, because we never assume evidence survived.
    """

    if deadline_exceeded:
        return TelemetryLossCategory.DEADLINE_EXPIRED
    if verified_duplicate:
        return TelemetryLossCategory.POLICY_DEDUPLICATED
    _ = error
    return TelemetryLossCategory.SINK_FAILURE


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
        self._overload_latest_tokens: dict[str, int] = {}
        self._pending_dedupe: dict[str, int] = {}
        self._dedupe_seen: OrderedDict[str, None] = OrderedDict()
        self._state_seen: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._next_token = 1
        self._thread: Optional[threading.Thread] = None
        self._accepting = True
        self._stop_requested = False
        self._drain_on_stop = True
        self._drain_stop_failed = False
        self._drain_stop_start_dropped = 0
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
        self._offered = 0
        self._admitted = 0
        self._coalesced = 0
        self._preoverflow_coalesced = 0
        self._sampled = 0
        self._deferred = 0
        self._deduplicated = 0
        self._dropped = 0
        self._admission_overflow_rows = 0
        self._drop_reasons: dict[str, int] = {}
        self._policy_reasons: dict[str, int] = {}
        # Terminal-outcome accounting.  Every logical row that leaves the lane
        # lands in exactly one of these buckets, so ``submitted`` can be
        # reconciled exactly against its terminal states.  ``_dropped`` above
        # remains the historical union of the unexpected causes and is kept for
        # continuity of the lifetime series; the per-cause counters below are
        # what the health model reads.
        self._loss_by_category: dict[str, int] = {
            category.value: 0 for category in TelemetryLossCategory}
        self._reconciliation_mismatch_rows = 0
        self._last_reconciliation: dict[str, int] = {}
        # Logical rows still owned by the lane (queued pendings, including rows
        # aggregated into one pending).  Tracked incrementally because summing
        # the queue on every submit would be O(queue) on the hot path.
        self._queued_logical = 0
        # Rows merged into an already-queued pending.  They appear in
        # ``_coalesced`` *and* travel to the sink inside that pending, so
        # reconciliation subtracts this overlap exactly once.
        self._aggregated_in_queue = 0
        # Logical rows removed from the queue and handed to the sink but not yet
        # acknowledged.  Without this the conservation identity briefly fails
        # for every batch in flight, which is indistinguishable from real loss.
        self._inflight_logical = 0
        self._sample_sequence = 0
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
        # Learned physical-chunk ceiling.  See the module docstring: a chunk
        # that exceeds the sink's cooperative transaction budget at the current
        # per-row cost can only ever miss, roll back, and lose its rows.  Each
        # deadline miss halves this bound; it never grows again inside one
        # process, so the number of deadline failures per launch is finite.
        self._physical_batch_ceiling = self.physical_batch_size
        self._deadline_shrink_events = 0
        # Transient bisection ceiling used to isolate a content-addressed
        # duplicate to the single row it actually proves something about.  It is
        # cleared by the first clean commit, so it can never become a permanent
        # throughput cap; see the isolation branch in ``_dispatch``.
        self._duplicate_isolation_ceiling: Optional[int] = None
        self._duplicate_isolation_events = 0
        # UNIQUE collisions whose stored row disagreed about the evidence.
        # These are blocking sink failures, never deduplication; the last one is
        # retained field-by-field so the failure is diagnosable from the lane.
        self._evidence_conflicts = 0
        self._last_evidence_conflict: Optional[dict[str, Any]] = None
        self._consecutive_successful_batches = 0
        self._last_priority_skip_ts_ms = 0
        self._checkpoint_deferral_batches = 0
        self._checkpoint_deferral_rows = 0
        self._last_checkpoint_deferral_ts_ms = 0
        self._requeued_rows = 0
        # Measured cost control.  Once the sink's transaction budget is known,
        # the chunk size is computed from the observed per-row cost with a
        # safety margin rather than discovered by losing a batch to the
        # deadline.  This lets the size recover when the database gets faster
        # again (for example right after a WAL truncation) instead of being
        # permanently pinned at its worst observed value.
        self._budget_ms: Optional[float] = None
        self._ms_per_row: Optional[float] = None
        budget_getter = getattr(
            self._persistence_writer, "telemetry_transaction_budget_ms", None)
        self._controller = _AdaptiveTelemetryController(
            physical_max_chunk=self.physical_batch_size,
            queue_capacity=self.capacity,
            flush_interval_s=self.flush_interval_s,
            budgeted_sink=callable(budget_getter),
            started_monotonic=time.monotonic(),
        )
        self._physical_batch_ceiling = self._controller.selected_chunk
        self._last_control_decision: Optional[_ControlDecision] = None
        self._last_controller_sample_ts_ms = now_wall
        self._last_controller_sample_monotonic = time.monotonic()
        # Bound once so passing it per submission is a plain attribute read.
        # ``None`` until an operator enables the observer, and the controller
        # calls it only on a control tick it is already recording.
        self._trace_context_fn: Optional[
            Callable[[], Mapping[str, Any]]] = None

    def enable_readiness_trace(self, *, capacity: int = 4096) -> None:
        """Record one dense observation per controller tick, for an operator.

        Observation only: it is written after a tick has been decided, from
        state the lane already holds under its own lock, and it feeds nothing
        back into any control path.  Draining is the consumer's job; the ring
        is bounded so a stalled consumer costs bounded memory.
        """

        with self._condition:
            self._controller.enable_readiness_trace(capacity=capacity)
            self._trace_context_fn = self._trace_context_locked

    def drain_readiness_trace(self) -> list[dict[str, Any]]:
        """Remove and return the dense observations recorded since last drain."""

        with self._condition:
            return self._controller.drain_readiness_trace()

    def _trace_context_locked(self) -> dict[str, Any]:
        """Writer-owned counters for one control tick, read under the lock.

        Every field is a plain integer already maintained on the lane, so the
        snapshot is internally consistent by construction: nothing here is
        sampled at a different instant from the controller terms it will be
        compared against.
        """

        pending_retries = 0
        for pending in self._pending.values():
            if pending.requeue_attempts:
                pending_retries += 1
        return {
            "queued_pendings": len(self._queue),
            "queued_logical": int(self._queued_logical),
            "inflight_logical": int(self._inflight_logical),
            "inflight_batches": int(self._inflight_batches),
            "pending_retry_rows": pending_retries,
            "pending_aggregate_slots": len(self._aggregate_tokens),
            "aggregated_in_queue": int(self._aggregated_in_queue),
            "physical_batch_ceiling": int(self._physical_batch_ceiling),
            "dispatch_ceiling": self._dispatch_ceiling_locked(),
            "duplicate_isolation_ceiling": self._duplicate_isolation_ceiling,
            "duplicate_isolation_events": int(
                self._duplicate_isolation_events),
            "evidence_conflicts": int(self._evidence_conflicts),
            "last_evidence_conflict": (
                None if self._last_evidence_conflict is None
                else dict(self._last_evidence_conflict)),
            "health": str(self._health),
            "submitted": int(self._submitted),
            "offered": int(self._offered),
            "admitted": int(self._admitted),
            "logical_written": int(self._logical_written),
            "written": int(self._written),
            "batches": int(self._batches),
            "batch_attempts": int(self._batch_attempts),
            "failed_batches": int(self._failed_batches),
            "deadline_exceeded_batches": int(self._deadline_exceeded_batches),
            "priority_skipped_batches": int(self._priority_skipped_batches),
            "checkpoint_deferral_batches": int(
                self._checkpoint_deferral_batches),
            "requeued_rows": int(self._requeued_rows),
            "coalesced": int(self._coalesced),
            "preoverflow_coalesced": int(self._preoverflow_coalesced),
            "sampled": int(self._sampled),
            "deferred": int(self._deferred),
            "deduplicated": int(self._deduplicated),
            "dropped": int(self._dropped),
            "admission_overflow_rows": int(self._admission_overflow_rows),
            "overflow_count": int(self._overflow_count),
            "reconciliation_mismatch_rows": int(
                self._reconciliation_mismatch_rows),
            # Cumulative per-category loss.  The delta between consecutive
            # ticks is what attributes a readiness reset to its exact cause.
            "loss_by_category": dict(self._loss_by_category),
            "flush_latency_ms": self._latency_percentiles(
                self._flush_latencies_ms),
            "batch_latency_ms": self._latency_percentiles(
                self._batch_latencies_ms),
            # Scheduling and physical-cost attribution.  Both are bounded --
            # the gate publishes a fixed set of counters, and the per-method
            # cost map cannot exceed the telemetry allowlist -- and both are
            # read only when an operator has enabled the dense observer.
            "gate": self._observer_gate_snapshot(),
            "sink_cost": self._observer_sink_cost(),
        }

    def _observer_gate_snapshot(self) -> dict[str, Any]:
        getter = getattr(self._persistence_writer, "write_gate_snapshot", None)
        if not callable(getter):
            return {}
        try:
            return dict(getter())
        except Exception:  # noqa: BLE001 - observation is never fatal
            return {"error": "gate_snapshot_failed"}

    def _observer_sink_cost(self) -> dict[str, Any]:
        getter = getattr(
            self._persistence_writer, "telemetry_commit_metrics", None)
        if not callable(getter):
            return {}
        try:
            metrics = getter()
        except Exception:  # noqa: BLE001 - observation is never fatal
            return {"error": "sink_metrics_failed"}
        if not isinstance(metrics, Mapping):
            return {}
        keys = (
            "batches", "calls", "failures", "last_duration_ms",
            "last_transaction_duration_ms", "max_transaction_duration_ms",
            "last_transaction_fixed_overhead_ms", "last_row_work_duration_ms",
            "last_row_call_p95_ms", "last_row_call_max_ms",
            "last_begin_duration_ms", "max_begin_duration_ms",
            "last_commit_duration_ms", "max_commit_duration_ms",
            "last_outer_transactions", "priority_skipped_batches",
            "deadline_exceeded_batches", "batch_rejected_count", "state",
        )
        snapshot: dict[str, Any] = {
            key: metrics[key] for key in keys if key in metrics
        }
        by_method = metrics.get("row_cost_by_method")
        if isinstance(by_method, Mapping):
            snapshot["row_cost_by_method"] = {
                str(method): dict(entry)
                for method, entry in by_method.items()
                if isinstance(entry, Mapping)
            }
        return snapshot

    @staticmethod
    def _latency_percentiles(samples: "deque[float]") -> dict[str, float]:
        if not samples:
            return {"p50": 0.0, "p90": 0.0, "p99": 0.0, "max": 0.0, "n": 0}
        ordered = sorted(samples)
        size = len(ordered)

        def at(fraction: float) -> float:
            index = min(size - 1, max(0, math.ceil(fraction * size) - 1))
            return round(float(ordered[index]), 3)

        return {
            "p50": at(0.50), "p90": at(0.90), "p99": at(0.99),
            "max": round(float(ordered[-1]), 3), "n": size,
        }

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

    def reconcile(self) -> dict[str, int]:
        """Close the conservation identity over the lane's whole lifetime.

        Every logical row the lane accepted must be in exactly one terminal or
        in-flight state::

            submitted
              == logical_committed
               + buffered
               + policy_sampled
               + policy_coalesced
               + policy_deduplicated
               + policy_deferred
               + unexpected_loss

        ``policy_coalesced`` is the *pre-admission* coalescing count only: a row
        merged into an already-queued pending raises that pending's logical
        count and is therefore counted once, at commit, in ``logical_committed``.
        Counting it in both places is the double-count this reconciliation
        exists to catch.

        A non-zero ``mismatch`` means the taxonomy no longer describes reality,
        which is itself blocking -- unaccounted rows are indistinguishable from
        silent loss.
        """

        with self._condition:
            return self._reconcile_locked()

    def _reconcile_locked(self) -> dict[str, int]:
        loss = dict(self._loss_by_category)
        policy_sampled = int(loss.get(
            TelemetryLossCategory.POLICY_SAMPLED.value, 0)) + self._sampled
        policy_coalesced = int(loss.get(
            TelemetryLossCategory.POLICY_COALESCED.value, 0)) + self._coalesced
        policy_deduplicated = int(loss.get(
            TelemetryLossCategory.POLICY_DEDUPLICATED.value, 0)
        ) + self._deduplicated
        unexpected = sum(
            int(count) for name, count in loss.items()
            if name in UNEXPECTED_LOSS_CATEGORIES
        )
        # Rows merged into a queued pending were counted in ``_coalesced`` but
        # are still carried to the sink inside that pending, so they must not be
        # subtracted twice.  ``_aggregated_in_queue`` is that overlap.
        accounted = (
            self._logical_written
            + self._queued_logical
            + self._inflight_logical
            + policy_sampled
            + policy_coalesced
            + policy_deduplicated
            + self._deferred
            + unexpected
            - self._aggregated_in_queue
        )
        mismatch = int(self._submitted) - int(accounted)
        self._reconciliation_mismatch_rows = abs(mismatch)
        result = {
            "submitted": int(self._submitted),
            "logical_committed": int(self._logical_written),
            "buffered": int(self._queued_logical),
            "inflight": int(self._inflight_logical),
            "policy_sampled": int(policy_sampled),
            "policy_coalesced": int(policy_coalesced),
            "policy_deduplicated": int(policy_deduplicated),
            "policy_deferred": int(self._deferred),
            "aggregated_in_queue": int(self._aggregated_in_queue),
            "unexpected_loss": int(unexpected),
            "accounted": int(accounted),
            "mismatch": int(mismatch),
        }
        self._last_reconciliation = result
        return result

    def _oldest_queued_age_s(self, now: float) -> float:
        """How long the oldest still-queued row has been waiting.

        Depth alone cannot tell a shallow queue that drains from a shallow queue
        that never drains, so residence age is tracked as an independent backlog
        signal.  The queue is FIFO by token order, so the head is the oldest and
        this stays O(1) on the hot path.
        """

        while self._queue:
            pending = self._pending.get(self._queue[0])
            if pending is not None:
                return max(0.0, float(now) - float(pending.admitted_monotonic))
            # A token whose pending was already taken; skip it.
            self._queue.popleft()
        return 0.0

    def _record_policy_locked(self, reason: str, count: int = 1) -> None:
        key = str(reason)
        self._policy_reasons[key] = (
            int(self._policy_reasons.get(key, 0)) + max(0, int(count)))

    def _sync_controller_locked(
        self, now_mono: Optional[float] = None,
        *, advance_state: bool = True,
    ) -> _ControlDecision:
        now = time.monotonic() if now_mono is None else float(now_mono)
        decision = self._controller.decide(
            now=now,
            queue_depth=len(self._queue),
            transaction_budget_ms=self._resolve_budget_locked(),
            advance_state=advance_state,
            queued_logical=self._queued_logical,
            oldest_age_s=self._oldest_queued_age_s(now),
            inflight_logical=self._inflight_logical,
            trace_context=self._trace_context_fn,
        )
        if advance_state:
            self._physical_batch_ceiling = decision.selected_chunk
            self._last_controller_sample_ts_ms = int(time.time() * 1_000)
            self._last_controller_sample_monotonic = now
        self._ms_per_row = decision.tail_ms_per_row
        self._last_control_decision = decision
        return decision

    def _sample_under_pressure_locked(
        self, *, item: TelemetryCommand, overload_key: Optional[str],
        keep_ratio: float,
    ) -> bool:
        """Return True when an explicitly sampleable row should be retained."""

        ratio = max(0.0, min(1.0, float(keep_ratio)))
        if ratio >= 1.0:
            return True
        if ratio <= 0.0:
            return False
        self._sample_sequence += 1
        digest = hashlib.sha256(json.dumps(
            {
                "sequence": self._sample_sequence,
                "key": overload_key,
                "command": item.payload(),
            },
            sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False, default=str,
        ).encode("utf-8")).digest()
        draw = int.from_bytes(digest[:8], "big") / float(2**64)
        return draw < ratio

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
        overload_policy: TelemetryOverloadPolicy | str = (
            TelemetryOverloadPolicy.ADMIT),
        overload_key: Optional[Hashable] = None,
    ) -> TelemetryDisposition:
        """Admit telemetry without waiting for persistence.

        ``state_key`` stores the first state and every material state change,
        while identical state is sampled only after ``coalescing_interval_s``.
        ``bucket_key`` aggregates commands within an event-time bucket using a
        caller-supplied deterministic ``merge_hook``.  The two policies are
        intentionally mutually exclusive.
        """

        item = self._command(command, tuple(args), kwargs)
        try:
            policy = (
                overload_policy
                if isinstance(overload_policy, TelemetryOverloadPolicy)
                else TelemetryOverloadPolicy(str(overload_policy).strip().upper())
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid telemetry overload policy") from exc
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
        if policy is TelemetryOverloadPolicy.LATEST and overload_key is None:
            if state_key is not None:
                overload_key = ("state", state_key)
            elif bucket_key is not None:
                overload_key = ("bucket", bucket_key)
            elif dedupe_key is not None:
                overload_key = ("dedupe", dedupe_key)
            else:
                raise ValueError(
                    "LATEST overload policy requires overload_key or a coalescing key")

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
        overload = (
            _canonical_digest((item.method, overload_key))
            if overload_key is not None else None
        )

        with self._condition:
            self._submitted += 1
            self._controller.add(
                now_mono, queue_depth=len(self._queue), incoming=1)
            if not self._accepting or self._stop_requested:
                self._drop_locked(
                    1, "telemetry_submit_after_stop",
                    category=TelemetryLossCategory.SHUTDOWN_ABANDONED)
                return TelemetryDisposition.DROPPED

            if dedupe is not None and (
                    dedupe in self._dedupe_seen or dedupe in self._pending_dedupe):
                self._coalesced += 1
                self._deduplicated += 1
                self._controller.add(
                    now_mono, queue_depth=len(self._queue), coalesced=1)
                return TelemetryDisposition.COALESCED

            if state is not None and signature is not None:
                prior = self._state_seen.get(state)
                if (prior is not None and prior[0] == signature
                        and now_mono - prior[1] < self.coalescing_interval_s):
                    self._state_seen.move_to_end(state)
                    self._coalesced += 1
                    self._controller.add(
                        now_mono, queue_depth=len(self._queue), coalesced=1)
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
                        self._drop_locked(
                            1, "telemetry_merge_failed",
                            category=TelemetryLossCategory.MALFORMED_ROW)
                        return TelemetryDisposition.DROPPED
                    pending.logical_count += 1
                    self._queued_logical += 1
                    self._aggregated_in_queue += 1
                    self._coalesced += 1
                    # This is the one coalescing path that puts a row *into*
                    # the lane's inventory without ever counting it as
                    # ``admitted``, so the conservation identity needs it as a
                    # second inflow.  Without the term the row leaves through
                    # ``logical_committed`` having entered through nothing, and
                    # an exact identity would report a phantom surplus for every
                    # aggregated row.
                    self._controller.add(
                        now_mono, queue_depth=len(self._queue),
                        coalesced=1, aggregated=1)
                    if dedupe is not None:
                        pending.dedupe_keys.add(dedupe)
                        self._pending_dedupe[dedupe] = token
                        self._cache_put(
                            self._dedupe_seen, dedupe, None, self.dedupe_capacity)
                    return TelemetryDisposition.COALESCED

            # Offered demand is counted after ordinary semantic
            # dedupe/state/bucket coalescing, but before overload policy.  It
            # therefore remains visible while SAMPLE/DEFER/LATEST deliberately
            # reduce physical admission and cannot self-clear overload.
            self._offered += 1
            self._controller.add(
                now_mono, queue_depth=len(self._queue), offered=1)
            decision = self._sync_controller_locked(now_mono)
            pressure = (
                decision.overload_active
                or len(self._queue) >= self._controller.high_water
            )
            if (pressure and policy is TelemetryOverloadPolicy.LATEST
                    and overload is not None):
                existing_token = self._overload_latest_tokens.get(overload)
                existing = self._pending.get(existing_token or -1)
                if existing is not None:
                    existing.command = item
                    # LATEST means the superseded state is intentionally
                    # coalesced, not physically/logically committed.  Keep the
                    # pending physical row count at one and update its state
                    # cache metadata to the replacement payload.
                    if state is not None and signature is not None:
                        existing.state_key = state
                        existing.state_signature = signature
                        existing.state_admitted_monotonic = now_mono
                        self._cache_put(
                            self._state_seen, state,
                            (signature, now_mono), self.state_capacity,
                        )
                    if dedupe is not None:
                        existing.dedupe_keys.add(dedupe)
                        self._pending_dedupe[dedupe] = existing.token
                        self._cache_put(
                            self._dedupe_seen, dedupe, None,
                            self.dedupe_capacity,
                        )
                    self._coalesced += 1
                    self._preoverflow_coalesced += 1
                    self._record_policy_locked("overload_latest_replaced")
                    self._controller.add(
                        now_mono, queue_depth=len(self._queue),
                        coalesced=1, overload_handled=1)
                    return TelemetryDisposition.COALESCED
            if pressure and policy is TelemetryOverloadPolicy.SAMPLE:
                if not self._sample_under_pressure_locked(
                        item=item, overload_key=overload,
                        keep_ratio=decision.sampling_keep_ratio):
                    self._sampled += 1
                    self._record_policy_locked(
                        decision.overload_reason or "queue_pressure_sampling")
                    self._controller.add(
                        now_mono, queue_depth=len(self._queue),
                        sampled=1, overload_handled=1)
                    return TelemetryDisposition.SAMPLED
            if pressure and policy is TelemetryOverloadPolicy.DEFER:
                self._deferred += 1
                self._record_policy_locked(
                    decision.overload_reason or "queue_pressure_deferred")
                self._controller.add(
                    now_mono, queue_depth=len(self._queue),
                    deferred=1, overload_handled=1)
                return TelemetryDisposition.DEFERRED

            if len(self._queue) >= self.capacity:
                # Reaching the hard bound is a capacity event, always counted.
                # Whether it is *loss* depends on what happens next: an approved
                # policy resolves it deterministically and without losing
                # evidence the lane promised to carry.  ``admission_overflow``
                # feeds the recovery-health gate, so recording it on the policy
                # paths made a correctly-shedding bounded queue permanently
                # unhealthy -- it is now recorded only when a row is truly lost.
                self._overflow_count += 1
                self._last_overflow_ts_ms = int(time.time() * 1_000)
                if policy is TelemetryOverloadPolicy.SAMPLE:
                    self._sampled += 1
                    self._record_policy_locked("queue_full_sampled")
                    self._controller.add(
                        now_mono, queue_depth=len(self._queue),
                        sampled=1, overload_handled=1)
                    return TelemetryDisposition.SAMPLED
                if policy is TelemetryOverloadPolicy.DEFER:
                    self._deferred += 1
                    self._record_policy_locked("queue_full_deferred")
                    self._controller.add(
                        now_mono, queue_depth=len(self._queue),
                        deferred=1, overload_handled=1)
                    return TelemetryDisposition.DEFERRED
                # The approved overload policies (LATEST / SAMPLE / DEFER) were
                # already offered above.  Reaching the hard bound anyway is
                # overflow outside policy and stays blocking.
                self._admission_overflow_rows += 1
                self._controller.add(
                    now_mono, queue_depth=len(self._queue),
                    admission_overflow=1)
                self._drop_locked(
                    1, "telemetry_queue_full",
                    category=TelemetryLossCategory.QUEUE_OVERFLOW,
                    health="DEGRADED_OVERFLOW")
                return TelemetryDisposition.DROPPED

            token = self._next_token
            self._next_token += 1
            pending = _Pending(
                token=token, command=item, admitted_monotonic=now_mono,
                dedupe_keys={dedupe} if dedupe is not None else set(),
                state_key=state, state_signature=signature,
                state_admitted_monotonic=now_mono if state is not None else None,
                aggregate_key=aggregate, merge_hook=merge_hook,
                overload_key=overload,
            )
            self._pending[token] = pending
            self._queue.append(token)
            self._queued_logical += 1
            self._admitted += 1
            self._controller.add(
                now_mono, queue_depth=len(self._queue), admitted=1)
            if aggregate is not None:
                self._aggregate_tokens[aggregate] = token
            if overload is not None:
                self._overload_latest_tokens[overload] = token
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
                     *, category: TelemetryLossCategory,
                     health: str = "DEGRADED_TELEMETRY",
                     post_admission: bool = False) -> None:
        """Account rows that leave the lane without being committed.

        ``category`` is mandatory: an uncategorised drop would break the
        conservation identity, and "we lost rows but cannot say why" is exactly
        the state the health model must treat as blocking.

        An approved-policy category is recorded and reported but does not mark
        the lane failed, does not touch ``_last_failure_ts_ms`` and does not
        reset the recovery streak -- those are reserved for unexpected loss.

        ``post_admission`` says whether these rows were counted in the window's
        ``admitted`` inflow.  It is the caller's knowledge, not something this
        method can infer, and the windowed conservation identity is wrong
        without it: an approved policy that resolves an *admitted* row removes
        it from the lane with no commit and no loss, so unless that exit is
        counted the window reports a shortfall that looks exactly like a service
        fault.  See :func:`_service_balanced`.
        """

        count = max(0, int(logical_count))
        if not count:
            return
        key = str(category.value)
        self._loss_by_category[key] = int(self._loss_by_category.get(key, 0)) + count
        self._drop_reasons[str(reason)] = (
            int(self._drop_reasons.get(str(reason), 0)) + count)
        if key in POLICY_LOSS_CATEGORIES:
            self._record_policy_locked(str(reason), count)
            resolved = {"policy_resolved": count} if post_admission else {}
            self._controller.add(
                time.monotonic(), queue_depth=len(self._queue),
                overload_handled=count, **resolved)
            return
        if self._stop_requested and self._drain_on_stop:
            self._drain_stop_failed = True
        self._dropped += count
        # ``lost`` is every unexpected loss and stays the health signal.
        # ``lost_admitted`` is the subset that had entered the admitted cohort,
        # and only that subset may be subtracted from it.  A row refused before
        # admission -- submitted after stop, a failed merge, hard queue overflow
        # -- was never inflow to the identity, so charging it as an outflow would
        # report a shortfall of rows the window never took in.
        admitted_loss = {"lost_admitted": count} if post_admission else {}
        self._controller.add(
            time.monotonic(), queue_depth=len(self._queue), lost=count,
            **admitted_loss)
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

    def _resolve_budget_locked(self) -> Optional[float]:
        if self._budget_ms is not None:
            return self._budget_ms
        getter = getattr(
            self._persistence_writer, "telemetry_transaction_budget_ms", None)
        if callable(getter):
            try:
                value = float(getter())
            except Exception:  # noqa: BLE001 - budget discovery is best effort
                return None
            if math.isfinite(value) and value > 0:
                self._budget_ms = value
        return self._budget_ms

    def _requeue_locked(
        self, rows: list[_Pending], *, consume_retry: bool = True,
    ) -> int:
        """Return unattempted rows to the front of the queue, preserving order.

        Used only when the physical sink cooperatively yielded before writing
        anything, so no row is duplicated: the sink's transaction rolled back.
        A requeued row becomes a standalone command -- its aggregate slot may
        already be owned by a newer pending row, and merging into it after the
        fact would reorder or hide that newer row.

        Retries are strictly finite.  A draining stop continues the same
        bounded retries; a stop request alone must never turn a cooperative
        maintenance/critical deferral into a false-successful discard.

        Returns the logical count that could not be requeued.
        """

        abandoned = 0
        keep: list[_Pending] = []
        for pending in rows:
            exhausted = (
                consume_retry
                and pending.requeue_attempts >= _MAX_PRIORITY_REQUEUE_ATTEMPTS
            )
            if ((self._stop_requested and not self._drain_on_stop)
                    or exhausted):
                abandoned += pending.logical_count
                self._rollback_admission_locked(pending)
                if self._stop_requested and self._drain_on_stop:
                    self._drain_stop_failed = True
                continue
            if consume_retry:
                pending.requeue_attempts += 1
            keep.append(pending)
        for pending in reversed(keep):
            pending.aggregate_key = None
            pending.merge_hook = None
            self._pending[pending.token] = pending
            self._queue.appendleft(pending.token)
            self._queued_logical += max(0, int(pending.logical_count))
            if (pending.overload_key is not None
                    and pending.overload_key not in self._overload_latest_tokens):
                self._overload_latest_tokens[pending.overload_key] = pending.token
            self._requeued_rows += pending.logical_count
        self._high_water = max(self._high_water, len(self._queue))
        return abandoned

    def _dispatch_ceiling_locked(self) -> int:
        """Rows per physical dispatch: the controller's ceiling, bisected.

        The controller owns the deadline-safe size.  Duplicate isolation may
        temporarily ask for something smaller, and never for something larger,
        so the two compose by taking the minimum.
        """

        ceiling = max(1, int(self._physical_batch_ceiling))
        isolation = self._duplicate_isolation_ceiling
        if isolation is None:
            return ceiling
        return max(1, min(ceiling, int(isolation)))

    def _take_batch_locked(self, limit: Optional[int] = None) -> list[_Pending]:
        maximum = (
            self.batch_size if limit is None
            else max(1, min(self.batch_size, int(limit)))
        )
        batch: list[_Pending] = []
        while self._queue and len(batch) < maximum:
            token = self._queue.popleft()
            pending = self._pending.pop(token, None)
            if pending is None:
                continue
            moved = max(0, int(pending.logical_count))
            self._queued_logical = max(0, self._queued_logical - moved)
            self._inflight_logical += moved
            if (pending.aggregate_key is not None
                    and self._aggregate_tokens.get(pending.aggregate_key) == token):
                self._aggregate_tokens.pop(pending.aggregate_key, None)
            if (pending.overload_key is not None
                    and self._overload_latest_tokens.get(
                        pending.overload_key) == token):
                self._overload_latest_tokens.pop(pending.overload_key, None)
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
                    self._sync_controller_locked(now, advance_state=True)
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
                        self._sync_controller_locked(now, advance_state=True)
                        next_heartbeat = now + self.heartbeat_interval_s

                if self._stop_requested and (
                        not self._drain_on_stop or not self._queue):
                    break

                # Cooperative deadline backoff: if the previous dispatch hit a
                # deadline-exceeded failure, defer the next flush until the
                # backoff window expires rather than resubmitting into a still-
                # stuck writer (which would only produce another miss + drops).
                # An explicit flush or non-draining stop overrides it.  A
                # draining stop must respect cooperative priority backoff or it
                # can exhaust all retries while maintenance is still closing.
                if (self._deadline_backoff_until > 0.0
                        and (
                            not self._stop_requested
                            or self._drain_on_stop
                        )
                        and not self._flush_requested):
                    remaining_backoff = self._deadline_backoff_until - time.monotonic()
                    if remaining_backoff > 0.0:
                        # Deferral must actually defer.  Dispatching inside the
                        # backoff window resubmits into a sink that is still
                        # missing its deadline and destroys another whole chunk
                        # -- exactly the tight retry-and-miss loop the backoff
                        # exists to prevent.  Admission keeps accepting rows and
                        # the heartbeat keeps ticking; only the flush waits.
                        self._condition.wait(min(
                            remaining_backoff, self.heartbeat_interval_s))
                        if time.monotonic() >= next_heartbeat:
                            heartbeat_now = time.monotonic()
                            self._last_heartbeat_ts_ms = int(time.time() * 1_000)
                            self._sync_controller_locked(
                                heartbeat_now, advance_state=True)
                            next_heartbeat = heartbeat_now + self.heartbeat_interval_s
                        continue
                    self._deadline_backoff_until = 0.0
                    self._deadline_backoff_s = 0.0

                if self._queue and not self._stop_requested and not self._flush_requested:
                    first = self._pending.get(self._queue[0])
                    deadline = (
                        first.admitted_monotonic + self.flush_interval_s
                        if first is not None else time.monotonic()
                    )
                    while (len(self._queue) < self._dispatch_ceiling_locked()
                           and not self._stop_requested
                           and not self._flush_requested):
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self._condition.wait(min(remaining, self.heartbeat_interval_s))
                        if time.monotonic() >= next_heartbeat:
                            heartbeat_now = time.monotonic()
                            self._last_heartbeat_ts_ms = int(time.time() * 1_000)
                            self._sync_controller_locked(
                                heartbeat_now, advance_state=True)
                            next_heartbeat = heartbeat_now + self.heartbeat_interval_s

                self._flush_requested = False
                # One physical chunk per dispatch: a rolled-back sink
                # transaction can then only ever cost the rows it actually
                # attempted, never a larger logical batch behind it.
                batch = self._take_batch_locked(
                    self._dispatch_ceiling_locked())

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

    def _transaction_cost_observation(
        self, result: Any, total_ms: float, *, rows: int = 0,
    ) -> tuple[float, Optional[float], Optional[float]]:
        """Extract transaction, fixed, and per-command tail cost metrics."""

        candidates: list[Mapping[str, Any]] = []
        if isinstance(result, Mapping):
            candidates.append(result)
        getter = getattr(
            self._persistence_writer, "telemetry_commit_metrics", None)
        if callable(getter):
            try:
                metrics = getter()
            except Exception:  # noqa: BLE001 - controller falls back to call time
                metrics = None
            if isinstance(metrics, Mapping):
                candidates.append(metrics)
        transaction_ms = max(1e-3, float(total_ms))
        for metrics in candidates:
            for key in (
                "transaction_duration_ms", "transaction_ms",
                "last_transaction_duration_ms", "last_transaction_ms",
            ):
                value = metrics.get(key)
                if (isinstance(value, (int, float))
                        and not isinstance(value, bool)
                        and math.isfinite(float(value))
                        and float(value) >= 0):
                    transaction_ms = min(
                        max(1e-3, float(value)),
                        max(1e-3, total_ms),
                    )
                    break
            else:
                continue
            break

        def metric_value(keys: tuple[str, ...]) -> Optional[float]:
            for metrics in candidates:
                for key in keys:
                    value = metrics.get(key)
                    if (isinstance(value, (int, float))
                            and not isinstance(value, bool)
                            and math.isfinite(float(value))
                            and float(value) >= 0):
                        return float(value)
            return None

        fixed = metric_value((
            "transaction_fixed_overhead_ms",
            "last_transaction_fixed_overhead_ms",
        ))
        row_work = metric_value((
            "row_work_duration_ms", "last_row_work_duration_ms",
        ))
        row_call_max = metric_value((
            "row_call_max_ms", "last_row_call_max_ms",
        ))
        tail_call = metric_value((
            "row_call_p95_ms", "last_row_call_p95_ms",
            "row_call_max_ms", "last_row_call_max_ms",
        ))
        # Separate the once-per-transaction wait from the true per-row cost.
        #
        # SQLite takes its write lock on the *first* write statement of a
        # transaction, so a cooperative yield to the critical writer, a
        # checkpoint, or ordinary lock contention is charged in full to
        # whichever single row call happens to block.  A p95 (or max) over the
        # per-row calls of a small chunk therefore *is* that wait, and the
        # controller multiplied it by the row count.
        #
        # Measured on the 54.6-minute gate at 44e58e2, during the nineteen
        # accumulation episodes -- every one of them preceded within 3 s by a
        # cooperative priority skip:
        #
        #     predicted transaction = fixed + marginal * size
        #                           = 6.76 + 3.25 * 54.16  = 183 ms
        #     measured transaction p95                     =  84 ms
        #
        # The model over-predicted by more than a factor of two, shrank the
        # chunk from 6.54 to 3.25 rows, and halved committed throughput (53.9
        # to 30.5 rows/s) while the sink completed the *same* number of
        # transactions per second (10.7 to 9.3).  The lane is transaction-rate
        # limited, not row limited, so shrinking the chunk could only deepen
        # the backlog it was trying to drain.
        #
        # A leave-one-out mean over the row calls recovers the real marginal
        # cost: drop the single most expensive call -- the one that absorbed
        # the wait -- and average the rest.  The excess that call carried is
        # genuine cost, so it is not discarded: it moves to the fixed term,
        # where a larger chunk amortises it instead of multiplying it.  Both
        # terms still feed ``deadline_safe_capacity`` unchanged, so the chunk
        # remains bounded by the same cooperative budget as before.
        marginal = None
        contended_excess = 0.0
        count = max(0, int(rows))
        if (count > 1 and row_work is not None and row_call_max is not None
                and row_work >= row_call_max):
            typical = (row_work - row_call_max) / float(count - 1)
            if math.isfinite(typical) and typical >= 0.0:
                marginal = max(1e-3, typical)
                contended_excess = max(0.0, row_call_max - marginal)
        if marginal is None and tail_call is not None:
            # Not enough per-row detail to isolate the blocked call (a
            # single-row chunk, or a sink that does not publish row work).
            # Fall back to the previous, deliberately conservative estimate.
            marginal = max(1e-3, tail_call)
        resolved_fixed: Optional[float] = None
        if fixed is not None or contended_excess:
            resolved_fixed = min(
                transaction_ms, max(0.0, (fixed or 0.0) + contended_excess))
        return (transaction_ms, resolved_fixed, marginal)

    def _dispatch(self, batch: list[_Pending]) -> None:
        started = time.monotonic()
        started_clock = time.perf_counter()
        written = 0
        logical_written = 0
        successful_batches = 0
        error = ""
        priority_skip = False
        checkpoint_deferral = False
        deadline_exceeded = False
        # Set only when the sink read the stored row and proved every
        # evidence-bearing field equivalent.  Never inferred from a message.
        verified_duplicate = False
        evidence_conflict: Optional[Any] = None
        failed_rows: list[_Pending] = []
        failed_chunk_rows = 0
        deadline_budget_ms: Optional[float] = None
        commit_observations: list[
            tuple[
                int, int, float, float,
                Optional[float], Optional[float],
            ]
        ] = []
        cursor = 0
        with self._condition:
            chunk_limit = self._dispatch_ceiling_locked()
        while cursor < len(batch):
            chunk = batch[cursor:cursor + chunk_limit]
            payload = [pending.command.payload() for pending in chunk]
            self._physical_batch_high_water = max(
                self._physical_batch_high_water, len(payload))
            call_started = time.monotonic()
            call_started_clock = time.perf_counter()
            try:
                with self._condition:
                    self._batch_attempts += 1
                    self._controller.add(
                        call_started, queue_depth=len(self._queue),
                        dispatch_attempts=1)
                result = self._persistence_writer.submit_telemetry_batch(
                    payload, timeout_s=self.submit_timeout_s)
                result = self._resolve_result(result)
                call_completed_clock = time.perf_counter()
                chunk_written = self._written_from_result(result, len(payload))
                if chunk_written != len(payload):
                    raise RuntimeError(
                        "physical telemetry writer returned a partial success")
                total_ms = max(
                    1e-3,
                    (call_completed_clock - call_started_clock) * 1_000.0)
                transaction_ms, fixed_ms, marginal_ms = (
                    self._transaction_cost_observation(
                        result, total_ms, rows=chunk_written))
                commit_observations.append((
                    chunk_written,
                    sum(row.logical_count for row in chunk),
                    transaction_ms,
                    total_ms,
                    fixed_ms,
                    marginal_ms,
                ))
            except Exception as exc:  # noqa: BLE001 - lossy lane is contained
                error = f"{type(exc).__name__}:{exc}"[:240]
                priority_skip = bool(
                    getattr(exc, "telemetry_priority_skip", False))
                checkpoint_deferral = bool(
                    getattr(exc, "telemetry_checkpoint_deferral", False))
                deadline_exceeded = bool(
                    getattr(exc, "telemetry_deadline_exceeded", False))
                verified_duplicate = bool(
                    getattr(exc, "telemetry_verified_duplicate", False))
                if getattr(exc, "telemetry_integrity_conflict", False):
                    # The stored row disagrees about the evidence.  Keep the
                    # full field-by-field verdict so the failure is diagnosable
                    # without re-reading the database.
                    evidence_conflict = getattr(exc, "collision", None)
                budget_attr = getattr(exc, "telemetry_deadline_ms", None)
                if isinstance(budget_attr, (int, float)) and not isinstance(
                        budget_attr, bool):
                    deadline_budget_ms = float(budget_attr)
                # The failing sink transaction rolled this chunk back.  Stop
                # immediately; all later chunks remain unwritten and are
                # explicitly accounted as dropped below.
                # A skipped, deferred or failed dispatch occupied the lane just
                # as a committed one did, so it is charged as service time too.
                # Committed chunks are charged by ``observe_commit`` from the
                # same ``total_ms`` they report, so no attempt is counted twice.
                with self._condition:
                    self._controller.observe_dispatch_busy(
                        time.monotonic(),
                        max(0.0,
                            (time.perf_counter() - call_started_clock)
                            * 1_000.0),
                    )
                failed_rows = batch[cursor:]
                failed_chunk_rows = len(chunk)
                break
            written += chunk_written
            logical_written += sum(row.logical_count for row in chunk)
            successful_batches += 1
            cursor += len(chunk)
        success = not failed_rows and cursor == len(batch)
        completed = time.monotonic()
        completed_clock = time.perf_counter()
        batch_ms = max(0.0, (completed_clock - started_clock) * 1_000.0)
        flush_ms = max(
            0.0,
            (completed - min(row.admitted_monotonic for row in batch)) * 1_000.0,
        )
        # Logical rows the sink did not write.  They are requeued on a
        # cooperative priority skip and dropped on a genuine batch failure.
        logical_unwritten = sum(row.logical_count for row in failed_rows)
        with self._condition:
            # The batch is no longer in flight: every row in it is about to be
            # counted as committed, requeued, or dropped below.  Release it
            # first so exactly one of those states owns each row.
            self._inflight_logical = max(
                0,
                self._inflight_logical
                - sum(max(0, int(row.logical_count)) for row in batch),
            )
            self._batch_latencies_ms.append(batch_ms)
            self._flush_latencies_ms.append(flush_ms)
            self._inflight_batches = max(0, self._inflight_batches - 1)
            self._batches += successful_batches
            self._written += written
            self._logical_written += logical_written
            for (
                committed_rows, committed_logical, transaction_ms,
                total_ms, fixed_ms, marginal_ms,
            ) in commit_observations:
                self._controller.observe_commit(
                    now=completed,
                    rows=committed_rows,
                    logical_rows=committed_logical,
                    transaction_ms=transaction_ms,
                    total_ms=total_ms,
                    queue_depth=len(self._queue),
                    fixed_overhead_ms=fixed_ms,
                    marginal_ms_per_row=marginal_ms,
                )
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
                # A clean commit also ends duplicate isolation: whatever the
                # bisection was hunting for is behind us, and the ceiling must
                # not outlive it.
                self._duplicate_isolation_ceiling = None
                self._consecutive_successful_batches += successful_batches
                self._sync_controller_locked(completed)
            elif priority_skip:
                # A cooperative yield to critical persistence is designed
                # behaviour, not a telemetry fault.  The sink rolled its
                # transaction back without writing anything, so these rows are
                # requeued rather than destroyed, the lane stays HEALTHY, and
                # no failure timestamp is stamped (which would otherwise make
                # every checkpoint and every burst of critical writes look like
                # a telemetry outage on the dashboard).  Only the next dispatch
                # is briefly deferred so yielding cannot become a spin against a
                # gate that is still held.
                self._consecutive_successful_batches = 0
                self._priority_skipped_batches += 1
                self._priority_skipped_rows += logical_unwritten
                self._last_priority_skip_ts_ms = int(time.time() * 1_000)
                self._controller.add(
                    completed, queue_depth=len(self._queue),
                    priority_deferrals=1)
                if checkpoint_deferral:
                    self._checkpoint_deferral_batches += 1
                    self._checkpoint_deferral_rows += logical_unwritten
                    self._last_checkpoint_deferral_ts_ms = int(
                        time.time() * 1_000)
                    self._controller.add(
                        completed, queue_depth=len(self._queue),
                        checkpoint_deferrals=1)
                abandoned = self._requeue_locked(
                    failed_rows,
                    consume_retry=not checkpoint_deferral,
                )
                if abandoned:
                    # Retry budget exhausted (or shutting down): the remaining
                    # rows are honest loss, not a deferral.
                    self._drop_locked(
                        abandoned, "telemetry_priority_skip_exhausted",
                        category=(
                            TelemetryLossCategory.ACKNOWLEDGEMENT_FAILURE),
                        health="DEGRADED_CRITICAL_PRIORITY",
                        post_admission=True)
                self._deadline_backoff_s = max(
                    self.flush_interval_s, 0.250)
                self._deadline_backoff_until = (
                    time.monotonic() + self._deadline_backoff_s)
            else:
                category = _classify_batch_failure(
                    error, deadline_exceeded=deadline_exceeded,
                    verified_duplicate=verified_duplicate)
                if evidence_conflict is not None:
                    # Surface the differing fields on the lane's own error, so
                    # an operator sees what disagreed rather than "batch
                    # failed".  This is blocking loss, not a policy outcome.
                    self._last_evidence_conflict = {
                        "table": getattr(evidence_conflict, "table", ""),
                        "key": dict(getattr(evidence_conflict, "key", {}) or {}),
                        "existing_rowid": getattr(
                            evidence_conflict, "existing_rowid", None),
                        "differing": {
                            name: [stored, offered] for name, (stored, offered)
                            in dict(getattr(
                                evidence_conflict, "differing", {}) or {}).items()
                        },
                    }
                    self._evidence_conflicts += 1
                # A batch rejected only because a content-addressed row is
                # already stored is a deduplication outcome, not a sink failure:
                # the evidence is present and nothing was lost.  Counting it as
                # a controller failure reset the recovery settle clock roughly
                # once a minute in production, which alone kept ``settling`` the
                # dominant blocker and held operational readiness down.
                policy_outcome = category.value in POLICY_LOSS_CATEGORIES
                # ...but the collision proves that about *one* row, and the
                # sink rolled the whole chunk back.  Attributing every row in it
                # to deduplication says "already stored" about rows that were
                # never stored at all -- measured on the 477 s run at 132382b, a
                # single collision discarded a seven-row chunk that way, and the
                # six innocent rows are exactly the shortfall that surfaced as
                # ``service_imbalance`` four seconds later.
                #
                # The chunk rolled back, so the evidence is intact and the rows
                # are still writable.  Halve the dispatch ceiling and requeue
                # them under the same bounded budget the cooperative priority
                # path uses: each retry commits the innocent half and re-collides
                # only on the half that holds the duplicate, so the collision is
                # isolated to a single row in at most log2(chunk) dispatches --
                # nine for the largest configured batch, against a budget of
                # thirty-two.  Only when the chunk *is* one row does the
                # collision prove anything about that row, and only then is it
                # attributed.
                # A draining stop can still afford the bisection -- it is the
                # same bounded retry the drain already performs, and stopping
                # early would put the false attribution back.  A *forced* stop
                # is discarding the queue anyway, so it does not bisect.
                # An integrity conflict is bisected on exactly the same
                # reasoning, and needs it more.  The collision proves something
                # about one row; the chunk rolled back, so the other rows are
                # untouched and still writable.  Condemning them as SINK_FAILURE
                # would destroy valid evidence to punish a row they have nothing
                # to do with -- the same false attribution the duplicate path
                # already exists to prevent, only now the outcome is blocking,
                # so the innocent rows would be counted as real loss.
                isolating_collision = (
                    failed_chunk_rows > 1
                    and not (self._stop_requested and not self._drain_on_stop)
                    and (
                        (policy_outcome
                         and category
                         is TelemetryLossCategory.POLICY_DEDUPLICATED)
                        or evidence_conflict is not None
                    )
                )
                if not policy_outcome:
                    self._failed_batches += 1
                    self._consecutive_successful_batches = 0
                if isolating_collision:
                    self._duplicate_isolation_ceiling = max(
                        1, failed_chunk_rows // 2)
                    self._duplicate_isolation_events += 1
                    abandoned = self._requeue_locked(failed_rows)
                    if abandoned:
                        # The budget is finite by design: a chunk that cannot be
                        # isolated is honest loss, never a silent policy outcome.
                        self._drop_locked(
                            abandoned,
                            "telemetry_duplicate_isolation_exhausted",
                            category=TelemetryLossCategory.SINK_FAILURE,
                            health="DEGRADED_WRITER",
                            post_admission=True)
                    self._sync_controller_locked(completed)
                    self._last_heartbeat_ts_ms = int(time.time() * 1_000)
                    self._condition.notify_all()
                    return
                # A policy outcome skips the *deadline adaptation* only.  It must
                # still fall through to the accounting branch below, because
                # ``_drop_locked`` is the sole place these rows are attributed --
                # they were already released from ``_inflight_logical`` when the
                # batch was taken, so short-circuiting here left them owned by
                # nobody and broke conservation permanently.
                if deadline_exceeded and not policy_outcome:
                    self._deadline_exceeded_batches += 1
                    self._deadline_exceeded_rows += logical_unwritten
                    health = "DEGRADED_TELEMETRY_DEADLINE"
                    budget_hint = deadline_budget_ms or self._budget_ms
                    if (budget_hint is not None
                            and math.isfinite(float(budget_hint))
                            and float(budget_hint) > 0):
                        self._budget_ms = float(budget_hint)
                    prior_ceiling = self._physical_batch_ceiling
                    self._controller.observe_deadline_miss(
                        now=completed,
                        failed_rows=failed_chunk_rows,
                        transaction_budget_ms=budget_hint,
                        queue_depth=len(self._queue),
                    )
                    # The controller is deliberately *not* synchronised here.
                    # The batch was released from ``_inflight_logical`` at the
                    # top of this block and is not returned to
                    # ``_queued_logical`` until the requeue below, so between
                    # those two points its rows are owned by nobody.  Sampling
                    # the conservation identity inside that window reports rows
                    # that are mid-transition as unaccounted -- measured once
                    # in 13,661 controller ticks on the 54.6-minute gate at
                    # dd52e9c, a deficit of exactly the two rows then being
                    # requeued, which alone raised ``service_imbalance``.
                    # The sync happens after the requeue instead, where every
                    # row is owned by exactly one state again.
                    # Exponential backoff (capped at 2 s) so a WAL-pinned slow
                    # commit does not cause a tight resubmit-and-miss loop.
                    # The queue keeps accepting; only the next flush is deferred.
                    previous = self._deadline_backoff_s
                    self._deadline_backoff_s = min(
                        2.0, max(self.flush_interval_s, previous * 2.0
                                 if previous > 0.0 else self.flush_interval_s))
                    self._deadline_backoff_until = (
                        time.monotonic() + self._deadline_backoff_s)
                    # A deadline miss under transient contention -- the failing
                    # chunk was within the deadline-safe size the controller had
                    # certified -- rolled its transaction back without committing
                    # anything, so the evidence is still valid.  Requeue it with
                    # the same bounded retry budget a cooperative priority skip
                    # uses, rather than dropping it immediately.  The controller
                    # has already shrunk the chunk above, so the next dispatch is
                    # smaller and more likely to commit under the same load.
                    # Only rows that exhaust the retry budget (or are abandoned
                    # at shutdown) become honest DEADLINE_EXPIRED loss; the
                    # requeue helper rolls back admission for those itself.
                    # A deadline miss rolls the transaction back whatever the
                    # chunk's size, so the evidence is intact either way and the
                    # size of the chunk says nothing about whether the rows are
                    # still writable.  Dropping the oversize case outright was
                    # the entire source of unexpected noncritical loss on the
                    # 48-minute gate at 5259b94: every one of its nine lost rows
                    # arrived through this branch, one-for-one with a
                    # deadline-exceeded batch, while the sink was starved by an
                    # 8.8-second integrity chunk rather than by anything about
                    # the batch.  The controller has already shrunk the chunk
                    # above, so the retry is dispatched smaller.
                    #
                    # Retries stay strictly finite -- ``_requeue_locked`` spends
                    # the same bounded budget the cooperative priority path uses
                    # -- so genuinely unwritable rows still become visible
                    # DEADLINE_EXPIRED loss instead of retrying forever.
                    abandoned = self._requeue_locked(failed_rows)
                    if abandoned:
                        self._drop_locked(
                            abandoned,
                            "telemetry_deadline_retry_exhausted",
                            category=TelemetryLossCategory.DEADLINE_EXPIRED,
                            health=health, post_admission=True)
                    # Now that every row is requeued or attributed, the
                    # controller may observe a consistent lane.  This is the
                    # same synchronisation the shrink accounting needs, moved
                    # to the far side of the transition.
                    self._sync_controller_locked(completed)
                    if self._physical_batch_ceiling < prior_ceiling:
                        self._deadline_shrink_events += 1
                else:
                    if policy_outcome:
                        # Evidence is already stored; keep the writer's health.
                        health = self._health
                    else:
                        health = "DEGRADED_WRITER"
                        # Only a genuine sink failure is a controller setback.
                        self._controller.add(
                            completed, queue_depth=len(self._queue),
                            failed_batches=1)
                    # The category decided above, not a second inference from
                    # the same inputs: recomputing it here is how the typed
                    # verdict could silently be dropped on the attribution path
                    # while the branch above still believed it.
                    self._drop_locked(
                        logical_unwritten, error or "telemetry_batch_failed",
                        category=category,
                        health=health, post_admission=True)
                    for pending in failed_rows:
                        self._rollback_admission_locked(pending)
            self._sync_controller_locked(completed)
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
                return (
                    self._health == "STOPPED"
                    and not self._queue
                    and (
                        not drain
                        or (
                            not self._drain_stop_failed
                            and self._dropped
                            == self._drain_stop_start_dropped
                        )
                    )
                )
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
                self._drain_stop_failed = False
                self._drain_stop_start_dropped = self._dropped
            elif not drain:
                # An explicit non-draining stop must be able to escalate a
                # draining stop that is already in progress.  Without this the
                # caller's forced second attempt silently keeps draining and
                # times out exactly like the first, so the shutdown path could
                # never actually force the lane to stop -- and its caller then
                # aborted before durably ending the runtime session.
                self._drain_on_stop = False
            self._health = "STOPPING"
            if not self._drain_on_stop:
                discarded = sum(row.logical_count for row in self._pending.values())
                for pending in self._pending.values():
                    self._rollback_admission_locked(pending)
                self._queue.clear()
                self._queued_logical = 0
                self._pending.clear()
                self._aggregate_tokens.clear()
                self._overload_latest_tokens.clear()
                self._pending_dedupe.clear()
                if discarded:
                    self._drop_locked(
                        discarded, "telemetry_shutdown_discard",
                        category=TelemetryLossCategory.SHUTDOWN_ABANDONED,
                        health="STOPPING", post_admission=True,
                    )
            self._condition.notify_all()
        if thread is None:
            with self._condition:
                self._health = "STOPPED"
            return (
                not self._queue
                and (
                    not drain
                    or (
                        not self._drain_stop_failed
                        and self._dropped == self._drain_stop_start_dropped
                    )
                )
            )
        thread.join(float(timeout_s))
        if thread.is_alive():
            with self._condition:
                self._health = "STOPPING_TIMEOUT"
                self._last_error = "telemetry_shutdown_timeout"
            return False
        with self._condition:
            return (
                self._health == "STOPPED"
                and not self._queue
                and (
                    not drain
                    or (
                        not self._drain_stop_failed
                        and self._dropped == self._drain_stop_start_dropped
                    )
                )
            )

    def snapshot(self) -> dict[str, Any]:
        """Return a sanitized, internally consistent telemetry health snapshot."""

        with self._condition:
            now_ms = int(time.time() * 1_000)
            decision = self._sync_controller_locked(advance_state=False)
            window = decision.view
            batch_values = tuple(self._batch_latencies_ms)
            flush_values = tuple(self._flush_latencies_ms)
            reconciliation = self._reconcile_locked()
            # ``decision.view`` deliberately excludes the current second so the
            # recovery streak is judged on settled buckets.  Data safety must be
            # more responsive than that: a row lost a moment ago is unsafe now,
            # not one second from now.  ``controller_view`` includes it.
            recent = decision.controller_view
            loss = dict(self._loss_by_category)
            recent_unexpected = int(recent.lost)
            # Data safety is about evidence, not throughput.  Approved policy
            # outcomes never appear here; only rows the lane was expected to
            # carry and did not.
            # Evidence loss is the only thing that makes the lane UNSAFE.  A
            # deadline miss or a failed batch whose rows were requeued and
            # committed cost no evidence at all -- measured on the soak, those
            # fired while unexpected loss stayed at exactly zero for 64.7
            # minutes.  They are real events an operator must see, so they are
            # reported as DEGRADED, but they do not brand the data unsafe and
            # they do not block readiness on their own.
            unsafe_reasons = [
                reason for reason, present in (
                    ("unexpected_noncritical_loss", recent_unexpected > 0),
                    ("accounting_reconciliation_mismatch",
                     int(reconciliation["mismatch"]) != 0),
                    ("recent_queue_overflow",
                     int(recent.admission_overflow) > 0),
                ) if present
            ]
            degraded_reasons = [
                reason for reason, present in (
                    ("recent_deadline_expiry_recovered",
                     int(recent.deadline_failures) > 0),
                    ("recent_sink_failure_recovered",
                     int(recent.failed_batches) > 0),
                ) if present
            ]
            data_safety_reasons = unsafe_reasons + degraded_reasons
            data_safety = (
                "UNSAFE" if unsafe_reasons
                else "DEGRADED" if degraded_reasons
                else "HEALTHY"
            )
            capacity_state = _capacity_state(
                decision, queue_depth=len(self._queue), capacity=self.capacity)
            return {
                "health": self._health,
                # --- explicit taxonomy / conservation ---------------------
                "loss_by_category": loss,
                "policy_loss_categories": sorted(POLICY_LOSS_CATEGORIES),
                "unexpected_loss_categories": sorted(UNEXPECTED_LOSS_CATEGORIES),
                "noncritical_rows_unexpectedly_lost": int(
                    reconciliation["unexpected_loss"]),
                "noncritical_rows_policy_sampled": int(
                    reconciliation["policy_sampled"]),
                "noncritical_rows_policy_coalesced": int(
                    reconciliation["policy_coalesced"]),
                "noncritical_rows_policy_deduplicated": int(
                    reconciliation["policy_deduplicated"]),
                "noncritical_rows_policy_deferred_outstanding": int(
                    reconciliation["policy_deferred"]),
                "noncritical_rows_buffered": int(reconciliation["buffered"]),
                "noncritical_rows_committed": int(
                    reconciliation["logical_committed"]),
                "queue_overflow_rows": int(loss.get(
                    TelemetryLossCategory.QUEUE_OVERFLOW.value, 0)),
                "deadline_expired_rows": int(loss.get(
                    TelemetryLossCategory.DEADLINE_EXPIRED.value, 0)),
                "sink_failure_rows": int(loss.get(
                    TelemetryLossCategory.SINK_FAILURE.value, 0)),
                "acknowledgement_failure_rows": int(loss.get(
                    TelemetryLossCategory.ACKNOWLEDGEMENT_FAILURE.value, 0)),
                "malformed_rows": int(loss.get(
                    TelemetryLossCategory.MALFORMED_ROW.value, 0)),
                "shutdown_abandoned_rows": int(loss.get(
                    TelemetryLossCategory.SHUTDOWN_ABANDONED.value, 0)),
                "accounting_reconciliation": reconciliation,
                "accounting_reconciliation_mismatch_rows": abs(
                    int(reconciliation["mismatch"])),
                # --- separated health model -------------------------------
                "telemetry_data_safety": data_safety,
                "telemetry_data_safety_reasons": data_safety_reasons,
                "telemetry_capacity_state": capacity_state,
                "window_unexpected_loss_rows": recent_unexpected,
                # Bounded means the queue never reached its hard limit.  Sitting
                # above the high-water mark is the shedding policy working, not
                # a bound being breached.
                "queue_bounded": bool(int(recent.max_depth) < self.capacity),
                # Backlog evidence, so a false or true accumulation verdict is
                # always explainable from the exported sample alone.
                "queue_oldest_age_s": round(
                    self._oldest_queued_age_s(time.monotonic()), 3),
                "queue_floor_before": recent.first_half_min_depth,
                "queue_floor_after": recent.second_half_min_depth,
                "queue_danger_depth": int(
                    self.capacity * _QUEUE_DANGER_FRACTION),
                "queue_max_depth_window": int(recent.max_depth),
                "queue_depth": len(self._queue),
                "queue_capacity": self.capacity,
                "queue_high_water": self._high_water,
                # The pressure threshold is published alongside the hard bound
                # precisely so the two are never conflated again: crossing low
                # water starts the shedding policy, it does not fail the lane.
                "queue_low_water": self._controller.low_water,
                "inflight_batches": self._inflight_batches,
                "submitted": self._submitted,
                "rows_submitted": self._submitted,
                "incoming": self._submitted,
                "rows_incoming": self._submitted,
                "offered": self._offered,
                "rows_offered": self._offered,
                "admitted": self._admitted,
                "rows_admitted": self._admitted,
                "coalesced": self._coalesced,
                "rows_coalesced": self._coalesced,
                "preoverflow_coalesced": self._preoverflow_coalesced,
                "sampled": self._sampled,
                "rows_sampled": self._sampled,
                "deferred": self._deferred,
                "rows_deferred": self._deferred,
                "deduplicated": self._deduplicated,
                "dropped": self._dropped,
                "rows_dropped": self._dropped,
                "true_lost_critical_rows": 0,
                "critical_rows_lost": 0,
                "admission_overflow_rows": self._admission_overflow_rows,
                "drop_reasons": dict(sorted(self._drop_reasons.items())),
                "overload_policy_reasons": dict(
                    sorted(self._policy_reasons.items())),
                "written": self._written,
                "rows_written": self._written,
                "logical_written": self._logical_written,
                "batches": self._batches,
                "batch_attempts": self._batch_attempts,
                "failed_batches": self._failed_batches,
                "priority_skipped_batches": self._priority_skipped_batches,
                "priority_skipped_rows": self._priority_skipped_rows,
                "requeued_rows": self._requeued_rows,
                "last_priority_skip_ts_ms": self._last_priority_skip_ts_ms or None,
                "checkpoint_deferral_batches": self._checkpoint_deferral_batches,
                "checkpoint_deferral_rows": self._checkpoint_deferral_rows,
                "last_checkpoint_deferral_ts_ms": (
                    self._last_checkpoint_deferral_ts_ms or None),
                "deadline_exceeded_batches": self._deadline_exceeded_batches,
                "deadline_exceeded_rows": self._deadline_exceeded_rows,
                "physical_batch_size": self.physical_batch_size,
                "physical_max_chunk": decision.physical_max_chunk,
                "physical_batch_ceiling": self._physical_batch_ceiling,
                "selected_chunk": decision.selected_chunk,
                "deadline_safe_chunk": decision.deadline_safe_chunk,
                "deadline_safe_chunk_estimate": decision.deadline_safe_chunk,
                "throughput_required_chunk": (
                    decision.throughput_required_chunk),
                "required_rows_per_dispatch": (
                    decision.throughput_required_chunk),
                "offered_required_chunk": decision.offered_required_chunk,
                "sustainable_dispatches_per_second": round(
                    decision.sustainable_dispatches_per_second, 4),
                "estimated_sink_capacity_rows_per_second": round(
                    decision.estimated_sink_capacity_rows_per_second, 4),
                "controller_state": decision.controller_state,
                "overload_active": decision.overload_active,
                "controlled_overload": decision.controlled_overload,
                "overload_reason": decision.overload_reason,
                "sampling_keep_ratio": round(
                    decision.sampling_keep_ratio, 6),
                "current_operational_healthy": (
                    decision.current_operational_healthy),
                "recovery_healthy_windows": decision.recovery_healthy_windows,
                "recovery_blockers": list(decision.recovery_blockers),
                "recovery_required_windows": _RECOVERY_HEALTHY_WINDOWS,
                "rate_window_seconds": _RATE_WINDOW_S,
                "recovery_settle_seconds": _RECOVERY_SETTLE_S,
                "recovery_sample_ts_ms": self._last_controller_sample_ts_ms,
                "recovery_sample_age_ms": round(max(
                    0.0,
                    time.monotonic()
                    - self._last_controller_sample_monotonic,
                ) * 1_000.0, 3),
                "incoming_rows_per_second": round(window.incoming_rps, 4),
                "offered_rows_per_second": round(window.offered_rps, 4),
                "admitted_rows_per_second": round(window.admitted_rps, 4),
                "committed_rows_per_second": round(window.committed_rps, 4),
                "logical_committed_rows_per_second": round(
                    window.logical_committed_rps, 4),
                "critical_rows_per_second": 0.0,
                "noncritical_rows_per_second": round(
                    window.incoming_rps, 4),
                "dispatches_per_second": round(
                    window.dispatch_success_rps, 4),
                "dispatch_attempts_per_second": round(
                    window.dispatch_attempt_rps, 4),
                "queue_depth_slope_per_second": round(
                    window.queue_slope_rps, 4),
                "window_incoming_rows": window.incoming,
                "window_offered_rows": window.offered,
                "window_admitted_rows": window.admitted,
                "window_committed_rows": window.committed,
                "window_coalesced_rows": window.coalesced,
                "window_sampled_rows": window.sampled,
                "window_deferred_rows": window.deferred,
                "window_overload_handled_rows": window.overload_handled,
                "window_lost_rows": window.lost,
                "window_admission_overflow_rows": (
                    window.admission_overflow),
                "window_failed_batches": window.failed_batches,
                "window_deadline_failures": window.deadline_failures,
                "window_checkpoint_deferrals": (
                    window.checkpoint_deferrals),
                # Admitted rows an approved policy resolved after admission.
                # Reported, never hidden: it is the fifth exit in the windowed
                # conservation identity and an operator must be able to see it.
                "window_policy_resolved_rows": window.policy_resolved,
                "duplicate_isolation_events": (
                    self._duplicate_isolation_events),
                # UNIQUE collisions the sink refused to call duplicates because
                # the stored row disagreed about the evidence.  Blocking, and
                # counted separately from deduplication so the two can never be
                # confused in the record.  The last one is carried field by
                # field so an operator sees what disagreed, not just that
                # something did.
                "evidence_conflicts": int(self._evidence_conflicts),
                "last_evidence_conflict": (
                    None if self._last_evidence_conflict is None
                    else dict(self._last_evidence_conflict)),
                "deadline_shrink_events": self._deadline_shrink_events,
                "transaction_budget_ms": self._budget_ms,
                "observed_ms_per_row": (
                    round(self._ms_per_row, 4)
                    if self._ms_per_row is not None else None),
                "transaction_duration_avg_ms": round(
                    decision.transaction_duration_avg_ms, 3),
                "transaction_duration_p95_ms": round(
                    decision.transaction_duration_p95_ms, 3),
                "transaction_duration_p99_ms": round(
                    decision.transaction_duration_p99_ms, 3),
                "transaction_fixed_overhead_ms": round(
                    decision.transaction_fixed_overhead_ms, 3),
                "transaction_tail_ms_per_row": (
                    round(decision.tail_ms_per_row, 4)
                    if decision.tail_ms_per_row is not None else None),
                "consecutive_successful_batches": (
                    self._consecutive_successful_batches),
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
                "drain_stop_failed": self._drain_stop_failed,
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
    "TelemetryOverloadPolicy",
    "TelemetrySink",
    "V4TelemetryWriter",
    "deadline_safe_capacity",
    "required_rows_per_dispatch",
    "sum_kwargs",
]
