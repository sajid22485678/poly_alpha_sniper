"""The readiness contract: which conditions may reset it, and which may not.

A 891-second identity-pinned run at 132382b reset ``_healthy_streak`` twenty-four
times.  Every reset carried exactly ``service_imbalance`` and
``uncontrolled_overload``, and every one of the 281 ticks that carried them had a
positive conservation deficit -- a one-to-one correspondence.  The cause was not
a service fault: unexpected loss, reconciliation mismatch, failed batches and
deadline failures were all zero for the whole run, the queue was bounded and
draining (depth zero at the reset tick), the controller was chunk-safe and the
policy was coherent.

The window's conservation identity was simply missing a term.  An admitted row
has five possible fates -- committed, queued, in flight, lost, or **resolved
after admission by an approved policy** -- and only the first four were counted.
The fifth exit is real: a UNIQUE collision on a content-addressed table proves
the byte-identical row is already durably stored, so the lane stops carrying it.
Its rows were counted as ``admitted`` inflow and never as any outflow, so the
window ran short by exactly that many rows for as long as the admitted bucket
stayed in the fifteen-second window.

These tests pin both halves of the repair and, just as importantly, pin what it
must *not* do: every genuinely unsafe condition still resets readiness, and the
new term cannot be used to hide one.
"""
from __future__ import annotations

import threading

import pytest

from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    POLICY_LOSS_CATEGORIES,
    TelemetryCapacityState,
    TelemetryLossCategory,
    TelemetryOverloadPolicy,
    V4TelemetryWriter,
    _AdaptiveTelemetryController,
    _RECOVERY_HEALTHY_WINDOWS,
    _RECOVERY_SETTLE_S,
    _RATE_WINDOW_S,
    _service_balanced,
)

#: One controller tick at the shadow configuration's flush interval.
TICK = 0.25
#: Comfortably past the settle window so ``settling`` is not the blocker.
SETTLED = _RECOVERY_SETTLE_S + 5.0


class _Sink:
    """Physical writer stand-in whose failure mode is programmable per call."""

    def __init__(self) -> None:
        self.batches: list[list[dict]] = []
        self.failures: list[BaseException | None] = []
        self.chunk_sizes: list[int] = []
        self._lock = threading.Lock()

    def fail_next(self, error: BaseException, times: int = 1) -> None:
        self.failures.extend([error] * times)

    def submit_telemetry_batch(self, commands, *, timeout_s):
        _ = timeout_s
        rows = [dict(command) for command in commands]
        with self._lock:
            self.chunk_sizes.append(len(rows))
            if self.failures:
                error = self.failures.pop(0)
                if error is not None:
                    raise error
            self.batches.append(rows)
        return len(rows)


class _DuplicateRowSink(_Sink):
    """One designated row is already stored; any chunk containing it rolls back.

    This is what SQLite actually does when one row of a multi-row transaction
    violates a UNIQUE constraint: the *whole* transaction rolls back, and the
    error names a single constraint without saying which row raised it.
    Bisecting the chunk is the only way to find out, and that is precisely what
    the lane must do before it may claim anything is "already stored".
    """

    def __init__(self, duplicate_marker: int) -> None:
        super().__init__()
        self.duplicate_marker = int(duplicate_marker)
        self.collisions = 0

    def _carries_duplicate(self, rows) -> bool:
        for row in rows:
            args = row.get("args") or ()
            for arg in args:
                if isinstance(arg, dict) and arg.get("n") == self.duplicate_marker:
                    return True
        return False

    def submit_telemetry_batch(self, commands, *, timeout_s):
        _ = timeout_s
        rows = [dict(command) for command in commands]
        with self._lock:
            self.chunk_sizes.append(len(rows))
            if self._carries_duplicate(rows):
                self.collisions += 1
                raise RuntimeError(
                    "IntegrityError:UNIQUE constraint failed: "
                    "book_snapshots.state_hash")
            self.batches.append(rows)
        return len(rows)


def _writer(sink, **overrides) -> V4TelemetryWriter:
    values = {
        "capacity": 256,
        "batch_size": 16,
        "flush_interval_s": 0.01,
        "coalescing_interval_s": 60.0,
        "submit_timeout_s": 1.0,
        "heartbeat_interval_s": 0.01,
    }
    values.update(overrides)
    return V4TelemetryWriter(sink, **values)


def _controller(*, maximum: int = 64,
                capacity: int = 20_000) -> _AdaptiveTelemetryController:
    return _AdaptiveTelemetryController(
        physical_max_chunk=maximum,
        queue_capacity=capacity,
        flush_interval_s=TICK,
        budgeted_sink=False,
        started_monotonic=0.0,
    )


def _tick(controller: _AdaptiveTelemetryController, now: float, *,
          queue_depth: int = 0, queued_logical: int | None = None,
          inflight_logical: int = 0, oldest_age_s: float = 0.0):
    """One control decision at an explicit time; no sleeping, no wall clock."""

    return controller.decide(
        now=now,
        queue_depth=queue_depth,
        transaction_budget_ms=None,
        queued_logical=(
            queue_depth if queued_logical is None else queued_logical),
        inflight_logical=inflight_logical,
        oldest_age_s=oldest_age_s,
    )


def _run_clean(controller: _AdaptiveTelemetryController, *, start: float,
               ticks: int, rows_per_tick: int = 4) -> float:
    """Admit and immediately commit ``rows_per_tick`` rows for ``ticks`` ticks."""

    now = start
    for _ in range(ticks):
        controller.add(now, queue_depth=0, incoming=rows_per_tick,
                       offered=rows_per_tick, admitted=rows_per_tick)
        controller.observe_commit(
            now=now, rows=rows_per_tick, logical_rows=rows_per_tick,
            transaction_ms=1.0, total_ms=1.0, queue_depth=0)
        _tick(controller, now)
        now += TICK
    return now


# ---------------------------------------------------------------------------
# The predicate itself, in isolation
# ---------------------------------------------------------------------------


def test_service_balanced_counts_post_admission_policy_resolution():
    """The exact 132382b shortfall: seven admitted rows resolved by policy."""

    controller = _controller()
    controller.add(0.0, queue_depth=0, admitted=100)
    controller.observe_commit(
        now=0.0, rows=93, logical_rows=93, transaction_ms=1.0, total_ms=1.0,
        queue_depth=0)
    view = controller.window.view(0.0, include_current=True)
    # Before the seven are accounted the identity is short by exactly seven,
    # and with an empty queue nothing absorbs it.
    assert view.admitted == 100
    assert view.logical_committed == 93
    assert not _service_balanced(view, queued_logical=0, inflight_logical=0)

    controller.add(0.0, queue_depth=0, policy_resolved=7, overload_handled=7)
    view = controller.window.view(0.0, include_current=True)
    assert view.policy_resolved == 7
    assert _service_balanced(view, queued_logical=0, inflight_logical=0)


def test_pre_admission_policy_events_cannot_close_the_identity():
    """``overload_handled`` alone must never satisfy conservation.

    Sampling, deferral and LATEST replacement all happen *before* admission, so
    their rows are on neither side.  If the predicate had simply used
    ``overload_handled`` it would have credited them, and a genuine gap of the
    same size would have been hidden.
    """

    controller = _controller()
    controller.add(0.0, queue_depth=0, admitted=100)
    controller.observe_commit(
        now=0.0, rows=93, logical_rows=93, transaction_ms=1.0, total_ms=1.0,
        queue_depth=0)
    # A large pre-admission policy volume, and not one admitted row resolved.
    controller.add(0.0, queue_depth=0, sampled=500, deferred=250,
                   coalesced=125, overload_handled=875)
    view = controller.window.view(0.0, include_current=True)
    assert view.overload_handled == 875
    assert view.policy_resolved == 0
    assert not _service_balanced(view, queued_logical=0, inflight_logical=0)


def test_genuine_conservation_gap_still_fails_the_predicate():
    """Rows that vanish without any accounting remain an imbalance."""

    controller = _controller()
    controller.add(0.0, queue_depth=0, admitted=100)
    controller.observe_commit(
        now=0.0, rows=80, logical_rows=80, transaction_ms=1.0, total_ms=1.0,
        queue_depth=0)
    controller.add(0.0, queue_depth=0, policy_resolved=5, overload_handled=5)
    view = controller.window.view(0.0, include_current=True)
    # 80 committed + 5 resolved + 0 held = 85 of 100.  Fifteen are unexplained.
    assert not _service_balanced(view, queued_logical=0, inflight_logical=0)
    # And they are still an imbalance when only partly held by the lane.
    assert not _service_balanced(view, queued_logical=10, inflight_logical=4)
    assert _service_balanced(view, queued_logical=10, inflight_logical=5)


def test_logical_and_physical_commit_units_are_not_compared():
    """Aggregation makes physical rows scarcer than logical ones, by design."""

    controller = _controller()
    controller.add(0.0, queue_depth=0, admitted=90)
    # Ninety logical rows aggregated into nine physical rows.
    controller.observe_commit(
        now=0.0, rows=9, logical_rows=90, transaction_ms=1.0, total_ms=1.0,
        queue_depth=0)
    view = controller.window.view(0.0, include_current=True)
    assert view.committed == 9 and view.logical_committed == 90
    assert _service_balanced(view, queued_logical=0, inflight_logical=0)


def test_in_flight_and_queued_rows_are_not_an_imbalance():
    """A bounded burst still inside the lane is held, not lost."""

    controller = _controller()
    controller.add(0.0, queue_depth=500, admitted=500)
    view = controller.window.view(0.0, include_current=True)
    assert _service_balanced(view, queued_logical=300, inflight_logical=200)
    assert not _service_balanced(view, queued_logical=300, inflight_logical=199)


# ---------------------------------------------------------------------------
# The readiness contract around the predicate
# ---------------------------------------------------------------------------


def test_post_admission_policy_resolution_does_not_reset_readiness():
    """The regression this whole change exists to prevent."""

    controller = _controller()
    now = _run_clean(controller, start=SETTLED, ticks=_RECOVERY_HEALTHY_WINDOWS)
    decision = _tick(controller, now)
    assert decision.recovery_blockers == ()
    assert decision.current_operational_healthy
    streak = decision.recovery_healthy_windows

    # A chunk of admitted rows is resolved by an approved policy, and the queue
    # then drains completely -- the exact shape of the observed transient.
    now += TICK
    controller.add(now, queue_depth=0, incoming=8, offered=8, admitted=8)
    now += TICK
    controller.add(now, queue_depth=0, policy_resolved=8, overload_handled=8)
    decision = _tick(controller, now, queue_depth=0, queued_logical=0)
    assert "service_imbalance" not in decision.recovery_blockers
    assert "uncontrolled_overload" not in decision.recovery_blockers
    assert decision.recovery_healthy_windows > streak


def test_genuine_service_imbalance_still_resets_readiness():
    controller = _controller()
    now = _run_clean(controller, start=SETTLED, ticks=_RECOVERY_HEALTHY_WINDOWS)
    assert _tick(controller, now).current_operational_healthy

    # Rows admitted and never accounted for anywhere.
    now += TICK
    controller.add(now, queue_depth=0, incoming=50, offered=50, admitted=50)
    decision = _tick(controller, now, queue_depth=0, queued_logical=0)
    assert "service_imbalance" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0
    assert not decision.current_operational_healthy


def test_genuine_uncontrolled_overload_still_resets_readiness():
    """Overload plus a real accumulating queue is uncontrolled, and blocks."""

    controller = _controller(maximum=4, capacity=64)
    now = SETTLED
    # Drive the queue up monotonically with nothing committing: the floor rises,
    # depth reaches a material fraction of capacity, and admissions outrun the
    # sink -- overload with the policy failing to resolve it.
    depth = 0
    for _ in range(40):
        depth += 8
        controller.add(now, queue_depth=depth, incoming=8, offered=8,
                       admitted=8)
        decision = _tick(controller, now, queue_depth=depth,
                         queued_logical=depth, oldest_age_s=depth / 8.0)
        now += TICK
    assert decision.overload_active
    assert "uncontrolled_overload" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0


def test_uncontrolled_overload_is_not_reported_for_a_safely_absorbed_burst():
    """Sustained shedding with bounded, draining queues is controlled."""

    controller = _controller(maximum=4, capacity=20_000)
    now = SETTLED
    decision = None
    for index in range(60):
        # Offered load far exceeds what a 50 ms transaction can sustain at the
        # deadline-safe chunk, so overload engages; every *admitted* row still
        # commits and the queue returns to empty on every tick.
        controller.add(now, queue_depth=0, incoming=200, offered=200,
                       admitted=40, sampled=160, overload_handled=160)
        controller.observe_commit(
            now=now, rows=4, logical_rows=40, transaction_ms=50.0,
            total_ms=50.0, queue_depth=0)
        decision = _tick(controller, now, queue_depth=0, queued_logical=0)
        now += TICK
    assert decision.overload_active
    assert decision.controlled_overload
    assert "uncontrolled_overload" not in decision.recovery_blockers
    assert "service_imbalance" not in decision.recovery_blockers
    assert decision.current_operational_healthy


def test_healthy_streak_rebuilds_on_the_unchanged_contract():
    controller = _controller()
    now = _run_clean(controller, start=SETTLED, ticks=_RECOVERY_HEALTHY_WINDOWS)
    assert _tick(controller, now).current_operational_healthy

    now += TICK
    controller.add(now, queue_depth=0, incoming=50, offered=50, admitted=50)
    assert _tick(controller, now, queue_depth=0,
                 queued_logical=0).recovery_healthy_windows == 0

    # Exactly ``_RECOVERY_HEALTHY_WINDOWS`` clean ticks, no more and no fewer.
    now += TICK
    controller.add(now, queue_depth=0, policy_resolved=50, overload_handled=50)
    seen = []
    for index in range(_RECOVERY_HEALTHY_WINDOWS + 2):
        decision = _tick(controller, now, queue_depth=0, queued_logical=0)
        seen.append(
            (decision.recovery_healthy_windows,
             decision.current_operational_healthy))
        now += TICK
    assert [count for count, _ in seen][:_RECOVERY_HEALTHY_WINDOWS] == list(
        range(1, _RECOVERY_HEALTHY_WINDOWS + 1))
    assert not any(healthy for _, healthy in seen[:_RECOVERY_HEALTHY_WINDOWS - 1])
    assert seen[_RECOVERY_HEALTHY_WINDOWS - 1][1] is True


def test_alternating_transient_blockers_never_certify():
    """A blocker every other tick can never accumulate ten clean windows."""

    controller = _controller()
    now = _run_clean(controller, start=SETTLED, ticks=_RECOVERY_HEALTHY_WINDOWS)
    healthy_seen = []
    for index in range(40):
        if index % 2 == 0:
            controller.add(now, queue_depth=0, incoming=9, offered=9,
                           admitted=9)
        decision = _tick(controller, now, queue_depth=0, queued_logical=0)
        healthy_seen.append(decision.current_operational_healthy)
        if index % 2 == 0:
            # Account them honestly on the following tick.
            controller.add(now, queue_depth=0, policy_resolved=9,
                           overload_handled=9)
        now += TICK
    assert not any(healthy_seen)


def test_stale_controller_view_cannot_survive_a_revision_change():
    """The cached window view is keyed on the ring revision, not just time."""

    controller = _controller()
    window = controller.window
    first = window.view(1.0, include_current=True)
    assert window.view(1.0, include_current=True) is first  # pure cache hit
    window.add(1.0, admitted=5)
    second = window.view(1.0, include_current=True)
    assert second is not first
    assert second.admitted == first.admitted + 5
    # Recycling a bucket also changes which seconds the window covers.
    revision = window._revision
    window.add(1.0 + _RATE_WINDOW_S, admitted=1)
    assert window._revision > revision
    assert window.view(1.0 + _RATE_WINDOW_S, include_current=True) is not second


def test_controller_snapshot_terms_are_internally_consistent():
    """The observed terms must satisfy the predicate the controller evaluated."""

    controller = _controller()
    controller.enable_readiness_trace()
    now = _run_clean(controller, start=SETTLED, ticks=6)
    controller.add(now, queue_depth=3, incoming=10, offered=10, admitted=10)
    _tick(controller, now, queue_depth=3, queued_logical=3, inflight_logical=2)
    samples = controller.drain_readiness_trace()
    assert samples
    for sample in samples:
        terms = sample["conservation"]
        recomputed = (
            terms["admitted_window"]
            - terms["logical_committed_window"]
            - terms["lost_window"]
            - terms["policy_resolved_window"]
            - terms["queued_logical_now"]
            - terms["inflight_logical_now"]
        )
        assert recomputed == terms["deficit"]
        assert sample["service_balanced"] == (terms["deficit"] <= 0)


# ---------------------------------------------------------------------------
# End-to-end through the writer
# ---------------------------------------------------------------------------


def test_duplicate_collision_isolates_instead_of_condemning_the_chunk():
    """One collision must not be attributed to every row it rolled back."""

    sink = _DuplicateRowSink(duplicate_marker=9)
    writer = _writer(sink, batch_size=16)
    writer.start()
    try:
        for index in range(16):
            writer.submit("record_book_snapshot", {"n": index})
        assert writer.flush(timeout_s=10.0)
    finally:
        assert writer.stop(drain=True, timeout_s=10.0)

    snapshot = writer.snapshot()
    loss = snapshot["loss_by_category"]
    # Exactly one row is proven already-stored; every other row still commits.
    assert loss[TelemetryLossCategory.POLICY_DEDUPLICATED.value] == 1
    assert snapshot["noncritical_rows_unexpectedly_lost"] == 0
    written = sum(len(batch) for batch in sink.batches)
    assert written == 15, f"expected 15 innocent rows committed, got {written}"
    # Bisection, not a single condemning attribution.
    assert sink.collisions >= 2
    assert min(sink.chunk_sizes) == 1
    assert writer.reconcile()["mismatch"] == 0


def test_duplicate_isolation_ceiling_does_not_outlive_the_collision():
    sink = _DuplicateRowSink(duplicate_marker=9)
    writer = _writer(sink, batch_size=16)
    writer.start()
    try:
        for index in range(16):
            writer.submit("record_book_snapshot", {"n": index})
        assert writer.flush(timeout_s=10.0)
        for index in range(16, 48):
            writer.submit("record_book_snapshot", {"n": index})
        assert writer.flush(timeout_s=10.0)
    finally:
        assert writer.stop(drain=True, timeout_s=10.0)
    assert writer._duplicate_isolation_ceiling is None
    # The ceiling recovered, so later dispatches are not stuck at one row.
    assert max(sink.chunk_sizes) > 1
    assert writer.reconcile()["mismatch"] == 0


def test_conservation_closes_under_burst_retry_sampling_and_shutdown():
    sink = _Sink()
    # Two transient sink failures inside the run exercise the retry path.
    sink.fail_next(RuntimeError("SinkBusy:transient"), times=1)
    writer = _writer(sink, capacity=64, batch_size=8)
    writer.start()
    try:
        for index in range(600):
            writer.submit(
                "record_book_snapshot", {"n": index},
                overload_policy=TelemetryOverloadPolicy.SAMPLE,
                overload_key="books")
        writer.flush(timeout_s=10.0)
    finally:
        writer.stop(drain=True, timeout_s=10.0)
    reconciliation = writer.reconcile()
    assert reconciliation["mismatch"] == 0
    assert reconciliation["submitted"] == 600
    assert reconciliation["accounted"] == 600


def test_policy_sampling_stays_fail_closed_while_rows_are_actually_lost():
    """Shedding is safe; shedding *and* losing rows is not."""

    sink = _Sink()
    writer = _writer(sink, capacity=8, batch_size=4)
    for index in range(400):
        # ADMIT refuses to shed, so the hard bound is reached outside policy.
        writer.submit("record_event", {"n": index},
                      overload_policy=TelemetryOverloadPolicy.ADMIT)
    snapshot = writer.snapshot()
    assert snapshot["noncritical_rows_unexpectedly_lost"] > 0
    assert snapshot["telemetry_data_safety"] != "HEALTHY"
    assert snapshot["telemetry_capacity_state"] == (
        TelemetryCapacityState.HARD_OVERLOAD.value)


def test_post_admission_policy_rows_are_still_reported_never_hidden():
    """The new term is an accounting fix, not a way to stop reporting."""

    sink = _DuplicateRowSink(duplicate_marker=5)
    writer = _writer(sink, batch_size=8)
    writer.start()
    try:
        for index in range(8):
            writer.submit("record_book_snapshot", {"n": index})
        assert writer.flush(timeout_s=10.0)
    finally:
        assert writer.stop(drain=True, timeout_s=10.0)
    snapshot = writer.snapshot()
    assert snapshot["noncritical_rows_policy_deduplicated"] == 1
    assert snapshot["duplicate_isolation_events"] >= 1
    assert TelemetryLossCategory.POLICY_DEDUPLICATED.value in (
        POLICY_LOSS_CATEGORIES)
    assert snapshot["accounting_reconciliation_mismatch_rows"] == 0


# ---------------------------------------------------------------------------
# The observer must be invisible unless an operator asks for it
# ---------------------------------------------------------------------------


def test_readiness_trace_is_off_by_default():
    controller = _controller()
    _run_clean(controller, start=SETTLED, ticks=4)
    assert controller._trace is None
    assert controller.drain_readiness_trace() == []


def test_readiness_trace_ring_is_bounded_and_drains():
    controller = _controller()
    controller.enable_readiness_trace(capacity=8)
    _run_clean(controller, start=SETTLED, ticks=40)
    drained = controller.drain_readiness_trace()
    assert len(drained) == 8              # bounded: oldest samples are dropped
    assert controller._trace_dropped > 0  # and the loss is counted, not hidden
    assert controller.drain_readiness_trace() == []
    assert [sample["seq"] for sample in drained] == sorted(
        sample["seq"] for sample in drained)


@pytest.mark.parametrize("capacity", [0, -1, True])
def test_readiness_trace_rejects_a_nonsense_capacity(capacity):
    controller = _controller()
    with pytest.raises(ValueError):
        controller.enable_readiness_trace(capacity=capacity)
