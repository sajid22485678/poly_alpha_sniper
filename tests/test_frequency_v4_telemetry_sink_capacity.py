"""Sink capacity, scheduling fairness and their accounting.

The defect these cover was measured on a 22-minute real-ingest run at
``10f23e9``.  The controller derived its sustainable dispatch rate from
``dispatch_successes / backlogged_seconds``.  ``backlogged_seconds`` counts every
second in which the queue was non-empty -- including the time the aggregator
spends *deliberately waiting* for a batch to fill before dispatching.  Dividing
by it measures the batching cadence, never the sink.

Measured consequence: the clamp fired on 99.9% of control ticks and held the
estimate at 3.47 dispatches/s while the same run reached 12/s at p90 and 140/s
at peak, and while the sink's own p95 transaction time permitted 8/s.  The
controller concluded it needed 43 rows per dispatch against a 32-row physical
chunk, declared ``throughput_exceeds_deadline_safe_capacity`` on 93.9% of ticks,
and shed 28.0% of all offered telemetry.  Shedding then held admitted load at
the cadence ceiling, which held the observed rate low: a closed loop.

The fix charges *service* time -- the wall time a dispatch actually occupied the
lane -- and leaves every safety clamp in place.  These tests pin both halves of
that: the false pressure is gone, and genuine incapacity is still detected.
"""
from __future__ import annotations

import threading
import time

import pytest

from poly_alpha_sniper.lite_frequency_v4.persistence import (
    V4PersistenceCommand,
    V4PersistenceWriter,
    V4TelemetryPrioritySkip,
    _CriticalFirstWriteGate,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store
from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    _AdaptiveTelemetryController,
    required_rows_per_dispatch,
)

NOW = 1_785_000_000_000


def _controller(*, maximum: int = 32, capacity: int = 20_000, flush_s: float = 0.25):
    return _AdaptiveTelemetryController(
        physical_max_chunk=maximum,
        queue_capacity=capacity,
        flush_interval_s=flush_s,
        budgeted_sink=True,
        started_monotonic=0.0,
    )


def _drive(controller, *, ticks: int, depth: int, admitted: int,
           rows: int, dispatch_ms: float, budget_ms: float = 250.0):
    """One dispatch per whole second, exactly as a flush-paced lane behaves."""

    decision = None
    for tick in range(1, ticks + 1):
        now = float(tick)
        controller.add(now, queue_depth=depth, incoming=admitted,
                       offered=admitted, admitted=admitted)
        controller.observe_commit(
            now=now, rows=rows, logical_rows=rows,
            transaction_ms=dispatch_ms, total_ms=dispatch_ms,
            queue_depth=depth)
        decision = controller.decide(
            now=now, queue_depth=depth, transaction_budget_ms=budget_ms,
            queued_logical=depth)
    return decision


def _cadence_estimate(controller, now: float) -> float:
    """What the reverted implementation would have computed."""

    view = controller.window.view(now, include_current=True)
    assert view.backlogged_seconds > 0
    return view.dispatch_successes / view.backlogged_seconds


# ---------------------------------------------------------------------------
# 1. The defect itself, and that reverting the fix reproduces it.
# ---------------------------------------------------------------------------


def test_capacity_is_measured_from_service_time_not_backlog_time():
    controller = _controller()
    decision = _drive(controller, ticks=60, depth=50, admitted=100,
                      rows=32, dispatch_ms=10.0)

    view = controller.window.view(60.0, include_current=True)
    # The lane held rows in every second of the window but only worked for a
    # hundredth of each one.  These two denominators are the whole defect.
    assert view.backlogged_seconds >= 10.0
    assert view.dispatch_busy_s == pytest.approx(
        view.dispatch_successes * 0.010, rel=1e-6)

    cadence = _cadence_estimate(controller, 60.0)
    assert cadence <= 2.0, "the reverted formula must still measure the cadence"

    # A 10 ms dispatch sustains 100/s, and that is what the controller now uses.
    assert decision.sustainable_dispatches_per_second == pytest.approx(
        100.0, rel=0.05)
    # Non-vacuity: the two answers differ by more than an order of magnitude.
    assert decision.sustainable_dispatches_per_second > 25 * cadence


def test_idle_batching_delay_no_longer_declares_false_capacity_pressure():
    """The exact production shape: fast sink, standing queue, flush cadence."""

    controller = _controller()
    decision = _drive(controller, ticks=60, depth=50, admitted=130,
                      rows=32, dispatch_ms=10.0)

    # Reverted behaviour, computed from the same window the controller saw.
    cadence = _cadence_estimate(controller, 60.0)
    view = controller.window.view(60.0, include_current=True)
    would_have_required = required_rows_per_dispatch(
        admitted_rows_per_second=view.offered_rps,
        sustainable_dispatches_per_second=cadence,
        queue_depth=50,
        queue_target=controller.queue_target,
    )
    assert would_have_required > controller.physical_max_chunk, (
        "the reverted estimate must still demand an impossible chunk")

    # Fixed behaviour: the requirement is satisfiable, so nothing is shed.
    assert decision.offered_required_chunk <= decision.deadline_safe_chunk
    assert decision.overload_active is False
    assert decision.sampling_keep_ratio == 1.0


# ---------------------------------------------------------------------------
# 2. Every safety clamp the old term was reaching for is still in force.
# ---------------------------------------------------------------------------


def test_a_genuinely_slow_sink_still_declares_overload():
    """Real incapacity must still be detected and still shed."""

    controller = _controller()
    decision = _drive(controller, ticks=60, depth=50, admitted=400,
                      rows=32, dispatch_ms=500.0)

    # Two dispatches a second is a real ceiling, not a batching artefact.
    assert decision.sustainable_dispatches_per_second <= 2.5
    assert decision.offered_required_chunk > decision.deadline_safe_chunk
    assert decision.overload_active is True
    assert decision.sampling_keep_ratio < 1.0


def test_queue_accumulation_still_blocks_readiness():
    """The fail-closed blocker must survive the capacity change."""

    controller = _controller()
    decision = None
    # A queue whose floor rises every second under a sink that cannot keep up.
    for tick in range(1, 61):
        now = float(tick)
        depth = 40 + tick * 12
        controller.add(now, queue_depth=depth, incoming=600, offered=600,
                       admitted=600)
        controller.observe_commit(
            now=now, rows=32, logical_rows=32,
            transaction_ms=400.0, total_ms=400.0, queue_depth=depth)
        decision = controller.decide(
            now=now, queue_depth=depth, transaction_budget_ms=250.0,
            queued_logical=depth)

    assert "queue_accumulating" in decision.recovery_blockers
    assert decision.current_operational_healthy is False


def test_unproductive_dispatches_still_consume_capacity():
    """Skips and failures burn the lane; capacity must not ignore them."""

    productive = _controller()
    _drive(productive, ticks=40, depth=50, admitted=100, rows=32,
           dispatch_ms=10.0)

    wasteful = _controller()
    for tick in range(1, 41):
        now = float(tick)
        wasteful.add(now, queue_depth=50, incoming=100, offered=100,
                     admitted=100)
        # Nine dispatches out of ten yield nothing but still occupy the lane.
        wasteful.observe_dispatch_busy(now, 90.0)
        wasteful.observe_commit(
            now=now, rows=32, logical_rows=32,
            transaction_ms=10.0, total_ms=10.0, queue_depth=50)
        wasteful.decide(now=now, queue_depth=50, transaction_budget_ms=250.0,
                        queued_logical=50)

    fast = productive.decide(
        now=40.0, queue_depth=50, transaction_budget_ms=250.0,
        advance_state=False)
    slow = wasteful.decide(
        now=40.0, queue_depth=50, transaction_budget_ms=250.0,
        advance_state=False)
    assert slow.sustainable_dispatches_per_second < (
        fast.sustainable_dispatches_per_second / 5)


# ---------------------------------------------------------------------------
# 3. Busy-time accounting is exact: charged once, never negative, validated.
# ---------------------------------------------------------------------------


def test_commit_charges_its_service_time_exactly_once():
    controller = _controller()
    controller.observe_commit(
        now=1.0, rows=8, logical_rows=8,
        transaction_ms=12.0, total_ms=12.0, queue_depth=4)
    after_one = controller.window.view(1.0).dispatch_busy_s
    controller.observe_commit(
        now=1.0, rows=8, logical_rows=8,
        transaction_ms=12.0, total_ms=12.0, queue_depth=4)
    after_two = controller.window.view(1.0).dispatch_busy_s
    assert after_one == pytest.approx(0.012, rel=1e-9)
    assert after_two == pytest.approx(0.024, rel=1e-9)


def test_a_dispatch_that_writes_nothing_is_still_charged():
    """An empty commit occupied the lane; ignoring it overstates capacity."""

    controller = _controller()
    controller.observe_commit(
        now=1.0, rows=0, logical_rows=0,
        transaction_ms=7.0, total_ms=7.0, queue_depth=1)
    view = controller.window.view(1.0)
    assert view.dispatch_busy_s == pytest.approx(0.007, rel=1e-9)
    assert view.dispatch_successes == 0


@pytest.mark.parametrize("bad", [-1.0, float("nan"), float("inf"), True])
def test_busy_time_rejects_invalid_durations(bad):
    controller = _controller()
    with pytest.raises(ValueError):
        controller.observe_dispatch_busy(1.0, bad)


def test_busy_time_is_windowed_and_cannot_grow_without_bound():
    controller = _controller()
    for tick in range(1, 200):
        controller.observe_dispatch_busy(float(tick), 50.0)
    view = controller.window.view(199.0)
    # Only the rolling window is retained, so the term is bounded by it.
    assert view.dispatch_busy_s <= controller.window.window_s * 0.050 + 1e-9


# ---------------------------------------------------------------------------
# 4. Write-gate starvation accounting.
# ---------------------------------------------------------------------------


def test_consecutive_priority_skips_are_counted_and_reset_on_admission():
    gate = _CriticalFirstWriteGate()
    gate.register_critical()
    for _ in range(5):
        assert gate.try_acquire_telemetry_reason() == "critical_persistence_pending"
    assert gate.snapshot()["consecutive_telemetry_skips"] == 5
    assert gate.snapshot()["max_consecutive_telemetry_skips"] == 5

    gate.acquire_critical()
    gate.release_critical()
    # The critical work is done, so the very next telemetry attempt is admitted
    # and the consecutive run resets to zero.
    assert gate.try_acquire_telemetry_reason() is None
    snapshot = gate.snapshot()
    assert snapshot["consecutive_telemetry_skips"] == 0
    assert snapshot["max_consecutive_telemetry_skips"] == 5
    gate.release_telemetry()


def test_one_critical_command_cannot_starve_the_normal_lane_after_release():
    """A single critical event must not deny service indefinitely."""

    gate = _CriticalFirstWriteGate()
    gate.register_critical()
    gate.acquire_critical()
    assert gate.try_acquire_telemetry_reason() == "critical_persistence_pending"
    gate.release_critical()

    # Priority state is released with the work, not left asserted.
    assert gate.critical_pending() is False
    for _ in range(20):
        assert gate.try_acquire_telemetry_reason() is None
        gate.release_telemetry()
    assert gate.snapshot()["consecutive_telemetry_skips"] == 0


def test_a_cancelled_critical_command_does_not_leave_stale_priority():
    gate = _CriticalFirstWriteGate()
    gate.register_critical()
    assert gate.critical_pending() is True
    gate.cancel_critical()
    assert gate.critical_pending() is False
    assert gate.try_acquire_telemetry_reason() is None
    gate.release_telemetry()


def test_worker_failure_resets_priority_rather_than_pinning_it():
    gate = _CriticalFirstWriteGate()
    for _ in range(4):
        gate.register_critical()
    assert gate.critical_pending() is True
    gate.reset_critical()
    assert gate.critical_pending() is False
    assert gate.try_acquire_telemetry_reason() is None
    gate.release_telemetry()


def test_admission_gap_is_observable_and_bounded():
    gate = _CriticalFirstWriteGate()
    assert gate.try_acquire_telemetry_reason() is None
    gate.release_telemetry()
    snapshot = gate.snapshot()
    assert snapshot["seconds_since_telemetry_admit"] >= 0.0
    assert snapshot["max_telemetry_admit_gap_s"] >= 0.0
    # Bounded set of counters -- an observer can never be handed an unbounded
    # per-event log by this snapshot.
    assert set(snapshot) == {
        "critical_pending", "critical_inflight", "telemetry_inflight",
        "maintenance_waiting", "maintenance_inflight",
        "telemetry_priority_skips", "telemetry_maintenance_deferrals",
        "maintenance_deferrals", "consecutive_telemetry_skips",
        "max_consecutive_telemetry_skips", "seconds_since_telemetry_admit",
        "max_telemetry_admit_gap_s",
    }


# ---------------------------------------------------------------------------
# 5. End-to-end against a real SQLite sink.
# ---------------------------------------------------------------------------


def _event_count_call(index: int) -> dict:
    return {
        "method": "record_event_count",
        "kwargs": {
            "receipt_ts_ms": NOW + index, "source": "okx",
            "channel": "ticker", "asset": "BTC", "event_type": "ticker",
            "classification": "NEW_TICK", "unique": True,
            "duplicate": False, "invalid": False,
        },
    }


def test_critical_evidence_is_prioritised_and_telemetry_defers(tmp_path,
                                                               monkeypatch):
    """Critical work wins; telemetry yields without losing its rows."""

    path = tmp_path / "sink-capacity.db"
    entered = threading.Event()
    release = threading.Event()

    def block_writer(_store, _rows):
        entered.set()
        assert release.wait(5.0)
        return None

    monkeypatch.setattr(V4Store, "record_event_count_batch", block_writer)
    writer = V4PersistenceWriter(path, sample_interval_s=60.0)
    try:
        critical = writer.submit(V4PersistenceCommand(
            command_id="critical-holds-the-gate",
            method="record_event_count_batch",
            args=([],), ordering_key="critical",
        ))
        assert entered.wait(5.0)

        with pytest.raises(V4TelemetryPrioritySkip):
            writer.submit_telemetry_batch([_event_count_call(0)], timeout_s=1.0)

        gate = writer.write_gate_snapshot()
        assert gate["critical_inflight"] == 1
        assert gate["consecutive_telemetry_skips"] >= 1

        release.set()
        critical.result(timeout=5.0)

        # Once the critical command completes, priority is genuinely released
        # and the normal lane is served on its next attempt.
        for _ in range(50):
            if not writer.critical_write_pending():
                break
            time.sleep(0.02)
        assert writer.critical_write_pending() is False
        writer.submit_telemetry_batch([_event_count_call(1)], timeout_s=5.0)
        assert writer.write_gate_snapshot()["consecutive_telemetry_skips"] == 0
    finally:
        release.set()
        writer.close_telemetry_sink()
        writer.close()


def test_sink_reports_a_begin_commit_and_per_method_cost_split(tmp_path):
    path = tmp_path / "sink-cost.db"
    writer = V4PersistenceWriter(path, sample_interval_s=60.0)
    try:
        writer.submit_telemetry_batch(
            [_event_count_call(index) for index in range(8)], timeout_s=10.0)
        metrics = writer.telemetry_commit_metrics()

        assert metrics["last_begin_duration_ms"] >= 0.0
        assert metrics["last_commit_duration_ms"] >= 0.0
        # The two halves of fixed overhead cannot together exceed the whole
        # transaction they were measured inside.
        assert (metrics["last_begin_duration_ms"]
                + metrics["last_commit_duration_ms"]
                <= metrics["last_duration_ms"] + 1e-6)

        by_method = metrics["row_cost_by_method"]
        assert set(by_method) == {"record_event_count"}
        assert by_method["record_event_count"]["calls"] == 8
        assert by_method["record_event_count"]["total_ms"] >= 0.0
        # Bounded by the telemetry allowlist, never by the row count.
        assert len(by_method) <= 16
    finally:
        writer.close_telemetry_sink()
        writer.close()


def test_per_method_cost_snapshot_is_a_copy(tmp_path):
    """An observer must not be handed the live map the sink is mutating."""

    path = tmp_path / "sink-copy.db"
    writer = V4PersistenceWriter(path, sample_interval_s=60.0)
    try:
        writer.submit_telemetry_batch([_event_count_call(0)], timeout_s=10.0)
        first = writer.telemetry_commit_metrics()["row_cost_by_method"]
        first["record_event_count"]["calls"] = -1
        second = writer.telemetry_commit_metrics()["row_cost_by_method"]
        assert second["record_event_count"]["calls"] == 1
    finally:
        writer.close_telemetry_sink()
        writer.close()
