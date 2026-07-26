from __future__ import annotations

import math

import threading
import time

from poly_alpha_sniper.lite_frequency_v4.config import ACTIVE_COHORT
from poly_alpha_sniper.lite_frequency_v4.engine import (
    DASHBOARD_EXPORT_INTERVAL_MS,
)
from poly_alpha_sniper.lite_frequency_v4.persistence import (
    V4PersistenceCommand,
    V4PersistenceWriter,
    V4TelemetryMaintenanceDeferral,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store
from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    TelemetryCommand,
    TelemetryDisposition,
    TelemetryOverloadPolicy,
    V4TelemetryWriter,
    _AdaptiveTelemetryController,
    _UP_HEADROOM_WINDOWS,
    deadline_safe_capacity,
    required_rows_per_dispatch,
)


class _MemorySink:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def submit_telemetry_batch(self, commands, *, timeout_s):
        del timeout_s
        self.rows.extend(commands)
        return len(commands)


def _controller(*, maximum: int = 32) -> _AdaptiveTelemetryController:
    return _AdaptiveTelemetryController(
        physical_max_chunk=maximum,
        queue_capacity=1_000,
        flush_interval_s=0.25,
        budgeted_sink=True,
        started_monotonic=0.0,
    )


def test_required_throughput_floor_calculation_includes_drain_margin():
    assert required_rows_per_dispatch(
        admitted_rows_per_second=120.0,
        sustainable_dispatches_per_second=20.0,
        queue_depth=40,
        queue_target=20,
        drain_horizon_s=10.0,
        safety_margin=1.20,
    ) == 8


def test_deadline_safe_capacity_uses_tail_cost_and_physical_bound():
    assert deadline_safe_capacity(
        transaction_budget_ms=250.0,
        tail_ms_per_row=25.0,
        physical_max_chunk=32,
    ) == 8
    assert deadline_safe_capacity(
        transaction_budget_ms=250.0,
        fixed_overhead_ms=55.0,
        tail_ms_per_row=1.0,
        physical_max_chunk=32,
    ) == 32
    assert deadline_safe_capacity(
        transaction_budget_ms=None,
        tail_ms_per_row=None,
        physical_max_chunk=32,
    ) == 32


def test_chunk_selection_reaches_floor_below_deadline_safe_capacity():
    controller = _controller()
    decision = None
    for tick in range(1, 41):
        controller.add(tick, queue_depth=20, offered=8, admitted=8)
        rows = controller.selected_chunk
        controller.observe_commit(
            now=tick,
            rows=rows,
            logical_rows=rows,
            transaction_ms=5.0 * rows,
            total_ms=5.0 * rows,
            queue_depth=20,
        )
        decision = controller.decide(
            now=tick, queue_depth=20, transaction_budget_ms=250.0)
    assert decision is not None
    assert decision.throughput_required_chunk <= decision.deadline_safe_chunk
    assert decision.selected_chunk >= decision.throughput_required_chunk


def test_overload_when_required_throughput_exceeds_physical_capacity():
    controller = _controller(maximum=4)
    decision = None
    for tick in range(1, 4):
        controller.add(tick, queue_depth=10, offered=40, admitted=40)
        controller.observe_commit(
            now=tick,
            rows=1,
            logical_rows=1,
            transaction_ms=50.0,
            total_ms=50.0,
            queue_depth=10,
        )
        decision = controller.decide(
            now=tick, queue_depth=10, transaction_budget_ms=100.0)
    assert decision is not None
    assert decision.throughput_required_chunk > decision.deadline_safe_chunk
    assert decision.overload_active is True
    assert decision.overload_reason == (
        "throughput_exceeds_deadline_safe_capacity")


def test_noncritical_latest_coalesces_before_admission_overflow():
    writer = V4TelemetryWriter(
        _MemorySink(), capacity=256, batch_size=32,
        physical_batch_size=32, flush_interval_s=0.25)
    for index in range(128):
        assert writer.submit(TelemetryCommand(
            "record", (index,), {"value": index})) is (
                TelemetryDisposition.ACCEPTED)
    first = writer.submit(
        TelemetryCommand("record", ("latest",), {"value": 1}),
        overload_policy=TelemetryOverloadPolicy.LATEST,
        overload_key="latest-state",
    )
    second = writer.submit(
        TelemetryCommand("record", ("latest",), {"value": 2}),
        overload_policy=TelemetryOverloadPolicy.LATEST,
        overload_key="latest-state",
    )
    snapshot = writer.snapshot()
    assert first is TelemetryDisposition.ACCEPTED
    assert second is TelemetryDisposition.COALESCED
    assert snapshot["preoverflow_coalesced"] == 1
    assert snapshot["admission_overflow_rows"] == 0
    assert snapshot["rows_dropped"] == 0
    assert writer.stop(drain=False, timeout_s=2.0)


def test_latest_replacement_updates_state_cache_without_fake_logical_writes():
    sink = _MemorySink()
    writer = V4TelemetryWriter(
        sink, capacity=256, batch_size=32,
        physical_batch_size=32, flush_interval_s=0.25)
    for index in range(128):
        writer.submit(TelemetryCommand("record", (index,), {}))
    assert writer.submit(
        TelemetryCommand("record", ("latest",), {"value": 1}),
        state_key="latest-state",
        state_value={"value": 1},
        overload_policy=TelemetryOverloadPolicy.LATEST,
    ) is TelemetryDisposition.ACCEPTED
    assert writer.submit(
        TelemetryCommand("record", ("latest",), {"value": 2}),
        state_key="latest-state",
        state_value={"value": 2},
        overload_policy=TelemetryOverloadPolicy.LATEST,
    ) is TelemetryDisposition.COALESCED
    # The replacement refreshed state metadata, so the same latest value is an
    # ordinary semantic coalesce rather than another stale replacement.
    assert writer.submit(
        TelemetryCommand("record", ("latest",), {"value": 2}),
        state_key="latest-state",
        state_value={"value": 2},
        overload_policy=TelemetryOverloadPolicy.LATEST,
    ) is TelemetryDisposition.COALESCED
    writer.start()
    assert writer.stop(drain=True, timeout_s=5.0)
    snapshot = writer.snapshot()
    assert snapshot["rows_written"] == 129
    assert snapshot["logical_written"] == 129
    assert snapshot["preoverflow_coalesced"] == 1
    latest = [
        row for row in sink.rows
        if tuple(row.get("args") or ()) == ("latest",)
    ]
    assert len(latest) == 1
    assert latest[0]["kwargs"]["value"] == 2


def test_noncritical_sampling_is_explicit_and_counted_before_overflow():
    writer = V4TelemetryWriter(
        _MemorySink(), capacity=256, batch_size=32,
        physical_batch_size=32, flush_interval_s=0.25)
    for index in range(128):
        writer.submit(TelemetryCommand("record", (index,), {}))
    dispositions = [
        writer.submit(
            TelemetryCommand("record", ("sample", index), {}),
            overload_policy=TelemetryOverloadPolicy.SAMPLE,
            overload_key=("sample", index),
        )
        for index in range(128)
    ]
    snapshot = writer.snapshot()
    assert TelemetryDisposition.SAMPLED in dispositions
    assert snapshot["rows_sampled"] > 0
    assert snapshot["admission_overflow_rows"] == 0
    assert snapshot["rows_dropped"] == 0
    assert writer.stop(drain=False, timeout_s=2.0)


def test_queue_hysteresis_holds_then_clears_overload_after_pressure():
    controller = _controller(maximum=4)
    for tick in range(1, 4):
        controller.add(tick, queue_depth=10, offered=40, admitted=40)
        controller.observe_commit(
            now=tick, rows=1, logical_rows=1,
            transaction_ms=50.0, total_ms=50.0, queue_depth=10)
        controller.decide(
            now=tick, queue_depth=10, transaction_budget_ms=100.0)
    assert controller.decide(
        now=20.0, queue_depth=0,
        transaction_budget_ms=100.0).overload_active is True
    recovered = None
    for tick in range(21, 27):
        recovered = controller.decide(
            now=float(tick), queue_depth=0, transaction_budget_ms=100.0)
    assert recovered is not None
    assert recovered.overload_active is False


def test_controller_recovers_upward_after_deadline_pressure_declines():
    controller = _controller()
    controller.observe_deadline_miss(
        now=0.0, failed_rows=4, transaction_budget_ms=250.0,
        queue_depth=0)
    controller.observe_deadline_miss(
        now=0.1, failed_rows=2, transaction_budget_ms=250.0,
        queue_depth=0)
    assert controller.selected_chunk == 1
    decision = None
    for tick in range(21, 45):
        rows = controller.selected_chunk
        controller.observe_commit(
            now=float(tick), rows=rows, logical_rows=rows,
            transaction_ms=55.0 + rows, total_ms=55.0 + rows,
            queue_depth=0)
        decision = controller.decide(
            now=float(tick), queue_depth=0, transaction_budget_ms=250.0)
    assert decision is not None
    assert decision.selected_chunk > 1
    assert decision.deadline_safe_chunk > 1
    assert decision.transaction_fixed_overhead_ms >= 50.0


def test_sustained_recent_cost_replaces_obsolete_slow_transaction_regime():
    controller = _controller()
    initial = controller.selected_chunk
    for index in range(32):
        now = 1.0 + index * 0.001
        controller.observe_commit(
            now=now, rows=initial, logical_rows=initial,
            transaction_ms=80.0, total_ms=80.0, queue_depth=32)
        controller.decide(
            now=now, queue_depth=32, transaction_budget_ms=200.0)
    slow = controller.decide(
        now=2.0, queue_depth=32, transaction_budget_ms=200.0)

    decision = slow
    for index in range(320):
        now = 3.0 + index * 0.01
        rows = controller.selected_chunk
        controller.observe_commit(
            now=now, rows=rows, logical_rows=rows,
            transaction_ms=max(0.1, rows * 0.05),
            total_ms=max(0.1, rows * 0.05), queue_depth=32)
        decision = controller.decide(
            now=now, queue_depth=32, transaction_budget_ms=200.0)

    assert decision.transaction_duration_p95_ms < (
        slow.transaction_duration_p95_ms)
    assert decision.deadline_safe_chunk > slow.deadline_safe_chunk
    assert decision.selected_chunk > initial


def test_fixed_overhead_above_naive_one_row_floor_recovers_from_chunk_one():
    controller = _controller()
    controller.observe_deadline_miss(
        now=0.0, failed_rows=4, transaction_budget_ms=250.0,
        queue_depth=0)
    controller.observe_deadline_miss(
        now=0.1, failed_rows=2, transaction_budget_ms=250.0,
        queue_depth=0)
    assert controller.selected_chunk == 1
    decision = None
    for tick in range(21, 46):
        rows = controller.selected_chunk
        controller.observe_commit(
            now=float(tick), rows=rows, logical_rows=rows,
            transaction_ms=130.0 + rows,
            total_ms=130.0 + rows,
            fixed_overhead_ms=130.0,
            marginal_ms_per_row=1.0,
            queue_depth=0)
        decision = controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0)
    assert decision is not None
    assert decision.transaction_fixed_overhead_ms >= 130.0
    assert decision.deadline_safe_chunk > 1
    assert decision.selected_chunk > 1


def test_sparse_idle_window_cannot_reuse_one_success_for_multiple_growth_steps():
    controller = _controller()
    initial = controller.selected_chunk
    controller.observe_commit(
        now=1.0, rows=initial, logical_rows=initial,
        transaction_ms=10.0, total_ms=10.0, queue_depth=0)
    for tick in range(1, 21):
        controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0)
    assert controller.selected_chunk == initial


def test_burst_successes_cannot_accelerate_upward_hysteresis():
    controller = _controller()
    initial = controller.selected_chunk
    for _ in range(100):
        controller.observe_commit(
            now=1.0, rows=initial, logical_rows=initial,
            transaction_ms=10.0, total_ms=10.0, queue_depth=4)
        controller.decide(
            now=1.0, queue_depth=4, transaction_budget_ms=250.0)
    assert controller.selected_chunk == initial
    for tick in range(2, 7):
        controller.observe_commit(
            now=float(tick), rows=initial, logical_rows=initial,
            transaction_ms=10.0, total_ms=10.0, queue_depth=4)
        controller.decide(
            now=float(tick), queue_depth=4,
            transaction_budget_ms=250.0)
    assert controller.selected_chunk > initial


def test_completed_burst_cannot_mature_into_growth_while_idle():
    controller = _controller()
    initial = controller.selected_chunk
    for _ in range(100):
        controller.observe_commit(
            now=1.0, rows=initial, logical_rows=initial,
            transaction_ms=10.0, total_ms=10.0, queue_depth=4)
    controller.decide(
        now=1.0, queue_depth=4, transaction_budget_ms=250.0)
    for tick in range(2, 12):
        controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0)
    assert controller.selected_chunk == initial


def test_widely_spaced_successes_are_not_sustained_headroom():
    controller = _controller()
    initial = controller.selected_chunk
    for now in (1.0, 100.0, 200.0, 300.0, 400.0):
        controller.observe_commit(
            now=now, rows=initial, logical_rows=initial,
            transaction_ms=10.0, total_ms=10.0, queue_depth=4)
        controller.decide(
            now=now, queue_depth=4, transaction_budget_ms=250.0)
    assert controller.selected_chunk == initial


def _busy_windows(controller, *, start_tick: int, count: int,
                  step_s: float = 0.25, queue_depth: int = 8,
                  transaction_ms: float = 10.0,
                  budget_ms: float = 250.0):
    """Drive ``count`` consecutive busy control windows and return the last."""

    decision = None
    for index in range(count):
        now = start_tick * step_s + index * step_s
        rows = controller.selected_chunk
        controller.observe_commit(
            now=now, rows=rows, logical_rows=rows,
            transaction_ms=transaction_ms, total_ms=transaction_ms,
            queue_depth=queue_depth)
        decision = controller.decide(
            now=now, queue_depth=queue_depth,
            transaction_budget_ms=budget_ms)
    return decision


def test_sustained_backlog_earns_exactly_one_bounded_probe_per_interval():
    """Sustained load earns a probe, and only one until it is re-earned.

    The upward step is bounded (a fraction of the current size, never a jump
    to the deadline-safe ceiling), and a single earned probe must not keep
    paying out on subsequent windows.
    """

    controller = _controller()
    initial = controller.selected_chunk

    _busy_windows(controller, start_tick=4, count=_UP_HEADROOM_WINDOWS)
    after_first = controller.selected_chunk
    assert after_first > initial, "sustained backlog must earn one probe"
    # Bounded: a fraction of the previous size, not a jump to the ceiling.
    assert after_first <= initial + max(
        1, math.ceil(initial * 0.25))
    assert after_first < controller.deadline_safe_chunk

    # The very next window cannot pay out again: the streak was consumed.
    _busy_windows(controller, start_tick=4 + _UP_HEADROOM_WINDOWS, count=1)
    assert controller.selected_chunk == after_first

    # Re-earning a full run of windows grants exactly one more step.
    _busy_windows(
        controller, start_tick=4 + _UP_HEADROOM_WINDOWS + 1,
        count=_UP_HEADROOM_WINDOWS)
    after_second = controller.selected_chunk
    assert after_second > after_first
    assert after_second <= after_first + max(
        1, math.ceil(after_first * 0.25))


def test_growth_requires_proven_success_at_the_currently_probed_size():
    """A window with no commit at the probed size cannot count as headroom.

    After a probe raises the size, evidence must be produced *at that size*.
    Windows that only decide, without committing, must not mature the streak.
    """

    controller = _controller()
    _busy_windows(controller, start_tick=4, count=_UP_HEADROOM_WINDOWS)
    probed = controller.selected_chunk

    # Plenty of control windows, but no commit lands in any of them.
    for index in range(4 * _UP_HEADROOM_WINDOWS):
        now = 20.0 + index * 0.25
        controller.decide(
            now=now, queue_depth=8, transaction_budget_ms=250.0)
    assert controller.selected_chunk == probed

    # Commits that are smaller than the probed size are not proof either:
    # they never demonstrate the probed size is affordable.
    for index in range(4 * _UP_HEADROOM_WINDOWS):
        now = 40.0 + index * 0.25
        controller.observe_commit(
            now=now, rows=max(1, probed - 1), logical_rows=max(1, probed - 1),
            transaction_ms=10.0, total_ms=10.0, queue_depth=8)
        controller.decide(
            now=now, queue_depth=8, transaction_budget_ms=250.0)
    assert controller.selected_chunk == probed


def test_failed_probe_causes_a_bounded_decrease_not_a_collapse():
    """A deadline miss halves the size; it must not slam straight to one."""

    controller = _controller()
    _busy_windows(controller, start_tick=4, count=_UP_HEADROOM_WINDOWS * 6)
    grown = controller.selected_chunk
    assert grown >= 4, "need a grown size to observe a bounded decrease"

    controller.observe_deadline_miss(
        now=100.0, failed_rows=grown, transaction_budget_ms=250.0,
        queue_depth=8)
    after = controller.selected_chunk
    assert after == max(1, grown // 2)
    assert after > 1
    # The cap is a bound, not a permanent floor: it also bounds the safe size.
    assert controller.deadline_safe_chunk <= max(1, grown // 2)


def test_dispatches_slower_than_the_control_interval_still_earn_growth():
    """A busy sink must not be pinned merely for being slower than one tick.

    The headroom gap allowance is expressed in control ticks so that a sink
    dispatching well below the control cadence still counts as sustained load.
    Without that, any sink slower than one control interval could never earn a
    probe and a reduced chunk size would become permanent.
    """

    controller = _controller()
    initial = controller.selected_chunk
    # 0.75 s dispatches: three times the 0.25 s control interval, still inside
    # the one-second sustained-cadence allowance.
    for index in range(_UP_HEADROOM_WINDOWS + 1):
        now = 1.0 + index * 0.75
        rows = controller.selected_chunk
        controller.observe_commit(
            now=now, rows=rows, logical_rows=rows,
            transaction_ms=20.0, total_ms=20.0, queue_depth=64)
        controller.decide(
            now=now, queue_depth=64, transaction_budget_ms=250.0)
    assert controller.selected_chunk > initial


def test_headroom_gap_allowance_is_expressed_in_control_ticks():
    """The allowance must scale with the configured control cadence."""

    fast = _AdaptiveTelemetryController(
        physical_max_chunk=32, queue_capacity=1_000,
        flush_interval_s=0.25, budgeted_sink=True, started_monotonic=0.0)
    slow = _AdaptiveTelemetryController(
        physical_max_chunk=32, queue_capacity=1_000,
        flush_interval_s=1.0, budgeted_sink=True, started_monotonic=0.0)
    assert fast._up_max_headroom_gap_ticks == 4
    assert slow._up_max_headroom_gap_ticks == 1


def test_throughput_floor_is_applied_not_merely_reported():
    """The computed floor must actually raise the selected chunk.

    A floor that is calculated, published and validated against but never
    enforced leaves the controller selecting below its own requirement: the
    queue then grows under sustained load and current health can never
    certify, because controller_safe requires selected >= required.
    """

    controller = _controller()
    bootstrap = controller.selected_chunk
    decision = None
    # Sustained admitted load above what the bootstrap chunk can commit, but
    # still within reach of a cheap sink's deadline-safe capacity.
    for index in range(40):
        now = 1.0 + index * 0.25
        rows = controller.selected_chunk
        controller.add(now, queue_depth=20, incoming=6, offered=6, admitted=6)
        controller.observe_commit(
            now=now, rows=rows, logical_rows=rows,
            transaction_ms=2.0, total_ms=2.0, queue_depth=20)
        decision = controller.decide(
            now=now, queue_depth=20, transaction_budget_ms=250.0)

    assert decision is not None
    # The requirement is real: it exceeds the conservative bootstrap size and
    # is genuinely meetable within the deadline budget.
    assert decision.throughput_required_chunk > bootstrap
    assert decision.throughput_required_chunk <= decision.deadline_safe_chunk
    # The floor is honoured ...
    assert decision.selected_chunk >= decision.throughput_required_chunk
    # ... and never at the cost of deadline safety.
    assert decision.selected_chunk <= decision.deadline_safe_chunk


def test_throughput_floor_never_exceeds_deadline_safe_capacity():
    """An unmeetable floor must clamp, not push past the deadline budget."""

    controller = _controller(maximum=4)
    decision = None
    for index in range(20):
        now = 1.0 + index * 0.25
        controller.add(now, queue_depth=500, incoming=400, offered=400,
                       admitted=400)
        controller.observe_commit(
            now=now, rows=1, logical_rows=1,
            transaction_ms=90.0, total_ms=90.0, queue_depth=500)
        decision = controller.decide(
            now=now, queue_depth=500, transaction_budget_ms=100.0)

    assert decision is not None
    # Demand exceeds what the budget allows, so overload is declared rather
    # than the chunk being pushed past its deadline-safe bound.
    assert decision.throughput_required_chunk > decision.deadline_safe_chunk
    assert decision.selected_chunk <= decision.deadline_safe_chunk
    assert decision.overload_active is True


def test_offered_load_remains_visible_while_admission_is_sampled():
    controller = _controller(maximum=4)
    decision = None
    for tick in range(1, 12):
        controller.add(
            float(tick), queue_depth=10,
            incoming=100, offered=100, admitted=20, sampled=80,
            overload_handled=80)
        controller.observe_commit(
            now=float(tick), rows=1, logical_rows=1,
            transaction_ms=50.0, total_ms=50.0, queue_depth=10)
        decision = controller.decide(
            now=float(tick), queue_depth=10,
            transaction_budget_ms=100.0)
    assert decision is not None
    assert decision.offered_required_chunk > decision.deadline_safe_chunk
    assert decision.overload_active is True
    assert decision.sampling_keep_ratio < 1.0


def test_explicitly_controlled_overload_can_recover_current_health():
    controller = _controller(maximum=4)
    decision = None
    for tick in range(1, 40):
        controller.add(
            float(tick), queue_depth=0,
            incoming=200, offered=200, admitted=1, sampled=199,
            overload_handled=199)
        controller.observe_commit(
            now=float(tick), rows=1, logical_rows=1,
            transaction_ms=20.0, total_ms=20.0, queue_depth=0)
        decision = controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=100.0)
    assert decision is not None
    assert decision.overload_active is True
    assert decision.controlled_overload is True
    assert decision.current_operational_healthy is True


def test_positive_queue_growth_never_certifies_current_health():
    controller = _controller()
    decision = None
    for tick in range(1, 46):
        depth = tick
        controller.add(
            float(tick), queue_depth=depth,
            offered=20, admitted=20)
        controller.observe_commit(
            now=float(tick), rows=19, logical_rows=19,
            transaction_ms=20.0, total_ms=20.0, queue_depth=depth)
        decision = controller.decide(
            now=float(tick), queue_depth=depth,
            transaction_budget_ms=250.0)
    assert decision is not None
    assert decision.view.committed < decision.view.admitted
    assert decision.view.queue_slope_rps > 0
    assert decision.current_operational_healthy is False


def test_stable_headroom_does_not_oscillate_between_minimum_and_maximum():
    controller = _controller()
    selected: list[int] = []
    for tick in range(1, 41):
        controller.add(tick, queue_depth=4, offered=4, admitted=4)
        controller.observe_commit(
            now=tick, rows=4, logical_rows=4,
            transaction_ms=8.0, total_ms=8.0, queue_depth=4)
        selected.append(controller.decide(
            now=tick, queue_depth=4,
            transaction_budget_ms=250.0).selected_chunk)
    assert selected == sorted(selected)
    assert selected[-1] > selected[0]


def test_checkpoint_deferral_requeues_without_failure_or_loss():
    class CheckpointDeferral(RuntimeError):
        telemetry_priority_skip = True
        telemetry_checkpoint_deferral = True

    class DeferringSink(_MemorySink):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def submit_telemetry_batch(self, commands, *, timeout_s):
            self.calls += 1
            if self.calls == 1:
                raise CheckpointDeferral("checkpoint pending")
            return super().submit_telemetry_batch(
                commands, timeout_s=timeout_s)

    writer = V4TelemetryWriter(
        DeferringSink(), capacity=32, batch_size=8,
        physical_batch_size=8, flush_interval_s=0.01)
    writer.start()
    writer.submit(TelemetryCommand("record", (1,), {}))
    assert writer.flush(timeout_s=3.0)
    snapshot = writer.snapshot()
    assert snapshot["checkpoint_deferral_batches"] == 1
    assert snapshot["failed_batches"] == 0
    assert snapshot["rows_dropped"] == 0
    assert snapshot["rows_written"] == 1
    assert writer.stop(drain=True, timeout_s=2.0)


def test_draining_stop_retries_cooperative_deferral_without_false_loss():
    class PriorityDeferral(RuntimeError):
        telemetry_priority_skip = True

    class DeferringSink(_MemorySink):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def submit_telemetry_batch(self, commands, *, timeout_s):
            self.calls += 1
            if self.calls <= 3:
                raise PriorityDeferral("critical pending")
            return super().submit_telemetry_batch(
                commands, timeout_s=timeout_s)

    writer = V4TelemetryWriter(
        DeferringSink(), capacity=32, batch_size=8,
        physical_batch_size=8, flush_interval_s=0.01)
    writer.start()
    writer.submit(TelemetryCommand("record", (1,), {}))
    assert writer.stop(drain=True, timeout_s=3.0)
    snapshot = writer.snapshot()
    assert snapshot["rows_written"] == 1
    assert snapshot["rows_dropped"] == 0
    assert snapshot["drain_stop_failed"] is False


def test_draining_stop_reports_false_if_deferral_retry_budget_is_exhausted():
    class PriorityDeferral(RuntimeError):
        telemetry_priority_skip = True

    class AlwaysDeferringSink(_MemorySink):
        def submit_telemetry_batch(self, commands, *, timeout_s):
            del commands, timeout_s
            raise PriorityDeferral("critical never clears")

    writer = V4TelemetryWriter(
        AlwaysDeferringSink(), capacity=32, batch_size=8,
        physical_batch_size=8, flush_interval_s=0.01)
    writer.submit(TelemetryCommand("record", (1,), {}))
    # Exercise the terminal retry boundary without sleeping through the full
    # production roughly eight-second cooperative-deferral allowance.
    with writer._condition:
        pending = next(iter(writer._pending.values()))
        pending.requeue_attempts = 32
    writer.start()
    assert writer.stop(drain=True, timeout_s=3.0) is False
    snapshot = writer.snapshot()
    assert snapshot["health"] == "STOPPED"
    assert snapshot["rows_dropped"] == 1
    assert snapshot["drain_stop_failed"] is True


def test_shared_gate_labels_maintenance_deferral_without_sink_failure(tmp_path):
    persistence = V4PersistenceWriter(
        tmp_path / "maintenance-deferral.db",
        sample_interval_s=60.0,
        checkpoint_on_close=False,
    )
    persistence.start()
    assert persistence.try_acquire_background_write() is True
    try:
        try:
            persistence.submit_telemetry_batch([{
                "method": "record_event_count_batch",
                "args": ([],),
            }], timeout_s=1.0)
        except V4TelemetryMaintenanceDeferral:
            pass
        else:
            raise AssertionError("telemetry did not yield to maintenance")
    finally:
        persistence.release_background_write()
    gate = persistence.metrics()
    sink = persistence.telemetry_commit_metrics()
    assert gate["telemetry_maintenance_deferrals"] == 1
    assert int(sink.get("failures") or 0) == 0
    persistence.close_telemetry_sink()
    persistence.close()


def test_critical_command_does_not_wait_behind_active_maintenance_gate(tmp_path):
    persistence = V4PersistenceWriter(
        tmp_path / "critical-preempts-maintenance.db",
        sample_interval_s=60.0,
        checkpoint_on_close=False,
    )
    persistence.start()
    assert persistence.try_acquire_background_write() is True
    try:
        critical = persistence.submit(V4PersistenceCommand(
            command_id="critical-during-maintenance",
            method="record_event_count_batch",
            args=([],),
            ordering_key="critical",
            terminal=True,
        ))
        assert critical.result(timeout=2.0) is None
    finally:
        persistence.release_background_write()
        persistence.close()


def test_current_health_recovers_without_erasing_lifetime_loss():
    controller = _controller()
    controller.add(0.0, queue_depth=0, lost=3)
    assert controller.decide(
        now=0.0, queue_depth=0,
        transaction_budget_ms=250.0).current_operational_healthy is False
    decision = None
    for tick in range(20, 36):
        decision = controller.decide(
            now=float(tick), queue_depth=0,
            transaction_budget_ms=250.0)
    assert decision is not None
    assert decision.view.lost == 0
    assert decision.current_operational_healthy is True


def test_rapid_policy_sampling_transitions_do_not_block_recovery():
    """Chunk changes during safe policy sampling must not reset the settle timer.

    Previously any ``selected_chunk != prior_selected`` re-armed the 20s settle,
    which made recovery unreachable whenever the controller legitimately adapted
    the physical chunk up/down while every safety invariant held.  Now only a
    genuine deadline-safe capacity shrink re-arms; healthy growth/floor-driven
    changes are tolerated.  This drives rapid chunk fluctuation with zero loss,
    zero failures, and a bounded queue, then asserts recovery still certifies.
    """
    controller = _controller(maximum=8)
    decision = None
    # Alternate the offered load every other tick so the throughput floor and
    # growth probes move the selected chunk around, but keep the queue bounded
    # and never introduce loss/failures/deadline-misses.
    for tick in range(1, 60):
        offered = 4 if tick % 2 == 0 else 8
        controller.add(
            float(tick), queue_depth=2, offered=offered, admitted=offered)
        controller.observe_commit(
            now=float(tick), rows=offered, logical_rows=offered,
            transaction_ms=8.0, total_ms=8.0, queue_depth=2)
        decision = controller.decide(
            now=float(tick), queue_depth=2, transaction_budget_ms=250.0)
    assert decision is not None
    assert decision.view.lost == 0
    assert decision.view.failed_batches == 0
    assert decision.view.deadline_failures == 0
    # Settle must elapse and recovery must certify despite the fluctuation.
    assert "settling" not in decision.recovery_blockers
    assert decision.current_operational_healthy is True


def test_overload_toggle_while_safe_does_not_reset_settle():
    """An overload toggle with all safety invariants healthy is recoverable.

    HEALTHY_WITH_POLICY_SAMPLING -- overload active while queues are bounded
    and loss/failures are zero -- is a valid state.  Resetting settle on every
    overload toggle made recovery impossible whenever load fluctuated around
    the capacity boundary.  Now only entering overload via the hard
    queue-capacity (queue_high_water) path re-arms settle.
    """
    controller = _controller(maximum=4)
    decision = None
    # Sustained offered load above deadline-safe capacity puts the lane into
    # controlled overload (policy sampling) with zero loss.
    for tick in range(1, 50):
        controller.add(
            float(tick), queue_depth=0,
            incoming=200, offered=200, admitted=1, sampled=199,
            overload_handled=199)
        controller.observe_commit(
            now=float(tick), rows=1, logical_rows=1,
            transaction_ms=20.0, total_ms=20.0, queue_depth=0)
        decision = controller.decide(
            now=float(tick), queue_depth=0, transaction_budget_ms=100.0)
    assert decision is not None
    assert decision.overload_active is True
    assert decision.controlled_overload is True
    assert decision.view.lost == 0
    # Once settle elapses the lane certifies even though overload is active:
    # this is HEALTHY_WITH_POLICY_SAMPLING, not a permanent latch.
    assert "settling" not in decision.recovery_blockers
    assert decision.current_operational_healthy is True


def test_high_water_overload_entry_still_re_arms_settle():
    """Entering overload via hard queue capacity remains a genuine setback.

    The transition-aware fix must not weaken safety: a queue_high_water entry
    is real capacity pressure and must still reset the settle timer so a fresh
    recovery window is required after it.
    """
    controller = _controller(maximum=4)
    # Drive the queue to the hard high-water mark to enter overload via
    # queue_high_water, with zero loss.
    for tick in range(1, 6):
        controller.add(
            float(tick), queue_depth=controller.high_water,
            offered=20, admitted=20)
        controller.observe_commit(
            now=float(tick), rows=4, logical_rows=4,
            transaction_ms=20.0, total_ms=20.0,
            queue_depth=controller.high_water)
        decision = controller.decide(
            now=float(tick), queue_depth=controller.high_water,
            transaction_budget_ms=100.0)
    assert decision is not None
    assert decision.overload_active is True
    assert decision.overload_reason == "queue_high_water"
    # A fresh high-water entry must re-arm the settle timer.
    assert "settling" in decision.recovery_blockers


def test_critical_command_acknowledges_during_sustained_telemetry(tmp_path):
    db_path = tmp_path / "critical-ack-under-telemetry.db"
    persistence = V4PersistenceWriter(
        db_path, sample_interval_s=60.0, checkpoint_on_close=False)
    persistence.start()
    telemetry = V4TelemetryWriter(
        persistence, capacity=1_024, batch_size=32,
        physical_batch_size=32, flush_interval_s=0.01)
    telemetry.start()
    for index in range(200):
        telemetry.submit(TelemetryCommand(
            "record_event_count",
            kwargs={
                "receipt_ts_ms": 1_800_000_000_000 + index,
                "source": "okx",
                "channel": "ticker",
                "asset": "BTC",
                "event_type": "ticker",
                "classification": "NEW_TICK",
                "unique": True,
                "duplicate": False,
                "invalid": False,
            },
        ))
    critical = persistence.submit(V4PersistenceCommand(
        command_id="critical-ack-under-load",
        method="record_event_count_batch",
        args=([],),
        ordering_key="critical",
    ))
    assert critical.result(timeout=5.0) is None
    assert persistence.metrics()["unconfirmed_command_count"] == 0
    assert telemetry.stop(drain=True, timeout_s=10.0)
    assert telemetry.snapshot()["true_lost_critical_rows"] == 0
    persistence.close()
    check = V4Store(db_path)
    try:
        assert check.query_one(
            "SELECT status FROM persistence_commands WHERE command_id=?",
            ("critical-ack-under-load",),
        ) == {"status": "COMMITTED"}
    finally:
        check.close()


def test_clean_writer_shutdown_leaves_no_owner_threads(tmp_path):
    persistence = V4PersistenceWriter(
        tmp_path / "clean-telemetry-stop.db",
        sample_interval_s=60.0,
        checkpoint_on_close=False,
    )
    persistence.start()
    telemetry = V4TelemetryWriter(
        persistence, capacity=32, batch_size=8,
        physical_batch_size=8, flush_interval_s=0.01)
    telemetry.start()
    telemetry.submit(TelemetryCommand(
        "record_event_count_batch", ([],), {}))
    assert telemetry.stop(drain=True, timeout_s=5.0)
    assert telemetry.snapshot()["health"] == "STOPPED"
    persistence.close()
    assert not any(
        thread.name == "v4-telemetry-writer" and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_export_freshness_contract_is_within_ten_second_reader_limit():
    # Two export opportunities fit inside the reader-facing <=10 second limit.
    assert DASHBOARD_EXPORT_INTERVAL_MS <= 5_000


def test_graceful_session_terminal_leaves_zero_open_sessions(tmp_path):
    store = V4Store(tmp_path / "graceful-session.db")
    try:
        started = 1_800_000_000_000
        store.ensure_cohort({
            "cohort": ACTIVE_COHORT,
            "activation_ts_ms": started,
            "activation_commit": "a" * 40,
            "starting_equity_usd": 130.0,
            "max_exposure_pct": 1.0,
            "authoritative": 1,
        })
        store.record_runtime_session({
            "session_id": "throughput-clean-stop",
            "launch_nonce": "throughput-clean-stop-nonce",
            "pid": 12345,
            "git_commit": "b" * 40,
            "config_hash": "c" * 64,
            "started_ts_ms": started,
            "cohort": ACTIVE_COHORT,
        })
        store.end_runtime_session(
            "throughput-clean-stop", started + 60_000, "graceful_stop")
        assert store.query_one(
            "SELECT COUNT(*) AS n FROM runtime_sessions "
            "WHERE ended_ts_ms IS NULL") == {"n": 0}
    finally:
        store.close()
