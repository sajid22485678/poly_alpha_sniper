"""Transaction cost attribution: contention is fixed cost, not per-row cost.

SQLite takes its write lock on the first write statement of a transaction, so a
cooperative yield to the critical writer, a checkpoint, or ordinary lock
contention is charged in full to whichever single row call happens to block.
Reading a p95 (or max) over the per-row calls of a small chunk therefore reads
that wait, and multiplying it by the row count predicts a transaction cost the
sink never demonstrated.

Measured on the 54.6-minute controlled gate at 44e58e2, over the nineteen queue
accumulation episodes -- every one preceded within 3 s by a cooperative priority
skip:

                            accumulating     clean
    dispatch_success_rps           9.27      10.73     same transaction rate
    selected_chunk                 3.25       6.54     half the rows
    logical_committed_rps         30.49      53.90     half the throughput
    offered_rps                  133.72     126.99     unchanged load
    tail_ms_per_row               54.16      38.29
    transaction p95_ms            84.12      69.33

    predicted transaction = 6.76 + 3.25 * 54.16 = 183 ms
    measured  transaction p95                   =  84 ms

The lane is transaction-rate limited, not row limited, so shrinking the chunk
could only deepen the backlog it was trying to drain.  These tests pin the
corrected attribution and, equally, pin that it does not hide anything: a sink
that is genuinely slow *per row* still shrinks the chunk, critical writes still
take priority, and a queue that genuinely accumulates still fails readiness
closed.
"""
from __future__ import annotations

import threading

import pytest

from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    TelemetryLossCategory,
    V4TelemetryWriter,
    _AdaptiveTelemetryController,
    _DEADLINE_BUDGET_FRACTION,
    _RECOVERY_SETTLE_S,
    deadline_safe_capacity,
)

TICK = 0.25
SETTLED = _RECOVERY_SETTLE_S + 5.0

#: The cooperative transaction budget the shadow sink publishes, in ms.
BUDGET_MS = 122.0


class _MetricSink:
    """Sink that publishes the same commit metrics the real writer does."""

    def __init__(self, *, transaction_ms: float, row_work_ms: float,
                 row_call_max_ms: float, fixed_overhead_ms: float) -> None:
        self.transaction_ms = float(transaction_ms)
        self.row_work_ms = float(row_work_ms)
        self.row_call_max_ms = float(row_call_max_ms)
        self.fixed_overhead_ms = float(fixed_overhead_ms)
        self.batches: list[list[dict]] = []
        self._lock = threading.Lock()

    def telemetry_transaction_budget_ms(self) -> float:
        return BUDGET_MS

    def telemetry_commit_metrics(self) -> dict[str, float]:
        return {
            "last_transaction_duration_ms": self.transaction_ms,
            "last_row_work_duration_ms": self.row_work_ms,
            "last_row_call_max_ms": self.row_call_max_ms,
            "last_row_call_p95_ms": self.row_call_max_ms,
            "last_transaction_fixed_overhead_ms": self.fixed_overhead_ms,
        }

    def submit_telemetry_batch(self, commands, *, timeout_s):
        _ = timeout_s
        rows = [dict(command) for command in commands]
        with self._lock:
            self.batches.append(rows)
        return len(rows)


def _writer(sink, **overrides) -> V4TelemetryWriter:
    values = {
        "capacity": 256,
        "batch_size": 32,
        "flush_interval_s": 0.01,
        "coalescing_interval_s": 60.0,
        "submit_timeout_s": 1.0,
        "heartbeat_interval_s": 0.01,
    }
    values.update(overrides)
    return V4TelemetryWriter(sink, **values)


def _controller(*, maximum: int = 32) -> _AdaptiveTelemetryController:
    return _AdaptiveTelemetryController(
        physical_max_chunk=maximum,
        queue_capacity=20_000,
        flush_interval_s=TICK,
        budgeted_sink=True,
        started_monotonic=0.0,
    )


# ---------------------------------------------------------------------------
# 1. A once-per-transaction wait is not multiplied by the row count
# ---------------------------------------------------------------------------


def test_contended_row_call_is_attributed_to_fixed_not_per_row():
    """The blocked call is one wait, not three rows that each cost 70 ms."""

    # A three-row transaction: two ordinary 3.5 ms row calls and one that
    # blocked for 70 ms acquiring the write lock behind the critical writer.
    sink = _MetricSink(transaction_ms=84.0, row_work_ms=77.0,
                       row_call_max_ms=70.0, fixed_overhead_ms=7.0)
    writer = _writer(sink)
    transaction_ms, fixed_ms, marginal_ms = (
        writer._transaction_cost_observation(
            {"rows_written": 3}, 84.0, rows=3))

    assert transaction_ms == pytest.approx(84.0)
    # Leave-one-out over the row calls: (77 - 70) / 2 = 3.5 ms per row.
    assert marginal_ms == pytest.approx(3.5, abs=0.01)
    # The 66.5 ms the blocked call carried over a typical one is real cost and
    # is kept -- as fixed overhead, which a larger chunk amortises.
    assert fixed_ms == pytest.approx(7.0 + (70.0 - 3.5), abs=0.01)
    # Nothing was invented: the affine model must not exceed what was measured.
    assert fixed_ms + marginal_ms * 3 <= transaction_ms + 1e-6


def test_attribution_keeps_the_affine_model_consistent_with_observation():
    """The old reading predicted 183 ms for a transaction that took 84 ms."""

    sink = _MetricSink(transaction_ms=84.0, row_work_ms=77.0,
                       row_call_max_ms=70.0, fixed_overhead_ms=6.76)
    writer = _writer(sink)
    _, fixed_ms, marginal_ms = writer._transaction_cost_observation(
        {"rows_written": 3}, 84.0, rows=3)
    corrected = fixed_ms + marginal_ms * 3
    # What the previous attribution produced from the same sample.
    naive = 6.76 + 70.0 * 3
    assert naive > 200.0
    assert corrected <= 84.0 + 1e-6
    assert corrected < naive / 2


def test_single_row_chunk_falls_back_conservatively():
    """One row cannot separate the wait from the work, so nothing is assumed."""

    sink = _MetricSink(transaction_ms=80.0, row_work_ms=74.0,
                       row_call_max_ms=74.0, fixed_overhead_ms=6.0)
    writer = _writer(sink)
    _, fixed_ms, marginal_ms = writer._transaction_cost_observation(
        {"rows_written": 1}, 80.0, rows=1)
    assert marginal_ms == pytest.approx(74.0)
    assert fixed_ms == pytest.approx(6.0)


# ---------------------------------------------------------------------------
# 2. Chunk collapse under priority starvation
# ---------------------------------------------------------------------------


def test_priority_starvation_no_longer_collapses_the_deadline_safe_chunk():
    """The gate's measured numbers, run through the capacity function."""

    usable = BUDGET_MS * _DEADLINE_BUDGET_FRACTION

    # What the lane did at 44e58e2: the whole contended transaction read as
    # per-row cost, so barely more than one row fits the budget.
    collapsed = deadline_safe_capacity(
        transaction_budget_ms=BUDGET_MS,
        tail_ms_per_row=54.16,
        physical_max_chunk=32,
        fixed_overhead_ms=6.76,
    )
    # With the wait attributed to fixed cost, the same measurement supports a
    # materially larger chunk -- and therefore materially more throughput.
    corrected = deadline_safe_capacity(
        transaction_budget_ms=BUDGET_MS,
        tail_ms_per_row=3.5,
        physical_max_chunk=32,
        fixed_overhead_ms=73.5,
    )
    assert collapsed <= 2
    assert corrected >= 3 * max(1, collapsed)
    # Still bounded, and still inside the cooperative budget.
    assert corrected <= 32
    assert 73.5 + corrected * 3.5 <= usable + 1e-6


def test_one_observed_chunk_size_nets_out_measured_fixed_overhead():
    """Sustained starvation collapses to a single size; that path must not
    charge the whole transaction to marginal cost."""

    controller = _controller()
    now = SETTLED
    # Every observation is the same contended three-row transaction.
    for _ in range(12):
        controller.observe_commit(
            now=now, rows=3, logical_rows=3,
            transaction_ms=84.0, total_ms=84.0, queue_depth=40,
            fixed_overhead_ms=73.5, marginal_ms_per_row=3.5,
        )
        now += TICK
    avg, p95, p99, fixed, tail, chunk_p95 = controller._transaction_stats(now)
    assert len(chunk_p95) == 1, "single observed size is the case under test"
    assert fixed == pytest.approx(73.5, abs=0.5)
    assert tail == pytest.approx(3.5, abs=0.5)
    # And the affine model still respects the observation.
    assert fixed + tail * 3 <= p95 + 1e-6


# ---------------------------------------------------------------------------
# 3. Critical-write priority is still protected
# ---------------------------------------------------------------------------


class _PrioritySkipSink(_MetricSink):
    """Yields to the critical writer for the first N dispatches, then commits."""

    def __init__(self, *, skips: int) -> None:
        super().__init__(transaction_ms=84.0, row_work_ms=77.0,
                         row_call_max_ms=70.0, fixed_overhead_ms=7.0)
        self.remaining_skips = int(skips)
        self.skipped = 0

    def submit_telemetry_batch(self, commands, *, timeout_s):
        with self._lock:
            if self.remaining_skips > 0:
                self.remaining_skips -= 1
                self.skipped += 1
                error = RuntimeError("critical writer holds the lane")
                setattr(error, "telemetry_priority_skip", True)
                raise error
        return super().submit_telemetry_batch(commands, timeout_s=timeout_s)


def test_critical_priority_still_yields_and_loses_nothing():
    sink = _PrioritySkipSink(skips=4)
    writer = _writer(sink, capacity=128, batch_size=16)
    writer.start()
    try:
        for index in range(48):
            writer.submit("record_book_snapshot", {"n": index})
        assert writer.flush(timeout_s=15.0)
    finally:
        assert writer.stop(drain=True, timeout_s=15.0)

    assert sink.skipped == 4, "the sink really did yield to critical writes"
    snapshot = writer.snapshot()
    assert snapshot["priority_skipped_batches"] >= 4
    # Yielding is designed behaviour, never loss.
    assert snapshot["noncritical_rows_unexpectedly_lost"] == 0
    assert writer.reconcile()["mismatch"] == 0
    written = sum(len(batch) for batch in sink.batches)
    assert written == 48, f"every row must still commit, got {written}"


# ---------------------------------------------------------------------------
# 4. Genuinely slow per-row persistence must still shrink the chunk
# ---------------------------------------------------------------------------


def test_uniformly_slow_row_calls_still_shrink_the_chunk():
    """No single outlier means no contention to reattribute: the sink really
    is slow per row, and the chunk must come down."""

    # Three row calls of ~30 ms each: uniformly slow, no blocked outlier.
    sink = _MetricSink(transaction_ms=95.0, row_work_ms=90.0,
                       row_call_max_ms=31.0, fixed_overhead_ms=5.0)
    writer = _writer(sink)
    _, fixed_ms, marginal_ms = writer._transaction_cost_observation(
        {"rows_written": 3}, 95.0, rows=3)
    # Leave-one-out still reports an expensive row, because every row is.
    assert marginal_ms == pytest.approx((90.0 - 31.0) / 2.0, abs=0.01)
    assert marginal_ms > 25.0
    # Almost nothing moves to fixed, so the chunk stays small.
    assert fixed_ms == pytest.approx(5.0 + max(0.0, 31.0 - marginal_ms),
                                     abs=0.01)
    safe = deadline_safe_capacity(
        transaction_budget_ms=BUDGET_MS,
        tail_ms_per_row=marginal_ms,
        physical_max_chunk=32,
        fixed_overhead_ms=fixed_ms,
    )
    assert safe <= 3, f"a genuinely slow sink must stay small, got {safe}"


def test_deadline_miss_floor_still_bounds_the_chunk():
    """A deadline miss is terminal evidence the chunk was too big."""

    controller = _controller()
    now = SETTLED
    controller.observe_commit(
        now=now, rows=8, logical_rows=8, transaction_ms=20.0, total_ms=20.0,
        queue_depth=0, fixed_overhead_ms=4.0, marginal_ms_per_row=2.0)
    before = controller.deadline_safe_chunk
    controller.observe_deadline_miss(
        now=now + TICK, failed_rows=8, transaction_budget_ms=BUDGET_MS,
        queue_depth=64)
    assert controller.deadline_safe_chunk <= max(1, before // 2)
    assert controller.selected_chunk <= controller.deadline_safe_chunk


# ---------------------------------------------------------------------------
# 5. Real accumulation still fails readiness closed
# ---------------------------------------------------------------------------


def test_genuine_queue_accumulation_still_blocks_readiness():
    """The corrected cost model must not make an unsafe queue look safe."""

    controller = _controller(maximum=4)
    now = SETTLED
    depth = 0
    decision = None
    for _ in range(40):
        depth += 8
        controller.add(now, queue_depth=depth, incoming=8, offered=8,
                       admitted=8)
        decision = controller.decide(
            now=now, queue_depth=depth, transaction_budget_ms=BUDGET_MS,
            queued_logical=depth, inflight_logical=0,
            oldest_age_s=depth / 8.0)
        now += TICK
    assert "queue_accumulating" in decision.recovery_blockers
    assert "uncontrolled_overload" in decision.recovery_blockers
    assert decision.recovery_healthy_windows == 0
    assert not decision.current_operational_healthy


def test_stale_residence_still_blocks_even_with_a_shallow_queue():
    """A small queue that never drains is still backlog."""

    controller = _controller()
    now = SETTLED
    decision = None
    for _ in range(20):
        controller.add(now, queue_depth=5, incoming=2, offered=2, admitted=2)
        controller.observe_commit(
            now=now, rows=2, logical_rows=2, transaction_ms=5.0, total_ms=5.0,
            queue_depth=5, fixed_overhead_ms=3.0, marginal_ms_per_row=1.0)
        decision = controller.decide(
            now=now, queue_depth=5, transaction_budget_ms=BUDGET_MS,
            queued_logical=5, inflight_logical=0,
            oldest_age_s=45.0)          # past _QUEUE_MAX_RESIDENCE_S
        now += TICK
    assert "queue_accumulating" in decision.recovery_blockers
    assert not decision.current_operational_healthy
