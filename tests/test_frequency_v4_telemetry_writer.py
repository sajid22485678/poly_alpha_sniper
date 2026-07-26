from __future__ import annotations

import ast
from concurrent.futures import Future
import inspect
from pathlib import Path
import threading
import time
from typing import Any

import pytest

from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    TelemetryCommand,
    TelemetryDisposition,
    V4TelemetryWriter,
    _MAX_PRIORITY_REQUEUE_ATTEMPTS,
    sum_kwargs,
)


class RecordingSink:
    """Thread-safe stand-in for the physical, priority-aware DB writer."""

    def __init__(self, *, delay_s: float = 0.0, fail: bool = False) -> None:
        self.delay_s = delay_s
        self.fail = fail
        self.batches: list[list[dict]] = []
        self.timeouts: list[float] = []
        self.thread_ids: list[int] = []
        self.critical_commits = 0
        self._lock = threading.Lock()

    def submit_telemetry_batch(self, commands, *, timeout_s):
        self.thread_ids.append(threading.get_ident())
        self.timeouts.append(timeout_s)
        if self.delay_s:
            time.sleep(self.delay_s)
        if self.fail:
            raise RuntimeError("telemetry sink unavailable")
        copied = [
            {
                "method": command["method"],
                "args": tuple(command["args"]),
                "kwargs": dict(command["kwargs"]),
            }
            for command in commands
        ]
        with self._lock:
            self.batches.append(copied)
        return {"ok": True, "rows_written": len(copied)}

    @property
    def commands(self):
        with self._lock:
            return [command for batch in self.batches for command in batch]


def _writer(sink, **overrides):
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


def test_worker_has_no_sqlite_connection_or_store_dependency():
    source = (Path(__file__).resolve().parent.parent / "lite_frequency_v4" /
              "telemetry.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_from = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    assert "sqlite3" not in imported
    assert "store" not in imported_from
    assert "lite_frequency_v4.store" not in imported_from


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"capacity": 0}, "capacity"),
        ({"capacity": 2, "batch_size": 3}, "batch size"),
        ({"flush_interval_s": 0}, "flush_interval"),
        ({"coalescing_interval_s": float("nan")}, "coalescing_interval"),
        ({"submit_timeout_s": False}, "submit_timeout"),
        ({"dedupe_capacity": 0}, "dedupe_capacity"),
    ],
)
def test_configuration_is_strict_and_bounded(overrides, message):
    with pytest.raises(ValueError, match=message):
        V4TelemetryWriter(RecordingSink(), **overrides)


def test_sink_contract_is_validated_before_worker_start():
    with pytest.raises(TypeError, match="submit_telemetry_batch"):
        V4TelemetryWriter(object())


def test_batching_runs_on_dedicated_thread_and_reports_exact_metrics():
    sink = RecordingSink()
    writer = _writer(sink, batch_size=4)
    main_thread = threading.get_ident()
    for index in range(10):
        assert writer.submit(
            "record_latency", kwargs={"sample": index},
        ) is TelemetryDisposition.ACCEPTED
    writer.start()
    assert writer.stop(drain=True, timeout_s=2.0)

    metrics = writer.snapshot()
    assert [len(batch) for batch in sink.batches] == [4, 4, 2]
    assert metrics["submitted"] == 10
    assert metrics["written"] == 10
    assert metrics["logical_written"] == 10
    assert metrics["coalesced"] == 0
    assert metrics["dropped"] == 0
    assert metrics["batches"] == 3
    assert metrics["batch_attempts"] == 3
    assert metrics["failed_batches"] == 0
    assert metrics["health"] == "STOPPED"
    assert sink.thread_ids and all(value != main_thread for value in sink.thread_ids)
    assert sink.timeouts == [1.0, 1.0, 1.0]


def test_admission_deep_snapshots_caller_retained_command_payload():
    sink = RecordingSink()
    writer = _writer(sink)
    nested = {"labels": ["original"]}
    command = TelemetryCommand(
        "record_runtime_health", kwargs={"evidence": nested})
    assert writer.submit(command) is TelemetryDisposition.ACCEPTED
    nested["labels"].append("source-mutated")
    command.kwargs["evidence"]["labels"].append("command-mutated")
    writer.start()
    assert writer.stop(drain=True, timeout_s=2.0)
    assert sink.commands[0]["kwargs"]["evidence"] == {"labels": ["original"]}


def test_repeated_state_is_coalesced_but_every_material_transition_is_kept():
    sink = RecordingSink()
    writer = _writer(sink)
    common = {"state_key": ("okx", "public")}

    assert writer.submit(
        "record_source_health", kwargs={"status": "READY"},
        state_value={"connected": True, "status": "READY"}, **common,
    ) is TelemetryDisposition.ACCEPTED
    assert writer.submit(
        "record_source_health", kwargs={"status": "READY", "age_ms": 1},
        state_value={"connected": True, "status": "READY"}, **common,
    ) is TelemetryDisposition.COALESCED
    assert writer.submit(
        "record_source_health", kwargs={"status": "DISCONNECTED"},
        state_value={"connected": False, "status": "DISCONNECTED"}, **common,
    ) is TelemetryDisposition.ACCEPTED
    assert writer.submit(
        "record_source_health", kwargs={"status": "READY"},
        state_value={"connected": True, "status": "READY"}, **common,
    ) is TelemetryDisposition.ACCEPTED

    writer.start()
    assert writer.stop(drain=True, timeout_s=2.0)
    assert [row["kwargs"]["status"] for row in sink.commands] == [
        "READY", "DISCONNECTED", "READY",
    ]
    metrics = writer.snapshot()
    assert metrics["submitted"] == 4
    assert metrics["coalesced"] == 1
    assert metrics["written"] == 3
    assert metrics["logical_written"] == 3


def test_deduplication_is_exact_and_does_not_consume_queue_capacity():
    sink = RecordingSink()
    writer = _writer(sink, capacity=2, batch_size=2)
    first = writer.submit(
        "record_event", kwargs={"event": 1}, dedupe_key="event-1")
    duplicate = writer.submit(
        "record_event", kwargs={"event": 1}, dedupe_key="event-1")
    second = writer.submit(
        "record_event", kwargs={"event": 2}, dedupe_key="event-2")
    assert (first, duplicate, second) == (
        TelemetryDisposition.ACCEPTED,
        TelemetryDisposition.COALESCED,
        TelemetryDisposition.ACCEPTED,
    )
    writer.start()
    assert writer.stop(drain=True, timeout_s=2.0)
    metrics = writer.snapshot()
    assert metrics["submitted"] == 3
    assert metrics["coalesced"] == 1
    assert metrics["deduplicated"] == 1
    assert metrics["written"] == 2
    assert metrics["overflow_count"] == 0


def test_time_bucket_hook_aggregates_counts_without_implicit_numeric_summing():
    sink = RecordingSink()
    writer = _writer(sink)
    merge = sum_kwargs("raw_count", "invalid_count")
    base = 1_750_000_000_000
    for index in range(10):
        disposition = writer.submit(
            "record_reject_bucket",
            kwargs={
                "bucket_start_ts_ms": base,
                "raw_count": 1,
                "invalid_count": int(index % 2 == 0),
                "latest_reason": f"reason-{index}",
            },
            bucket_key=("BTC", "NO_BOOK", "STALE"),
            event_ts_ms=base + index * 10,
            bucket_ms=1_000,
            merge_hook=merge,
        )
        assert disposition is (
            TelemetryDisposition.ACCEPTED if index == 0
            else TelemetryDisposition.COALESCED)
    writer.start()
    assert writer.stop(drain=True, timeout_s=2.0)

    assert len(sink.commands) == 1
    command = sink.commands[0]
    assert command["kwargs"] == {
        "bucket_start_ts_ms": base,
        "raw_count": 10,
        "invalid_count": 5,
        "latest_reason": "reason-9",
    }
    metrics = writer.snapshot()
    assert metrics["submitted"] == 10
    assert metrics["coalesced"] == 9
    assert metrics["written"] == 1
    assert metrics["logical_written"] == 10
    assert metrics["dropped"] == 0


def test_bad_bucket_merge_is_explicitly_dropped_not_silently_lost():
    sink = RecordingSink()
    writer = _writer(sink)
    merge = sum_kwargs("count")
    assert writer.submit(
        "record_bucket", kwargs={"count": 1}, bucket_key="x",
        event_ts_ms=1_000, merge_hook=merge,
    ) is TelemetryDisposition.ACCEPTED
    assert writer.submit(
        "record_bucket", kwargs={"count": "not-a-number"}, bucket_key="x",
        event_ts_ms=1_001, merge_hook=merge,
    ) is TelemetryDisposition.DROPPED
    writer.start()
    assert writer.stop(drain=True, timeout_s=2.0)
    metrics = writer.snapshot()
    assert metrics["submitted"] == 2
    assert metrics["dropped"] == 1
    assert metrics["written"] == 1
    assert metrics["last_failure_ts_ms"] is not None


def test_queue_overflow_is_nonblocking_counted_and_recovers_after_flush():
    sink = RecordingSink()
    writer = _writer(sink, capacity=2, batch_size=2)
    assert writer.submit("record", kwargs={"n": 1}) is TelemetryDisposition.ACCEPTED
    assert writer.submit("record", kwargs={"n": 2}) is TelemetryDisposition.ACCEPTED
    started = time.monotonic()
    assert writer.submit("record", kwargs={"n": 3}) is TelemetryDisposition.DROPPED
    assert time.monotonic() - started < 0.05
    before = writer.snapshot()
    assert before["health"] == "DEGRADED_OVERFLOW"
    assert before["queue_depth"] == 2
    assert before["queue_high_water"] == 2
    assert before["overflow_count"] == 1
    assert before["dropped"] == 1

    writer.start()
    assert writer.stop(drain=True, timeout_s=2.0)
    after = writer.snapshot()
    assert after["written"] == 2
    assert after["dropped"] == 1
    assert after["last_overflow_ts_ms"] is not None


def test_sink_failure_is_contained_counted_and_never_touches_critical_state():
    sink = RecordingSink(fail=True)
    sink.critical_commits = 7
    writer = _writer(sink)
    writer.start()
    assert writer.submit(
        "record_runtime_health", kwargs={"state": "RUNNING"},
    ) is TelemetryDisposition.ACCEPTED
    assert writer.flush(timeout_s=2.0)
    failed = writer.snapshot()
    assert failed["health"] == "DEGRADED_WRITER"
    assert failed["failed_batches"] == 1
    assert failed["dropped"] == 1
    assert failed["written"] == 0
    assert "RuntimeError" in failed["last_error"]
    assert sink.critical_commits == 7

    sink.fail = False
    assert writer.submit(
        "record_runtime_health", kwargs={"state": "RECOVERED"},
    ) is TelemetryDisposition.ACCEPTED
    assert writer.flush(timeout_s=2.0)
    recovered = writer.snapshot()
    assert recovered["health"] == "HEALTHY"
    assert recovered["written"] == 1
    assert recovered["failed_batches"] == 1
    assert recovered["dropped"] == 1
    assert sink.critical_commits == 7
    assert writer.stop(drain=True, timeout_s=2.0)


class PrioritySkip(RuntimeError):
    telemetry_priority_skip = True


class _TransientPrioritySink:
    """Yields to critical persistence for the first ``skips`` submissions."""

    def __init__(self, skips: int) -> None:
        self.skips = int(skips)
        self.attempts = 0
        self.written: list[Any] = []

    def submit_telemetry_batch(self, commands, *, timeout_s):
        del timeout_s
        self.attempts += 1
        if self.attempts <= self.skips:
            raise PrioritySkip("critical persistence pending")
        self.written.extend(commands)
        return len(commands)


def test_transient_priority_skip_defers_without_losing_rows():
    """A cooperative yield rolls back before writing, so nothing is lost.

    The sink never attempted the rows, so accounting them as loss (and as a
    batch failure) reported a telemetry outage every time critical persistence
    or a maintenance checkpoint briefly held the shared write gate.
    """

    sink = _TransientPrioritySink(skips=3)
    writer = _writer(sink)
    for index in range(4):
        assert writer.submit(
            TelemetryCommand("record", (index,), {"value": index}),
        ) is TelemetryDisposition.ACCEPTED
    writer.start()
    assert writer.flush(timeout_s=5.0)
    metrics = writer.snapshot()
    assert metrics["written"] == 4
    assert metrics["dropped"] == 0
    # A cooperative yield is designed backpressure, not a fault.
    assert metrics["failed_batches"] == 0
    assert metrics["priority_skipped_batches"] == 3
    assert metrics["requeued_rows"] >= 4
    assert metrics["health"] == "HEALTHY"
    assert metrics["last_priority_skip_ts_ms"] is not None
    assert writer.stop(drain=True, timeout_s=2.0)


def test_permanent_priority_skip_retries_a_bounded_number_of_times():
    """Requeueing is finite: a gate that never frees must not spin forever."""

    class PrioritySink:
        def __init__(self) -> None:
            self.attempts = 0

        def submit_telemetry_batch(self, commands, *, timeout_s):
            del commands, timeout_s
            self.attempts += 1
            raise PrioritySkip("critical persistence pending")

    sink = PrioritySink()
    writer = _writer(sink)
    assert writer.submit(
        "record_runtime_health", kwargs={"state": "RUNNING"},
    ) is TelemetryDisposition.ACCEPTED
    writer.start()
    assert writer.flush(timeout_s=10.0)
    metrics = writer.snapshot()
    # Exhausting the retry budget is honest loss, and only then degraded.
    assert metrics["health"] == "DEGRADED_CRITICAL_PRIORITY"
    assert metrics["dropped"] == 1
    assert metrics["written"] == 0
    assert metrics["priority_skipped_rows"] >= 1
    assert sink.attempts <= _MAX_PRIORITY_REQUEUE_ATTEMPTS + 1
    assert "telemetry_priority_skip_exhausted" in (metrics["last_error"] or "")
    assert writer.stop(drain=True, timeout_s=2.0)


def test_deadline_miss_shrinks_the_physical_chunk_and_stops_failing():
    """Deadline failures must be self-limiting, not a permanent loss source.

    A chunk larger than the sink's cooperative budget can only ever miss it, so
    resubmitting the same size forever is guaranteed loss.  The learned ceiling
    halves on each miss and never grows back inside one process, which bounds
    total deadline failures to log2(physical_batch_size).
    """

    class DeadlineExceeded(RuntimeError):
        telemetry_deadline_exceeded = True

    class BudgetedSink:
        """Commits at most ``capacity`` rows in one transaction."""

        def __init__(self, capacity: int) -> None:
            self.capacity = int(capacity)
            self.written = 0
            self.misses = 0
            self.max_accepted = 0

        def submit_telemetry_batch(self, commands, *, timeout_s):
            del timeout_s
            if len(commands) > self.capacity:
                self.misses += 1
                raise DeadlineExceeded("telemetry transaction exceeded deadline")
            self.max_accepted = max(self.max_accepted, len(commands))
            self.written += len(commands)
            return len(commands)

    sink = BudgetedSink(capacity=5)
    writer = _writer(sink, capacity=2_048, batch_size=64,
                     physical_batch_size=32)
    writer.start()
    for index in range(400):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    assert writer.flush(timeout_s=20.0)
    metrics = writer.snapshot()
    # 32 -> 16 -> 8 -> 4: at most four misses, then it stays under budget.
    assert sink.misses <= 4
    assert metrics["deadline_shrink_events"] == sink.misses
    assert metrics["physical_batch_ceiling"] <= 5
    assert sink.max_accepted <= 5

    # After warm-up the lane stops producing new failures entirely.
    failures_after_warmup = metrics["failed_batches"]
    for index in range(400, 800):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    assert writer.flush(timeout_s=20.0)
    steady = writer.snapshot()
    assert steady["failed_batches"] == failures_after_warmup
    assert steady["deadline_exceeded_batches"] == metrics["deadline_exceeded_batches"]
    assert steady["health"] == "HEALTHY"
    assert writer.stop(drain=True, timeout_s=5.0)


def test_chunk_size_is_computed_from_measured_cost_not_from_losses():
    """The size must recover when the database gets faster again.

    Learning only by failure pins the chunk at its worst observed value for
    the life of the process: one bad stretch (an oversized WAL) leaves the
    lane writing a single row per transaction forever, even after the
    condition clears.
    """

    class BudgetedSink:
        budget_ms = 200.0

        def __init__(self) -> None:
            self.ms_per_row = 20.0
            self.sizes: list[int] = []

        def telemetry_transaction_budget_ms(self) -> float:
            return self.budget_ms

        def submit_telemetry_batch(self, commands, *, timeout_s):
            del timeout_s
            self.sizes.append(len(commands))
            time.sleep(len(commands) * self.ms_per_row / 1000.0)
            return len(commands)

    sink = BudgetedSink()
    writer = _writer(sink, capacity=4_096, batch_size=64,
                     physical_batch_size=32)
    writer.start()
    for index in range(120):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    assert writer.flush(timeout_s=30.0)
    slow = writer.snapshot()
    assert slow["transaction_budget_ms"] == 200.0
    # 200 ms budget, half of it usable, ~20 ms per row -> about five rows.
    assert 2 <= slow["physical_batch_ceiling"] <= 8
    assert slow["observed_ms_per_row"] is not None

    # The database gets faster (WAL truncated); the size must climb back.
    sink.ms_per_row = 1.0
    for index in range(120, 900):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    assert writer.flush(timeout_s=30.0)
    fast = writer.snapshot()
    assert fast["physical_batch_ceiling"] > slow["physical_batch_ceiling"]
    assert fast["physical_batch_ceiling"] <= 32
    assert fast["failed_batches"] == 0
    assert fast["dropped"] == 0
    assert writer.stop(drain=True, timeout_s=10.0)


def test_deadline_miss_teaches_the_cost_floor_from_the_budget():
    """A miss proves per-row cost is at least budget/rows; use that."""

    class DeadlineExceeded(RuntimeError):
        telemetry_deadline_exceeded = True
        telemetry_deadline_ms = 120.0

    class LinearCostSink:
        """Cost grows with rows and the deadline bites past the budget."""

        budget_ms = 120.0
        ms_per_row = 12.0

        def __init__(self) -> None:
            self.sizes: list[int] = []

        def submit_telemetry_batch(self, commands, *, timeout_s):
            del timeout_s
            self.sizes.append(len(commands))
            cost_ms = len(commands) * self.ms_per_row
            if cost_ms > self.budget_ms:
                time.sleep(self.budget_ms / 1000.0)
                raise DeadlineExceeded("exceeded")
            time.sleep(cost_ms / 1000.0)
            return len(commands)

    sink = LinearCostSink()
    writer = _writer(sink, capacity=1_024, batch_size=32,
                     physical_batch_size=16)
    writer.start()
    for index in range(40):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    assert writer.flush(timeout_s=30.0)
    warm = writer.snapshot()
    assert warm["transaction_budget_ms"] == 120.0
    # 16 rows missed a 120 ms budget -> at least 7.5 ms per row.
    assert warm["observed_ms_per_row"] >= 120.0 / 16
    assert warm["physical_batch_ceiling"] < 16

    # Once the size reflects measured cost it settles below the deadline and
    # stops producing new failures.
    failures = warm["failed_batches"]
    for index in range(40, 160):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    assert writer.flush(timeout_s=60.0)
    steady = writer.snapshot()
    assert steady["failed_batches"] == failures
    assert steady["health"] == "HEALTHY"
    assert max(sink.sizes) <= 16
    assert writer.stop(drain=True, timeout_s=10.0)


def test_one_failed_chunk_never_destroys_rows_it_did_not_attempt():
    """A dispatch carries one physical chunk, so loss is bounded by it."""

    class OneShotFailingSink:
        def __init__(self) -> None:
            self.calls = 0
            self.written = 0

        def submit_telemetry_batch(self, commands, *, timeout_s):
            del timeout_s
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("physical writer rejected batch")
            self.written += len(commands)
            return len(commands)

    sink = OneShotFailingSink()
    writer = _writer(sink, batch_size=64, physical_batch_size=4)
    for index in range(40):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    writer.start()
    assert writer.stop(drain=True, timeout_s=10.0)
    metrics = writer.snapshot()
    assert metrics["dropped"] == 4
    assert metrics["written"] == 36
    assert metrics["failed_batches"] == 1


def test_deadline_backoff_actually_defers_the_next_dispatch():
    """The backoff window must suppress dispatch, not merely delay it briefly.

    Dispatching inside the window resubmits into a sink that is still missing
    its deadline, which is the tight retry-and-miss loop the backoff exists to
    prevent.
    """

    class DeadlineExceeded(RuntimeError):
        telemetry_deadline_exceeded = True

    class AlwaysMissingSink:
        def __init__(self) -> None:
            self.attempt_monotonics: list[float] = []

        def submit_telemetry_batch(self, commands, *, timeout_s):
            del commands, timeout_s
            self.attempt_monotonics.append(time.monotonic())
            raise DeadlineExceeded("telemetry transaction exceeded deadline")

    sink = AlwaysMissingSink()
    writer = _writer(sink, batch_size=8, physical_batch_size=1,
                     flush_interval_s=0.05)
    writer.start()
    for index in range(6):
        writer.submit(TelemetryCommand("record", (index,), {"value": index}))
    time.sleep(1.0)
    writer.stop(drain=False, timeout_s=2.0)
    gaps = [
        second - first
        for first, second in zip(sink.attempt_monotonics,
                                 sink.attempt_monotonics[1:])
    ]
    assert sink.attempt_monotonics, "sink was never called"
    # Every retry is separated by at least the first backoff step.
    assert all(gap >= 0.04 for gap in gaps), gaps
    assert writer.snapshot()["deadline_backoff_s"] > 0.0


def test_graceful_stop_drains_every_accepted_command_and_rejects_late_submit():
    sink = RecordingSink(delay_s=0.002)
    writer = _writer(sink, batch_size=5)
    writer.start()
    for index in range(23):
        assert writer.submit(
            TelemetryCommand("record", (index,), {"value": index}),
        ) is TelemetryDisposition.ACCEPTED
    assert writer.stop(drain=True, timeout_s=3.0)
    metrics = writer.snapshot()
    assert metrics["submitted"] == 23
    assert metrics["written"] == 23
    assert metrics["dropped"] == 0
    assert metrics["queue_depth"] == 0
    assert metrics["inflight_batches"] == 0
    assert metrics["health"] == "STOPPED"

    assert writer.submit("late") is TelemetryDisposition.DROPPED
    late = writer.snapshot()
    assert late["submitted"] == 24
    assert late["dropped"] == 1


def test_non_draining_stop_accounts_for_every_discarded_logical_event():
    sink = RecordingSink()
    writer = _writer(sink, capacity=20, batch_size=10)
    for index in range(7):
        writer.submit("record", kwargs={"n": index})
    assert writer.stop(drain=False, timeout_s=1.0)
    metrics = writer.snapshot()
    assert metrics["submitted"] == 7
    assert metrics["dropped"] == 7
    assert metrics["written"] == 0
    assert metrics["queue_depth"] == 0


def test_stop_timeout_can_be_retried_and_sink_closes_on_owner_thread():
    class BlockingClosableSink:
        def __init__(self):
            self.entered = threading.Event()
            self.release = threading.Event()
            self.submit_thread = 0
            self.close_thread = 0

        def submit_telemetry_batch(self, commands, *, timeout_s):
            del timeout_s
            self.submit_thread = threading.get_ident()
            self.entered.set()
            assert self.release.wait(3.0)
            return {"ok": True, "rows_written": len(commands)}

        def close_telemetry_sink(self):
            self.close_thread = threading.get_ident()
            return True

    sink = BlockingClosableSink()
    writer = _writer(sink)
    assert writer.submit(
        "record_runtime_health", kwargs={"state": "RUNNING"},
    ) is TelemetryDisposition.ACCEPTED
    writer.start()
    assert sink.entered.wait(2.0)
    assert writer.stop(drain=True, timeout_s=0.001) is False
    assert writer.snapshot()["health"] == "STOPPING_TIMEOUT"
    sink.release.set()
    assert writer.stop(drain=True, timeout_s=2.0) is True
    assert writer.stop(drain=True, timeout_s=0.01) is True
    assert writer.snapshot()["health"] == "STOPPED"
    assert sink.close_thread == sink.submit_thread
    assert sink.close_thread != threading.get_ident()


class FutureSink(RecordingSink):
    def submit_telemetry_batch(self, commands, *, timeout_s):
        future = Future()
        copied = [dict(command) for command in commands]
        self.batches.append(copied)
        future.set_result({"ok": True, "written": len(commands)})
        return future


class AsyncSink(RecordingSink):
    async def submit_telemetry_batch(self, commands, *, timeout_s):
        await __import__("asyncio").sleep(0)
        self.batches.append([dict(command) for command in commands])
        return {"success": True, "count": len(commands)}


@pytest.mark.parametrize("sink", [FutureSink(), AsyncSink()])
def test_duck_typed_sink_accepts_future_or_awaitable_result(sink):
    writer = _writer(sink)
    writer.start()
    writer.submit({"method": "record", "args": (1,), "kwargs": {"x": 2}})
    assert writer.stop(drain=True, timeout_s=2.0)
    metrics = writer.snapshot()
    assert metrics["written"] == 1
    assert metrics["failed_batches"] == 0


def test_deterministic_128_events_per_second_load_reduces_transactions():
    """Replay the required prior throughput without wall-clock flakiness."""

    sink = RecordingSink()
    writer = _writer(
        sink, capacity=256, batch_size=16, flush_interval_s=0.5)
    base_ts_ms = 1_750_000_000_000
    # Logical timestamps span exactly one second at 128 Hz.  Admission happens
    # before start so the batching result is completely deterministic.
    for index in range(128):
        event_ts_ms = base_ts_ms + index * 1_000 // 128
        assert writer.submit(
            "record_source_event",
            kwargs={"event_id": index, "event_ts_ms": event_ts_ms},
            dedupe_key=("load", index),
        ) is TelemetryDisposition.ACCEPTED
    writer.start()
    assert writer.stop(drain=True, timeout_s=3.0)

    metrics = writer.snapshot()
    assert metrics["submitted"] == 128
    assert metrics["written"] == 128
    assert metrics["logical_written"] == 128
    assert metrics["dropped"] == 0
    assert metrics["batches"] == 8
    assert len(sink.batches) == 8
    assert max(len(batch) for batch in sink.batches) == 16
    assert metrics["batches"] <= metrics["submitted"] // 16
    assert metrics["batch_latency_p95_ms"] >= 0
    assert metrics["flush_latency_p95_ms"] >= 0
    assert metrics["heartbeat_ts_ms"] > 0
    assert metrics["heartbeat_age_ms"] >= 0


def test_public_submit_is_synchronous_and_never_an_awaitable():
    writer = _writer(RecordingSink())
    result = writer.submit("record", kwargs={"x": 1})
    assert result is TelemetryDisposition.ACCEPTED
    assert not inspect.isawaitable(result)
    assert writer.stop(drain=False)
