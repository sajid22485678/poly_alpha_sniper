from __future__ import annotations

import ast
from concurrent.futures import Future
import inspect
from pathlib import Path
import threading
import time

import pytest

from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    TelemetryCommand,
    TelemetryDisposition,
    V4TelemetryWriter,
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


def test_critical_priority_skip_is_explicitly_dropped_and_degraded():
    class PrioritySkip(RuntimeError):
        telemetry_priority_skip = True

    class PrioritySink:
        def submit_telemetry_batch(self, commands, *, timeout_s):
            del commands, timeout_s
            raise PrioritySkip("critical persistence pending")

    writer = _writer(PrioritySink())
    assert writer.submit(
        "record_runtime_health", kwargs={"state": "RUNNING"},
    ) is TelemetryDisposition.ACCEPTED
    writer.start()
    assert writer.flush(timeout_s=2.0)
    metrics = writer.snapshot()
    assert metrics["health"] == "DEGRADED_CRITICAL_PRIORITY"
    assert metrics["priority_skipped_batches"] == 1
    assert metrics["priority_skipped_rows"] == 1
    assert metrics["dropped"] == 1
    assert metrics["written"] == 0
    assert writer.stop(drain=True, timeout_s=2.0)


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
