from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import time

import pytest

from poly_alpha_sniper.lite_frequency_v4.maintenance import (
    CheckpointDecision,
    CheckpointMode,
    CheckpointStatus,
    MaintenancePolicy,
    MaintenanceSnapshot,
    MaintenanceStatus,
    decide_checkpoint,
    perform_checkpoint,
    run_bounded_maintenance_pass,
)


def _policy(**overrides) -> MaintenancePolicy:
    values = {
        "wal_trigger_bytes": 100,
        "restart_trigger_bytes": 400,
        "truncate_trigger_bytes": 100,
        "checkpoint_min_interval_ms": 1_000,
        "max_commit_p95_ms": 100.0,
        "restart_max_commit_p95_ms": 25.0,
        "telemetry_queue_gate": 5,
        "window_guard_ms": 3_000,
        "restart_window_guard_ms": 10_000,
        "retention_ms": 60_000,
        "retention_chunk_rows": 30,
        "retention_row_budget": 90,
        "retention_time_budget_ms": 100,
    }
    values.update(overrides)
    return MaintenancePolicy(**values)


def _snapshot(**overrides) -> MaintenanceSnapshot:
    values = {
        "now_ms": 20_000,
        "wal_bytes": 200,
        "critical_queue_depth": 0,
        "telemetry_queue_depth": 0,
        "runtime_active": True,
        "runtime_health": "READY",
        "writer_healthy": True,
        "open_positions": 0,
        "active_readers": 0,
        "long_reader_count": 0,
        "critical_commit_p95_ms": 10.0,
        "time_to_window_boundary_ms": 20_000,
        "last_checkpoint_attempt_ts_ms": None,
        "last_successful_checkpoint_ts_ms": None,
    }
    values.update(overrides)
    return MaintenanceSnapshot(**values)


class CheckpointStore:
    def __init__(self, response=(0, 10, 10), *, failure=None) -> None:
        self.response = response
        self.failure = failure
        self.checkpoint_calls: list[str] = []
        self.records: list[dict] = []

    def run_wal_checkpoint(self, mode: str):
        self.checkpoint_calls.append(mode)
        if self.failure is not None:
            raise self.failure
        return self.response

    def record_checkpoint_run(self, record: dict) -> None:
        self.records.append(dict(record))

    def database_file_size_bytes(self) -> int:
        return 1_024


class RetentionStore(CheckpointStore):
    def __init__(self, steps, response=(0, 10, 10), *, delay_s=0.0) -> None:
        super().__init__(response)
        self.steps = list(steps)
        self.delay_s = delay_s
        self.retention_calls: list[dict] = []

    def bounded_retention_step(self, **kwargs):
        self.retention_calls.append(dict(kwargs))
        if self.delay_s:
            time.sleep(self.delay_s)
        if not self.steps:
            return {"rows_deleted": 0, "protected_rows_skipped": 0}
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if isinstance(step, dict):
            return step
        return {"rows_deleted": step, "protected_rows_skipped": 2}


class V4BackgroundWriteDeferred(RuntimeError):
    pass


def _wal_reader(*sizes: int):
    remaining = list(sizes)

    def read() -> int:
        if not remaining:
            raise AssertionError("unexpected WAL-size read")
        return remaining.pop(0)

    return read


def _fixed_wall_clock() -> int:
    return 20_000


def test_normal_active_checkpoint_is_passive():
    decision = decide_checkpoint(_snapshot(), _policy())
    assert decision.should_run
    assert decision.mode is CheckpointMode.PASSIVE
    assert decision.reason == "normal_bounded_checkpoint"


def test_restart_requires_demonstrably_quiescent_runtime():
    safe = _snapshot(wal_bytes=500)
    assert decide_checkpoint(safe, _policy()).mode is CheckpointMode.RESTART

    unsafe_variants = (
        replace(safe, telemetry_queue_depth=1),
        replace(safe, open_positions=1),
        replace(safe, active_readers=1),
        replace(safe, long_reader_count=1),
        replace(safe, critical_commit_p95_ms=26.0),
        replace(safe, time_to_window_boundary_ms=10_000),
    )
    for snapshot in unsafe_variants:
        decision = decide_checkpoint(snapshot, _policy())
        assert decision.mode is CheckpointMode.PASSIVE


def test_truncate_requires_verified_stopped_and_quiescent_runtime():
    stopped = _snapshot(
        runtime_active=False,
        runtime_health="STOPPED",
        writer_healthy=False,
        critical_commit_p95_ms=None,
        time_to_window_boundary_ms=None,
    )
    assert decide_checkpoint(stopped, _policy()).mode is CheckpointMode.TRUNCATE

    for snapshot in (
        replace(stopped, runtime_health="UNKNOWN"),
        replace(stopped, active_readers=1),
        replace(stopped, open_positions=1),
        replace(stopped, telemetry_queue_depth=1),
    ):
        assert decide_checkpoint(snapshot, _policy()).mode is CheckpointMode.PASSIVE


@pytest.mark.parametrize(
    ("snapshot", "reason"),
    [
        (_snapshot(wal_bytes=99), "wal_below_trigger"),
        (
            _snapshot(last_checkpoint_attempt_ts_ms=19_500),
            "checkpoint_min_interval",
        ),
        (
            _snapshot(last_checkpoint_attempt_ts_ms=20_001),
            "checkpoint_timestamp_in_future",
        ),
        (_snapshot(critical_queue_depth=1), "critical_queue_not_empty"),
        (_snapshot(telemetry_queue_depth=6), "telemetry_queue_above_gate"),
        (_snapshot(writer_healthy=False), "critical_writer_unhealthy"),
        (_snapshot(runtime_health="DEGRADED"), "runtime_not_healthy"),
        (
            _snapshot(critical_commit_p95_ms=100.1),
            "critical_commit_latency_high",
        ),
        (_snapshot(time_to_window_boundary_ms=3_000), "market_window_guard"),
    ],
)
def test_checkpoint_skip_reasons_are_deterministic(snapshot, reason):
    decision = decide_checkpoint(snapshot, _policy())
    assert not decision.should_run
    assert decision.mode is None
    assert decision.reason == reason


def test_wal_pressure_forces_passive_checkpoint_through_closed_gate():
    # A gate-closed runtime (busy critical lane, degraded health, latency)
    # must not be able to defer checkpoints indefinitely once the WAL passes
    # the emergency threshold: PASSIVE never blocks the critical writer.
    gated_variants = (
        _snapshot(wal_bytes=400, critical_queue_depth=1),
        _snapshot(wal_bytes=401, runtime_health="DEGRADED"),
        _snapshot(wal_bytes=5_000, critical_commit_p95_ms=100.1),
        _snapshot(wal_bytes=400, writer_healthy=False),
    )
    for snapshot in gated_variants:
        decision = decide_checkpoint(snapshot, _policy())
        assert decision.should_run
        assert decision.mode is CheckpointMode.PASSIVE
        assert decision.reason == "emergency_wal_pressure"

    # Below the emergency threshold the gate still wins.
    below = _snapshot(wal_bytes=399, critical_queue_depth=1)
    assert not decide_checkpoint(below, _policy()).should_run
    # The bounded min-interval still rate-limits emergency passes.
    rate_limited = _snapshot(
        wal_bytes=5_000, critical_queue_depth=1,
        last_checkpoint_attempt_ts_ms=19_500,
    )
    assert not decide_checkpoint(rate_limited, _policy()).should_run


def test_checkpoint_records_wal_before_after_frames_and_duration():
    store = CheckpointStore(
        response={
            "busy": 0,
            "log_frames": 10,
            "checkpointed_frames": 10,
        }
    )
    decision = decide_checkpoint(_snapshot(), _policy())
    result = perform_checkpoint(
        store,
        decision,
        _policy(),
        wall_clock_ms=_fixed_wall_clock,
        wal_size_reader=_wal_reader(200, 20),
    )

    assert result.status is CheckpointStatus.SUCCESS
    assert result.before_wal_bytes == 200
    assert result.after_wal_bytes == 20
    assert result.busy_result == 0
    assert result.frames_total == 10
    assert result.frames_checkpointed == 10
    assert result.database_bytes == 1_024
    assert result.duration_ms >= 0
    assert result.record_attempted and result.recorded
    assert store.checkpoint_calls == ["PASSIVE"]
    assert store.records == [result.checkpoint_record()]
    assert store.records[0]["success"] == 1


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ((1, 20, 4), CheckpointStatus.BUSY),
        ((0, 20, 4), CheckpointStatus.PARTIAL),
    ],
)
def test_busy_and_partial_checkpoint_are_accounted_not_raised(response, expected):
    store = CheckpointStore(response=response)
    result = perform_checkpoint(
        store,
        decide_checkpoint(_snapshot(), _policy()),
        _policy(),
        wall_clock_ms=_fixed_wall_clock,
        wal_size_reader=_wal_reader(200, 180),
    )
    assert result.status is expected
    assert result.before_wal_bytes == 200
    assert result.after_wal_bytes == 180
    assert result.frames_total == 20
    assert result.frames_checkpointed == 4
    assert result.recorded


def test_checkpoint_failure_is_structured_and_recorded():
    store = CheckpointStore(failure=OSError("disk busy"))
    result = perform_checkpoint(
        store,
        decide_checkpoint(_snapshot(), _policy()),
        _policy(),
        wall_clock_ms=_fixed_wall_clock,
        wal_size_reader=_wal_reader(200, 200),
    )
    assert result.status is CheckpointStatus.FAILED
    assert result.failure_reason == "OSError:disk busy"
    assert result.before_wal_bytes == result.after_wal_bytes == 200
    assert result.recorded
    assert store.records[0]["success"] == 0
    assert store.records[0]["failure_reason"] == "OSError:disk busy"


def test_background_gate_defer_is_skipped_and_never_recorded_as_failure():
    store = CheckpointStore(failure=V4BackgroundWriteDeferred("critical pending"))
    result = perform_checkpoint(
        store,
        decide_checkpoint(_snapshot(), _policy()),
        _policy(),
        wall_clock_ms=_fixed_wall_clock,
        wal_size_reader=_wal_reader(200, 200),
    )
    assert result.status is CheckpointStatus.SKIPPED
    assert result.reason == "critical_write_pending"
    assert result.failure_reason is None
    assert store.records == []


def test_checkpoint_record_failure_does_not_hide_completed_operation():
    class BrokenRecorderStore(CheckpointStore):
        def record_checkpoint_run(self, record):
            raise RuntimeError("record unavailable")

    store = BrokenRecorderStore()
    result = perform_checkpoint(
        store,
        decide_checkpoint(_snapshot(), _policy()),
        _policy(),
        wall_clock_ms=_fixed_wall_clock,
        wal_size_reader=_wal_reader(200, 0),
    )
    assert result.status is CheckpointStatus.SUCCESS
    assert result.record_attempted and not result.recorded
    assert result.record_error == "RuntimeError:record unavailable"


def test_current_v4_store_contract_executes_and_records_off_loop(tmp_path):
    from poly_alpha_sniper.lite_frequency_v4.store import V4Store

    store = V4Store(tmp_path / "maintenance.db")
    try:
        snapshot = _snapshot(wal_bytes=200)
        result = perform_checkpoint(
            store,
            decide_checkpoint(snapshot, _policy()),
            _policy(),
            wall_clock_ms=_fixed_wall_clock,
        )
        row = store.query_one(
            "SELECT * FROM checkpoint_runs ORDER BY checkpoint_run_id DESC LIMIT 1"
        )
        assert result.status is CheckpointStatus.SUCCESS
        assert result.record_attempted and result.recorded
        assert row is not None
        assert row["mode"] == "PASSIVE"
        assert row["before_wal_bytes"] == result.before_wal_bytes
        assert row["after_wal_bytes"] == result.after_wal_bytes
        assert row["frames_checkpointed"] == result.frames_checkpointed
    finally:
        store.close()


def test_long_reader_forces_passive_and_partial_pass_stops_retention():
    snapshot = _snapshot(wal_bytes=500, active_readers=1, long_reader_count=1)
    decision = decide_checkpoint(snapshot, _policy())
    assert decision.mode is CheckpointMode.PASSIVE

    store = RetentionStore([10], response=(0, 20, 4))
    result = run_bounded_maintenance_pass(
        store,
        snapshot=snapshot,
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
        wal_size_reader=_wal_reader(500, 480),
    )
    assert result.status is MaintenanceStatus.PARTIAL
    assert result.reason == "checkpoint_partial"
    assert result.checkpoint is not None
    assert result.checkpoint.status is CheckpointStatus.PARTIAL
    assert store.retention_calls == []


def test_busy_checkpoint_stops_retention_without_crashing_worker_pass():
    store = RetentionStore([10], response=(1, 20, 0))
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(),
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
        wal_size_reader=_wal_reader(200, 200),
    )
    assert result.status is MaintenanceStatus.PARTIAL
    assert result.reason == "checkpoint_busy"
    assert result.failure_reason is None
    assert store.retention_calls == []


def test_queue_gate_skips_all_store_work():
    store = RetentionStore([10])
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(critical_queue_depth=1),
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is MaintenanceStatus.SKIPPED
    assert result.reason == "critical_queue_not_empty"
    assert store.checkpoint_calls == []
    assert store.retention_calls == []


def test_telemetry_queue_gate_skips_all_store_work():
    store = RetentionStore([10])
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(wal_bytes=0, telemetry_queue_depth=6),
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is MaintenanceStatus.SKIPPED
    assert result.reason == "telemetry_queue_above_gate"
    assert store.retention_calls == []


def test_bounded_pass_honors_exact_row_budget_and_protection_flag():
    store = RetentionStore([30, 30, 30, 30])
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(wal_bytes=0),
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is MaintenanceStatus.PARTIAL
    assert result.reason == "row_budget_exhausted"
    assert result.rows_deleted == 90
    assert result.protected_rows_skipped == 6
    assert result.chunks_completed == 3
    assert result.row_budget_exhausted
    assert all(call["protect_trade_evidence"] for call in store.retention_calls)
    assert [call["max_rows"] for call in store.retention_calls] == [30, 30, 30]


def test_nondefault_raw_cap_and_compaction_limits_reach_store_contract():
    store = RetentionStore([1, 0])
    policy = _policy(
        raw_event_max_rows=1_234,
        event_bucket_detail_retention_ms=12_345,
        metadata_retention_ms=67_890,
        metadata_max_rows=2_345,
        journal_payload_retention_ms=98_765,
    )
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(wal_bytes=0),
        policy=policy,
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.rows_deleted == 1
    first = store.retention_calls[0]
    assert first["raw_event_max_rows"] == 1_234
    assert first["event_bucket_detail_retention_ms"] == 12_345
    assert first["metadata_retention_ms"] == 67_890
    assert first["metadata_max_rows"] == 2_345
    assert first["journal_payload_retention_ms"] == 98_765
    assert first["now_ms"] == 20_000


def test_compaction_units_share_budget_and_emit_separate_metrics():
    store = RetentionStore([
        {
            "rows_deleted": 0,
            "rows_compacted": 30,
            "budget_units": 30,
            "protected_rows_skipped": 0,
            "metrics": {"journal_payloads_compacted": 30},
        },
        {"rows_deleted": 0, "rows_compacted": 0, "budget_units": 0},
    ])
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(wal_bytes=0),
        policy=_policy(retention_row_budget=60),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.rows_deleted == 0
    assert result.rows_compacted == 30
    assert result.budget_units_consumed == 30
    assert result.metrics == {"journal_payloads_compacted": 30}


def test_bounded_pass_honors_time_budget_after_one_incremental_chunk():
    # Use a delay above the coarsest Windows monotonic-clock tick so this stays
    # deterministic on hosts whose clock resolution is about 15.6 ms.
    store = RetentionStore([10, 10], delay_s=0.05)
    policy = _policy(retention_time_budget_ms=1)
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(wal_bytes=0),
        policy=policy,
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is MaintenanceStatus.PARTIAL
    assert result.reason == "time_budget_exhausted"
    assert result.rows_deleted == 10
    assert result.chunks_completed == 1
    assert result.time_budget_exhausted


def test_retention_failure_and_protected_delete_are_fail_closed():
    failed = RetentionStore([RuntimeError("locked")])
    result = run_bounded_maintenance_pass(
        failed,
        snapshot=_snapshot(wal_bytes=0),
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is MaintenanceStatus.FAILED
    assert result.failure_reason == "RuntimeError:locked"

    protected = RetentionStore([{"rows_deleted": 1, "protected_rows_deleted": 1}])
    result = run_bounded_maintenance_pass(
        protected,
        snapshot=_snapshot(wal_bytes=0),
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is MaintenanceStatus.FAILED
    assert "protected-row deletion" in (result.failure_reason or "")


def test_background_gate_defer_yields_retention_to_critical_writer():
    store = RetentionStore([V4BackgroundWriteDeferred("critical pending")])
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(wal_bytes=0),
        policy=_policy(),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is MaintenanceStatus.SKIPPED
    assert result.reason == "critical_write_pending"
    assert result.failure_reason is None


def test_current_store_compaction_contract_is_kept_within_combined_budget():
    class ExistingStoreContract(CheckpointStore):
        def __init__(self):
            super().__init__()
            self.calls = []

        def compact_raw_evidence(self, now_ms, **kwargs):
            self.calls.append((now_ms, dict(kwargs)))
            batch = kwargs["batch_size"]
            return {
                "source_events": batch,
                "book_snapshots": batch,
                "cex_observations": batch,
                "pinned_rows_skipped": 7,
            }

    store = ExistingStoreContract()
    policy = _policy(retention_chunk_rows=9, retention_row_budget=9)
    result = run_bounded_maintenance_pass(
        store,
        snapshot=_snapshot(wal_bytes=0),
        policy=policy,
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.rows_deleted == 9
    assert result.protected_rows_skipped == 7
    assert result.row_budget_exhausted
    assert store.calls == [
        (
            20_000,
            {
                "retention_ms": 60_000,
                "batch_size": 3,
                "run_integrity": False,
            },
        )
    ]


def test_active_runtime_cannot_force_truncate_at_execution_boundary():
    snapshot = _snapshot(wal_bytes=500)
    forced = CheckpointDecision(
        True, CheckpointMode.TRUNCATE, "forced_by_caller", snapshot
    )
    store = CheckpointStore()
    result = perform_checkpoint(
        store,
        forced,
        _policy(),
        wall_clock_ms=_fixed_wall_clock,
    )
    assert result.status is CheckpointStatus.SKIPPED
    assert result.reason == "truncate_requires_verified_stopped_runtime"
    assert store.checkpoint_calls == []


def test_no_unbounded_database_rewrite_statement_exists_in_policy_module():
    source = (
        Path(__file__).resolve().parent.parent / "lite_frequency_v4" / "maintenance.py"
    ).read_text(encoding="utf-8")
    forbidden = "V" + "ACUUM"
    assert forbidden not in source.upper()
    assert "wal_checkpoint(FULL)" not in source


def test_checkpoint_and_retention_refuse_active_asyncio_loop():
    async def exercise():
        snapshot = _snapshot()
        store = RetentionStore([10])
        checkpoint = perform_checkpoint(
            store,
            decide_checkpoint(snapshot, _policy()),
            _policy(),
            wall_clock_ms=_fixed_wall_clock,
        )
        maintenance = run_bounded_maintenance_pass(
            store,
            snapshot=snapshot,
            policy=_policy(),
            wall_clock_ms=_fixed_wall_clock,
        )
        return checkpoint, maintenance, store

    checkpoint, maintenance, store = asyncio.run(exercise())
    assert checkpoint.status is CheckpointStatus.SKIPPED
    assert checkpoint.reason == "dedicated_maintenance_worker_required"
    assert maintenance.status is MaintenanceStatus.SKIPPED
    assert maintenance.reason == "dedicated_maintenance_worker_required"
    assert store.checkpoint_calls == []
    assert store.retention_calls == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"wal_trigger_bytes": 0},
        {"restart_trigger_bytes": 99},
        {"truncate_trigger_bytes": 99},
        {"restart_max_commit_p95_ms": 101.0},
        {"retention_chunk_rows": 91},
        {"max_commit_p95_ms": float("nan")},
    ],
)
def test_policy_limits_are_validated(kwargs):
    with pytest.raises((TypeError, ValueError)):
        _policy(**kwargs)
