"""Reader-lifetime diagnostics: bounded registry, worker/store wiring, and
per-checkpoint correlation.

WAL reclamation is gated by whichever read transaction holds the oldest
read-mark.  These tests pin the contract that makes the pinning reader
identifiable: every read-worker job and every read statement registers its
lifetime, the registry stays bounded no matter how many operations run, and a
checkpoint result carries the active-reader view captured at its start and end.
"""
from __future__ import annotations

from pathlib import Path
import threading

import pytest

from poly_alpha_sniper.lite_frequency_v4.maintenance import (
    CheckpointDecision,
    CheckpointMode,
    CheckpointStatus,
    MaintenancePolicy,
    MaintenanceSnapshot,
    perform_checkpoint,
    run_bounded_maintenance_pass,
)
from poly_alpha_sniper.lite_frequency_v4.reader_diag import (
    READER_DIAGNOSTICS,
    ReaderDiagnostics,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4ReadOnlyStore, V4Store
from poly_alpha_sniper.lite_frequency_v4.workers import V4ReadWorker


@pytest.fixture(autouse=True)
def _clean_registry():
    READER_DIAGNOSTICS.reset()
    yield
    READER_DIAGNOSTICS.reset()


def _fresh_db(tmp_path: Path, name: str = "frequency-v4-reader-diag.db") -> Path:
    path = tmp_path / name
    store = V4Store(path)
    store.close()
    return path


# ---------------------------------------------------------------------------
# Registry contract
# ---------------------------------------------------------------------------


def test_statement_lifecycle_updates_counters_and_last_release():
    registry = ReaderDiagnostics()
    token = registry.begin_statement(conn_id=42)
    active = registry.snapshot()
    assert active["active_reader_count"] == 1
    assert active["oldest_reader_age_ms"] >= 0.0
    assert active["oldest_reader_connection_id"] == 42
    registry.end_statement(token, rows=7)
    done = registry.snapshot()
    assert done["active_reader_count"] == 0
    assert done["counters"]["statements_started"] == 1
    assert done["counters"]["statements_completed"] == 1
    assert done["last_release"]["scope"] == "statement"
    assert done["last_reader_release_ms"] >= 0.0


def test_statement_is_attributed_to_the_enclosing_job():
    registry = ReaderDiagnostics()
    job = registry.begin_job("worker-a", "dashboard_export", "REPORT")
    token = registry.begin_statement(conn_id=1)
    view = registry.snapshot()
    assert view["oldest_reader_name"] == "dashboard_export"
    assert view["oldest_reader_worker"] == "worker-a"
    assert view["oldest_job_name"] == "dashboard_export"
    registry.end_statement(token, rows=1)
    registry.end_job(job)
    ops = registry.snapshot()["ops"]
    assert "job:worker-a:dashboard_export" in ops
    assert "statement:worker-a:dashboard_export" in ops
    assert ops["statement:worker-a:dashboard_export"]["count"] == 1
    assert ops["statement:worker-a:dashboard_export"]["last_rows"] == 1


def test_statement_outside_any_job_is_unattributed_not_lost():
    registry = ReaderDiagnostics()
    token = registry.begin_statement()
    assert registry.snapshot()["oldest_reader_worker"] == "unattributed"
    registry.end_statement(token, rows=0)
    assert "statement:unattributed:statement" in registry.snapshot()["ops"]


def test_job_context_is_thread_local():
    registry = ReaderDiagnostics()
    registry.begin_job("worker-a", "export", "REPORT")
    seen: dict[str, str] = {}

    def other_thread() -> None:
        token = registry.begin_statement()
        seen["worker"] = registry.snapshot()["oldest_reader_worker"]
        registry.end_statement(token)

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join()
    # The other thread runs no job, so its statement must not inherit ours.
    assert seen["worker"] == "unattributed"


def test_error_and_unknown_token_paths_are_safe():
    registry = ReaderDiagnostics()
    token = registry.begin_statement()
    registry.end_statement(token, error="OperationalError")
    assert registry.snapshot()["counters"]["errors"] == 1
    # Unknown and zero tokens are no-ops, not failures.
    registry.end_statement(0)
    registry.end_statement(999_999)
    registry.end_job(0)
    assert registry.snapshot()["counters"]["statements_completed"] == 1


def test_registry_stays_bounded_under_unbounded_operation_names():
    registry = ReaderDiagnostics()
    for index in range(500):
        job = registry.begin_job("worker-a", f"op-{index}", "REPORT")
        registry.end_job(job)
    view = registry.snapshot()
    # Aggregates cap at the bucket bound plus a single overflow bucket.
    assert len(view["ops"]) <= 49
    assert "job:__overflow__" in view["ops"]
    total = sum(bucket["count"] for bucket in view["ops"].values())
    assert total == 500


def test_active_set_is_bounded_and_snapshot_active_list_capped():
    registry = ReaderDiagnostics()
    tokens = [registry.begin_statement() for _ in range(200)]
    view = registry.snapshot()
    assert view["active_reader_count"] <= 64
    assert len(view["active"]) <= 12
    assert view["counters"]["active_overflow_dropped"] > 0
    for token in tokens:
        registry.end_statement(token)
    assert registry.snapshot()["active_reader_count"] == 0


def test_long_reader_threshold_flags_old_readers():
    registry = ReaderDiagnostics()
    registry.configure(long_reader_threshold_ms=1.0)
    token = registry.begin_statement()
    import time as _time

    # Windows time.monotonic ticks at ~15.6 ms; sleep well past one tick.
    _time.sleep(0.05)
    assert registry.snapshot()["readers_over_threshold"] >= 1
    registry.end_statement(token)
    assert registry.snapshot()["readers_over_threshold"] == 0


def test_compact_snapshot_omits_the_aggregate_table():
    registry = ReaderDiagnostics()
    job = registry.begin_job("worker-a", "export", "REPORT")
    registry.end_job(job)
    assert "ops" not in registry.snapshot(compact=True)
    assert "ops" in registry.snapshot()


def test_wal_size_is_observed_via_stat_only(tmp_path):
    registry = ReaderDiagnostics()
    wal = tmp_path / "test.db-wal"
    wal.write_bytes(b"x" * 1234)
    registry.configure(wal_path=wal)
    assert registry.snapshot()["wal_bytes"] == 1234
    wal.unlink()
    assert registry.snapshot()["wal_bytes"] == 0
    registry.configure(wal_path=None)
    assert registry.snapshot()["wal_bytes"] is None


# ---------------------------------------------------------------------------
# Worker and store wiring
# ---------------------------------------------------------------------------


def test_read_worker_jobs_and_statements_register(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path, worker_name="diag-read-worker").start()
    try:
        worker.query_sync("SELECT COUNT(*) AS n FROM markets")
        worker.query_one_sync("SELECT COUNT(*) AS n FROM entries")
    finally:
        worker.stop()
    view = READER_DIAGNOSTICS.snapshot()
    assert view["active_reader_count"] == 0
    assert view["active_job_count"] == 0
    assert view["counters"]["jobs_started"] == 2
    assert view["counters"]["jobs_completed"] == 2
    assert view["counters"]["statements_completed"] >= 2
    assert "job:diag-read-worker:query" in view["ops"]
    assert "statement:diag-read-worker:query" in view["ops"]


def test_failed_job_still_releases_its_registration(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path, worker_name="diag-error-worker").start()
    try:
        with pytest.raises(Exception):
            worker.query_sync("SELECT * FROM this_table_does_not_exist")
    finally:
        worker.stop()
    view = READER_DIAGNOSTICS.snapshot()
    assert view["active_reader_count"] == 0
    assert view["active_job_count"] == 0
    assert view["counters"]["errors"] >= 1


def test_read_only_store_registers_statement_scope(tmp_path):
    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        store.query("SELECT COUNT(*) AS n FROM markets")
        store.query_one("SELECT COUNT(*) AS n FROM entries")
        store.integrity_check(quick=True)
    finally:
        store.close()
    view = READER_DIAGNOSTICS.snapshot()
    assert view["counters"]["statements_started"] == 3
    assert view["counters"]["statements_completed"] == 3
    assert view["active_reader_count"] == 0


def test_writer_store_reads_do_not_register(tmp_path):
    """Only read-only connections register; the writer is not a WAL reader
    in the sense that gates checkpoint progress."""

    path = _fresh_db(tmp_path)
    store = V4Store(path)
    try:
        store.query("SELECT COUNT(*) AS n FROM markets")
    finally:
        store.close()
    assert READER_DIAGNOSTICS.snapshot()["counters"]["statements_started"] == 0


# ---------------------------------------------------------------------------
# Checkpoint correlation
# ---------------------------------------------------------------------------


class _CheckpointStore:
    def __init__(self, wal_bytes: int = 200_000_000) -> None:
        self.wal_bytes = int(wal_bytes)

    def checkpoint(self, *, mode: str = "PASSIVE", reason: str = "manual",
                   busy_timeout_ms=None):
        return {
            "checkpoint_run_id": 1, "mode": mode,
            "before_wal_bytes": self.wal_bytes,
            "after_wal_bytes": self.wal_bytes,
            "duration_ms": 2.0, "busy_result": 0,
            "frames_total": 100, "frames_checkpointed": 100,
            "success": True, "failure_reason": None,
        }

    def database_size_bytes(self) -> int:
        return 1_000_000


def _snapshot(**overrides):
    base = dict(
        now_ms=1_000, wal_bytes=200_000_000,
        critical_queue_depth=0, telemetry_queue_depth=0,
        runtime_active=True, runtime_health="HEALTHY", writer_healthy=True,
        open_positions=0, active_readers=0, long_reader_count=0,
        critical_commit_p95_ms=15.0, time_to_window_boundary_ms=120_000,
    )
    base.update(overrides)
    return MaintenanceSnapshot(**base)


def test_checkpoint_result_carries_reader_views():
    decision = CheckpointDecision(
        True, CheckpointMode.PASSIVE, "normal_bounded_checkpoint", _snapshot())
    calls: list[str] = []

    def provider():
        calls.append("snap")
        return {"active_reader_count": 2, "oldest_reader_name": "dashboard_export"}

    result = perform_checkpoint(
        _CheckpointStore(), decision, MaintenancePolicy(),
        wal_size_reader=lambda: 200_000_000,
        reader_snapshot_provider=provider,
    )
    assert result.status is CheckpointStatus.SUCCESS
    assert result.readers_at_start == {
        "active_reader_count": 2, "oldest_reader_name": "dashboard_export"}
    assert result.readers_at_end["active_reader_count"] == 2
    assert calls == ["snap", "snap"]
    payload = result.as_dict()
    assert payload["readers_at_start"]["oldest_reader_name"] == "dashboard_export"
    # The persisted checkpoint_runs row keeps its fixed schema.
    assert "readers_at_start" not in result.checkpoint_record()


def test_checkpoint_without_provider_reports_none():
    decision = CheckpointDecision(
        True, CheckpointMode.PASSIVE, "normal_bounded_checkpoint", _snapshot())
    result = perform_checkpoint(
        _CheckpointStore(), decision, MaintenancePolicy(),
        wal_size_reader=lambda: 200_000_000,
    )
    assert result.readers_at_start is None
    assert result.readers_at_end is None


def test_reader_snapshot_failure_never_fails_the_checkpoint():
    decision = CheckpointDecision(
        True, CheckpointMode.PASSIVE, "normal_bounded_checkpoint", _snapshot())

    def broken():
        raise RuntimeError("diagnostics fault")

    result = perform_checkpoint(
        _CheckpointStore(), decision, MaintenancePolicy(),
        wal_size_reader=lambda: 200_000_000,
        reader_snapshot_provider=broken,
    )
    assert result.status is CheckpointStatus.SUCCESS
    assert result.readers_at_start is None
    assert result.readers_at_end is None


def test_maintenance_pass_threads_the_provider_through():
    result = run_bounded_maintenance_pass(
        _CheckpointStore(),
        snapshot=_snapshot(),
        policy=MaintenancePolicy(),
        wal_size_reader=lambda: 200_000_000,
        reader_snapshot_provider=lambda: {"active_reader_count": 1},
    )
    assert result.checkpoint is not None
    assert result.checkpoint.readers_at_start == {"active_reader_count": 1}


# ---------------------------------------------------------------------------
# Publication: export payload and soak sampler
# ---------------------------------------------------------------------------


def test_export_forwards_reader_diagnostics(tmp_path):
    from poly_alpha_sniper.lite_frequency_v4.export import (
        build_frequency_v4_dashboard,
    )
    from tests.test_frequency_v4_export import (
        CACHED_OK_INTEGRITY,
        _healthy_runtime_state,
        _store_with_health,
    )
    from tests.test_frequency_v4_store import NOW

    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["persistence"]["sqlite_readers"] = {
            "active_reader_count": 1,
            "oldest_reader_name": "dashboard_export",
            "oldest_reader_age_ms": 12.5,
        }
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, runtime_state=state,
            session_id=session, integrity=CACHED_OK_INTEGRITY,
        )
        readers = payload["persistence"]["sqlite_readers"]
        assert readers["oldest_reader_name"] == "dashboard_export"
        assert readers["active_reader_count"] == 1
    finally:
        store.close()


def test_sampler_records_reader_lifetime_fields(tmp_path, monkeypatch):
    import json

    from poly_alpha_sniper.tools import v4_soak_sampler as sampler

    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "state.json").write_text(json.dumps({
        "current_commit": "c" * 40, "pid": 4242, "session_id": "s" * 32,
        "launch_nonce": "n" * 32, "state": "RUNNING",
        "process_ownership_valid": True,
        "persistence": {
            "sqlite_readers": {
                "active_reader_count": 2,
                "active_job_count": 1,
                "oldest_reader_age_ms": 44.5,
                "oldest_reader_name": "dashboard_export",
                "oldest_reader_worker": "lite-frequency-v4-report-read-worker",
                "readers_over_threshold": 0,
                "last_reader_release_ms": 3.2,
                "max_statement_ms": 310.0,
                "max_job_ms": 900.0,
                "counters": {"statements_completed": 10},
                "ops": {"stmt:x:y": {"count": 10}},
            },
            "latest_checkpoint": {
                "status": "SUCCESS", "frames_total": 100,
                "frames_checkpointed": 100, "busy_result": 0,
                "duration_ms": 4.5, "started_ts_ms": 1,
                "readers_at_start": {
                    "active_reader_count": 1,
                    "oldest_reader_age_ms": 20.0,
                    "oldest_reader_name": "dashboard_export",
                    "wal_bytes": 5_000,
                },
            },
        },
    }), encoding="utf-8")
    (runtime_dir / "heartbeat.json").write_text(
        json.dumps({"ts_ms": 1}), encoding="utf-8")
    export_path = tmp_path / "export.json"
    export_path.write_text(json.dumps({"generated_ts_ms": 1}), encoding="utf-8")
    monkeypatch.setattr(sampler, "open_runtime_sessions", lambda _p: 1)

    row = sampler.sample_once(
        1, {"commit": "c" * 40, "pid": 4242},
        export_path=export_path, runtime_dir=runtime_dir,
        db_path=tmp_path / "db.sqlite")
    assert row["active_reader_count"] == 2
    assert row["oldest_reader_age_ms"] == 44.5
    assert row["oldest_reader_name"] == "dashboard_export"
    assert row["max_statement_ms"] == 310.0
    assert row["reader_ops"] == {"stmt:x:y": {"count": 10}}
    assert row["checkpoint_status"] == "SUCCESS"
    assert row["checkpoint_frames_checkpointed"] == 100
    assert row["checkpoint_readers_at_start"]["oldest_reader_name"] == (
        "dashboard_export")
    assert row["checkpoint_readers_at_start"]["wal_bytes"] == 5_000
