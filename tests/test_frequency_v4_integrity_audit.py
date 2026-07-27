"""Integrity architecture: bounded live checks and an off-live full audit.

Two failures are pinned here, both measured on the 9.29 GB production evidence
store during the controlled gate at ``1f30bd5``:

- the periodic scan cut a chunk at one *table*, and one table
  (``source_events``, 3.18 M rows) held a single WAL read-mark for **over
  140 s**.  A checkpoint cannot pass an open read-mark, so the log grew
  monotonically 73 MB -> 246 MB and the dashboard export missed its <=10 s
  freshness contract twice (18.3 s and 13.4 s).
- the six-hourly full ``PRAGMA integrity_check`` ran against the *live*
  database, which is the same defect with a longer statement.

The fix is two separate things that must never be conflated: a bounded,
resumable live *health* check whose chunk cost is a row count, and a full audit
that runs against a completed incremental snapshot.  Both halves are asserted
below, including that the live database never sees a full integrity_check again.

Convergence evidence for the snapshot design (synthetic 206 MB WAL database,
1024-page steps):

===================  =========  ==========  ===========
source               restarts   steps       converged
===================  =========  ==========  ===========
quiet                0          50          yes, 3.05 s
external writer 5/s  218        2,702        no, 45 s
own connection       0          50          yes, 1.31 s
===================  =========  ==========  ===========

That is why the snapshot is taken in the runtime's quiescent startup window and
why a copy attempted under live write load reports ``DEFERRED_SOURCE_CHURN``
rather than publishing a torn image.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from poly_alpha_sniper.lite_frequency_v4 import engine as engine_module
from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config
from poly_alpha_sniper.lite_frequency_v4.persistence import V4PersistenceWriter
from poly_alpha_sniper.lite_frequency_v4.reader_diag import READER_DIAGNOSTICS
from poly_alpha_sniper.lite_frequency_v4.runtime import V4RuntimeFiles
from poly_alpha_sniper.lite_frequency_v4.store import (
    V4ReadOnlyStore,
    V4Store,
    audit_snapshot_database,
)
from poly_alpha_sniper.lite_frequency_v4.workers import (
    V4ReadWorker,
    V4RuntimeIOWorker,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    READER_DIAGNOSTICS.reset()
    yield
    READER_DIAGNOSTICS.reset()


@pytest.fixture
def engine_harness(tmp_path, monkeypatch):
    """A real engine with the dedicated integrity and runtime-IO workers.

    Deliberately production-shaped for the two workers this contract is about:
    the audit must be provably running on the integrity worker's own thread and
    connection, and the snapshot sweep on the runtime IO worker -- neither can
    be asserted against a harness that serialises everything.
    """

    cfg = FrequencyV4Config()
    cfg.db_path = str(tmp_path / "poly_alpha_frequency_v4.db")
    cfg.runtime_dir = str(tmp_path / "runtime" / "lite_frequency_v4_shadow")
    cfg.export_dir = str(tmp_path / "export" / "poly_alpha_frequency_v4")
    monkeypatch.setattr(
        engine_module, "validate_frequency_v4_config", lambda _cfg: None)

    runtime = V4RuntimeFiles(cfg.runtime_dir, repo_root=tmp_path)
    runtime.acquire()
    monkeypatch.setattr(runtime, "process_ownership", lambda: {
        "process_ownership_valid": True, "exact_v4_processes": 1,
        "owned_v4_processes": 1, "orphan_processes": 0,
        "exact_pids": [runtime.pid],
    })
    engine = engine_module.FrequencyV4Engine(cfg, runtime)
    persistence = V4PersistenceWriter(
        cfg.db_path,
        queue_capacity=cfg.critical_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        checkpoint_on_close=False,
    )
    persistence.start()
    integrity_worker = V4ReadWorker(
        cfg.db_path,
        worker_name="test-v4-audit-integrity-reader",
        worker_kind="READ_INTEGRITY",
        queue_capacity=cfg.reporting_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        default_timeout_s=cfg.reporting_worker_timeout_s,
    ).start()
    report_worker = V4ReadWorker(
        cfg.db_path,
        worker_name="test-v4-audit-report-reader",
        worker_kind="READ_REPORT",
        queue_capacity=cfg.reporting_queue_capacity,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        default_timeout_s=cfg.reporting_worker_timeout_s,
    ).start()
    runtime_io_worker = V4RuntimeIOWorker(
        queue_capacity=cfg.reporting_queue_capacity,
        default_timeout_s=cfg.reporting_worker_timeout_s,
    ).start()
    engine.persistence = persistence
    engine.integrity_worker = integrity_worker
    engine.report_worker = report_worker
    engine.runtime_io_worker = runtime_io_worker
    engine._process_ownership_cache = {
        "process_ownership_valid": True, "exact_v4_processes": 1,
        "owned_v4_processes": 1, "orphan_processes": 0,
    }
    asyncio.run(engine._record_session())
    try:
        yield SimpleNamespace(
            engine=engine, cfg=cfg, runtime=runtime,
            integrity_worker=integrity_worker, report_worker=report_worker,
            runtime_io_worker=runtime_io_worker, persistence=persistence,
            root=tmp_path,
        )
    finally:
        integrity_worker.stop(timeout_s=5.0)
        report_worker.stop(timeout_s=5.0)
        runtime_io_worker.stop(timeout_s=5.0)
        persistence.close(timeout_s=5.0)
        runtime.release()


def _fresh_db(tmp_path: Path, name: str = "integrity-audit.db") -> Path:
    path = tmp_path / name
    store = V4Store(path)
    store.close()
    return path


def _bulk_db(tmp_path: Path, rows: int = 5_000) -> Path:
    """A store with one table large enough that a whole-table scan is not a
    bound: the exact shape that broke the previous chunking."""

    path = _fresh_db(tmp_path, "integrity-bulk.db")
    writer = sqlite3.connect(path)
    writer.execute("CREATE TABLE bulk_probe (x INTEGER, payload TEXT)")
    writer.executemany(
        "INSERT INTO bulk_probe (x, payload) VALUES (?, ?)",
        [(i, "p" * 64) for i in range(rows)],
    )
    writer.commit()
    writer.close()
    return path


def _run_cycle(store: V4ReadOnlyStore, limit: int = 2_000) -> dict:
    for _ in range(limit):
        result = store.integrity_check_chunked()
        if result["cycle_complete"]:
            return result
    raise AssertionError("bounded live scan never completed a cycle")


# ---------------------------------------------------------------------------
# 1-6. The live check is bounded, resumable and leaks nothing.
# ---------------------------------------------------------------------------


def test_live_chunk_cost_is_a_row_count_not_a_table(tmp_path):
    """No live read transaction scales with total table size."""

    path = _bulk_db(tmp_path, rows=20_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        plan = store._live_integrity_plan()
        unit = next(u for u in plan["units"]
                    if u["kind"] == "table" and u["name"] == "bulk_probe")
        rows_in_table = store.query_one(
            "SELECT count(*) AS c FROM bulk_probe")["c"]
        step = store._live_integrity_table_unit(
            unit, after_rowid=0, max_rows=500)
    finally:
        store.close()
    assert rows_in_table == 20_000
    # 40x the rows, same bounded chunk.
    assert step["rows_read"] == 500
    assert step["exhausted"] is False


def test_live_chunks_release_their_cursor_and_snapshot(tmp_path):
    path = _bulk_db(tmp_path, rows=4_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        result = _run_cycle(store)
        assert store.connection.in_transaction is False
    finally:
        store.close()
    view = READER_DIAGNOSTICS.snapshot()
    assert view["active_reader_count"] == 0
    assert view["counters"]["statements_started"] == (
        view["counters"]["statements_completed"])
    assert result["chunks_over_bound"] == 0


def test_live_progress_resumes_without_duplication_or_gaps(tmp_path):
    path = _bulk_db(tmp_path, rows=3_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        plan = store._live_integrity_plan()
        unit = next(u for u in plan["units"]
                    if u["kind"] == "table" and u["name"] == "bulk_probe")
        after = 0
        total = 0
        boundaries = []
        while True:
            step = store._live_integrity_table_unit(
                unit, after_rowid=after, max_rows=211)
            total += step["rows_read"]
            boundaries.append((after, step["last_rowid"]))
            after = step["last_rowid"]
            if step["exhausted"]:
                break
    finally:
        store.close()
    assert total == 3_000                                # no row read twice
    assert [start for start, _ in boundaries][1:] == [
        end for _, end in boundaries][:-1]               # no gap between windows


def test_live_scan_exception_releases_every_registration(tmp_path):
    path = _bulk_db(tmp_path, rows=1_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    real_connection = store._conn

    class FailingConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def execute(self, sql, *args):
            if "bulk_probe" in sql and "ORDER BY rowid" in sql:
                raise sqlite3.OperationalError("interrupted")
            return self._wrapped.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    store._conn = FailingConnection(real_connection)
    try:
        with pytest.raises(sqlite3.OperationalError):
            for _ in range(2_000):
                store.integrity_check_chunked()
    finally:
        store._conn = real_connection
        store.close()
    view = READER_DIAGNOSTICS.snapshot()
    assert view["active_reader_count"] == 0
    assert view["counters"]["errors"] >= 1
    assert view["counters"]["statements_started"] == (
        view["counters"]["statements_completed"])


def test_live_scan_adapts_chunk_size_per_unit(tmp_path):
    """A cheap unit's calibration must not be inherited by an expensive one.

    Carrying one global row count across units is what produced the single
    1,594 ms chunk measured against a 1,000 ms bound on the production store.
    """

    path = _bulk_db(tmp_path, rows=8_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        _run_cycle(store)
        state = store.live_integrity_state()
        sizes = dict(state["unit_rows"])
    finally:
        store.close()
    assert sizes, "no unit was calibrated"
    assert all(
        store.LIVE_INTEGRITY_MIN_ROWS <= value <= store.LIVE_INTEGRITY_MAX_ROWS
        for value in sizes.values()
    )
    # Per-unit, not one shared number.
    assert len(sizes) > 1


def test_live_check_is_never_labelled_a_full_audit(tmp_path):
    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        result = store.integrity_check_chunked()
    finally:
        store.close()
    assert result["scope"] == "live_bounded_health_check"
    assert result["bounded"] is True
    assert "full" not in result["scope"]


# ---------------------------------------------------------------------------
# 7-14. The snapshot copy and the audit that consumes it.
# ---------------------------------------------------------------------------


def test_snapshot_backup_copies_bounded_pages_per_step(tmp_path):
    path = _bulk_db(tmp_path, rows=40_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=8, max_seconds=60)
    finally:
        store.close()
    assert manifest["status"] == "COMPLETED"
    assert manifest["completed"] is True
    assert manifest["pages_per_step"] == 8
    # Bounded steps means many of them, not one big copy.
    assert manifest["steps"] >= manifest["page_count"] // 8
    assert manifest["pages_copied"] == manifest["page_count"]


def test_snapshot_backup_does_not_hold_one_read_lock_for_the_copy(tmp_path):
    """The source must be readable by another connection *during* the copy.

    A copy that held one read transaction for its duration is precisely the
    defect being removed; this proves the read state is released between steps
    by writing to the source from another connection mid-copy and observing
    that the write is not blocked.
    """

    path = _bulk_db(tmp_path, rows=30_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    writer = sqlite3.connect(path, isolation_level=None, timeout=5.0)
    writer.execute("PRAGMA busy_timeout=5000")
    observed = {"writes": 0, "blocked": 0}

    def sink(_stats):
        if observed["writes"] >= 5:
            return
        try:
            writer.execute(
                "INSERT INTO bulk_probe (x, payload) VALUES (?, ?)", (-1, "x"))
            observed["writes"] += 1
        except sqlite3.OperationalError:
            observed["blocked"] += 1

    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=4,
            max_seconds=60, max_restarts=10_000, progress_sink=sink)
    finally:
        writer.close()
        store.close()
    assert observed["writes"] >= 5      # the source stayed writable throughout
    assert observed["blocked"] == 0     # nothing waited on a held read lock
    # Those writes are external, so SQLite rewinds the copy -- the documented
    # behaviour this design accounts for rather than ignores.
    assert manifest["non_progress_steps"] >= 1


def test_snapshot_backup_defers_when_the_source_churns(tmp_path):
    path = _bulk_db(tmp_path, rows=30_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    writer = sqlite3.connect(path, isolation_level=None, timeout=5.0)

    def sink(_stats):
        writer.execute(
            "INSERT INTO bulk_probe (x, payload) VALUES (?, ?)", (-2, "y"))

    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=4,
            max_seconds=60, max_restarts=2, progress_sink=sink)
    finally:
        writer.close()
        store.close()
    assert manifest["status"] == "DEFERRED_SOURCE_CHURN"
    assert manifest["completed"] is False
    assert manifest["path"] == ""
    # A partial image is never published, and never left behind.
    assert not (tmp_path / "audit" / "snap.db").exists()
    assert not (tmp_path / "audit" / "snap.db.partial").exists()


def test_snapshot_backup_cancels_and_removes_its_partial(tmp_path):
    path = _bulk_db(tmp_path, rows=40_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    cancel = threading.Event()
    steps = {"n": 0}

    def should_cancel():
        steps["n"] += 1
        return steps["n"] > 3

    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=2,
            max_seconds=60, should_cancel=should_cancel)
    finally:
        store.close()
    assert cancel.is_set() is False
    assert manifest["status"] == "CANCELLED"
    assert manifest["completed"] is False
    assert not (tmp_path / "audit" / "snap.db").exists()
    assert not (tmp_path / "audit" / "snap.db.partial").exists()


def test_snapshot_backup_honours_its_deadline(tmp_path):
    path = _bulk_db(tmp_path, rows=40_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)

    def slow(_stats):
        time.sleep(0.02)

    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=1,
            max_seconds=0.05, progress_sink=slow)
    finally:
        store.close()
    assert manifest["status"] == "DEADLINE_EXCEEDED"
    assert manifest["completed"] is False
    assert not (tmp_path / "audit" / "snap.db").exists()


def test_snapshot_backup_refuses_to_target_the_live_database(tmp_path):
    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        with pytest.raises(ValueError):
            store.snapshot_backup(path)
        # The live database is untouched by the refusal.
        assert path.exists() and path.stat().st_size > 0
    finally:
        store.close()


def test_full_audit_runs_only_on_a_completed_snapshot(tmp_path):
    path = _bulk_db(tmp_path, rows=5_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=64)
    finally:
        store.close()
    assert manifest["completed"] is True

    verdict = audit_snapshot_database(
        manifest["path"], expected_identity=manifest["source_identity"])
    assert verdict["status"] == "COMPLETED_OK"
    assert verdict["ok"] is True
    assert verdict["integrity"] == "ok"
    assert verdict["quick_check"] == "ok"
    assert verdict["identity_matches"] is True
    assert verdict["foreign_key_violations"] == []
    assert verdict["integrity_check_ms"] >= 0.0

    missing = audit_snapshot_database(tmp_path / "audit" / "absent.db")
    assert missing["status"] == "UNAVAILABLE"
    assert missing["ok"] is None


def test_full_audit_fails_closed_on_a_corrupt_snapshot(tmp_path):
    path = _bulk_db(tmp_path, rows=5_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=64)
    finally:
        store.close()
    snapshot = Path(manifest["path"])
    # Corrupt a page well past the header so the file still opens.
    with open(snapshot, "r+b") as handle:
        handle.seek(4096 * 6)
        handle.write(b"\xde\xad\xbe\xef" * 256)

    verdict = audit_snapshot_database(snapshot)
    assert verdict["ok"] is False
    assert verdict["status"] in {"SNAPSHOT_CORRUPT", "COMPLETED_FAILED", "FAILED"}
    assert verdict["failure_reason"]


def test_full_audit_rejects_a_snapshot_of_another_database(tmp_path):
    path = _bulk_db(tmp_path, rows=2_000)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        manifest = store.snapshot_backup(
            tmp_path / "audit" / "snap.db", pages_per_step=64)
    finally:
        store.close()
    verdict = audit_snapshot_database(
        manifest["path"],
        expected_identity={**manifest["source_identity"], "user_version": 99},
    )
    assert verdict["status"] == "IDENTITY_MISMATCH"
    assert verdict["ok"] is False
    assert verdict["identity_matches"] is False


# ---------------------------------------------------------------------------
# 15-20. Engine wiring: separation, scheduling, cleanup, and the live database
#        never seeing a full scan again.
# ---------------------------------------------------------------------------


def test_worker_runs_the_live_scan_without_a_long_reader(tmp_path):
    from poly_alpha_sniper.lite_frequency_v4.engine import (
        _run_chunked_integrity,
    )

    path = _bulk_db(tmp_path, rows=20_000)
    worker = V4ReadWorker(path, worker_name="audit-integrity-worker").start()
    try:
        result = worker.run_report_sync(
            _run_chunked_integrity, name="sqlite_integrity_check")
    finally:
        worker.stop()
    view = READER_DIAGNOSTICS.snapshot()
    key = "statement:audit-integrity-worker:sqlite_integrity_check"
    assert result["bounded"] is True
    assert view["ops"][key]["max_ms"] <= result["chunk_bound_ms"]
    assert view["active_reader_count"] == 0


def test_sweep_removes_partials_and_respects_retention(tmp_path):
    from poly_alpha_sniper.lite_frequency_v4.engine import (
        _sweep_audit_snapshot_dir,
    )

    directory = tmp_path / "integrity_audit"
    directory.mkdir()
    for name in (
        "full_audit_snapshot.db",
        "full_audit_snapshot.db.partial",
        "full_audit_snapshot.db-wal",
        "full_audit_snapshot.db-shm",
    ):
        (directory / name).write_bytes(b"x")

    result = _sweep_audit_snapshot_dir(directory, 0)
    assert sorted(result["removed"]) == sorted([
        "full_audit_snapshot.db", "full_audit_snapshot.db.partial",
        "full_audit_snapshot.db-wal", "full_audit_snapshot.db-shm",
    ])
    assert list(directory.iterdir()) == []

    # With retention, the complete image survives but partials never do.
    (directory / "full_audit_snapshot.db").write_bytes(b"x")
    (directory / "full_audit_snapshot.db.partial").write_bytes(b"x")
    result = _sweep_audit_snapshot_dir(directory, 1)
    assert result["kept"] == ["full_audit_snapshot.db"]
    assert result["removed"] == ["full_audit_snapshot.db.partial"]

    # A missing directory is not an error.
    assert _sweep_audit_snapshot_dir(tmp_path / "absent", 0) == {
        "removed": [], "kept": []}


def test_live_and_full_statuses_are_reported_separately(engine_harness):
    engine = engine_harness.engine


    asyncio.run(engine._run_integrity_check())
    state = engine._runtime_state("RUNNING")

    # Live: bounded health check with published coverage.
    assert state["live_integrity_health"] == "OK"
    assert state["live_integrity_scope"] == "live_bounded_health_check"
    assert state["live_integrity_bounded"] is True
    assert state["live_integrity_chunks_over_bound"] == 0
    assert state["live_integrity_max_chunk_ms"] <= state[
        "live_integrity_chunk_bound_ms"]
    assert set(state["live_integrity_progress"]) == {
        "unit_index", "units_total", "progress_pct", "rows_scanned_last_pass"}

    # Full: never inherits the live verdict.  It has not run here, so it fails
    # closed as UNKNOWN rather than reporting the live check's "ok".
    assert state["full_audit_status"] == "UNKNOWN"
    assert state["full_audit_ok"] is None
    assert state["full_audit_completed_ms"] is None
    assert state["full_audit_age_ms"] is None
    assert state["full_audit_runs_on_live_database"] is False


def test_full_audit_publishes_snapshot_progress_and_cleans_up(engine_harness):


    engine = engine_harness.engine
    assert asyncio.run(engine._run_full_integrity_audit()) is True
    state = engine._runtime_state("RUNNING")
    assert state["full_audit_status"] == "COMPLETED_OK"
    assert state["full_audit_ok"] is True
    assert state["full_audit_snapshot_total_pages"] > 0
    assert state["full_audit_snapshot_progress_pages"] == state[
        "full_audit_snapshot_total_pages"]
    assert state["full_audit_snapshot_progress_pct"] == 100.0
    assert state["full_audit_snapshot_steps"] >= 1
    assert state["full_audit_snapshot_max_step_ms"] >= 0.0
    assert state["full_audit_age_ms"] is not None
    # Cleanup: no snapshot, no partial, no sidecar left behind -- the small
    # verdict manifest is the only survivor, by design.
    from poly_alpha_sniper.lite_frequency_v4.engine import AUDIT_MANIFEST_NAME

    directory = engine._audit_snapshot_dir()
    survivors = sorted(p.name for p in directory.iterdir())
    assert survivors == [AUDIT_MANIFEST_NAME]


def test_stale_full_audit_is_reported_stale_not_ok(engine_harness):


    engine = engine_harness.engine
    assert asyncio.run(engine._run_full_integrity_audit()) is True
    assert engine._runtime_state("RUNNING")["full_audit_status"] == "COMPLETED_OK"

    # Age it past the configured staleness policy.
    engine._full_audit_completed_ms -= (
        int(engine.cfg.full_integrity_audit_max_age_ms) + 1_000)
    state = engine._runtime_state("RUNNING")
    assert state["full_audit_status"] == "STALE"
    # The verdict itself is not rewritten -- only its currency is judged.
    assert state["full_audit_ok"] is True
    assert state["full_audit_age_ms"] > state["full_audit_max_age_ms"]


def test_startup_snapshot_is_skipped_when_not_due(engine_harness):


    from poly_alpha_sniper.lite_frequency_v4.engine import (
        AUDIT_MANIFEST_NAME,
        _write_audit_manifest,
    )

    engine = engine_harness.engine
    directory = engine._audit_snapshot_dir()
    recent = int(time.time() * 1000)
    assert _write_audit_manifest(directory, {
        "status": "COMPLETED_OK", "ok": True, "completed_ms": recent,
        "source_as_of_ms": recent, "failure_reason": "",
    }) is True
    taken: list[str] = []

    async def spy(*, reason):
        taken.append(reason)
        return {"completed": True}

    engine._run_full_audit_snapshot = spy
    assert asyncio.run(engine._maybe_snapshot_for_full_audit()) is False
    assert taken == []
    # The cadence was seeded from the manifest, not from process-local state.
    assert engine._full_audit_completed_ms == recent
    assert engine._full_audit_status == "COMPLETED_OK"
    # The sweep does not take the verdict with the image.
    assert (directory / AUDIT_MANIFEST_NAME).exists()

    # Due again once the interval has passed.
    assert _write_audit_manifest(directory, {
        "status": "COMPLETED_OK", "ok": True,
        "completed_ms": recent - int(
            engine.cfg.full_integrity_audit_interval_ms) - 1_000,
        "source_as_of_ms": recent, "failure_reason": "",
    }) is True
    assert asyncio.run(engine._maybe_snapshot_for_full_audit()) is True
    assert taken == ["startup_quiescent_window"]


def test_audit_manifest_carries_the_verdict_across_a_restart(engine_harness):
    from poly_alpha_sniper.lite_frequency_v4.engine import _read_audit_manifest

    engine = engine_harness.engine
    assert asyncio.run(engine._run_full_integrity_audit()) is True
    directory = engine._audit_snapshot_dir()
    manifest = _read_audit_manifest(directory)
    assert manifest["status"] == "COMPLETED_OK"
    assert manifest["ok"] is True
    assert manifest["completed_ms"] > 0
    assert manifest["integrity"] == "ok"
    assert manifest["foreign_key_violations"] == 0
    assert manifest["scope"] == "full_integrity_check_on_completed_snapshot"
    assert manifest["snapshot"]["restarts"] == 0
    assert manifest["snapshot"]["page_count"] > 0
    assert manifest["source_identity"]["user_version"] == 5

    # A fresh engine state reading that manifest is not due for another audit.
    engine._full_audit_completed_ms = 0
    engine._full_audit_status = "UNKNOWN"
    assert asyncio.run(engine._load_audit_manifest())
    assert engine._full_audit_due() is False
    assert engine._full_audit_status == "COMPLETED_OK"


def test_snapshot_quiesces_only_the_diagnostic_sampler(engine_harness):
    """The one writer that runs in the startup window is suppressed, and only
    for the copy.  Without this the 1 Hz ``persistence_worker_samples`` row
    restarts the backup forever; with it, nothing else is affected and the
    window is always released."""

    engine = engine_harness.engine
    persistence = engine_harness.persistence
    seen: list[bool] = []
    real = engine.integrity_worker.run_report

    async def watching(operation, *args, **kwargs):
        if kwargs.get("name") == "full_audit_snapshot":
            seen.append(persistence.quiescent_window_active)
        return await real(operation, *args, **kwargs)

    engine.integrity_worker.run_report = watching
    try:
        assert asyncio.run(engine._run_full_integrity_audit()) is True
    finally:
        engine.integrity_worker.run_report = real
    assert seen == [True]                              # held for the copy
    assert persistence.quiescent_window_active is False  # and always released

    # Critical evidence is untouched by the window: the writer keeps its
    # committed-command accounting exactly as before.
    metrics = persistence.metrics()
    assert metrics["state"] == "RUNNING"
    assert int(metrics.get("commands_failed") or 0) == 0


def test_quiescent_window_is_released_even_when_the_copy_fails(engine_harness):
    engine = engine_harness.engine
    persistence = engine_harness.persistence

    async def exploding(*_args, **_kwargs):
        raise RuntimeError("copy blew up")

    engine.integrity_worker.run_report = exploding
    manifest = asyncio.run(
        engine._run_full_audit_snapshot(reason="test"))
    assert manifest["completed"] is False
    assert engine._full_audit_status == "SNAPSHOT_FAILED"
    assert persistence.quiescent_window_active is False


def test_a_collided_verdict_keeps_its_snapshot_and_retries(engine_harness):
    """A submission that never reached the worker must not destroy the image.

    Measured in the first gate attempt: the integrity worker's queue held one
    job, the live scan now runs every 15 s, and the verdict's submission was
    rejected with ``V4WorkerQueueFull``.  The failure path then swept a
    completed 9.97 GB snapshot that had taken 62 s to produce, and published a
    manifest whose timestamp satisfied the six-hour cadence -- so nothing was
    audited and nothing would be for six hours.
    """
    from poly_alpha_sniper.lite_frequency_v4.engine import _read_audit_manifest
    from poly_alpha_sniper.lite_frequency_v4.workers import V4WorkerQueueFull

    engine = engine_harness.engine
    manifest = asyncio.run(engine._run_full_audit_snapshot(reason="test"))
    assert manifest["completed"] is True
    assert engine._full_audit_snapshot_ready is True
    snapshot = engine._audit_snapshot_path()
    assert snapshot.exists()

    real = engine.integrity_worker.run_report

    async def rejecting(*_args, **_kwargs):
        raise V4WorkerQueueFull("integrity worker queue is full (1)")

    engine.integrity_worker.run_report = rejecting
    try:
        assert asyncio.run(engine._run_full_audit_verdict()) is False
    finally:
        engine.integrity_worker.run_report = real

    # The image survives, the audit is still pending, and no verdict was faked.
    assert snapshot.exists()
    assert engine._full_audit_snapshot_ready is True
    assert engine._full_audit_status == "SNAPSHOT_READY"
    assert engine._full_integrity_inflight is False
    assert engine._full_integrity_runs == 0
    assert engine._full_audit_completed_ms == 0
    assert _read_audit_manifest(engine._audit_snapshot_dir()) == {}

    # And the retry succeeds against the very same snapshot.
    assert asyncio.run(engine._run_full_audit_verdict()) is True
    assert engine._full_audit_status == "COMPLETED_OK"
    assert engine._full_audit_completed_ms > 0
    assert not snapshot.exists()


def test_the_two_scans_never_share_the_integrity_worker(engine_harness):
    engine = engine_harness.engine
    engine._full_audit_snapshot_ready = True

    engine._integrity_inflight = True
    assert asyncio.run(engine._run_full_audit_verdict()) is False
    assert asyncio.run(
        engine._run_full_audit_snapshot(reason="t"))["completed"] is False
    engine._integrity_inflight = False

    engine._full_integrity_inflight = True
    assert asyncio.run(engine._run_full_audit_verdict()) is False
    engine._full_integrity_inflight = False


def test_a_failed_audit_does_not_satisfy_the_cadence(engine_harness):
    from poly_alpha_sniper.lite_frequency_v4.engine import _read_audit_manifest

    engine = engine_harness.engine
    manifest = asyncio.run(engine._run_full_audit_snapshot(reason="test"))
    assert manifest["completed"] is True

    async def exploding(*_args, **_kwargs):
        raise RuntimeError("audit worker blew up")

    engine.integrity_worker.run_report = exploding
    assert asyncio.run(engine._run_full_audit_verdict()) is True
    assert engine._full_audit_status == "FAILED"
    assert engine._full_audit_completed_ms == 0
    published = _read_audit_manifest(engine._audit_snapshot_dir())
    # Published (the failure is evidence) but with no completion timestamp, so
    # the next launch is still due for an audit.
    assert published == {} or published.get("completed_ms") == 0
    assert engine._full_audit_due() is True


def test_a_damaged_audit_manifest_makes_an_audit_due(engine_harness):
    from poly_alpha_sniper.lite_frequency_v4.engine import (
        AUDIT_MANIFEST_NAME,
        _read_audit_manifest,
    )

    engine = engine_harness.engine
    directory = engine._audit_snapshot_dir()
    directory.mkdir(parents=True, exist_ok=True)
    (directory / AUDIT_MANIFEST_NAME).write_text("{not json", encoding="utf-8")
    assert _read_audit_manifest(directory) == {}
    assert asyncio.run(engine._load_audit_manifest()) == {}
    assert engine._full_audit_due() is True
