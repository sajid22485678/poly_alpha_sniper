"""Reader-lifecycle contract for WAL reclamation.

In WAL mode a checkpoint can only reset the log at an instant when no
connection holds a read-mark, and it can only backfill up to the oldest
read-mark.  A single long read transaction therefore stops the WAL shrinking
for its whole duration, no matter how often the policy checkpoints.

The controlled reproduction at 9ac481e measured exactly that, and one more
thing: the reclamation escalation could not fire at all.  Both failures are
pinned here.

Measured inputs (28 min live, 9.06 GB database, 835 observer samples):

- ``consecutive_no_progress_passive`` reached **112** with **0** escalations
  and **0** reclaimed bytes; the WAL grew to **1.28 GB** and reset exactly
  once, on an accidental reader gap.
- the periodic whole-database ``quick_check`` was a single read statement of
  **130.2 s**; per-table it is 44 statements with a **8.4 s** maximum.
- of 258 zero-progress PASSIVE checkpoints, 159 had a reader live at their
  start: 134 dashboard_export, 21 sqlite_integrity_check, 4 operational.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
import threading

import pytest

from poly_alpha_sniper.lite_frequency_v4.maintenance import (
    EMERGENCY_WAL_REASON,
    LIVE_RECLAIM_REASON,
    CheckpointMode,
    MaintenancePolicy,
    MaintenanceSnapshot,
    decide_checkpoint,
)
from poly_alpha_sniper.lite_frequency_v4.reader_diag import READER_DIAGNOSTICS
from poly_alpha_sniper.lite_frequency_v4.store import V4ReadOnlyStore, V4Store
from poly_alpha_sniper.lite_frequency_v4.workers import V4ReadWorker


@pytest.fixture(autouse=True)
def _clean_registry():
    READER_DIAGNOSTICS.reset()
    yield
    READER_DIAGNOSTICS.reset()


def _fresh_db(tmp_path: Path, name: str = "reader-lifecycle.db") -> Path:
    path = tmp_path / name
    store = V4Store(path)
    store.close()
    return path


def _snapshot(**overrides):
    base = dict(
        now_ms=1_000_000, wal_bytes=300_000_000,
        critical_queue_depth=0, telemetry_queue_depth=0,
        runtime_active=True, runtime_health="HEALTHY", writer_healthy=True,
        open_positions=0, active_readers=0, long_reader_count=0,
        critical_commit_p95_ms=15.0, time_to_window_boundary_ms=120_000,
        consecutive_no_progress_passive=5,
    )
    base.update(overrides)
    return MaintenanceSnapshot(**base)


# ---------------------------------------------------------------------------
# 1. The escalation must be reachable at the pressure cadence.
# ---------------------------------------------------------------------------


def test_pressure_cadence_backfill_no_longer_starves_the_escalation():
    """The measured failure: 112 no-progress passes, 0 escalations, 1.28 GB WAL.

    Under WAL pressure the cheap PASSIVE backfill runs every ~5 s and refreshes
    ``last_checkpoint_attempt_ts_ms``.  Pacing the escalation against that
    timestamp meant ``now - last_attempt`` was essentially always below the
    60 s routine interval, so the decision returned PASSIVE before the
    reclamation branch was ever evaluated.  The escalation was structurally
    unreachable exactly while it was armed.
    """

    policy = MaintenancePolicy()
    snapshot = _snapshot(
        # A backfill ran 5 s ago, as it does continuously under pressure...
        last_checkpoint_attempt_ts_ms=1_000_000 - 5_000,
        # ...but no reclamation has been attempted for well over the interval.
        last_escalation_attempt_ts_ms=1_000_000 - 120_000,
    )
    decision = decide_checkpoint(snapshot, policy)
    assert decision.mode is CheckpointMode.TRUNCATE
    assert decision.reason == LIVE_RECLAIM_REASON


def test_escalation_still_keeps_its_own_routine_interval():
    """It is a cadence change, not a cadence removal."""

    policy = MaintenancePolicy()
    snapshot = _snapshot(
        last_checkpoint_attempt_ts_ms=1_000_000 - 5_000,
        last_escalation_attempt_ts_ms=(
            1_000_000 - policy.checkpoint_min_interval_ms + 1),
    )
    decision = decide_checkpoint(snapshot, policy)
    assert decision.mode is CheckpointMode.PASSIVE
    assert decision.reason == EMERGENCY_WAL_REASON


def test_escalation_cadence_counts_attempts_not_successes():
    """A BUSY or deferred reclaim consumes its slot, so it cannot spin.

    ``last_escalation_attempt_ts_ms`` is stamped for every decided
    RESTART/TRUNCATE regardless of outcome; a reclaim that returned BUSY
    therefore waits a full interval rather than retrying every 5 s.
    """

    policy = MaintenancePolicy()
    snapshot = _snapshot(
        last_checkpoint_attempt_ts_ms=1_000_000 - 6_000,
        last_escalation_attempt_ts_ms=1_000_000 - 30_000,
    )
    assert decide_checkpoint(snapshot, policy).mode is CheckpointMode.PASSIVE


def test_engine_arms_the_escalation_clock_on_its_first_maintenance_pass():
    """The bootstrap deadlock: an unarmed clock can never become due.

    ``_escalation_due`` falls back to the last checkpoint attempt of any mode
    when no escalation has been recorded.  Under WAL pressure that timestamp
    is refreshed every ~5 s by the backfill, so an engine that left the clock
    unset would need an escalation to have already happened before one could
    happen.  Measured: a full 29-minute run at 5ac88c0 produced 56/56 PASSIVE
    samples, 243 zero-progress checkpoints, zero escalations and a 1.35 GB
    WAL.  The clock is therefore armed on the first maintenance pass.
    """

    import asyncio
    import types

    from poly_alpha_sniper.lite_frequency_v4 import engine as engine_module

    engine = object.__new__(engine_module.FrequencyV4Engine)
    engine._maintenance_inflight = False
    engine.maintenance_worker = None
    engine._last_escalation_attempt_ts_ms = None
    engine._maintenance_runs = 0
    engine._last_maintenance_duration_ms = 0.0

    # maintenance_worker is None, so the pass returns before doing any work --
    # arming must happen regardless, on the very first pass.
    assert engine._last_escalation_attempt_ts_ms is None
    engine._maintenance_inflight = False

    # Re-enter with a worker present but every dependency stubbed to a no-op,
    # so only the arming step is exercised.
    calls: list[int] = []

    async def run_maintenance(*_a, **_k):
        raise RuntimeError("stop after arming")

    engine.maintenance_worker = types.SimpleNamespace(
        run_maintenance=run_maintenance)
    engine.read_worker = None
    engine.report_worker = None
    engine.integrity_worker = None
    engine.telemetry = None
    engine.model_health = types.SimpleNamespace(needs_resample=lambda: False)
    engine._writer_health = lambda: {"state": "HEALTHY", "queue_depth": 0}
    engine._runtime_state_name = lambda _c: "RUNNING"
    engine._open_positions_count = 0
    engine._wal_size_cache = 0
    engine._loop_lag_ms = 0.0
    engine._last_integrity_ok = True
    engine._checkpoint_state = {}
    engine._consecutive_no_progress_passive = 0
    engine._last_error = ""
    engine.cfg = engine_module.FrequencyV4Config()

    asyncio.run(engine._run_maintenance_pass())
    assert isinstance(engine._last_escalation_attempt_ts_ms, int)
    assert engine._last_escalation_attempt_ts_ms > 0
    calls.append(engine._last_escalation_attempt_ts_ms)

    # Armed once, then owned by the checkpoint result -- not re-armed every
    # pass, which would reset the cadence and starve the escalation again.
    engine._maintenance_inflight = False
    asyncio.run(engine._run_maintenance_pass())
    assert engine._last_escalation_attempt_ts_ms == calls[0]


def test_absent_escalation_timestamp_falls_back_to_prior_behaviour():
    """A caller that does not track escalations keeps the historical cadence."""

    policy = MaintenancePolicy()
    inside = _snapshot(
        last_checkpoint_attempt_ts_ms=1_000_000 - 5_000,
        last_escalation_attempt_ts_ms=None,
    )
    assert decide_checkpoint(inside, policy).mode is CheckpointMode.PASSIVE
    outside = _snapshot(
        last_checkpoint_attempt_ts_ms=1_000_000 - 120_000,
        last_escalation_attempt_ts_ms=None,
    )
    assert decide_checkpoint(outside, policy).mode is CheckpointMode.TRUNCATE


def test_pressure_floor_still_rate_limits_everything():
    """Below the pressure floor no checkpoint of any mode runs."""

    policy = MaintenancePolicy()
    snapshot = _snapshot(
        last_checkpoint_attempt_ts_ms=(
            1_000_000 - policy.pressure_checkpoint_min_interval_ms + 1),
        last_escalation_attempt_ts_ms=1_000_000 - 600_000,
    )
    decision = decide_checkpoint(snapshot, policy)
    assert decision.should_run is False
    assert decision.reason == "checkpoint_min_interval"


@pytest.mark.parametrize(
    "unsafe",
    [
        {"long_reader_count": 1},
        {"critical_queue_depth": 1},
        {"writer_healthy": False},
        {"integrity_ok": False},
        {"consecutive_no_progress_passive": 0},
        {"wal_bytes": 1_000},
    ],
    ids=["long_reader", "critical_queued", "writer_unhealthy",
         "integrity_not_ok", "no_evidence", "wal_below_restart"],
)
def test_reachable_escalation_still_honours_every_safety_gate(unsafe):
    """Making the escalation reachable must not make it unconditional."""

    policy = MaintenancePolicy()
    snapshot = _snapshot(
        last_checkpoint_attempt_ts_ms=1_000_000 - 5_000,
        last_escalation_attempt_ts_ms=1_000_000 - 600_000,
        **unsafe,
    )
    decision = decide_checkpoint(snapshot, policy)
    assert decision.reason != LIVE_RECLAIM_REASON


def test_a_long_reader_defers_the_reclaim_it_would_block():
    """A 130 s integrity scan holds the read-mark past any bounded lock wait."""

    policy = MaintenancePolicy()
    blocked = _snapshot(
        last_checkpoint_attempt_ts_ms=1_000_000 - 5_000,
        last_escalation_attempt_ts_ms=1_000_000 - 600_000,
        long_reader_count=1,
    )
    assert decide_checkpoint(blocked, policy).mode is CheckpointMode.PASSIVE
    released = _snapshot(
        last_checkpoint_attempt_ts_ms=1_000_000 - 5_000,
        last_escalation_attempt_ts_ms=1_000_000 - 600_000,
        long_reader_count=0,
    )
    assert decide_checkpoint(released, policy).mode is CheckpointMode.TRUNCATE


# ---------------------------------------------------------------------------
# 2. The chunked integrity scan releases its snapshot per table.
# ---------------------------------------------------------------------------


def test_chunked_scan_reports_ok_and_chunk_accounting(tmp_path):
    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        result = store.integrity_check_chunked()
    finally:
        store.close()
    assert result["integrity"] == "ok"
    assert result["foreign_key_violations"] == []
    assert result["chunked"] is True
    assert result["chunks"] > 10
    assert result["max_chunk_ms"] >= 0.0


def test_chunked_scan_agrees_with_the_monolithic_scan(tmp_path):
    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        monolithic = store.integrity_check(quick=True)
        chunked = store.integrity_check_chunked()
    finally:
        store.close()
    assert monolithic["integrity"] == chunked["integrity"] == "ok"
    assert monolithic["foreign_key_violations"] == (
        chunked["foreign_key_violations"])


def test_bounded_scan_releases_every_statement_it_starts(tmp_path):
    """Each chunk is its own read transaction -- that is the whole point.

    One statement per chunk means the read-mark is released at every chunk
    boundary; a single statement covering one whole table is what pinned the
    WAL for over 140 s on the 9.29 GB store.
    """

    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        result = store.integrity_check_chunked()
    finally:
        store.close()
    view = READER_DIAGNOSTICS.snapshot()
    # Every table/foreign-key chunk is exactly one statement, so the statement
    # count is at least the chunk count and every one of them closed.
    assert view["counters"]["statements_started"] >= result["chunks"]
    assert view["counters"]["statements_started"] == (
        view["counters"]["statements_completed"])
    # Nothing left holding a snapshot.
    assert view["active_reader_count"] == 0
    # No read-mark hold exceeded the declared chunk bound.
    assert view["max_statement_ms"] <= result["chunk_bound_ms"]
    assert result["chunks_over_bound"] == 0


def test_bounded_scan_cycle_covers_every_user_table_and_foreign_key(tmp_path):
    """One completed cycle visits every table and every declared foreign key."""

    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        tables = [
            row["name"] for row in store.query(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'")
        ]
        plan = store._live_integrity_plan()
        constraints = sum(
            len(store._foreign_key_constraints(name)) for name in tables)
        # metadata + one unit per table + one unit per declared constraint
        assert len(plan["units"]) == 1 + len(tables) + constraints
        assert {
            unit["name"] for unit in plan["units"] if unit["kind"] == "table"
        } == set(tables)

        for _ in range(500):
            result = store.integrity_check_chunked()
            if result["cycle_complete"]:
                break
        else:  # pragma: no cover - a cycle must terminate
            raise AssertionError("bounded scan never completed a cycle")
    finally:
        store.close()
    assert result["completed_cycles"] == 1
    assert result["last_completed_cycle_ok"] is True
    assert result["scope"] == "live_bounded_health_check"


def test_bounded_scan_reports_a_foreign_key_problem(tmp_path):
    """A bounded chunk's verdict must fail closed, and name the table."""

    path = _fresh_db(tmp_path)

    class FailingStore(V4ReadOnlyStore):
        def _integrity_statement(self, pragma_sql, params=(), *,
                                 budget_ms=None):
            # Match the child table of the constraint, not any table merely
            # named as a parent inside the same statement.
            if "NOT EXISTS" in pragma_sql and 'FROM "entries" c' in pragma_sql:
                # rows_read, last_rowid, violations
                return [(1, 1, 3)]
            return super()._integrity_statement(
                pragma_sql, params, budget_ms=budget_ms)

    store = FailingStore(path, enforce_thread_ownership=False)
    try:
        for _ in range(500):
            result = store.integrity_check_chunked()
            if result["integrity"] != "ok":
                break
        else:  # pragma: no cover - the injected violation must be reported
            raise AssertionError("injected foreign key violation not reported")
    finally:
        store.close()
    assert "entries:" in result["integrity"]
    assert result["foreign_key_violations"]
    assert result["foreign_key_violations"][0]["table"] == "entries"


def test_bounded_scan_reports_a_metadata_problem(tmp_path):
    """Database-level invariants fail closed too, not just row-level ones."""

    path = _fresh_db(tmp_path)

    class FailingStore(V4ReadOnlyStore):
        def _integrity_statement(self, pragma_sql, params=(), *,
                                 budget_ms=None):
            if pragma_sql == "PRAGMA user_version":
                return [(4,)]
            return super()._integrity_statement(
                pragma_sql, params, budget_ms=budget_ms)

    store = FailingStore(path, enforce_thread_ownership=False)
    try:
        result = store.integrity_check_chunked()
    finally:
        store.close()
    assert result["integrity"] != "ok"
    assert "user_version" in result["integrity"]


def test_bounded_scan_chunk_cost_does_not_track_table_size(tmp_path):
    """The bound is a row count, not a table -- the defect being fixed.

    A chunk over a table with 40x the rows must read the same number of rows,
    which is what makes the read-mark hold independent of database size.
    """

    path = _fresh_db(tmp_path)
    writer = sqlite3.connect(path)
    writer.execute("CREATE TABLE small_probe (x INTEGER)")
    writer.execute("CREATE TABLE large_probe (x INTEGER)")
    writer.executemany(
        "INSERT INTO small_probe (x) VALUES (?)", [(i,) for i in range(100)])
    writer.executemany(
        "INSERT INTO large_probe (x) VALUES (?)", [(i,) for i in range(4_000)])
    writer.commit()
    writer.close()

    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        plan = store._live_integrity_plan()
        units = {unit["name"]: unit for unit in plan["units"]
                 if unit["kind"] == "table"}
        small = store._live_integrity_table_unit(
            units["small_probe"], after_rowid=0, max_rows=50)
        large = store._live_integrity_table_unit(
            units["large_probe"], after_rowid=0, max_rows=50)
    finally:
        store.close()
    assert small["rows_read"] == large["rows_read"] == 50
    assert large["exhausted"] is False       # resumable, not truncated silently
    assert large["last_rowid"] == 50         # and it says exactly where to resume


def test_bounded_scan_resumes_without_gaps_or_duplicates(tmp_path):
    """Resumption is exact: every row once, no row skipped."""

    path = _fresh_db(tmp_path)
    writer = sqlite3.connect(path)
    writer.execute("CREATE TABLE walk_probe (x INTEGER)")
    writer.executemany(
        "INSERT INTO walk_probe (x) VALUES (?)", [(i,) for i in range(1_000)])
    # A hole in the rowid sequence must not confuse the cursor.
    writer.execute("DELETE FROM walk_probe WHERE rowid BETWEEN 300 AND 400")
    writer.commit()
    writer.close()

    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        plan = store._live_integrity_plan()
        unit = next(unit for unit in plan["units"]
                    if unit["kind"] == "table" and unit["name"] == "walk_probe")
        expected = store.query_one("SELECT count(*) AS c FROM walk_probe")["c"]
        after = 0
        seen = 0
        windows: list[tuple[int, int]] = []
        while True:
            step = store._live_integrity_table_unit(
                unit, after_rowid=after, max_rows=37)
            seen += step["rows_read"]
            windows.append((after, step["last_rowid"]))
            after = step["last_rowid"]
            if step["exhausted"]:
                break
    finally:
        store.close()
    assert seen == expected                      # every row exactly once
    starts = [start for start, _ in windows]
    ends = [end for _, end in windows]
    assert starts == [0, *ends[:-1]]             # each window resumes where the
    assert all(b >= a for a, b in windows)       # previous one ended: no gaps


def test_chunked_scan_releases_its_snapshot_when_a_chunk_raises(tmp_path):
    """A chunk interrupted mid-scan must not leave its snapshot registered.

    The failure is injected at the connection, below the instrumented
    statement wrapper, so the wrapper's own error path is what is exercised.
    """

    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    real_connection = store._conn

    class FailingConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def execute(self, sql, *args):
            if "book_snapshots" in sql:
                raise sqlite3.OperationalError("interrupted")
            return self._wrapped.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    store._conn = FailingConnection(real_connection)
    try:
        with pytest.raises(sqlite3.OperationalError):
            store.integrity_check_chunked()
    finally:
        store._conn = real_connection
        store.close()
    view = READER_DIAGNOSTICS.snapshot()
    assert view["active_reader_count"] == 0
    assert view["counters"]["errors"] >= 1
    assert view["counters"]["statements_started"] == (
        view["counters"]["statements_completed"])


def test_chunked_scan_leaves_no_open_transaction(tmp_path):
    path = _fresh_db(tmp_path)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        store.integrity_check_chunked()
        assert store.connection.in_transaction is False
    finally:
        store.close()


def test_chunked_scan_quotes_table_names_safely(tmp_path):
    """Identifier quoting is applied even to an adversarial table name."""

    path = _fresh_db(tmp_path)
    writer = sqlite3.connect(path)
    writer.execute('CREATE TABLE "weird ""name"" table" (x INTEGER)')
    writer.commit()
    writer.close()
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        result = store.integrity_check_chunked()
    finally:
        store.close()
    assert result["integrity"] == "ok"


# ---------------------------------------------------------------------------
# 3. Worker-level lifecycle: no snapshot survives a job.
# ---------------------------------------------------------------------------


def test_integrity_job_on_the_worker_releases_between_chunks(tmp_path):
    from poly_alpha_sniper.lite_frequency_v4.engine import (
        _run_chunked_integrity,
    )

    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path, worker_name="diag-integrity-worker").start()
    try:
        result = worker.run_report_sync(
            _run_chunked_integrity, name="sqlite_integrity_check")
    finally:
        worker.stop()
    assert result["integrity"] == "ok"
    assert result["chunked"] is True
    assert result["bounded"] is True
    view = READER_DIAGNOSTICS.snapshot()
    ops = view["ops"]
    key = "statement:diag-integrity-worker:sqlite_integrity_check"
    assert ops[key]["count"] >= result["chunks"]
    # The longest single pin is one chunk, never the whole pass.
    assert ops[key]["max_ms"] <= ops[
        "job:diag-integrity-worker:sqlite_integrity_check"]["max_ms"]
    assert ops[key]["max_ms"] <= result["chunk_bound_ms"]
    assert view["active_reader_count"] == 0


def test_engine_helper_falls_back_for_a_store_without_chunking():
    from poly_alpha_sniper.lite_frequency_v4.engine import (
        _run_chunked_integrity,
    )

    class LegacyStore:
        def integrity_check(self, *, quick=False):
            return {"integrity": "ok", "foreign_key_violations": [],
                    "quick": quick}

    assert _run_chunked_integrity(LegacyStore()) == {
        "integrity": "ok", "foreign_key_violations": [], "quick": True}


def test_worker_stop_leaves_no_open_reader_transaction(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path, worker_name="diag-shutdown-worker").start()
    worker.query_sync("SELECT COUNT(*) AS n FROM entries")
    worker.stop()
    view = READER_DIAGNOSTICS.snapshot()
    assert view["active_reader_count"] == 0
    assert view["active_job_count"] == 0
    assert view["counters"]["jobs_started"] == view["counters"]["jobs_completed"]
    assert view["counters"]["statements_started"] == (
        view["counters"]["statements_completed"])


def test_concurrent_read_workers_do_not_cross_attribute(tmp_path):
    """Two workers reading at once must each own their statements."""

    path = _fresh_db(tmp_path)
    first = V4ReadWorker(path, worker_name="diag-worker-one").start()
    second = V4ReadWorker(path, worker_name="diag-worker-two").start()
    barrier = threading.Barrier(2)
    try:
        def run(worker):
            barrier.wait(timeout=10)
            worker.query_sync("SELECT COUNT(*) AS n FROM entries")

        threads = [threading.Thread(target=run, args=(worker,))
                   for worker in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
    finally:
        first.stop()
        second.stop()
    ops = READER_DIAGNOSTICS.snapshot()["ops"]
    assert ops["statement:diag-worker-one:query"]["count"] == 1
    assert ops["statement:diag-worker-two:query"]["count"] == 1


def test_a_held_reader_is_visible_to_the_policy_as_a_long_reader(tmp_path):
    """Pinned-reader simulation: an open snapshot is observable and blocking.

    A real read transaction is opened and held, the registry reports it as a
    long reader, and the policy refuses the reclamation it would block --
    then permits it once the reader releases.
    """

    path = _fresh_db(tmp_path)
    READER_DIAGNOSTICS.configure(long_reader_threshold_ms=1.0)
    store = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    connection = store.connection
    try:
        token = READER_DIAGNOSTICS.begin_statement(conn_id=id(connection))
        connection.execute("BEGIN")
        connection.execute("SELECT COUNT(*) FROM entries").fetchall()
        assert connection.in_transaction is True
        pinned = READER_DIAGNOSTICS.snapshot()
        assert pinned["active_reader_count"] == 1
        policy = MaintenancePolicy()
        blocked = _snapshot(
            last_escalation_attempt_ts_ms=1_000_000 - 600_000,
            long_reader_count=1)
        assert decide_checkpoint(blocked, policy).reason != LIVE_RECLAIM_REASON

        connection.rollback()
        READER_DIAGNOSTICS.end_statement(token)
        released = READER_DIAGNOSTICS.snapshot()
        assert released["active_reader_count"] == 0
        assert connection.in_transaction is False
        allowed = _snapshot(
            last_escalation_attempt_ts_ms=1_000_000 - 600_000,
            long_reader_count=0)
        assert decide_checkpoint(allowed, policy).reason == LIVE_RECLAIM_REASON
    finally:
        store.close()
