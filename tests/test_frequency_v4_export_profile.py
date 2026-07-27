"""Named-stage export cost profiling for the Frequency V4 dashboard.

These tests pin the profiler's contract, not its numbers: a stage must be
attributable, a slow statement must be blamed on the stage that issued it, the
registry must stay bounded, and none of it may leak SQL text, bound parameters,
or extend a SQLite read transaction.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

import pytest

from lite_frequency_v4.export_profile import (
    EXPORT_PROFILE,
    ExportProfiler,
    normalize_sql,
    row_digest,
    statement_label,
)


@pytest.fixture()
def profiler() -> ExportProfiler:
    return ExportProfiler()


# -- naming -------------------------------------------------------------------


def test_statement_label_is_stable_and_names_the_primary_table():
    sql = "SELECT COUNT(*) FROM event_buckets WHERE bucket_start_ts_ms>=?"
    first = statement_label(sql)
    second = statement_label(sql)
    assert first == second
    assert first.startswith("event_buckets#")


def test_statement_label_ignores_formatting_but_not_meaning():
    tight = "SELECT a FROM entries WHERE x=?"
    loose = "SELECT   a\n   FROM entries\n   WHERE x=?   -- comment\n"
    other = "SELECT b FROM entries WHERE x=?"
    assert statement_label(tight) == statement_label(loose)
    assert statement_label(tight) != statement_label(other)


def test_statement_label_carries_no_sql_text_or_parameters():
    sql = "SELECT secret_column FROM entries WHERE token='super-secret-value'"
    label = statement_label(sql)
    assert "secret" not in label
    assert "super-secret-value" not in label
    assert "select" not in label.lower().split("#")[1]


def test_normalize_sql_collapses_whitespace_and_comments():
    assert normalize_sql("SELECT  a\nFROM t -- note\n") == "select a from t"


# -- stage attribution --------------------------------------------------------


def test_stages_nest_into_dotted_names(profiler: ExportProfiler):
    with profiler.build():
        with profiler.stage("metrics"):
            with profiler.stage("frequency"):
                with profiler.stage("12h"):
                    assert profiler.current_stage() == "metrics.frequency.12h"
    stages = profiler.snapshot()["stages"]
    assert "metrics.frequency.12h" in stages
    assert "metrics.frequency" in stages
    assert "metrics" in stages
    assert "build.total" in stages


def test_stage_is_inert_outside_a_build(profiler: ExportProfiler):
    with profiler.stage("orphan"):
        pass
    assert profiler.snapshot()["stages"] == {}
    assert profiler.current_stage() == "unstaged"


def test_slow_statement_is_attributed_to_the_stage_that_issued_it(
    profiler: ExportProfiler,
):
    fast = "SELECT 1 FROM cohorts"
    slow = "SELECT SUM(raw_count) FROM event_buckets WHERE bucket_start_ts_ms>=?"
    with profiler.build():
        with profiler.stage("metrics"):
            with profiler.stage("frequency"):
                profiler.record_statement(slow, 4_200.0, rows=1)
        with profiler.stage("ledger"):
            profiler.record_statement(fast, 0.4, rows=1)

    statements = profiler.snapshot()["statements"]
    slow_names = [name for name in statements if name.endswith(
        statement_label(slow))]
    assert len(slow_names) == 1
    assert slow_names[0].startswith("metrics.frequency|")
    assert statements[slow_names[0]]["max_ms"] == pytest.approx(4_200.0)
    # The costliest statement is reported first so an operator reads the cause
    # before the noise.
    assert next(iter(statements)) == slow_names[0]


def test_same_sql_in_two_stages_stays_separately_attributable(
    profiler: ExportProfiler,
):
    sql = "SELECT SUM(raw_count) FROM event_buckets WHERE bucket_start_ts_ms>=?"
    with profiler.build():
        with profiler.stage("frequency.1h"):
            profiler.record_statement(sql, 5.0, rows=1)
        with profiler.stage("frequency.12h"):
            profiler.record_statement(sql, 900.0, rows=1)
    statements = profiler.snapshot()["statements"]
    assert any(name.startswith("frequency.1h|") for name in statements)
    assert any(name.startswith("frequency.12h|") for name in statements)


def test_repeated_statements_within_one_build_are_counted(
    profiler: ExportProfiler,
):
    sql = "SELECT * FROM model_contributions WHERE candidate_id=?"
    with profiler.build():
        with profiler.stage("candidates"):
            for _ in range(12):
                profiler.record_statement(sql, 1.0, rows=7)
    name = next(iter(profiler.snapshot()["statements"]))
    assert profiler.snapshot()["statements"][name]["max_calls_per_build"] == 12
    assert profiler.snapshot()["last_build"]["statements"] == 12
    assert profiler.snapshot()["last_build"]["distinct_statements"] == 1


# -- percentiles and aggregates ----------------------------------------------


def test_percentiles_are_exposed_per_stage(profiler: ExportProfiler):
    for value in range(1, 101):
        with profiler.build():
            with profiler.stage("build"):
                profiler.record_statement("SELECT 1 FROM t", float(value))
    view = profiler.snapshot()["stages"]["build"]
    assert view["count"] == 100
    assert view["p50_ms"] is not None and view["p90_ms"] is not None
    assert view["p50_ms"] <= view["p90_ms"] <= view["p99_ms"] <= view["max_ms"]


def test_result_change_is_reported_without_storing_the_result(
    profiler: ExportProfiler,
):
    sql = "SELECT * FROM cohorts"
    with profiler.build():
        with profiler.stage("cohort"):
            profiler.record_statement(sql, 1.0, rows=1, digest="aaa")
            profiler.record_statement(sql, 1.0, rows=1, digest="aaa")
            profiler.record_statement(sql, 1.0, rows=1, digest="bbb")
    view = next(iter(profiler.snapshot()["statements"].values()))
    assert view["result_unchanged"] == 1
    assert view["result_changed"] == 1


def test_row_digest_is_deterministic_and_order_sensitive():
    a = [{"x": 1, "y": "two"}]
    b = [{"y": "two", "x": 1}]
    c = [{"x": 2, "y": "two"}]
    assert row_digest(a) == row_digest(b)
    assert row_digest(a) != row_digest(c)
    assert row_digest(None) is None
    assert row_digest([{"x": i} for i in range(10_000)]) is None


# -- bounds -------------------------------------------------------------------


def test_stage_and_statement_registries_stay_bounded(profiler: ExportProfiler):
    with profiler.build():
        for index in range(600):
            with profiler.stage(f"s{index}"):
                profiler.record_statement(f"SELECT {index} FROM t{index}", 1.0)
    snapshot = profiler.snapshot(top=10_000)
    assert len(snapshot["stages"]) <= 130
    assert snapshot["statement_bucket_count"] <= 194
    assert snapshot["dropped_stage_buckets"] >= 0
    assert len(snapshot["statements"]) == snapshot["statement_bucket_count"]


def test_snapshot_is_json_safe(profiler: ExportProfiler):
    with profiler.build():
        with profiler.stage("metrics"):
            profiler.record_statement("SELECT 1 FROM t", 3.5, rows=2,
                                      digest="abc")
    encoded = json.dumps(profiler.snapshot(), allow_nan=False)
    assert "SELECT" not in encoded


def test_profiler_is_thread_local_and_thread_safe(profiler: ExportProfiler):
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            with profiler.build():
                with profiler.stage(f"thread{index % 3}"):
                    profiler.record_statement("SELECT 1 FROM t", 1.0)
        except BaseException as exc:  # pragma: no cover - failure detail
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(24)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert profiler.snapshot()["builds"] == 24
    # No thread leaked its stage path into another.
    assert set(profiler.snapshot()["stages"]) == {
        "build.total", "thread0", "thread1", "thread2"}


def test_an_error_inside_a_stage_is_recorded_and_reraised(
    profiler: ExportProfiler,
):
    with pytest.raises(ValueError):
        with profiler.build():
            with profiler.stage("boom"):
                raise ValueError("nope")
    stages = profiler.snapshot()["stages"]
    assert stages["boom"]["errors"] == 1
    assert stages["build.total"]["errors"] == 1
    assert profiler.snapshot()["last_build"]["error"] is True
    assert not profiler.active


def test_external_stage_observation_needs_no_active_build(
    profiler: ExportProfiler,
):
    profiler.observe_stage("worker.queue_wait.dashboard_export", 1_500.0)
    view = profiler.snapshot()["stages"]["worker.queue_wait.dashboard_export"]
    assert view["max_ms"] == pytest.approx(1_500.0)


def test_plan_capture_is_rate_limited_to_slow_statements(
    profiler: ExportProfiler,
):
    with profiler.build():
        with profiler.stage("metrics"):
            fast = profiler.record_statement("SELECT 1 FROM fast_t", 10.0)
            slow = profiler.record_statement("SELECT 1 FROM slow_t", 900.0)
    assert not profiler.wants_plan(fast, 10.0)
    assert profiler.wants_plan(slow, 900.0)
    profiler.record_plan(slow, "SCAN slow_t")
    assert not profiler.wants_plan(slow, 900.0)
    assert profiler.snapshot()["statements"][slow]["plan"] == "SCAN slow_t"
    assert profiler.snapshot()["plans_captured"] == 1


def test_reset_clears_everything_but_leaves_the_profiler_usable(
    profiler: ExportProfiler,
):
    with profiler.build():
        with profiler.stage("metrics"):
            profiler.record_statement("SELECT 1 FROM t", 1.0)
    profiler.reset()
    assert profiler.snapshot()["stages"] == {}
    assert profiler.snapshot()["builds"] == 0
    with profiler.build():
        with profiler.stage("metrics"):
            profiler.record_statement("SELECT 1 FROM t", 1.0)
    assert profiler.snapshot()["builds"] == 1


# -- integration with the read-only store -------------------------------------


@pytest.fixture()
def read_only_store(tmp_path):
    from lite_frequency_v4.store import V4ReadOnlyStore, V4Store

    path = tmp_path / "profile.db"
    V4Store(path).close()
    reader = V4ReadOnlyStore(path, enforce_thread_ownership=False)
    try:
        yield reader
    finally:
        reader.close()


def test_store_statements_are_profiled_only_inside_a_build(read_only_store):
    EXPORT_PROFILE.reset()
    read_only_store.query("SELECT COUNT(*) c FROM cohorts")
    assert EXPORT_PROFILE.snapshot()["statements"] == {}

    with EXPORT_PROFILE.build():
        with EXPORT_PROFILE.stage("cohort_row"):
            read_only_store.query("SELECT COUNT(*) c FROM cohorts")
    statements = EXPORT_PROFILE.snapshot()["statements"]
    assert len(statements) == 1
    assert next(iter(statements)).startswith("cohort_row|cohorts#")
    EXPORT_PROFILE.reset()


def test_profiling_leaves_no_open_read_transaction(read_only_store):
    """The read snapshot must be released before the profiler names it."""

    EXPORT_PROFILE.reset()
    try:
        with EXPORT_PROFILE.build():
            with EXPORT_PROFILE.stage("cohort_row"):
                read_only_store.query("SELECT * FROM cohorts")
                assert not read_only_store.connection.in_transaction
        assert not read_only_store.connection.in_transaction
    finally:
        EXPORT_PROFILE.reset()


def test_a_failed_statement_is_profiled_as_an_error(read_only_store):
    EXPORT_PROFILE.reset()
    try:
        with EXPORT_PROFILE.build():
            with EXPORT_PROFILE.stage("broken"):
                with pytest.raises(sqlite3.Error):
                    read_only_store.query("SELECT * FROM table_that_is_absent")
        statements = EXPORT_PROFILE.snapshot()["statements"]
        assert len(statements) == 1
        assert next(iter(statements.values()))["errors"] == 1
    finally:
        EXPORT_PROFILE.reset()


def test_query_plan_capture_returns_index_attribution(read_only_store):
    plan = read_only_store._query_plan(
        "SELECT * FROM runtime_sessions WHERE session_id=?", ("x",))
    assert "runtime_sessions" in plan
    # A malformed statement degrades to a labelled miss, never an exception.
    assert read_only_store._query_plan("NOT SQL").startswith(
        "plan_unavailable:")
