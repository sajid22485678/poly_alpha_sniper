"""Loaded-path CPU, integrity time bounds, and telemetry capacity semantics.

Every case here is pinned to something measured on the 48-minute controlled
gate at ``5259b94`` and on the 19-minute identity-pinned loaded reproduction
that followed it (PID 11064, session ``e835b1e567b3``, ~1,590 events/second):

- the engine's pending event-count buffer grew monotonically to its capacity
  (20,001 buckets) because every flush was refused while the sink was under
  sustained pressure.  At capacity each inbound event ran two linear scans of
  the whole buffer: 1,252,184 executions, and 14.3 percent of all MainThread
  executing samples -- the largest single Python cost on the event loop.
- one live-integrity chunk ran 8,828 ms (gate) and 3,765 ms (reproduction)
  against a 1,000 ms bound, because chunk *rows* cannot bound chunk *time*: the
  same 3,127-row chunk of the same unit measured 297 ms and 3,765 ms.
- all nine unexpected noncritical losses arrived one-for-one with a
  deadline-exceeded batch, through the branch that dropped an oversize chunk
  outright instead of requeueing it under its bounded retry budget.

These tests assert the mechanisms, not the timings, so they stay deterministic.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    V4TelemetryWriter,
    _AdaptiveTelemetryController,
    _MAX_PRIORITY_REQUEUE_ATTEMPTS,
    _RollingTelemetryWindow,
)
from poly_alpha_sniper.lite_frequency_v4.store import (
    LiveIntegrityBudgetExceeded,
    V4ReadOnlyStore,
    V4Store,
)

# The engine harness builds the engine around real thread-owned workers; it is
# reused rather than re-declared so these cases exercise the same wiring the
# rest of the V4 engine suite does.
from poly_alpha_sniper.tests.test_frequency_v4_engine import (  # noqa: F401
    engine_harness,
)


# ---- live integrity: a real wall-clock contract ---------------------------


def _bulk_db(tmp_path: Path, rows: int = 4_000) -> Path:
    path = tmp_path / "integrity-budget.db"
    store = V4Store(path)
    store.close()
    writer = sqlite3.connect(path)
    writer.execute("CREATE TABLE bulk_probe (x INTEGER, payload TEXT)")
    writer.executemany(
        "INSERT INTO bulk_probe (x, payload) VALUES (?, ?)",
        [(index, "p" * 128) for index in range(rows)],
    )
    writer.commit()
    writer.close()
    return path


def test_a_statement_past_its_budget_is_abandoned_not_completed(tmp_path):
    """The bound is enforced by SQLite, not predicted by a row count."""

    store = V4ReadOnlyStore(_bulk_db(tmp_path), enforce_thread_ownership=False)
    try:
        with pytest.raises(LiveIntegrityBudgetExceeded):
            store._integrity_statement(
                "SELECT count(*) FROM (SELECT rowid, payload FROM bulk_probe "
                "WHERE rowid > ? ORDER BY rowid LIMIT ?)",
                (0, 4_000),
                budget_ms=0.0,
            )
        # The connection is still usable: an abandoned read commits nothing and
        # leaves no handler behind.
        rows = store._integrity_statement("PRAGMA page_count")
        assert int(rows[0][0]) > 0
    finally:
        store.close()


def test_an_abandoned_chunk_resumes_rather_than_restarting(tmp_path):
    """A budget yield must never cost the progress already made."""

    store = V4ReadOnlyStore(_bulk_db(tmp_path), enforce_thread_ownership=False)
    real = store._live_integrity_table_unit
    visits = {"bulk": 0}
    offered: list[int] = []

    def sometimes_abandons(unit, *, after_rowid, max_rows, budget_ms=None):
        if unit["name"] != "bulk_probe":
            return real(unit, after_rowid=after_rowid, max_rows=max_rows,
                        budget_ms=budget_ms)
        visits["bulk"] += 1
        offered.append(int(after_rowid))
        # Abandon the second chunk of the bulk table, after real progress.
        if visits["bulk"] == 2:
            raise LiveIntegrityBudgetExceeded("forced")
        return real(unit, after_rowid=after_rowid, max_rows=max_rows,
                    budget_ms=budget_ms)

    store._live_integrity_table_unit = sometimes_abandons
    try:
        for _ in range(400):
            store.integrity_check_chunked()
            if visits["bulk"] >= 3:
                break
        assert int(
            store.live_integrity_state()["budget_abandons"]) == 1
        # The first chunk made real progress, the second was abandoned, and the
        # third resumed from exactly where the second started -- neither
        # rewinding to zero nor skipping the abandoned window.
        assert offered[0] == 0
        assert offered[1] > 0
        assert offered[2] == offered[1]
    finally:
        store.close()


def test_an_abandoned_chunk_shrinks_that_unit_ceiling(tmp_path):
    """Abandoning is evidence the size was too big, and must be learned from."""

    store = V4ReadOnlyStore(_bulk_db(tmp_path), enforce_thread_ownership=False)
    real = store._live_integrity_table_unit
    fired = {"done": False}

    def abandon_once(unit, *, after_rowid, max_rows, budget_ms=None):
        if unit["name"] == "bulk_probe" and not fired["done"]:
            fired["done"] = True
            time.sleep(store.LIVE_INTEGRITY_CHUNK_TARGET_MS / 1_000.0)
            raise LiveIntegrityBudgetExceeded("forced")
        return real(unit, after_rowid=after_rowid, max_rows=max_rows,
                    budget_ms=budget_ms)

    store._live_integrity_table_unit = abandon_once
    try:
        store.integrity_check_chunked()
        ceilings = dict(store.live_integrity_state()["unit_ceiling"])
        assert ceilings, "an abandoned chunk must learn a ceiling"
        learned = next(iter(ceilings.values()))
        assert learned < store.LIVE_INTEGRITY_PROBE_ROWS
        assert learned >= store.LIVE_INTEGRITY_MIN_ROWS
    finally:
        store.close()


def test_a_new_unit_is_probed_rather_than_opened_at_the_start_size(tmp_path):
    """Row count does not predict wall time, so a fresh unit starts small."""

    store = V4ReadOnlyStore(_bulk_db(tmp_path), enforce_thread_ownership=False)
    seen: list[int] = []
    real = store._live_integrity_table_unit

    def record(unit, *, after_rowid, max_rows, budget_ms=None):
        if unit["name"] == "bulk_probe":
            seen.append(int(max_rows))
        return real(unit, after_rowid=after_rowid, max_rows=max_rows,
                    budget_ms=budget_ms)

    store._live_integrity_table_unit = record
    try:
        for _ in range(50):
            store.integrity_check_chunked()
            if seen:
                break
        assert seen, "the bulk unit was never reached"
        assert seen[0] == store.LIVE_INTEGRITY_PROBE_ROWS
        assert seen[0] < store.LIVE_INTEGRITY_START_ROWS
    finally:
        store.close()


def test_the_chunk_floor_can_reach_a_size_that_honours_the_bound(tmp_path):
    """A 250-row floor cannot bound a unit whose rows cost more than that."""

    assert V4Store.LIVE_INTEGRITY_MIN_ROWS == 1


def test_the_live_check_still_reports_its_bound_and_scope(tmp_path):
    store = V4ReadOnlyStore(_bulk_db(tmp_path), enforce_thread_ownership=False)
    try:
        result = store.integrity_check_chunked()
    finally:
        store.close()
    assert result["scope"] == "live_bounded_health_check"
    assert result["bounded"] is True
    assert result["chunk_bound_ms"] == store.LIVE_INTEGRITY_CHUNK_MAX_MS
    assert result["budget_abandons"] == 0
    assert result["chunks_over_bound"] == 0


# ---- engine: the pending aggregate buffer stays bounded and cheap ----------


def _seed_buffer(engine, count: int, *, source: str = "polymarket") -> None:
    for index in range(count):
        key = (1_000 * index, source, f"chan-{index}", "BTC", "tick", "OK")
        engine._event_count_buffer[key] = [1, 1, 0, 0]
        engine._event_count_index_add(key)


def test_the_capacity_lookup_no_longer_scans_the_whole_buffer(engine_harness):
    """The coarsening path must not cost O(buffer) per inbound event.

    Measured at 5259b94 this ran 1,252,184 times against a 20,001-entry buffer
    and was 14.3 percent of MainThread executing samples.
    """

    engine = engine_harness.engine
    _seed_buffer(engine, 400)
    # Every seeded bucket shares one semantic tuple, so the index answers the
    # "is there a compatible bucket?" question with a single lookup.
    index = engine._event_count_semantic_index
    assert len(index) == 1
    bucket = next(iter(index.values()))
    assert len(bucket) == 400
    # And the answer is the first-inserted match, exactly as the linear scan's
    # ``next(...)`` over the dict returned.
    assert next(iter(bucket)) == next(iter(engine._event_count_buffer))


def test_indices_recover_when_the_buffer_is_replaced_wholesale(engine_harness):
    engine = engine_harness.engine
    _seed_buffer(engine, 8)
    replacement = {
        (5_000, "okx", "chan-x", "ETH", "ticker", "OK"): [3, 3, 0, 0],
    }
    engine._event_count_buffer = replacement
    engine._event_count_indices_synced()
    assert len(engine._event_count_semantic_index) == 1
    resolved = next(iter(next(iter(
        engine._event_count_semantic_index.values()))))
    assert resolved in engine._event_count_buffer


def test_a_sustained_flush_refusal_cannot_grow_the_buffer_without_bound(
        engine_harness, monkeypatch):
    """Deferral is bounded by the backlog it is allowed to build.

    Under sustained sink pressure every ``DEFER`` flush was refused, so the
    buffer only grew: it pinned at 20,001 buckets on the 19-minute loaded
    reproduction and stayed there, coarsening away attribution for the rest of
    the run.  Above the backlog bound one flush per heartbeat is admitted.
    """

    engine = engine_harness.engine
    policies: list[str] = []

    def refuse_defer(method, *args, **policy):
        chosen = policy.get("overload_policy")
        policies.append(str(getattr(chosen, "value", chosen)))
        # Model a permanently pressured sink: DEFER is always refused, ADMIT
        # always lands.
        return ("ACCEPTED"
                if str(getattr(chosen, "value", chosen)) == "ADMIT"
                else "DEFERRED")

    monkeypatch.setattr(engine, "_telemetry_disposition", refuse_defer)

    bound = engine._event_count_backlog_bound()
    _seed_buffer(engine, bound // 2)
    before = len(engine._event_count_buffer)
    engine._flush_event_counts()
    # Below the bound nothing changes: the flush defers and keeps its counts.
    assert set(policies) == {"DEFER"}
    assert len(engine._event_count_buffer) == before

    policies.clear()
    _seed_buffer(engine, bound + 16)
    assert len(engine._event_count_buffer) > bound
    over = len(engine._event_count_buffer)
    engine._flush_event_counts()
    # Above it the flush is admitted, so the backlog actually drains.
    assert set(policies) == {"ADMIT"}
    assert len(engine._event_count_buffer) < over
    assert engine.counters["telemetry_event_flush_escalated"] == 1


def test_a_refused_flush_still_returns_every_count_to_the_buffer(
        engine_harness, monkeypatch):
    """A rejected aggregate flush must never destroy the accumulated value."""

    engine = engine_harness.engine
    _seed_buffer(engine, 24)
    expected = sum(counts[0] for counts in engine._event_count_buffer.values())
    monkeypatch.setattr(
        engine, "_telemetry_disposition", lambda *a, **k: "DROPPED")
    assert engine._flush_event_counts() is False
    restored = sum(counts[0] for counts in engine._event_count_buffer.values())
    assert restored == expected
    # The indices came back with the counts.
    engine._event_count_indices_synced()
    indexed = sum(
        len(bucket) for bucket in engine._event_count_semantic_index.values())
    assert indexed == len(engine._event_count_buffer)


# ---- telemetry: a starved deadline is a retry, not a drop -----------------


class _DeadlineExceeded(RuntimeError):
    telemetry_deadline_exceeded = True


class _StarvedSink:
    """Misses its deadline for the first ``misses`` batches, then commits.

    This is the sink the gate actually had: starved by an 8.8-second integrity
    chunk, not asked to write anything it could not write.
    """

    def __init__(self, misses: int) -> None:
        self.remaining = int(misses)
        self.committed: list[dict] = []
        self.attempts = 0
        self._lock = threading.Lock()

    def submit_telemetry_batch(self, commands, *, timeout_s):
        with self._lock:
            self.attempts += 1
            if self.remaining > 0:
                self.remaining -= 1
                raise _DeadlineExceeded("deadline")
            copied = [dict(command) for command in commands]
            self.committed.extend(copied)
        return {"ok": True, "rows_written": len(copied)}


def _drain(writer, deadline_s: float = 5.0) -> None:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if writer.flush(timeout_s=0.5):
            return
    raise AssertionError("telemetry writer never drained")


def test_a_deadline_starved_batch_is_requeued_not_dropped():
    """Every unexpected loss on the 5259b94 gate came through this branch.

    The batch's transaction rolled back, so its rows were still writable; they
    were dropped only because the chunk was larger than the size the controller
    had certified after the miss.  Chunk size is not evidence about the rows.
    """

    sink = _StarvedSink(misses=1)
    writer = V4TelemetryWriter(
        sink, capacity=256, batch_size=16, flush_interval_s=0.01,
        coalescing_interval_s=60.0, submit_timeout_s=1.0,
        heartbeat_interval_s=0.01,
    )
    writer.start()
    try:
        for index in range(24):
            writer.submit("record_reject_event", {"n": index})
        _drain(writer)
        metrics = writer.snapshot()
    finally:
        assert writer.stop(drain=True, timeout_s=5.0)

    # The miss was observed and reported honestly...
    assert metrics["deadline_exceeded_batches"] >= 1
    # ...but it cost no rows: the requeue rewrote them.
    assert metrics["noncritical_rows_unexpectedly_lost"] == 0
    assert metrics["rows_dropped"] == 0
    assert len(sink.committed) == 24


def test_a_permanently_unwritable_batch_still_becomes_visible_loss():
    """Retries stay finite, so genuine loss is never hidden by requeueing."""

    sink = _StarvedSink(misses=10_000)
    writer = V4TelemetryWriter(
        sink, capacity=256, batch_size=16, flush_interval_s=0.01,
        coalescing_interval_s=60.0, submit_timeout_s=1.0,
        heartbeat_interval_s=0.01,
    )
    writer.start()
    try:
        for index in range(8):
            writer.submit("record_reject_event", {"n": index})
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if writer.snapshot()["rows_dropped"]:
                break
            time.sleep(0.01)
        metrics = writer.snapshot()
    finally:
        writer.stop(drain=False, timeout_s=5.0)

    assert metrics["rows_dropped"] > 0
    assert metrics["deadline_exceeded_batches"] >= 1
    # Bounded: the sink was not asked to retry forever.
    assert sink.attempts <= (_MAX_PRIORITY_REQUEUE_ATTEMPTS + 2) * 8


# ---- controller: memoized statistics are the same statistics --------------


def test_transaction_stats_cache_returns_the_recomputed_value():
    """``decide`` runs per submission; the summary must not change when cached."""

    controller = _AdaptiveTelemetryController(
        queue_capacity=256, physical_max_chunk=64, flush_interval_s=0.25,
        budgeted_sink=True, started_monotonic=0.0,
    )
    for index in range(40):
        controller.observe_commit(
            now=1.0 + index * 0.01, rows=(index % 7) + 1,
            logical_rows=(index % 7) + 1,
            transaction_ms=1.0 + index, total_ms=2.0 + index,
            queue_depth=index,
        )
    first = controller._transaction_stats(2.0)
    cached = controller._transaction_stats(2.0)
    assert cached == first
    # Force a recomputation of the identical sample set and compare.
    controller._transaction_stats_signature = None
    assert controller._transaction_stats(2.0) == first

    # A new observation must invalidate it.
    controller.observe_commit(
        now=2.1, rows=64, logical_rows=64,
        transaction_ms=900.0, total_ms=950.0, queue_depth=1,
    )
    assert controller._transaction_stats(2.2) != first


def test_window_view_cache_returns_the_recomputed_view():
    window = _RollingTelemetryWindow(window_s=15, started_monotonic=0.0)
    for tick in range(10):
        window.add(float(tick), offered=3, admitted=2)
        window.observe_depth(float(tick), tick * 2)
    first = window.view(9.5)
    assert window.view(9.5) is first
    window._view_cache.clear()
    assert window.view(9.5) == first
    # A real change invalidates it; a repeated identical depth does not.
    before = window._revision
    window.observe_depth(9.5, 18)
    assert window._revision == before
    window.observe_depth(9.5, 99)
    assert window._revision != before
    assert window.view(9.5) != first
