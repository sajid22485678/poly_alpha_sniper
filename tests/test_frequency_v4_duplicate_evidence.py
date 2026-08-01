"""A UNIQUE collision must be *verified* before it may be called a duplicate.

Driven through a real ``V4Store``, a real ``V4TelemetryStoreSink`` and real
SQLite constraints.  The previous regression used a fake sink that raised a
hand-written error string containing ``book_snapshots.state_hash``, and the lane
pattern-matched that string to conclude the row was already durably stored.  So
the property under test was a message, not evidence: no test ever created two
rows that collide on the constraint and disagree about what was observed, which
is exactly the case where the inference is wrong.

It is wrong often enough to matter.  The constraint is over
``(market_identity_id, token_id, state_hash, receipt_ts_ms)`` and the engine's
hash covers only token, condition, provider timestamp, connection epoch and the
two ladders.  Provenance, sequence number, monotonic receipt, derived
top-of-book, hydration and staleness all sit outside both, and the state
coalescer deliberately re-offers a book whose ``stale`` flag has changed.  Under
the old rule that re-offer was discarded as "already stored" and its observation
was lost silently.
"""
from __future__ import annotations

import sqlite3

import pytest

from poly_alpha_sniper.lite_frequency_v4.persistence import V4TelemetryStoreSink
from poly_alpha_sniper.lite_frequency_v4.store import (
    V4EvidenceConflict,
    V4EvidenceDuplicate,
    V4Store,
    _BOOK_SNAPSHOT_EVIDENCE_FIELDS,
    _BOOK_SNAPSHOT_NON_EVIDENCE_FIELDS,
)
from poly_alpha_sniper.lite_frequency_v4.telemetry import (
    TelemetryLossCategory,
    V4TelemetryWriter,
)
from tests.test_frequency_v4_store import NOW, seed_market_window, seed_session


TS = NOW - 120_000


@pytest.fixture
def seeded(tmp_path):
    """A real store with one market identity, ready to take book snapshots."""

    path = tmp_path / "duplicate_evidence.db"
    store = V4Store(path)
    seed_session(store)
    context = seed_market_window(store)
    store.close()
    return path, context


def _book(context, **overrides):
    """A complete, valid book snapshot row for the seeded identity."""

    row = {
        "source_event_id": None,
        "market_identity_id": context["identity_id"],
        "token_id": context["yes_token"],
        "outcome_side": "YES",
        "provider_ts_ms": TS - 5,
        "receipt_ts_ms": TS,
        "monotonic_ns": TS * 1_000_000,
        "sequence_no": 7,
        "state_hash": "hash-alpha",
        "best_bid": 0.48,
        "best_ask": 0.49,
        "spread": 0.01,
        "bid_depth_5": 10.0,
        "ask_depth_5": 12.0,
        "bids_json": "[[0.48,10]]",
        "asks_json": "[[0.49,12]]",
        "hydrated": 1,
        "stale": 0,
        "invalid_reason": None,
    }
    row.update(overrides)
    return row


def _rows(store: V4Store, sql: str, params=()) -> list[dict]:
    return store.query(sql, params)


def _snapshot_count(path) -> int:
    reader = V4Store(path)
    try:
        return int(reader.query(
            "SELECT COUNT(*) AS n FROM book_snapshots")[0]["n"])
    finally:
        reader.close()


# ---------------------------------------------------------------------------
# 1. The comparison itself, against real SQLite rows
# ---------------------------------------------------------------------------


def test_exact_duplicate_is_verified_field_by_field(seeded):
    """A byte-identical re-offer is a duplicate -- and it is *proven* so."""

    path, context = seeded
    store = V4Store(path)
    try:
        first = store.record_book_snapshot(_book(context))
        assert first > 0

        with pytest.raises(V4EvidenceDuplicate) as caught:
            store.record_book_snapshot(_book(context))
    finally:
        store.close()

    collision = caught.value.collision
    assert collision.equivalent
    assert collision.differing == {}
    assert collision.existing_rowid == first
    assert collision.table == "book_snapshots"
    # Every evidence-bearing column was actually compared, not just the key.
    assert collision.compared == _BOOK_SNAPSHOT_EVIDENCE_FIELDS
    assert len(collision.compared) > 4
    assert "UNIQUE constraint failed" in collision.constraint_error
    # The typed attribute is the contract; nothing reads the message.
    assert caught.value.telemetry_verified_duplicate is True

    assert _snapshot_count(path) == 1


@pytest.mark.parametrize(
    "field, value",
    [
        # Outside the UNIQUE key and outside the engine's state hash.  Each of
        # these is a real observation that the old rule discarded.
        ("stale", 1),
        ("hydrated", 0),
        ("sequence_no", 8),
        ("monotonic_ns", TS * 1_000_000 + 1),
        ("best_bid", 0.47),
        ("best_ask", 0.50),
        ("spread", 0.03),
        ("bid_depth_5", 11.0),
        ("ask_depth_5", 13.0),
        ("outcome_side", "NO"),
        ("invalid_reason", "stale_book"),
        ("provider_ts_ms", TS - 9),
    ],
)
def test_same_unique_key_with_differing_evidence_fails_closed(
        seeded, field, value):
    """Same key, different observation: a conflict, never a duplicate."""

    path, context = seeded
    store = V4Store(path)
    try:
        store.record_book_snapshot(_book(context))
        with pytest.raises(V4EvidenceConflict) as caught:
            store.record_book_snapshot(_book(context, **{field: value}))
    finally:
        store.close()

    collision = caught.value.collision
    assert not collision.equivalent
    assert field in collision.differing
    stored, offered = collision.differing[field]
    assert offered == value
    assert stored != offered
    # Complete diagnostics: the summary names the field and both values.
    assert field in caught.value.args[0]
    assert caught.value.telemetry_integrity_conflict is True
    assert not getattr(caught.value, "telemetry_verified_duplicate", False)

    # Fail-closed means the differing row is *not* written, and the stored one
    # is not overwritten either.  Nothing is silently reconciled.
    assert _snapshot_count(path) == 1


def test_mutable_retention_state_is_excluded_with_reason(seeded):
    """A pinned stored row is still a duplicate of an unpinned re-offer.

    ``retention_class`` and ``pin_count`` are written *after* insert when a row
    is promoted to trade evidence.  Comparing them would report a conflict for a
    row carrying no new observation at all, so they are excluded deliberately --
    and this pins that decision rather than leaving it to be rediscovered.
    """

    path, context = seeded
    store = V4Store(path)
    try:
        stored_id = store.record_book_snapshot(_book(context))
        with store.transaction() as conn:
            conn.execute(
                "UPDATE book_snapshots SET retention_class='TRADE_EVIDENCE',"
                "pin_count=pin_count+3 WHERE book_snapshot_id=?",
                (stored_id,),
            )
        pinned = _rows(
            store,
            "SELECT retention_class,pin_count FROM book_snapshots "
            "WHERE book_snapshot_id=?", (stored_id,))[0]
        assert pinned["retention_class"] == "TRADE_EVIDENCE"
        assert pinned["pin_count"] == 3

        with pytest.raises(V4EvidenceDuplicate):
            store.record_book_snapshot(_book(context))
    finally:
        store.close()

    assert set(_BOOK_SNAPSHOT_NON_EVIDENCE_FIELDS) == {
        "book_snapshot_id", "retention_class", "pin_count"}
    assert not (set(_BOOK_SNAPSHOT_EVIDENCE_FIELDS)
                & set(_BOOK_SNAPSHOT_NON_EVIDENCE_FIELDS))


def test_every_stored_column_is_either_evidence_or_excluded(seeded):
    """No column may be forgotten: the two sets must cover the whole table.

    A column added later and left out of both would be compared by nobody, and
    a duplicate verdict would quietly stop meaning what it claims.
    """

    path, _ = seeded
    store = V4Store(path)
    try:
        columns = {
            str(row["name"]) for row in store.query(
                "SELECT name FROM pragma_table_info('book_snapshots')")
        }
    finally:
        store.close()

    declared = set(_BOOK_SNAPSHOT_EVIDENCE_FIELDS) | set(
        _BOOK_SNAPSHOT_NON_EVIDENCE_FIELDS)
    assert columns == declared


def test_omitted_column_is_compared_against_the_schema_default(seeded):
    """An absent column is not "unknown": SQLite would have stored its default.

    Comparing it as ``None`` would call a row equivalent whose stored value is
    the default and whose offered value is anything else -- or the reverse.
    """

    path, context = seeded
    store = V4Store(path)
    try:
        # ``stale`` omitted; the schema default is 0, which is what the first
        # insert stored, so the re-offer really is equivalent.
        baseline = _book(context)
        baseline.pop("stale")
        store.record_book_snapshot(dict(baseline))
        with pytest.raises(V4EvidenceDuplicate):
            store.record_book_snapshot(dict(baseline))

        # And the stored row genuinely holds the default rather than NULL.
        stored = _rows(
            store, "SELECT stale FROM book_snapshots LIMIT 1")[0]
        assert stored["stale"] == 0

        # Now offer the same row *with* a non-default value: a conflict.
        with pytest.raises(V4EvidenceConflict) as caught:
            store.record_book_snapshot(_book(context, stale=1))
    finally:
        store.close()

    assert caught.value.collision.differing["stale"] == (0, 1)


def test_a_non_unique_integrity_error_is_not_reinterpreted(seeded):
    """Only UNIQUE collisions are resolved this way; others stay themselves."""

    path, context = seeded
    store = V4Store(path)
    try:
        with pytest.raises(sqlite3.IntegrityError) as caught:
            # ``outcome_side`` has a CHECK constraint, not a UNIQUE one.
            store.record_book_snapshot(_book(context, outcome_side="MAYBE"))
    finally:
        store.close()

    assert not isinstance(caught.value, (V4EvidenceDuplicate, V4EvidenceConflict))
    assert "CHECK constraint failed" in str(caught.value)
    assert _snapshot_count(path) == 0


# ---------------------------------------------------------------------------
# 2. Through the physical telemetry sink, in a real multi-row transaction
# ---------------------------------------------------------------------------


def _call(context, **overrides):
    return {"method": "record_book_snapshot", "args": (_book(context, **overrides),)}


def test_sink_raises_the_typed_duplicate_for_a_whole_rolled_back_chunk(seeded):
    """One collision rolls the chunk back -- and says so with a type."""

    path, context = seeded
    store = V4Store(path)
    try:
        store.record_book_snapshot(_book(context))
    finally:
        store.close()

    sink = V4TelemetryStoreSink(path)
    with pytest.raises(V4EvidenceDuplicate):
        sink.submit_telemetry_batch([
            _call(context, receipt_ts_ms=TS + 1, state_hash="hash-beta"),
            _call(context),                                   # the collision
            _call(context, receipt_ts_ms=TS + 2, state_hash="hash-gamma"),
        ], timeout_s=10.0)

    assert sink.health()["verified_duplicate_batches"] == 1
    assert sink.health()["evidence_conflict_batches"] == 0
    # The whole transaction rolled back: only the original row survives, and
    # the two innocent rows in the chunk were *not* written.
    assert _snapshot_count(path) == 1


def test_sink_raises_the_typed_conflict_and_writes_nothing(seeded):
    path, context = seeded
    store = V4Store(path)
    try:
        store.record_book_snapshot(_book(context))
    finally:
        store.close()

    sink = V4TelemetryStoreSink(path)
    with pytest.raises(V4EvidenceConflict) as caught:
        sink.submit_telemetry_batch([
            _call(context, receipt_ts_ms=TS + 1, state_hash="hash-beta"),
            _call(context, stale=1),                          # same key, new evidence
        ], timeout_s=10.0)

    assert caught.value.collision.differing["stale"] == (0, 1)
    assert sink.health()["evidence_conflict_batches"] == 1
    assert sink.health()["verified_duplicate_batches"] == 0
    assert _snapshot_count(path) == 1


# ---------------------------------------------------------------------------
# 3. End to end through the aggregating lane: bisection, order, shutdown
# ---------------------------------------------------------------------------


def _writer(path, **overrides) -> V4TelemetryWriter:
    values = {
        "capacity": 256,
        "batch_size": 16,
        "flush_interval_s": 0.01,
        "coalescing_interval_s": 60.0,
        "submit_timeout_s": 10.0,
        "heartbeat_interval_s": 0.01,
    }
    values.update(overrides)
    return V4TelemetryWriter(V4TelemetryStoreSink(path), **values)


def _drain(writer: V4TelemetryWriter) -> dict:
    writer.start()
    try:
        assert writer.flush(timeout_s=20.0)
    finally:
        assert writer.stop(drain=True, timeout_s=20.0)
    return writer.snapshot()


def _distinct_rows(context, count, *, start=0):
    """``count`` book snapshots that share nothing but their market identity."""

    return [
        _book(context, receipt_ts_ms=TS + 1_000 + start + index,
              state_hash=f"hash-{start + index:04d}", sequence_no=100 + start + index,
              monotonic_ns=(TS + start + index) * 1_000_000)
        for index in range(count)
    ]


def test_one_duplicate_among_valid_rows_condemns_only_itself(seeded):
    """The innocent rows in a rolled-back chunk must still be written."""

    path, context = seeded
    store = V4Store(path)
    try:
        rows = _distinct_rows(context, 16)
        store.record_book_snapshot(dict(rows[9]))          # already stored
    finally:
        store.close()

    writer = _writer(path, batch_size=16)
    for row in rows:
        writer.submit("record_book_snapshot", dict(row))
    snapshot = _drain(writer)

    loss = snapshot["loss_by_category"]
    assert loss[TelemetryLossCategory.POLICY_DEDUPLICATED.value] == 1
    # The other fifteen are not "already stored", and they are not lost either.
    for category in (TelemetryLossCategory.SINK_FAILURE,
                     TelemetryLossCategory.ACKNOWLEDGEMENT_FAILURE,
                     TelemetryLossCategory.SHUTDOWN_ABANDONED,
                     TelemetryLossCategory.DEADLINE_EXPIRED):
        assert loss[category.value] == 0
    assert snapshot["accounting_reconciliation"]["mismatch"] == 0
    assert snapshot["evidence_conflicts"] == 0
    # Bisection actually ran rather than the chunk being condemned wholesale.
    assert snapshot["duplicate_isolation_events"] >= 1

    # Sixteen distinct rows exist exactly once each: the fifteen the lane wrote
    # plus the one that was already there.  No duplicate write.
    assert _snapshot_count(path) == 16
    reader = V4Store(path)
    try:
        hashes = [r["state_hash"] for r in reader.query(
            "SELECT state_hash FROM book_snapshots ORDER BY state_hash")]
    finally:
        reader.close()
    assert len(hashes) == len(set(hashes)) == 16


def test_reordered_input_reaches_the_same_verdict(seeded):
    """The duplicate's position in the batch must not change the outcome."""

    path, context = seeded
    base = _distinct_rows(context, 16)

    for marker in (0, 7, 15):
        store = V4Store(path)
        try:
            store.query("SELECT 1")
            with store.transaction() as conn:
                conn.execute("DELETE FROM book_snapshots")
            store.record_book_snapshot(dict(base[marker]))
        finally:
            store.close()

        ordered = base[marker:] + base[:marker]        # rotate the input
        writer = _writer(path, batch_size=16)
        for row in ordered:
            writer.submit("record_book_snapshot", dict(row))
        snapshot = _drain(writer)

        loss = snapshot["loss_by_category"]
        assert loss[TelemetryLossCategory.POLICY_DEDUPLICATED.value] == 1, marker
        assert loss[TelemetryLossCategory.SINK_FAILURE.value] == 0, marker
        assert snapshot["accounting_reconciliation"]["mismatch"] == 0, marker
        assert _snapshot_count(path) == 16, marker


def test_one_conflicting_row_among_valid_rows_condemns_only_itself(seeded):
    """A conflict is blocking, but only for the row that actually conflicts."""

    path, context = seeded
    rows = _distinct_rows(context, 16)
    store = V4Store(path)
    try:
        store.record_book_snapshot(dict(rows[6]))
    finally:
        store.close()

    # Re-offer row 6 with a changed observation; the other fifteen are new.
    rows[6] = dict(rows[6], stale=1)

    writer = _writer(path, batch_size=16)
    for row in rows:
        writer.submit("record_book_snapshot", dict(row))
    snapshot = _drain(writer)

    loss = snapshot["loss_by_category"]
    # Blocking, and never laundered into a policy outcome.
    assert loss[TelemetryLossCategory.SINK_FAILURE.value] == 1
    assert loss[TelemetryLossCategory.POLICY_DEDUPLICATED.value] == 0
    assert snapshot["evidence_conflicts"] >= 1
    conflict = snapshot["last_evidence_conflict"]
    assert conflict is not None
    assert conflict["table"] == "book_snapshots"
    assert conflict["differing"]["stale"] == [0, 1]
    assert snapshot["accounting_reconciliation"]["mismatch"] == 0

    # The fifteen valid rows were still written; only the conflicting one was
    # withheld, and the stored row it collided with is untouched.
    assert _snapshot_count(path) == 16
    reader = V4Store(path)
    try:
        stale_values = [r["stale"] for r in reader.query(
            "SELECT stale FROM book_snapshots WHERE state_hash=?",
            (rows[6]["state_hash"],))]
    finally:
        reader.close()
    assert stale_values == [0]


def test_shutdown_during_retry_never_double_writes_or_double_counts(seeded):
    """A draining stop finishes the bisection; a forced stop abandons cleanly.

    Either way every row is attributed exactly once and nothing is written
    twice -- which is the property a retry loop around a rolled-back chunk is
    most likely to break.
    """

    path, context = seeded
    rows = _distinct_rows(context, 24)
    store = V4Store(path)
    try:
        store.record_book_snapshot(dict(rows[11]))
    finally:
        store.close()

    writer = _writer(path, batch_size=8)
    writer.start()
    try:
        for row in rows:
            writer.submit("record_book_snapshot", dict(row))
    finally:
        # Draining stop: the bisection is bounded, so it completes.
        assert writer.stop(drain=True, timeout_s=20.0)
    snapshot = writer.snapshot()

    reconciliation = snapshot["accounting_reconciliation"]
    assert reconciliation["mismatch"] == 0
    submitted = reconciliation["submitted"]
    accounted = (
        reconciliation["logical_committed"]
        + reconciliation["buffered"] + reconciliation["inflight"]
        + reconciliation["policy_sampled"] + reconciliation["policy_coalesced"]
        + reconciliation["policy_deduplicated"]
        + reconciliation["policy_deferred"] + reconciliation["unexpected_loss"]
        - reconciliation["aggregated_in_queue"]
    )
    assert accounted == submitted

    loss = snapshot["loss_by_category"]
    assert loss[TelemetryLossCategory.POLICY_DEDUPLICATED.value] == 1
    assert loss[TelemetryLossCategory.SHUTDOWN_ABANDONED.value] == 0

    # 24 distinct rows, each present exactly once.
    assert _snapshot_count(path) == 24
    reader = V4Store(path)
    try:
        counts = reader.query(
            "SELECT state_hash, COUNT(*) AS n FROM book_snapshots "
            "GROUP BY state_hash HAVING n > 1")
    finally:
        reader.close()
    assert counts == []


def test_repeated_duplicate_offers_never_accumulate_rows_or_counts(seeded):
    """Offering the same stored row again and again changes nothing durable."""

    path, context = seeded
    store = V4Store(path)
    try:
        store.record_book_snapshot(_book(context))
    finally:
        store.close()

    writer = _writer(path, batch_size=4)
    for _ in range(6):
        writer.submit("record_book_snapshot", _book(context))
    snapshot = _drain(writer)

    # Every one of the six is admitted (the lane's own dedupe key is not set
    # here) and every one is resolved as a verified duplicate.
    loss = snapshot["loss_by_category"]
    assert loss[TelemetryLossCategory.POLICY_DEDUPLICATED.value] == 6
    assert loss[TelemetryLossCategory.SINK_FAILURE.value] == 0
    assert snapshot["accounting_reconciliation"]["mismatch"] == 0
    assert _snapshot_count(path) == 1
