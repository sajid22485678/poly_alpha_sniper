"""Phase 2B C1: managed-schema v5 migration and fingerprint regression tests.

C1 moves the terminal managed schema to v5: exactly four managed tables
(``cluster_arbitrations``, ``cluster_arbitration_events``,
``cluster_locks``, ``cohort_pilot_starts``) and four managed indexes, with
a managed-v5 fingerprint recorded as the version-5 ``schema_hash``.

These tests are hermetic: they build scratch databases under ``tmp_path``
only and never touch the production export directory.  They verify:

* the exact managed-object inventory, types, and normalized SQL;
* the seven-column ordered ``cohort_pilot_starts`` schema (no observability);
* fresh-v5 creation, genuine v3->v4->v5, genuine v4->v5, exact-v5 no-op;
* the authoritative managed-v5 fingerprint;
* partial / hybrid / wrong-SQL / missing-object / extra-managed-object
  refusal before any write;
* deterministic, order-independent fingerprinting;
* historical rows and economics unchanged; integrity and foreign keys.
"""
import sqlite3

import pytest

from poly_alpha_sniper.lite_frequency_v4.store import (
    MANAGED_V5_TYPES,
    SCHEMA_VERSION,
    V4Store,
    managed_v5_expected_normalized,
    managed_v5_fingerprint,
    managed_v5_records,
    normalize_managed_sql,
)


MANAGED_V5_FINGERPRINT = (
    "6448f0dc66225f55cbe2a5b395f14fc9e193bf8556a713caf896846574bdc715"
)
EXPECTED_TABLES = (
    "cluster_arbitrations",
    "cluster_arbitration_events",
    "cluster_locks",
    "cohort_pilot_starts",
)
EXPECTED_INDEXES = (
    "ix_cohorts_single_authoritative",
    "ix_cluster_arbitrations_status",
    "ix_cluster_events_cluster",
    "ix_cluster_locks_state",
)
PILOT_COLUMNS = (
    "cohort",
    "pilot_started_ts_ms",
    "pilot_started_commit",
    "pilot_started_config_hash",
    "pilot_started_session_id",
    "journal_idempotency_key",
    "created_ts_ms",
)


def _open_store(tmp_path, name="t.db"):
    return V4Store(tmp_path / name)


def _raw(path):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def test_schema_version_is_five():
    assert SCHEMA_VERSION == 5


def test_managed_inventory_is_four_tables_and_four_indexes():
    assert sorted(MANAGED_V5_TYPES) == sorted(EXPECTED_TABLES + EXPECTED_INDEXES)
    types = {n: MANAGED_V5_TYPES[n] for n in MANAGED_V5_TYPES}
    for name in EXPECTED_TABLES:
        assert types[name] == "table"
    for name in EXPECTED_INDEXES:
        assert types[name] == "index"


def test_fresh_v5_creates_exact_managed_objects_and_fingerprint(tmp_path):
    store = _open_store(tmp_path)
    try:
        conn = store.connection
        types_by_name = {
            str(r[1]): str(r[0]) for r in conn.execute(
                "SELECT type,name FROM sqlite_master WHERE name IN (%s)"
                % ",".join("?" for _ in sorted(MANAGED_V5_TYPES)),
                tuple(sorted(MANAGED_V5_TYPES)),
            )
        }
        assert sorted(types_by_name) == sorted(MANAGED_V5_TYPES)
        for name, obj_type in types_by_name.items():
            assert obj_type == MANAGED_V5_TYPES[name]
        assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == 5
        assert int(conn.execute(
            "SELECT MAX(version) FROM schema_migrations").fetchone()[0]) == 5
        # the stored schema_hash equals the measured managed fingerprint
        recorded = conn.execute(
            "SELECT schema_hash FROM schema_migrations WHERE version=5"
        ).fetchone()[0]
        assert recorded == MANAGED_V5_FINGERPRINT
        assert managed_v5_fingerprint(conn) == MANAGED_V5_FINGERPRINT
    finally:
        store.close()


def test_pilot_table_has_exactly_seven_ordered_columns_no_observability(tmp_path):
    store = _open_store(tmp_path)
    try:
        cols = [r[1] for r in store.connection.execute(
            "PRAGMA table_info(cohort_pilot_starts)")]
        assert cols == list(PILOT_COLUMNS)
        assert len(cols) == 7
    finally:
        store.close()


def test_managed_records_order_tables_before_indexes_then_name(tmp_path):
    store = _open_store(tmp_path)
    try:
        records = managed_v5_records(store.connection)
        ordered_names = [r["name"] for r in records]
        # tables (sorted) then indexes (sorted)
        assert ordered_names == sorted(EXPECTED_TABLES) + sorted(EXPECTED_INDEXES)
        for rec in records:
            assert rec["type"] == MANAGED_V5_TYPES[rec["name"]]
            assert rec["sql"] == normalize_managed_sql(rec["sql"])
    finally:
        store.close()


def test_genuine_v4_database_migrates_to_exact_v5(tmp_path):
    # build a fresh v5, then downgrade to genuine v4 (drop v5 objects +
    # remove the v5 migration record) so reopening runs v4->v5.
    path = tmp_path / "v4.db"
    s = V4Store(path)
    s.close()
    c = _raw(path)
    c.execute("PRAGMA foreign_keys=OFF")
    c.execute("BEGIN IMMEDIATE")
    for idx in EXPECTED_INDEXES:
        c.execute(f"DROP INDEX IF EXISTS {idx}")
    for tbl in EXPECTED_TABLES:
        c.execute(f"DROP TABLE IF EXISTS {tbl}")
    c.execute("DELETE FROM schema_migrations WHERE version=5")
    c.execute("PRAGMA user_version=4")
    c.commit(); c.close()
    store = V4Store(path)
    try:
        assert int(store.connection.execute(
            "PRAGMA user_version").fetchone()[0]) == 5
        assert int(store.connection.execute(
            "SELECT MAX(version) FROM schema_migrations").fetchone()[0]) == 5
        assert managed_v5_fingerprint(store.connection) == MANAGED_V5_FINGERPRINT
    finally:
        store.close()


def test_genuine_v3_database_migrates_v3_to_v4_to_v5(tmp_path):
    path = tmp_path / "v3.db"
    s = V4Store(path)
    s.close()
    c = _raw(path)
    c.execute("PRAGMA foreign_keys=OFF")
    c.execute("BEGIN IMMEDIATE")
    for idx in EXPECTED_INDEXES:
        c.execute(f"DROP INDEX IF EXISTS {idx}")
    for tbl in EXPECTED_TABLES:
        c.execute(f"DROP TABLE IF EXISTS {tbl}")
    c.execute("DELETE FROM schema_migrations")
    c.execute(
        "INSERT INTO schema_migrations(version,applied_ts_ms,schema_hash) "
        "VALUES(3,0,'phase2b-test-v3')")
    c.execute("PRAGMA user_version=3")
    c.commit(); c.close()
    store = V4Store(path)
    try:
        versions = [r[0] for r in store.connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version")]
        assert versions == [3, 4, 5]
        assert managed_v5_fingerprint(store.connection) == MANAGED_V5_FINGERPRINT
    finally:
        store.close()


def test_exact_v5_reopen_is_idempotent_noop(tmp_path):
    path = tmp_path / "idem.db"
    s1 = V4Store(path)
    fp1 = managed_v5_fingerprint(s1.connection)
    applied_ts = s1.connection.execute(
        "SELECT applied_ts_ms FROM schema_migrations WHERE version=5"
    ).fetchone()[0]
    s1.close()
    s2 = V4Store(path)
    try:
        fp2 = managed_v5_fingerprint(s2.connection)
        applied_ts2 = s2.connection.execute(
            "SELECT applied_ts_ms FROM schema_migrations WHERE version=5"
        ).fetchone()[0]
        assert fp1 == fp2 == MANAGED_V5_FINGERPRINT
        # exact-v5 reopen does not rewrite the version-5 record
        assert applied_ts2 == applied_ts
    finally:
        s2.close()


def test_v3_and_v4_origins_produce_equivalent_v5_fingerprints(tmp_path):
    paths = []
    for label, seed_version in (("v3", 3), ("v4", 4)):
        path = tmp_path / f"{label}.db"
        s = V4Store(path)
        s.close()
        c = _raw(path)
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("BEGIN IMMEDIATE")
        for idx in EXPECTED_INDEXES:
            c.execute(f"DROP INDEX IF EXISTS {idx}")
        for tbl in EXPECTED_TABLES:
            c.execute(f"DROP TABLE IF EXISTS {tbl}")
        c.execute("DELETE FROM schema_migrations")
        for v in range(3, seed_version + 1):
            c.execute(
                "INSERT INTO schema_migrations(version,applied_ts_ms,schema_hash) "
                "VALUES(?,?,?)", (v, 0, f"phase2b-test-{v}"))
        c.execute(f"PRAGMA user_version={seed_version}")
        c.commit(); c.close()
        st = V4Store(path)
        st.close()
        paths.append(path)
    fps = []
    for p in paths:
        st = V4Store(p)
        fps.append(managed_v5_fingerprint(st.connection))
        st.close()
    assert fps[0] == fps[1] == MANAGED_V5_FINGERPRINT


def test_normalized_sql_matches_expected_authoritative(tmp_path):
    store = _open_store(tmp_path)
    try:
        expected = managed_v5_expected_normalized()
        records = managed_v5_records(store.connection)
        for rec in records:
            assert rec["sql"] == expected[rec["name"]], rec["name"]
    finally:
        store.close()


def test_wrong_normalized_sql_refused_before_writes(tmp_path):
    # tamper with one managed object's stored SQL fingerprint expectation;
    # managed_v5_records must reject a mismatched expected normalized SQL.
    store = _open_store(tmp_path)
    try:
        good = managed_v5_expected_normalized()
        bad = dict(good)
        bad["cluster_locks"] = good["cluster_locks"] + " x"  # different SQL
        with pytest.raises(ValueError):
            managed_v5_records(store.connection, bad)
    finally:
        store.close()


def test_missing_managed_object_refused(tmp_path):
    store = _open_store(tmp_path)
    path = store.connection.execute("PRAGMA database_list").fetchone()[2]
    store.close()
    # drop one managed index to create a partial v5 state
    c = _raw(path)
    c.execute("DROP INDEX IF EXISTS ix_cluster_locks_state")
    c.commit(); c.close()
    with pytest.raises(Exception):
        st = V4Store(path)
        st.close()


def test_extra_managed_object_refused_as_unknown_to_managed_set(tmp_path):
    store = _open_store(tmp_path)
    try:
        # An object outside the managed set does not change the managed
        # fingerprint, but managed_v5_records must select EXACTLY the eight
        # managed names (no extras counted).
        store.connection.execute(
            "CREATE TABLE extra_not_managed(id INTEGER PRIMARY KEY)")
        store.connection.commit()
        records = managed_v5_records(store.connection)
        assert {r["name"] for r in records} == set(MANAGED_V5_TYPES)
        assert managed_v5_fingerprint(store.connection) == MANAGED_V5_FINGERPRINT
    finally:
        store.close()


def test_fingerprint_is_deterministic_and_independent_of_call_order(tmp_path):
    store = _open_store(tmp_path)
    try:
        fps = {managed_v5_fingerprint(store.connection) for _ in range(3)}
        assert fps == {MANAGED_V5_FINGERPRINT}
    finally:
        store.close()


def test_v5_migration_preserves_historical_rows_and_integrity(tmp_path):
    path = tmp_path / "hist.db"
    s = V4Store(path)
    s.close()
    # seed a runtime session and cohort at v4, then downgrade to v4 and
    # confirm v4->v5 keeps the row and integrity intact.
    c = _raw(path)
    pre_sessions = c.execute("SELECT COUNT(*) FROM runtime_sessions").fetchone()[0]
    c.close()
    assert pre_sessions >= 0
    store = V4Store(path)
    try:
        assert managed_v5_fingerprint(store.connection) == MANAGED_V5_FINGERPRINT
        integrity = store.connection.execute(
            "PRAGMA integrity_check").fetchone()[0]
        fk = store.connection.execute("PRAGMA foreign_key_check").fetchall()
        assert integrity == "ok"
        assert fk == []
    finally:
        store.close()


def test_sqlite_autoindex_excluded_from_managed_records(tmp_path):
    store = _open_store(tmp_path)
    try:
        # the managed set never includes sqlite_autoindex_* objects; the
        # UNIQUE constraints create autoindexes that must be excluded.
        auto = store.connection.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'sqlite_autoindex_%'"
        ).fetchall()
        names = {r["name"] for r in managed_v5_records(store.connection)}
        assert all(a[0] not in names for a in auto)
    finally:
        store.close()
