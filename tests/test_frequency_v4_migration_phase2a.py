"""Phase 2A Step A: additive schema v3 -> v4 migration regression tests.

The v4 migration adds cohort lineage columns (parent_cohort, status,
config_hash) and nothing else: no successor cohort exists, the Phase 1
cohort stays ACTIVE and authoritative, and every historical row is
preserved exactly.
"""
import dataclasses
import gc
import shutil
import sqlite3

import pytest

from poly_alpha_sniper.lite_frequency_v4 import store as store_module
from poly_alpha_sniper.lite_frequency_v4.config import (
    ACTIVE_COHORT,
    LEGACY_COHORT,
    FrequencyV4Config,
)
from poly_alpha_sniper.lite_frequency_v4.export import build_frequency_v4_dashboard
from poly_alpha_sniper.lite_frequency_v4.ledger import compute_capital_ledger
from poly_alpha_sniper.lite_frequency_v4.replay import replay_entries
from poly_alpha_sniper.lite_frequency_v4.store import (
    PHASE2A_SCHEMA_V4_SQL,
    SCHEMA_VERSION,
    V4Store,
)
from tests.test_frequency_v4_cohort import seed_legacy_closed_trade
from tests.test_frequency_v4_store import (
    NOW,
    create_entry,
    entry_payload,
    seed_candidate_entry_context,
    seed_market_window,
    seed_session,
)


SUCCESSOR_COHORT = "shadow_survivor_phase2a"
NEW_COLUMNS = ("parent_cohort", "status", "config_hash")
V3_COHORT_COLUMNS = (
    "cohort", "activation_ts_ms", "activation_commit", "starting_equity_usd",
    "max_exposure_pct", "fixed_shares", "authoritative", "peak_committed_usd",
    "peak_exposure_pct", "label", "created_ts_ms",
)
COUNTED_TABLES = (
    "runtime_sessions", "entries", "positions", "exits", "pnl_records",
    "cohorts",
)


def _v3_cohorts_ddl() -> str:
    for statement in store_module.PHASE1_SCHEMA_V3_SQL.split(";"):
        if "CREATE TABLE IF NOT EXISTS cohorts" in statement:
            return statement.strip().replace(
                "CREATE TABLE IF NOT EXISTS cohorts", "CREATE TABLE cohorts_v3")
    raise AssertionError("v3 cohorts DDL not found in PHASE1_SCHEMA_V3_SQL")


def downgrade_to_v3(path) -> None:
    """Rebuild the cohorts table into its exact v3 shape for migration tests."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(_v3_cohorts_ddl())
        columns = ",".join(V3_COHORT_COLUMNS)
        conn.execute(
            f"INSERT INTO cohorts_v3({columns}) SELECT {columns} FROM cohorts")
        conn.execute("DROP TABLE cohorts")
        conn.execute("ALTER TABLE cohorts_v3 RENAME TO cohorts")
        conn.execute("DELETE FROM schema_migrations WHERE version=4")
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations("
            "version,applied_ts_ms,schema_hash) VALUES(3,0,'phase2a-test-v3')")
        conn.execute("PRAGMA user_version=3")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


def seeded_v3_database(tmp_path):
    """A genuine v3-shaped database with legacy and Phase 1 cohort history."""
    path = tmp_path / "poly_alpha_frequency_v4.db"
    store = V4Store(path)
    try:
        seed_session(store)
        seed_legacy_closed_trade(store)
        context = seed_market_window(
            store, asset="ETH", open_ts=NOW - 1_800_000, suffix="p2a-open")
        evidence = seed_candidate_entry_context(store, context)
        create_entry(store, entry_payload(context, evidence, idem="p2a-open"))
    finally:
        store.close()
    downgrade_to_v3(path)
    return path


def _columns(conn, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]


def _snapshot(path):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in COUNTED_TABLES
        }
        cohorts = {
            row["cohort"]: dict(row)
            for row in conn.execute("SELECT * FROM cohorts")
        }

        def query(sql, params=()):
            return [dict(row) for row in conn.execute(sql, params)]

        ledgers = {
            cohort: dataclasses.asdict(compute_capital_ledger(query, cohort=cohort))
            for cohort in (ACTIVE_COHORT, LEGACY_COHORT)
        }
        return counts, cohorts, ledgers
    finally:
        conn.close()


def _raw(path):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


# A. Fresh v4 database creation carries the complete lineage schema.
def test_fresh_v4_schema_has_lineage_columns_and_closed_status(tmp_path):
    assert SCHEMA_VERSION == 4
    store = V4Store(tmp_path / "fresh.db")
    try:
        columns = [row["name"] for row in store.query("PRAGMA table_info(cohorts)")]
        for column in NEW_COLUMNS:
            assert columns.count(column) == 1
        assert store.query_one("PRAGMA user_version")["user_version"] == 4
        assert store.query_one(
            "SELECT MAX(version) v FROM schema_migrations")["v"] == 4
        row = store.ensure_cohort({
            "cohort": ACTIVE_COHORT,
            "activation_ts_ms": NOW,
            "activation_commit": "a" * 40,
            "starting_equity_usd": 13.0,
            "max_exposure_pct": 1.0,
            "authoritative": 1,
        })
        assert row["status"] == "ACTIVE"
        assert row["parent_cohort"] is None
        assert row["config_hash"] is None
        # N. Unknown status values are rejected by schema and write path.
        with pytest.raises(sqlite3.IntegrityError):
            store.connection.execute(
                "INSERT INTO cohorts(cohort,activation_ts_ms,starting_equity_usd,"
                "max_exposure_pct,created_ts_ms,status) VALUES('bad',1,1,0.5,1,'BOGUS')")
        with pytest.raises(ValueError):
            store.ensure_cohort({
                "cohort": "another",
                "activation_ts_ms": NOW,
                "starting_equity_usd": 13.0,
                "max_exposure_pct": 1.0,
                "status": "BOGUS",
            })
        frozen = store.ensure_cohort({
            "cohort": "frozen-history",
            "activation_ts_ms": NOW,
            "starting_equity_usd": 13.0,
            "max_exposure_pct": 1.0,
            "status": "FROZEN",
        })
        assert frozen["status"] == "FROZEN"
    finally:
        store.close()


# B, F, G, H, I, J, K, L, M. Genuine v3 -> v4 migration preserves everything.
def test_genuine_v3_database_migrates_additively(tmp_path):
    path = seeded_v3_database(tmp_path)
    with _raw(path) as conn:
        for column in NEW_COLUMNS:
            assert column not in _columns(conn, "cohorts")
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
    pre_counts, pre_cohorts, pre_ledgers = _snapshot(path)

    store = V4Store(path)
    try:
        assert store.query_one("PRAGMA user_version")["user_version"] == 4
        assert store.query_one(
            "SELECT MAX(version) v FROM schema_migrations")["v"] == 4
        columns = [row["name"] for row in store.query("PRAGMA table_info(cohorts)")]
        for column in NEW_COLUMNS:
            assert columns.count(column) == 1

        post_counts, post_cohorts, post_ledgers = _snapshot(path)
        assert post_counts == pre_counts
        assert post_ledgers == pre_ledgers
        for cohort, before in pre_cohorts.items():
            after = post_cohorts[cohort]
            for column in V3_COHORT_COLUMNS:
                assert after[column] == before[column], (cohort, column)
            assert after["parent_cohort"] is None
            assert after["config_hash"] is None

        # F/H. Phase 1 remains the single ACTIVE authoritative cohort.
        assert post_cohorts[ACTIVE_COHORT]["authoritative"] == 1
        assert post_cohorts[ACTIVE_COHORT]["status"] == "ACTIVE"
        assert post_cohorts[LEGACY_COHORT]["status"] == "FROZEN"
        assert store.query_one(
            "SELECT COUNT(*) n FROM cohorts WHERE authoritative=1")["n"] == 1
        # G. No successor cohort exists.
        assert store.query_one(
            "SELECT COUNT(*) n FROM cohorts WHERE cohort=?",
            (SUCCESSOR_COHORT,))["n"] == 0

        # L/M. Referential and page-level integrity.
        integrity = store.integrity_check()
        assert integrity["integrity"] == "ok"
        assert integrity["foreign_key_violations"] == []
    finally:
        store.close()


# O/P. Old runtime, export, and replay paths keep working after migration,
# and no entry is rejected merely because the status column now exists.
def test_migrated_database_supports_existing_runtime_paths(tmp_path):
    path = seeded_v3_database(tmp_path)
    store = V4Store(path)
    try:
        # Startup re-registration is idempotent and returns the original row.
        row = store.ensure_cohort({
            "cohort": ACTIVE_COHORT,
            "activation_ts_ms": NOW,
            "activation_commit": "c" * 40,
            "starting_equity_usd": 13.0,
            "max_exposure_pct": 1.0,
            "authoritative": 1,
        })
        assert row["activation_ts_ms"] == NOW - 7_200_000
        assert row["status"] == "ACTIVE"

        context = seed_market_window(
            store, asset="SOL", open_ts=NOW - 900_000, suffix="p2a-post")
        evidence = seed_candidate_entry_context(store, context)
        entry_id = create_entry(
            store, entry_payload(context, evidence, idem="p2a-post"))
        assert entry_id > 0

        replay = replay_entries(store)
        assert replay["entries"] >= 2

        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(),
            session_id="session-v4")
        assert payload["cohort"]["authoritative"] == ACTIVE_COHORT
        assert payload["authoritative_capital"]["ledger_available"] is True

        # N. The migrated CHECK constraint enforces the closed vocabulary.
        with pytest.raises(sqlite3.IntegrityError):
            store.connection.execute(
                "UPDATE cohorts SET status='BOGUS' WHERE cohort=?",
                (ACTIVE_COHORT,))
    finally:
        store.close()


# C/D/Q. Double apply and reopen are idempotent and cannot create a second
# authoritative cohort.
def test_migration_double_apply_and_reopen_are_idempotent(tmp_path):
    path = seeded_v3_database(tmp_path)
    store = V4Store(path)
    try:
        store._apply_migration_v4()
        store._apply_migration_v4()
        columns = [row["name"] for row in store.query("PRAGMA table_info(cohorts)")]
        for column in NEW_COLUMNS:
            assert columns.count(column) == 1
        assert store.query_one(
            "SELECT COUNT(*) n FROM schema_migrations WHERE version=4")["n"] == 1
        assert store.query_one(
            "SELECT COUNT(*) n FROM cohorts WHERE authoritative=1")["n"] == 1
        assert store.query_one(
            "SELECT COUNT(*) n FROM cohorts WHERE cohort=?",
            (SUCCESSOR_COHORT,))["n"] == 0
        cohorts_before = store.query("SELECT * FROM cohorts ORDER BY cohort")
    finally:
        store.close()

    reopened = V4Store(path)
    try:
        assert reopened.query_one("PRAGMA user_version")["user_version"] == 4
        assert reopened.query(
            "SELECT * FROM cohorts ORDER BY cohort") == cohorts_before
    finally:
        reopened.close()


class _FailingConnection:
    """Connection proxy that fails the Nth execute inside the migration."""

    def __init__(self, conn, fail_at):
        self._real = conn
        self._fail_at = fail_at
        self._calls = 0
        self.triggered = False

    def execute(self, *args, **kwargs):
        if self._calls == self._fail_at:
            self.triggered = True
            raise sqlite3.OperationalError("phase2a injected fault")
        self._calls += 1
        return self._real.execute(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


# E. A fault after any migration statement rolls the whole migration back
# and the store fails closed at open.
def test_migration_fault_injection_rolls_back_every_statement(tmp_path):
    pristine = seeded_v3_database(tmp_path)
    _, pristine_cohorts, _ = _snapshot(pristine)
    original = V4Store._apply_migration_v4
    exercised = 0

    for fail_at in range(0, 16):
        target = tmp_path / f"fault-{fail_at}.db"
        shutil.copyfile(pristine, target)
        state = {}

        def failing(self, _original=original, _fail_at=fail_at, _state=state):
            proxy = _FailingConnection(self._conn, _fail_at)
            self._conn = proxy
            try:
                _original(self)
            finally:
                self._conn = proxy._real
                _state["triggered"] = proxy.triggered

        V4Store._apply_migration_v4 = failing
        try:
            try:
                store = V4Store(target)
            except sqlite3.OperationalError:
                pass
            else:
                store.close()
        finally:
            V4Store._apply_migration_v4 = original
        gc.collect()

        if not state.get("triggered"):
            break  # fail_at exceeded the migration's statement count
        exercised += 1
        with _raw(target) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
            for column in NEW_COLUMNS:
                assert column not in _columns(conn, "cohorts")
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version=4"
            ).fetchone()[0] == 0
            cohorts = {
                row["cohort"]: dict(row)
                for row in conn.execute("SELECT * FROM cohorts")
            }
            assert cohorts == pristine_cohorts

    # BEGIN, table_info, 4 migration statements, migration record, and the
    # user_version pragma must all have been exercised as fault points.
    assert exercised >= 8

    # The pristine copy still migrates cleanly after all injected faults.
    store = V4Store(pristine)
    try:
        assert store.query_one("PRAGMA user_version")["user_version"] == 4
    finally:
        store.close()
