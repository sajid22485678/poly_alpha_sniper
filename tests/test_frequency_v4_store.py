from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sqlite3
import time

import pytest

from poly_alpha_sniper.lite_frequency_v4 import store as store_module
from poly_alpha_sniper.lite_frequency_v4.contracts import CexObservation, SourceEvent
from poly_alpha_sniper.lite_frequency_v4.store import (
    ACTIVE_COHORT,
    EXPECTED_TABLES,
    ExposureLimitExceeded,
    MANAGED_V5_TYPES,
    V4BackgroundWriteDeferred,
    V4SchemaError,
    V4Store,
    WindowReservationConflict,
    _persistent_user_schema_objects,
)


NOW = 2_000_000_000_000


def seed_session(store: V4Store, *, session_id: str = "session-v4",
                 started=NOW-7_200_000, starting_equity=130.0):
    store.ensure_cohort({
        "cohort": ACTIVE_COHORT,
        "activation_ts_ms": started,
        "activation_commit": "a" * 40,
        "starting_equity_usd": starting_equity,
        "max_exposure_pct": 1.0,
        "authoritative": 1,
    })
    store.record_runtime_session({
        "session_id": session_id,
        "launch_nonce": f"nonce-{session_id}",
        "pid": 12345,
        "git_commit": "a" * 40,
        "config_hash": "b" * 64,
        "started_ts_ms": started,
        "cohort": ACTIVE_COHORT,
    })
    return session_id


def seed_market_window(
    store: V4Store, *, asset="BTC", open_ts=NOW-300_000,
    session_id="session-v4", suffix="1",
):
    close_ts = open_ts + 300_000
    market_id = store.upsert_market({
        "polymarket_market_id": f"pm-{suffix}",
        "asset": asset,
        "slug": f"{asset.lower()}-updown-5m-{open_ts//1000}",
        "question": f"{asset} Up or Down",
        "duration_ms": 300_000,
        "open_ts_ms": open_ts,
        "close_ts_ms": close_ts,
        "status": "ACTIVE",
        "accepting_orders": True,
        "first_seen_ts_ms": open_ts-60_000,
        "last_seen_ts_ms": open_ts,
    })
    identity_id = store.record_market_identity({
        "market_id": market_id,
        "event_id": f"event-{suffix}",
        "condition_id": f"condition-{suffix}",
        "yes_token_id": f"yes-{suffix}",
        "no_token_id": f"no-{suffix}",
        "association_valid": True,
        "token_pair_valid": True,
        "ambiguous": False,
        "verification_reason": "exact_identity",
        "verified_ts_ms": open_ts,
    })
    window_id = store.ensure_asset_window({
        "asset": asset,
        "window_open_ts_ms": open_ts,
        "window_close_ts_ms": close_ts,
        "lifecycle_status": "ACTIVE",
        "created_ts_ms": open_ts-60_000,
        "updated_ts_ms": open_ts,
    })
    store.link_window_market({
        "window_id": window_id,
        "market_identity_id": identity_id,
        "eligibility_status": "ELIGIBLE",
        "selected": True,
        "linked_ts_ms": open_ts,
    })
    store.update_window_funnel(
        window_id, open_ts, available=1, eligible=1,
        available_ts_ms=open_ts, eligible_ts_ms=open_ts,
    )
    return {
        "session_id": session_id, "asset": asset, "market_id": market_id,
        "identity_id": identity_id, "window_id": window_id,
        "open_ts": open_ts, "close_ts": close_ts,
        "yes_token": f"yes-{suffix}", "no_token": f"no-{suffix}",
    }


def seed_candidate_entry_context(store: V4Store, context: dict, *, seq=1, side="YES"):
    ts = context["open_ts"] + 10_000
    event = store.record_source_event({
        "session_id": context["session_id"],
        "source": "POLYMARKET_CLOB",
        "channel": f"market:{context['identity_id']}",
        "event_type": "book",
        "asset": context["asset"],
        "market_identity_id": context["identity_id"],
        "token_id": context["yes_token"],
        "provider_ts_ms": ts-5,
        "receipt_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "sequence_no": seq,
        "payload": {"bids": [[0.48, 10]], "asks": [[0.49, 10]]},
    }, now_ms=ts)
    book_id = store.record_book_snapshot({
        "source_event_id": event["source_event_id"],
        "market_identity_id": context["identity_id"],
        "token_id": context["yes_token" if side == "YES" else "no_token"],
        "outcome_side": side,
        "provider_ts_ms": ts-5,
        "receipt_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "sequence_no": seq,
        "best_bid": 0.48,
        "best_ask": 0.49,
        "spread": 0.01,
        "bid_depth_5": 10,
        "ask_depth_5": 10,
        "bids_json": [[0.48, 10]],
        "asks_json": [[0.49, 10]],
        "hydrated": True,
        "stale": False,
    })
    cex = store.record_cex_observation({
        "source_event_id": event["source_event_id"],
        "session_id": context["session_id"],
        "provider": "OKX",
        "instrument": f"{context['asset']}-USDT",
        "asset": context["asset"],
        "price": 100.0,
        "bid": 99.9,
        "ask": 100.1,
        "provider_ts_ms": ts-4,
        "receipt_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "sequence_no": seq,
        "fresh": True,
    })
    candidate_id = store.record_candidate({
        "session_id": context["session_id"],
        "window_id": context["window_id"],
        "market_identity_id": context["identity_id"],
        "trigger_source_event_id": event["source_event_id"],
        "evaluation_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "evaluation_seq": seq,
        "status": "POSITIVE_EDGE",
        "regime": "TREND",
        "selected_side": side,
        "fair_probability_yes": 0.55 if side == "YES" else 0.45,
        "fair_probability_no": 0.45 if side == "YES" else 0.55,
        "calibrated": False,
        "reliability": 0.7,
        "positive_edge": True,
        "dominant_model": "lead_lag_impulse",
    })
    store.link_candidate_book(candidate_id, side, book_id, 5)
    store.link_candidate_cex(candidate_id, cex["cex_observation_id"], "PRIMARY", 0, 4)
    store.record_model_contribution({
        "candidate_id": candidate_id,
        "model_name": "lead_lag_impulse",
        "model_version": "v1",
        "correlation_group": "CEX_MOMENTUM",
        "direction": side,
        "raw_score": 0.6,
        "estimated_probability": 0.55,
        "evidence_age_ms": 4,
        "confidence": 0.7,
        "reliability": 0.7,
        "expected_net_edge": 0.025,
        "regime_weight": 0.8,
        "gated": False,
        "model_contribution": 0.48,
        "calibrated": False,
    })
    fair_id = store.record_fair_value({
        "candidate_id": candidate_id,
        "phase": "FINAL",
        "calculation_seq": 1,
        "calculated_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "regime": "TREND",
        "fair_probability_yes": 0.55 if side == "YES" else 0.45,
        "fair_probability_no": 0.45 if side == "YES" else 0.55,
        "calibrated": False,
        "calibration_label": "UNCALIBRATED",
    }, [{
        "outcome_side": side,
        "token_id": context["yes_token" if side == "YES" else "no_token"],
        "book_snapshot_id": book_id,
        "executable_vwap": 0.49,
        "worst_consumed_price": 0.49,
        "spread": 0.01,
        "depth_shares": 10,
        "exact_five_share_depth": True,
        "estimated_fee": 0.005,
        "execution_buffer": 0.003,
        "latency_buffer": 0.002,
        "uncertainty_buffer": 0.005,
        "net_edge": 0.045,
        "evidence_fresh": True,
        "selected": True,
    }])
    decision_id = store.record_decision({
        "candidate_id": candidate_id,
        "fair_value_calculation_id": fair_id,
        "decision_seq": 1,
        "decision_ts_ms": ts,
        "monotonic_ns": ts * 1_000_000,
        "phase": "FINAL",
        "action": "CROSS_SPREAD",
        "selected_side": side,
        "selected_net_edge": 0.045,
        "economic_gate_passed": True,
        "exact_depth_passed": True,
        "evidence_fresh": True,
        "reason": "strong_positive_net_edge",
    })
    store.reserve_window({
        "window_id": context["window_id"],
        "session_id": context["session_id"],
        "market_identity_id": context["identity_id"],
        "owner_launch_nonce": f"nonce-{context['session_id']}",
        "outcome_side": side,
        "state": "RESERVED",
        "candidate_id": candidate_id,
        "decision_id": decision_id,
        "reserved_ts_ms": ts,
        "updated_ts_ms": ts,
    })
    return {
        "candidate_id": candidate_id, "fair_id": fair_id,
        "decision_id": decision_id, "book_id": book_id,
        "event_id": event["source_event_id"], "cex_id": cex["cex_observation_id"],
        "entry_ts": ts,
    }


def entry_payload(context: dict, evidence: dict, *, idem="entry-one"):
    return {
        "session_id": context["session_id"],
        "window_id": context["window_id"],
        "market_identity_id": context["identity_id"],
        "candidate_id": evidence["candidate_id"],
        "decision_id": evidence["decision_id"],
        "fair_value_calculation_id": evidence["fair_id"],
        "book_snapshot_id": evidence["book_id"],
        "outcome_side": "YES",
        "token_id": context["yes_token"],
        "entry_ts_ms": evidence["entry_ts"] + 1,
        "entry_mode": "CROSS_SPREAD",
        "executable_vwap": 0.49,
        "worst_consumed_price": 0.49,
        "depth_shares": 10,
        "gross_cost": 2.45,
        "estimated_fee": 0.05,
        "execution_buffer": 0.003,
        "latency_buffer": 0.002,
        "uncertainty_buffer": 0.005,
        "selected_net_edge": 0.045,
        "execution_verified": True,
        "idempotency_key": idem,
    }


def create_entry(store: V4Store, payload: dict) -> int:
    return store.create_entry(
        payload,
        max_concurrent_positions=4,
        cohort=ACTIVE_COHORT,
        starting_equity_usd=130.0,
        max_exposure_pct=1.0,
        exit_fee_buffer_usd=0.1075,
    )


def _sqlite_schema_snapshot(path):
    """Capture the logical schema bytes and safety pragmas for refusal tests."""
    conn = sqlite3.connect(path)
    try:
        inventory = tuple(
            tuple(row) for row in conn.execute(
                "SELECT type,name,tbl_name,rootpage,sql "
                "FROM sqlite_master ORDER BY type,name"
            ).fetchall()
        )
        migration_object = next(
            (row for row in inventory if row[1] == "schema_migrations"),
            None,
        )
        migration_columns = ()
        migration_rows = ()
        if migration_object is not None and migration_object[0] == "table":
            migration_columns = tuple(
                tuple(row) for row in conn.execute(
                    "PRAGMA table_xinfo('schema_migrations')"
                ).fetchall()
            )
            migration_rows = tuple(
                tuple(row) for row in conn.execute(
                    "SELECT * FROM schema_migrations ORDER BY rowid"
                ).fetchall()
            )
        try:
            foreign_key_violations = tuple(
                tuple(row)
                for row in conn.execute("PRAGMA foreign_key_check").fetchall()
            )
        except sqlite3.DatabaseError as exc:
            foreign_key_violations = (
                ("ERROR", type(exc).__name__, str(exc)),
            )
        return {
            "inventory": inventory,
            "inventory_bytes": repr(inventory).encode("utf-8"),
            "schema_migrations_columns": migration_columns,
            "schema_migrations_rows": migration_rows,
            "user_version": int(conn.execute("PRAGMA user_version").fetchone()[0]),
            "integrity": conn.execute("PRAGMA integrity_check").fetchone()[0],
            "foreign_key_violations": foreign_key_violations,
        }
    finally:
        conn.close()


def _physical_sqlite_files(path):
    """Capture exact database and sidecar bytes without opening SQLite."""
    path = Path(path)
    return {
        suffix: (
            candidate.exists(),
            candidate.read_bytes() if candidate.exists() else None,
        )
        for suffix in ("", "-wal", "-shm")
        for candidate in (Path(f"{path}{suffix}"),)
    }


def _read_only_sqlite_state(path):
    """Inspect a cleanly closed database without permitting sidecar creation."""
    path = Path(path)
    uri = f"{path.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        inventory = tuple(
            tuple(row) for row in conn.execute(
                "SELECT type,name,tbl_name,rootpage,sql "
                "FROM sqlite_master ORDER BY type,name"
            ).fetchall()
        )
        has_migrations = any(
            row[0] == "table" and row[1] == "schema_migrations"
            for row in inventory
        )
        return {
            "journal_mode": str(
                conn.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower(),
            "inventory": inventory,
            "schema_migrations_rows": (
                tuple(
                    tuple(row) for row in conn.execute(
                        "SELECT * FROM schema_migrations ORDER BY version"
                    ).fetchall()
                )
                if has_migrations else ()
            ),
            "user_version": int(
                conn.execute("PRAGMA user_version").fetchone()[0]
            ),
        }
    finally:
        conn.close()


def _malformed_sqlite_state(path):
    """Inspect intentionally corrupt sqlite_master rows without repairing them."""
    path = Path(path)
    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.execute("PRAGMA writable_schema=ON")
        inventory = tuple(
            tuple(row) for row in conn.execute(
                "SELECT type,name,tbl_name,rootpage,sql "
                "FROM sqlite_master ORDER BY type,name"
            ).fetchall()
        )
        migrations = tuple(
            tuple(row) for row in conn.execute(
                "SELECT version,applied_ts_ms,schema_hash "
                "FROM schema_migrations ORDER BY version"
            ).fetchall()
        )
        managed_names = tuple(sorted(MANAGED_V5_TYPES))
        managed_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name IN ("
                + ",".join("?" for _ in managed_names)
                + ")",
                managed_names,
            ).fetchone()[0]
        )
        try:
            integrity = tuple(
                str(row[0])
                for row in conn.execute("PRAGMA integrity_check").fetchall()
            )
        except sqlite3.DatabaseError as exc:
            integrity = (type(exc).__name__, str(exc))
        return {
            "inventory": inventory,
            "migrations": migrations,
            "user_version": int(
                conn.execute("PRAGMA user_version").fetchone()[0]
            ),
            "managed_v5_count": managed_count,
            "integrity_behavior": integrity,
        }
    finally:
        conn.close()


def _assert_authority_refusal_precedes_all_sqlite_connects(
        path, monkeypatch,
):
    """Force exact source drift and prove no target or in-memory connect runs."""
    original_connect = sqlite3.connect
    connect_calls = []

    def tracking_connect(database, *args, **kwargs):
        connect_calls.append(str(database))
        return original_connect(database, *args, **kwargs)

    with monkeypatch.context() as drift:
        drift.setattr(
            store_module, "V4_BASE_SCHEMA_AUTHORITY_RAW_SHA256", "0" * 64
        )
        drift.setattr(store_module.sqlite3, "connect", tracking_connect)
        with pytest.raises(
            V4SchemaError, match="SCHEMA_SQL raw SHA-256"
        ) as raised:
            V4Store(path)

    assert "new human authority resolution required" in str(raised.value)
    assert connect_calls == []


def _assert_schema_refusal_is_read_only(path, *, match):
    before = _sqlite_schema_snapshot(path)
    with pytest.raises(V4SchemaError, match=match):
        V4Store(path)
    after = _sqlite_schema_snapshot(path)
    assert after == before
    assert after["inventory_bytes"] == before["inventory_bytes"]
    assert after["user_version"] == before["user_version"]
    assert after["schema_migrations_columns"] == before["schema_migrations_columns"]
    assert after["schema_migrations_rows"] == before["schema_migrations_rows"]
    assert after["integrity"] == "ok"
    if before["foreign_key_violations"] == ():
        assert after["foreign_key_violations"] == ()
    return after


def _create_foreign_table(path, name):
    conn = sqlite3.connect(path)
    try:
        conn.execute(f'CREATE TABLE "{name}"(id INTEGER)')
        conn.commit()
    finally:
        conn.close()


def _create_incomplete_v4_like_database(path, *, foreign_parent_shells=False):
    """Create the audit's canonical-metadata but non-authoritative V4 shell."""
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_ts_ms INTEGER NOT NULL CHECK(applied_ts_ms >= 0),
                schema_hash TEXT NOT NULL
            );
            INSERT INTO schema_migrations(version,applied_ts_ms,schema_hash)
            VALUES(4,0,'foreign-v4-shell');
            PRAGMA user_version=0;
            """
        )
        if foreign_parent_shells:
            # These are just sufficient for the old V5 DDL to run to
            # completion: all referenced parent keys exist and the partial
            # cohorts index can be created.  They are deliberately not the
            # authoritative V4 base tables.
            conn.executescript(
                """
                CREATE TABLE cohorts(
                    cohort TEXT PRIMARY KEY,
                    authoritative INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE asset_windows(window_id INTEGER PRIMARY KEY);
                CREATE TABLE entries(entry_id INTEGER PRIMARY KEY);
                CREATE TABLE runtime_sessions(session_id TEXT PRIMARY KEY);
                """
            )
        conn.commit()
    finally:
        conn.close()


def _downgrade_fresh_store_to_v4(path, *, user_version=4):
    """Build a complete genuine V4 base by removing only managed V5 state."""
    V4Store(path).close()
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("BEGIN IMMEDIATE")
        for name, object_type in MANAGED_V5_TYPES.items():
            if object_type == "index":
                conn.execute(f'DROP INDEX IF EXISTS "{name}"')
        for name, object_type in MANAGED_V5_TYPES.items():
            if object_type == "table":
                conn.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.execute("DELETE FROM schema_migrations WHERE version=5")
        conn.execute(f"PRAGMA user_version={int(user_version)}")
        conn.commit()
    finally:
        conn.close()


def _rewrite_table_definition(path, table_name, old, new):
    """Change one scratch-table definition while preserving its indexes."""
    _rewrite_table_definitions(path, table_name, [(old, new)])


def _rewrite_table_definitions(path, table_name, replacements):
    """Apply exact scratch-table SQL replacements and preserve its indexes."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        table_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()[0]
        rewritten = str(table_sql)
        for old, new in replacements:
            assert rewritten.count(old) == 1
            rewritten = rewritten.replace(old, new, 1)
        assert rewritten != table_sql
        index_sql = [
            row[0] for row in conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='index' AND tbl_name=? AND sql IS NOT NULL "
                "ORDER BY name",
                (table_name,),
            )
        ]
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f'DROP TABLE "{table_name}"')
        conn.execute(rewritten)
        for statement in index_sql:
            conn.execute(statement)
        conn.commit()
    finally:
        conn.close()


def _independent_schema_sql_canonical_hash(schema_sql):
    """Test-local reproduction of the ratified canonical string hash."""
    raw = json.dumps(
        schema_sql,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def test_fresh_schema_has_all_normalized_tables_wal_fk_and_integrity(tmp_path):
    sentinel = tmp_path / "advanced.db"
    sentinel.write_bytes(b"advanced-sentinel")
    path = tmp_path / "poly_alpha_frequency_v4.db"
    store = V4Store(path)
    try:
        tables = {row["name"] for row in store.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )}
        assert EXPECTED_TABLES <= tables
        assert store.query_one("PRAGMA journal_mode")["journal_mode"].lower() == "wal"
        assert store.query_one("PRAGMA foreign_keys")["foreign_keys"] == 1
        assert store.integrity_check() == {"integrity": "ok", "foreign_key_violations": []}
    finally:
        store.close()
    assert sentinel.read_bytes() == b"advanced-sentinel"
    reopened = V4Store(path)
    reopened.close()


def test_non_v4_database_is_refused_without_migration(tmp_path):
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE lite_trades(id INTEGER PRIMARY KEY)")
    conn.close()
    with pytest.raises(V4SchemaError, match="refusing non-v4"):
        V4Store(path)


@pytest.mark.parametrize(
    "view_name",
    ["cluster_arbitrations_probe", "application_report"],
)
def test_foreign_view_database_is_refused_before_mutation(tmp_path, view_name):
    path = tmp_path / f"{view_name}.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(f'CREATE VIEW "{view_name}" AS SELECT 1 AS value')
        conn.commit()
    finally:
        conn.close()

    after = _assert_schema_refusal_is_read_only(path, match="refusing non-v4")
    assert {row[1] for row in after["inventory"]} == {view_name}
    assert "schema_migrations" not in {row[1] for row in after["inventory"]}


@pytest.mark.parametrize("object_kind", ["table", "index", "trigger"])
def test_foreign_persistent_schema_object_is_refused_before_mutation(
        tmp_path, object_kind,
):
    path = tmp_path / f"foreign-{object_kind}.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE application_data(id INTEGER PRIMARY KEY)")
        if object_kind == "index":
            conn.execute(
                "CREATE INDEX application_data_explicit_idx "
                "ON application_data(id)"
            )
        elif object_kind == "trigger":
            conn.execute(
                "CREATE TRIGGER application_data_explicit_trigger "
                "AFTER INSERT ON application_data BEGIN SELECT 1; END"
            )
        conn.commit()
        inventoried_names = {
            row[1] for row in _persistent_user_schema_objects(conn)
        }
    finally:
        conn.close()

    assert "application_data" in inventoried_names
    if object_kind == "index":
        assert "application_data_explicit_idx" in inventoried_names
    elif object_kind == "trigger":
        assert "application_data_explicit_trigger" in inventoried_names
    after = _assert_schema_refusal_is_read_only(path, match="refusing non-v4")
    names = {row[1] for row in after["inventory"]}
    assert "application_data" in names
    if object_kind == "index":
        assert "application_data_explicit_idx" in names
    elif object_kind == "trigger":
        assert "application_data_explicit_trigger" in names


def test_fresh_classifier_excludes_internal_objects_and_implicit_autoindexes(
        tmp_path,
):
    path = tmp_path / "classifier-exclusions.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE application_data("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, external_id TEXT UNIQUE)"
        )
        conn.commit()
        sqlite_names = {
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master ORDER BY name"
            )
        }
        inventoried_names = {
            row[1] for row in _persistent_user_schema_objects(conn)
        }
    finally:
        conn.close()

    assert "sqlite_sequence" in sqlite_names
    assert any(name.startswith("sqlite_autoindex_") for name in sqlite_names)
    assert inventoried_names == {"application_data"}


def test_sqliteX_foreign_database_is_refused_without_mutation(tmp_path):
    """A LIKE ``_`` wildcard must not hide a non-empty foreign database."""
    path = tmp_path / "sqliteX-foreign.db"
    object_name = "sqliteXmanaged_probe"
    _create_foreign_table(path, object_name)
    before = _sqlite_schema_snapshot(path)

    with pytest.raises(V4SchemaError, match="refusing non-v4"):
        V4Store(path)

    after = _sqlite_schema_snapshot(path)
    names = {row[1] for row in after["inventory"]}
    assert after == before
    assert after["inventory_bytes"] == before["inventory_bytes"]
    assert names == {object_name}
    assert object_name in names
    assert EXPECTED_TABLES.isdisjoint(names)
    assert "schema_migrations" not in names
    assert after["user_version"] == before["user_version"] == 0
    assert after["integrity"] == "ok"
    assert after["foreign_key_violations"] == ()


@pytest.mark.parametrize(
    "object_name",
    ["sqliteXprobe", "sqliteAprobe", "sqlite1managed_probe"],
)
def test_false_wildcard_foreign_database_is_refused_without_mutation(
        tmp_path, object_name,
):
    """Other seventh-character LIKE matches are user objects, not internals."""
    path = tmp_path / f"{object_name}.db"
    _create_foreign_table(path, object_name)
    before = _sqlite_schema_snapshot(path)

    with pytest.raises(V4SchemaError, match="refusing non-v4"):
        V4Store(path)

    after = _sqlite_schema_snapshot(path)
    names = {row[1] for row in after["inventory"]}
    assert after == before
    assert names == {object_name}
    assert EXPECTED_TABLES.isdisjoint(names)
    assert "schema_migrations" not in names
    assert after["user_version"] == 0
    assert after["integrity"] == "ok"
    assert after["foreign_key_violations"] == ()


@pytest.mark.parametrize(
    "case,setup_sql",
    [
        (
            "missing_version",
            "CREATE TABLE schema_migrations("
            "applied_ts_ms INTEGER NOT NULL, schema_hash TEXT NOT NULL);"
            "INSERT INTO schema_migrations VALUES(7,'foreign-hash');",
        ),
        (
            "wrong_object_type",
            "CREATE VIEW schema_migrations AS "
            "SELECT 1 AS version, 0 AS applied_ts_ms, 'foreign-hash' AS schema_hash;",
        ),
        (
            "incompatible_columns",
            "CREATE TABLE schema_migrations("
            "revision INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL, "
            "fingerprint TEXT NOT NULL);"
            "INSERT INTO schema_migrations VALUES(4,7,'foreign-hash');",
        ),
        (
            "version_not_primary_key",
            "CREATE TABLE schema_migrations("
            "version INTEGER NOT NULL, applied_ts_ms INTEGER NOT NULL, "
            "schema_hash TEXT NOT NULL);"
            "INSERT INTO schema_migrations VALUES(4,7,'foreign-hash');",
        ),
    ],
)
def test_malformed_schema_migrations_is_refused_without_mutation(
        tmp_path, case, setup_sql,
):
    path = tmp_path / f"malformed-schema-migrations-{case}.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(setup_sql)
        conn.commit()
    finally:
        conn.close()

    _assert_schema_refusal_is_read_only(path, match="schema_migrations")


def test_v4_base_contract_human_authority_literals_match_schema_sql():
    assert store_module.V4_BASE_SCHEMA_AUTHORITY_UTF8_BYTES == 38_801
    assert store_module.V4_BASE_SCHEMA_AUTHORITY_RAW_SHA256 == (
        "1383cd04b6134b950d8b69de90596dc045ca4539f0a79674e14d3e94d2b4a2f7"
    )
    assert store_module.V4_BASE_SCHEMA_AUTHORITY_CANONICAL_HASH == (
        "892892b417c4ea22fa929bbcb28b478760620dbd77c564ca4ecc3453b09d0b8a"
    )
    assert (
        store_module.V4_BASE_SCHEMA_AUTHORITY_TABLES,
        store_module.V4_BASE_SCHEMA_AUTHORITY_EXPLICIT_INDEXES,
        store_module.V4_BASE_SCHEMA_AUTHORITY_IMPLICIT_AUTOINDEXES,
        store_module.V4_BASE_SCHEMA_AUTHORITY_VIEWS,
        store_module.V4_BASE_SCHEMA_AUTHORITY_TRIGGERS,
    ) == (40, 27, 32, 0, 0)

    encoded = store_module.SCHEMA_SQL.encode("utf-8")
    assert len(encoded) == 38_801
    assert hashlib.sha256(encoded).hexdigest() == (
        "1383cd04b6134b950d8b69de90596dc045ca4539f0a79674e14d3e94d2b4a2f7"
    )
    assert _independent_schema_sql_canonical_hash(store_module.SCHEMA_SQL) == (
        "892892b417c4ea22fa929bbcb28b478760620dbd77c564ca4ecc3453b09d0b8a"
    )

    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(store_module.SCHEMA_SQL)
        rows = tuple(
            tuple(row) for row in conn.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall()
        )
    finally:
        conn.close()
    implicit_autoindexes = tuple(
        name for object_type, name, sql in rows
        if object_type == "index" and sql is None
    )
    assert all(
        name.startswith("sqlite_autoindex_") for name in implicit_autoindexes
    )
    assert (
        sum(
            object_type == "table" and not name.startswith("sqlite_")
            for object_type, name, _sql in rows
        ),
        sum(
            object_type == "index"
            and sql is not None
            and not name.startswith("sqlite_")
            for object_type, name, sql in rows
        ),
        len(implicit_autoindexes),
        sum(
            object_type == "view" and not name.startswith("sqlite_")
            for object_type, name, _sql in rows
        ),
        sum(
            object_type == "trigger" and not name.startswith("sqlite_")
            for object_type, name, _sql in rows
        ),
    ) == (40, 27, 32, 0, 0)
    store_module.verify_v4_base_contract_authority()


@pytest.mark.parametrize(
    "attribute,replacement,reason",
    [
        ("V4_BASE_SCHEMA_AUTHORITY_UTF8_BYTES", 38_802, "UTF-8 byte length"),
        ("V4_BASE_SCHEMA_AUTHORITY_RAW_SHA256", "0" * 64, "raw SHA-256"),
        ("V4_BASE_SCHEMA_AUTHORITY_CANONICAL_HASH", "f" * 64, "canonical hash"),
        ("V4_BASE_SCHEMA_AUTHORITY_TABLES", 41, "tables"),
        ("V4_BASE_SCHEMA_AUTHORITY_EXPLICIT_INDEXES", 28, "explicit indexes"),
        (
            "V4_BASE_SCHEMA_AUTHORITY_IMPLICIT_AUTOINDEXES",
            33,
            "implicit autoindexes",
        ),
        ("V4_BASE_SCHEMA_AUTHORITY_VIEWS", 1, "views"),
        ("V4_BASE_SCHEMA_AUTHORITY_TRIGGERS", 1, "triggers"),
    ],
)
def test_v4_base_contract_authority_literal_drift_refuses_before_mutation(
        tmp_path, monkeypatch, attribute, replacement, reason,
):
    path = tmp_path / f"authority-{attribute.lower()}.db"
    sqlite3.connect(path).close()
    before = _sqlite_schema_snapshot(path)
    assert before["inventory"] == ()

    monkeypatch.setattr(store_module, attribute, replacement)
    with pytest.raises(V4SchemaError) as raised:
        V4Store(path)

    assert reason in str(raised.value)
    assert "new human authority resolution required" in str(raised.value)
    after = _sqlite_schema_snapshot(path)
    assert after == before
    assert after["inventory"] == ()
    assert after["user_version"] == 0
    assert after["integrity"] == "ok"
    assert after["foreign_key_violations"] == ()


@pytest.mark.parametrize(
    "changed_schema,reason",
    [
        (
            store_module.SCHEMA_SQL + "\n",
            "SCHEMA_SQL UTF-8 byte length",
        ),
        (
            store_module.SCHEMA_SQL.replace(
                "loop_lag_ms REAL NOT NULL DEFAULT 0",
                "loop_lag_ms REAL NOT NULL DEFAULT 1",
                1,
            ),
            "SCHEMA_SQL raw SHA-256",
        ),
    ],
    ids=["byte-length", "raw-hash"],
)
def test_v4_base_contract_schema_sql_drift_refuses_before_mutation(
        tmp_path, monkeypatch, changed_schema, reason,
):
    path = tmp_path / f"authority-schema-sql-{reason.split()[-1].lower()}.db"
    sqlite3.connect(path).close()
    before = _sqlite_schema_snapshot(path)
    assert before["inventory"] == ()

    monkeypatch.setattr(store_module, "SCHEMA_SQL", changed_schema)
    with pytest.raises(V4SchemaError) as raised:
        V4Store(path)

    assert reason in str(raised.value)
    assert "new human authority resolution required" in str(raised.value)
    after = _sqlite_schema_snapshot(path)
    assert after == before
    assert after["inventory"] == ()
    assert after["user_version"] == 0
    assert after["integrity"] == "ok"
    assert after["foreign_key_violations"] == ()


@pytest.mark.parametrize(
    "object_type,added_sql,reason",
    [
        (
            "view",
            "\nCREATE VIEW authority_inventory_probe AS SELECT 1 AS id;\n",
            "views",
        ),
        (
            "trigger",
            "\nCREATE TRIGGER authority_inventory_probe "
            "AFTER INSERT ON runtime_health BEGIN SELECT 1; END;\n",
            "triggers",
        ),
    ],
)
def test_v4_base_contract_added_view_or_trigger_fails_inventory_authority(
        tmp_path, monkeypatch, object_type, added_sql, reason,
):
    changed_schema = store_module.SCHEMA_SQL + added_sql
    encoded = changed_schema.encode("utf-8")
    monkeypatch.setattr(store_module, "SCHEMA_SQL", changed_schema)
    monkeypatch.setattr(
        store_module, "V4_BASE_SCHEMA_AUTHORITY_UTF8_BYTES", len(encoded))
    monkeypatch.setattr(
        store_module,
        "V4_BASE_SCHEMA_AUTHORITY_RAW_SHA256",
        hashlib.sha256(encoded).hexdigest(),
    )
    monkeypatch.setattr(
        store_module,
        "V4_BASE_SCHEMA_AUTHORITY_CANONICAL_HASH",
        _independent_schema_sql_canonical_hash(changed_schema),
    )

    path = tmp_path / f"authority-added-{object_type}.db"
    sqlite3.connect(path).close()
    before = _sqlite_schema_snapshot(path)
    with pytest.raises(V4SchemaError) as raised:
        V4Store(path)

    assert reason in str(raised.value)
    assert "expected 0, got 1" in str(raised.value)
    assert "new human authority resolution required" in str(raised.value)
    assert _sqlite_schema_snapshot(path) == before


@pytest.mark.parametrize("database_kind", ["v4", "v5", "foreign"])
def test_v4_base_contract_authority_drift_preserves_existing_database(
        tmp_path, monkeypatch, database_kind,
):
    path = tmp_path / f"authority-existing-{database_kind}.db"
    if database_kind == "v4":
        _downgrade_fresh_store_to_v4(path)
    elif database_kind == "v5":
        V4Store(path).close()
    else:
        _create_foreign_table(path, "application_data")
    before = _sqlite_schema_snapshot(path)

    monkeypatch.setattr(
        store_module, "V4_BASE_SCHEMA_AUTHORITY_RAW_SHA256", "0" * 64)
    with pytest.raises(V4SchemaError, match="SCHEMA_SQL raw SHA-256"):
        V4Store(path)

    assert _sqlite_schema_snapshot(path) == before


def test_authority_drift_leaves_nonexistent_target_and_parent_absent(
        tmp_path, monkeypatch,
):
    path = tmp_path / "missing-authority-parent" / "authority.db"
    assert not path.parent.exists()
    before = _physical_sqlite_files(path)

    _assert_authority_refusal_precedes_all_sqlite_connects(
        path, monkeypatch
    )

    assert _physical_sqlite_files(path) == before
    assert not path.parent.exists()
    assert not path.exists()
    assert not Path(f"{path}-wal").exists()
    assert not Path(f"{path}-shm").exists()


def test_authority_drift_preserves_existing_zero_byte_file(
        tmp_path, monkeypatch,
):
    path = tmp_path / "authority-zero-byte.db"
    path.write_bytes(b"")
    before = _physical_sqlite_files(path)
    assert before[""] == (True, b"")
    assert not path.read_bytes().startswith(b"SQLite format 3")

    _assert_authority_refusal_precedes_all_sqlite_connects(
        path, monkeypatch
    )

    assert _physical_sqlite_files(path) == before
    assert path.stat().st_size == 0
    assert path.read_bytes() == b""


def test_authority_drift_preserves_foreign_delete_journal_database(
        tmp_path, monkeypatch,
):
    path = tmp_path / "authority-foreign-delete.db"
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0] == "delete"
        conn.execute(
            "CREATE TABLE foreign_object("
            "id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO foreign_object(value) VALUES('foreign-sentinel')"
        )
        conn.execute("PRAGMA user_version=17")
        conn.commit()
    finally:
        conn.close()
    before = _read_only_sqlite_state(path)
    before_files = _physical_sqlite_files(path)
    assert before["journal_mode"] == "delete"
    assert before["user_version"] == 17
    assert {row[1] for row in before["inventory"]} == {"foreign_object"}

    _assert_authority_refusal_precedes_all_sqlite_connects(
        path, monkeypatch
    )

    assert _physical_sqlite_files(path) == before_files
    assert _read_only_sqlite_state(path) == before
    assert not Path(f"{path}-wal").exists()
    assert not Path(f"{path}-shm").exists()


def test_authority_drift_preserves_valid_v5_wal_database_and_reopens(
        tmp_path, monkeypatch,
):
    path = tmp_path / "authority-valid-v5-wal.db"
    V4Store(path).close()
    checkpoint = sqlite3.connect(path)
    try:
        checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        checkpoint.close()
    before = _read_only_sqlite_state(path)
    before_files = _physical_sqlite_files(path)
    assert before["journal_mode"] == "wal"
    assert [row[0] for row in before["schema_migrations_rows"]] == [4, 5]
    assert before["user_version"] == 5

    _assert_authority_refusal_precedes_all_sqlite_connects(
        path, monkeypatch
    )

    assert _physical_sqlite_files(path) == before_files
    assert _read_only_sqlite_state(path) == before
    reopened = V4Store(path)
    reopened.close()


def test_genuinely_malformed_sqlite_master_is_translated_without_mutation(
        tmp_path,
):
    path = tmp_path / "malformed-sqlite-master.db"
    _downgrade_fresh_store_to_v4(path)
    setup = sqlite3.connect(path)
    try:
        original_sql = setup.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='runtime_health'"
        ).fetchone()[0]
        assert str(original_sql).startswith("CREATE TABLE runtime_health")
        setup.execute("PRAGMA writable_schema=ON")
        setup.execute(
            "UPDATE sqlite_master "
            "SET sql='CREATE TABLE runtime_health (' "
            "WHERE type='table' AND name='runtime_health'"
        )
        setup.execute("PRAGMA writable_schema=OFF")
        setup.commit()
    finally:
        setup.close()

    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    raw = sqlite3.connect(uri, uri=True)
    try:
        with pytest.raises(
            sqlite3.DatabaseError,
            match=r"malformed database schema .*incomplete input",
        ):
            raw.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall()
    finally:
        raw.close()

    before_files = _physical_sqlite_files(path)
    before = _malformed_sqlite_state(path)
    assert [row[0] for row in before["migrations"]] == [4]
    assert before["user_version"] == 4
    assert before["managed_v5_count"] == 0
    assert not Path(f"{path}-wal").exists()
    assert not Path(f"{path}-shm").exists()

    with pytest.raises(
        V4SchemaError,
        match="malformed or unreadable SQLite schema",
    ) as raised:
        V4Store(path)

    assert isinstance(raised.value.__cause__, sqlite3.DatabaseError)
    assert _physical_sqlite_files(path) == before_files
    assert _malformed_sqlite_state(path) == before
    assert [row[0] for row in before["migrations"]] == [4]
    assert before["user_version"] == 4
    assert before["managed_v5_count"] == 0


def test_v4_base_incomplete_canonical_metadata_refuses_before_v5_mutation(
        tmp_path,
):
    path = tmp_path / "incomplete-canonical-v4-like.db"
    _create_incomplete_v4_like_database(path)

    after = _assert_schema_refusal_is_read_only(
        path, match=r"incomplete v4 base schema: missing table ")

    names = {row[1] for row in after["inventory"]}
    assert set(MANAGED_V5_TYPES).isdisjoint(names)
    assert after["schema_migrations_rows"] == (
        (4, 0, "foreign-v4-shell"),
    )
    assert after["user_version"] == 0


def test_v4_base_foreign_parent_shells_refuse_before_v5_mutation(tmp_path):
    path = tmp_path / "foreign-parent-shells-v4-like.db"
    _create_incomplete_v4_like_database(path, foreign_parent_shells=True)

    after = _assert_schema_refusal_is_read_only(
        path, match=r"incomplete v4 base schema: missing table ")

    names = {row[1] for row in after["inventory"]}
    assert {
        "schema_migrations", "cohorts", "asset_windows",
        "entries", "runtime_sessions",
    } <= names
    assert set(MANAGED_V5_TYPES).isdisjoint(names)
    assert after["schema_migrations_rows"] == (
        (4, 0, "foreign-v4-shell"),
    )
    assert after["user_version"] == 0


def test_v4_base_missing_required_table_is_named_and_read_only(tmp_path):
    path = tmp_path / "missing-base-table.db"
    _downgrade_fresh_store_to_v4(path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DROP TABLE anchor_observations")
        conn.commit()
    finally:
        conn.close()

    _assert_schema_refusal_is_read_only(
        path,
        match=r"incomplete v4 base schema: missing table anchor_observations",
    )


def test_v4_base_missing_required_column_is_named_and_read_only(tmp_path):
    path = tmp_path / "missing-base-column.db"
    _downgrade_fresh_store_to_v4(path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("ALTER TABLE retention_runs DROP COLUMN integrity_result")
        conn.commit()
    finally:
        conn.close()

    _assert_schema_refusal_is_read_only(
        path,
        match=r"missing column retention_runs\.integrity_result",
    )


def test_v4_base_incompatible_column_shape_is_named_and_read_only(tmp_path):
    path = tmp_path / "incompatible-base-column.db"
    _downgrade_fresh_store_to_v4(path)
    _rewrite_table_definition(
        path,
        "runtime_health",
        "loop_lag_ms REAL",
        "loop_lag_ms TEXT",
    )

    _assert_schema_refusal_is_read_only(
        path,
        match=r"incompatible v4 base column runtime_health\.loop_lag_ms",
    )


def test_v4_base_nullability_mismatch_is_named_and_read_only(tmp_path):
    path = tmp_path / "v4-nullability-mismatch.db"
    _downgrade_fresh_store_to_v4(path)
    _rewrite_table_definition(
        path,
        "runtime_health",
        "loop_lag_ms REAL NOT NULL DEFAULT 0",
        "loop_lag_ms REAL DEFAULT 0",
    )

    _assert_schema_refusal_is_read_only(
        path,
        match=r"incompatible v4 base column runtime_health\.loop_lag_ms",
    )


def test_v4_base_default_mismatch_is_named_and_read_only(tmp_path):
    path = tmp_path / "v4-default-mismatch.db"
    _downgrade_fresh_store_to_v4(path)
    _rewrite_table_definition(
        path,
        "runtime_health",
        "loop_lag_ms REAL NOT NULL DEFAULT 0",
        "loop_lag_ms REAL NOT NULL DEFAULT 1",
    )

    _assert_schema_refusal_is_read_only(
        path,
        match=r"incompatible v4 base column runtime_health\.loop_lag_ms",
    )


def test_v4_base_primary_key_metadata_mismatch_is_named_and_read_only(tmp_path):
    path = tmp_path / "v4-primary-key-mismatch.db"
    _downgrade_fresh_store_to_v4(path)
    _rewrite_table_definition(
        path,
        "retention_runs",
        "retention_run_id INTEGER PRIMARY KEY",
        "retention_run_id INTEGER NOT NULL UNIQUE",
    )

    _assert_schema_refusal_is_read_only(
        path,
        match=r"incompatible v4 base column retention_runs\.retention_run_id",
    )


def test_v4_base_explicit_index_uniqueness_mismatch_is_read_only(tmp_path):
    path = tmp_path / "v4-index-uniqueness-mismatch.db"
    _downgrade_fresh_store_to_v4(path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DROP INDEX ix_runtime_sessions_cohort")
        conn.execute(
            "CREATE UNIQUE INDEX ix_runtime_sessions_cohort "
            "ON runtime_sessions(cohort)"
        )
        conn.commit()
    finally:
        conn.close()

    _assert_schema_refusal_is_read_only(
        path,
        match=r"index mismatch for ix_runtime_sessions_cohort",
    )


@pytest.mark.parametrize(
    "case,replacements",
    [
        (
            "source-column",
            (
                (
                    "market_identity_id INTEGER NOT NULL "
                    "REFERENCES market_identities(market_identity_id) "
                    "ON DELETE CASCADE",
                    "market_identity_id INTEGER NOT NULL",
                ),
                (
                    "receipt_ts_ms INTEGER NOT NULL "
                    "CHECK(receipt_ts_ms >= 0)",
                    "receipt_ts_ms INTEGER NOT NULL "
                    "REFERENCES market_identities(market_identity_id) "
                    "ON DELETE CASCADE CHECK(receipt_ts_ms >= 0)",
                ),
            ),
        ),
        (
            "target-table",
            (
                (
                    "REFERENCES market_identities(market_identity_id) "
                    "ON DELETE CASCADE",
                    "REFERENCES markets(market_identity_id) "
                    "ON DELETE CASCADE",
                ),
            ),
        ),
        (
            "target-column",
            (
                (
                    "REFERENCES market_identities(market_identity_id) "
                    "ON DELETE CASCADE",
                    "REFERENCES market_identities(market_id) "
                    "ON DELETE CASCADE",
                ),
            ),
        ),
        (
            "on-update",
            (
                (
                    "REFERENCES market_identities(market_identity_id) "
                    "ON DELETE CASCADE",
                    "REFERENCES market_identities(market_identity_id) "
                    "ON UPDATE CASCADE ON DELETE CASCADE",
                ),
            ),
        ),
        (
            "on-delete",
            (
                (
                    "REFERENCES market_identities(market_identity_id) "
                    "ON DELETE CASCADE",
                    "REFERENCES market_identities(market_identity_id) "
                    "ON DELETE RESTRICT",
                ),
            ),
        ),
    ],
)
def test_v4_base_foreign_key_drift_names_table_and_is_physically_read_only(
        tmp_path, case, replacements,
):
    path = tmp_path / f"v4-foreign-key-{case}.db"
    _downgrade_fresh_store_to_v4(path)
    _rewrite_table_definitions(
        path,
        "anchor_observations",
        replacements,
    )
    before_files = _physical_sqlite_files(path)

    after = _assert_schema_refusal_is_read_only(
        path,
        match=r"foreign keys mismatch for table anchor_observations",
    )
    assert _physical_sqlite_files(path) == before_files
    assert [row[0] for row in after["schema_migrations_rows"]] == [4]
    assert after["user_version"] == 4


def test_v4_base_plausible_parent_shell_is_named_and_read_only(tmp_path):
    path = tmp_path / "v4-plausible-parent-shell.db"
    _downgrade_fresh_store_to_v4(path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DROP TABLE cohorts")
        conn.execute(
            "CREATE TABLE cohorts("
            "cohort TEXT PRIMARY KEY, "
            "authoritative INTEGER NOT NULL DEFAULT 0)"
        )
        conn.commit()
    finally:
        conn.close()

    _assert_schema_refusal_is_read_only(
        path,
        match=r"missing column cohorts\.activation_ts_ms",
    )


def test_v4_base_actual_database_sql_mismatch_is_named_and_read_only(tmp_path):
    path = tmp_path / "v4-actual-sql-mismatch.db"
    _downgrade_fresh_store_to_v4(path)
    _rewrite_table_definition(
        path,
        "runtime_health",
        "last_error TEXT",
        "last_error TEXT CHECK(1 = 1)",
    )

    _assert_schema_refusal_is_read_only(
        path,
        match=r"canonical SQL mismatch for table runtime_health",
    )


def test_v4_contract_preserves_real_target_composite_foreign_key_order(
        tmp_path,
):
    def contract_for(path, foreign_key_sql):
        conn = sqlite3.connect(path)
        try:
            conn.executescript(
                """
                CREATE TABLE synthetic_parent(
                    left_id INTEGER NOT NULL,
                    right_id INTEGER NOT NULL,
                    PRIMARY KEY(left_id, right_id)
                );
                CREATE TABLE synthetic_child(
                    child_id INTEGER PRIMARY KEY,
                    left_id INTEGER NOT NULL,
                    right_id INTEGER NOT NULL,
                    FOREIGN KEY """
                + foreign_key_sql
                + """
                );
                """
            )
            conn.execute("PRAGMA user_version=4")
            conn.commit()
        finally:
            conn.close()

        before = _physical_sqlite_files(path)
        uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
        conn = sqlite3.connect(uri, uri=True)
        try:
            sql = conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND name='synthetic_child'"
            ).fetchone()[0]
            contract = store_module._v4_table_contract(
                conn, "synthetic_child", sql)
            assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == 4
            return contract
        finally:
            conn.close()
            assert _physical_sqlite_files(path) == before

    ordered = contract_for(
        tmp_path / "composite-ordered.db",
        "(left_id, right_id) "
        "REFERENCES synthetic_parent(left_id, right_id) "
        "ON UPDATE CASCADE ON DELETE RESTRICT"
    )
    reordered = contract_for(
        tmp_path / "composite-reordered.db",
        "(right_id, left_id) "
        "REFERENCES synthetic_parent(left_id, right_id) "
        "ON UPDATE CASCADE ON DELETE RESTRICT"
    )

    assert ordered["foreign_keys"] == (
        (
            0, 0, "synthetic_parent", "left_id", "left_id",
            "CASCADE", "RESTRICT", "NONE",
        ),
        (
            0, 1, "synthetic_parent", "right_id", "right_id",
            "CASCADE", "RESTRICT", "NONE",
        ),
    )
    assert reordered["foreign_keys"] != ordered["foreign_keys"]


def test_v4_base_user_version_ahead_of_recorded_v4_is_refused_read_only(
        tmp_path,
):
    path = tmp_path / "v4-user-version-ahead.db"
    _downgrade_fresh_store_to_v4(path, user_version=5)

    _assert_schema_refusal_is_read_only(
        path,
        match=r"user_version 5 is ahead of recorded schema version 4",
    )


@pytest.mark.parametrize("user_version", [0, 4])
def test_complete_v4_base_migrates_with_reconcilable_user_version(
        tmp_path, user_version,
):
    path = tmp_path / f"complete-v4-user-version-{user_version}.db"
    _downgrade_fresh_store_to_v4(path, user_version=user_version)

    store = V4Store(path)
    try:
        assert store.query_one("PRAGMA user_version") == {"user_version": 5}
        assert [
            row["version"] for row in store.query(
                "SELECT version FROM schema_migrations ORDER BY version")
        ] == [4, 5]
        assert len(store_module.managed_v5_records(store.connection)) == 8
    finally:
        store.close()


def test_complete_v5_reopen_preserves_exact_schema_state(tmp_path):
    path = tmp_path / "complete-v5-reopen.db"
    V4Store(path).close()
    before = _sqlite_schema_snapshot(path)

    V4Store(path).close()

    assert _sqlite_schema_snapshot(path) == before


def test_v5_post_ddl_validation_failure_rolls_back_every_mutation(
        tmp_path, monkeypatch,
):
    path = tmp_path / "v5-post-ddl-failure.db"
    _downgrade_fresh_store_to_v4(path)

    def injected_failure(_connection):
        raise V4SchemaError("injected post-DDL V5 validation failure")

    monkeypatch.setattr(
        store_module, "verify_managed_v5_census", injected_failure)
    after = _assert_schema_refusal_is_read_only(
        path, match="injected post-DDL V5 validation failure")

    names = {row[1] for row in after["inventory"]}
    assert set(MANAGED_V5_TYPES).isdisjoint(names)
    assert [row[0] for row in after["schema_migrations_rows"]] == [4]
    assert after["user_version"] == 4


def test_truly_empty_database_still_initializes_after_literal_prefix_fix(tmp_path):
    path = tmp_path / "truly-empty.db"
    sqlite3.connect(path).close()
    assert _sqlite_schema_snapshot(path)["inventory"] == ()

    store = V4Store(path)
    try:
        names = {
            row["name"] for row in store.query(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert EXPECTED_TABLES <= names
        assert store.integrity_check() == {
            "integrity": "ok", "foreign_key_violations": [],
        }
    finally:
        store.close()


def test_existing_valid_v4_database_reopens_after_literal_prefix_fix(tmp_path):
    path = tmp_path / "existing-v4.db"
    V4Store(path).close()

    reopened = V4Store(path)
    try:
        assert reopened.integrity_check() == {
            "integrity": "ok", "foreign_key_violations": [],
        }
    finally:
        reopened.close()


def test_sqlite_internal_table_only_database_still_initializes(tmp_path):
    """A literal ``sqlite_`` internal table remains excluded by classification."""
    path = tmp_path / "internal-only.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE scratch_autoincrement("
            "id INTEGER PRIMARY KEY AUTOINCREMENT)"
        )
        conn.execute("DROP TABLE scratch_autoincrement")
        conn.commit()
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall() == [("sqlite_sequence",)]
    finally:
        conn.close()

    store = V4Store(path)
    try:
        names = {
            row["name"] for row in store.query(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "sqlite_sequence" in names
        assert EXPECTED_TABLES <= names
        assert store.integrity_check() == {
            "integrity": "ok", "foreign_key_violations": [],
        }
    finally:
        store.close()


def test_user_version_ahead_of_schema_version_is_refused(tmp_path):
    """STORE-F: a database claiming a future schema version must fail closed.

    Authority derives from MAX(schema_migrations.version), not from
    user_version; a pragma ahead of SCHEMA_VERSION must not be silently
    accepted as exact managed v5 (C1.B probe F).
    """
    from poly_alpha_sniper.lite_frequency_v4.store import SCHEMA_VERSION
    path = tmp_path / "v4.db"
    store = V4Store(path)
    store.close()
    conn = sqlite3.connect(path)
    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION + 3}")
    conn.commit()
    conn.close()
    with pytest.raises(V4SchemaError, match="ahead of SCHEMA_VERSION"):
        V4Store(path)
    # The pragma must not have been silently lowered.
    conn = sqlite3.connect(path)
    assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == SCHEMA_VERSION + 3
    conn.close()


def test_user_version_behind_is_reconciled_after_fingerprint_gate(tmp_path):
    """STORE-G: a lagging user_version is advanced to SCHEMA_VERSION, but
    only after the full managed-fingerprint gate has passed.  Safe, not
    masking: authority is MAX(schema_migrations.version), never user_version.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import SCHEMA_VERSION
    path = tmp_path / "v4.db"
    store = V4Store(path)
    store.close()
    # Lower the pragma to simulate a stale cache while the v5 migration
    # record (and the full managed fingerprint) remain authoritative.
    conn = sqlite3.connect(path)
    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION - 1}")
    conn.commit()
    conn.close()
    reopened = V4Store(path)
    try:
        cur = reopened.query_one("PRAGMA user_version")
        assert int(cur["user_version"]) == SCHEMA_VERSION
        # The fingerprint gate must still pass on the reconciled store.
        ic = reopened.integrity_check()
        assert ic == {"integrity": "ok", "foreign_key_violations": []}
    finally:
        reopened.close()


def test_source_event_dedup_future_regression_and_unchanged_fresh_tick(tmp_path):
    store = V4Store(tmp_path / "v4.db")
    try:
        session = seed_session(store)
        base = {
            "session_id": session, "source": "OKX", "channel": "tickers:BTC-USDT",
            "event_type": "ticker", "asset": "BTC", "provider_ts_ms": NOW-10,
            "receipt_ts_ms": NOW, "monotonic_ns": NOW*1_000_000,
            "sequence_no": 1, "payload": {"last": "100"},
        }
        first = store.record_source_event(base, now_ms=NOW)
        duplicate = store.record_source_event(base, now_ms=NOW)
        future = store.record_source_event({**base, "dedupe_key": "future", "sequence_no": 2,
                                            "provider_ts_ms": NOW+2_000}, now_ms=NOW)
        assert first["accepted"] and first["inserted"]
        assert duplicate["duplicate"] and duplicate["classification"] == "DUPLICATE"
        assert not future["accepted"] and future["classification"] == "FUTURE_EVENT"
        one = store.record_cex_observation({
            "session_id": session, "provider": "OKX", "instrument": "BTC-USDT",
            "asset": "BTC", "price": 100.0, "bid": 99.9, "ask": 100.1,
            "provider_ts_ms": NOW, "receipt_ts_ms": NOW,
            "monotonic_ns": NOW*1_000_000, "fresh": True,
        })
        unchanged = store.record_cex_observation({
            "session_id": session, "provider": "OKX", "instrument": "BTC-USDT",
            "asset": "BTC", "price": 100.0, "bid": 99.9, "ask": 100.1,
            "provider_ts_ms": NOW+100, "receipt_ts_ms": NOW+100,
            "monotonic_ns": (NOW+100)*1_000_000, "fresh": True,
        })
        assert one["classification"] == "NEW_TICK"
        assert unchanged["classification"] == "NO_NEW_TICK"
        assert unchanged["fresh"] is True
        with pytest.raises(ValueError, match="secret-like field"):
            store.record_source_event({
                **base, "dedupe_key": "secret-payload", "sequence_no": 3,
                "payload": {"api_key": "must-never-persist"},
            }, now_ms=NOW)
        counts = store.query_one("SELECT SUM(raw_count) raw,SUM(unique_count) uniq,SUM(duplicate_count) dup FROM event_buckets")
        assert counts == {"raw": 3, "uniq": 2, "dup": 1}
    finally:
        store.close()


def test_contract_ingestion_sequence_semantics_and_zero_future_tolerance(tmp_path):
    store = V4Store(tmp_path / "contract-events.db")
    try:
        session = seed_session(store)

        def event(key, sequence, provider_ts, receipt_ts, *, channel="books", epoch=4):
            return SourceEvent(
                source="POLYMARKET_CLOB",
                channel=channel,
                event_type="book",
                event_key=key,
                payload_hash=f"hash-{key}",
                provider_ts_ms=provider_ts,
                receipt_ts_ms=receipt_ts,
                receipt_monotonic_ns=receipt_ts * 1_000_000,
                sequence=sequence,
                connection_epoch=epoch,
                asset="BTC",
                market_id="pm-contract",
                condition_id="condition-contract",
                token_id="yes-contract",
                window_open_ms=NOW - 300_000,
                payload_json='{"kind":"book"}',
            )

        first = store.record_source_event(
            event("event-10-a", 10, NOW - 10, NOW), session_id=session
        )
        equal = store.record_source_event(
            event("event-10-b", 10, NOW - 9, NOW + 1), session_id=session
        )
        jump = store.record_source_event(
            event("event-42", 42, NOW - 8, NOW + 2), session_id=session
        )
        regressed = store.record_source_event(
            event("event-41", 41, NOW - 7, NOW + 3), session_id=session
        )

        assert first["accepted"] is True
        assert equal["accepted"] is True
        assert jump["accepted"] is True
        assert regressed["accepted"] is False
        assert regressed["classification"] == "REGRESSED_SEQUENCE"

        contiguous_first = store.record_source_event(
            event("contiguous-100", 100, NOW - 6, NOW + 4, channel="ordered"),
            session_id=session,
            sequence_contiguous=True,
        )
        contiguous_gap = store.record_source_event(
            event("contiguous-102", 102, NOW - 5, NOW + 5, channel="ordered"),
            session_id=session,
            sequence_contiguous=True,
        )
        assert contiguous_first["accepted"] is True
        assert contiguous_first["sequence_contiguous"] is True
        assert contiguous_gap["accepted"] is False
        assert contiguous_gap["classification"] == "SEQUENCE_GAP"

        future = store.record_source_event(
            event("future-default-zero", 1, NOW + 11, NOW + 10, channel="future"),
            session_id=session,
        )
        assert future["accepted"] is False
        assert future["classification"] == "FUTURE_EVENT"

        stored = store.query_one(
            """SELECT external_market_id,condition_id,window_open_ts_ms,
                      sequence_no,connection_epoch,payload_json
               FROM source_events WHERE dedupe_key='event-10-a'"""
        )
        assert stored == {
            "external_market_id": "pm-contract",
            "condition_id": "condition-contract",
            "window_open_ts_ms": NOW - 300_000,
            "sequence_no": 10,
            "connection_epoch": 4,
            "payload_json": '{"kind":"book"}',
        }
        with pytest.raises(ValueError, match="future_tolerance_ms"):
            store.record_source_event(
                event("bad-tolerance", 2, NOW, NOW + 20, channel="future"),
                session_id=session,
                future_tolerance_ms=-1,
            )
    finally:
        store.close()


def test_cex_contract_aliases_and_zero_future_tolerance(tmp_path):
    store = V4Store(tmp_path / "contract-cex.db")
    try:
        session = seed_session(store)
        first = store.record_cex_observation(
            CexObservation(
                provider="OKX", asset="BTC", instrument="BTC-USDT",
                price=100.0, provider_ts_ms=NOW, receipt_ts_ms=NOW,
                receipt_monotonic_ns=NOW * 1_000_000, event_id="tick-1",
                event_type="trade", side="buy", sequence=10,
                connection_epoch=2, bid=99.9, ask=100.1, size=1.25,
            ),
            session_id=session,
        )
        unchanged = store.record_cex_observation(
            CexObservation(
                provider="OKX", asset="BTC", instrument="BTC-USDT",
                price=100.0, provider_ts_ms=NOW + 1, receipt_ts_ms=NOW + 1,
                receipt_monotonic_ns=(NOW + 1) * 1_000_000, event_id="tick-2",
                event_type="ticker", sequence=11, connection_epoch=2,
                bid=99.9, ask=100.1,
            ),
            session_id=session,
        )
        future = store.record_cex_observation(
            CexObservation(
                provider="OKX", asset="BTC", instrument="BTC-USDT",
                price=100.2, provider_ts_ms=NOW + 3, receipt_ts_ms=NOW + 2,
                receipt_monotonic_ns=(NOW + 2) * 1_000_000, event_id="tick-3",
                event_type="ticker", sequence=12, connection_epoch=2,
                bid=100.1, ask=100.3,
            ),
            session_id=session,
        )

        assert first == {
            "cex_observation_id": first["cex_observation_id"],
            "inserted": True,
            "classification": "NEW_TICK",
            "fresh": True,
        }
        assert unchanged["classification"] == "NO_NEW_TICK"
        assert unchanged["fresh"] is True
        assert future["inserted"] is True
        assert future["classification"] == "INVALID"
        assert future["fresh"] is False
        stored = store.query_one(
            """SELECT event_id,event_type,trade_side,size,sequence_no,
                      connection_epoch,invalid_reason
               FROM cex_observations WHERE event_id='tick-1'"""
        )
        assert stored == {
            "event_id": "tick-1",
            "event_type": "trade",
            "trade_side": "buy",
            "size": 1.25,
            "sequence_no": 10,
            "connection_epoch": 2,
            "invalid_reason": None,
        }
        invalid = store.query_one(
            "SELECT invalid_reason FROM cex_observations WHERE event_id='tick-3'"
        )
        assert invalid == {"invalid_reason": "future_provider_timestamp"}
    finally:
        store.close()


def test_atomic_entry_race_allows_exactly_one_asset_window_entry(tmp_path):
    path = tmp_path / "race.db"
    seed_store = V4Store(path)
    try:
        seed_session(seed_store)
        context = seed_market_window(seed_store)
        evidence = seed_candidate_entry_context(seed_store, context)
    finally:
        seed_store.close()
    payloads = [entry_payload(context, evidence, idem=f"contender-{n}") for n in (1, 2)]

    def attempt(payload):
        store = V4Store(path)
        try:
            try:
                return ("ok", create_entry(store, payload))
            except WindowReservationConflict as exc:
                return ("blocked", exc.reason)
        finally:
            store.close()

    verifier = None
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, payloads))
        verifier = V4Store(path)
        assert sorted(result[0] for result in results) == ["blocked", "ok"]
        assert verifier.query_one("SELECT COUNT(*) count FROM entries")["count"] == 1
        entry = verifier.query_one("SELECT * FROM entries")
        assert entry["shares"] == 5.0
        assert entry["maker_fill_assumed"] == 0
        assert verifier.query_one(
            "SELECT COUNT(*) count FROM positions WHERE status='OPEN'"
        )["count"] == 1
        assert verifier.integrity_check()["foreign_key_violations"] == []
    finally:
        if verifier is not None:
            verifier.close()


def test_entry_risk_limit_rolls_back_without_partial_rows(tmp_path):
    store = V4Store(tmp_path / "risk.db")
    try:
        # A depleted cohort with only $2.00 of equity cannot afford one
        # five-share entry costing gross 2.45 + fee 0.05 + exit buffer.
        seed_session(store, starting_equity=2.0)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        with pytest.raises(ValueError, match="maker fill may never be assumed"):
            store.create_entry(
                {**entry_payload(context, evidence), "maker_fill_assumed": True},
                max_concurrent_positions=4, cohort=ACTIVE_COHORT,
                starting_equity_usd=2.0, max_exposure_pct=1.0,
                exit_fee_buffer_usd=0.1075,
            )
        with pytest.raises(ExposureLimitExceeded, match="insufficient_capital"):
            store.create_entry(
                entry_payload(context, evidence), max_concurrent_positions=4,
                cohort=ACTIVE_COHORT, starting_equity_usd=2.0,
                max_exposure_pct=1.0, exit_fee_buffer_usd=0.1075,
            )
        assert store.query_one("SELECT COUNT(*) count FROM entries")["count"] == 0
        assert store.query_one("SELECT COUNT(*) count FROM positions")["count"] == 0
        assert store.query_one("SELECT state FROM window_locks")["state"] == "RESERVED"
    finally:
        store.close()


def test_management_post_close_book_exit_forbidden_and_official_close_reconciles(tmp_path):
    store = V4Store(tmp_path / "manage.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        entry_id = create_entry(store, entry_payload(context, evidence))
        position = store.open_positions()[0]
        with pytest.raises(ValueError, match="post-close book exit"):
            store.record_management_decision({
                "position_id": position["position_id"], "decision_seq": 1,
                "decision_ts_ms": context["close_ts"], "monotonic_ns": NOW*1_000_000,
                "updated_fair_probability": 0.6, "executable_exit_value": 2.0,
                "hold_to_resolution_value": 3.0, "remaining_time_ms": 0,
                "spread": 0.01, "depth_shares": 10, "estimated_fee": 0.01,
                "uncertainty": 0.01, "thesis_state": "CLOSED",
                "action": "EXIT_BOOK", "reason": "forbidden_stale_exit",
            })
        store.record_resolution_attempt({
            "entry_id": entry_id, "attempt_no": 1, "attempt_ts_ms": context["close_ts"]+1,
            "source": "POLYMARKET_OFFICIAL", "result": "RESOLVED",
            "observed_outcome": "YES", "evidence_hash": "f"*64,
            "verified": True,
        })
        exit_id = store.close_position({
            "position_id": position["position_id"],
            "exit_ts_ms": context["close_ts"]+2,
            "exit_source": "OFFICIAL_RESOLUTION",
            "shares": 5.0,
            "payout_usd": 5.0,
            "gross_pnl": 2.55,
            "exit_fee": 0.0,
            "net_pnl": 2.50,
            "evidence_verified": True,
            "resolution_outcome": "YES",
            "reason": "official_resolution",
        })
        assert exit_id > 0
        pnl = store.query_one("SELECT * FROM pnl_records WHERE entry_id=?", (entry_id,))
        assert pnl["total_fees"] == pytest.approx(0.05)
        assert pnl["net_pnl"] == pytest.approx(2.50)
        assert pnl["verified"] == 1
        assert store.open_positions() == []
    finally:
        store.close()


def test_bounded_retention_deletes_only_old_unpinned_raw_evidence(tmp_path):
    store = V4Store(tmp_path / "retention.db")
    try:
        session = seed_session(store, started=NOW-20_000_000)
        common = {
            "session_id": session, "source": "OKX", "channel": "trades:BTC-USDT",
            "event_type": "trade", "asset": "BTC",
            "receipt_ts_ms": NOW-10_000_000, "monotonic_ns": NOW*1_000_000,
        }
        first = store.record_source_event({
            **common, "provider_ts_ms": NOW-10_000_010, "sequence_no": 1,
            "dedupe_key": "old-pinned", "payload": {"price": 100},
        }, now_ms=NOW-10_000_000)
        store.record_source_event({
            **common, "provider_ts_ms": NOW-10_000_009, "sequence_no": 2,
            "dedupe_key": "old-raw", "payload": {"price": 101},
        }, now_ms=NOW-10_000_000)
        store.pin_source_event(first["source_event_id"])
        result = store.compact_raw_evidence(NOW, retention_ms=1_000_000, batch_size=1)
        assert result["source_events"] == 1
        rows = store.query("SELECT dedupe_key,retention_class,pin_count FROM source_events")
        assert rows == [{"dedupe_key": "old-pinned", "retention_class": "TRADE_EVIDENCE", "pin_count": 1}]
        assert result["integrity"] == "ok"
        assert result["foreign_key_violations"] == []
    finally:
        store.close()


def test_reservation_release_requires_exact_owner_and_never_releases_entry(tmp_path):
    store = V4Store(tmp_path / "reservation-release.db")
    try:
        seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        lock = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?",
            (context["window_id"],),
        )

        for overrides in (
            {"session_id": "another-session"},
            {"owner_launch_nonce": "another-nonce"},
            {"idempotency_key": "another-idempotency-key"},
        ):
            owner = {
                "window_id": context["window_id"],
                "session_id": context["session_id"],
                "owner_launch_nonce": f"nonce-{context['session_id']}",
                "idempotency_key": lock["idempotency_key"],
                **overrides,
            }
            assert store.release_window_reservation(**owner) is False
            assert store.query_one(
                "SELECT COUNT(*) count FROM window_locks WHERE window_id=?",
                (context["window_id"],),
            )["count"] == 1

        exact_owner = {
            "window_id": context["window_id"],
            "session_id": context["session_id"],
            "owner_launch_nonce": f"nonce-{context['session_id']}",
            "idempotency_key": lock["idempotency_key"],
        }
        assert store.release_window_reservation(**exact_owner) is True
        assert store.release_window_reservation(**exact_owner) is False

        store.reserve_window({
            "window_id": context["window_id"],
            "session_id": context["session_id"],
            "market_identity_id": context["identity_id"],
            "owner_launch_nonce": exact_owner["owner_launch_nonce"],
            "outcome_side": "YES",
            "state": "RESERVED",
            "candidate_id": evidence["candidate_id"],
            "decision_id": evidence["decision_id"],
            "reserved_ts_ms": evidence["entry_ts"],
            "updated_ts_ms": evidence["entry_ts"],
            "idempotency_key": exact_owner["idempotency_key"],
        })
        create_entry(store, entry_payload(context, evidence))
        assert store.release_window_reservation(**exact_owner) is False
        assert store.query_one(
            "SELECT state FROM window_locks WHERE window_id=?",
            (context["window_id"],),
        ) == {"state": "ENTERED"}
    finally:
        store.close()


def test_atomic_per_asset_open_position_cap_across_windows(tmp_path):
    path = tmp_path / "per-asset-race.db"
    seed_store = V4Store(path)
    try:
        seed_session(seed_store)
        first_context = seed_market_window(
            seed_store, open_ts=NOW - 600_000, suffix="asset-cap-one"
        )
        second_context = seed_market_window(
            seed_store, open_ts=NOW - 300_000, suffix="asset-cap-two"
        )
        first_evidence = seed_candidate_entry_context(
            seed_store, first_context, seq=1
        )
        second_evidence = seed_candidate_entry_context(
            seed_store, second_context, seq=2
        )
    finally:
        seed_store.close()

    def attempt(context, evidence, contender):
        store = V4Store(path)
        try:
            try:
                entry_id = store.create_entry(
                    entry_payload(
                        context, evidence, idem=f"asset-cap-{contender}"
                    ),
                    max_concurrent_positions=10,
                    cohort=ACTIVE_COHORT,
                    starting_equity_usd=130.0,
                    max_exposure_pct=1.0,
                    exit_fee_buffer_usd=0.1075,
                    max_open_per_asset=1,
                )
                return "ok", entry_id
            except ExposureLimitExceeded as exc:
                return "blocked", exc.reason
        finally:
            store.close()

    verifier = None
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (
                pool.submit(attempt, first_context, first_evidence, "one"),
                pool.submit(attempt, second_context, second_evidence, "two"),
            )
            results = [future.result() for future in futures]
        verifier = V4Store(path)
        assert sorted(result[0] for result in results) == ["blocked", "ok"]
        assert {result[1] for result in results if result[0] == "blocked"} == {
            "max_open_per_asset"
        }
        assert verifier.query_one("SELECT COUNT(*) count FROM entries") == {"count": 1}
        assert verifier.query_one(
            "SELECT COUNT(*) count FROM positions WHERE asset='BTC' AND status='OPEN'"
        ) == {"count": 1}
        assert verifier.integrity_check() == {
            "integrity": "ok",
            "foreign_key_violations": [],
        }
    finally:
        if verifier is not None:
            verifier.close()


def test_event_bucket_compaction_preserves_totals_and_is_idempotent(tmp_path):
    store = V4Store(tmp_path / "bucket-compaction.db")
    try:
        minute_start = (NOW - 180_000) // 60_000 * 60_000
        observations = (
            (minute_start + 1_000, True, False, False),
            (minute_start + 2_000, False, True, False),
            (minute_start + 2_000, False, False, True),
        )
        for receipt_ts_ms, unique, duplicate, invalid in observations:
            store.record_event_count(
                receipt_ts_ms=receipt_ts_ms,
                source="OKX",
                channel="tickers:BTC-USDT",
                asset="BTC",
                event_type="ticker",
                classification="COUNTED",
                unique=unique,
                duplicate=duplicate,
                invalid=invalid,
            )
        store.record_event_count(
            receipt_ts_ms=NOW,
            source="OKX",
            channel="tickers:BTC-USDT",
            asset="BTC",
            event_type="ticker",
            classification="COUNTED",
            unique=True,
            duplicate=False,
            invalid=False,
        )

        assert store.compact_event_buckets(NOW, detail_retention_ms=10_000) == 2
        minute = store.query_one(
            """SELECT raw_count,unique_count,duplicate_count,invalid_count
               FROM event_buckets WHERE bucket_ms=60000 AND bucket_start_ts_ms=?""",
            (minute_start,),
        )
        assert minute == {
            "raw_count": 3,
            "unique_count": 1,
            "duplicate_count": 1,
            "invalid_count": 1,
        }
        assert store.query_one(
            "SELECT COUNT(*) count FROM event_buckets WHERE bucket_ms=1000"
        ) == {"count": 1}
        assert store.compact_event_buckets(NOW, detail_retention_ms=10_000) == 0
        assert store.query_one(
            "SELECT raw_count FROM event_buckets WHERE bucket_ms=60000"
        ) == {"raw_count": 3}
    finally:
        store.close()


def test_batched_event_counts_flush_exact_aggregates_atomically(tmp_path):
    store = V4Store(tmp_path / "batched-event-counts.db")
    try:
        store.record_event_count_batch([{
            "receipt_ts_ms": NOW,
            "source": "polymarket",
            "channel": "market:book:btc",
            "asset": "BTC",
            "event_type": "book",
            "classification": "REJECT_STALE",
            "raw_count": 25,
            "unique_count": 24,
            "duplicate_count": 1,
            "invalid_count": 25,
        }])
        row = store.query_one(
            "SELECT raw_count,unique_count,duplicate_count,invalid_count "
            "FROM event_buckets"
        )
        assert row == {
            "raw_count": 25,
            "unique_count": 24,
            "duplicate_count": 1,
            "invalid_count": 25,
        }

        with pytest.raises(ValueError, match="invalid event bucket counts"):
            store.record_event_count_batch([{
                "receipt_ts_ms": NOW+1_000,
                "source": "polymarket", "channel": "market", "asset": "BTC",
                "event_type": "book", "classification": "INVALID",
                "raw_count": 1, "unique_count": 2,
                "duplicate_count": 0, "invalid_count": 1,
            }])
        assert store.query_one(
            "SELECT COUNT(*) AS n FROM event_buckets")["n"] == 1
    finally:
        store.close()


def test_raw_row_cap_deletes_oldest_unlinked_rows_and_preserves_trade_evidence(tmp_path):
    store = V4Store(tmp_path / "raw-row-cap.db")
    try:
        session = seed_session(store)
        context = seed_market_window(store)
        evidence = seed_candidate_entry_context(store, context)
        for sequence in range(1_001):
            receipt_ts_ms = NOW - 1_000_000 + sequence
            store.record_source_event({
                "session_id": session,
                "source": "OKX",
                "channel": "raw-row-cap",
                "event_type": "ticker",
                "asset": "BTC",
                "provider_ts_ms": receipt_ts_ms,
                "receipt_ts_ms": receipt_ts_ms,
                "monotonic_ns": receipt_ts_ms * 1_000_000,
                "sequence_no": sequence,
                "dedupe_key": f"raw-row-cap-{sequence}",
                "payload": {"price": 100 + sequence / 10_000},
            }, now_ms=receipt_ts_ms)

        protected_id = evidence["event_id"]
        before = store.query_one(
            """SELECT COUNT(*) count FROM source_events
               WHERE retention_class='RAW' AND pin_count=0"""
        )["count"]
        assert before == 1_002
        deleted = store.enforce_raw_row_cap(1_000)
        assert deleted == {
            "source_events": 2,
            "book_snapshots": 0,
            "cex_observations": 0,
        }
        assert store.query_one(
            """SELECT COUNT(*) count FROM source_events
               WHERE retention_class='RAW' AND pin_count=0"""
        ) == {"count": 1_000}
        assert store.query_one(
            "SELECT dedupe_key FROM source_events WHERE source_event_id=?",
            (protected_id,),
        ) is not None
        assert store.enforce_raw_row_cap(1_000) == {
            "source_events": 0,
            "book_snapshots": 0,
            "cex_observations": 0,
        }
    finally:
        store.close()


def test_late_admitted_evidence_bypasses_regression_without_double_counting(tmp_path):
    store = V4Store(tmp_path / "late-admitted.db")
    try:
        session = seed_session(store)
        common = {
            "session_id": session,
            "source": "POLYMARKET_CLOB",
            "channel": "market:late-admitted",
            "event_type": "book",
            "asset": "BTC",
        }
        current = store.record_source_event({
            **common,
            "dedupe_key": "current-evidence",
            "provider_ts_ms": NOW - 10,
            "receipt_ts_ms": NOW,
            "monotonic_ns": NOW * 1_000_000,
            "sequence_no": 20,
            "payload": {"version": "current"},
        }, now_ms=NOW)
        assert current["accepted"] is True

        store.record_event_count(
            receipt_ts_ms=NOW + 1,
            source=common["source"],
            channel=common["channel"],
            asset=common["asset"],
            event_type=common["event_type"],
            classification="ACCEPTED",
            unique=True,
            duplicate=False,
            invalid=False,
        )
        late_payload = {
            **common,
            "dedupe_key": "late-trade-evidence",
            "provider_ts_ms": NOW - 1_000,
            "receipt_ts_ms": NOW + 1,
            "monotonic_ns": (NOW + 1) * 1_000_000,
            "sequence_no": 10,
            "payload": {"version": "late-but-already-admitted"},
        }
        late = store.record_source_event(
            late_payload,
            now_ms=NOW + 1,
            count_in_bucket=False,
            admitted_at_receipt=True,
        )
        duplicate = store.record_source_event(
            late_payload,
            now_ms=NOW + 1,
            count_in_bucket=False,
            admitted_at_receipt=True,
        )

        assert late["accepted"] is True
        assert late["admitted_at_receipt"] is True
        assert duplicate["duplicate"] is True
        assert duplicate["classification"] == "DUPLICATE"
        assert store.query_one(
            """SELECT last_provider_ts_ms,last_sequence FROM source_cursors
               WHERE session_id=? AND source=? AND channel=? AND connection_epoch=0""",
            (session, common["source"], common["channel"]),
        ) == {"last_provider_ts_ms": NOW - 10, "last_sequence": 20}
        assert store.query_one(
            """SELECT SUM(raw_count) raw,SUM(unique_count) uniq,
                      SUM(duplicate_count) dup
               FROM event_buckets"""
        ) == {"raw": 2, "uniq": 2, "dup": 0}
        assert store.query_one("SELECT COUNT(*) count FROM source_events") == {
            "count": 2
        }
    finally:
        store.close()


def test_entry_bundle_ack_contains_committed_position_without_followup_read(tmp_path):
    store = V4Store(tmp_path / "entry-position-ack.db")
    try:
        seed_session(store)
        context = seed_market_window(store, suffix="position-ack")
        evidence = seed_candidate_entry_context(store, context)
        reservation = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?", (context["window_id"],))
        result = store.reserve_and_create_entry_bundle(
            reservation,
            entry_payload(context, evidence, idem="position-ack-entry"),
            max_concurrent_positions=4,
            cohort=ACTIVE_COHORT,
            starting_equity_usd=130.0,
            max_exposure_pct=1.0,
            exit_fee_buffer_usd=0.1075,
        )
        assert result["entry_id"] > 0
        assert result["position_id"] == result["position"]["position_id"]
        assert result["position"]["entry_id"] == result["entry_id"]
        assert result["position"]["status"] == "OPEN"
        assert result["position"]["shares"] == 5.0
    finally:
        store.close()


def test_validated_cex_feature_horizons_are_not_reclassified_by_bundle_order(tmp_path):
    store = V4Store(tmp_path / "validated-horizons.db")
    try:
        session = seed_session(store)
        context = seed_market_window(store, suffix="validated-horizons")
        base = {
            "session_id": session,
            "provider": "OKX",
            "instrument": "BTC-USDT",
            "asset": "BTC",
            "event_type": "ticker",
            "price": 100.0,
            "bid": 99.9,
            "ask": 100.1,
            "receipt_ts_ms": NOW,
            "monotonic_ns": NOW * 1_000_000,
            "classification": "NEW_TICK",
            "fresh": True,
        }
        source_reference = {
            "value": {
                "session_id": session,
                "source": "OKX",
                "channel": "tickers:BTC-USDT",
                "event_type": "ticker",
                "asset": "BTC",
                "dedupe_key": "validated-horizon-source",
                "provider_ts_ms": NOW-10,
                "receipt_ts_ms": NOW,
                "monotonic_ns": NOW * 1_000_000,
                "sequence_no": 20,
                "payload": {"price": 100},
            },
            "kwargs": {"session_id": session},
        }
        result = store.persist_evaluation_bundle({
            "cex": [
                {"value": {**base, "event_id": "newest", "provider_ts_ms": NOW-10,
                            "sequence_no": 20},
                 "kwargs": {"session_id": session,
                            "already_validated_at_receipt": True},
                 "source_event": source_reference,
                 "role": "POINT_IN_TIME_FEATURE", "horizon_ms": 100,
                 "evidence_age_ms": 10},
                {"value": {**base, "event_id": "older", "provider_ts_ms": NOW-1_000,
                            "sequence_no": 10},
                 "kwargs": {"session_id": session,
                            "already_validated_at_receipt": True},
                 "source_event": source_reference,
                 "role": "POINT_IN_TIME_FEATURE", "horizon_ms": 1_000,
                 "evidence_age_ms": 1_000},
            ],
            "candidate": {
                "session_id": session,
                "window_id": context["window_id"],
                "market_identity_id": context["identity_id"],
                "evaluation_ts_ms": NOW,
                "monotonic_ns": NOW * 1_000_000,
                "evaluation_seq": 99,
                "status": "NO_EDGE",
                "regime": "QUIET",
                "fair_probability_yes": 0.5,
                "fair_probability_no": 0.5,
                "calibrated": False,
                "reliability": 0.5,
                "positive_edge": False,
            },
            "fair_value": {
                "calculation": {
                    "phase": "FINAL",
                    "calculation_seq": 1,
                    "calculated_ts_ms": NOW,
                    "monotonic_ns": NOW * 1_000_000,
                    "regime": "QUIET",
                    "fair_probability_yes": 0.5,
                    "fair_probability_no": 0.5,
                    "calibrated": False,
                    "calibration_label": "UNCALIBRATED",
                },
                "sides": [],
            },
            "decision": {
                "decision_seq": 1,
                "decision_ts_ms": NOW,
                "monotonic_ns": NOW * 1_000_000,
                "phase": "FINAL",
                "action": "SKIP",
                "economic_gate_passed": False,
                "exact_depth_passed": False,
                "evidence_fresh": True,
                "reason": "no_positive_edge",
            },
        })
        assert len(result["cex_observation_ids"]) == 2
        assert result["cex_source_event_ids"][0] == result["cex_source_event_ids"][1]
        assert store.query(
            "SELECT event_id,classification,fresh,invalid_reason "
            "FROM cex_observations ORDER BY provider_ts_ms DESC"
        ) == [
            {"event_id": "newest", "classification": "NEW_TICK",
             "fresh": 1, "invalid_reason": None},
            {"event_id": "older", "classification": "NEW_TICK",
             "fresh": 1, "invalid_reason": None},
        ]
        assert store.query_one(
            "SELECT COUNT(*) count FROM candidate_cex_evidence "
            "WHERE candidate_id=?", (result["candidate_id"],)
        ) == {"count": 2}
        assert store.query_one(
            "SELECT duplicate_count FROM source_events WHERE dedupe_key=?",
            ("validated-horizon-source",),
        ) == {"duplicate_count": 0}
    finally:
        store.close()


def test_reference_only_source_resolution_never_inflates_duplicate_telemetry(tmp_path):
    store = V4Store(tmp_path / "reference-only-source.db")
    try:
        session = seed_session(store)
        event = {
            "session_id": session,
            "source": "OKX",
            "channel": "tickers:BTC-USDT",
            "event_type": "ticker",
            "asset": "BTC",
            "dedupe_key": "already-admitted-event",
            "provider_ts_ms": NOW-1,
            "receipt_ts_ms": NOW,
            "monotonic_ns": NOW * 1_000_000,
            "sequence_no": 1,
            "payload": {"price": 100},
        }
        first = store.record_source_event(
            event, now_ms=NOW, count_in_bucket=False,
            admitted_at_receipt=True, reference_only=True)
        second = store.record_source_event(
            event, now_ms=NOW, count_in_bucket=False,
            admitted_at_receipt=True, reference_only=True)
        row = store.query_one(
            "SELECT duplicate_count,last_duplicate_receipt_ts_ms "
            "FROM source_events WHERE source_event_id=?",
            (first["source_event_id"],),
        )
        assert first["inserted"] is True
        assert second["inserted"] is False
        assert second["duplicate"] is False
        assert second["accepted"] is True
        assert row == {"duplicate_count": 0,
                       "last_duplicate_receipt_ts_ms": None}
        assert store.query_one(
            "SELECT COUNT(*) count FROM event_buckets") == {"count": 0}
    finally:
        store.close()


def test_startup_reconciliation_abandons_only_proven_absent_owner_makers(tmp_path):
    store = V4Store(tmp_path / "maker-startup-reconcile.db")
    try:
        seed_session(store, session_id="absent-session")
        absent_context = seed_market_window(
            store, session_id="absent-session", suffix="absent-maker")
        absent_evidence = seed_candidate_entry_context(store, absent_context)
        seed_session(store, session_id="current-session")
        current_context = seed_market_window(
            store, session_id="current-session", open_ts=NOW-600_000,
            suffix="current-maker")
        current_evidence = seed_candidate_entry_context(store, current_context)

        def maker(context, evidence):
            return store.record_maker_observation({
                "window_id": context["window_id"],
                "candidate_id": evidence["candidate_id"],
                "decision_id": evidence["decision_id"],
                "initial_fair_value_calculation_id": evidence["fair_id"],
                "initial_book_snapshot_id": evidence["book_id"],
                "maker_start_ts_ms": evidence["entry_ts"],
                "maker_deadline_ts_ms": evidence["entry_ts"] + 1_000,
                "start_monotonic_ns": evidence["entry_ts"] * 1_000_000,
                "maker_target_price": 0.48,
                "chase_cap_price": 0.50,
                "initial_net_edge": 0.015,
                "maker_fill_assumed": False,
            })

        absent_maker = maker(absent_context, absent_evidence)
        current_maker = maker(current_context, current_evidence)
        result = store.reconcile_startup_state(
            current_launch_nonce="nonce-current-session",
            proven_absent_launch_nonces=("nonce-absent-session",),
            reconciled_ts_ms=NOW+5_000,
        )
        assert result["unfinished_maker_observations"] == 2
        assert result["reconciled_abandoned_maker_observations"] == 1
        assert result["unfinished_makers_left_fail_closed"] == 1
        assert store.query_one(
            "SELECT maker_end_ts_ms,outcome,reason,maker_fill_assumed "
            "FROM maker_observations WHERE maker_observation_id=?",
            (absent_maker,),
        ) == {
            "maker_end_ts_ms": NOW+5_000,
            "outcome": "ABANDONED",
            "reason": "STARTUP_RECONCILED",
            "maker_fill_assumed": 0,
        }
        assert store.query_one(
            "SELECT maker_end_ts_ms,outcome,reason,maker_fill_assumed "
            "FROM maker_observations WHERE maker_observation_id=?",
            (current_maker,),
        ) == {
            "maker_end_ts_ms": None,
            "outcome": None,
            "reason": None,
            "maker_fill_assumed": 0,
        }
        assert store.query_one("SELECT COUNT(*) count FROM entries") == {"count": 0}
    finally:
        store.close()


def test_startup_reconciliation_abandons_makers_of_durably_ended_sessions(
        tmp_path):
    """A durably journaled session end proves its makers cannot complete.

    A graceful stop removes the process lock, so the next launch may hold no
    external nonce proof at all.  The session's own terminal
    end_runtime_session record is deterministic database-internal proof that
    the owning process finished; without it the unfinished maker would latch
    the execution gate fail-closed forever across every restart.
    """

    store = V4Store(tmp_path / "maker-ended-session-reconcile.db")
    try:
        seed_session(store, session_id="ended-session")
        ended_context = seed_market_window(
            store, session_id="ended-session", suffix="ended-maker")
        ended_evidence = seed_candidate_entry_context(store, ended_context)
        ended_maker = store.record_maker_observation({
            "window_id": ended_context["window_id"],
            "candidate_id": ended_evidence["candidate_id"],
            "decision_id": ended_evidence["decision_id"],
            "initial_fair_value_calculation_id": ended_evidence["fair_id"],
            "initial_book_snapshot_id": ended_evidence["book_id"],
            "maker_start_ts_ms": ended_evidence["entry_ts"],
            "maker_deadline_ts_ms": ended_evidence["entry_ts"] + 1_000,
            "start_monotonic_ns": ended_evidence["entry_ts"] * 1_000_000,
            "maker_target_price": 0.48,
            "chase_cap_price": 0.50,
            "initial_net_edge": 0.015,
            "maker_fill_assumed": False,
        })
        store.end_runtime_session("ended-session", NOW + 1_000, "graceful_stop")

        # No external nonce proof at all: the recorded session end alone
        # must finish the maker honestly, without assuming a fill.
        result = store.reconcile_startup_state(
            current_launch_nonce="nonce-new-session",
            proven_absent_launch_nonces=(),
            reconciled_ts_ms=NOW + 5_000,
        )
        assert result["unfinished_maker_observations"] == 1
        assert result["reconciled_abandoned_maker_observations"] == 1
        assert result["unfinished_makers_left_fail_closed"] == 0
        assert store.query_one(
            "SELECT maker_end_ts_ms,outcome,reason,maker_fill_assumed "
            "FROM maker_observations WHERE maker_observation_id=?",
            (ended_maker,),
        ) == {
            "maker_end_ts_ms": NOW + 5_000,
            "outcome": "ABANDONED",
            "reason": "STARTUP_RECONCILED",
            "maker_fill_assumed": 0,
        }
        assert store.query_one("SELECT COUNT(*) count FROM entries") == {"count": 0}
    finally:
        store.close()


def test_bounded_retention_prunes_only_nontrade_graph_and_linked_raw_evidence(tmp_path):
    store = V4Store(tmp_path / "bounded-graph-retention.db")
    try:
        seed_session(store)
        trade_context = seed_market_window(
            store, open_ts=NOW-600_000, suffix="retained-trade")
        trade_evidence = seed_candidate_entry_context(store, trade_context, seq=1)
        trade_entry = create_entry(
            store, entry_payload(trade_context, trade_evidence,
                                 idem="retained-trade-entry"))

        raw_context = seed_market_window(
            store, open_ts=NOW-900_000, suffix="pruned-nontrade")
        raw_evidence = seed_candidate_entry_context(store, raw_context, seq=2)
        raw_lock = store.query_one(
            "SELECT * FROM window_locks WHERE window_id=?", (raw_context["window_id"],))
        assert store.release_window_reservation(
            window_id=raw_context["window_id"],
            session_id=raw_context["session_id"],
            owner_launch_nonce=raw_lock["owner_launch_nonce"],
            idempotency_key=raw_lock["idempotency_key"],
        )

        for _ in range(30):
            result = store.bounded_retention_step(
                cutoff_ts_ms=NOW-100_000,
                max_rows=250,
                deadline_monotonic=time.monotonic()+1.0,
                protect_trade_evidence=True,
                raw_event_max_rows=250_000,
                now_ms=NOW,
            )
            if result["action"] == "no_eligible_rows":
                break

        assert store.query_one(
            "SELECT candidate_id FROM candidates WHERE candidate_id=?",
            (raw_evidence["candidate_id"],),
        ) is None
        assert store.query_one(
            "SELECT source_event_id FROM source_events WHERE source_event_id=?",
            (raw_evidence["event_id"],),
        ) is None
        assert store.query_one(
            "SELECT book_snapshot_id FROM book_snapshots WHERE book_snapshot_id=?",
            (raw_evidence["book_id"],),
        ) is None
        assert store.query_one(
            "SELECT cex_observation_id FROM cex_observations "
            "WHERE cex_observation_id=?", (raw_evidence["cex_id"],),
        ) is None
        assert store.query_one(
            "SELECT candidate_id FROM candidates WHERE candidate_id=?",
            (trade_evidence["candidate_id"],),
        ) is not None
        assert store.query_one(
            "SELECT entry_id FROM entries WHERE entry_id=?", (trade_entry,)
        ) == {"entry_id": trade_entry}
        assert store.query_one(
            "SELECT retention_class,pin_count FROM source_events "
            "WHERE source_event_id=?", (trade_evidence["event_id"],)
        )["retention_class"] == "TRADE_EVIDENCE"
        assert store.integrity_check() == {
            "integrity": "ok", "foreign_key_violations": []}
    finally:
        store.close()


def test_journal_payload_compaction_keeps_idempotency_and_protected_commands(tmp_path):
    store = V4Store(tmp_path / "journal-retention.db")
    try:
        seed_session(store)
        context = seed_market_window(
            store, open_ts=NOW-600_000, suffix="journal-trade")
        evidence = seed_candidate_entry_context(store, context)
        create_entry(store, entry_payload(context, evidence, idem="journal-trade-entry"))

        def insert_command(name, *, status="COMMITTED", terminal=0,
                           window_id=None, trade_id=None):
            payload_hash = (name[0] * 64)[:64]
            values = {
                "command_id": name,
                "command_type": "EVALUATION",
                "method": "persist_evaluation_bundle",
                "idempotency_key": f"idem-{name}",
                "ordering_key": f"order-{name}",
                "priority": 10,
                "terminal": terminal,
                "associated_window_id": window_id,
                "associated_trade_id": trade_id,
                "payload_hash": payload_hash,
                "payload_json": '{"large":"' + ("x" * 1000) + '"}',
                "status": status,
                "attempt_count": 1,
                "submitted_ts_ms": NOW-20_000,
                "started_ts_ms": NOW-19_000,
            }
            if status == "COMMITTED":
                values.update({
                    "committed_ts_ms": NOW-18_000,
                    "completed_ts_ms": NOW-18_000,
                    "result_json": '{"candidate_id":1}',
                })
            with store.transaction(immediate=True) as conn:
                return store._insert("persistence_commands", values, conn=conn)

        insert_command("compactable")
        insert_command("terminal", terminal=1)
        insert_command("trade", window_id=context["window_id"])
        insert_command("ambiguous", status="EXECUTING")
        before = {
            row["command_id"]: row for row in store.query(
                "SELECT command_id,idempotency_key,payload_hash,payload_json,"
                "result_json,status FROM persistence_commands")
        }

        for _ in range(20):
            result = store.bounded_retention_step(
                cutoff_ts_ms=0,
                max_rows=10,
                deadline_monotonic=time.monotonic()+1.0,
                protect_trade_evidence=True,
                journal_payload_retention_ms=1_000,
                now_ms=NOW,
            )
            if result["metrics"].get("journal_payloads_compacted"):
                break

        after = {
            row["command_id"]: row for row in store.query(
                "SELECT command_id,idempotency_key,payload_hash,payload_json,"
                "result_json,status FROM persistence_commands")
        }
        assert after["compactable"]["payload_json"].startswith(
            '{"compacted":true,"payload_hash":')
        for field in ("idempotency_key", "payload_hash", "result_json", "status"):
            assert after["compactable"][field] == before["compactable"][field]
        for name in ("terminal", "trade", "ambiguous"):
            assert after[name] == before[name]
    finally:
        store.close()


def test_bounded_retention_deadline_prevents_any_transaction(tmp_path):
    store = V4Store(tmp_path / "retention-deadline.db")
    try:
        seed_session(store)
        before = store.transaction_counters
        result = store.bounded_retention_step(
            cutoff_ts_ms=NOW,
            max_rows=10,
            deadline_monotonic=time.monotonic()-1.0,
            protect_trade_evidence=True,
            now_ms=NOW,
        )
        assert result["deadline_exhausted"] is True
        assert result["budget_units"] == 0
        assert store.transaction_counters == before
    finally:
        store.close()


def test_bounded_retention_deadline_caps_sqlite_lock_wait(tmp_path):
    path = tmp_path / "retention-lock-deadline.db"
    owner = V4Store(path)
    contender = V4Store(path)
    try:
        seed_session(owner)
        started = time.monotonic()
        with owner.transaction(immediate=True):
            result = contender.bounded_retention_step(
                cutoff_ts_ms=NOW,
                max_rows=10,
                deadline_monotonic=time.monotonic()+0.02,
                protect_trade_evidence=True,
                now_ms=NOW,
            )
        elapsed = time.monotonic() - started
        assert result["deadline_exhausted"] is True
        assert result["budget_units"] == 0
        assert elapsed < 0.5
        assert contender.query_one("PRAGMA busy_timeout") == {
            "timeout": contender.busy_timeout_ms}
    finally:
        contender.close()
        owner.close()


def test_bounded_retention_rolls_buckets_and_bounds_maintenance_metadata(tmp_path):
    store = V4Store(tmp_path / "bounded-metadata.db")
    try:
        seed_session(store)
        store.record_event_count(
            receipt_ts_ms=NOW-10_000,
            source="OKX", channel="ticker", asset="BTC",
            event_type="ticker", classification="ACCEPTED",
            unique=True, duplicate=False, invalid=False,
        )
        store.record_event_count(
            receipt_ts_ms=NOW,
            source="OKX", channel="ticker", asset="BTC",
            event_type="ticker", classification="ACCEPTED",
            unique=True, duplicate=False, invalid=False,
        )
        sample = {
            "worker_thread_id": 123,
            "state": "HEALTHY",
            "queue_depth": 0,
            "queue_capacity": 100,
            "queue_high_water": 1,
            "oldest_queue_age_ms": 0,
            "commands_submitted": 1,
            "commands_committed": 1,
            "commands_failed": 0,
            "commands_retried": 0,
            "idempotent_replays": 0,
            "queue_full_count": 0,
            "timeout_count": 0,
            "transactions_started": 1,
            "transactions_committed": 1,
            "transactions_rolled_back": 0,
        }
        for ts in (NOW-10_000, NOW):
            store.record_persistence_worker_sample(
                {**sample, "sample_ts_ms": ts})
            with store.transaction(immediate=True) as conn:
                store._insert("checkpoint_runs", {
                    "started_ts_ms": ts,
                    "completed_ts_ms": ts,
                    "mode": "PASSIVE",
                    "reason": "test",
                    "before_wal_bytes": 0,
                    "after_wal_bytes": 0,
                    "duration_ms": 0.0,
                    "database_bytes": 0,
                    "success": 1,
                }, conn=conn)
                store._insert("retention_runs", {
                    "started_ts_ms": ts,
                    "completed_ts_ms": ts,
                    "raw_cutoff_ts_ms": max(0, ts-1_000),
                    "requested_batch_size": 1,
                }, conn=conn)

        aggregate_metrics = {}
        for _ in range(20):
            result = store.bounded_retention_step(
                cutoff_ts_ms=0,
                max_rows=10,
                deadline_monotonic=time.monotonic()+1.0,
                protect_trade_evidence=True,
                event_bucket_detail_retention_ms=1_000,
                metadata_retention_ms=1_000,
                metadata_max_rows=1_000,
                now_ms=NOW,
            )
            aggregate_metrics.update(result["metrics"])
            if result["action"] == "no_eligible_rows":
                break

        assert aggregate_metrics["event_bucket_detail_rows_compacted"] == 1
        assert aggregate_metrics["persistence_worker_samples_deleted"] == 1
        assert aggregate_metrics["checkpoint_runs_deleted"] == 1
        assert aggregate_metrics["retention_runs_deleted"] == 1
        assert store.query_one(
            "SELECT COUNT(*) count FROM event_buckets WHERE bucket_ms=1000"
        ) == {"count": 1}
        assert store.query_one(
            "SELECT SUM(raw_count) raw FROM event_buckets WHERE bucket_ms=60000"
        ) == {"raw": 1}
        assert store.query_one(
            "SELECT COUNT(*) count FROM persistence_worker_samples"
        ) == {"count": 1}
        assert store.query_one(
            "SELECT COUNT(*) count FROM checkpoint_runs") == {"count": 1}
        assert store.query_one(
            "SELECT COUNT(*) count FROM retention_runs") == {"count": 1}
    finally:
        store.close()


def test_background_gate_defers_outer_write_and_checkpoint_without_leak(tmp_path):
    path = tmp_path / "background-gate.db"
    initial = V4Store(path)
    seed_session(initial)
    initial.close()
    calls = {"admit": 0, "release": 0}

    def deny():
        calls["admit"] += 1
        return False

    def release():
        calls["release"] += 1

    store = V4Store(
        path,
        background_write_admission=deny,
        background_write_release=release,
    )
    try:
        with pytest.raises(V4BackgroundWriteDeferred):
            store.record_runtime_health({
                "session_id": "session-v4",
                "sample_ts_ms": NOW,
                "heartbeat_ts_ms": NOW,
                "pid": 123,
                "state": "RUNNING",
                "loop_lag_ms": 0,
            })
        with pytest.raises(V4BackgroundWriteDeferred):
            store.checkpoint(mode="PASSIVE", reason="critical_pending")
        assert calls == {"admit": 2, "release": 0}
        assert store.query_one(
            "SELECT COUNT(*) count FROM runtime_health") == {"count": 0}
        assert store.query_one(
            "SELECT COUNT(*) count FROM checkpoint_runs") == {"count": 0}
    finally:
        store.close()


def test_background_gate_releases_once_per_outer_transaction(tmp_path):
    calls = {"admit": 0, "release": 0}

    def admit():
        calls["admit"] += 1
        return True

    def release():
        calls["release"] += 1

    store = V4Store(
        tmp_path / "background-gate-release.db",
        background_write_admission=admit,
        background_write_release=release,
    )
    try:
        # seed_session performs two outer transactions (cohort + session),
        # each admitting and releasing the gate exactly once.
        seed_session(store)
        assert calls == {"admit": 2, "release": 2}
        with store.transaction(immediate=True):
            store.record_runtime_health({
                "session_id": "session-v4",
                "sample_ts_ms": NOW,
                "heartbeat_ts_ms": NOW,
                "pid": 123,
                "state": "RUNNING",
                "loop_lag_ms": 0,
            })
        assert calls == {"admit": 3, "release": 3}
    finally:
        store.close()


# --- STORE-DE managed-object census (Human Authority Ruling 2) -----------
#
# The managed-v5 fingerprint selects only the eight managed objects by
# name, so it is structurally blind to a ninth object attached to a
# managed table (C1.B probes D/E in the read-only review).  The separate
# fail-closed census check refuses any extra index, trigger, table, or
# view in the managed namespace without mutating the database.

# The authoritative literals the census must preserve:
_STORE_DE_FINGERPRINT = (
    "6448f0dc66225f55cbe2a5b395f14fc9e193bf8556a713caf896846574bdc715"
)
_STORE_DE_PAYLOAD_BYTES = 4603
_STORE_DE_MANAGED_COUNT = 8


def _fresh_v5_db(tmp_path):
    """Build a fresh managed-v5 database and return its path.

    Construction through V4Store lands on exact managed v5 (v4 base then
    the additive v5 migration).  Returns the path; the caller reopens
    directly with sqlite3 for the probe.
    """
    path = tmp_path / "v5.db"
    store = V4Store(path)
    store.close()
    return path


def test_STORE_DE_valid_eight_object_schema_is_accepted(tmp_path):
    """The canonical eight-object schema passes the census, and the
    fingerprint, payload, and managed count remain the authority literals.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_fingerprint, managed_v5_records, managed_v5_object_census,
    )
    import json
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        # The census accepts the valid schema.
        census = managed_v5_object_census(conn)
        assert len(census) == _STORE_DE_MANAGED_COUNT
        # The fingerprint remains the authority literal.
        assert managed_v5_fingerprint(conn) == _STORE_DE_FINGERPRINT
        # The payload remains the authority byte count.
        records = managed_v5_records(conn)
        payload = json.dumps(
            records, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            allow_nan=False).encode("utf-8")
        assert len(payload) == _STORE_DE_PAYLOAD_BYTES
        # integrity_check / foreign_key_check stay clean.
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_STORE_DE_extra_explicit_index_on_managed_table_is_refused(tmp_path):
    """C1.B probe D: a ninth explicit index on a managed table is refused
    by the census (the named-selection fingerprint alone would miss it).
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census, verify_managed_v5_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE INDEX ix_extra_probe_d "
                     "ON cluster_arbitration_events(seq)")
        conn.commit()
        with pytest.raises(ValueError, match="unexpected managed-namespace object"):
            managed_v5_object_census(conn)
    finally:
        conn.close()


def test_STORE_DE_extra_trigger_on_managed_table_is_refused(tmp_path):
    """C1.B probe E: a trigger on a managed table is refused by the census.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TRIGGER tr_extra_probe_e AFTER INSERT ON cluster_locks "
            "BEGIN SELECT 1; END")
        conn.commit()
        with pytest.raises(ValueError, match="unexpected managed-namespace object"):
            managed_v5_object_census(conn)
    finally:
        conn.close()


def test_STORE_DE_unrelated_app_table_outside_namespace_not_rejected(tmp_path):
    """A user-authored table OUTSIDE the managed namespace (different name
    AND different tbl_name) is not falsely rejected by the census.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE app_unrelated_log("
                     "id INTEGER PRIMARY KEY, note TEXT)")
        conn.execute("CREATE INDEX ix_app_unrelated_note "
                     "ON app_unrelated_log(note)")
        conn.commit()
        # Census still sees exactly the eight managed objects.
        census = managed_v5_object_census(conn)
        assert len(census) == _STORE_DE_MANAGED_COUNT
        assert {row["name"] for row in census} == {
            "cluster_arbitrations", "cluster_arbitration_events",
            "cluster_locks", "cohort_pilot_starts",
            "ix_cohorts_single_authoritative", "ix_cluster_arbitrations_status",
            "ix_cluster_events_cluster", "ix_cluster_locks_state"}
    finally:
        conn.close()


def test_STORE_DE_sqlite_internal_and_implicit_autoindexes_excluded(tmp_path):
    """SQLite internal objects (sqlite_*) and implicit autoindexes
    (sql IS NULL) are excluded from the census even though they attach to
    managed tables.  The canonical schema carries several such implicit
    indexes (PRIMARY KEY / UNIQUE autoindexes); the census must see 8, not
    more.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        # Sanity: the fresh v5 schema DOES contain implicit autoindexes
        # (sqlite_autoindex_*) on the managed tables.  These must be
        # excluded by name prefix and by sql IS NULL.
        auto = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE name LIKE 'sqlite_autoindex_%' AND sql IS NULL").fetchone()[0]
        assert auto >= 1, "expected implicit autoindexes in the canonical schema"
        census = managed_v5_object_census(conn)
        assert len(census) == _STORE_DE_MANAGED_COUNT
    finally:
        conn.close()


def test_STORE_DE_sqlite_literal_prefix_does_not_hide_managed_object(tmp_path):
    """Only the literal ``sqlite_`` internal namespace is excluded.

    An explicit object named ``sqliteX...`` is user-authored, so the ``X``
    must not satisfy an unescaped underscore wildcard and hide an otherwise
    forbidden index attached to a managed table.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE INDEX sqliteXmanaged_probe "
            "ON cluster_locks(updated_ts_ms)")
        conn.commit()
        with pytest.raises(
                ValueError,
                match="unexpected managed-namespace object",
        ) as exc_info:
            managed_v5_object_census(conn)
        assert "sqliteXmanaged_probe" in str(exc_info.value)
    finally:
        conn.close()


def test_STORE_DE_refusal_does_not_modify_user_version_or_schema(tmp_path):
    """A census refusal must not mutate user_version, schema_migrations,
    or any managed object.  The check is read-only.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census, SCHEMA_VERSION,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE INDEX ix_refuse_probe "
                     "ON cluster_arbitrations(created_ts_ms)")
        conn.commit()
        uv_before = int(conn.execute("PRAGMA user_version").fetchone()[0])
        sm_before = conn.execute(
            "SELECT version, schema_hash FROM schema_migrations "
            "ORDER BY version").fetchall()
        with pytest.raises(ValueError):
            managed_v5_object_census(conn)
        uv_after = int(conn.execute("PRAGMA user_version").fetchone()[0])
        sm_after = conn.execute(
            "SELECT version, schema_hash FROM schema_migrations "
            "ORDER BY version").fetchall()
        assert uv_before == uv_after == SCHEMA_VERSION
        assert sm_before == sm_after
    finally:
        conn.close()


def test_STORE_DE_integrity_and_fk_check_ok_after_refusal(tmp_path):
    """After a census refusal, the database still passes integrity_check
    and foreign_key_check (the check must not corrupt anything).
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE INDEX ix_integrity_probe "
                     "ON cluster_locks(updated_ts_ms)")
        conn.commit()
        with pytest.raises(ValueError):
            managed_v5_object_census(conn)
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_STORE_DE_store_open_refuses_extra_managed_object(tmp_path):
    """End-to-end: a database carrying a ninth managed-namespace object
    must be refused when reopened through V4Store (not just by the bare
    census helper).  This proves the census is wired into the open path.

    The census raises ``ValueError`` (the same exception family the
    fingerprint's ``managed_v5_records`` raises); V4Store does not
    swallow it.
    """
    path = _fresh_v5_db(tmp_path)
    # Add the extra object via a direct connection, then reopen via V4Store.
    conn = sqlite3.connect(path)
    conn.execute("CREATE INDEX ix_e2e_probe ON cluster_arbitrations(updated_ts_ms)")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="unexpected managed-namespace object"):
        V4Store(path)


def test_STORE_DE_shadow_table_with_managed_prefix_is_refused(tmp_path):
    """A standalone table whose name shadows a managed-table prefix
    (e.g. 'cluster_arbitrations_shadow_probe') is refused by the census
    as a same-namespace imitation, even though its tbl_name is itself
    and it is not one of the eight canonical names.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE cluster_arbitrations_shadow_probe("
                     "id INTEGER PRIMARY KEY)")
        conn.commit()
        with pytest.raises(ValueError, match="unexpected managed-namespace object"):
            managed_v5_object_census(conn)
    finally:
        conn.close()


def test_STORE_DE_out_of_namespace_view_is_not_falsely_rejected(tmp_path):
    """Boundary check: a view whose name carries no managed prefix and
    whose tbl_name is not a managed table is OUT of the managed
    namespace and must NOT be falsely rejected, even if its body
    references a managed table.  The namespace is defined by object name
    and tbl_name only (SQL-text scanning is deliberately not performed).
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE VIEW v_cluster_arbitrations_probe "
                     "AS SELECT cohort FROM cluster_arbitrations")
        conn.commit()
        # Out-of-namespace -> census still sees exactly the eight.
        census = managed_v5_object_census(conn)
        assert len(census) == _STORE_DE_MANAGED_COUNT
    finally:
        conn.close()


def test_STORE_DE_app_table_with_cluster_prefix_not_falsely_rejected(tmp_path):
    """Boundary check: an application table whose name shares a broader
    prefix (e.g. 'cluster_app_log') but is NOT a managed-table prefix
    ('cluster_arbitrations', etc.) is OUT of the managed namespace and
    must not be falsely rejected.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE cluster_app_log("
                     "id INTEGER PRIMARY KEY, note TEXT)")
        conn.commit()
        census = managed_v5_object_census(conn)
        assert len(census) == _STORE_DE_MANAGED_COUNT
    finally:
        conn.close()


# --- STORE-DE regression: SQLite LIKE false positive ---------------------
#
# The managed-object census builds a SQLite ``LIKE ? ESCAPE '\\'`` prefix
# pattern for shadow detection.  The original implementation escaped only
# the appended separator underscore, leaving the underscores already present
# inside managed table names (e.g. the '_' in 'cluster_arbitrations') as
# unescaped single-char wildcards.  As a result a pattern meant to represent
# ``cluster_arbitrations_%`` could wrongly accept an unrelated
# ``clusterXarbitrations_probe`` (the internal '_' matched the 'X').
#
# The fix escapes the whole literal table name before appending the escaped
# separator underscore and the wildcard, so the pattern matches only names
# beginning with ``<exact managed table>_``.  These tests pin the fix.


@pytest.mark.parametrize("probe_table", [
    "clusterXarbitrations_probe",      # would match if internal '_' were a wildcard
    "clusterXlocks_probe",
    "cohortXpilotXstarts_probe",
    "clusterXarbitrationXevents_probe",
])
def test_STORE_DE_false_positive_like_pattern_accepts_unrelated_probe(
        tmp_path, probe_table):
    """STORE-DE false-positive regression: an unrelated application table
    whose name differs from a managed table only by characters that an
    unescaped '_' wildcard could absorb must be ACCEPTED (not refused).

    Before the fix, the LIKE pattern 'cluster_arbitrations\\_%' treated the
    internal underscores as wildcards and matched 'clusterXarbitrations_probe',
    falsely refusing an unrelated object.  The escaped pattern must match
    only names beginning with the exact literal 'cluster_arbitrations_'.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            f"CREATE TABLE {probe_table}(id INTEGER PRIMARY KEY, note TEXT)")
        conn.commit()
        census = managed_v5_object_census(conn)
        assert len(census) == _STORE_DE_MANAGED_COUNT
        assert probe_table not in {row["name"] for row in census}
    finally:
        conn.close()


@pytest.mark.parametrize("probe_table,managed_table", [
    ("cluster_arbitrations_probe", "cluster_arbitrations"),
    ("cluster_locks_probe", "cluster_locks"),
    ("cohort_pilot_starts_probe", "cohort_pilot_starts"),
    ("cluster_arbitration_events_probe", "cluster_arbitration_events"),
])
def test_STORE_DE_exact_managed_prefix_shadow_is_refused(
        tmp_path, probe_table, managed_table):
    """STORE-DE regression: a table whose name begins with an exact managed
    table name followed by a literal underscore ('<table>_...') IS in the
    managed namespace and must be REFUSED as a same-namespace shadow.

    This is the positive side of the same fix: after escaping the whole
    table name, the literal prefix still matches a genuine shadow object.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            f"CREATE TABLE {probe_table}(id INTEGER PRIMARY KEY)")
        conn.commit()
        with pytest.raises(ValueError,
                           match="unexpected managed-namespace object"):
            managed_v5_object_census(conn)
        # Managed table name referenced only to anchor intent; assert it is
        # present so the test cannot silently pass against a renamed schema.
        assert managed_table in {
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


def test_STORE_DE_escape_helper_produces_no_unescaped_metachars():
    """Unit coverage for the SQLite LIKE literal escaper used by the census.

    The escaped output must contain no unescaped ``%`` or ``_`` so it can be
    used verbatim before appending an escaped separator and wildcard.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        escape_sqlite_like_literal,
    )
    # Backslash must be doubled FIRST so it does not double the later escapes.
    assert escape_sqlite_like_literal("\\") == "\\\\"
    assert escape_sqlite_like_literal("%") == "\\%"
    assert escape_sqlite_like_literal("_") == "\\_"
    # A real managed table name: every internal '_' is escaped.
    assert escape_sqlite_like_literal("cluster_arbitrations") == (
        "cluster\\_arbitrations")
    # Order independence: a string mixing all three metacharacters.
    assert escape_sqlite_like_literal("a%_\\b") == "a\\%\\_\\\\b"
    # Append the escaped separator + wildcard to build the census pattern.
    assert (escape_sqlite_like_literal("cluster_arbitrations") + "\\_%") == (
        "cluster\\_arbitrations\\_%")


def test_STORE_DE_missing_canonical_object_is_refused(tmp_path):
    """STORE-DE regression: removing one canonical managed object from an
    isolated scratch schema is refused, with a deterministic error reason
    that identifies the missing canonical object by name.

    The census's namespace query only returns managed-namespace objects, so a
    missing canonical object surfaces from the set-difference check that names
    the missing objects explicitly.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        # Drop the canonical index ix_cluster_locks_state (and its rows) to
        # model a missing canonical managed object on an otherwise-correct
        # schema.  SQLite permits DROP INDEX on an explicit index.
        conn.execute("DROP INDEX ix_cluster_locks_state")
        conn.commit()
        with pytest.raises(
                ValueError,
                match="missing managed-namespace objects",
        ) as exc_info:
            managed_v5_object_census(conn)
        assert "ix_cluster_locks_state" in str(exc_info.value)
    finally:
        conn.close()


def test_STORE_DE_missing_canonical_table_is_refused(tmp_path):
    """STORE-DE regression: a missing canonical managed TABLE is refused
    with a deterministic error reason naming the missing table.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        # cluster_locks is referenced by an index; drop dependent index first.
        conn.execute("DROP INDEX IF EXISTS ix_cluster_locks_state")
        conn.execute("DROP TABLE cluster_locks")
        conn.commit()
        with pytest.raises(
                ValueError,
                match="missing managed-namespace objects",
        ) as exc_info:
            managed_v5_object_census(conn)
        assert "cluster_locks" in str(exc_info.value)
    finally:
        conn.close()


def test_STORE_DE_missing_canonical_object_refused_through_store_open(tmp_path):
    """STORE-DE regression: V4Store open also refuses a database missing a
    canonical managed object.  The open path runs the managed-v5 fingerprint
    (managed_v5_records) before the census, so a missing canonical object is
    reported by whichever check runs first; both messages deterministically
    name the missing object.
    """
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    conn.execute("DROP INDEX ix_cluster_locks_state")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="missing managed") as exc_info:
        V4Store(path)
    assert "ix_cluster_locks_state" in str(exc_info.value)


def test_STORE_DE_renamed_canonical_object_is_refused(tmp_path):
    """STORE-DE regression: a canonical managed object that has been RENAMED
    (recreated under a non-canonical name) is refused.  The original name is
    then reported as missing, and the new non-canonical name is reported as
    an unexpected namespace object (it carries the managed-table prefix).

    This catches a schema where an attacker keeps the column shape but moves
    a managed object out of its canonical name.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        # Recreate the canonical index under a non-canonical name beginning
        # with the managed-table prefix, then drop the canonical one.  The
        # non-canonical name shares the 'cluster_locks_' prefix, so it is in
        # the managed namespace and must be refused as unexpected; the
        # canonical name is simultaneously missing.
        conn.execute(
            "CREATE INDEX cluster_locks_state_renamed_probe "
            "ON cluster_locks(state)")
        conn.execute("DROP INDEX ix_cluster_locks_state")
        conn.commit()
        with pytest.raises(
                ValueError,
                match="unexpected managed-namespace object",
        ) as exc_info:
            managed_v5_object_census(conn)
        assert "cluster_locks_state_renamed_probe" in str(exc_info.value)
    finally:
        conn.close()


def test_STORE_DE_renamed_canonical_object_refused_through_store_open(tmp_path):
    """V4Store's actual open gate refuses a renamed canonical object.

    The named-object fingerprint runs before the census on an exact-v5 open,
    so this path reports the missing canonical name.  The failed open must not
    change user_version, schema_migrations, or sqlite_master.
    """
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE INDEX cluster_locks_state_renamed_probe "
        "ON cluster_locks(state)")
    conn.execute("DROP INDEX ix_cluster_locks_state")
    conn.commit()
    uv_before = int(conn.execute("PRAGMA user_version").fetchone()[0])
    sm_before = conn.execute(
        "SELECT version, schema_hash FROM schema_migrations "
        "ORDER BY version").fetchall()
    master_before = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "ORDER BY type, name").fetchall()
    conn.close()

    with pytest.raises(ValueError, match="missing managed") as exc_info:
        V4Store(path)
    assert "ix_cluster_locks_state" in str(exc_info.value)

    conn = sqlite3.connect(path)
    try:
        assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == uv_before
        assert conn.execute(
            "SELECT version, schema_hash FROM schema_migrations "
            "ORDER BY version").fetchall() == sm_before
        assert conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "ORDER BY type, name").fetchall() == master_before
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_STORE_DE_false_positive_acceptance_is_read_only(tmp_path):
    """STORE-DE regression: acceptance under the corrected LIKE pattern does
    not mutate user_version, schema_migrations, or any managed object;
    integrity/FK checks stay clean.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census, SCHEMA_VERSION,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE clusterXarbitrations_probe("
                     "id INTEGER PRIMARY KEY, note TEXT)")
        conn.commit()
        uv_before = int(conn.execute("PRAGMA user_version").fetchone()[0])
        sm_before = conn.execute(
            "SELECT version, schema_hash FROM schema_migrations "
            "ORDER BY version").fetchall()
        master_before = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "ORDER BY type, name").fetchall()
        # Accepted (out of namespace) -> no raise, read-only.
        census = managed_v5_object_census(conn)
        assert len(census) == _STORE_DE_MANAGED_COUNT
        uv_after = int(conn.execute("PRAGMA user_version").fetchone()[0])
        sm_after = conn.execute(
            "SELECT version, schema_hash FROM schema_migrations "
            "ORDER BY version").fetchall()
        master_after = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "ORDER BY type, name").fetchall()
        assert uv_before == uv_after == SCHEMA_VERSION
        assert sm_before == sm_after
        assert master_before == master_after
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_STORE_DE_exact_prefix_refusal_is_read_only(tmp_path):
    """STORE-DE regression: an exact-prefix shadow refusal is read-only.
    user_version, schema_migrations, and all managed objects are unchanged;
    sqlite_master differs ONLY by the intentionally-created probe object.
    """
    from poly_alpha_sniper.lite_frequency_v4.store import (
        managed_v5_object_census,
    )
    path = _fresh_v5_db(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE cluster_arbitrations_probe("
                     "id INTEGER PRIMARY KEY)")
        conn.commit()
        uv_before = int(conn.execute("PRAGMA user_version").fetchone()[0])
        sm_before = conn.execute(
            "SELECT version, schema_hash FROM schema_migrations "
            "ORDER BY version").fetchall()
        managed_before = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' "
            "ORDER BY type, name").fetchall()
        with pytest.raises(ValueError):
            managed_v5_object_census(conn)
        uv_after = int(conn.execute("PRAGMA user_version").fetchone()[0])
        sm_after = conn.execute(
            "SELECT version, schema_hash FROM schema_migrations "
            "ORDER BY version").fetchall()
        managed_after = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' "
            "ORDER BY type, name").fetchall()
        assert uv_before == uv_after
        assert sm_before == sm_after
        assert managed_before == managed_after
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()
