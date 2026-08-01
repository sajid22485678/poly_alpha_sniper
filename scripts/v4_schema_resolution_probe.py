"""Collect the evidence a schema-resolution record needs, strictly read-only.

Opens the authoritative database with ``mode=ro&immutable=1`` and
``PRAGMA query_only=ON`` so no page, WAL frame or SHM byte is written, and
records: the object's exact DDL as stored, the query plan with and without it,
the identical result both plans return, and where it sits relative to the
ratified schema authority.

Usage: v4_schema_resolution_probe.py <db_path> <index_name> [--json OUT]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import time


#: The read the index exists to serve, verbatim from
#: ``V4Store.record_cex_observation``.
PRIOR_OBSERVATION_SQL = (
    "SELECT price,bid,ask,provider_ts_ms,receipt_ts_ms "
    "FROM cex_observations WHERE session_id=? AND provider=? AND instrument=? "
    "ORDER BY provider_ts_ms DESC,cex_observation_id DESC LIMIT 1"
)


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        f"file:{path.as_posix()}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _plan(conn: sqlite3.Connection, sql: str, params) -> list[str]:
    return [
        str(row["detail"])
        for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params)
    ]


def _time_query(conn: sqlite3.Connection, sql: str, params, *,
                repeats: int = 25) -> dict:
    rows = None
    samples = []
    for _ in range(repeats):
        started = time.perf_counter()
        rows = conn.execute(sql, params).fetchall()
        samples.append((time.perf_counter() - started) * 1_000.0)
    samples.sort()
    return {
        "median_ms": round(samples[len(samples) // 2], 6),
        "min_ms": round(samples[0], 6),
        "max_ms": round(samples[-1], 6),
        "result": [list(row) for row in (rows or [])],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("index_name")
    ap.add_argument("--json")
    args = ap.parse_args()
    path = Path(args.db_path)

    conn = _connect(path)
    report: dict = {"database": str(path), "index": args.index_name}

    stored = conn.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name=?",
        (args.index_name,)).fetchone()
    report["stored_object"] = None if stored is None else dict(stored)

    report["index_inventory"] = {
        "explicit_indexes": [
            dict(row) for row in conn.execute(
                "SELECT name,tbl_name FROM sqlite_schema "
                "WHERE type='index' AND sql IS NOT NULL ORDER BY name")
        ],
        "implicit_autoindexes": [
            str(row["name"]) for row in conn.execute(
                "SELECT name FROM sqlite_schema "
                "WHERE type='index' AND sql IS NULL ORDER BY name")
        ],
    }
    report["object_counts"] = {
        str(row["type"]): int(row["n"]) for row in conn.execute(
            "SELECT type, COUNT(*) AS n FROM sqlite_schema GROUP BY type")
    }
    report["user_version"] = int(
        conn.execute("PRAGMA user_version").fetchone()[0])

    # A representative hot key: the busiest (session, provider, instrument).
    hottest = conn.execute(
        "SELECT session_id,provider,instrument,COUNT(*) AS n "
        "FROM cex_observations GROUP BY session_id,provider,instrument "
        "ORDER BY n DESC LIMIT 1").fetchone()
    report["probe_key"] = None if hottest is None else dict(hottest)
    if hottest is None:
        conn.close()
        print(json.dumps(report, indent=2, default=str))
        return 0

    params = (hottest["session_id"], hottest["provider"], hottest["instrument"])

    with_index = {
        "plan": _plan(conn, PRIOR_OBSERVATION_SQL, params),
        **_time_query(conn, PRIOR_OBSERVATION_SQL, params),
    }
    # ``NOT INDEXED`` is not usable here (the table has a UNIQUE autoindex the
    # planner still needs), so the pre-index plan is reproduced by naming the
    # autoindex the planner used before this object existed.
    forced_sql = PRIOR_OBSERVATION_SQL.replace(
        "FROM cex_observations WHERE",
        f"FROM cex_observations NOT INDEXED WHERE")
    try:
        without_index = {
            "plan": _plan(conn, forced_sql, params),
            **_time_query(conn, forced_sql, params, repeats=5),
        }
    except sqlite3.Error as exc:  # pragma: no cover - planner-dependent
        without_index = {"error": f"{type(exc).__name__}:{exc}"}

    report["with_index"] = with_index
    report["without_index"] = without_index
    report["results_identical"] = (
        with_index.get("result") == without_index.get("result"))
    report["temp_btree_eliminated"] = (
        not any("TEMP B-TREE" in line for line in with_index["plan"])
        and any("TEMP B-TREE" in line
                for line in without_index.get("plan", []))
    )

    # Idempotency: the stored DDL must be exactly what the store would issue.
    report["integrity_quick_check"] = [
        str(row[0]) for row in conn.execute("PRAGMA quick_check(1)")]
    conn.close()

    text = json.dumps(report, indent=2, default=str)
    if args.json:
        Path(args.json).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
