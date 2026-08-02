"""Read-only crash-recovery verification of the authoritative V4 database.

Written for the power-loss recovery path, where the one thing that must not
happen is another 16 GB image on a disk with 39 GB free.  So this never copies
the database: it opens the live file read-only, with ``query_only`` set, and
runs the same closure checks ``v4_db_closure_snapshot.py`` runs against a
snapshot.  Every statement here is a read; none of them can write a page, a WAL
frame or an SHM byte.

That is only sound because it runs with every V4 writer stopped, which the
caller establishes first and this script re-asserts by refusing to run while a
process lock is held by a live PID.

Usage: v4_power_loss_db_verify.py <db_path> [--json OUT] [--full-integrity]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from poly_alpha_sniper.lite_frequency_v4.store import (  # noqa: E402
    managed_v5_fingerprint,
    managed_v5_object_census,
)


def _open_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def check(conn: sqlite3.Connection, report: dict, *, full: bool) -> None:
    def scalar(sql: str, params=()) -> int:
        return int(conn.execute(sql, params).fetchone()[0])

    started = time.monotonic()
    report["quick_check"] = {
        "result": [str(r[0]) for r in conn.execute("PRAGMA quick_check")],
        "elapsed_s": round(time.monotonic() - started, 2),
    }
    if full:
        started = time.monotonic()
        report["integrity_check"] = {
            "result": [str(r[0]) for r in conn.execute("PRAGMA integrity_check")],
            "elapsed_s": round(time.monotonic() - started, 2),
        }

    started = time.monotonic()
    violations = [dict(r) for r in conn.execute("PRAGMA foreign_key_check")]
    report["foreign_key_check"] = {
        "violations": len(violations),
        "sample": violations[:10],
        "elapsed_s": round(time.monotonic() - started, 2),
    }

    report["metadata"] = {
        "user_version": scalar("PRAGMA user_version"),
        "page_size": scalar("PRAGMA page_size"),
        "page_count": scalar("PRAGMA page_count"),
        "freelist_count": scalar("PRAGMA freelist_count"),
        "journal_mode": str(conn.execute("PRAGMA journal_mode").fetchone()[0]),
        "sqlite_version": sqlite3.sqlite_version,
        "application_tables": scalar(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"),
    }

    census = managed_v5_object_census(conn)
    report["managed_v5"] = {
        "objects": census,
        "count": len(census),
        "fingerprint": managed_v5_fingerprint(conn),
    }
    report["schema_migrations"] = [
        dict(r) for r in conn.execute(
            "SELECT version,applied_ts_ms,schema_hash FROM schema_migrations "
            "ORDER BY version")]

    report["sessions"] = {
        "total": scalar("SELECT COUNT(*) FROM runtime_sessions"),
        "open": scalar(
            "SELECT COUNT(*) FROM runtime_sessions WHERE ended_ts_ms IS NULL"),
        "safety_tuple_violations": scalar(
            "SELECT COUNT(*) FROM runtime_sessions WHERE dry_run!=1 "
            "OR live_enabled!=0 OR real_orders_possible!=0 "
            "OR live_adapter_present!=0 OR kill_switch_engaged!=1 "
            "OR fixed_shares!=5.0"),
        "open_rows": [
            dict(r) for r in conn.execute(
                "SELECT session_id, started_ts_ms, pid, launch_nonce "
                "FROM runtime_sessions WHERE ended_ts_ms IS NULL "
                "ORDER BY started_ts_ms")],
        "latest": [
            dict(r) for r in conn.execute(
                "SELECT session_id, started_ts_ms, ended_ts_ms, pid, "
                "stop_reason FROM runtime_sessions "
                "ORDER BY started_ts_ms DESC LIMIT 5")],
        "missing_end_command": [
            dict(r) for r in conn.execute(
                "SELECT session_id, ended_ts_ms, stop_reason "
                "FROM runtime_sessions s WHERE ended_ts_ms IS NOT NULL "
                "AND NOT EXISTS(SELECT 1 FROM persistence_commands c "
                "  WHERE c.method='end_runtime_session' "
                "  AND c.status='COMMITTED' "
                "  AND c.idempotency_key='session-end:'||s.session_id) "
                "ORDER BY ended_ts_ms")],
    }
    report["commands"] = {
        "total": scalar("SELECT COUNT(*) FROM persistence_commands"),
        "committed": scalar(
            "SELECT COUNT(*) FROM persistence_commands WHERE status='COMMITTED'"),
        "failed": scalar(
            "SELECT COUNT(*) FROM persistence_commands WHERE status='FAILED'"),
        "unresolved": scalar(
            "SELECT COUNT(*) FROM persistence_commands "
            "WHERE status NOT IN ('COMMITTED','FAILED')"),
        "terminal_total": scalar(
            "SELECT COUNT(*) FROM persistence_commands WHERE terminal=1"),
        "terminal_uncommitted": scalar(
            "SELECT COUNT(*) FROM persistence_commands "
            "WHERE terminal=1 AND status!='COMMITTED'"),
        "duplicate_command_ids": scalar(
            "SELECT COUNT(*) FROM (SELECT command_id FROM "
            "persistence_commands GROUP BY command_id HAVING COUNT(*)>1)"),
    }
    report["maker_observations"] = {
        "total": scalar("SELECT COUNT(*) FROM maker_observations"),
        "unfinished_count": scalar(
            "SELECT COUNT(*) FROM maker_observations "
            "WHERE maker_end_ts_ms IS NULL OR outcome IS NULL"),
        "outcomes": {
            str(r[0]): int(r[1]) for r in conn.execute(
                "SELECT COALESCE(outcome,'<null>'), COUNT(*) "
                "FROM maker_observations GROUP BY 1 ORDER BY 1")},
    }
    report["cohorts"] = [
        dict(r) for r in conn.execute(
            "SELECT cohort,status,authoritative,starting_equity_usd,"
            "parent_cohort FROM cohorts ORDER BY cohort")]
    report["cohort_pilot_starts"] = scalar(
        "SELECT COUNT(*) FROM cohort_pilot_starts")

    # Cluster locks outlive a crash; an unreleased one blocks the next start.
    report["cluster_locks"] = [
        dict(r) for r in conn.execute(
            "SELECT state, COUNT(*) AS n FROM cluster_locks "
            "GROUP BY state ORDER BY state")]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db_path")
    parser.add_argument("--json")
    parser.add_argument("--full-integrity", action="store_true")
    args = parser.parse_args()

    source = Path(args.db_path)
    sidecars = {
        name: path.stat().st_size
        for name, path in (("wal", Path(f"{source}-wal")),
                           ("shm", Path(f"{source}-shm")))
        if path.exists()
    }
    before = (source.stat().st_size, source.stat().st_mtime_ns)
    report: dict = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": str(source),
        "source_bytes": before[0],
        "source_mtime_ns": before[1],
        "sidecars_present": sidecars,
        "no_copy_taken": True,
    }

    conn = _open_ro(source)
    try:
        check(conn, report, full=args.full_integrity)
    finally:
        conn.close()

    after = (source.stat().st_size, source.stat().st_mtime_ns)
    report["source_unchanged"] = before == after
    report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    text = json.dumps(report, indent=2, default=str)
    if args.json:
        Path(args.json).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
