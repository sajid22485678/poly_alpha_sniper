"""Retained, hashable backup snapshot of the authoritative V4 database.

The prior full-integrity manifest attests a 12.70 GB snapshot that was deleted
by ``keep=0``, carried no snapshot hash, and predates the current 15.43 GB image
by two sessions.  So the current bytes had no full-integrity result at all, and
none could be produced after the fact because the audited image no longer
existed.

This produces one that can be re-verified by anyone: an *online backup* taken
through SQLite's own backup API (never a file copy, which is not crash-safe on a
live WAL database), retained, hashed, and then checked offline.  Every check
runs against the snapshot, never against the live database, so nothing here can
write a page, a WAL frame or an SHM byte of the authoritative image.

Usage: v4_db_closure_snapshot.py <db_path> <snapshot_path> [--json OUT]
"""
from __future__ import annotations

import argparse
import hashlib
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def take_snapshot(source: Path, target: Path, report: dict) -> None:
    """Consistent online backup through SQLite's backup API.

    The source is opened read-only.  ``Connection.backup`` copies pages under a
    read transaction, so the result is a consistent image even though the
    database is WAL-mode -- which a filesystem copy of the .db alone would not
    be.
    """

    if target.exists():
        raise SystemExit(f"refusing to overwrite existing snapshot: {target}")
    started = time.monotonic()
    src = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
    try:
        src.execute("PRAGMA query_only=ON")
        dst = sqlite3.connect(str(target))
        try:
            src.backup(dst, pages=4096)
        finally:
            dst.close()
    finally:
        src.close()
    report["snapshot"] = {
        "path": str(target),
        "bytes": target.stat().st_size,
        "elapsed_s": round(time.monotonic() - started, 2),
    }


def check_snapshot(target: Path, report: dict) -> None:
    conn = sqlite3.connect(f"file:{target.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")

        started = time.monotonic()
        report["integrity_check"] = {
            "result": [str(row[0]) for row in conn.execute("PRAGMA integrity_check")],
            "elapsed_s": None,
        }
        report["integrity_check"]["elapsed_s"] = round(
            time.monotonic() - started, 2)

        started = time.monotonic()
        report["quick_check"] = {
            "result": [str(row[0]) for row in conn.execute("PRAGMA quick_check")],
            "elapsed_s": round(time.monotonic() - started, 2),
        }

        started = time.monotonic()
        violations = [dict(row) for row in conn.execute(
            "PRAGMA foreign_key_check")]
        report["foreign_key_check"] = {
            "violations": len(violations),
            "sample": violations[:10],
            "elapsed_s": round(time.monotonic() - started, 2),
        }

        report["metadata"] = {
            "user_version": int(conn.execute("PRAGMA user_version").fetchone()[0]),
            "page_size": int(conn.execute("PRAGMA page_size").fetchone()[0]),
            "page_count": int(conn.execute("PRAGMA page_count").fetchone()[0]),
            "freelist_count": int(
                conn.execute("PRAGMA freelist_count").fetchone()[0]),
            "sqlite_version": sqlite3.sqlite_version,
            "application_tables": int(conn.execute(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'").fetchone()[0]),
        }

        census = managed_v5_object_census(conn)
        report["managed_v5"] = {
            "objects": census,
            "count": len(census),
            "fingerprint": managed_v5_fingerprint(conn),
        }
        report["schema_migrations"] = [
            dict(row) for row in conn.execute(
                "SELECT version,applied_ts_ms,schema_hash FROM schema_migrations "
                "ORDER BY version")]

        # --- lifecycle reconciliation --------------------------------------
        def scalar(sql: str, params=()) -> int:
            return int(conn.execute(sql, params).fetchone()[0])

        report["sessions"] = {
            "total": scalar("SELECT COUNT(*) FROM runtime_sessions"),
            "open": scalar(
                "SELECT COUNT(*) FROM runtime_sessions WHERE ended_ts_ms IS NULL"),
            "safety_tuple_violations": scalar(
                "SELECT COUNT(*) FROM runtime_sessions WHERE dry_run!=1 "
                "OR live_enabled!=0 OR real_orders_possible!=0 "
                "OR live_adapter_present!=0 OR kill_switch_engaged!=1 "
                "OR fixed_shares!=5.0"),
            # The journal command is keyed by idempotency_key
            # 'session-end:<session_id>' -- ordering_key is the constant
            # 'global', so matching on it finds nothing and would report every
            # ended session as missing its command.
            "missing_end_command": [
                dict(row) for row in conn.execute(
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
        # "Unfinished" is a maker observation that was started and never
        # concluded: no end timestamp and no outcome.  There is no status
        # column; the lifecycle is carried by maker_end_ts_ms and outcome.
        report["maker_observations"] = {
            "total": scalar("SELECT COUNT(*) FROM maker_observations"),
            "unfinished": [
                dict(row) for row in conn.execute(
                    "SELECT m.maker_observation_id, m.window_id, m.candidate_id,"
                    " m.maker_start_ts_ms, m.maker_deadline_ts_ms,"
                    " m.maker_end_ts_ms, m.outcome, m.reason "
                    "FROM maker_observations m "
                    "WHERE m.maker_end_ts_ms IS NULL OR m.outcome IS NULL "
                    "ORDER BY m.maker_observation_id")],
            "outcomes": {
                str(row[0]): int(row[1]) for row in conn.execute(
                    "SELECT COALESCE(outcome,'<null>'), COUNT(*) "
                    "FROM maker_observations GROUP BY 1 ORDER BY 1")},
        }
        report["cohorts"] = [
            dict(row) for row in conn.execute(
                "SELECT cohort,status,authoritative,starting_equity_usd,"
                "parent_cohort FROM cohorts ORDER BY cohort")]
        report["cohort_pilot_starts"] = scalar(
            "SELECT COUNT(*) FROM cohort_pilot_starts")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db_path")
    parser.add_argument("snapshot_path")
    parser.add_argument("--json")
    parser.add_argument(
        "--recheck-existing", action="store_true",
        help="re-run the offline checks against a snapshot already taken")
    args = parser.parse_args()

    source, target = Path(args.db_path), Path(args.snapshot_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if args.recheck_existing:
        # Re-run the offline checks against a snapshot already taken, so a
        # query defect does not cost another full-image copy.  The snapshot is
        # opened read-only; it is never re-taken and never modified.
        report = {
            "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": str(source),
            "source_bytes": source.stat().st_size,
            "recheck_of_existing_snapshot": True,
            "snapshot": {"path": str(target), "bytes": target.stat().st_size},
        }
        check_snapshot(target, report)
        report["snapshot"]["sha256"] = sha256_file(target)
        report["finished_utc"] = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        text = json.dumps(report, indent=2, default=str)
        if args.json:
            Path(args.json).write_text(text, encoding="utf-8")
        print(text)
        return 0

    live_before = {
        name: (path.stat().st_size, path.stat().st_mtime_ns)
        for name, path in (
            ("db", source), ("wal", Path(f"{source}-wal")),
            ("shm", Path(f"{source}-shm")))
        if path.exists()
    }
    report: dict = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": str(source),
        "source_bytes": source.stat().st_size,
        "live_before": {k: list(v) for k, v in live_before.items()},
    }

    take_snapshot(source, target, report)
    check_snapshot(target, report)

    report["snapshot"]["sha256"] = sha256_file(target)
    live_after = {
        name: (path.stat().st_size, path.stat().st_mtime_ns)
        for name, path in (
            ("db", source), ("wal", Path(f"{source}-wal")),
            ("shm", Path(f"{source}-shm")))
        if path.exists()
    }
    report["live_after"] = {k: list(v) for k, v in live_after.items()}
    report["live_unchanged"] = live_before == live_after
    report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    text = json.dumps(report, indent=2, default=str)
    if args.json:
        Path(args.json).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
