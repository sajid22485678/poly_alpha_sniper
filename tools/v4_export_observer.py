"""Dense read-only observer for the Frequency V4 export cost profile.

The identity-pinned soak sampler runs at a 30 s cadence, which is the right
resolution for a soak verdict but too coarse to catch an individual export
build that exceeds its freshness limit.  This observer samples the published
runtime state every ~2 s and records the named-stage export profile alongside
the correlates that could explain a slow build: WAL size, event-loop lag,
reader ages, telemetry queue state and payload size.

Read-only.  It opens no database connection and writes only its own JSONL.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime" / "lite_frequency_v4_shadow"
DB_PATH = REPO_ROOT / "data" / "poly_alpha_frequency_v4.db"


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:  # a torn read against a live publisher is normal
        return {"_read_error": f"{type(exc).__name__}:{exc}"}


def observe(index: int, *, runtime_dir: Path, db_path: Path) -> dict[str, Any]:
    state = read_json(runtime_dir / "state.json")
    persistence = state.get("persistence") or {}
    profile = persistence.get("export_profile") or {}
    stages = profile.get("stages") or {}
    readers = persistence.get("sqlite_readers") or {}
    checkpoint = persistence.get("latest_checkpoint") or {}
    telemetry = persistence.get("telemetry") or {}
    wal = Path(f"{db_path}-wal")
    return {
        "i": index,
        "ts_ms": int(time.time() * 1000),
        "utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "commit": state.get("current_commit"),
        "pid": state.get("pid"),
        "session_id": state.get("session_id"),
        "state": state.get("state"),
        "loop_lag_ms": state.get("loop_lag_ms"),
        "loop_lag_max_ms": state.get("loop_lag_max_ms"),
        "export_runs": state.get("dashboard_export_runs"),
        "export_ms": state.get("dashboard_export_ms"),
        "export_ok": state.get("dashboard_export_ok"),
        "maintenance_ms": state.get("maintenance_ms"),
        "integrity_in_progress": state.get("integrity_scan_in_progress"),
        "wal_bytes": wal.stat().st_size if wal.exists() else 0,
        "builds": profile.get("builds"),
        "payload_bytes": profile.get("payload_bytes_last"),
        "last_build": profile.get("last_build"),
        "stages": stages,
        "statements": profile.get("statements"),
        "active_reader_count": readers.get("active_reader_count"),
        "oldest_reader_age_ms": readers.get("oldest_reader_age_ms"),
        "oldest_reader_name": readers.get("oldest_reader_name"),
        "oldest_job_age_ms": readers.get("oldest_job_age_ms"),
        "max_statement_ms": readers.get("max_statement_ms"),
        "max_job_ms": readers.get("max_job_ms"),
        "checkpoint_mode": checkpoint.get("mode"),
        "checkpoint_status": checkpoint.get("status"),
        "checkpoint_duration_ms": checkpoint.get("duration_ms"),
        "queue_depth": telemetry.get("queue_depth"),
        "queue_oldest_age_s": telemetry.get("queue_oldest_age_s"),
        "capacity_state": telemetry.get("telemetry_capacity_state"),
        "unexpected_loss": telemetry.get("window_unexpected_loss_rows"),
        "recon_mismatch": telemetry.get(
            "accounting_reconciliation_mismatch_rows"),
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("duration_s", type=float)
    parser.add_argument("interval_s", type=float, nargs="?", default=2.0)
    args = parser.parse_args(argv)

    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + float(args.duration_s)
    index = 0
    with path.open("x", encoding="utf-8") as handle:
        while time.monotonic() < deadline:
            index += 1
            handle.write(json.dumps(
                observe(index, runtime_dir=RUNTIME_DIR, db_path=DB_PATH),
                default=str) + "\n")
            handle.flush()
            time.sleep(float(args.interval_s))
    print(f"OBSERVER FINISHED samples={index} path={path}", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
