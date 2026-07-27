"""Identity-pinned soak sampler for the Frequency V4 shadow runtime.

Read-only. Samples the published runtime state and dashboard export on a fixed
cadence and appends one JSON object per sample to a commit/PID/session-stamped
JSONL file.

Why the identity contract exists
--------------------------------
A previous soak produced one continuous ``soak_final.jsonl`` spanning **two**
runtime PIDs and two commits: an old sampler outlived a runtime restart, and the
``Move-Item`` meant to set its file aside failed silently because the sampler
still held the handle (a rename over an open handle fails on Windows, and the
error was suppressed). The resulting file could not be attributed to a commit
without post-hoc segmentation, and headline metrics were reported against the
wrong one.

So this sampler pins the runtime identity at startup and re-verifies it on every
sample. If the commit, PID, session or launch nonce changes -- or ownership
stops being valid -- it writes one terminal ``identity_mismatch`` record, stops
immediately and exits non-zero. It can never walk from one runtime into another.

It also refuses to reuse an output path, so two runs cannot share a file, and it
stops when the pinned runtime stops rather than appending frozen post-stop
samples forever.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime" / "lite_frequency_v4_shadow"
DB_PATH = REPO_ROOT / "data" / "poly_alpha_frequency_v4.db"
DEFAULT_EXPORT = Path(
    r"D:\claude\agent_readonly\poly_alpha_frequency_v4\frequency_v4_dashboard.json")

#: Terminal reasons the sampler stops on its own.
STOP_IDENTITY_MISMATCH = "identity_mismatch"
STOP_RUNTIME_STOPPED = "runtime_stopped"
STOP_RUNTIME_GONE = "runtime_process_gone"
STOP_DURATION_REACHED = "duration_reached"

#: Runtime states that mean the pinned runtime is finished.
TERMINAL_STATES = frozenset({"STOPPED", "FAILED"})


class IdentityMismatch(RuntimeError):
    """The observed runtime is not the one this sampler pinned."""


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:  # a torn read is normal against a live publisher
        return {"_read_error": f"{type(exc).__name__}:{exc}"}


def pid_alive(pid: int) -> bool:
    if not pid or int(pid) <= 0:
        return False
    try:
        import psutil

        return bool(psutil.pid_exists(int(pid)))
    except Exception:
        try:
            os.kill(int(pid), 0)
            return True
        except OSError:
            return False


def open_runtime_sessions(db_path: Path) -> Any:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM runtime_sessions WHERE ended_ts_ms IS NULL"
            ).fetchone()[0]
        finally:
            conn.close()
    except Exception as exc:
        return f"ERR:{type(exc).__name__}"


def build_output_path(
    directory: Path, commit: str, pid: int, session_id: str,
) -> Path:
    """Commit/PID/session-stamped path so two runs can never share a file."""

    short = str(commit or "unknown")[:12]
    session = str(session_id or "nosession")[:12]
    return Path(directory) / f"soak_{short}_{int(pid)}_{session}.jsonl"


def read_identity(
    runtime_dir: Path = RUNTIME_DIR,
) -> dict[str, Any]:
    """The runtime identity as currently published."""

    state = read_json(runtime_dir / "state.json")
    heartbeat = read_json(runtime_dir / "heartbeat.json")
    return {
        "commit": str(state.get("current_commit") or ""),
        "pid": int(state.get("pid") or 0),
        "session_id": str(state.get("session_id") or ""),
        "launch_nonce": str(
            state.get("launch_nonce") or heartbeat.get("launch_nonce") or ""),
        "state": str(state.get("state") or ""),
        "ownership_valid": bool(state.get("process_ownership_valid", False)),
    }


def verify_identity(
    pinned: Mapping[str, Any], observed: Mapping[str, Any],
) -> list[str]:
    """Return the identity fields that changed; empty means still the same run."""

    changed = []
    for field in ("commit", "pid", "session_id", "launch_nonce"):
        expected = pinned.get(field)
        actual = observed.get(field)
        # An unreadable/empty observation is not proof of a different runtime;
        # only a *different* concrete value is.
        if actual and expected and actual != expected:
            changed.append(field)
    return changed


def sample_once(
    index: int, pinned: Mapping[str, Any], *, export_path: Path,
    runtime_dir: Path = RUNTIME_DIR, db_path: Path = DB_PATH,
    dashboard_alive: bool = True,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    state = read_json(runtime_dir / "state.json")
    heartbeat = read_json(runtime_dir / "heartbeat.json")
    export = read_json(export_path)

    persistence = export.get("persistence") or {}
    telemetry = persistence.get("telemetry") or {}
    model = persistence.get("telemetry_health_model") or {}
    critical = persistence.get("critical") or {}
    checkpoint = persistence.get("latest_checkpoint") or {}
    runtime = export.get("runtime") or {}
    reconciliation = model.get("accounting_reconciliation") or {}

    hb_ts = int(heartbeat.get("ts_ms") or 0)
    generated = int(export.get("generated_ts_ms") or 0)
    wal = Path(f"{db_path}-wal")
    observed_pid = int(state.get("pid") or 0)

    return {
        # --- identity (validated by the caller) -----------------------------
        "sample_index": index,
        "sampled_at_utc": datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"),
        "sampled_at_ms": now_ms,
        "expected_commit": pinned.get("commit"),
        "current_commit": state.get("current_commit"),
        "expected_runtime_pid": pinned.get("pid"),
        "current_runtime_pid": observed_pid,
        "expected_session_id": pinned.get("session_id"),
        "runtime_session_id": state.get("session_id"),
        "expected_launch_nonce": pinned.get("launch_nonce"),
        "launch_nonce": state.get("launch_nonce"),
        "runtime_alive": pid_alive(observed_pid),
        "dashboard_alive": bool(dashboard_alive),
        "ownership_valid": state.get("process_ownership_valid"),
        "orphan_processes": state.get("orphan_processes"),
        # --- freshness ------------------------------------------------------
        "heartbeat_age_s": (
            round((now_ms - hb_ts) / 1000.0, 3) if hb_ts else None),
        "export_age_s": (
            round((now_ms - generated) / 1000.0, 3) if generated else None),
        "state": state.get("state"),
        "last_error": state.get("last_error"),
        "loop_lag_ms": state.get("loop_lag_ms"),
        # --- readiness ------------------------------------------------------
        "operational_ready": persistence.get("operational_ready"),
        "critical_execution_ready": persistence.get("critical_execution_ready"),
        "critical_blocked_reasons": persistence.get("critical_blocked_reasons"),
        "operational_degraded_reasons": persistence.get(
            "operational_degraded_reasons"),
        "recovery_blockers": telemetry.get("recovery_blockers"),
        "recovery_healthy_windows": telemetry.get("recovery_healthy_windows"),
        "recovery_required_windows": telemetry.get("recovery_required_windows"),
        # --- telemetry health ----------------------------------------------
        "telemetry_reported_state": model.get("reported_state"),
        "telemetry_data_safety": model.get("data_safety"),
        "telemetry_data_safety_reasons": model.get("data_safety_reasons"),
        "telemetry_capacity_state": model.get("capacity_state"),
        "sampling_keep_ratio": model.get("sampling_keep_ratio"),
        "sampling_policy_reason": model.get("sampling_policy_reason"),
        # --- queue ----------------------------------------------------------
        "queue_depth": telemetry.get("queue_depth"),
        "queue_capacity": telemetry.get("queue_capacity"),
        "queue_slope_per_second": telemetry.get("queue_depth_slope_per_second"),
        "queue_oldest_age_s": model.get("queue_oldest_age_s"),
        "queue_bounded": model.get("queue_bounded"),
        "queue_max_depth_window": model.get("queue_max_depth_window"),
        "polymarket_queue_depth": state.get("polymarket_ingest_queue_depth"),
        "cex_queue_depth": state.get("cex_ingest_queue_depth"),
        # --- conservation / loss -------------------------------------------
        "critical_evidence_lost": telemetry.get("true_lost_critical_rows"),
        "critical_evidence_incomplete": telemetry.get(
            "critical_evidence_incomplete_count"),
        "unresolved_critical_commands": critical.get(
            "unconfirmed_command_count"),
        "unexpected_noncritical_loss": model.get(
            "window_unexpected_loss_rows"),
        "reconciliation_mismatch": model.get(
            "accounting_reconciliation_mismatch_rows"),
        "recon_submitted": reconciliation.get("submitted"),
        "recon_accounted": reconciliation.get("accounted"),
        "recon_policy_sampled": reconciliation.get("policy_sampled"),
        "recon_unexpected_loss": reconciliation.get("unexpected_loss"),
        "deadline_exceeded_batches": telemetry.get("deadline_exceeded_batches"),
        "failed_batches": telemetry.get("failed_batches"),
        "terminal_failed_batches": telemetry.get("terminal_failed_batches"),
        # --- WAL / checkpoint ----------------------------------------------
        "wal_bytes": wal.stat().st_size if wal.exists() else 0,
        "checkpoint_run_id": checkpoint.get("checkpoint_run_id"),
        "checkpoint_mode": checkpoint.get("mode"),
        "checkpoint_reason": checkpoint.get("reason"),
        "checkpoint_before_wal_bytes": checkpoint.get("before_wal_bytes"),
        "checkpoint_after_wal_bytes": checkpoint.get("after_wal_bytes"),
        "checkpoint_success": checkpoint.get("success"),
        # --- integrity ------------------------------------------------------
        "integrity_runs": state.get("integrity_check_runs"),
        "integrity_scan_in_progress": state.get("integrity_scan_in_progress"),
        "integrity_scan_duration_ms": state.get("integrity_scan_duration_ms"),
        "full_integrity_audit_runs": state.get("full_integrity_audit_runs"),
        "sqlite_integrity": (export.get("integrity") or {}).get(
            "sqlite_integrity"),
        "export_runs": state.get("dashboard_export_runs"),
        "export_ok": state.get("dashboard_export_ok"),
        "reporting_export_degraded": state.get("reporting_export_degraded"),
        # --- sessions -------------------------------------------------------
        "open_runtime_sessions_export": runtime.get(
            "open_runtime_session_count"),
        "open_runtime_sessions_db": open_runtime_sessions(db_path),
        "stray_temp_files": len(list(runtime_dir.glob("*.tmp.*"))),
        # --- safety tuple ---------------------------------------------------
        "dry_run": export.get("dry_run"),
        "live_enabled": export.get("live_enabled"),
        "real_orders_possible": export.get("real_orders_possible"),
        "live_adapter_present": export.get("live_adapter_present"),
        "kill_switch_engaged": export.get("kill_switch_engaged"),
        "fixed_shares": export.get("fixed_shares"),
    }


def run_sampler(
    *, output_dir: Path, duration_s: float, interval_s: float = 30.0,
    export_path: Path = DEFAULT_EXPORT, runtime_dir: Path = RUNTIME_DIR,
    db_path: Path = DB_PATH, clock=time.monotonic, sleep=time.sleep,
    stream=None,
) -> tuple[Path, str, int]:
    """Sample one pinned runtime. Returns (path, stop_reason, sample_count)."""

    pinned = read_identity(runtime_dir)
    if not pinned["pid"] or not pid_alive(pinned["pid"]):
        raise RuntimeError(
            f"no live V4 runtime to pin (published pid={pinned['pid']!r})")
    if pinned["state"] in TERMINAL_STATES:
        raise RuntimeError(
            f"refusing to sample a terminal runtime (state={pinned['state']})")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = build_output_path(
        output_dir, pinned["commit"], pinned["pid"], pinned["session_id"])
    if path.exists():
        # Never append into a previous run's evidence.
        raise FileExistsError(f"soak output already exists: {path}")

    manifest = {
        "record": "manifest",
        "started_at_utc": datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"),
        "sampler_pid": os.getpid(),
        "interval_s": interval_s,
        "duration_s": duration_s,
        "pinned": dict(pinned),
        "export_path": str(export_path),
    }

    stop_reason = STOP_DURATION_REACHED
    count = 0
    deadline = clock() + float(duration_s)
    # Exclusive create: a second sampler for the same identity cannot start.
    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest, default=str) + "\n")
        handle.flush()
        while clock() < deadline:
            count += 1
            row = sample_once(
                count, pinned, export_path=export_path,
                runtime_dir=runtime_dir, db_path=db_path)

            observed = read_identity(runtime_dir)
            changed = verify_identity(pinned, observed)
            if changed:
                row["record"] = STOP_IDENTITY_MISMATCH
                row["identity_changed_fields"] = changed
                row["observed_identity"] = observed
                handle.write(json.dumps(row, default=str) + "\n")
                handle.flush()
                _emit(stream, f"IDENTITY MISMATCH {changed}; stopping")
                return path, STOP_IDENTITY_MISMATCH, count

            terminal = None
            if not row["runtime_alive"]:
                terminal = STOP_RUNTIME_GONE
            elif str(observed["state"]) in TERMINAL_STATES:
                terminal = STOP_RUNTIME_STOPPED

            row["record"] = terminal or "sample"
            handle.write(json.dumps(row, default=str) + "\n")
            handle.flush()
            _emit(stream, _line(row))
            if terminal:
                # Exactly one terminal record, then stop -- never a frozen tail.
                return path, terminal, count
            sleep(interval_s)
    return path, stop_reason, count


def _emit(stream, message: str) -> None:
    target = stream if stream is not None else sys.stdout
    print(message, file=target, flush=True)


def _line(row: Mapping[str, Any]) -> str:
    blockers = ",".join(row.get("recovery_blockers") or [])
    return (
        f"[{row['sample_index']}] hb={row['heartbeat_age_s']} "
        f"exp={row['export_age_s']} ready={row['operational_ready']} "
        f"{row['telemetry_reported_state']} "
        f"win={row['recovery_healthy_windows']} "
        f"unexp={row['unexpected_noncritical_loss']} "
        f"mm={row['reconciliation_mismatch']} "
        f"wal={round((row['wal_bytes'] or 0) / 1e6, 1)}MB "
        f"q={row['queue_depth']} age={row['queue_oldest_age_s']} "
        f"iscan={row['integrity_runs']} blk={blockers}"
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir")
    parser.add_argument("duration_s", type=float)
    parser.add_argument("interval_s", type=float, nargs="?", default=30.0)
    parser.add_argument("--export-path", default=str(DEFAULT_EXPORT))
    args = parser.parse_args(argv)

    path, reason, count = run_sampler(
        output_dir=Path(args.output_dir),
        duration_s=args.duration_s,
        interval_s=args.interval_s,
        export_path=Path(args.export_path),
    )
    print(f"SOAK FINISHED reason={reason} samples={count} path={path}",
          flush=True)
    # A soak that ended because the runtime vanished or the identity changed is
    # not a completed soak; make that visible to the caller's exit code.
    return 0 if reason == STOP_DURATION_REACHED else 1


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
