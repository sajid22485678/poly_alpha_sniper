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
import socket
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime" / "lite_frequency_v4_shadow"
# Taken from configuration, not rebuilt from the repository root: the database
# lives on the SSD now, and a sampler that guessed the old repository-relative
# path would silently report on a stale copy the runtime no longer writes.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from lite_frequency_v4.config import V4_DB_PATH  # noqa: E402

DB_PATH = Path(V4_DB_PATH)
DEFAULT_EXPORT = Path(
    r"D:\claude\agent_readonly\poly_alpha_frequency_v4\frequency_v4_dashboard.json")

#: Where the V4 dashboard listens.  Probed for real rather than assumed: an
#: exported JSON file proves the exporter ran, not that the dashboard is serving.
DASHBOARD_URL = "http://127.0.0.1:8504/"
DASHBOARD_PORT = 8504
DASHBOARD_TIMEOUT_S = 5.0

#: Terminal reasons the sampler stops on its own.
STOP_IDENTITY_MISMATCH = "identity_mismatch"
STOP_RUNTIME_STOPPED = "runtime_stopped"
STOP_RUNTIME_GONE = "runtime_process_gone"
STOP_DURATION_REACHED = "duration_reached"
#: The tail ran out before the runtime stopped.  Distinct from a clean terminal
#: observation, because a run that was never seen to stop was never evaluated
#: through its shutdown.
STOP_TAIL_TIMEOUT = "tail_timeout"

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


def probe_dashboard(url: str = DASHBOARD_URL, *,
                    port: int = DASHBOARD_PORT,
                    timeout_s: float = DASHBOARD_TIMEOUT_S) -> dict[str, Any]:
    """Actually ask the dashboard whether it is serving.

    ``dashboard_alive`` used to be a parameter defaulting to ``True`` that no
    caller ever supplied, so every sample asserted the dashboard was up without
    anyone looking.  A fresh export file proves the *exporter* ran inside the
    bot; it says nothing about the separate Next.js process that serves the
    page, and the two stop independently.

    Both a listening socket and an HTTP status are recorded, because they fail
    apart: a wedged server still holds the port, and a listener check alone
    would call that healthy.
    """

    result: dict[str, Any] = {
        "url": url, "listening": False, "http_status": None,
        "latency_ms": None, "error": None,
    }
    started = time.perf_counter()
    try:
        with socket.create_connection(("127.0.0.1", int(port)),
                                      timeout=timeout_s):
            result["listening"] = True
    except OSError as exc:
        result["error"] = f"{type(exc).__name__}:{exc}"[:200]
        return result
    try:
        request = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            response.read(2048)
            result["http_status"] = int(response.status)
    except urllib.error.HTTPError as exc:
        # A 4xx/5xx is still the server answering; record it rather than
        # collapsing it into "down".
        result["http_status"] = int(exc.code)
        result["error"] = f"HTTPError:{exc.code}"
    except Exception as exc:  # noqa: BLE001 - a probe must never stop a sample
        result["error"] = f"{type(exc).__name__}:{exc}"[:200]
    result["latency_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
    return result


def process_memory(pid: int) -> dict[str, Any]:
    """Resident and virtual memory for the pinned runtime, in bytes.

    Late-run memory growth is a mandatory certification criterion and was never
    sampled, so no run had any evidence for or against it.
    """

    result: dict[str, Any] = {"rss_bytes": None, "vms_bytes": None,
                             "num_handles": None, "num_threads": None,
                             "error": None}
    if not pid:
        result["error"] = "no_pid"
        return result
    try:
        import psutil  # noqa: PLC0415 - optional at import time, required here

        process = psutil.Process(int(pid))
        info = process.memory_info()
        result["rss_bytes"] = int(info.rss)
        result["vms_bytes"] = int(getattr(info, "vms", 0)) or None
        result["num_threads"] = int(process.num_threads())
        handles = getattr(process, "num_handles", None)
        if callable(handles):
            result["num_handles"] = int(handles())
    except Exception as exc:  # noqa: BLE001 - observation is never fatal
        result["error"] = f"{type(exc).__name__}:{exc}"[:200]
    return result


def sample_once(
    index: int, pinned: Mapping[str, Any], *, export_path: Path,
    runtime_dir: Path = RUNTIME_DIR, db_path: Path = DB_PATH,
    dashboard: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    # Measured, not assumed.  ``None`` means "probe it now"; a caller may pass a
    # reading it already took, but there is no way to assert one without having
    # looked.
    dashboard_probe = dict(
        probe_dashboard() if dashboard is None else dashboard)
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
    # Reader-lifetime diagnostics come from the ~2s state publish, which is
    # fresher than the 5s export; the in-memory latest checkpoint carries the
    # per-checkpoint reader correlation that never enters the database row.
    state_persistence = state.get("persistence") or {}
    readers = state_persistence.get("sqlite_readers") or {}
    state_checkpoint = state_persistence.get("latest_checkpoint") or {}
    ckpt_readers = state_checkpoint.get("readers_at_start") or {}
    profile = state_persistence.get("export_profile") or {}
    profile_stages = profile.get("stages") or {}
    build_total = profile_stages.get("build.total") or {}
    export_freshness = export.get("export_freshness") or {}

    hb_ts = int(heartbeat.get("ts_ms") or 0)
    generated = int(export.get("generated_ts_ms") or 0)
    wal = Path(f"{db_path}-wal")
    observed_pid = int(state.get("pid") or 0)
    memory = process_memory(observed_pid)

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
        # A real listener plus a real HTTP status.  Healthy means both.
        "dashboard_alive": bool(
            dashboard_probe.get("listening")
            and dashboard_probe.get("http_status") is not None
            and 200 <= int(dashboard_probe["http_status"]) < 400),
        "dashboard_listening": bool(dashboard_probe.get("listening")),
        "dashboard_http_status": dashboard_probe.get("http_status"),
        "dashboard_probe_ms": dashboard_probe.get("latency_ms"),
        "dashboard_probe_error": dashboard_probe.get("error"),
        # Memory of the pinned runtime, so late-run growth is measurable.
        "runtime_rss_bytes": memory.get("rss_bytes"),
        "runtime_vms_bytes": memory.get("vms_bytes"),
        "runtime_threads": memory.get("num_threads"),
        "runtime_handles": memory.get("num_handles"),
        "runtime_memory_error": memory.get("error"),
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
        "checkpoint_status": state_checkpoint.get("status"),
        "checkpoint_frames_total": state_checkpoint.get("frames_total"),
        "checkpoint_frames_checkpointed": state_checkpoint.get(
            "frames_checkpointed"),
        "checkpoint_busy_result": state_checkpoint.get("busy_result"),
        "checkpoint_duration_ms": state_checkpoint.get("duration_ms"),
        "checkpoint_started_ts_ms": state_checkpoint.get("started_ts_ms"),
        # --- SQLite reader lifetimes ---------------------------------------
        "active_reader_count": readers.get("active_reader_count"),
        "active_job_count": readers.get("active_job_count"),
        "oldest_reader_age_ms": readers.get("oldest_reader_age_ms"),
        "oldest_reader_name": readers.get("oldest_reader_name"),
        "oldest_reader_worker": readers.get("oldest_reader_worker"),
        "oldest_job_age_ms": readers.get("oldest_job_age_ms"),
        "oldest_job_name": readers.get("oldest_job_name"),
        "readers_over_threshold": readers.get("readers_over_threshold"),
        "last_reader_release_ms": readers.get("last_reader_release_ms"),
        "max_statement_ms": readers.get("max_statement_ms"),
        "max_job_ms": readers.get("max_job_ms"),
        "reader_counters": readers.get("counters"),
        "reader_ops": readers.get("ops"),
        "checkpoint_readers_at_start": {
            "active_reader_count": ckpt_readers.get("active_reader_count"),
            "active_job_count": ckpt_readers.get("active_job_count"),
            "oldest_reader_age_ms": ckpt_readers.get("oldest_reader_age_ms"),
            "oldest_reader_name": ckpt_readers.get("oldest_reader_name"),
            "oldest_reader_worker": ckpt_readers.get("oldest_reader_worker"),
            "oldest_job_age_ms": ckpt_readers.get("oldest_job_age_ms"),
            "oldest_job_name": ckpt_readers.get("oldest_job_name"),
            "wal_bytes": ckpt_readers.get("wal_bytes"),
        } if ckpt_readers else None,
        # --- integrity ------------------------------------------------------
        "integrity_runs": state.get("integrity_check_runs"),
        "integrity_scan_in_progress": state.get("integrity_scan_in_progress"),
        "integrity_scan_duration_ms": state.get("integrity_scan_duration_ms"),
        "integrity_scan_chunked": state.get("integrity_scan_chunked"),
        "integrity_scan_chunks": state.get("integrity_scan_chunks"),
        "integrity_scan_max_chunk_ms": state.get("integrity_scan_max_chunk_ms"),
        "full_integrity_audit_runs": state.get("full_integrity_audit_runs"),
        "sqlite_integrity": (export.get("integrity") or {}).get(
            "sqlite_integrity"),
        # --- bounded live integrity health check ----------------------------
        # Reported separately from the full audit below: the live check proves
        # every page it read decodes and every declared foreign key resolves,
        # and publishes how much of a cycle that covers.  It is never a full
        # audit, so the two must never be read from one field.
        "live_integrity_health": state.get("live_integrity_health"),
        "live_integrity_scope": state.get("live_integrity_scope"),
        "live_integrity_bounded": state.get("live_integrity_bounded"),
        "live_integrity_max_chunk_ms": state.get("live_integrity_max_chunk_ms"),
        "live_integrity_chunk_bound_ms": state.get(
            "live_integrity_chunk_bound_ms"),
        "live_integrity_chunks_over_bound": state.get(
            "live_integrity_chunks_over_bound"),
        "live_integrity_rows_per_chunk": state.get(
            "live_integrity_rows_per_chunk"),
        "live_integrity_progress": state.get("live_integrity_progress"),
        "live_integrity_cycles_completed": state.get(
            "live_integrity_cycles_completed"),
        "live_integrity_last_completed_ms": state.get(
            "live_integrity_last_completed_ms"),
        "live_integrity_last_completed_ok": state.get(
            "live_integrity_last_completed_ok"),
        # --- full audit, on a completed snapshot ----------------------------
        "full_audit_status": state.get("full_audit_status"),
        "full_audit_ok": state.get("full_audit_ok"),
        "full_audit_scope": state.get("full_audit_scope"),
        "full_audit_runs_on_live_database": state.get(
            "full_audit_runs_on_live_database"),
        "full_audit_source_as_of_ms": state.get("full_audit_source_as_of_ms"),
        "full_audit_completed_ms": state.get("full_audit_completed_ms"),
        "full_audit_age_ms": state.get("full_audit_age_ms"),
        "full_audit_failure_reason": state.get("full_audit_failure_reason"),
        "full_audit_snapshot_status": state.get("full_audit_snapshot_status"),
        "full_audit_snapshot_progress_pct": state.get(
            "full_audit_snapshot_progress_pct"),
        "full_audit_snapshot_progress_pages": state.get(
            "full_audit_snapshot_progress_pages"),
        "full_audit_snapshot_total_pages": state.get(
            "full_audit_snapshot_total_pages"),
        "full_audit_snapshot_steps": state.get("full_audit_snapshot_steps"),
        "full_audit_snapshot_restarts": state.get(
            "full_audit_snapshot_restarts"),
        "full_audit_snapshot_max_step_ms": state.get(
            "full_audit_snapshot_max_step_ms"),
        "full_audit_snapshot_duration_ms": state.get(
            "full_audit_snapshot_duration_ms"),
        "full_audit_integrity_check_ms": state.get(
            "full_audit_integrity_check_ms"),
        "export_runs": state.get("dashboard_export_runs"),
        "export_ok": state.get("dashboard_export_ok"),
        "export_duration_ms": state.get("dashboard_export_ms"),
        # --- export cost profile -------------------------------------------
        "export_build_p50_ms": build_total.get("p50_ms"),
        "export_build_p90_ms": build_total.get("p90_ms"),
        "export_build_p99_ms": build_total.get("p99_ms"),
        "export_build_max_ms": build_total.get("max_ms"),
        "export_build_last_ms": build_total.get("last_ms"),
        "export_builds": profile.get("builds"),
        "export_payload_bytes": profile.get("payload_bytes_last"),
        "export_last_build": profile.get("last_build"),
        "export_stages": profile_stages,
        "export_statements": profile.get("statements"),
        # --- export freshness contract -------------------------------------
        "export_complete": export_freshness.get("complete"),
        "export_degraded_sections": export_freshness.get("degraded_sections"),
        "export_stale_sections": export_freshness.get("stale_sections"),
        "export_section_cache_hits": export_freshness.get("cache_hits"),
        "export_section_max_age_ms": export_freshness.get("max_section_age_ms"),
        "maintenance_duration_ms": state.get("maintenance_ms"),
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
    stream=None, tail_timeout_s: float = 0.0,
) -> tuple[Path, str, int]:
    """Sample one pinned runtime. Returns (path, stop_reason, sample_count).

    ``tail_timeout_s`` keeps sampling after the requested duration until the
    runtime actually stops, so the stream ends on a real terminal observation
    rather than wherever the clock happened to run out.

    That matters because a window is not a run.  The controlled gate at 4d8655c
    sampled a clean 57.1 minutes and then kept running for another 6m40s, during
    which four ticks raised ``queue_accumulating`` and ``uncontrolled_overload``
    with a readiness reset -- entirely outside the evidence, and the evaluator
    filtered the dense trace to the sampled window, so nothing ever saw it.  A
    run is certifiable through its shutdown or it is not certifiable.
    """

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

    manifest["tail_timeout_s"] = float(tail_timeout_s)
    count = 0
    started = clock()
    deadline = started + float(duration_s)
    tail_deadline = deadline + max(0.0, float(tail_timeout_s))
    # Exclusive create: a second sampler for the same identity cannot start.
    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest, default=str) + "\n")
        handle.flush()
        while True:
            now = clock()
            in_tail = now >= deadline
            if in_tail and tail_timeout_s <= 0.0:
                # No tail requested: the duration is the run, exactly as before.
                return path, STOP_DURATION_REACHED, count
            if in_tail and now >= tail_deadline:
                # Requested duration served and the tail budget spent without
                # ever observing the runtime stop.  Say so explicitly instead of
                # ending on an ordinary sample that reads as a clean finish.
                _emit(stream, "TAIL TIMEOUT: runtime never reached a terminal "
                              "state within the tail budget")
                handle.write(json.dumps({
                    "record": STOP_TAIL_TIMEOUT,
                    "sample_index": count,
                    "sampled_at_utc": datetime.now(timezone.utc).isoformat(
                        timespec="milliseconds"),
                    "sampled_at_ms": int(time.time() * 1000),
                    "observed_state": read_identity(runtime_dir).get("state"),
                    "elapsed_s": round(now - started, 3),
                }, default=str) + "\n")
                handle.flush()
                return path, STOP_TAIL_TIMEOUT, count

            count += 1
            row = sample_once(
                count, pinned, export_path=export_path,
                runtime_dir=runtime_dir, db_path=db_path)
            row["phase"] = "tail" if in_tail else "window"
            row["elapsed_s"] = round(clock() - started, 3)

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
        f"rdrs={row.get('active_reader_count')}"
        f"/{row.get('oldest_reader_age_ms')} "
        f"iscan={row['integrity_runs']} "
        f"live={row.get('live_integrity_max_chunk_ms')}ms"
        f"/{(row.get('live_integrity_progress') or {}).get('progress_pct')}% "
        f"audit={row.get('full_audit_status')}"
        f"/{row.get('full_audit_snapshot_progress_pct')}% "
        f"dash={row.get('dashboard_http_status')} "
        f"rss={round((row.get('runtime_rss_bytes') or 0) / 1e6, 1)}MB "
        f"blk={blockers}"
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir")
    parser.add_argument("duration_s", type=float)
    parser.add_argument("interval_s", type=float, nargs="?", default=30.0)
    parser.add_argument("--export-path", default=str(DEFAULT_EXPORT))
    parser.add_argument(
        "--tail-timeout-s", type=float, default=0.0,
        help=("keep sampling past the duration until the runtime reaches a "
              "terminal state, so the stream ends on a real terminal record"))
    args = parser.parse_args(argv)

    path, reason, count = run_sampler(
        output_dir=Path(args.output_dir),
        duration_s=args.duration_s,
        interval_s=args.interval_s,
        tail_timeout_s=args.tail_timeout_s,
        export_path=Path(args.export_path),
    )
    print(f"SOAK FINISHED reason={reason} samples={count} path={path}",
          flush=True)
    # A soak that ended because the runtime vanished or the identity changed is
    # not a completed soak; make that visible to the caller's exit code.
    return 0 if reason == STOP_DURATION_REACHED else 1


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
