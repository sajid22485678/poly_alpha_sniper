"""Read-only process/DB-file diagnostics for the dashboard's Runtime health
panel. Uses psutil ONLY for introspection (pid_exists, cmdline) -- nothing
here can start, stop, kill, or signal any process. No trading calls, no
order placement/cancellation of any kind.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional


def check_single_instance(mode_substring: str = "shadow_live",
                          expect_per_instance: int = 2) -> dict:
    """Scans running processes (read-only) for anything that looks like a
    poly_alpha_sniper main.py instance in the given mode. A single running
    bot normally shows up as 2 OS processes (the venv launcher + the actual
    interpreter it re-execs into) -- more than that suggests a duplicate
    instance, which the live-readiness checklist explicitly calls out as a
    reason to investigate before any live promotion.

    Returns {"available": bool, "matches": [{"pid": int, "cmdline": str}],
    "count": int, "likely_duplicate": bool}. available=False (with an
    explanatory reason) if psutil isn't installed -- never fabricates a
    process count it couldn't actually observe."""
    try:
        import psutil
    except ImportError:
        return {"available": False, "reason": "psutil not installed", "matches": [],
               "count": 0, "likely_duplicate": False}

    matches = []
    try:
        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                cmdline = " ".join(proc.info.get("cmdline") or [])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            if "main.py" in cmdline and "--mode" in cmdline and mode_substring in cmdline:
                matches.append({"pid": proc.info["pid"], "cmdline": cmdline})
    except Exception as exc:  # noqa: BLE001 -- diagnostics must never crash the dashboard
        return {"available": False, "reason": f"scan failed: {exc}", "matches": [],
               "count": 0, "likely_duplicate": False}

    return {"available": True, "matches": matches, "count": len(matches),
           "likely_duplicate": len(matches) > expect_per_instance}


def wal_status(db_path: str) -> dict:
    """Whether SQLite's WAL sidecar file is present and its size -- purely
    informational, this project already runs PRAGMA journal_mode=WAL by
    design (storage/sqlite_store.py)."""
    wal = Path(str(db_path) + "-wal")
    shm = Path(str(db_path) + "-shm")
    return {
        "wal_present": wal.exists(),
        "wal_size_bytes": wal.stat().st_size if wal.exists() else 0,
        "shm_present": shm.exists(),
    }
