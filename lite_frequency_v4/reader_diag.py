"""Bounded diagnostics for SQLite read lifetimes in the Frequency V4 lane.

In WAL mode a checkpoint can only backfill up to the oldest active read-mark,
and the writer can only rewind (reset) the WAL file at an instant when no
connection holds a WAL read-mark at all.  Identifying which logical read
operation prevents reclamation therefore requires knowing, at any instant,
which readers are active, how old their snapshots are, and how the WAL moved
while they ran.

This registry records exactly that, at two scopes:

- ``job``: one logical operation on a dedicated read worker (a dashboard
  export, an integrity scan, an operational query batch).  Jobs measure
  reader *occupancy* -- how much wall time the worker spends with any read
  work in flight.
- ``statement``: one actual SQLite read transaction (a single ``SELECT`` /
  ``PRAGMA`` executed to completion).  Statements measure the true read-mark
  hold times that gate checkpoint progress.

Hard bounds: a small map of currently active operations plus fixed-size
per-operation aggregates.  The registry never opens a cursor, transaction, or
connection of its own -- WAL size is observed with ``os.stat`` -- so the
diagnostics cannot themselves pin the WAL.  Operation names never include SQL
text or parameters, so no secret can leak through them.
"""
from __future__ import annotations

import os
import threading
import time
from itertools import count
from pathlib import Path
from typing import Any, Optional


_JSON_STR_MAX = 120
#: Upper bound on distinct per-operation aggregate buckets.
_MAX_OP_BUCKETS = 48
#: Upper bound on concurrently tracked active operations (defensive; the
#: three read workers are serial, so steady state is a handful).
_MAX_ACTIVE = 64
#: Upper bound on active entries included in a snapshot payload.
_SNAPSHOT_ACTIVE_MAX = 12


def _wall_ms() -> int:
    return int(time.time() * 1_000)


class ReaderDiagnostics:
    """Thread-safe, bounded registry of active SQLite read operations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens = count(1)
        self._active: dict[int, dict[str, Any]] = {}
        self._ops: dict[str, dict[str, Any]] = {}
        self._context = threading.local()
        self._wal_path: Optional[Path] = None
        self._long_threshold_ms = 5_000.0
        self._counters = {
            "jobs_started": 0,
            "jobs_completed": 0,
            "statements_started": 0,
            "statements_completed": 0,
            "errors": 0,
            "active_overflow_dropped": 0,
        }
        self._last_release: Optional[dict[str, Any]] = None
        self._max_statement_ms = 0.0
        self._max_job_ms = 0.0

    # -- configuration ----------------------------------------------------

    def configure(
        self,
        *,
        wal_path: Optional[str | Path] = None,
        long_reader_threshold_ms: float = 5_000.0,
    ) -> None:
        with self._lock:
            self._wal_path = Path(wal_path) if wal_path is not None else None
            self._long_threshold_ms = max(1.0, float(long_reader_threshold_ms))

    def reset(self) -> None:
        """Drop all state (tests only); configuration is preserved."""

        with self._lock:
            self._active.clear()
            self._ops.clear()
            self._last_release = None
            self._max_statement_ms = 0.0
            self._max_job_ms = 0.0
            for key in self._counters:
                self._counters[key] = 0

    # -- registration -----------------------------------------------------

    def begin_job(self, worker: str, operation: str, kind: str = "") -> int:
        """Register one logical read operation starting on ``worker``."""

        token = self._begin(
            scope="job",
            worker=str(worker)[:_JSON_STR_MAX],
            operation=str(operation)[:_JSON_STR_MAX],
            kind=str(kind)[:_JSON_STR_MAX],
            conn_id=None,
        )
        self._context.job = (str(worker)[:_JSON_STR_MAX],
                             str(operation)[:_JSON_STR_MAX])
        return token

    def end_job(self, token: int, *, error: Optional[str] = None) -> None:
        self._context.job = None
        self._end(token, rows=None, error=error)

    def begin_statement(self, *, conn_id: Optional[int] = None) -> int:
        """Register one SQLite read transaction (a single statement).

        The statement is attributed to the job currently running on this
        thread, so aggregates group by logical operation rather than by SQL.
        """

        worker, operation = getattr(self._context, "job", None) or (
            "unattributed", "statement")
        return self._begin(
            scope="statement", worker=worker, operation=operation,
            kind="STATEMENT", conn_id=conn_id,
        )

    def end_statement(
        self, token: int, *,
        rows: Optional[int] = None, error: Optional[str] = None,
    ) -> None:
        self._end(token, rows=rows, error=error)

    def _begin(
        self, *, scope: str, worker: str, operation: str, kind: str,
        conn_id: Optional[int],
    ) -> int:
        started_mono = time.monotonic()
        wal_bytes = self._wal_size_bytes()
        with self._lock:
            if len(self._active) >= _MAX_ACTIVE:
                self._counters["active_overflow_dropped"] += 1
                return 0
            token = next(self._tokens)
            self._counters[
                "jobs_started" if scope == "job" else "statements_started"
            ] += 1
            self._active[token] = {
                "scope": scope,
                "worker": worker,
                "operation": operation,
                "kind": kind,
                "conn_id": conn_id,
                "thread_id": threading.get_ident(),
                "started_ts_ms": _wall_ms(),
                "started_mono": started_mono,
                "wal_bytes_start": wal_bytes,
            }
        return token

    def _end(
        self, token: int, *, rows: Optional[int], error: Optional[str],
    ) -> None:
        if not token:
            return
        ended_mono = time.monotonic()
        wal_bytes = self._wal_size_bytes()
        with self._lock:
            entry = self._active.pop(token, None)
            if entry is None:
                return
            scope = entry["scope"]
            duration_ms = max(0.0, (ended_mono - entry["started_mono"]) * 1_000.0)
            self._counters[
                "jobs_completed" if scope == "job" else "statements_completed"
            ] += 1
            if error:
                self._counters["errors"] += 1
            if scope == "statement":
                self._max_statement_ms = max(self._max_statement_ms, duration_ms)
            else:
                self._max_job_ms = max(self._max_job_ms, duration_ms)
            wal_growth = (
                wal_bytes - entry["wal_bytes_start"]
                if wal_bytes is not None and entry["wal_bytes_start"] is not None
                else None
            )
            key = f"{scope}:{entry['worker']}:{entry['operation']}"
            bucket = self._ops.get(key)
            if bucket is None:
                if len(self._ops) >= _MAX_OP_BUCKETS:
                    key = f"{scope}:__overflow__"
                    bucket = self._ops.get(key)
                if bucket is None:
                    bucket = {
                        "count": 0, "errors": 0, "total_ms": 0.0,
                        "max_ms": 0.0, "last_ms": 0.0, "last_rows": None,
                        "last_end_ts_ms": 0, "max_wal_growth_bytes": 0,
                    }
                    self._ops[key] = bucket
            bucket["count"] += 1
            if error:
                bucket["errors"] += 1
            bucket["total_ms"] += duration_ms
            bucket["max_ms"] = max(bucket["max_ms"], duration_ms)
            bucket["last_ms"] = duration_ms
            if rows is not None:
                bucket["last_rows"] = int(rows)
            bucket["last_end_ts_ms"] = _wall_ms()
            if wal_growth is not None and wal_growth > int(
                    bucket["max_wal_growth_bytes"]):
                bucket["max_wal_growth_bytes"] = int(wal_growth)
            self._last_release = {
                "scope": scope,
                "worker": entry["worker"],
                "operation": entry["operation"],
                "duration_ms": round(duration_ms, 3),
                "ended_mono": ended_mono,
                "ended_ts_ms": _wall_ms(),
                "error": (str(error)[:_JSON_STR_MAX] if error else None),
            }

    # -- observation ------------------------------------------------------

    def snapshot(self, *, compact: bool = False) -> dict[str, Any]:
        """A JSON-safe, bounded view of current reader state.

        ``compact=True`` omits the per-operation aggregate table; it is meant
        for embedding in per-checkpoint records where only the active-reader
        correlation matters.
        """

        now_mono = time.monotonic()
        wal_bytes = self._wal_size_bytes()
        with self._lock:
            statements = [
                entry for entry in self._active.values()
                if entry["scope"] == "statement"
            ]
            jobs = [
                entry for entry in self._active.values()
                if entry["scope"] == "job"
            ]
            oldest_statement = min(
                statements, key=lambda e: e["started_mono"], default=None)
            oldest_job = min(jobs, key=lambda e: e["started_mono"], default=None)
            over_threshold = sum(
                1 for entry in self._active.values()
                if (now_mono - entry["started_mono"]) * 1_000.0
                >= self._long_threshold_ms
            )
            last_release = self._last_release
            result: dict[str, Any] = {
                "ts_ms": _wall_ms(),
                "wal_bytes": wal_bytes,
                "active_reader_count": len(statements),
                "active_job_count": len(jobs),
                "oldest_reader_age_ms": self._age_ms(oldest_statement, now_mono),
                "oldest_reader_name": (
                    oldest_statement["operation"] if oldest_statement else None),
                "oldest_reader_worker": (
                    oldest_statement["worker"] if oldest_statement else None),
                "oldest_reader_connection_id": (
                    oldest_statement["conn_id"] if oldest_statement else None),
                "oldest_job_age_ms": self._age_ms(oldest_job, now_mono),
                "oldest_job_name": oldest_job["operation"] if oldest_job else None,
                "oldest_job_worker": oldest_job["worker"] if oldest_job else None,
                "readers_over_threshold": over_threshold,
                "long_reader_threshold_ms": self._long_threshold_ms,
                "last_reader_release_ms": (
                    round(max(0.0, (now_mono - last_release["ended_mono"]))
                          * 1_000.0, 3)
                    if last_release else None
                ),
                "last_release": (
                    {
                        key: last_release[key]
                        for key in ("scope", "worker", "operation",
                                    "duration_ms", "error")
                    }
                    if last_release else None
                ),
                "max_statement_ms": round(self._max_statement_ms, 3),
                "max_job_ms": round(self._max_job_ms, 3),
                "counters": dict(self._counters),
                "active": [
                    {
                        "scope": entry["scope"],
                        "worker": entry["worker"],
                        "operation": entry["operation"],
                        "age_ms": self._age_ms(entry, now_mono),
                        "wal_bytes_start": entry["wal_bytes_start"],
                    }
                    for entry in sorted(
                        self._active.values(),
                        key=lambda e: e["started_mono"],
                    )[:_SNAPSHOT_ACTIVE_MAX]
                ],
            }
            if not compact:
                result["ops"] = {
                    key: {
                        "count": bucket["count"],
                        "errors": bucket["errors"],
                        "total_ms": round(bucket["total_ms"], 3),
                        "max_ms": round(bucket["max_ms"], 3),
                        "last_ms": round(bucket["last_ms"], 3),
                        "last_rows": bucket["last_rows"],
                        "last_end_ts_ms": bucket["last_end_ts_ms"],
                        "max_wal_growth_bytes": bucket["max_wal_growth_bytes"],
                    }
                    for key, bucket in self._ops.items()
                }
            return result

    @staticmethod
    def _age_ms(entry: Optional[dict[str, Any]], now_mono: float) -> Optional[float]:
        if entry is None:
            return None
        return round(max(0.0, (now_mono - entry["started_mono"]) * 1_000.0), 3)

    def _wal_size_bytes(self) -> Optional[int]:
        path = self._wal_path
        if path is None:
            return None
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0


#: Process-wide registry.  The engine configures it at startup; the read
#: workers and read-only stores feed it; maintenance and the exporter read it.
READER_DIAGNOSTICS = ReaderDiagnostics()


__all__ = ["READER_DIAGNOSTICS", "ReaderDiagnostics"]
