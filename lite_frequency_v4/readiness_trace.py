"""Dense readiness observation sink for the Frequency V4 telemetry lane.

The readiness controller decides once per flush interval (250 ms in the shadow
configuration), but every published diagnostic is sampled on the ~2 s heartbeat
or the ~5 s dashboard export.  A blocker that appears and clears inside one
heartbeat is therefore invisible to every existing observer, even though it
resets ``_healthy_streak`` and costs ten ticks of recovery.

This module closes that gap without changing a single control decision:

* the controller appends one bounded observation per tick to an in-memory ring
  (see ``_AdaptiveTelemetryController.enable_readiness_trace``);
* the engine's existing heartbeat loop drains that ring every ~2 s and hands
  the batch to the runtime I/O worker, which appends it as JSON lines.

So the hot path costs one dict append per 250 ms, the event loop costs one
deque drain, and every byte of file I/O happens on the worker that already owns
runtime file writes.  Nothing here feeds back into the lane.

The observer is opt-in through :data:`READINESS_TRACE_ENV`.  Unset -- the
default, including every test -- and no ring is allocated and no file is
touched.
"""
from __future__ import annotations

import gc
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

#: Set to a writable file path to enable dense readiness observation.
READINESS_TRACE_ENV = "POLY_ALPHA_V4_READINESS_TRACE"

#: Hard bound on one trace file.  A 250 ms tick writes ~4 records per second;
#: at roughly 1.5 KB per record a 50-minute gate produces ~18 MB, so this bound
#: is never reached in normal use and exists only so a runaway cannot fill the
#: disk.  On reaching it the sink stops writing and says so, exactly once.
MAX_TRACE_BYTES = 512 * 1024 * 1024


def trace_path_from_env(
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[Path]:
    """Resolve the configured trace path, or ``None`` when disabled."""

    source = os.environ if environ is None else environ
    raw = str(source.get(READINESS_TRACE_ENV, "") or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def thread_cpu_snapshot() -> list[dict[str, Any]]:
    """Per-thread CPU time for this process, named.

    ``psutil`` reports OS thread ids and CPU times but not Python thread names;
    ``threading.enumerate`` reports names and native ids but no CPU time.  Only
    the process itself can join them, which is why this is sampled in-process
    rather than by the external sampler.
    """

    names: dict[int, str] = {}
    for thread in threading.enumerate():
        native = getattr(thread, "native_id", None)
        if native is not None:
            names[int(native)] = str(thread.name)
    try:
        import psutil

        rows = psutil.Process().threads()
    except Exception:  # noqa: BLE001 - observation is never fatal
        return []
    return [
        {
            "tid": int(row.id),
            "name": names.get(int(row.id), "native"),
            "user_s": round(float(row.user_time), 4),
            "system_s": round(float(row.system_time), 4),
        }
        for row in rows
    ]


def gc_snapshot() -> dict[str, Any]:
    """Cumulative garbage-collection activity, per generation."""

    try:
        stats = gc.get_stats()
    except Exception:  # noqa: BLE001
        stats = []
    return {
        "counts": list(gc.get_count()),
        "enabled": gc.isenabled(),
        "generations": [
            {
                "collections": int(entry.get("collections", 0)),
                "collected": int(entry.get("collected", 0)),
                "uncollectable": int(entry.get("uncollectable", 0)),
            }
            for entry in stats
        ],
    }


class ReadinessTraceSink:
    """Append-only JSON-lines sink for dense readiness observations.

    Every method is safe to call from the runtime I/O worker thread and from
    nowhere else at the same time; the engine serialises its drains, so no lock
    is needed and none is taken on the event loop.
    """

    def __init__(self, path: str | Path, *, max_bytes: int = MAX_TRACE_BYTES):
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive int")
        self.path = Path(path)
        self.max_bytes = int(max_bytes)
        self.records_written = 0
        self.bytes_written = 0
        self.write_failures = 0
        self.truncated = False
        self._last_error: Optional[str] = None

    def open(self) -> None:
        """Create the parent directory and the (empty) trace file."""

        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch()

    def append(self, records: Sequence[Mapping[str, Any]]) -> int:
        """Append records as JSON lines; returns how many were written."""

        if not records or self.truncated:
            return 0
        lines = []
        for record in records:
            try:
                lines.append(json.dumps(record, separators=(",", ":"),
                                        default=str))
            except (TypeError, ValueError) as exc:
                self.write_failures += 1
                self._last_error = f"{type(exc).__name__}:{exc}"[:200]
        if not lines:
            return 0
        blob = "\n".join(lines) + "\n"
        encoded = blob.encode("utf-8")
        if self.bytes_written + len(encoded) > self.max_bytes:
            self.truncated = True
            self._last_error = "trace_size_limit_reached"
            return 0
        try:
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(blob)
        except OSError as exc:
            self.write_failures += 1
            self._last_error = f"{type(exc).__name__}:{exc}"[:200]
            return 0
        self.records_written += len(lines)
        self.bytes_written += len(encoded)
        return len(lines)

    def status(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "records_written": self.records_written,
            "bytes_written": self.bytes_written,
            "write_failures": self.write_failures,
            "truncated": self.truncated,
            "last_error": self._last_error,
        }


def engine_context_record(
    *,
    state_name: str,
    loop_lag_ms: float,
    heartbeat_age_ms: Optional[float],
    export_age_ms: Optional[float],
    extra: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """One ~2 s engine-cadence record, correlated to the tick records by time."""

    return {
        "kind": "engine",
        "mono": round(time.monotonic(), 4),
        "wall_ms": int(time.time() * 1_000),
        "state": str(state_name),
        "loop_lag_ms": round(float(loop_lag_ms), 3),
        "heartbeat_age_ms": (
            None if heartbeat_age_ms is None
            else round(float(heartbeat_age_ms), 1)),
        "export_age_ms": (
            None if export_age_ms is None else round(float(export_age_ms), 1)),
        "threads": thread_cpu_snapshot(),
        "gc": gc_snapshot(),
        **(dict(extra) if extra else {}),
    }
