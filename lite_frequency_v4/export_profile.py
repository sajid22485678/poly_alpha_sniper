"""Bounded, named-stage cost profile for the Frequency V4 dashboard export.

The reader-lifetime work (``reader_diag``) answers *which worker held a WAL
read-mark and for how long*.  It deliberately attributes every statement to
the enclosing **job** (``dashboard_export``), because that is the unit WAL
reclamation cares about.  That is the wrong resolution for a different
question: *which part of the export build is expensive*.  One export build is
~78 statements over a multi-gigabyte store, and a single ``dashboard_export``
aggregate cannot distinguish a 20 ms primary-key lookup from a 4 s full-table
scan.

This module adds the missing resolution without disturbing the reader
diagnostics:

- a **stage** is a named region of the build (``metrics.frequency.12h``,
  ``serialize``, ``publish``), nestable, timed on the monotonic clock;
- a **statement** is one SQL read, attributed to the stage that issued it and
  named by a stable fingerprint of its normalized SQL, so the same query keeps
  the same name across processes and runs.

Everything is hard-bounded: a fixed cap on distinct stage and statement
buckets, a fixed-size ring of recent durations per bucket for percentiles, and
no unbounded strings.  Statement names carry the primary table plus a hash of
the normalized SQL -- never SQL text, never bound parameters -- so no value
from the database can leak through a profile.

The profiler is inert unless a build activates it: ``stage()`` and
``record_statement()`` are cheap no-ops when no profile is bound to the calling
thread, so tests, replays, and any other ``build_metrics`` caller pay nothing.
"""
from __future__ import annotations

import hashlib
import re
import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence


#: Upper bound on distinct named stage buckets.
_MAX_STAGES = 128
#: Upper bound on distinct statement buckets.
_MAX_STATEMENTS = 192
#: Recent durations retained per bucket for percentile estimation.
_MAX_SAMPLES = 256
#: Upper bound on retained query-plan strings.
_MAX_PLANS = 48
#: A statement must be at least this slow before its plan is worth capturing.
_PLAN_CAPTURE_MS = 250.0
#: Rows above this count are counted but not digested (bounded change check).
_DIGEST_ROW_LIMIT = 4_000
#: Maximum characters of any single retained name/plan string.
_STR_MAX = 240

_WHITESPACE = re.compile(r"\s+")
_SQL_COMMENT = re.compile(r"--[^\n]*")
_FROM_TABLE = re.compile(r"\bfrom\s+([a-z_][a-z0-9_]*)")
_ANY_TABLE = re.compile(r"\b(?:from|join|into|update)\s+([a-z_][a-z0-9_]*)")


def _percentile(ordered: Sequence[float], fraction: float) -> Optional[float]:
    """Nearest-rank percentile of an already sorted sequence."""

    if not ordered:
        return None
    if len(ordered) == 1:
        return round(float(ordered[0]), 3)
    rank = max(1, min(len(ordered), int(-(-len(ordered) * fraction // 1))))
    return round(float(ordered[rank - 1]), 3)


def normalize_sql(sql: str) -> str:
    """Whitespace/comment-normalized, lowercased SQL used for naming only."""

    text = _SQL_COMMENT.sub(" ", str(sql))
    return _WHITESPACE.sub(" ", text).strip().lower()


def statement_label(sql: str) -> str:
    """A stable, secret-free name for one SQL statement.

    ``<primary table>#<8 hex of sha256(normalized sql)>``.  The hash is over
    the SQL *text* only -- bound parameters are never seen by this function --
    so the name is stable across runs and cannot carry a database value.
    """

    normalized = normalize_sql(sql)
    match = _FROM_TABLE.search(normalized) or _ANY_TABLE.search(normalized)
    table = match.group(1) if match else "sql"
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:8]
    return f"{table}#{digest}"[:_STR_MAX]


class _Bucket:
    """Fixed-size aggregate for one named stage or statement."""

    __slots__ = (
        "count", "errors", "total_ms", "max_ms", "last_ms", "samples",
        "last_rows", "total_rows", "changed_count", "unchanged_count",
        "last_digest", "plan", "calls_in_build", "max_calls_in_build",
    )

    def __init__(self) -> None:
        self.count = 0
        self.errors = 0
        self.total_ms = 0.0
        self.max_ms = 0.0
        self.last_ms = 0.0
        self.samples: deque[float] = deque(maxlen=_MAX_SAMPLES)
        self.last_rows: Optional[int] = None
        self.total_rows = 0
        self.changed_count = 0
        self.unchanged_count = 0
        self.last_digest: Optional[str] = None
        self.plan: Optional[str] = None
        self.calls_in_build = 0
        self.max_calls_in_build = 0

    def observe(self, duration_ms: float, *, error: bool) -> None:
        self.count += 1
        if error:
            self.errors += 1
        self.total_ms += duration_ms
        self.max_ms = max(self.max_ms, duration_ms)
        self.last_ms = duration_ms
        self.samples.append(duration_ms)

    def view(self) -> dict[str, Any]:
        ordered = sorted(self.samples)
        result: dict[str, Any] = {
            "count": self.count,
            "errors": self.errors,
            "total_ms": round(self.total_ms, 3),
            "last_ms": round(self.last_ms, 3),
            "max_ms": round(self.max_ms, 3),
            "p50_ms": _percentile(ordered, 0.50),
            "p90_ms": _percentile(ordered, 0.90),
            "p99_ms": _percentile(ordered, 0.99),
            "sampled": len(ordered),
        }
        if self.last_rows is not None:
            result["last_rows"] = self.last_rows
            result["total_rows"] = self.total_rows
        if self.changed_count or self.unchanged_count:
            result["result_changed"] = self.changed_count
            result["result_unchanged"] = self.unchanged_count
        if self.max_calls_in_build:
            result["max_calls_per_build"] = self.max_calls_in_build
        if self.plan:
            result["plan"] = self.plan
        return result


class ExportProfiler:
    """Thread-safe, bounded stage/statement profile for the export build."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stages: dict[str, _Bucket] = {}
        self._statements: dict[str, _Bucket] = {}
        self._local = threading.local()
        self._builds = 0
        self._plans_captured = 0
        self._dropped_stages = 0
        self._dropped_statements = 0
        self._last_build: dict[str, Any] = {}
        self._payload_bytes_last = 0
        self._payload_bytes_max = 0

    # -- lifecycle --------------------------------------------------------

    def reset(self) -> None:
        """Drop all accumulated state (tests and fresh runtimes)."""

        with self._lock:
            self._stages.clear()
            self._statements.clear()
            self._builds = 0
            self._plans_captured = 0
            self._dropped_stages = 0
            self._dropped_statements = 0
            self._last_build = {}
            self._payload_bytes_last = 0
            self._payload_bytes_max = 0
        self._local.active = None
        self._local.path = None
        self._local.calls = None

    @property
    def active(self) -> bool:
        """True while the calling thread is inside a profiled build."""

        return bool(getattr(self._local, "active", False))

    def observe_payload_bytes(self, size: int) -> None:
        """Record the encoded size of the export just published."""

        with self._lock:
            value = max(0, int(size))
            self._payload_bytes_last = value
            self._payload_bytes_max = max(self._payload_bytes_max, value)

    def observe_stage(self, name: str, duration_ms: float) -> None:
        """Record one externally measured stage (e.g. worker queue wait).

        Unlike :meth:`stage`, this does not require an active build: the wait a
        job spends queued is measured before the build it belongs to starts.
        """

        with self._lock:
            bucket = self._bucket(self._stages, str(name)[:_STR_MAX], _MAX_STAGES)
            if bucket is None:
                self._dropped_stages += 1
                return
            bucket.observe(max(0.0, float(duration_ms)), error=False)

    @contextmanager
    def build(self) -> Iterator[None]:
        """Bind this profiler to the calling thread for one export build."""

        outer = getattr(self._local, "active", False)
        if outer:
            # A nested build would corrupt the per-build call counters; the
            # export is single-flight, so this is defensive only.
            yield
            return
        self._local.active = True
        self._local.path = []
        self._local.calls = {}
        started = time.perf_counter()
        error = False
        try:
            yield
        except BaseException:
            error = True
            raise
        finally:
            duration_ms = max(0.0, (time.perf_counter() - started) * 1_000.0)
            calls = dict(getattr(self._local, "calls", None) or {})
            self._local.active = False
            self._local.path = None
            self._local.calls = None
            with self._lock:
                self._builds += 1
                bucket = self._bucket(self._stages, "build.total", _MAX_STAGES)
                if bucket is not None:
                    bucket.observe(duration_ms, error=error)
                for name, repeats in calls.items():
                    target = self._statements.get(name)
                    if target is not None:
                        target.max_calls_in_build = max(
                            target.max_calls_in_build, repeats)
                self._last_build = {
                    "duration_ms": round(duration_ms, 3),
                    "statements": sum(calls.values()),
                    "distinct_statements": len(calls),
                    "error": error,
                }

    # -- stage timing -----------------------------------------------------

    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        """Time a named region.  A no-op outside a profiled build."""

        if not getattr(self._local, "active", False):
            yield
            return
        path = self._local.path
        path.append(str(name)[:_STR_MAX])
        full = ".".join(path)
        started = time.perf_counter()
        error = False
        try:
            yield
        except BaseException:
            error = True
            raise
        finally:
            duration_ms = max(0.0, (time.perf_counter() - started) * 1_000.0)
            path.pop()
            with self._lock:
                bucket = self._bucket(self._stages, full, _MAX_STAGES)
                if bucket is None:
                    self._dropped_stages += 1
                else:
                    bucket.observe(duration_ms, error=error)

    def current_stage(self) -> str:
        path = getattr(self._local, "path", None)
        return ".".join(path) if path else "unstaged"

    # -- statement timing -------------------------------------------------

    def record_statement(
        self, sql: str, duration_ms: float, *,
        rows: Optional[int] = None, digest: Optional[str] = None,
        error: bool = False,
    ) -> str:
        """Record one SQL read, attributed to the current stage.

        Returns the bucket name.  Callers must compute ``duration_ms`` around
        the SQLite call only: every other cost here (naming, digesting) happens
        after the read-mark is already released.
        """

        if not getattr(self._local, "active", False):
            return ""
        name = f"{self.current_stage()}|{statement_label(sql)}"[:_STR_MAX]
        calls = self._local.calls
        calls[name] = calls.get(name, 0) + 1
        with self._lock:
            bucket = self._bucket(self._statements, name, _MAX_STATEMENTS)
            if bucket is None:
                self._dropped_statements += 1
                return name
            bucket.observe(duration_ms, error=error)
            if rows is not None:
                bucket.last_rows = int(rows)
                bucket.total_rows += int(rows)
            if digest is not None:
                if bucket.last_digest is None:
                    pass
                elif digest == bucket.last_digest:
                    bucket.unchanged_count += 1
                else:
                    bucket.changed_count += 1
                bucket.last_digest = digest
        return name

    def wants_plan(self, name: str, duration_ms: float) -> bool:
        """True when this statement is slow enough to be worth explaining."""

        if not name or duration_ms < _PLAN_CAPTURE_MS:
            return False
        with self._lock:
            if self._plans_captured >= _MAX_PLANS:
                return False
            bucket = self._statements.get(name)
            return bucket is not None and bucket.plan is None

    def record_plan(self, name: str, plan: str) -> None:
        with self._lock:
            bucket = self._statements.get(name)
            if bucket is None or bucket.plan is not None:
                return
            if self._plans_captured >= _MAX_PLANS:
                return
            bucket.plan = str(plan)[:_STR_MAX]
            self._plans_captured += 1

    # -- observation ------------------------------------------------------

    def snapshot(self, *, top: int = 24) -> dict[str, Any]:
        """A bounded, JSON-safe view: every stage, the costliest statements."""

        with self._lock:
            statements = sorted(
                self._statements.items(),
                key=lambda item: item[1].total_ms,
                reverse=True,
            )
            return {
                "builds": self._builds,
                "last_build": dict(self._last_build),
                "payload_bytes_last": self._payload_bytes_last,
                "payload_bytes_max": self._payload_bytes_max,
                "dropped_stage_buckets": self._dropped_stages,
                "dropped_statement_buckets": self._dropped_statements,
                "plans_captured": self._plans_captured,
                "stages": {
                    name: bucket.view()
                    for name, bucket in sorted(self._stages.items())
                },
                "statements": {
                    name: bucket.view()
                    for name, bucket in statements[:max(0, int(top))]
                },
                "statement_bucket_count": len(self._statements),
            }

    @staticmethod
    def _bucket(
        table: dict[str, _Bucket], name: str, limit: int,
    ) -> Optional[_Bucket]:
        bucket = table.get(name)
        if bucket is not None:
            return bucket
        if len(table) >= limit:
            overflow = table.get("__overflow__")
            if overflow is None:
                if len(table) > limit:
                    return None
                overflow = _Bucket()
                table["__overflow__"] = overflow
            return overflow
        bucket = _Bucket()
        table[name] = bucket
        return bucket


#: Process-wide export profile.  The read-only store feeds statements into it,
#: the export build opens stages, and the engine publishes its snapshot.
EXPORT_PROFILE = ExportProfiler()


def stage(name: str):
    """Module-level shorthand so callers need no profiler reference."""

    return EXPORT_PROFILE.stage(name)


def row_digest(rows: Any) -> Optional[str]:
    """Bounded, deterministic digest of a result set, or None if too large.

    Used only to report *whether a section's data changed between exports*.
    It is a diagnostic, never a cache key: caching decisions are made from
    explicit source versions, not from hashes of already-paid-for reads.
    """

    try:
        if rows is None:
            return None
        if isinstance(rows, dict):
            items: Sequence[Any] = (rows,)
        elif isinstance(rows, (list, tuple)):
            items = rows
        else:
            return None
        if len(items) > _DIGEST_ROW_LIMIT:
            return None
        hasher = hashlib.sha256()
        for row in items:
            if isinstance(row, dict):
                for key in sorted(row):
                    hasher.update(str(key).encode("utf-8", "replace"))
                    hasher.update(b"\x1f")
                    hasher.update(repr(row[key]).encode("utf-8", "replace"))
                    hasher.update(b"\x1e")
            else:
                hasher.update(repr(row).encode("utf-8", "replace"))
            hasher.update(b"\x1d")
        return hasher.hexdigest()[:16]
    except Exception:  # a digest must never break an export
        return None


__all__ = [
    "EXPORT_PROFILE",
    "ExportProfiler",
    "normalize_sql",
    "row_digest",
    "stage",
    "statement_label",
]
