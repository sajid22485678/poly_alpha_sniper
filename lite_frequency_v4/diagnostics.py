"""Bounded, fail-safe fatal-task diagnostics for Frequency V4.

A fatal critical-task failure used to terminate the runtime with only the
exception's ``type: message`` printed to stderr.  The originating exception's
traceback, chained ``__cause__``/``__context__``, and the runtime context that
would identify *why* it happened were discarded by the top-level handler before
the original traceback could be inspected -- leaving a ``RuntimeError:
critical Frequency V4 task failed: v4-heartbeat-export`` with no recoverable
detail.

This module captures the complete exception context the moment a fatal error is
observed and persists it to a bounded diagnostic file in the runtime directory.
It is deliberately fail-safe: a diagnostic-persistence failure can never hide
the original failure or recursively crash the runtime, and it never persists
secrets or credentials.  Diagnostics survive even when the main runtime is
terminating, because they are written before the terminal ``engine.stop`` /
``runtime.release`` sequence.

The persistence path is bounded: a single diagnostic file is overwritten
atomically (no unbounded growth), and every value is coerced to a safe JSON
representation before it reaches disk.
"""
from __future__ import annotations

import json
import os
import time
import traceback
from pathlib import Path
from typing import Any, Optional


def now_ms() -> int:
    return int(time.time() * 1000)


def _safe_type(obj: Any) -> str:
    try:
        return type(obj).__name__
    except Exception:
        return "<unknown-type>"


def _format_chain(exc: Optional[BaseException]) -> list[dict[str, Any]]:
    """Walk an exception's ``__cause__``/``__context__`` chain.

    Each linked exception contributes its type, message and formatted
    traceback.  The chain is bounded by the depth of the linked-exception
    graph (Python caps implicit context at one link; explicit ``raise ...
    from`` chains are programmer-controlled and short), so this cannot grow
    without bound from a single failure.
    """
    chain: list[dict[str, Any]] = []
    seen: set[int] = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        entry: dict[str, Any] = {
            "exception_type": _safe_type(current),
            "exception_message": _coerce_text(current),
            "traceback": _coerce_traceback(current),
        }
        chain.append(entry)
        # Prefer the explicit cause; fall back to the implicit context.  An
        # explicit ``raise X from Y`` records Y in __cause__; an exception
        # raised inside an ``except`` records the in-flight exception in
        # __context__.  Capturing both links is what makes the originating
        # heartbeat/export defect recoverable when the supervisor wraps it.
        linked = current.__cause__
        link_kind = "cause"
        if linked is None:
            linked = current.__context__
            link_kind = "context"
        if linked is None or id(linked) in seen:
            break
        current = linked
        entry["links_via"] = link_kind
    return chain


def _coerce_text(value: Any, *, limit: int = 4096) -> str:
    try:
        text = str(value)
    except Exception:
        return "<unrepresentable>"
    return text if len(text) <= limit else text[:limit] + "<truncated>"


def _coerce_traceback(exc: Optional[BaseException], *, limit: int = 64) -> str:
    if exc is None:
        return ""
    try:
        return "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__, limit=limit)
        )
    except Exception:
        try:
            return traceback.format_exc()
        except Exception:
            return "<traceback-unavailable>"


def _coerce_state(value: Any, *, depth: int = 0) -> Any:
    """Reduce an arbitrary runtime value to a bounded JSON representation.

    Non-serializable objects (locks, threads, enum members outside the JSON
    primitive set, custom classes) are reduced to their type name and text
    form; non-finite floats are replaced with their string form so the JSON
    document cannot be invalidated by ``inf``/``nan``; recursion and dict/list
    depth are capped.
    """
    if depth > 5:
        return "<depth-capped>"
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        try:
            if value != value or value in (float("inf"), float("-inf")):
                return str(value)
        except Exception:
            return str(value)
        return value
    if isinstance(value, (list, tuple, set, frozenset)):
        try:
            items = list(value)
        except Exception:
            return _coerce_text(value)
        return [_coerce_state(item, depth=depth + 1) for item in items[:64]]
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for index, (key, val) in enumerate(value.items()):
            if index >= 64:
                out["<truncated>"] = f"{len(value) - index} more keys"
                break
            out[_coerce_text(key)] = _coerce_state(val, depth=depth + 1)
        return out
    if isinstance(value, Path):
        try:
            return str(value)
        except Exception:
            return "<path>"
    # Enum members commonly carry a value primitive; fall back to that.
    value_text = _coerce_text(value)
    return f"{_safe_type(value)}:{value_text}"


def _safe_attr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _telemetry_snapshot(engine: Any) -> dict[str, Any]:
    """Capture the telemetry safety/capacity state at failure time.

    ``_runtime_state`` builds the authoritative snapshot the heartbeat would
    publish; reusing it guarantees the diagnostic carries the same queue
    depth, in-flight, capacity and loss fields an operator would read on the
    dashboard.  A failure to build the snapshot is itself diagnostic, so it is
    recorded rather than raised.
    """
    snapshot: dict[str, Any] = {}
    telemetry = _safe_attr(engine, "telemetry")
    if telemetry is not None:
        try:
            snapshot["telemetry_snapshot"] = _coerce_state(
                _safe_attr(telemetry, "snapshot", lambda: {})()
                if callable(_safe_attr(telemetry, "snapshot"))
                else _safe_attr(telemetry, "snapshot", {})
            )
        except Exception as exc:
            snapshot["telemetry_snapshot_error"] = f"{_safe_type(exc)}:{_coerce_text(exc)}"
    try:
        runtime_state = engine._runtime_state("FAILED")  # type: ignore[attr-defined]
        snapshot["runtime_state"] = _coerce_state(runtime_state)
    except Exception as exc:
        snapshot["runtime_state_error"] = f"{_safe_type(exc)}:{_coerce_text(exc)}"
    return snapshot


def capture_fatal_diagnostic(
    exc: BaseException,
    *,
    engine: Any,
    task_name: Optional[str] = None,
) -> dict[str, Any]:
    """Build the complete fatal-task diagnostic payload.

    Captures the original exception type/message, the full formatted traceback,
    the chained ``__cause__``/``__context__`` graph, and the runtime context
    (session, PID, commit, timestamps, last heartbeat/export, queue/in-flight
    state, telemetry safety/capacity state).  This never raises: a failure to
    read engine state records the read error instead.
    """
    runtime = _safe_attr(engine, "runtime")
    telemetry_state = _telemetry_snapshot(engine)
    writer = {}
    try:
        writer = _coerce_state(engine._writer_health())  # type: ignore[attr-defined]
    except Exception as exc_w:
        writer = {"writer_health_error": f"{_safe_type(exc_w)}:{_coerce_text(exc_w)}"}

    chain = _format_chain(exc)
    originator = chain[-1] if chain else {
        "exception_type": _safe_type(exc),
        "exception_message": _coerce_text(exc),
        "traceback": _coerce_traceback(exc),
    }

    payload: dict[str, Any] = {
        "captured_at_ms": now_ms(),
        "captured_at_mono": _coerce_state(_safe_attr(time, "monotonic", lambda: None)()),
        "fatal_exception_type": _safe_type(exc),
        "fatal_exception_message": _coerce_text(exc),
        "fatal_exception_traceback": _coerce_traceback(exc),
        "exception_chain": chain,
        "originating_exception": originator,
        "task_name": task_name,
        "session_id": _coerce_state(_safe_attr(engine, "session_id")),
        "runtime_pid": _coerce_state(_safe_attr(runtime, "pid")),
        "launch_nonce": _coerce_state(_safe_attr(runtime, "launch_nonce")),
        "current_commit": _coerce_state(_safe_attr(runtime, "commit")),
        "started_ts_ms": _coerce_state(_safe_attr(runtime, "started_ts_ms")),
        "last_error": _coerce_text(_safe_attr(engine, "_last_error")),
        "last_export_ts_ms": _coerce_state(_safe_attr(engine, "_last_export_ms")),
        "last_export_publish_ok": _coerce_state(
            _safe_attr(engine, "_last_export_publish_ok")),
        "export_degraded_since_ts_ms": _coerce_state(
            _safe_attr(engine, "_export_degraded_since_ms")),
        "last_published_state": _coerce_state(
            _safe_attr(engine, "_last_published_state")),
        "stopping": _coerce_state(_safe_attr(engine, "_stopping")),
        "critical_evidence_lost_rows": _coerce_state(
            _safe_attr(engine, "_critical_evidence_lost_rows")),
        "writer_health": writer,
        "telemetry_safety_capacity": telemetry_state,
    }
    return payload


def persist_fatal_diagnostic(
    payload: dict[str, Any],
    runtime_dir: str | Path,
) -> Optional[Path]:
    """Persist a fatal diagnostic payload atomically.

    Writes a single bounded ``fatal_diagnostic.json`` into ``runtime_dir`` via
    a temporary file + atomic replace, so a crash mid-write cannot leave a
    corrupt half-document.  This function is fail-safe: any I/O or encoding
    failure is swallowed and ``None`` is returned, so diagnostic persistence
    can never hide the original failure or recursively crash the runtime.

    Returns the path written on success, or ``None`` on failure.
    """
    target_dir = Path(runtime_dir)
    target = target_dir / "fatal_diagnostic.json"
    temporary = target.with_name(
        f"{target.name}.tmp.{os.getpid()}.{time.time_ns()}")
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        temporary.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":"),
                       default=_coerce_state),
            encoding="utf-8",
        )
        os.replace(temporary, target)
        return target
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except Exception:
            pass
        return None
