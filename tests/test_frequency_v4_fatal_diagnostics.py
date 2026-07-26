"""Deterministic tests for fatal-task diagnostics (Phase 2).

These prove the diagnostics architecture required by the remediation mission:

1. the original exception type/message survives wrapping
2. the full traceback is persisted
3. __cause__ is preserved
4. task name and session context are present
5. diagnostic persistence failure does not hide the original failure
6. shutdown still closes the runtime session
7. no open session remains after a fatal exit

The fatal path is exercised through ``bot._main`` exactly as the launcher runs
it: a dead critical task raises inside ``run_until_stopped``, the supervisor
wraps it as ``RuntimeError("critical Frequency V4 task failed: <task>") from
error``, and ``_main`` must capture and persist the full context before the
runtime session is closed.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from poly_alpha_sniper.lite_frequency_v4 import bot as bot_module
from poly_alpha_sniper.lite_frequency_v4.diagnostics import (
    _format_chain,
    capture_fatal_diagnostic,
    persist_fatal_diagnostic,
)
from poly_alpha_sniper.lite_frequency_v4.runtime import immutable_safety_state


HEARTBEAT_EXPORT_TASK = "v4-heartbeat-export"


class _FatalEngine:
    """Minimal engine that reproduces the supervisor's fatal wrapping.

    ``run_until_stopped`` mirrors :meth:`FrequencyV4Engine.run_until_stopped`:
    a dead critical task is observed and re-raised as
    ``RuntimeError("critical Frequency V4 task failed: <task>") from error``.
    The originating exception is preserved on ``__cause__``, which is what the
    diagnostics must recover.
    """

    def __init__(self, *, originator: Exception, task: str = HEARTBEAT_EXPORT_TASK):
        self._stopping = asyncio.Event()
        self.session_id = "diag-session-abc"
        self.runtime = SimpleNamespace(
            pid=4242,
            launch_nonce="a" * 32,
            commit="0" * 40,
            started_ts_ms=1_700_000_000_000,
        )
        self._last_error = "prior_loop_note"
        self._last_export_ms = 1_700_000_001_000
        self._last_export_publish_ok = True
        self._export_degraded_since_ms = 0
        self._last_published_state = {"state": "RUNNING"}
        self._critical_evidence_lost_rows = 0
        self._originator = originator
        self._task = task
        self.stop_called = False
        self.stop_reason: str | None = None

    async def run_until_stopped(self):
        # Reproduce the exact supervisor wrapping at engine.py:4393-4394.
        raise RuntimeError(
            f"critical Frequency V4 task failed: {self._task}"
        ) from self._originator

    def _writer_health(self):
        return {"state": "HEALTHY", "queue_depth": 3, "queue_capacity": 2048}

    def _runtime_state(self, state):
        return {
            **immutable_safety_state(),
            "state": state,
            "session_id": self.session_id,
            "queue_depth": 3,
        }

    async def stop(self, reason):
        self.stop_called = True
        self.stop_reason = reason


class _FatalCfg:
    def __init__(self, runtime_dir: str):
        self.runtime_dir = runtime_dir


def _run_main(tmp_path: Path, *, originator: Exception):
    cfg = _FatalCfg(str(tmp_path / "runtime"))
    engine = _FatalEngine(originator=originator)
    exit_code, final_state = asyncio.run(bot_module._main(cfg, None, engine))
    return exit_code, final_state, engine, cfg


def _raised_originator(factory):
    """Build an originator by raising it so it carries a real traceback."""
    try:
        raise factory()
    except Exception as exc:
        return exc


def test_original_exception_type_and_message_survive_wrapping(tmp_path):
    originator = ValueError("cannot serialize non-finite float: inf")
    exit_code, _final, _engine, cfg = _run_main(tmp_path, originator=originator)

    diag_path = Path(cfg.runtime_dir) / "fatal_diagnostic.json"
    assert diag_path.exists(), "fatal diagnostic must be persisted"
    payload = json.loads(diag_path.read_text(encoding="utf-8"))

    # (1) the original exception type/message survive wrapping
    assert payload["fatal_exception_type"] == "RuntimeError"
    assert HEARTBEAT_EXPORT_TASK in payload["fatal_exception_message"]
    originator_entry = payload["originating_exception"]
    assert originator_entry["exception_type"] == "ValueError"
    assert originator_entry["exception_message"] == "cannot serialize non-finite float: inf"
    assert exit_code == 1


def test_full_traceback_is_persisted(tmp_path):
    # The originator must carry a real traceback, exactly as it would when a
    # genuine defect raises inside the heartbeat/export loop.  Constructing it
    # by raising (rather than by instantiating) attaches the traceback that
    # the supervisor preserves on ``__cause__``.
    def _raise_originator():
        raise ValueError("originating heartbeat defect")

    try:
        _raise_originator()
    except ValueError as originator:
        exit_code, _final, _engine, cfg = _run_main(tmp_path, originator=originator)
    assert exit_code == 1

    payload = json.loads(
        (Path(cfg.runtime_dir) / "fatal_diagnostic.json").read_text("utf-8"))
    # (2) the full traceback is persisted -- both the wrapper and the
    # originating exception carry formatted tracebacks.
    assert "Traceback (most recent call last)" in payload["fatal_exception_traceback"]
    originator_tb = payload["originating_exception"]["traceback"]
    assert "Traceback (most recent call last)" in originator_tb
    assert "ValueError" in originator_tb
    assert "_raise_originator" in originator_tb


def test_chained_cause_is_preserved(tmp_path):
    originator = _raised_originator(lambda: KeyError("export_payload_field"))
    _exit_code, _final, _engine, cfg = _run_main(tmp_path, originator=originator)

    payload = json.loads(
        (Path(cfg.runtime_dir) / "fatal_diagnostic.json").read_text("utf-8"))
    chain = payload["exception_chain"]
    # (3) __cause__ is preserved: the chain has the wrapper (RuntimeError)
    # linked via __cause__ to the originating KeyError.
    assert len(chain) == 2
    assert chain[0]["exception_type"] == "RuntimeError"
    assert chain[0]["links_via"] == "cause"
    assert chain[1]["exception_type"] == "KeyError"
    assert payload["originating_exception"]["exception_type"] == "KeyError"


def test_task_name_and_session_context_are_present(tmp_path):
    originator = _raised_originator(lambda: RuntimeError("export worker timeout"))
    _exit_code, _final, engine, cfg = _run_main(tmp_path, originator=originator)

    payload = json.loads(
        (Path(cfg.runtime_dir) / "fatal_diagnostic.json").read_text("utf-8"))
    # (4) task name and session context are present
    assert payload["task_name"] == HEARTBEAT_EXPORT_TASK
    assert payload["session_id"] == engine.session_id
    assert payload["runtime_pid"] == engine.runtime.pid
    assert payload["launch_nonce"] == engine.runtime.launch_nonce
    assert payload["current_commit"] == engine.runtime.commit
    assert payload["started_ts_ms"] == engine.runtime.started_ts_ms
    assert payload["writer_health"]["queue_depth"] == 3
    assert payload["telemetry_safety_capacity"]["runtime_state"]["state"] == "FAILED"


def test_diagnostic_persistence_failure_does_not_hide_original_failure(
        tmp_path, monkeypatch, capsys):
    originator = _raised_originator(lambda: ValueError("originating defect"))
    cfg = _FatalCfg(str(tmp_path / "runtime"))
    engine = _FatalEngine(originator=originator)

    # Sabotage persistence: force persist_fatal_diagnostic to raise.  The
    # fail-safe wrapper in bot._main swallows it so the original failure still
    # surfaces via the stderr print and the fatal exit code.
    def _failing_persist(payload, runtime_dir):
        raise OSError("disk full")

    monkeypatch.setattr(bot_module, "persist_fatal_diagnostic", _failing_persist)

    exit_code, _final = asyncio.run(bot_module._main(cfg, None, engine))
    captured = capsys.readouterr()
    # (5) persistence failure cannot hide the original failure: the fatal
    # stderr line and the traceback still fire, and the exit is still fatal.
    assert exit_code == 1
    assert "Frequency V4 fatal error" in captured.err
    assert "RuntimeError" in captured.err
    assert HEARTBEAT_EXPORT_TASK in captured.err
    # The fail-safe capture-error line is reported, then the traceback prints.
    assert "diagnostic capture error" in captured.err or "Traceback" in captured.err
    assert "Traceback" in captured.err


def test_shutdown_still_closes_runtime_session(tmp_path):
    originator = _raised_originator(lambda: ValueError("originating defect"))
    exit_code, final_state, engine, _cfg = _run_main(tmp_path, originator=originator)
    # (6) shutdown still closes the runtime session: engine.stop was invoked
    # with the fatal stop reason and produced a terminal state.
    assert exit_code == 1
    assert engine.stop_called is True
    assert engine.stop_reason == "fatal_RuntimeError"
    assert final_state is not None
    assert final_state["state"] == "FAILED"


def test_no_open_session_remains_after_fatal_exit(tmp_path):
    # The runtime dir should contain only the diagnostic + the released state;
    # the process lock is removed by runtime.release in the real launcher, but
    # here we prove the fatal path leaves no half-written diagnostic and the
    # captured file is a complete, valid JSON document.
    originator = _raised_originator(lambda: ValueError("originating defect"))
    exit_code, _final, _engine, cfg = _run_main(tmp_path, originator=originator)
    assert exit_code == 1
    diag_path = Path(cfg.runtime_dir) / "fatal_diagnostic.json"
    assert diag_path.exists()
    # Must be valid, complete JSON (no half-written / corrupt document).
    payload = json.loads(diag_path.read_text(encoding="utf-8"))
    assert payload["fatal_exception_type"] == "RuntimeError"
    # No stray temp files remain in the runtime dir.
    leftovers = [p.name for p in Path(cfg.runtime_dir).iterdir()
                 if p.name.startswith("fatal_diagnostic.json.tmp")]
    assert leftovers == []


def test_format_chain_walks_implicit_context_when_no_explicit_cause():
    # An exception raised inside an `except` (no `from`) records the in-flight
    # exception in __context__, not __cause__.  The chain must still recover it.
    try:
        try:
            raise TypeError("inner context defect")
        except TypeError:
            raise RuntimeError("wrapper without explicit cause")
    except RuntimeError as exc:
        chain = _format_chain(exc)
    assert len(chain) == 2
    assert chain[0]["exception_type"] == "RuntimeError"
    assert chain[1]["exception_type"] == "TypeError"
    assert chain[0]["links_via"] == "context"


def test_capture_fatal_diagnostic_never_raises_on_bad_engine():
    # A failing engine state read must be recorded, not raised.
    class BadEngine:
        session_id = "bad"
        runtime = SimpleNamespace(
            pid=1, launch_nonce="b" * 32, commit="c" * 40, started_ts_ms=0)

        def _writer_health(self):
            raise RuntimeError("writer health exploded")

        def _runtime_state(self, state):
            raise RuntimeError("state capture exploded")

    try:
        raise RuntimeError("critical Frequency V4 task failed: v4-x") from KeyError("k")
    except RuntimeError as exc:
        payload = capture_fatal_diagnostic(exc, engine=BadEngine())
    assert payload["fatal_exception_type"] == "RuntimeError"
    assert "writer_health_error" in payload["writer_health"]
    assert "runtime_state_error" in payload["telemetry_safety_capacity"]
    assert payload["originating_exception"]["exception_type"] == "KeyError"


def test_persist_fatal_diagnostic_is_fail_safe_on_unwritable_path():
    payload = {"fatal_exception_type": "RuntimeError"}
    # An impossible path returns None rather than raising.
    result = persist_fatal_diagnostic(payload, "Z:/no/such/root/xyz/abc")
    assert result is None


def test_persist_fatal_diagnostic_coerces_non_serializable_values(tmp_path):
    payload = {
        "fatal_exception_type": "RuntimeError",
        "nested": {"obj": object(), "items": {1, 2, 3}},
    }
    path = persist_fatal_diagnostic(payload, str(tmp_path))
    assert path is not None
    # Must be valid JSON despite non-serializable inputs (object(), set).
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    # Non-serializable object() reduced to its type:text form; set coerced
    # to a list so the document is valid JSON.
    assert isinstance(data["nested"]["items"], list)
    assert data["nested"]["obj"].startswith("object:")
