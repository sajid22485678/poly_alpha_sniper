"""Process entrypoint for the isolated Frequency V4 shadow engine."""
from __future__ import annotations

import asyncio
import re
import signal
import sys
import traceback
from typing import Any, Optional

from .config import FrequencyV4Config, load_frequency_v4_config
from .diagnostics import capture_fatal_diagnostic, persist_fatal_diagnostic
from .engine import FrequencyV4Engine
from .runtime import V4RuntimeFiles, immutable_safety_state


def _extract_task_name(exc: BaseException) -> Optional[str]:
    """Recover the failing task name from the supervisor's wrapping message.

    ``run_until_stopped`` wraps a dead critical task as
    ``RuntimeError("critical Frequency V4 task failed: <task>") from error``.
    The originating exception is preserved on ``__cause__``; the task name is
    only embedded in the wrapper message.  Recovering it here keeps the
    diagnostic payload self-describing without changing the supervisor's
    exception contract.
    """
    message = str(exc)
    match = re.search(
        r"critical Frequency V4 task failed:\s*(\S+)", message)
    return match.group(1) if match else None


async def _main(
    cfg: FrequencyV4Config, runtime: V4RuntimeFiles,
    engine: FrequencyV4Engine,
) -> tuple[int, Optional[dict[str, Any]]]:
    """Run V4 after synchronous process ownership has been acquired.

    Configuration and runtime lock-file I/O intentionally happen outside the
    asyncio loop in :func:`main`.  Once this coroutine starts, every SQLite and
    runtime-filesystem operation is delegated to an explicitly bounded owner
    worker by :class:`FrequencyV4Engine`.
    """

    exit_code = 0
    stop_reason = "graceful_stop"
    try:
        loop = asyncio.get_running_loop()

        def request_stop() -> None:
            nonlocal stop_reason
            stop_reason = "signal_stop"
            engine._stopping.set()

        for name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, request_stop)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, lambda *_args: loop.call_soon_threadsafe(request_stop))
        print("Frequency V4 shadow starting; live execution surface absent.", flush=True)
        await engine.run_until_stopped()
    except Exception as exc:
        exit_code = 1
        stop_reason = f"fatal_{type(exc).__name__}"
        # Persist the full fatal-task diagnostic *before* the (deliberately
        # terse) stderr print.  The originating exception, its chained
        # __cause__/__context__, traceback, session/PID/commit context, last
        # heartbeat/export state and telemetry safety/capacity snapshot are
        # captured atomically to the runtime dir.  Persistence is fail-safe:
        # a write failure cannot hide the original failure or recursively
        # crash the runtime, so the stderr line below always still fires.
        task_name = _extract_task_name(exc)
        try:
            diagnostic = capture_fatal_diagnostic(
                exc, engine=engine, task_name=task_name)
            persist_fatal_diagnostic(
                diagnostic,
                runtime_dir=cfg.runtime_dir,
            )
        except Exception as diag_exc:  # pragma: no cover - fail-safe path
            print(
                "Frequency V4 diagnostic capture error: "
                f"{type(diag_exc).__name__}: {diag_exc}",
                file=sys.stderr, flush=True)
        # The stderr line keeps the launcher log concise; the full traceback
        # is also echoed here so it is visible alongside the persisted file
        # even when the runtime directory is not inspected.
        print(f"Frequency V4 fatal error: {type(exc).__name__}: {exc}",
              file=sys.stderr, flush=True)
        traceback.print_exc()
    finally:
        final_state = None
        try:
            await engine.stop(stop_reason)
            final_state = engine._runtime_state("STOPPED" if exit_code == 0 else "FAILED")
        except Exception as exc:
            exit_code = 1
            print(f"Frequency V4 stop error: {type(exc).__name__}: {exc}",
                  file=sys.stderr, flush=True)
            # stop() may fail because its own terminal critical command was
            # durably rejected.  Preserve the resulting exact loss counters
            # and FAILED lifecycle state before runtime.release removes the
            # process lock.  The still-open DB session remains independently
            # fail-closed and visible to the next startup/reconciliation.
            try:
                final_state = engine._runtime_state("FAILED")
            except Exception as state_exc:
                print(
                    "Frequency V4 final-state capture error: "
                    f"{type(state_exc).__name__}: {state_exc}",
                    file=sys.stderr,
                    flush=True,
                )
                baseline = getattr(
                    engine, "_telemetry_lifetime_baseline", {})
                critical_lost = (
                    int(baseline.get("true_lost_critical_rows", 0))
                    + int(getattr(
                        engine, "_critical_evidence_lost_rows", 0)))
                telemetry_fallback = dict(baseline)
                telemetry_fallback.update({
                    "true_lost_critical_rows": critical_lost,
                    "critical_evidence_lost_count": critical_lost,
                })
                final_state = {
                    **immutable_safety_state(),
                    "session_id": getattr(engine, "session_id", None),
                    "state": "FAILED",
                    "db_path": getattr(engine, "_db_path_resolved", None),
                    "runtime_dir": getattr(
                        engine, "_runtime_dir_resolved", None),
                    "export_path": getattr(
                        engine, "_export_path_resolved", None),
                    "lineage_fingerprint": getattr(
                        engine, "_lineage_fingerprint", None),
                    "persistence": {
                        "telemetry": telemetry_fallback,
                    },
                }
    return exit_code, final_state


def main() -> int:
    # Keep configuration and lock/nonce filesystem work outside the shared
    # event loop.  V4RuntimeFiles.acquire is nonce-safe and process-specific.
    cfg = load_frequency_v4_config()
    runtime = V4RuntimeFiles(cfg.runtime_dir)
    runtime.acquire()
    final_state: Optional[dict[str, Any]] = None
    try:
        # This synchronous process scan is intentionally before asyncio and
        # before the persistence writer can reconcile any durable command.
        runtime.verify_process_ownership()
        # Engine construction precomputes immutable paths/config outside
        # asyncio; the shared source/strategy loop starts only afterward.
        engine = FrequencyV4Engine(cfg, runtime)
        exit_code, final_state = asyncio.run(_main(cfg, runtime, engine))
        return exit_code
    finally:
        runtime.release(final_state)


if __name__ == "__main__":
    raise SystemExit(main())
