"""Process entrypoint for the isolated Frequency V4 shadow engine."""
from __future__ import annotations

import asyncio
import signal
import sys
from typing import Any, Optional

from .config import FrequencyV4Config, load_frequency_v4_config
from .engine import FrequencyV4Engine
from .runtime import V4RuntimeFiles


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
        print(f"Frequency V4 fatal error: {type(exc).__name__}: {exc}",
              file=sys.stderr, flush=True)
    finally:
        final_state = None
        try:
            await engine.stop(stop_reason)
            final_state = engine._runtime_state("STOPPED" if exit_code == 0 else "FAILED")
        except Exception as exc:
            exit_code = 1
            print(f"Frequency V4 stop error: {type(exc).__name__}: {exc}",
                  file=sys.stderr, flush=True)
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
