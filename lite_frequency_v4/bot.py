"""Process entrypoint for the isolated Frequency V4 shadow engine."""
from __future__ import annotations

import asyncio
import signal
import sys
from typing import Optional

from .config import load_frequency_v4_config
from .engine import FrequencyV4Engine
from .runtime import V4RuntimeFiles
from .store import V4Store


async def _main() -> int:
    cfg = load_frequency_v4_config()
    runtime = V4RuntimeFiles(cfg.runtime_dir)
    store: Optional[V4Store] = None
    engine: Optional[FrequencyV4Engine] = None
    exit_code = 0
    stop_reason = "graceful_stop"
    runtime.acquire()
    try:
        store = V4Store(cfg.db_path)
        engine = FrequencyV4Engine(cfg, runtime, store)
        loop = asyncio.get_running_loop()

        def request_stop() -> None:
            nonlocal stop_reason
            stop_reason = "signal_stop"
            if engine is not None:
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
        if engine is not None:
            try:
                await engine.stop(stop_reason)
                final_state = engine._runtime_state("STOPPED" if exit_code == 0 else "FAILED")
            except Exception as exc:
                exit_code = 1
                print(f"Frequency V4 stop error: {type(exc).__name__}: {exc}",
                      file=sys.stderr, flush=True)
        if store is not None:
            store.close()
        runtime.release(final_state)
    return exit_code


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
