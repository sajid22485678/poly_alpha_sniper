"""Single-instance guard via pid lock file.

One instance per trading mode; stale locks (dead pid) are auto-cleared.
Live modes are hard-blocked if ANY other instance holds a live lock.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from poly_alpha_sniper.core.config_loader import PROJECT_ROOT
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("process_lock")

LIVE_MODES = ("live_micro", "live_full")


class AlreadyRunning(RuntimeError):
    pass


def _pid_alive(pid: int) -> bool:
    try:
        import psutil
        return psutil.pid_exists(pid)
    except ImportError:  # pragma: no cover
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


class ProcessLock:
    def __init__(self, path: Optional[str] = None):
        self.path = Path(path) if path else (PROJECT_ROOT / "runtime" / "live.lock")
        self._held = False
        self._mode = ""

    def acquire(self, mode: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existing = self._read()
        if existing:
            pid = int(existing.get("pid", -1))
            other_mode = str(existing.get("mode", ""))
            if pid != os.getpid() and _pid_alive(pid):
                if mode in LIVE_MODES or other_mode in LIVE_MODES or other_mode == mode:
                    raise AlreadyRunning(
                        f"another instance (pid={pid}, mode={other_mode}) holds the lock")
            else:
                log.warning("stale_lock_cleared", extra={"extra": {"pid": pid, "mode": other_mode}})
        self.path.write_text(json.dumps({"pid": os.getpid(), "mode": mode}), encoding="utf-8")
        self._held = True
        self._mode = mode

    def release(self) -> None:
        if not self._held:
            return
        try:
            existing = self._read()
            if existing and int(existing.get("pid", -1)) == os.getpid():
                self.path.unlink(missing_ok=True)
        except OSError:
            pass
        self._held = False

    def _read(self) -> Optional[dict]:
        try:
            if self.path.exists():
                return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return None

    def __enter__(self) -> "ProcessLock":
        return self

    def __exit__(self, *exc) -> None:
        self.release()
