"""Persistent runtime state + heartbeat files (crash-safe atomic writes)."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from poly_alpha_sniper.core.config_loader import PROJECT_ROOT
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("runtime_state")

DEFAULT_STATE: dict[str, Any] = {
    "mode": "", "started_ts_ms": 0, "heartbeat_ts_ms": 0, "panic_active": False,
    "kill_active": False, "paused": False, "aggression_mode": "NORMAL",
    "restart_events_ms": [], "open_position_summary": [],
}


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


class RuntimeState:
    def __init__(self, path: Optional[str] = None, clock=None):
        self.path = Path(path) if path else (PROJECT_ROOT / "runtime" / "state.json")
        self.heartbeat_path = self.path.parent / "heartbeat.json"
        self.clock = clock
        self.state: dict[str, Any] = dict(DEFAULT_STATE)
        self.load()

    def _now_ms(self) -> int:
        if self.clock is not None:
            return self.clock.now_ms()
        import time
        return int(time.time() * 1000)

    def load(self) -> dict[str, Any]:
        try:
            if self.path.exists():
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    self.state = {**DEFAULT_STATE, **loaded}
        except (json.JSONDecodeError, OSError) as exc:
            log.error("state_corrupt_using_defaults", extra={"extra": {"error": repr(exc)}})
            try:
                os.replace(self.path, self.path.with_suffix(".corrupt"))
            except OSError:
                pass
            self.state = dict(DEFAULT_STATE)
        return self.state

    def update(self, **kwargs: Any) -> None:
        self.state.update(kwargs)

    def save(self) -> None:
        try:
            _atomic_write(self.path, json.dumps(self.state, default=str))
        except OSError as exc:
            log.error("state_save_failed", extra={"extra": {"error": repr(exc)}})

    def heartbeat(self) -> None:
        now = self._now_ms()
        self.state["heartbeat_ts_ms"] = now
        self.save()
        try:
            _atomic_write(self.heartbeat_path, json.dumps(
                {"ts_ms": now, "pid": os.getpid(), "mode": self.state.get("mode", "")}))
        except OSError as exc:
            log.error("heartbeat_write_failed", extra={"extra": {"error": repr(exc)}})

    def record_restart(self, now_ms: Optional[int] = None) -> int:
        """Record a restart event; return count within the last hour."""
        now = now_ms if now_ms is not None else self._now_ms()
        events = [t for t in self.state.get("restart_events_ms", []) if now - t < 3_600_000]
        events.append(now)
        self.state["restart_events_ms"] = events
        self.save()
        return len(events)


def read_heartbeat(path: Optional[str] = None) -> dict[str, Any]:
    p = Path(path) if path else (PROJECT_ROOT / "runtime" / "heartbeat.json")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
