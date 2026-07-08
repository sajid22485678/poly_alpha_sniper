"""Watchdog supervisor process.

Spawns `python main.py --mode <mode>`, monitors the heartbeat file and the
child process, restarts on crash or stale heartbeat, bounded by
runtime.max_restarts_per_hour. Crash incidents are written to
runtime/incidents/.

Run:  python -m poly_alpha_sniper.core.watchdog --mode shadow_live
Test: --once evaluates health once without spawning.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from poly_alpha_sniper.core.clock import Clock, WallClock
from poly_alpha_sniper.core.config_loader import PROJECT_ROOT, load_config
from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.core.runtime_state import read_heartbeat

log = get_logger("watchdog")


class Watchdog:
    def __init__(self, cfg, mode: str, clock: Optional[Clock] = None,
                 python_exe: str = "", profile: str = ""):
        self.cfg = cfg
        self.mode = mode
        self.profile = profile
        self.clock = clock or WallClock()
        self.python_exe = python_exe or sys.executable
        self.restart_events_ms: list[int] = []
        self.proc: Optional[subprocess.Popen] = None
        self.stopped = False

    # --- overridable seams for tests -------------------------------------
    def spawn(self) -> None:
        args = [self.python_exe, str(PROJECT_ROOT / "main.py")]
        if self.profile:
            args += ["--profile", self.profile]
        else:
            args += ["--mode", self.mode]
        log.info("watchdog_spawn", extra={"extra": {"mode": self.mode}})
        self.proc = subprocess.Popen(args, cwd=str(PROJECT_ROOT))

    def child_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def kill_child(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=self.cfg.runtime.graceful_shutdown_seconds)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    # --- core logic (pure enough to test) --------------------------------
    def heartbeat_stale(self) -> bool:
        hb = read_heartbeat()
        if not hb:
            return False  # no heartbeat yet — child may still be starting
        age_s = (self.clock.now_ms() - int(hb.get("ts_ms", 0))) / 1000.0
        return age_s > self.cfg.runtime.stale_heartbeat_seconds

    def restarts_last_hour(self) -> int:
        now = self.clock.now_ms()
        self.restart_events_ms = [t for t in self.restart_events_ms if now - t < 3_600_000]
        return len(self.restart_events_ms)

    def can_restart(self) -> bool:
        return self.restarts_last_hour() < self.cfg.runtime.max_restarts_per_hour

    def record_restart(self) -> None:
        self.restart_events_ms.append(self.clock.now_ms())

    def write_incident(self, kind: str, detail: str) -> Path:
        inc_dir = PROJECT_ROOT / "runtime" / "incidents"
        inc_dir.mkdir(parents=True, exist_ok=True)
        path = inc_dir / f"watchdog_{self.clock.now_ms()}.json"
        path.write_text(json.dumps({
            "ts_ms": self.clock.now_ms(), "kind": kind, "detail": detail,
            "mode": self.mode, "restarts_last_hour": self.restarts_last_hour()}), encoding="utf-8")
        return path

    def check_once(self) -> dict:
        """Single health evaluation — decision only, no side effects on procs."""
        alive = self.child_alive()
        stale = self.heartbeat_stale()
        decision = "ok"
        if not alive:
            decision = "restart" if self.can_restart() else "give_up"
        elif stale:
            decision = "kill_and_restart" if self.can_restart() else "give_up"
        return {"child_alive": alive, "heartbeat_stale": stale,
                "restarts_last_hour": self.restarts_last_hour(),
                "max_restarts_per_hour": self.cfg.runtime.max_restarts_per_hour,
                "decision": decision}

    # --- run loop ---------------------------------------------------------
    def run(self) -> None:
        self.spawn()
        while not self.stopped:
            time.sleep(2.0)
            status = self.check_once()
            if status["decision"] == "ok":
                continue
            if status["decision"] == "give_up":
                self.write_incident("restart_budget_exhausted",
                                    f"max {self.cfg.runtime.max_restarts_per_hour}/h reached")
                log.error("watchdog_give_up", extra={"extra": status})
                self.kill_child()
                return
            # restart paths
            self.write_incident("crash" if status["decision"] == "restart" else "stale_heartbeat",
                                json.dumps(status))
            log.warning("watchdog_restart", extra={"extra": status})
            self.kill_child()
            if not self.cfg.runtime.auto_restart_on_crash:
                return
            self.record_restart()
            self.spawn()


def main() -> None:
    parser = argparse.ArgumentParser(description="poly_alpha_sniper watchdog")
    parser.add_argument("--mode", default="shadow_live")
    parser.add_argument("--profile", default="")
    parser.add_argument("--once", action="store_true", help="single health check, no spawn")
    args = parser.parse_args()

    cfg = load_config(mode_override=args.mode or None, profile_override=args.profile or None)
    wd = Watchdog(cfg, args.mode, profile=args.profile)
    if args.once:
        print(json.dumps(wd.check_once(), indent=2))
        return
    wd.run()


if __name__ == "__main__":
    main()
