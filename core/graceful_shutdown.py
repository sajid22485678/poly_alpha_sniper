"""Graceful shutdown coordinator.

Cleanup callbacks run in priority order (lower first: cancel orders -> save
state -> close sessions -> goodbye message), each bounded by the remaining
time budget. Windows-safe: uses signal.signal (Proactor loop does not support
add_signal_handler).
"""
from __future__ import annotations

import asyncio
import signal
from typing import Awaitable, Callable

from poly_alpha_sniper.core.logger import get_logger

log = get_logger("graceful_shutdown")


class GracefulShutdown:
    def __init__(self, clock, timeout_s: float = 10.0):
        self.clock = clock
        self.timeout_s = timeout_s
        self._callbacks: list[tuple[int, str, Callable[[], Awaitable[None]]]] = []
        self._ran = False
        self.triggered = asyncio.Event()

    def register(self, name: str, callback: Callable[[], Awaitable[None]], priority: int = 50) -> None:
        self._callbacks.append((priority, name, callback))

    def install(self) -> None:
        def _handler(signum, frame):  # noqa: ARG001
            log.info("shutdown_signal", extra={"extra": {"signal": signum}})
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.triggered.set)
            except RuntimeError:
                self.triggered.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handler)
            except (ValueError, OSError):  # non-main thread / unsupported
                pass

    async def run_shutdown(self) -> None:
        if self._ran:
            return
        self._ran = True
        start = self.clock.now_ms()
        for priority, name, cb in sorted(self._callbacks, key=lambda x: x[0]):
            remaining = self.timeout_s - (self.clock.now_ms() - start) / 1000.0
            if remaining <= 0:
                log.error("shutdown_budget_exhausted", extra={"extra": {"skipped": name}})
                continue
            try:
                await asyncio.wait_for(cb(), timeout=remaining)
                log.info("shutdown_step_done", extra={"extra": {"step": name, "priority": priority}})
            except Exception as exc:  # noqa: BLE001 - keep shutting down
                log.error("shutdown_step_failed", extra={"extra": {"step": name, "error": repr(exc)}})
