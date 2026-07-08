"""Panic mode: fail-safe freeze with async escalation callbacks.

Triggers accumulate for the incident report. Only clear(manual=True) resets.
Registered async callbacks fire on activation (cancel orders, emergency close,
Telegram alert) — scheduled on the running loop; if no loop is running they
are queued and can be drained by the app.
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

from poly_alpha_sniper.core.logger import get_logger

log = get_logger("panic_mode")

PanicCallback = Callable[[str], Awaitable[None]]


class PanicMode:
    def __init__(self):
        self._active = False
        self.triggers: list[str] = []
        self._callbacks: list[PanicCallback] = []
        self.pending_callbacks: list[tuple[PanicCallback, str]] = []

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def reason(self) -> str:
        return self.triggers[-1] if self.triggers else ""

    def on_activate(self, callback: PanicCallback) -> None:
        self._callbacks.append(callback)

    def activate(self, trigger: str) -> None:
        first = not self._active
        self._active = True
        self.triggers.append(trigger)
        log.error("panic_activated", extra={"extra": {"trigger": trigger, "first": first}})
        if first:
            for cb in self._callbacks:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(cb(trigger))
                except RuntimeError:
                    self.pending_callbacks.append((cb, trigger))

    async def drain_pending(self) -> None:
        pending, self.pending_callbacks = self.pending_callbacks, []
        for cb, trigger in pending:
            try:
                await cb(trigger)
            except Exception as exc:  # noqa: BLE001
                log.error("panic_callback_failed", extra={"extra": {"error": repr(exc)}})

    def clear(self, manual: bool = True) -> None:
        if not manual:
            raise PermissionError("panic mode requires manual clear")
        log.warning("panic_cleared", extra={"extra": {"triggers": self.triggers[-5:]}})
        self._active = False
