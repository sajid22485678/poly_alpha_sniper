"""Async in-process pub/sub event bus.

Topics are plain strings; payloads are arbitrary objects (usually contract
dataclasses). Subscribers are async callables. Publishing never raises out of
a subscriber failure — errors are logged and isolated so one bad consumer
cannot take the trading loop down.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Awaitable, Callable

from poly_alpha_sniper.core.logger import get_logger

log = get_logger("event_bus")

Handler = Callable[[Any], Awaitable[None]]


class Topics:
    CEX_TICK = "cex.tick"
    CEX_STATS = "cex.stats"
    SHOCK = "signal.shock"
    SIGNAL = "signal.opportunity"
    PREDICTION = "signal.prediction"
    ORDERBOOK = "poly.orderbook"
    MARKET_REFRESH = "poly.markets"
    ORDER_UPDATE = "exec.order_update"
    FILL = "exec.fill"
    POSITION_UPDATE = "portfolio.position"
    EXIT = "exec.exit"
    PANIC = "risk.panic"
    KILL_SWITCH = "risk.kill_switch"
    MODE_CHANGE = "risk.aggression_mode_change"
    INCIDENT = "ops.incident"
    HEALTH = "ops.health"
    TELEGRAM_OUT = "ops.telegram_out"
    NEAR_MISS = "signal.near_miss"


class EventBus:
    def __init__(self) -> None:
        self._subs: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._subs[topic].append(handler)

    def unsubscribe(self, topic: str, handler: Handler) -> None:
        if handler in self._subs.get(topic, []):
            self._subs[topic].remove(handler)

    async def publish(self, topic: str, payload: Any) -> None:
        for handler in list(self._subs.get(topic, [])):
            try:
                await handler(payload)
            except Exception as exc:  # noqa: BLE001 - isolation by design
                log.error("subscriber_error", extra={"extra": {
                    "topic": topic, "handler": getattr(handler, "__qualname__", str(handler)),
                    "error": repr(exc)}})

    def publish_nowait(self, topic: str, payload: Any) -> None:
        """Fire-and-forget publish from sync code inside a running loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self.publish(topic, payload))
