"""Clock abstraction.

All time reads in strategy/risk/execution logic go through a Clock instance so
that backtest (SimClock) and live (WallClock) share identical code paths.

ASSUMPTION: epoch milliseconds everywhere.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone


class Clock:
    def now_ms(self) -> int:
        raise NotImplementedError

    def now_s(self) -> float:
        return self.now_ms() / 1000.0

    def now_dt(self) -> datetime:
        return datetime.fromtimestamp(self.now_ms() / 1000.0, tz=timezone.utc)

    async def sleep(self, seconds: float) -> None:
        raise NotImplementedError


class WallClock(Clock):
    def now_ms(self) -> int:
        return int(time.time() * 1000)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class SimClock(Clock):
    """Deterministic clock for backtest/replay. Advance manually."""

    def __init__(self, start_ms: int = 0):
        self._now_ms = int(start_ms)

    def now_ms(self) -> int:
        return self._now_ms

    def set_ms(self, ts_ms: int) -> None:
        if ts_ms < self._now_ms:
            # never move backwards; replay data must be time-sorted
            return
        self._now_ms = int(ts_ms)

    def advance_ms(self, delta_ms: int) -> None:
        self._now_ms += int(delta_ms)

    async def sleep(self, seconds: float) -> None:
        # simulated sleep: advance time instantly
        self.advance_ms(int(seconds * 1000))
