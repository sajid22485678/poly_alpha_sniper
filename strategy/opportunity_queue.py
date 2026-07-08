"""TTL'd priority queue of live opportunities, best alpha first."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import Signal


class OpportunityQueue:
    def __init__(self, clock, ttl_ms: int = 3000):
        self.clock = clock
        self.ttl_ms = ttl_ms
        self._by_market: dict[str, Signal] = {}

    def push(self, signal: Signal) -> None:
        cur = self._by_market.get(signal.market.market_id)
        if cur is None or signal.alpha_score > cur.alpha_score:
            self._by_market[signal.market.market_id] = signal

    def _expire(self) -> None:
        now = self.clock.now_ms()
        dead = [mid for mid, s in self._by_market.items() if now - s.ts_ms > self.ttl_ms]
        for mid in dead:
            del self._by_market[mid]

    def best(self) -> Optional[Signal]:
        self._expire()
        if not self._by_market:
            return None
        return max(self._by_market.values(), key=lambda s: s.alpha_score)

    def pop_best(self) -> Optional[Signal]:
        s = self.best()
        if s is not None:
            del self._by_market[s.market.market_id]
        return s

    def __len__(self) -> int:
        self._expire()
        return len(self._by_market)
