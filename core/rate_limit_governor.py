"""Endpoint-aware rate-limit governor with strict priority ordering.

Priorities (contracts.RequestPriority): emergency exits and cancels are NEVER
starved — they may drive a bucket negative. Reconciliation waits briefly,
new entries wait a short bounded time, discovery/analytics never wait.

On 429: the endpoint's refill rate is halved for a penalty window.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from poly_alpha_sniper.core.contracts import RequestPriority
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("rate_governor")

# endpoint -> (tokens per second, burst)
DEFAULT_BUDGETS: dict[str, tuple[float, float]] = {
    "clob_order": (4.0, 8.0),
    "clob_public": (8.0, 16.0),
    "gamma": (3.0, 6.0),
    "data_api": (3.0, 6.0),
    "telegram": (1.0, 3.0),
}


@dataclass
class _Bucket:
    rate: float
    burst: float
    tokens: float = 0.0
    last_refill_ms: int = 0
    penalty_until_ms: int = 0
    used: int = 0
    throttled: int = 0

    def refill(self, now_ms: int) -> None:
        if self.last_refill_ms == 0:
            self.last_refill_ms = now_ms
            self.tokens = self.burst
            return
        rate = self.rate * (0.5 if now_ms < self.penalty_until_ms else 1.0)
        self.tokens = min(self.burst, self.tokens + rate * (now_ms - self.last_refill_ms) / 1000.0)
        self.last_refill_ms = now_ms


class RateLimitGovernor:
    def __init__(self, clock, budgets: dict[str, tuple[float, float]] | None = None):
        self.clock = clock
        self._buckets: dict[str, _Bucket] = {
            name: _Bucket(rate=r, burst=b) for name, (r, b) in (budgets or DEFAULT_BUDGETS).items()}

    def _bucket(self, endpoint: str) -> _Bucket:
        if endpoint not in self._buckets:
            self._buckets[endpoint] = _Bucket(rate=2.0, burst=4.0)
        return self._buckets[endpoint]

    async def acquire(self, endpoint: str, priority: RequestPriority) -> bool:
        b = self._bucket(endpoint)
        deadline_ms = {
            RequestPriority.EMERGENCY_EXIT: 0,
            RequestPriority.CANCEL: 0,
            RequestPriority.RECONCILE: 2000,
            RequestPriority.NEW_ENTRY: 500,
            RequestPriority.DISCOVERY: 0,
            RequestPriority.ANALYTICS: 0,
        }[priority]
        start = self.clock.now_ms()
        while True:
            b.refill(self.clock.now_ms())
            if priority in (RequestPriority.EMERGENCY_EXIT, RequestPriority.CANCEL):
                b.tokens -= 1.0  # may go negative: never starved
                b.used += 1
                return True
            if b.tokens >= 1.0:
                b.tokens -= 1.0
                b.used += 1
                return True
            if self.clock.now_ms() - start >= deadline_ms:
                b.throttled += 1
                return False
            await self.clock.sleep(0.02)

    def report_429(self, endpoint: str) -> None:
        b = self._bucket(endpoint)
        now = self.clock.now_ms()
        # exponential penalty: extend if already penalized
        base = 60_000
        if now < b.penalty_until_ms:
            base = min(240_000, (b.penalty_until_ms - now) * 2)
        b.penalty_until_ms = now + base
        b.throttled += 1
        log.warning("rate_limit_429", extra={"extra": {"endpoint": endpoint, "penalty_ms": base}})

    @property
    def healthy(self) -> bool:
        now = self.clock.now_ms()
        return all(now >= b.penalty_until_ms for b in self._buckets.values())

    @property
    def pressure_high(self) -> bool:
        total_used = sum(b.used for b in self._buckets.values()) or 1
        total_throttled = sum(b.throttled for b in self._buckets.values())
        return total_throttled / (total_used + total_throttled) > 0.2

    def usage(self) -> dict:
        now = self.clock.now_ms()
        return {name: {"used": b.used, "throttled": b.throttled,
                       "tokens": round(b.tokens, 2),
                       "penalized": now < b.penalty_until_ms}
                for name, b in self._buckets.items()}
