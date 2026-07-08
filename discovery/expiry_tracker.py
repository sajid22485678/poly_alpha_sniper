"""Track active markets' time-to-expiry and force-exit boundaries."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo


class ExpiryTracker:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._markets: dict[str, MarketInfo] = {}

    def track(self, market: MarketInfo) -> None:
        self._markets[market.market_id] = market

    def in_entry_window(self, market: MarketInfo, now_ms: int | None = None) -> bool:
        now = now_ms if now_ms is not None else self.clock.now_ms()
        tte = market.seconds_to_expiry(now)
        u = self.cfg.ultra_short_expiry
        return (u.min_time_to_expiry_seconds <= tte <= u.max_time_to_expiry_seconds
                and tte >= u.reject_if_less_than_seconds)

    def needs_force_exit(self, market: MarketInfo, now_ms: int | None = None) -> bool:
        now = now_ms if now_ms is not None else self.clock.now_ms()
        return market.seconds_to_expiry(now) <= self.cfg.ultra_short_expiry.force_exit_before_expiry_seconds

    def prune(self, grace_s: float = 120.0) -> list[MarketInfo]:
        now = self.clock.now_ms()
        expired = [m for m in self._markets.values()
                   if m.seconds_to_expiry(now) < -grace_s]
        for m in expired:
            self._markets.pop(m.market_id, None)
        return expired

    def active(self) -> list[MarketInfo]:
        now = self.clock.now_ms()
        return sorted((m for m in self._markets.values() if m.seconds_to_expiry(now) > 0),
                      key=lambda m: m.expiry_ts_ms)
