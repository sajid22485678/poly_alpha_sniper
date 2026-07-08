"""Market blacklist/whitelist with TTLs (Telegram + fill-quality driven)."""
from __future__ import annotations

from typing import Optional


class MarketFilterList:
    def __init__(self, clock):
        self.clock = clock
        self._black: dict[str, tuple[Optional[int], str]] = {}  # id -> (until_ms|None, reason)
        self._white: set[str] = set()

    def blacklist(self, market_id: str, reason: str, ttl_s: Optional[float] = None) -> None:
        until = None if ttl_s is None else self.clock.now_ms() + int(ttl_s * 1000)
        self._black[market_id] = (until, reason)
        self._white.discard(market_id)

    def whitelist(self, market_id: str) -> None:
        self._black.pop(market_id, None)
        self._white.add(market_id)

    def is_blocked(self, market_id: str) -> tuple[bool, str]:
        if market_id in self._white:
            return False, "whitelisted"
        entry = self._black.get(market_id)
        if entry is None:
            return False, ""
        until, reason = entry
        if until is not None and self.clock.now_ms() > until:
            del self._black[market_id]
            return False, "blacklist expired"
        return True, reason

    def export_records(self) -> list[dict]:
        now = self.clock.now_ms()
        rows = [{"market_id": mid, "blacklist_until_ms": until or -1, "notes": reason,
                 "ts_ms": now} for mid, (until, reason) in self._black.items()]
        rows += [{"market_id": mid, "blacklist_until_ms": 0, "notes": "whitelisted",
                  "ts_ms": now} for mid in self._white]
        return rows
