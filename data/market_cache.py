"""TTL cache of tradable MarketInfo, keyed by market_id and token_id."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import MarketInfo


class MarketCache:
    def __init__(self):
        self._by_market: dict[str, MarketInfo] = {}
        self._by_token: dict[str, str] = {}

    def upsert(self, markets: list[MarketInfo]) -> None:
        for m in markets:
            self._by_market[m.market_id] = m
            if m.yes_token_id:
                self._by_token[m.yes_token_id] = m.market_id
            if m.no_token_id:
                self._by_token[m.no_token_id] = m.market_id

    def get(self, market_id: str) -> Optional[MarketInfo]:
        return self._by_market.get(market_id)

    def by_token(self, token_id: str) -> Optional[MarketInfo]:
        mid = self._by_token.get(token_id)
        return self._by_market.get(mid) if mid else None

    def active_markets(self, now_ms: int) -> list[MarketInfo]:
        out = [m for m in self._by_market.values()
               if m.active and not m.closed and m.expiry_ts_ms > now_ms]
        out.sort(key=lambda m: m.expiry_ts_ms)
        return out

    def prune(self, now_ms: int, grace_ms: int = 120_000) -> list[MarketInfo]:
        """Drop expired markets (after grace); returns them so callers can
        untrack their orderbooks."""
        dead_ids = [mid for mid, m in self._by_market.items()
                    if m.expiry_ts_ms and m.expiry_ts_ms < now_ms - grace_ms]
        dead: list[MarketInfo] = []
        for mid in dead_ids:
            m = self._by_market.pop(mid)
            self._by_token.pop(m.yes_token_id, None)
            self._by_token.pop(m.no_token_id, None)
            dead.append(m)
        return dead

    def __len__(self) -> int:
        return len(self._by_market)
