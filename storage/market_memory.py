"""Per-market rolling memory persisted in the market_memory table."""
from __future__ import annotations


class MarketMemory:
    def __init__(self, store):
        self.store = store
        self._cache: dict[str, dict] = {}

    def load(self, market_id: str) -> dict:
        if market_id in self._cache:
            return self._cache[market_id]
        rows = self.store.query(
            "SELECT * FROM market_memory WHERE market_id=? ORDER BY ts_ms DESC LIMIT 1",
            (market_id,))
        mem = rows[0] if rows else {"market_id": market_id, "fills": 0,
                                    "realized_edge": 0.0, "fill_quality_avg": 100.0,
                                    "blacklist_until_ms": 0, "notes": ""}
        self._cache[market_id] = dict(mem)
        return self._cache[market_id]

    def bump_fill(self, market_id: str, realized_edge: float,
                  fill_quality: float, ts_ms: int) -> dict:
        mem = self.load(market_id)
        n = int(mem.get("fills") or 0)
        mem["fills"] = n + 1
        mem["realized_edge"] = ((float(mem.get("realized_edge") or 0) * n + realized_edge)
                                / (n + 1))
        mem["fill_quality_avg"] = ((float(mem.get("fill_quality_avg") or 100) * n
                                    + fill_quality) / (n + 1))
        mem["ts_ms"] = ts_ms
        self.store.insert("market_memory", mem)
        return mem

    def blacklist(self, market_id: str, until_ms: int, note: str, ts_ms: int) -> None:
        mem = self.load(market_id)
        mem["blacklist_until_ms"] = until_ms
        mem["notes"] = note[:200]
        mem["ts_ms"] = ts_ms
        self.store.insert("market_memory", mem)

    def is_blacklisted(self, market_id: str, now_ms: int) -> bool:
        return int(self.load(market_id).get("blacklist_until_ms") or 0) > now_ms
