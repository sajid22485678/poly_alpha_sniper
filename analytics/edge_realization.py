"""Edge realization: does realized PnL confirm the predicted edge?"""
from __future__ import annotations

from collections import defaultdict, deque

from poly_alpha_sniper.core.contracts import clamp


class EdgeRealization:
    def __init__(self, window: int = 50):
        self._records: deque[dict] = deque(maxlen=window)
        self._by_tier: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=30))

    def record(self, expected_edge: float, realized_pnl_usd: float,
               size_usd: float, tier: str = "") -> dict:
        realized_edge = realized_pnl_usd / size_usd if size_usd > 0 else 0.0
        row = {"expected": expected_edge, "realized": realized_edge, "tier": tier}
        self._records.append(row)
        if tier:
            self._by_tier[tier].append(row)
        return row

    @staticmethod
    def _ratio(rows) -> float:
        exp = sum(r["expected"] for r in rows if r["expected"] > 0)
        if exp <= 1e-9:
            return 0.0
        real = sum(r["realized"] for r in rows if r["expected"] > 0)
        return clamp(real / exp, -1.0, 2.0)

    def ratio(self) -> float:
        return self._ratio(self._records)

    def by_tier(self) -> dict[str, float]:
        return {tier: round(self._ratio(rows), 3) for tier, rows in self._by_tier.items()}

    def n(self) -> int:
        return len(self._records)
