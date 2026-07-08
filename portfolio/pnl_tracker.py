"""Closed-trade PnL aggregation: winrate, PF, expectancy, drawdown, slices."""
from __future__ import annotations

from collections import defaultdict


class PnlTracker:
    def __init__(self, clock):
        self.clock = clock
        self.closes: list[dict] = []

    def record_close(self, pnl: float, asset: str = "", tier: str = "",
                     direction: str = "", side: str = "", hold_s: float = 0.0,
                     ts_ms: int | None = None) -> None:
        self.closes.append({
            "pnl": pnl, "asset": asset, "tier": tier, "direction": direction,
            "side": side, "hold_s": hold_s,
            "ts_ms": ts_ms if ts_ms is not None else self.clock.now_ms()})

    # ------------------------------------------------------------------
    def _stats(self, closes: list[dict]) -> dict:
        n = len(closes)
        if n == 0:
            return {"n": 0, "pnl": 0.0, "winrate": 0.0, "profit_factor": 0.0,
                    "expectancy": 0.0, "max_drawdown": 0.0}
        pnls = [c["pnl"] for c in closes]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))
        pf = gross_win / gross_loss if gross_loss > 1e-9 else (2.0 if gross_win > 0 else 0.0)
        # max drawdown over the cumulative pnl curve
        peak = 0.0
        cum = 0.0
        max_dd = 0.0
        for p in pnls:
            cum += p
            peak = max(peak, cum)
            max_dd = max(max_dd, peak - cum)
        return {"n": n, "pnl": round(sum(pnls), 4), "winrate": len(wins) / n,
                "profit_factor": round(pf, 3), "expectancy": round(sum(pnls) / n, 4),
                "max_drawdown": round(max_dd, 4)}

    def overall(self) -> dict:
        return self._stats(self.closes)

    def rolling(self, n: int = 20) -> dict:
        return self._stats(self.closes[-n:])

    def by_field(self, field: str) -> dict[str, dict]:
        groups: dict[str, list[dict]] = defaultdict(list)
        for c in self.closes:
            groups[str(c.get(field) or "?")].append(c)
        return {k: self._stats(v) for k, v in groups.items()}

    def daily_series(self) -> list[dict]:
        days: dict[str, float] = defaultdict(float)
        for c in self.closes:
            from datetime import datetime, timezone
            day = datetime.fromtimestamp(c["ts_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            days[day] += c["pnl"]
        return [{"day": d, "pnl": round(p, 4)} for d, p in sorted(days.items())]
