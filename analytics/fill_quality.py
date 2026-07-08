"""Fill quality scoring 0-100 + rolling tracking with blacklist hints."""
from __future__ import annotations

from collections import defaultdict, deque

from poly_alpha_sniper.core.contracts import OrderRecord, OrderRequest, OrderSide, clamp


def score_fill(expected_price: float, actual_avg_price: float, side: OrderSide,
               submit_ts_ms: int, fill_ts_ms: int, requested_shares: float,
               filled_shares: float) -> dict:
    if expected_price <= 0 or requested_shares <= 0:
        return {"score": 0.0, "slippage_bps": 0.0, "delay_ms": 0.0, "partial_ratio": 0.0}
    # adverse slippage is positive (paid more on buys / received less on sells)
    if side.is_buy:
        slip = (actual_avg_price - expected_price) / expected_price * 10_000
    else:
        slip = (expected_price - actual_avg_price) / expected_price * 10_000
    delay_ms = max(0, fill_ts_ms - submit_ts_ms)
    partial_ratio = clamp(filled_shares / requested_shares, 0.0, 1.0)

    score = 100.0
    score -= clamp(slip, 0.0, 200.0) * 0.25          # -25 at 100bps adverse
    score -= clamp(delay_ms / 100.0, 0.0, 30.0)      # -10 at 1s, cap -30
    score -= (1.0 - partial_ratio) * 40.0            # -40 for total miss
    return {"score": round(clamp(score, 0.0, 100.0), 1),
            "slippage_bps": round(slip, 1), "delay_ms": float(delay_ms),
            "partial_ratio": round(partial_ratio, 3)}


class FillQualityTracker:
    BAD_SCORE = 40.0

    def __init__(self, window: int = 50):
        self._global: deque[float] = deque(maxlen=window)
        self._by_market: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=20))
        self._consecutive_bad: dict[str, int] = defaultdict(int)
        self.failed_fills = 0
        self.cancelled_orders = 0

    def score_and_track(self, req: OrderRequest, record: OrderRecord) -> dict:
        result = score_fill(req.price, record.avg_fill_price or req.price, req.side,
                            record.created_ts_ms, record.updated_ts_ms,
                            req.size_shares, record.filled_shares)
        self.observe(req.market_id, result["score"])
        if record.filled_shares <= 0:
            self.failed_fills += 1
        result["market_id"] = req.market_id
        return result

    def observe(self, market_id: str, score: float) -> None:
        self._global.append(score)
        self._by_market[market_id].append(score)
        if score < self.BAD_SCORE:
            self._consecutive_bad[market_id] += 1
        else:
            self._consecutive_bad[market_id] = 0

    def record_cancel(self) -> None:
        self.cancelled_orders += 1

    def global_avg(self) -> float:
        return sum(self._global) / len(self._global) if self._global else 100.0

    def market_avg(self, market_id: str) -> float:
        buf = self._by_market.get(market_id)
        return sum(buf) / len(buf) if buf else 100.0

    def blacklist_recommended(self, market_id: str) -> bool:
        """>=2 consecutive bad fills in a market -> recommend temp blacklist."""
        return self._consecutive_bad.get(market_id, 0) >= 2

    def is_good(self) -> bool:
        return self.global_avg() >= 70.0
