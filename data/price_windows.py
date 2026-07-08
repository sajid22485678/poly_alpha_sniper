"""Rolling price window: returns, volatility, z-score over recent ticks.

Deterministic, O(1) amortized appends, prunes samples older than max_age_s.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Optional


class PriceWindow:
    def __init__(self, max_age_s: float = 120.0):
        self.max_age_ms = int(max_age_s * 1000)
        self._samples: deque[tuple[int, float]] = deque()

    def add(self, price: float, ts_ms: int) -> None:
        if price <= 0:
            return
        if self._samples and ts_ms < self._samples[-1][0]:
            return  # never move backwards
        self._samples.append((ts_ms, price))
        cutoff = ts_ms - self.max_age_ms
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def last_price(self) -> Optional[float]:
        return self._samples[-1][1] if self._samples else None

    def last_ts_ms(self) -> Optional[int]:
        return self._samples[-1][0] if self._samples else None

    def price_at(self, ts_ms: int) -> Optional[float]:
        """Most recent price at or before ts_ms."""
        best = None
        for t, p in reversed(self._samples):
            if t <= ts_ms:
                best = p
                break
        return best

    def return_over(self, seconds: float, now_ms: int) -> Optional[float]:
        if not self._samples:
            return None
        past = self.price_at(now_ms - int(seconds * 1000))
        cur = self.last_price()
        if past is None or cur is None or past <= 0:
            return None
        return (cur - past) / past

    def volatility_per_s(self, now_ms: int, lookback_s: float = 60.0) -> float:
        """Stdev of 1-second log returns over the lookback (per-second vol)."""
        cutoff = now_ms - int(lookback_s * 1000)
        pts = [(t, p) for t, p in self._samples if t >= cutoff]
        if len(pts) < 3:
            return 0.0
        # resample to ~1s grid
        rets: list[float] = []
        last_t, last_p = pts[0]
        for t, p in pts[1:]:
            if t - last_t >= 900:
                if last_p > 0 and p > 0:
                    dt_s = (t - last_t) / 1000.0
                    rets.append(math.log(p / last_p) / math.sqrt(dt_s))
                last_t, last_p = t, p
        if len(rets) < 2:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(max(var, 0.0))

    def zscore(self, window_s: float, now_ms: int) -> float:
        ret = self.return_over(window_s, now_ms)
        vol = self.volatility_per_s(now_ms)
        if ret is None or vol <= 1e-9:
            return 0.0
        expected_sd = vol * math.sqrt(max(window_s, 1e-3))
        return ret / expected_sd

    def n_samples(self) -> int:
        return len(self._samples)
