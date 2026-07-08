"""Estimate the CEX -> Polymarket repricing delay by shifted correlation."""
from __future__ import annotations

import numpy as np


def estimate_lead_lag_ms(cex_series: list[tuple[int, float]],
                         poly_series: list[tuple[int, float]],
                         max_lag_ms: int = 5000, step_ms: int = 250) -> int:
    """Best lag (ms Polymarket trails CEX) by max |correlation| of resampled
    diff series. Returns 0 when data is insufficient."""
    if len(cex_series) < 10 or len(poly_series) < 10:
        return 0
    t0 = max(cex_series[0][0], poly_series[0][0])
    t1 = min(cex_series[-1][0], poly_series[-1][0])
    if t1 - t0 < 5 * step_ms:
        return 0
    grid = np.arange(t0, t1, step_ms, dtype=np.int64)

    def resample(series):
        ts = np.array([t for t, _ in series], dtype=np.int64)
        vs = np.array([v for _, v in series], dtype=np.float64)
        idx = np.searchsorted(ts, grid, side="right") - 1
        idx = np.clip(idx, 0, len(vs) - 1)
        return vs[idx]

    cex = np.diff(resample(cex_series))
    poly = np.diff(resample(poly_series))
    n = min(len(cex), len(poly))
    if n < 8:
        return 0
    cex, poly = cex[:n], poly[:n]
    best_lag, best_corr = 0, 0.0
    for shift in range(0, max_lag_ms // step_ms + 1):
        if shift >= n - 2:
            break
        a = cex[: n - shift]
        b = poly[shift:]
        sa, sb = a.std(), b.std()
        if sa < 1e-12 or sb < 1e-12:
            continue
        corr = float(np.corrcoef(a, b)[0, 1])
        if abs(corr) > abs(best_corr):
            best_corr, best_lag = corr, shift * step_ms
    return best_lag


class RollingLeadLag:
    def __init__(self, clock, window_ms: int = 120_000):
        self.clock = clock
        self.window_ms = window_ms
        self.cex: list[tuple[int, float]] = []
        self.poly: list[tuple[int, float]] = []
        self.last_estimate_ms = 0

    def add_cex(self, ts_ms: int, price: float) -> None:
        self.cex.append((ts_ms, price))
        self._prune()

    def add_poly(self, ts_ms: int, prob: float) -> None:
        self.poly.append((ts_ms, prob))
        self._prune()

    def _prune(self) -> None:
        cutoff = self.clock.now_ms() - self.window_ms
        self.cex = [(t, v) for t, v in self.cex if t >= cutoff]
        self.poly = [(t, v) for t, v in self.poly if t >= cutoff]

    def estimate(self) -> int:
        self.last_estimate_ms = estimate_lead_lag_ms(self.cex, self.poly)
        return self.last_estimate_ms
