"""Realized volatility helpers (per-second units)."""
from __future__ import annotations

import math

# deterministic per-second vol cutoffs for crypto majors
QUIET_VOL = 0.00005
HOT_VOL = 0.00035


def vol_per_s(samples: list[tuple[int, float]]) -> float:
    """Stdev of ~1s log returns from (ts_ms, price) samples."""
    if len(samples) < 3:
        return 0.0
    rets: list[float] = []
    last_t, last_p = samples[0]
    for t, p in samples[1:]:
        if t - last_t >= 900 and last_p > 0 and p > 0:
            dt_s = (t - last_t) / 1000.0
            rets.append(math.log(p / last_p) / math.sqrt(dt_s))
            last_t, last_p = t, p
    if len(rets) < 2:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(max(var, 0.0))


def ewma_vol(prev_vol: float, latest_ret_1s: float, alpha: float = 0.06) -> float:
    return math.sqrt(max((1 - alpha) * prev_vol ** 2 + alpha * latest_ret_1s ** 2, 0.0))


def vol_regime(vol: float) -> str:
    if vol < QUIET_VOL:
        return "quiet"
    if vol > HOT_VOL:
        return "hot"
    return "normal"
