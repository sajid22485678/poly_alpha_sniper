"""Distance-to-strike fair probability (gaussian, deterministic, no scipy).

P(price at expiry > threshold) under a driftless diffusion with per-second
volatility, plus a small momentum drift term shrunk by a mean-reversion
penalty when the move is already statistically extreme.
"""
from __future__ import annotations

import math

from poly_alpha_sniper.core.contracts import clamp

CLAMP_MIN = 0.02
CLAMP_MAX = 0.98


def _phi(x: float) -> float:
    """Standard normal CDF via erf."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def prob_above_threshold(price: float, threshold: float, tte_s: float,
                         vol_per_s: float, momentum: float = 0.0,
                         zscore: float = 0.0,
                         mean_reversion_penalty: float = 0.12) -> tuple[float, float, float]:
    """Returns (p_above, p_below, confidence_0_100)."""
    if price <= 0 or threshold <= 0:
        return 0.5, 0.5, 0.0
    tte = max(tte_s, 1.0)
    vol = max(vol_per_s, 1e-6)

    # standardized distance in units of expected move over remaining time
    sigma_total = price * vol * math.sqrt(tte)
    d = (threshold - price) / sigma_total

    # momentum drift: momentum in [-1,1] contributes a fraction of one sigma,
    # shrunk when zscore is extreme (mean reversion)
    drift = 0.35 * momentum
    if abs(zscore) > 3.0:
        drift *= max(0.0, 1.0 - mean_reversion_penalty * (abs(zscore) - 3.0))

    p_above = 1.0 - _phi(d - drift)
    p_above = clamp(p_above, CLAMP_MIN, CLAMP_MAX)
    p_below = 1.0 - p_above

    # confidence: decent vol estimate + tte in the tradable sweet spot
    conf = 70.0
    if vol_per_s <= 1e-6:
        conf -= 30.0
    if 45 <= tte_s <= 400:
        conf += 15.0
    elif tte_s > 900:
        conf -= 20.0
    if abs(d) > 4:
        conf += 10.0  # far from strike: outcome nearly decided, model very sure
    return p_above, p_below, clamp(conf, 0.0, 100.0)
