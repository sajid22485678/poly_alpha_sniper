"""Momentum scoring from short-window returns.

Signed score in [-1, 1]; a blended 0.15% move over the short windows is
treated as full momentum for 5-minute crypto (documented normalization).
"""
from __future__ import annotations

import math

FULL_MOVE = 0.0015  # 0.15% blended move -> |momentum| ~ tanh(1) ~ 0.76


def momentum_score(returns: dict[int, float]) -> float:
    blend = (0.5 * returns.get(2, 0.0)
             + 0.3 * returns.get(5, 0.0)
             + 0.2 * returns.get(10, 0.0))
    return math.tanh(blend / FULL_MOVE)


def momentum_fading(returns: dict[int, float]) -> bool:
    """Momentum considered fading when the 2s leg reverses against the 10s leg."""
    r2, r10 = returns.get(2, 0.0), returns.get(10, 0.0)
    if abs(r10) < 1e-6:
        return abs(r2) < 1e-6
    return (r2 * r10 < 0) or abs(r2) < abs(r10) * 0.1
