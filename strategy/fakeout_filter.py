"""Fakeout risk: is this move likely to reverse before it can be monetized?"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import clamp


def fakeout_risk(returns: dict[int, float], zscore: float, volatility: float) -> float:
    """0 = clean move, 1 = almost certainly a fakeout."""
    risk = 0.0
    r2 = returns.get(2, 0.0)
    r30 = returns.get(30, 0.0)

    # overextension: big 30s move with the 2s leg reversing against it
    if abs(r30) > 0.004 and r2 * r30 < 0:
        risk += 0.5
    # blowoff z-score
    if abs(zscore) > 4.5:
        risk += 0.3
    elif abs(zscore) > 3.5:
        risk += 0.15
    # retrace share: how much of the 30s move has already come back
    if abs(r30) > 1e-6 and r2 * r30 < 0:
        retrace = min(1.0, abs(r2) / abs(r30))
        risk += 0.3 * retrace
    # dead-vol environments produce noise moves
    if volatility < 1e-6 and abs(r2) > 0.001:
        risk += 0.2
    return clamp(risk, 0.0, 1.0)
