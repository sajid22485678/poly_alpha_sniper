"""Market regime classification (deterministic cutoffs)."""
from __future__ import annotations

from poly_alpha_sniper.strategy.volatility_model import HOT_VOL, QUIET_VOL


def market_regime(vol_per_s: float, momentum: float, spread: float | None = None) -> str:
    if vol_per_s < QUIET_VOL and abs(momentum) < 0.2:
        return "quiet"
    if abs(momentum) >= 0.4:
        return "trending"
    if vol_per_s > HOT_VOL and abs(momentum) < 0.3:
        return "choppy"
    return "trending" if abs(momentum) >= 0.25 else "choppy"


def favorable_for_entry(regime: str) -> bool:
    return regime in ("trending", "quiet")
