"""News-spike defensive mode: extreme volatility -> stricter entries."""
from __future__ import annotations


def detect_news_spike(vol_per_s: float, typical_vol_per_s: float) -> bool:
    if typical_vol_per_s <= 1e-9:
        return False
    return vol_per_s > 4.0 * typical_vol_per_s


def adjustments(active: bool) -> dict:
    if not active:
        return {"min_edge_bump": 0.0, "max_aggression": "AGGRESSIVE"}
    return {"min_edge_bump": 0.02, "max_aggression": "NORMAL",
            "note": "news spike: repricing may be instant, lag edge unreliable"}
