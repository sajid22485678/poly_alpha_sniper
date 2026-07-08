"""Alpha score 0-100.

score = 50 * edge_term + 30 * confidence_term + 20 * quality_term, plus a
time-to-expiry sweet-spot bonus (max +5, clamped to 100). Edge term saturates
at 15% post-slippage edge.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import clamp


def alpha_score(edge_after_slippage: float, confidence: float,
                market_quality: float, tte_s: float, cfg) -> float:
    edge_term = clamp(edge_after_slippage / 0.15, 0.0, 1.0)
    conf_term = clamp(confidence / 100.0, 0.0, 1.0)
    qual_term = clamp(market_quality / 100.0, 0.0, 1.0)
    score = 50.0 * edge_term + 30.0 * conf_term + 20.0 * qual_term
    preferred = cfg.ultra_short_expiry.preferred_time_to_expiry_seconds
    if preferred and min(abs(tte_s - p) for p in preferred) <= 30:
        score += 5.0
    return round(clamp(score, 0.0, 100.0), 1)
