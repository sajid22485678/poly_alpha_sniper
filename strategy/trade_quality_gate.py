"""Trade quality: how good is THIS execution opportunity (vs market quality
which rates the venue)."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import EdgeResult, MarketQualityResult, TradingMode, clamp


def trade_quality(edge: EdgeResult, quality: MarketQualityResult,
                  timing_ms_since_shock: float, cfg) -> tuple[float, dict]:
    score = 0.0
    # edge contribution (0-50)
    score += clamp(edge.edge_after_slippage / 0.12, 0.0, 1.0) * 50.0
    # market quality contribution (0-30)
    score += quality.score / 100.0 * 30.0
    # timing contribution (0-20): full marks under 300ms, zero at max delay
    max_delay = cfg.strategy.max_entry_delay_ms
    if timing_ms_since_shock <= 300:
        score += 20.0
    elif timing_ms_since_shock <= max_delay:
        score += 20.0 * (1.0 - (timing_ms_since_shock - 300) / max(max_delay - 300, 1))
    if timing_ms_since_shock > max_delay:
        score = min(score, 40.0)  # late entries are capped

    q = cfg.market_quality
    ok_by_mode = {
        TradingMode.SHADOW_LIVE: quality.score >= q.min_score_shadow,
        TradingMode.SIMULATION: quality.score >= q.min_score_shadow,
        TradingMode.LIVE_MICRO: quality.score >= q.min_score_live_micro,
        TradingMode.LIVE_FULL: quality.score >= q.min_score_live_full,
    }
    return round(clamp(score, 0.0, 100.0), 1), ok_by_mode
