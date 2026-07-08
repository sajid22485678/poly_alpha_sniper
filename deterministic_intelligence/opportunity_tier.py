"""Deterministic opportunity tier classification.

Tier is a pure function of (edge after slippage, confidence, market quality)
against configured thresholds — identical in every mode.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import EdgeResult, Tier, TradingMode


def min_edge_for_mode(cfg, mode: TradingMode) -> float:
    if mode == TradingMode.LIVE_FULL:
        return max(cfg.dynamic_edge.live_full_min_edge_floor, cfg.dynamic_edge.hard_min_edge)
    if mode == TradingMode.LIVE_MICRO:
        return max(cfg.dynamic_edge.live_micro_min_edge_floor, cfg.dynamic_edge.hard_min_edge)
    return max(cfg.strategy.min_edge_shadow, cfg.dynamic_edge.hard_min_edge)


def classify_tier(edge: EdgeResult, confidence: float, market_quality: float,
                  cfg, mode: TradingMode) -> Tier:
    e = edge.edge_after_slippage
    s = cfg.strategy
    q = cfg.market_quality

    if (e >= s.a_plus_edge_live_micro and confidence >= s.confidence_preferred_live
            and market_quality >= q.min_score_live_full):
        return Tier.A_PLUS
    if (e >= s.preferred_edge_live_micro and confidence >= s.confidence_min_live_hard_floor
            and market_quality >= q.min_score_live_micro):
        return Tier.A
    if (e >= min_edge_for_mode(cfg, mode) and confidence >= s.confidence_min_live_hard_floor * 0.8
            and market_quality >= q.min_score_shadow):
        return Tier.B
    return Tier.C
