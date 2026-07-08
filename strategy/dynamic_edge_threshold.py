"""Dynamic minimum edge by mode, regime and recent fill quality.

Only ever tightens relative to the configured floors — never loosens below
cfg.dynamic_edge.hard_min_edge.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import TradingMode
from poly_alpha_sniper.deterministic_intelligence.opportunity_tier import min_edge_for_mode


def current_min_edge(cfg, mode: TradingMode, regime: str = "normal",
                     fill_quality: float = 100.0) -> float:
    base = min_edge_for_mode(cfg, mode)
    if not cfg.dynamic_edge.enabled:
        return base
    bump = 0.0
    if regime == "choppy":
        bump += 0.01
    if fill_quality < 60:
        bump += 0.01
    if cfg.micro_bankroll_mode.enabled and cfg.micro_bankroll_mode.require_higher_edge_live \
            and mode.is_live:
        bump += 0.01
    return max(cfg.dynamic_edge.hard_min_edge, base + bump)
