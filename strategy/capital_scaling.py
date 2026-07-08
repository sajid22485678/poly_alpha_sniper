"""Capital scaling ladder.

While auto_increase_size is false (default) the ACTIVE cap stays pinned at
cfg.risk.max_trade_usd; the ladder only produces a RECOMMENDED cap plus
whether scaling requirements (live trade count, rolling PF, fill quality)
are met. Flipping auto_increase_size to true is a manual config decision.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ScalingDecision:
    active_cap_usd: float
    recommended_cap_usd: float
    scale_allowed: bool
    reason: str


def _tier_cap(cfg, equity: float) -> float:
    for tier in cfg.capital_scaling.tiers:
        if tier.equity_min <= equity < tier.equity_max:
            if tier.max_trade_usd is not None:
                return tier.max_trade_usd
            if tier.max_trade_pct_equity is not None:
                return equity * tier.max_trade_pct_equity
    return cfg.risk.max_trade_usd


def current_max_trade_usd(cfg, equity: float, live_trade_count: int,
                          rolling_pf: float, fill_quality_ok: bool) -> ScalingDecision:
    cs = cfg.capital_scaling
    base = cfg.risk.max_trade_usd
    if not cs.enabled:
        return ScalingDecision(base, base, False, "capital scaling disabled")

    recommended = _tier_cap(cfg, equity)
    requirements = []
    if live_trade_count < cs.require_min_live_trades_before_scale:
        requirements.append(f"live trades {live_trade_count} < {cs.require_min_live_trades_before_scale}")
    if rolling_pf < cs.require_rolling_pf_above:
        requirements.append(f"rolling PF {rolling_pf:.2f} < {cs.require_rolling_pf_above}")
    if cs.require_good_fill_quality and not fill_quality_ok:
        requirements.append("fill quality not good")
    scale_allowed = not requirements

    if not cs.auto_increase_size:
        return ScalingDecision(base, recommended, scale_allowed,
                               "auto_increase_size=false: active cap pinned to config"
                               + ("" if scale_allowed else f"; unmet: {'; '.join(requirements)}"))
    if not scale_allowed:
        return ScalingDecision(base, recommended, False, "; ".join(requirements))
    return ScalingDecision(min(recommended, max(base, recommended)), recommended, True,
                           "scaling requirements met")
