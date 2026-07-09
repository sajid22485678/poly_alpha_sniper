"""Tier-aware market exposure cap -- fixed_min_shares sizing mode only.

Shared by risk/position_sizer.py and execution/order_validator.py so the two
independent exposure checks never disagree (they previously did for the
analogous max_trade_usd-relative min-order checks -- see
fix_max_trade_usd_and_dashboard_export_refresh). max_trade_usd mode always
uses the flat cfg.risk.max_market_exposure_pct_equity; total exposure cap,
daily loss cap, loss streak cap, kill switch, and panic are untouched.
"""
from __future__ import annotations


def resolve_tier_exposure_cap_pct(cfg, tier) -> float:
    dyn = cfg.risk.dynamic_exposure_by_tier
    if not dyn.enabled:
        return cfg.risk.max_market_exposure_pct_equity
    tier_key = tier.value if hasattr(tier, "value") else str(tier or "")
    return dyn.tiers.get(tier_key, dyn.default_pct)
