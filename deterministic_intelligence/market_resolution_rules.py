"""Resolution-clarity rules.

The 5-minute up/down and above/below series resolve from a published price
source at a fixed timestamp — clear. Bespoke essay-style markets are unclear
and rejected for this strategy.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo, MarketType
from poly_alpha_sniper.deterministic_intelligence.rule_engine import Rule, evaluate

RESOLUTION_RULES = [
    Rule("mechanical_market_type",
         lambda m: m.market_type in (MarketType.UP_DOWN, MarketType.THRESHOLD), fatal=True),
    Rule("has_expiry", lambda m: m.expiry_ts_ms > 0, fatal=True),
    Rule("mapping_confident", lambda m: m.mapping_confidence >= 95, fatal=True),
    Rule("not_neg_risk_complex", lambda m: not m.neg_risk, fatal=False),
]


def resolution_clear(market: MarketInfo) -> tuple[bool, list[str]]:
    _passed, failed, fatal = evaluate(RESOLUTION_RULES, market)
    return not fatal, failed
