"""Text-quality rule set built on the rule engine."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import ParsedMarket
from poly_alpha_sniper.deterministic_intelligence.rule_engine import Rule, evaluate

TEXT_RULES = [
    Rule("parsed_ok", lambda p: p.ok, fatal=True),
    Rule("single_asset", lambda p: bool(p.asset), fatal=True),
    Rule("known_market_type", lambda p: p.market_type.value != "UNKNOWN", fatal=True),
    Rule("threshold_present_if_threshold_market",
         lambda p: p.market_type.value != "THRESHOLD" or p.threshold is not None, fatal=True),
    Rule("confidence_high", lambda p: p.confidence >= 95, fatal=False),
    Rule("expiry_hint_present", lambda p: bool(p.expiry_hint), fatal=False),
]


def evaluate_text_rules(parsed: ParsedMarket) -> tuple[list[str], list[str], bool]:
    return evaluate(TEXT_RULES, parsed)
