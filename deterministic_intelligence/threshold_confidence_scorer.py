"""Combined 0-100 confidence that we understand a market's mechanics."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo, ParsedMarket, TokenMappingResult
from poly_alpha_sniper.deterministic_intelligence.market_resolution_rules import resolution_clear


def combined_confidence(parsed: ParsedMarket, mapping: TokenMappingResult,
                        market: MarketInfo) -> float:
    if not parsed.ok or not mapping.ok:
        return 0.0
    clear, failed = resolution_clear(market)
    if not clear:
        return 0.0
    score = min(parsed.confidence, mapping.confidence)
    score -= 5.0 * len(failed)  # non-fatal resolution weaknesses
    return max(0.0, min(100.0, score))
