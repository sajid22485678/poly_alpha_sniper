"""Entry blocks tied to resolution/expiry risk."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo, RejectReason


def entry_blocked_by_resolution_risk(market: MarketInfo, now_ms: int, cfg) -> tuple[bool, str]:
    u = cfg.ultra_short_expiry
    tte = market.seconds_to_expiry(now_ms)
    if tte < u.reject_if_less_than_seconds:
        return True, f"{RejectReason.EXPIRY_WINDOW}: {tte:.0f}s to expiry"
    if u.require_clean_threshold_mapping and market.mapping_confidence < 95:
        return True, RejectReason.TOKEN_MAPPING_UNCLEAR
    if u.reject_ambiguous_markets and market.parse_reject_reason:
        return True, RejectReason.AMBIGUOUS_MARKET
    return False, ""
