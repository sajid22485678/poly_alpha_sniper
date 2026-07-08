"""Classify whether a mapped market is a tradable 5-minute crypto candidate."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo

# discovery envelope is wider than the entry window so we can subscribe books
# before markets enter the tradable window.
DISCOVERY_MAX_TTE_S = 20 * 60


def is_crypto_5min_candidate(market: MarketInfo, now_ms: int, cfg) -> tuple[bool, str]:
    if not market.asset:
        return False, "no_asset"
    if market.asset not in cfg.assets:
        return False, f"asset_disabled:{market.asset}"
    if market.closed:
        return False, "closed"
    if not market.active:
        return False, "inactive"
    if market.parse_reject_reason:
        return False, market.parse_reject_reason
    if not market.yes_token_id or not market.no_token_id:
        return False, "missing_token_ids"
    tte = market.seconds_to_expiry(now_ms)
    if tte <= 0:
        return False, "expired"
    if tte > DISCOVERY_MAX_TTE_S:
        return False, "expiry_too_far"
    if market.mapping_confidence < 95:
        return False, "mapping_confidence_low"
    return True, "ok"
