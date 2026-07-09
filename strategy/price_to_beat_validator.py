"""WS5B: price-to-beat validator -- runs before edge calculation.

This is a thin, explicitly-named wrapper around
strategy.oracle_anchor.validate_oracle_anchor (the same function
core.app.App._evaluate_market calls as its fail-closed pre-signal gate) so
the validator exists as its own identifiable module per spec, without
duplicating the check logic in two places.

Checks (see oracle_anchor.validate_oracle_anchor for the exact order):
- price_to_beat exists and is numeric/positive -> REJECTED_MISSING_ORACLE_ANCHOR
- oracle_source is known -> REJECTED_ORACLE_SOURCE_UNKNOWN
- anchor's market_id/asset matches the market being evaluated (no metadata
  mismatch) -> REJECTED_PRICE_TO_BEAT_MISMATCH
- asset is one of BTC/ETH/SOL -> REJECTED_ORACLE_SOURCE_UNKNOWN
- still within the market's own window, anchor not stale -> REJECTED_ORACLE_ANCHOR_STALE
- CEX-vs-anchor basis is stable -> REJECTED_ORACLE_CEX_BASIS_UNSTABLE
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo, OracleAnchor
from poly_alpha_sniper.strategy.oracle_anchor import validate_oracle_anchor


def validate_price_to_beat(anchor: OracleAnchor, market: MarketInfo, now_ms: int,
                           max_anchor_age_ms: float, max_basis_abs_pct: float) -> tuple[bool, str]:
    """Pure. Returns (ok, reject_reason) -- reject_reason is "" when ok."""
    return validate_oracle_anchor(anchor, market, now_ms, max_anchor_age_ms, max_basis_abs_pct)
