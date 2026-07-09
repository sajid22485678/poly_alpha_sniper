"""Oracle resolution anchor: resolve + validate price_to_beat for a market.

Polymarket's 5-minute crypto Up/Down markets resolve against a Chainlink
price stream sampled at window open ("price to beat"), NOT against CEX spot
price (confirmed via each live market's own description field: "this market
is about the price according to Chainlink data stream BTC/USD, not according
to other sources or spot markets"). The bot's signal generation is CEX-only
(see strategy/probability_model.py) -- this module is the fail-closed gate
that stops an entry when the resolution anchor is missing, stale, or
mismatched, rather than trading blind against a price the market doesn't
actually resolve on.

No live Chainlink subscription exists in this bot: `oracle_open_price` comes
from Polymarket's own event metadata (a relay of a Chainlink snapshot taken
at window open, see discovery/market_mapper.py::_extract_price_to_beat),
and `latest_oracle_price` is always unavailable. Both facts are represented
honestly rather than glossed over.
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import MarketInfo, OracleAnchor, RejectReason


def resolve_oracle_anchor(market: MarketInfo, cex_price: Optional[float],
                          cex_ts_ms: Optional[int], now_ms: int) -> OracleAnchor:
    """Pure: build an OracleAnchor snapshot for `market` from data already on
    the MarketInfo (populated by discovery) plus a caller-supplied live CEX
    price. Never fetches anything -- callers own data freshness."""
    window_start = int(market.raw.get("event_start_ts_ms") or 0) or None
    window_end = market.expiry_ts_ms
    time_remaining = max(0.0, (window_end - now_ms) / 1000.0)

    oracle_open = market.price_to_beat
    oracle_source = market.price_to_beat_source or ("" if oracle_open is None else "unknown")

    basis = None
    if oracle_open is not None and oracle_open > 0 and cex_price is not None:
        basis = (cex_price - oracle_open) / oracle_open

    quality = _quality_tier(oracle_open, oracle_source, window_start, window_end, now_ms)

    return OracleAnchor(
        market_id=market.market_id,
        asset=market.asset,
        window_start_ts_ms=window_start or 0,
        window_end_ts_ms=window_end,
        oracle_source=oracle_source,
        oracle_open_price=oracle_open,
        oracle_open_ts_ms=window_start if oracle_open is not None else None,
        latest_oracle_price=None,  # honest: no live oracle feed exists
        cex_price=cex_price,
        cex_ts_ms=cex_ts_ms,
        oracle_vs_cex_basis=basis,
        time_remaining_seconds=time_remaining,
        oracle_anchor_quality=quality,
        resolution_source_url=market.resolution_source_url,
    )


def _quality_tier(oracle_open: Optional[float], oracle_source: str,
                  window_start: Optional[int], window_end: int, now_ms: int) -> str:
    if oracle_open is None:
        return "missing"
    if now_ms >= window_end:
        return "stale"
    if oracle_source == "polymarket_event_metadata":
        return "good"
    return "fallback"


def validate_oracle_anchor(anchor: OracleAnchor, market: MarketInfo, now_ms: int,
                           max_anchor_age_ms: float,
                           max_basis_abs_pct: float) -> tuple[bool, str]:
    """Fail-closed checks (WS5B "price-to-beat validator"). Returns
    (ok, reject_reason) -- reject_reason is one of the REJECTED_* constants
    on RejectReason, or "" when ok. Order matters: the first failing check
    wins, most-fundamental first."""
    if anchor.oracle_open_price is None or anchor.oracle_open_price <= 0:
        return False, RejectReason.MISSING_ORACLE_ANCHOR
    if not anchor.oracle_source or anchor.oracle_source == "unknown":
        return False, RejectReason.ORACLE_SOURCE_UNKNOWN
    if anchor.market_id != market.market_id or anchor.asset != market.asset:
        return False, RejectReason.PRICE_TO_BEAT_MISMATCH
    if anchor.asset not in ("BTC", "ETH", "SOL"):
        return False, RejectReason.ORACLE_SOURCE_UNKNOWN
    if now_ms >= anchor.window_end_ts_ms:
        return False, RejectReason.ORACLE_ANCHOR_STALE
    if anchor.oracle_open_ts_ms is not None:
        age_ms = now_ms - anchor.oracle_open_ts_ms
        if age_ms < 0 or age_ms > max_anchor_age_ms:
            return False, RejectReason.ORACLE_ANCHOR_STALE
    if anchor.oracle_vs_cex_basis is not None and abs(anchor.oracle_vs_cex_basis) > max_basis_abs_pct:
        return False, RejectReason.ORACLE_CEX_BASIS_UNSTABLE
    return True, ""
