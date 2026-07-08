"""Edge engine — identical math in every mode.

BUY edge  = fair_probability - executable ask
SELL edge = executable bid  - fair_probability
Then: after_spread (half-spread cost), after_slippage (book walk for the
requested size), confidence-adjusted, and a capped suggested size.
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import EdgeResult, OrderbookSnapshot, OrderSide, clamp


def walk_book_avg_price(levels, size_usd: float) -> tuple[Optional[float], float]:
    """Average fill price walking price levels for size_usd. Returns
    (avg_price, filled_usd); avg_price None when book empty."""
    if not levels or size_usd <= 0:
        return None, 0.0
    remaining = size_usd
    cost = 0.0
    shares = 0.0
    for lvl in levels:
        lvl_usd = lvl.price * lvl.size
        take_usd = min(remaining, lvl_usd)
        if take_usd <= 0:
            break
        shares += take_usd / lvl.price
        cost += take_usd
        remaining -= take_usd
        if remaining <= 1e-9:
            break
    if shares <= 0:
        return None, 0.0
    return cost / shares, size_usd - remaining


def compute_edge(side: OrderSide, fair_p: float, book: OrderbookSnapshot, cfg,
                 size_usd: float, confidence: float,
                 slippage_bps: Optional[float] = None) -> Optional[EdgeResult]:
    if book is None:
        return None
    if side.is_buy:
        px = book.best_ask
        if px is None:
            return None
        raw_edge = fair_p - px
        walk_levels = book.asks
    else:
        px = book.best_bid
        if px is None:
            return None
        raw_edge = px - fair_p
        walk_levels = book.bids

    spread = book.spread if book.spread is not None else cfg.microstructure.max_spread
    edge_after_spread = raw_edge - spread / 2.0

    if slippage_bps is None:
        avg_px, filled = walk_book_avg_price(walk_levels, size_usd)
        if avg_px is None:
            return None
        slip = abs(avg_px - px) / max(px, 1e-9) * 10_000
        if filled < size_usd * 0.999:
            slip += cfg.microstructure.max_slippage_bps  # depth short: punitive
        slippage_bps = slip
    edge_after_slippage = edge_after_spread - (slippage_bps / 10_000.0)

    confidence_adjusted = edge_after_slippage * clamp(confidence / 100.0, 0.0, 1.0)

    # kelly-lite suggested size: proportional to edge, capped by config
    if edge_after_slippage > 0 and px > 0:
        kelly_fraction = clamp(edge_after_slippage / px, 0.0, 0.25)
        suggested = clamp(kelly_fraction * cfg.risk.starting_bankroll_usd,
                          0.0, cfg.risk.max_trade_usd)
    else:
        suggested = 0.0

    return EdgeResult(side=side, fair_probability=fair_p, market_price=px,
                      raw_edge=raw_edge, edge_after_spread=edge_after_spread,
                      edge_after_slippage=edge_after_slippage,
                      confidence_adjusted_edge=confidence_adjusted,
                      suggested_size_usd=suggested)
