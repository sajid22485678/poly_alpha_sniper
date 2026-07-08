"""Depth gate: enough size on the side we need to hit."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot, OrderSide, RejectReason


def depth_ok(book: Optional[OrderbookSnapshot], side: OrderSide, cfg,
             size_usd: float) -> tuple[bool, float, str]:
    if book is None:
        return False, 0.0, RejectReason.NO_BEST_BID_ASK
    depth = book.depth_usd_at_ask(3) if side.is_buy else book.depth_usd_at_bid(3)
    required = max(cfg.microstructure.min_depth_at_ask_usd * (1.0 if side.is_buy else 0.25),
                   2.0 * size_usd)
    if depth < required:
        return False, depth, RejectReason.LIQUIDITY_TOO_THIN
    return True, depth, ""
