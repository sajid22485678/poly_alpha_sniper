"""Detect sudden top-of-book liquidity disappearance (toxic to enter into)."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot


def vacuum(book_now: Optional[OrderbookSnapshot],
           book_prev: Optional[OrderbookSnapshot]) -> tuple[bool, str]:
    if book_now is None or book_prev is None:
        return False, "insufficient history"
    prev_depth = book_prev.depth_usd_at_bid(3) + book_prev.depth_usd_at_ask(3)
    now_depth = book_now.depth_usd_at_bid(3) + book_now.depth_usd_at_ask(3)
    if prev_depth <= 0:
        return False, "no prior depth"
    drop = 1.0 - now_depth / prev_depth
    if drop >= 0.6:
        return True, f"top-3 depth dropped {drop:.0%} (${prev_depth:.0f} -> ${now_depth:.0f})"
    return False, f"depth stable ({drop:+.0%})"
