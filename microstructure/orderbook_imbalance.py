"""Signed orderbook depth imbalance in [-1, 1] (USD-weighted top levels)."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot


def imbalance(book: Optional[OrderbookSnapshot], levels: int = 3) -> float:
    if book is None:
        return 0.0
    bid = book.depth_usd_at_bid(levels)
    ask = book.depth_usd_at_ask(levels)
    total = bid + ask
    if total <= 0:
        return 0.0
    return (bid - ask) / total
