"""Spread gate."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot, RejectReason


def spread_ok(book: Optional[OrderbookSnapshot], cfg) -> tuple[bool, Optional[float], str]:
    if book is None or book.best_bid is None or book.best_ask is None:
        return False, None, RejectReason.NO_BEST_BID_ASK
    spread = book.spread
    if spread is None or spread < 0:
        return False, spread, RejectReason.NO_BEST_BID_ASK
    if spread > cfg.microstructure.max_spread:
        return False, spread, RejectReason.SPREAD_TOO_WIDE
    return True, spread, ""
