"""Partial-exit sizing with small-bankroll feasibility (master rule: skip
partial exits when either part would violate min order size — full close)."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot, Position


def plan_partial_exit(position: Position, book: Optional[OrderbookSnapshot],
                      min_order_usd: float, fraction: float = 0.5) -> tuple[float, bool]:
    """Returns (shares_to_sell, is_full_close)."""
    if position.shares <= 0:
        return 0.0, True
    bid = book.best_bid if book is not None else None
    price = bid if bid else position.avg_entry_price
    if price <= 0:
        return position.shares, True
    sell_shares = position.shares * fraction
    keep_shares = position.shares - sell_shares
    if sell_shares * price < min_order_usd or keep_shares * price < min_order_usd:
        return position.shares, True  # partial infeasible -> full close
    return round(sell_shares, 2), False
