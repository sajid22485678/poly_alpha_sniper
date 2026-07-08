"""Book-walking expected fill model (shared by validator, edge engine
sanity, simulator sizing, and the backtest)."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot, OrderSide


def expected_fill(book: Optional[OrderbookSnapshot], side: OrderSide,
                  size_usd: float) -> dict:
    out = {"avg_price": None, "slippage_bps": 0.0, "filled_usd": 0.0, "levels_used": 0}
    if book is None or size_usd <= 0:
        return out
    levels = book.asks if side.is_buy else book.bids
    if not levels:
        return out
    ref = levels[0].price
    remaining = size_usd
    cost = 0.0
    shares = 0.0
    used = 0
    for lvl in levels:
        lvl_usd = lvl.price * lvl.size
        take = min(remaining, lvl_usd)
        if take <= 0:
            break
        shares += take / lvl.price
        cost += take
        remaining -= take
        used += 1
        if remaining <= 1e-9:
            break
    if shares <= 0:
        return out
    avg = cost / shares
    out.update({
        "avg_price": avg,
        "slippage_bps": abs(avg - ref) / max(ref, 1e-9) * 10_000,
        "filled_usd": size_usd - remaining,
        "levels_used": used,
    })
    return out


def estimate_slippage_bps(book: Optional[OrderbookSnapshot], side: OrderSide,
                          size_usd: float) -> Optional[float]:
    fill = expected_fill(book, side, size_usd)
    if fill["avg_price"] is None:
        return None
    if fill["filled_usd"] < size_usd * 0.999:
        return fill["slippage_bps"] + 100.0  # depth short: punitive
    return fill["slippage_bps"]
