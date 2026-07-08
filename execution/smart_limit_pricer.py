"""Entry pricing. Limit orders only (never market orders — config enforced).

Modes:
- aggressive_limit: cross to touch, capped at slippage tolerance from the touch
- passive_limit:   join the near side (maker)
- smart:           mid rounded toward passive; improve one tick when urgent
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot, OrderSide, round_to_tick
from poly_alpha_sniper.microstructure.orderbook_imbalance import imbalance


def price_entry(book: Optional[OrderbookSnapshot], side: OrderSide, mode: str, cfg,
                urgency: float = 0.5, tick: float = 0.01) -> tuple[Optional[float], str]:
    if book is None or book.best_bid is None or book.best_ask is None:
        return None, "no two-sided book"
    bid, ask = book.best_bid, book.best_ask
    max_slip = cfg.execution_pricing.max_slippage_bps / 10_000.0

    if side.is_buy:
        if mode == "aggressive_limit":
            price = min(ask, round_to_tick(ask * (1 + max_slip), tick))
            note = f"cross to ask {ask}"
        elif mode == "passive_limit":
            price = bid
            note = f"join bid {bid}"
        else:  # smart
            mid = (bid + ask) / 2
            price = round_to_tick(mid - tick / 2, tick)  # round toward passive
            if urgency > 0.7 or imbalance(book) < -0.3:  # ask pressure: pay up
                price = min(ask, price + tick)
            note = f"smart near mid {mid:.3f}"
        price = max(tick, min(price, 1.0 - tick))
    else:
        if mode == "aggressive_limit":
            price = max(bid, round_to_tick(bid * (1 - max_slip), tick))
            note = f"hit bid {bid}"
        elif mode == "passive_limit":
            price = ask
            note = f"join ask {ask}"
        else:
            mid = (bid + ask) / 2
            price = round_to_tick(mid + tick / 2, tick)
            if urgency > 0.7 or imbalance(book) > 0.3:
                price = max(bid, price - tick)
            note = f"smart near mid {mid:.3f}"
        price = max(tick, min(price, 1.0 - tick))

    return round_to_tick(price, tick), note
