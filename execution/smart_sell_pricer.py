"""Exit pricing by exit reason severity."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import ExitReason, OrderbookSnapshot, round_to_tick

EMERGENCY_REASONS = {ExitReason.EMERGENCY, ExitReason.PANIC, ExitReason.EXPIRY_RISK,
                     ExitReason.STOP_LOSS, ExitReason.KILL_SWITCH,
                     ExitReason.DAILY_LOSS_RISK, ExitReason.MANUAL}
PROFIT_REASONS = {ExitReason.TAKE_PROFIT, ExitReason.PARTIAL_TAKE_PROFIT,
                  ExitReason.PROFIT_LOCK}


def exit_mode_for_reason(cfg, reason: Optional[ExitReason]) -> str:
    if reason in EMERGENCY_REASONS:
        return cfg.sell_execution.emergency_mode
    if reason in PROFIT_REASONS:
        return cfg.sell_execution.profit_take_mode
    return cfg.sell_execution.default_mode


def price_exit(book: Optional[OrderbookSnapshot], cfg, exit_mode: str,
               reason: Optional[ExitReason] = None,
               tick: float = 0.01) -> tuple[Optional[float], str]:
    if book is None or book.best_bid is None:
        return None, "no bid"
    bid = book.best_bid
    ask = book.best_ask
    floor = cfg.sell_execution.min_acceptable_exit_price

    if exit_mode == "aggressive_limit":
        price = max(floor, bid)
        note = f"hit bid {bid}"
    elif exit_mode == "passive_limit":
        base = ask if ask is not None else bid + 2 * tick
        price = max(floor, round_to_tick(base - tick, tick))
        note = f"join near ask {base}"
    else:  # smart
        if ask is not None:
            mid = (bid + ask) / 2
            price = max(floor, round_to_tick(mid, tick))
            note = f"smart mid {mid:.3f}"
        else:
            price = max(floor, bid)
            note = "no ask; take bid"
    price = max(tick, min(price, 1.0 - tick))
    return round_to_tick(price, tick), note


def chase_price(current_price: float, chase_count: int, cfg,
                tick: float = 0.01) -> Optional[float]:
    """One tick down per chase, bounded by max_chase_ticks and the floor."""
    if chase_count >= cfg.sell_execution.max_chase_ticks:
        return None
    new_price = round_to_tick(current_price - tick, tick)
    if new_price < cfg.sell_execution.min_acceptable_exit_price:
        return None
    return new_price
