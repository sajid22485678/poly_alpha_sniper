"""Full close + direction-flip helpers.

Flip rule (master): close current side FIRST, confirm the sell, only then is
the opposite entry allowed — the ready_for_opposite flag proves it.
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import (
    ExitDecision, ExitReason, OrderbookSnapshot, OrderState, OrderSide, Position)
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("close_position")

SELL_CONFIRMED_STATES = {OrderState.MATCHED, OrderState.MINED, OrderState.CONFIRMED,
                         OrderState.RECONCILED}


async def close_full(position: Position, book: Optional[OrderbookSnapshot],
                     sell_executor, reason: ExitReason, detail: str = ""):
    decision = ExitDecision.full(reason, detail or f"full close ({reason.value})")
    return await sell_executor.execute_exit(position, decision, book)


async def flip_position(position: Position, target_side: OrderSide,
                        book: Optional[OrderbookSnapshot], sell_executor) -> dict:
    """Sell current side; report whether the opposite entry may proceed."""
    if target_side.outcome == position.outcome:
        return {"sold": False, "ready_for_opposite": False,
                "note": "target side equals held side — nothing to flip"}
    record = await close_full(position, book, sell_executor, ExitReason.OPPOSITE_SIGNAL,
                              "flip: close before opposite entry")
    full_fill = record.filled_shares >= position.shares - 1e-9
    confirmed = record.state in SELL_CONFIRMED_STATES and full_fill
    log.info("flip_attempt", extra={"extra": {
        "token": position.token_id, "state": record.state.value,
        "filled": record.filled_shares, "confirmed": confirmed}})
    return {"sold": record.filled_shares > 0, "ready_for_opposite": confirmed,
            "order": record,
            "note": "sell confirmed — opposite entry may proceed" if confirmed
                    else "sell NOT fully confirmed — opposite entry blocked"}
