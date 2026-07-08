"""Partial close wrapper honoring small-bankroll feasibility."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import ExitDecision, ExitReason, OrderbookSnapshot, Position
from poly_alpha_sniper.strategy.partial_exit_engine import plan_partial_exit


async def partial_close(position: Position, fraction: float,
                        book: Optional[OrderbookSnapshot], sell_executor,
                        min_order_usd: float):
    shares, is_full = plan_partial_exit(position, book, min_order_usd, fraction)
    if is_full:
        decision = ExitDecision.full(ExitReason.PARTIAL_TAKE_PROFIT,
                                     "partial infeasible at this size -> full close")
    else:
        decision = ExitDecision.partial(ExitReason.PARTIAL_TAKE_PROFIT,
                                        shares / position.shares,
                                        f"partial close {shares} shares")
    return await sell_executor.execute_exit(position, decision, book)
