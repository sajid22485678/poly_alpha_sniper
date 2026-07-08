"""Evaluate exits across all open positions; return firing decisions ordered
by priority then loss severity."""
from __future__ import annotations

from typing import Callable, Optional

from poly_alpha_sniper.core.contracts import ExitDecision, PortfolioSnapshot, Position


class SellSignalEngine:
    def __init__(self, exit_engine):
        self.exit_engine = exit_engine

    def evaluate_all(self, positions: list[Position],
                     market_lookup: Callable, book_lookup: Callable,
                     fair_lookup: Callable, view_lookup: Callable,
                     portfolio: PortfolioSnapshot,
                     panic: bool = False, kill: bool = False
                     ) -> list[tuple[Position, ExitDecision]]:
        firing: list[tuple[Position, ExitDecision]] = []
        for pos in positions:
            market = market_lookup(pos.market_id)
            book = book_lookup(pos.token_id)
            fair = fair_lookup(pos)
            view = view_lookup(market.asset) if market is not None else None
            decision = self.exit_engine.evaluate(pos, market, book, fair, view,
                                                 portfolio, panic=panic, kill=kill)
            if decision.should_exit:
                firing.append((pos, decision))

        def sort_key(pair: tuple[Position, ExitDecision]):
            pos, dec = pair
            pnl = pos.unrealized_pnl()
            return (dec.priority, pnl)  # worst losers first within a priority

        firing.sort(key=sort_key)
        return firing
