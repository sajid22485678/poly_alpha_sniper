"""OrderSide helpers: CLOB side strings, token selection, share math."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import MarketInfo, OrderSide, Outcome


def clob_side(side: OrderSide) -> str:
    return "BUY" if side.is_buy else "SELL"


def token_for_side(market: MarketInfo, side: OrderSide) -> str:
    return market.yes_token_id if side.outcome == Outcome.YES else market.no_token_id


def opening_side_for_outcome(outcome: Outcome) -> OrderSide:
    return OrderSide.BUY_YES if outcome == Outcome.YES else OrderSide.BUY_NO


def closing_side_for_outcome(outcome: Outcome) -> OrderSide:
    return OrderSide.SELL_YES if outcome == Outcome.YES else OrderSide.SELL_NO


def shares_for_usd(usd: float, price: float) -> float:
    if price <= 0:
        return 0.0
    return round(usd / price, 2)


def usd_for_shares(shares: float, price: float) -> float:
    return round(shares * price, 4)
