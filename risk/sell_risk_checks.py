"""SELL-side validation (master SELL VALIDATION list)."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import (
    OrderbookSnapshot, OrderSide, Position, RejectReason, RiskDecision)
from poly_alpha_sniper.microstructure.slippage_model import estimate_slippage_bps


def validate_sell(position: Optional[Position], sell_shares: float,
                  book: Optional[OrderbookSnapshot], cfg, now_ms: int) -> RiskDecision:
    checks: list[str] = []
    if position is None or position.shares <= 0:
        return RiskDecision(False, 0.0, RejectReason.INSUFFICIENT_SHARES, ["no position"])
    checks.append("position_exists")

    if sell_shares <= 0:
        return RiskDecision(False, 0.0, RejectReason.SELL_SIZE_TOO_SMALL, checks)
    if sell_shares > position.shares + 1e-9:
        return RiskDecision(False, 0.0, RejectReason.INSUFFICIENT_SHARES, checks)
    checks.append("shares_ok")

    if book is None:
        return RiskDecision(False, 0.0, RejectReason.STALE_ORDERBOOK, checks)
    if book.is_stale(now_ms, cfg.polymarket.max_orderbook_staleness_ms * 3):
        # exits get 3x staleness tolerance: better a slightly stale exit than none
        return RiskDecision(False, 0.0, RejectReason.STALE_ORDERBOOK, checks)
    checks.append("book_fresh_enough")

    if book.best_bid is None:
        return RiskDecision(False, 0.0, RejectReason.NO_BEST_BID_ASK, checks)
    if book.best_bid < cfg.sell_execution.min_acceptable_exit_price:
        return RiskDecision(False, 0.0, RejectReason.NO_BEST_BID_ASK,
                            checks + [f"bid {book.best_bid} below min acceptable"])
    checks.append("bid_exists")

    size_usd = sell_shares * book.best_bid
    slip = estimate_slippage_bps(book, OrderSide.SELL_YES, size_usd)
    if slip is None or slip > cfg.microstructure.max_slippage_bps * 2:
        # exits tolerate 2x entry slippage before demanding a resize
        return RiskDecision(False, 0.0, RejectReason.SLIPPAGE_TOO_HIGH,
                            checks + [f"slippage {slip}"])
    checks.append("slippage_ok")

    return RiskDecision(True, round(size_usd, 2), "", checks)
