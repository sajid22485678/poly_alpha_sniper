"""Order validator — the full pre-trade checklist for every BUY and SELL.

Same function in backtest/simulation/shadow/live. Returns RiskDecision with
canonical RejectReason strings.
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import (
    MarketInfo, OrderbookSnapshot, OrderRequest, PortfolioSnapshot, RejectReason,
    RiskDecision, is_valid_tick, round_to_tick)
from poly_alpha_sniper.microstructure.slippage_model import estimate_slippage_bps


def _min_order_sizing_detail(req: OrderRequest, portfolio: PortfolioSnapshot, cfg,
                             ask: float, min_shares: float) -> dict:
    """Numbers needed to explain a REJECTED_MIN_ORDER_SIZE_TOO_HIGH rejection
    without implying edge/confidence must improve -- the actual blocker here is
    Polymarket's share minimum vs. this bankroll's configured max_trade_usd."""
    min_required_usd = round(min_shares * ask, 4) if ask else 0.0
    return {
        "min_shares": round(min_shares, 4),
        "ask_price": round(ask, 4) if ask else 0.0,
        "min_required_usd": min_required_usd,
        "configured_max_trade_usd": cfg.risk.max_trade_usd,
        "proposed_usd": round(req.size_usd, 4),
        "available_cash_usd": round(portfolio.available_cash_usd, 4),
        "shortfall_usd": round(max(0.0, min_required_usd - req.size_usd), 4),
    }


def validate_order(req: OrderRequest, book: Optional[OrderbookSnapshot],
                   market: MarketInfo, portfolio: PortfolioSnapshot, cfg,
                   now_ms: int, owned_shares: float = 0.0) -> RiskDecision:
    checks: list[str] = []

    # market usable
    if market.closed or not market.active or market.parse_reject_reason:
        return RiskDecision(False, 0.0, RejectReason.AMBIGUOUS_MARKET, ["market unusable"])
    checks.append("market_active")

    # book present + fresh
    max_stale = cfg.polymarket.max_orderbook_staleness_ms * (3 if req.side.is_sell else 1)
    if book is None or book.is_stale(now_ms, max_stale):
        return RiskDecision(False, 0.0, RejectReason.STALE_ORDERBOOK, checks)
    checks.append("book_fresh")

    if book.best_bid is None or book.best_ask is None or book.crossed:
        return RiskDecision(False, 0.0, RejectReason.NO_BEST_BID_ASK, checks)
    checks.append("two_sided_book")

    # tick / precision
    tick = market.tick_size
    if tick <= 0 or not is_valid_tick(round_to_tick(req.price, tick), tick):
        return RiskDecision(False, 0.0, RejectReason.INVALID_TICK_SIZE, checks)
    if abs(req.price - round_to_tick(req.price, tick)) > 1e-9:
        return RiskDecision(False, 0.0, RejectReason.INVALID_PRICE_PRECISION, checks)
    checks.append("tick_ok")

    if req.size_shares <= 0 or req.size_usd <= 0:
        return RiskDecision(False, 0.0, RejectReason.INCOMPLETE_TRADE_PACKET, checks)
    checks.append("sizes_positive")

    if req.side.is_buy:
        ask = book.best_ask if book.best_ask is not None else req.price
        if req.size_usd < market.min_order_size_usd - 1e-9:
            detail = _min_order_sizing_detail(req, portfolio, cfg, ask,
                                              min_shares=market.min_order_size_usd / ask if ask else 0.0)
            return RiskDecision(False, 0.0, RejectReason.MIN_ORDER_SIZE_TOO_HIGH, checks,
                                sizing_detail=detail)
        # Polymarket minimums are share-denominated (typically 5 shares):
        # enforce the TRUE minimum against this order's share count.
        min_shares = float(market.raw.get("min_order_shares") or 0)
        if min_shares > 0 and req.size_shares < min_shares - 1e-9:
            detail = _min_order_sizing_detail(req, portfolio, cfg, ask, min_shares=min_shares)
            return RiskDecision(False, 0.0, RejectReason.MIN_ORDER_SIZE_TOO_HIGH,
                                checks + [f"needs >= {min_shares} shares"], sizing_detail=detail)
        checks.append("min_order_ok")
        if req.size_usd > portfolio.available_cash_usd + 1e-9:
            return RiskDecision(False, 0.0, RejectReason.INSUFFICIENT_CASH, checks)
        checks.append("cash_ok")
        # exposure caps
        eq = max(portfolio.equity_usd, 1e-9)
        mkt_exp = portfolio.exposure_by_market.get(req.market_id, 0.0)
        if mkt_exp + req.size_usd > eq * cfg.risk.max_market_exposure_pct_equity + 1e-9:
            return RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, checks)
        if portfolio.total_exposure_usd + req.size_usd > eq * cfg.risk.max_total_exposure_pct_equity + 1e-9:
            return RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, checks)
        checks.append("exposure_ok")
    else:
        if req.size_shares > owned_shares + 1e-9:
            return RiskDecision(False, 0.0, RejectReason.INSUFFICIENT_SHARES, checks)
        checks.append("shares_ok")
        if req.size_shares * (book.best_bid or 0) < 0.01:
            return RiskDecision(False, 0.0, RejectReason.SELL_SIZE_TOO_SMALL, checks)
        checks.append("sell_size_ok")

    # spread
    if book.spread is not None and book.spread > cfg.microstructure.max_spread \
            and req.side.is_buy:
        return RiskDecision(False, 0.0, RejectReason.SPREAD_TOO_WIDE, checks)
    checks.append("spread_ok")

    # slippage for the actual size
    slip = estimate_slippage_bps(book, req.side, req.size_usd)
    limit = cfg.microstructure.max_slippage_bps * (2 if req.side.is_sell else 1)
    if slip is None or slip > limit:
        return RiskDecision(False, 0.0, RejectReason.SLIPPAGE_TOO_HIGH,
                            checks + [f"slippage={slip}"])
    checks.append("slippage_ok")

    return RiskDecision(True, req.size_usd, "", checks)
