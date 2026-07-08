"""Position sizing — the ONE sizing function for every mode (master formula).

1.  equity = starting_bankroll + realized_pnl (realized ONLY — provided via
    PortfolioSnapshot.equity_usd)
2.  raw = equity * position_size_pct_equity
3.  size = clamp(raw, min_trade_usd, max_trade_usd)   [live caps applied]
4.  reject size > available cash
5.  reject market min order > size
6.  reject market exposure cap
7.  reject total exposure cap
8.  reject when spread/slippage killed the edge
9.  reject daily loss cap
10. reject loss streak cap
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import (
    MarketInfo, PortfolioSnapshot, RejectReason, RiskDecision, TradingMode, clamp)


def compute_position_size(cfg, portfolio: PortfolioSnapshot, market: MarketInfo,
                          mode: TradingMode, edge_after_slippage: float) -> RiskDecision:
    checks: list[str] = []
    r = cfg.risk

    equity = portfolio.equity_usd  # realized-only by contract
    raw = equity * r.position_size_pct_equity
    max_cap = r.max_trade_usd
    if mode == TradingMode.LIVE_MICRO:
        max_cap = min(max_cap, r.live_micro_trade_usd)
    size = clamp(raw, r.min_trade_usd, max_cap)
    checks.append(f"size={size:.2f} (raw={raw:.2f}, cap={max_cap:.2f})")

    if size > portfolio.available_cash_usd + 1e-9:
        return RiskDecision(False, 0.0, RejectReason.INSUFFICIENT_CASH, checks)
    checks.append("cash_ok")

    if market.min_order_size_usd > size + 1e-9:
        return RiskDecision(False, 0.0, RejectReason.MIN_ORDER_SIZE_TOO_HIGH, checks)
    checks.append("min_order_ok")

    market_exposure = portfolio.exposure_by_market.get(market.market_id, 0.0)
    if market_exposure + size > equity * r.max_market_exposure_pct_equity + 1e-9:
        return RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, checks)
    checks.append("market_exposure_ok")

    if portfolio.total_exposure_usd + size > equity * r.max_total_exposure_pct_equity + 1e-9:
        return RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, checks)
    checks.append("total_exposure_ok")

    if edge_after_slippage <= 0:
        return RiskDecision(False, 0.0, RejectReason.SLIPPAGE_TOO_HIGH, checks)
    checks.append("edge_survives_costs")

    daily_cap = min(r.max_daily_loss_usd, equity * r.max_daily_loss_pct_equity)
    if portfolio.realized_pnl_today_usd <= -daily_cap + 1e-9:
        return RiskDecision(False, 0.0, RejectReason.DAILY_LOSS_CAP, checks)
    checks.append("daily_loss_ok")

    if portfolio.consecutive_losses >= r.max_consecutive_losses:
        return RiskDecision(False, 0.0, RejectReason.LOSS_STREAK, checks)
    checks.append("loss_streak_ok")

    return RiskDecision(True, round(size, 2), "", checks)
