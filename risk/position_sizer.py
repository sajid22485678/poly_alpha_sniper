"""Position sizing — the ONE sizing function for every mode (master formula).

Two sizing modes (cfg.risk.sizing_mode):

"max_trade_usd" (default, prior behavior unchanged):
1.  equity = starting_bankroll + realized_pnl (realized ONLY — provided via
    PortfolioSnapshot.equity_usd)
2.  raw = equity * position_size_pct_equity
3.  size = clamp(raw, min_trade_usd, max_trade_usd)   [live caps applied]
4.  reject size > available cash
5.  reject market min order > size

"fixed_min_shares" (WS4): always size to exactly fixed_order_shares shares
at the executable price, ignoring max_trade_usd entirely -- this exists
because max_trade_usd=$1 was rejecting valid candidates whenever the ask
price needed more than $1 to buy Polymarket's minimum order (5 shares).
1.  size = fixed_order_shares * executable_price
2.  reject size > available cash -> REJECTED_INSUFFICIENT_CASH_FOR_5_SHARES
    (market.min_order_size_usd is not checked -- fixed sizing IS the min
    order by construction)

Both modes then share the same tail:
6.  reject market exposure cap (tier-aware in fixed_min_shares mode only --
    see risk/exposure_cap.py)
7.  reject total exposure cap
8.  reject when spread/slippage killed the edge
9.  reject daily loss cap
10. reject loss streak cap
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import (
    MarketInfo, PortfolioSnapshot, RejectReason, RiskDecision, TradingMode, clamp)
from poly_alpha_sniper.risk.exposure_cap import resolve_tier_exposure_cap_pct


def compute_position_size(cfg, portfolio: PortfolioSnapshot, market: MarketInfo,
                          mode: TradingMode, edge_after_slippage: float,
                          executable_price: float = 0.0, tier=None) -> RiskDecision:
    checks: list[str] = []
    r = cfg.risk
    equity = portfolio.equity_usd  # realized-only by contract
    market_cap_pct = r.max_market_exposure_pct_equity
    tier_exposure_detail = {}

    if r.sizing_mode == "fixed_min_shares":
        if executable_price <= 0:
            return RiskDecision(False, 0.0, RejectReason.DATA_QUALITY,
                                ["fixed_min_shares: no executable price available"])
        size = r.fixed_order_shares * executable_price
        checks.append(f"fixed_min_shares: shares={r.fixed_order_shares} "
                      f"price={executable_price:.4f} size={size:.2f}")
        fixed_detail = {
            "sizing_mode": "fixed_min_shares",
            "shares": r.fixed_order_shares,
            "ask_price": executable_price,
            "required_usd": round(size, 4),
            "available_cash_usd": round(portfolio.available_cash_usd, 4),
            "shortfall_usd": round(max(0.0, size - portfolio.available_cash_usd), 4),
        }
        if size > portfolio.available_cash_usd + 1e-9:
            return RiskDecision(False, 0.0, RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES,
                                checks, sizing_detail=fixed_detail)
        checks.append("cash_ok")

        market_cap_pct = resolve_tier_exposure_cap_pct(cfg, tier)
        tier_key = tier.value if hasattr(tier, "value") else str(tier or "")
        allowed_exposure_usd = round(equity * market_cap_pct, 4)
        tier_exposure_detail = {
            "sizing_mode": "fixed_min_shares",
            "tier": tier_key,
            "tier_cap_pct": market_cap_pct,
            "equity": round(equity, 4),
            "allowed_exposure_usd": allowed_exposure_usd,
            "proposed_usd": round(size, 4),
        }
        fixed_detail = {**fixed_detail, **tier_exposure_detail}
    else:
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
        fixed_detail = {}

    market_exposure = portfolio.exposure_by_market.get(market.market_id, 0.0)
    if market_exposure + size > equity * market_cap_pct + 1e-9:
        return RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, checks,
                            sizing_detail=tier_exposure_detail)
    checks.append("market_exposure_ok" if not tier_exposure_detail else "tier_exposure_ok")

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

    return RiskDecision(True, round(size, 2), "", checks, sizing_detail=fixed_detail)
