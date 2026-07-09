"""Risk manager — entry and sell gatekeeper composed from the shared pieces.

Order of entry checks: panic -> kill -> open-position caps -> one-per-market ->
micro-bankroll halt -> position sizing (which handles cash/exposure/daily-loss/
loss-streak). Telegram controls can never bypass this.
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import (
    OrderbookSnapshot, PortfolioSnapshot, Position, RejectReason, RiskDecision,
    Signal, TradingMode)
from poly_alpha_sniper.risk.micro_bankroll_mode import MicroBankrollMode
from poly_alpha_sniper.risk.position_sizer import compute_position_size
from poly_alpha_sniper.risk.sell_risk_checks import validate_sell


class RiskManager:
    def __init__(self, cfg, clock, kill_switch, panic_mode):
        self.cfg = cfg
        self.clock = clock
        self.kill_switch = kill_switch
        self.panic = panic_mode
        self.micro = MicroBankrollMode(cfg)

    def check_entry(self, signal: Signal, portfolio: PortfolioSnapshot,
                    mode: TradingMode) -> RiskDecision:
        checks: list[str] = []
        if self.panic.is_active:
            return RiskDecision(False, 0.0, RejectReason.PANIC_MODE, ["panic active"])
        checks.append("no_panic")
        if self.kill_switch.is_active:
            return RiskDecision(False, 0.0, RejectReason.KILL_SWITCH, ["kill switch active"])
        checks.append("no_kill_switch")

        max_positions = self.cfg.risk.max_open_positions
        if self.micro.active(portfolio.equity_usd):
            max_positions = min(max_positions, self.cfg.micro_bankroll_mode.max_positions)
        if portfolio.open_positions >= max_positions:
            return RiskDecision(False, 0.0, RejectReason.MAX_OPEN_POSITIONS, checks)
        checks.append("open_positions_ok")

        if self.cfg.risk.one_position_per_market \
                and signal.market.market_id in portfolio.positions_by_market:
            # also enforces no-averaging-down and no both-sides
            return RiskDecision(False, 0.0, RejectReason.ONE_POSITION_PER_MARKET, checks)
        checks.append("one_per_market_ok")

        if self.micro.active(portfolio.equity_usd) \
                and self.micro.should_halt(portfolio.consecutive_losses):
            return RiskDecision(False, 0.0, RejectReason.LOSS_STREAK,
                                checks + ["micro bankroll halt after losses"])
        checks.append("micro_bankroll_ok")

        sizing = compute_position_size(self.cfg, portfolio, signal.market, mode,
                                       signal.edge.edge_after_slippage,
                                       executable_price=signal.edge.market_price)
        sizing.checks = checks + sizing.checks
        return sizing

    def check_sell(self, position: Optional[Position], sell_shares: float,
                   book: Optional[OrderbookSnapshot]) -> RiskDecision:
        # NOTE: panic/kill do NOT block sells — closing risk is always allowed.
        return validate_sell(position, sell_shares, book, self.cfg, self.clock.now_ms())
