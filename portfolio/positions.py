"""Portfolio: position inventory + fills + realized-only equity.

BUY fills: increase shares, recompute average entry, reduce cash.
SELL fills: reduce shares (never negative), realize PnL vs average entry,
add proceeds to cash. Position removed when shares ~ 0.
Equity = starting bankroll + REALIZED pnl only (no unrealized compounding).
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import (
    FillRecord, MarketInfo, PortfolioSnapshot, Position, Outcome)
from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.portfolio.bankroll import Bankroll

log = get_logger("portfolio")


class Portfolio:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self.bankroll = Bankroll(cfg, clock)
        self._cash = cfg.risk.starting_bankroll_usd
        self._positions: dict[str, Position] = {}
        self._consecutive_losses = 0
        self._market_by_token: dict[str, str] = {}

    # ------------------------------------------------------------------
    def set_cash(self, cash_usd: float) -> None:
        """Adopt reconciled exchange balance (live startup)."""
        self._cash = cash_usd

    @property
    def cash(self) -> float:
        return self._cash

    def get(self, token_id: str) -> Optional[Position]:
        return self._positions.get(token_id)

    def open_positions(self) -> list[Position]:
        return list(self._positions.values())

    def mark(self, token_id: str, bid: Optional[float], ask: Optional[float]) -> None:
        pos = self._positions.get(token_id)
        if pos is not None:
            pos.current_bid = bid
            pos.current_ask = ask
            pos.last_update_ms = self.clock.now_ms()

    def record_trade_result(self, win: bool) -> None:
        self._consecutive_losses = 0 if win else self._consecutive_losses + 1

    # ------------------------------------------------------------------
    def apply_fill(self, fill: FillRecord, market: Optional[MarketInfo] = None) -> Position:
        pos = self._positions.get(fill.token_id)
        if fill.side.is_buy:
            cost = fill.size_shares * fill.price
            if cost > self._cash + 1e-6:
                raise ValueError(f"fill cost ${cost:.2f} exceeds cash ${self._cash:.2f}")
            if pos is None:
                pos = Position(token_id=fill.token_id, market_id=fill.market_id,
                               outcome=fill.side.outcome,
                               shares=fill.size_shares, avg_entry_price=fill.price,
                               entry_ts_ms=fill.ts_ms, last_update_ms=fill.ts_ms)
                self._positions[fill.token_id] = pos
                self._market_by_token[fill.token_id] = fill.market_id
            else:
                total_cost = pos.shares * pos.avg_entry_price + cost
                pos.shares += fill.size_shares
                pos.avg_entry_price = total_cost / pos.shares
                pos.last_update_ms = fill.ts_ms
            self._cash -= cost
        else:
            if pos is None or fill.size_shares > pos.shares + 1e-9:
                owned = pos.shares if pos else 0.0
                raise ValueError(
                    f"cannot sell {fill.size_shares} shares of {fill.token_id}; own {owned}")
            proceeds = fill.size_shares * fill.price
            realized = fill.size_shares * (fill.price - pos.avg_entry_price) - fill.fee_usd
            pos.shares -= fill.size_shares
            pos.realized_pnl += realized
            pos.last_update_ms = fill.ts_ms
            self._cash += proceeds
            self.bankroll.record_realized(realized)
            if pos.shares < 1e-6:
                del self._positions[fill.token_id]
            log.info("position_reduced", extra={"extra": {
                "token": fill.token_id, "sold": fill.size_shares,
                "realized": round(realized, 4), "remaining": round(max(pos.shares, 0), 4)}})
        return pos

    def settle_resolution(self, token_id: str, won: bool, ts_ms: int) -> Optional[float]:
        """Market resolved while holding: winners pay $1/share, losers $0."""
        pos = self._positions.pop(token_id, None)
        if pos is None:
            return None
        payout_price = 1.0 if won else 0.0
        realized = pos.shares * (payout_price - pos.avg_entry_price)
        self._cash += pos.shares * payout_price
        self.bankroll.record_realized(realized)
        self.record_trade_result(realized > 0)
        return realized

    # ------------------------------------------------------------------
    def snapshot(self, now_ms: int) -> PortfolioSnapshot:
        exposure_by_market: dict[str, float] = {}
        positions_by_market: dict[str, str] = {}
        total_exposure = 0.0
        unrealized = 0.0
        for p in self._positions.values():
            total_exposure += p.cost_usd
            exposure_by_market[p.market_id] = exposure_by_market.get(p.market_id, 0.0) + p.cost_usd
            positions_by_market[p.market_id] = p.outcome.value
            unrealized += p.unrealized_pnl()
        b = self.bankroll.snapshot_fields()
        return PortfolioSnapshot(
            equity_usd=b["equity_usd"], available_cash_usd=self._cash,
            realized_pnl_usd=b["realized_pnl_usd"], unrealized_pnl_usd=unrealized,
            realized_pnl_today_usd=b["realized_pnl_today_usd"],
            open_positions=len(self._positions), total_exposure_usd=total_exposure,
            exposure_by_market=exposure_by_market,
            consecutive_losses=self._consecutive_losses,
            positions_by_market=positions_by_market,
            equity_ath_usd=b["equity_ath_usd"], trades_today=b["trades_today"])
