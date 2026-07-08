"""Realized-equity bankroll math with daily rollover (UTC)."""
from __future__ import annotations

from datetime import datetime, timezone


class Bankroll:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self.starting = cfg.risk.starting_bankroll_usd
        self.realized_pnl = 0.0
        self.realized_pnl_today = 0.0
        self.equity_ath = self.starting
        self.day_start_equity = self.starting
        self._day = self._current_day()
        self.trades_today = 0

    def _current_day(self) -> str:
        return datetime.fromtimestamp(self.clock.now_s(), tz=timezone.utc).strftime("%Y-%m-%d")

    def _rollover_if_needed(self) -> None:
        day = self._current_day()
        if day != self._day:
            self._day = day
            self.realized_pnl_today = 0.0
            self.day_start_equity = self.equity
            self.trades_today = 0

    @property
    def equity(self) -> float:
        """Realized-only equity. Unrealized PnL NEVER compounds (master rule)."""
        return self.starting + self.realized_pnl

    def record_realized(self, pnl_usd: float) -> None:
        self._rollover_if_needed()
        self.realized_pnl += pnl_usd
        self.realized_pnl_today += pnl_usd
        self.trades_today += 1
        if self.equity > self.equity_ath:
            self.equity_ath = self.equity

    def daily_loss_cap_usd(self) -> float:
        return min(self.cfg.risk.max_daily_loss_usd,
                   self.equity * self.cfg.risk.max_daily_loss_pct_equity)

    def daily_loss_cap_hit(self) -> bool:
        self._rollover_if_needed()
        return self.realized_pnl_today <= -self.daily_loss_cap_usd()

    def snapshot_fields(self) -> dict:
        self._rollover_if_needed()
        return {"equity_usd": self.equity, "realized_pnl_usd": self.realized_pnl,
                "realized_pnl_today_usd": self.realized_pnl_today,
                "equity_ath_usd": self.equity_ath, "trades_today": self.trades_today}
