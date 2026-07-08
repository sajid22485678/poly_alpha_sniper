"""All-time-high equity tracking and drawdown breach detection."""
from __future__ import annotations


class DrawdownGuard:
    def __init__(self):
        self.ath = 0.0
        self.current = 0.0

    def update(self, equity: float) -> None:
        self.current = equity
        if equity > self.ath:
            self.ath = equity

    @property
    def drawdown_pct(self) -> float:
        if self.ath <= 0:
            return 0.0
        return max(0.0, (self.ath - self.current) / self.ath)

    def defensive_breached(self, cfg) -> bool:
        return self.drawdown_pct >= cfg.adaptive_aggression.defensive_max_drawdown_pct

    def profit_lock_breached(self, cfg) -> bool:
        return self.drawdown_pct * 100.0 >= cfg.profit_lock.if_drawdown_from_ath_pct
