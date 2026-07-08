"""Micro bankroll mode (~$10 capital) protections."""
from __future__ import annotations


class MicroBankrollMode:
    ACTIVE_BELOW_EQUITY = 25.0

    def __init__(self, cfg):
        self.cfg = cfg

    def active(self, equity: float) -> bool:
        return self.cfg.micro_bankroll_mode.enabled and equity < self.ACTIVE_BELOW_EQUITY

    def entry_adjustments(self) -> dict:
        m = self.cfg.micro_bankroll_mode
        return {
            "min_edge_bump": 0.01 if m.require_higher_edge_live else 0.0,
            "max_positions": m.max_positions,
            "reject_wide_spread": m.reject_wide_spread,
            "reject_min_order_too_high": m.reject_min_order_too_high,
        }

    def should_halt(self, consecutive_losses: int) -> bool:
        return (self.cfg.micro_bankroll_mode.stop_after_two_losses
                and consecutive_losses >= 2)
