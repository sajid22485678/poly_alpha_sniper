"""Rebalance: swap the weakest position for a clearly better opportunity."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import Position, Signal


class RebalanceEngine:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._last_rebalance_ms = 0

    def consider(self, positions: list[Position], entry_alphas: dict[str, float],
                 candidate: Optional[Signal]) -> Optional[dict]:
        r = self.cfg.rebalance
        if not r.enabled or candidate is None or not positions:
            return None
        now = self.clock.now_ms()
        if now - self._last_rebalance_ms < r.cooldown_seconds * 1000:
            return None
        # weakest position by its recorded entry alpha
        weakest = min(positions, key=lambda p: entry_alphas.get(p.token_id, 100.0))
        weakest_alpha = entry_alphas.get(weakest.token_id, 100.0)
        if candidate.alpha_score - weakest_alpha < r.min_alpha_improvement:
            return None
        if candidate.market.market_id == weakest.market_id:
            return None
        self._last_rebalance_ms = now
        return {
            "sell_token_id": weakest.token_id,
            "sell_market_id": weakest.market_id,
            "buy_signal_id": candidate.signal_id,
            "improvement": candidate.alpha_score - weakest_alpha,
            "require_sell_confirmed": r.require_sell_confirmed_before_new_buy,
            "reason": (f"candidate alpha {candidate.alpha_score:.0f} beats weakest "
                       f"{weakest_alpha:.0f} by >= {r.min_alpha_improvement}"),
        }
