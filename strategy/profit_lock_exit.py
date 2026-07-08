"""Position-level trailing profit lock."""
from __future__ import annotations


class ProfitLockTracker:
    def __init__(self, cfg):
        self.cfg = cfg
        self._peaks: dict[str, float] = {}

    def update(self, token_id: str, pnl_pct: float) -> None:
        cur = self._peaks.get(token_id, float("-inf"))
        if pnl_pct > cur:
            self._peaks[token_id] = pnl_pct

    def peak(self, token_id: str) -> float:
        return self._peaks.get(token_id, 0.0)

    def should_lock(self, token_id: str, current_pnl_pct: float, take_profit_pct: float) -> bool:
        """Lock when the position reached >=75% of its TP target then gave back
        >=40% of that peak."""
        peak = self._peaks.get(token_id)
        if peak is None or peak < take_profit_pct * 0.75:
            return False
        if peak <= 0:
            return False
        giveback = (peak - current_pnl_pct) / peak
        return giveback >= 0.4 and current_pnl_pct > 0

    def clear(self, token_id: str) -> None:
        self._peaks.pop(token_id, None)
