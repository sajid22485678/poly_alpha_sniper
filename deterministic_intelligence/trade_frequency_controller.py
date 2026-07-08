"""Trade frequency controller: high frequency without overtrading.

Hour-bucket counters + per-market/asset caps + result-dependent cooldowns.
All time via injected clock -> identical in backtest.
"""
from __future__ import annotations

from collections import defaultdict

from poly_alpha_sniper.core.contracts import TradingMode


class TradeFrequencyController:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._entries_ms: list[int] = []
        self._entries_by_asset: dict[str, list[int]] = defaultdict(list)
        self._entries_by_market: dict[str, int] = defaultdict(int)
        self._profit_by_market: dict[str, bool] = {}
        self._cooldown_until: dict[str, int] = {}

    def _hour_prune(self, now: int) -> None:
        cutoff = now - 3_600_000
        self._entries_ms = [t for t in self._entries_ms if t > cutoff]
        for asset in list(self._entries_by_asset):
            self._entries_by_asset[asset] = [t for t in self._entries_by_asset[asset] if t > cutoff]

    def _max_per_hour(self, mode: TradingMode) -> int:
        tf = self.cfg.trade_frequency
        if mode in (TradingMode.LIVE_MICRO, TradingMode.LIVE_FULL):
            return min(tf.max_trades_per_hour_live_micro, self.cfg.strategy.max_trades_per_hour)
        return self.cfg.strategy.max_trades_per_hour

    def can_trade(self, asset: str, market_id: str, mode: TradingMode) -> tuple[bool, str]:
        if not self.cfg.trade_frequency.enabled:
            return True, "frequency control disabled"
        now = self.clock.now_ms()
        self._hour_prune(now)
        tf = self.cfg.trade_frequency

        until = self._cooldown_until.get(market_id, 0)
        if now < until:
            return False, f"market cooldown {int((until - now) / 1000)}s remaining"

        if len(self._entries_ms) >= self._max_per_hour(mode):
            return False, f"hourly trade cap {self._max_per_hour(mode)} reached"
        if len(self._entries_by_asset[asset]) >= tf.max_trades_per_asset_per_hour:
            return False, f"asset {asset} hourly cap {tf.max_trades_per_asset_per_hour} reached"

        reentries = self._entries_by_market[market_id]
        if reentries >= 1:
            if reentries > tf.max_reentries_same_market:
                return False, f"max re-entries {tf.max_reentries_same_market} reached"
            if tf.allow_reentry_after_profit and not self._profit_by_market.get(market_id, False):
                return False, "re-entry only allowed after profitable exit"
        return True, "ok"

    def record_entry(self, asset: str, market_id: str) -> None:
        now = self.clock.now_ms()
        self._entries_ms.append(now)
        self._entries_by_asset[asset].append(now)
        self._entries_by_market[market_id] += 1

    def record_result(self, asset: str, market_id: str, win: bool, bad_fill: bool = False) -> None:
        tf = self.cfg.trade_frequency
        now = self.clock.now_ms()
        self._profit_by_market[market_id] = win
        if bad_fill:
            cd = tf.cooldown_after_bad_fill_seconds
        elif win:
            cd = tf.cooldown_after_win_seconds
        else:
            cd = tf.cooldown_after_loss_seconds
        self._cooldown_until[market_id] = now + int(cd * 1000)

    def record_reject(self, market_id: str) -> None:
        now = self.clock.now_ms()
        cd = int(self.cfg.trade_frequency.cooldown_after_reject_seconds * 1000)
        self._cooldown_until[market_id] = max(self._cooldown_until.get(market_id, 0), now + cd)

    def stats(self) -> dict:
        now = self.clock.now_ms()
        self._hour_prune(now)
        return {"trades_last_hour": len(self._entries_ms),
                "by_asset": {a: len(v) for a, v in self._entries_by_asset.items()},
                "cooldowns_active": sum(1 for t in self._cooldown_until.values() if t > now)}
