"""CEX shock detector — the trigger of the whole lag-arb pipeline.

A valid shock needs fresh data, an absolute short-window return AND z-score
above thresholds, clear direction, multi-exchange agreement, per-asset
cooldown, and tolerable fakeout risk.
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import Direction, MultiCexView, Shock
from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.strategy.fakeout_filter import fakeout_risk

log = get_logger("shock_detector")


class ShockDetector:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._last_shock_ms: dict[str, int] = {}

    def detect(self, view: Optional[MultiCexView]) -> Optional[Shock]:
        if view is None or view.primary is None:
            return None
        stats = view.primary
        if not stats.fresh or stats.reconnect_recent:
            return None
        if not view.direction_agreement:
            return None

        s = self.cfg.strategy
        # trigger window: strongest of the 1-3s returns
        trigger_ret = 0.0
        for sec in (1, 2, 3):
            r = stats.returns.get(sec, 0.0)
            if abs(r) > abs(trigger_ret):
                trigger_ret = r
        if abs(trigger_ret) < s.shock_min_abs_return:
            return None
        if abs(stats.zscore) < s.shock_min_zscore:
            return None
        # direction must be consistent across 2s and 5s legs
        r2, r5 = stats.returns.get(2, 0.0), stats.returns.get(5, 0.0)
        if r2 == 0.0 or (r5 != 0.0 and r2 * r5 < 0):
            return None

        now = self.clock.now_ms()
        cooldown_ms = int(s.cooldown_market_seconds * 1000)
        if now - self._last_shock_ms.get(stats.asset, 0) < cooldown_ms:
            return None

        risk = fakeout_risk(stats.returns, stats.zscore, stats.volatility)
        if risk > 0.8:
            log.info("shock_suppressed_fakeout", extra={"extra": {
                "asset": stats.asset, "risk": round(risk, 2)}})
            return None

        direction = Direction.UP if trigger_ret > 0 else Direction.DOWN
        self._last_shock_ms[stats.asset] = now
        shock = Shock(
            asset=stats.asset, direction=direction, ts_ms=now,
            returns=dict(stats.returns), zscore=stats.zscore,
            impulse=stats.impulse, momentum=stats.momentum,
            volatility=stats.volatility,
            confirming_exchanges=view.confirming_exchanges,
            fakeout_risk=risk,
            reason=(f"{stats.asset} {direction.value} ret={trigger_ret:.4%} "
                    f"z={stats.zscore:.2f} confirm={view.confirming_exchanges}"))
        log.info("shock_detected", extra={"extra": {
            "asset": shock.asset, "direction": shock.direction.value,
            "z": round(shock.zscore, 2), "fakeout": round(risk, 2)}})
        return shock

    def on_shock_consumed(self, asset: str) -> None:
        self._last_shock_ms[asset] = self.clock.now_ms()
