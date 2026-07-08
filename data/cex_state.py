"""Multi-exchange CEX state: rolling windows per (asset, exchange), blended
views for shock detection and multi-exchange confirmation.
"""
from __future__ import annotations

import math
from typing import Optional

from poly_alpha_sniper.core.contracts import CexTick, CexWindowStats, MultiCexView, clamp
from poly_alpha_sniper.data.price_windows import PriceWindow


class CexState:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._windows: dict[tuple[str, str], PriceWindow] = {}
        self._last_tick: dict[tuple[str, str], CexTick] = {}
        self._last_recv_ms: dict[tuple[str, str], int] = {}
        self._reconnect_flag: dict[str, int] = {}  # exchange -> until_ms
        self.tick_counts: dict[str, int] = {}

    def _window(self, asset: str, exchange: str) -> PriceWindow:
        key = (asset, exchange)
        if key not in self._windows:
            self._windows[key] = PriceWindow()
        return self._windows[key]

    def mark_reconnect(self, exchange: str) -> None:
        self._reconnect_flag[exchange] = self.clock.now_ms() + 3000

    def update(self, tick: CexTick) -> None:
        self._window(tick.asset, tick.exchange).add(tick.price, tick.ts_ms)
        self._last_tick[(tick.asset, tick.exchange)] = tick
        # Freshness is judged by LOCAL receive time: exchange event timestamps
        # carry network latency + clock skew and would mark a live stream
        # stale against a 500 ms budget.
        recv = tick.recv_ts_ms or self.clock.now_ms()
        self._last_recv_ms[(tick.asset, tick.exchange)] = recv
        self.tick_counts[tick.asset] = self.tick_counts.get(tick.asset, 0) + 1

    def window_ready(self, asset: str) -> bool:
        """Enough samples for returns/vol on ANY exchange for this asset."""
        now = self.clock.now_ms()
        for (a, ex), w in self._windows.items():
            if a == asset and w.n_samples() >= 10 and w.volatility_per_s(now) > 0:
                return True
        return False

    def windows_ready_by_asset(self) -> dict[str, bool]:
        return {a: self.window_ready(a) for a in self.cfg.assets}

    def latest_prices(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for asset in self.cfg.assets:
            st = self.stats(asset)
            if st is not None:
                out[asset] = st.price
        return out

    def last_recv_by_asset(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for (asset, _ex), recv in self._last_recv_ms.items():
            out[asset] = max(out.get(asset, 0), recv)
        return out

    def exchanges_for(self, asset: str) -> list[str]:
        return [e for (a, e) in self._windows if a == asset]

    def _all_stats_for_asset(self, asset: str, now_ms: int) -> dict[str, CexWindowStats]:
        out: dict[str, CexWindowStats] = {}
        for ex in self.exchanges_for(asset):
            st = self._build_stats(asset, ex, now_ms)
            if st is not None:
                out[ex] = st
        return out

    def _select_best(self, per_ex: dict[str, CexWindowStats]) -> Optional[CexWindowStats]:
        """Freshest-available source wins, full stop.

        A stale 'preferred' exchange (e.g. Binance geo-blocked, or Bybit
        between sparse trade prints on a low-volume pair like SOL) must NEVER
        shadow a genuinely fresher fallback that has real, current data. This
        is the single selection rule shared by stats()/multi_view()/is_fresh()
        so every freshness-dependent decision in the bot agrees on what "the
        current price" is. Config preference order (preferred > fallback >
        optional) is used ONLY to break an exact staleness tie.
        """
        if not per_ex:
            return None
        order = {self.cfg.cex.preferred_exchange: 0,
                 self.cfg.cex.fallback_exchange: 1,
                 self.cfg.cex.optional_exchange: 2}
        return min(per_ex.values(),
                  key=lambda s: (s.staleness_ms, order.get(s.exchange, 99)))

    def stats(self, asset: str, exchange: Optional[str] = None) -> Optional[CexWindowStats]:
        now = self.clock.now_ms()
        if exchange:
            return self._build_stats(asset, exchange, now)
        return self._select_best(self._all_stats_for_asset(asset, now))

    def _build_stats(self, asset: str, exchange: str, now_ms: int) -> Optional[CexWindowStats]:
        w = self._windows.get((asset, exchange))
        if w is None or w.n_samples() == 0:
            return None
        price = w.last_price() or 0.0
        last_ts = w.last_ts_ms() or 0
        last_recv = self._last_recv_ms.get((asset, exchange), last_ts)
        staleness = max(0, now_ms - last_recv)
        returns = {}
        for sec in self.cfg.strategy.prediction_windows_seconds:
            r = w.return_over(sec, now_ms)
            returns[sec] = r if r is not None else 0.0
        vol = w.volatility_per_s(now_ms)
        z = w.zscore(2.0, now_ms)
        momentum = self._momentum(returns)
        impulse = self._impulse(returns.get(2, 0.0), vol)
        return CexWindowStats(
            asset=asset, exchange=exchange, price=price, ts_ms=last_ts,
            returns=returns, volatility=vol, zscore=z, momentum=momentum,
            impulse=impulse,
            fresh=staleness <= self.cfg.cex.max_cex_staleness_ms,
            staleness_ms=staleness,
            reconnect_recent=now_ms < self._reconnect_flag.get(exchange, 0))

    @staticmethod
    def _momentum(returns: dict[int, float]) -> float:
        # weighted blend of short returns, squashed to [-1, 1].
        # 0.15% blended move ~ full momentum for 5-min crypto.
        blend = (0.5 * returns.get(2, 0.0) + 0.3 * returns.get(5, 0.0)
                 + 0.2 * returns.get(10, 0.0))
        return math.tanh(blend / 0.0015)

    @staticmethod
    def _impulse(ret_2s: float, vol_per_s: float) -> float:
        if vol_per_s <= 1e-9:
            return 0.0
        z = abs(ret_2s) / (vol_per_s * math.sqrt(2.0))
        return clamp(z / 5.0, 0.0, 1.0)

    def is_fresh(self, asset: str) -> bool:
        st = self.stats(asset)
        return bool(st and st.fresh)

    def multi_view(self, asset: str) -> Optional[MultiCexView]:
        now = self.clock.now_ms()
        per_ex = self._all_stats_for_asset(asset, now)
        if not per_ex:
            return None
        primary = self._select_best(per_ex)
        # direction agreement over 2s returns (ignore tiny moves < 0.02%)
        signs = {1 if s.returns.get(2, 0.0) > 0 else -1
                 for s in per_ex.values() if abs(s.returns.get(2, 0.0)) > 0.0002}
        agreement = len(signs) <= 1
        confirming = sum(1 for s in per_ex.values()
                         if s.fresh and abs(s.returns.get(2, 0.0)) > 0.0002
                         and (1 if s.returns.get(2, 0.0) > 0 else -1)
                         == (1 if primary.returns.get(2, 0.0) > 0 else -1))
        prices = [s.price for s in per_ex.values() if s.price > 0]
        max_dev = 0.0
        if len(prices) >= 2:
            lo, hi = min(prices), max(prices)
            max_dev = (hi - lo) / lo * 100.0
        return MultiCexView(
            asset=asset, primary=primary, per_exchange=per_ex,
            confirming_exchanges=max(confirming, 1 if primary.fresh else 0),
            direction_agreement=agreement, max_deviation_pct=max_dev,
            any_stale=any(not s.fresh for s in per_ex.values()))
