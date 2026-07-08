"""Synthetic delayed Polymarket odds — RESEARCH ONLY.

When no real Polymarket orderbook history is available, the backtest builds
synthetic books whose implied probability follows the model-fair probability
computed on a LAGGED CEX price (odds react `lag_ms` late — that lag IS the
edge the strategy monetizes; if there is no real lag in production the live
edge will be smaller than backtests suggest).

Every result produced with this module is labeled synthetic_odds=True /
"RESEARCH ONLY".
"""
from __future__ import annotations

from collections import deque

import numpy as np

from poly_alpha_sniper.core.contracts import (
    BookLevel, MarketInfo, MarketType, OrderbookSnapshot, clamp)
from poly_alpha_sniper.strategy.distance_to_strike_model import prob_above_threshold

RESEARCH_ONLY = "RESEARCH ONLY — synthetic delayed odds, not real Polymarket data"


class SyntheticPolyOdds:
    def __init__(self, seed: int = 7, lag_ms: int = 1500, base_spread: float = 0.02,
                 depth_usd: float = 300.0, noise_bp: float = 30.0):
        self.rng = np.random.RandomState(seed)
        self.lag_ms = lag_ms
        self.base_spread = base_spread
        self.depth_usd = depth_usd
        self.noise_bp = noise_bp
        self._price_history: dict[str, deque[tuple[int, float]]] = {}

    def observe_price(self, asset: str, ts_ms: int, price: float) -> None:
        buf = self._price_history.setdefault(asset, deque(maxlen=5000))
        buf.append((ts_ms, price))

    def lagged_price(self, asset: str, now_ms: int) -> float | None:
        buf = self._price_history.get(asset)
        if not buf:
            return None
        target = now_ms - self.lag_ms
        best = None
        for ts, price in reversed(buf):
            if ts <= target:
                best = price
                break
        return best if best is not None else buf[0][1]

    def fair_prob_lagged(self, market: MarketInfo, asset_open_price: float,
                         now_ms: int, vol_per_s: float) -> float:
        lagged = self.lagged_price(market.asset, now_ms)
        if lagged is None or asset_open_price <= 0:
            return 0.5
        tte_s = max(market.seconds_to_expiry(now_ms), 1.0)
        if market.market_type == MarketType.THRESHOLD and market.threshold:
            p_above, _, _ = prob_above_threshold(lagged, market.threshold, tte_s,
                                                 max(vol_per_s, 5e-5))
            return p_above
        # UP_DOWN: prob of closing above the window-open price
        p_above, _, _ = prob_above_threshold(lagged, asset_open_price, tte_s,
                                             max(vol_per_s, 5e-5))
        return p_above

    def books_for(self, market: MarketInfo, asset_open_price: float,
                  now_ms: int, vol_per_s: float
                  ) -> tuple[OrderbookSnapshot, OrderbookSnapshot]:
        p_yes_condition = self.fair_prob_lagged(market, asset_open_price, now_ms,
                                                vol_per_s)
        p_yes = p_yes_condition if market.direction_up_means_yes else 1 - p_yes_condition
        noise = self.rng.normal(0.0, self.noise_bp / 10_000.0)
        mid_yes = clamp(p_yes + noise, 0.03, 0.97)
        half = self.base_spread / 2

        def _book(token_id: str, mid: float) -> OrderbookSnapshot:
            bid = round(max(0.01, mid - half), 2)
            ask = round(min(0.99, mid + half), 2)
            if ask <= bid:
                ask = round(bid + 0.01, 2)
            size_at = self.depth_usd / max(mid, 0.05)
            return OrderbookSnapshot(
                token_id=token_id,
                bids=[BookLevel(bid, size_at), BookLevel(round(bid - 0.01, 2), size_at),
                      BookLevel(round(bid - 0.02, 2), size_at)],
                asks=[BookLevel(ask, size_at), BookLevel(round(ask + 0.01, 2), size_at),
                      BookLevel(round(ask + 0.02, 2), size_at)],
                ts_ms=now_ms, source="synthetic")

        return (_book(market.yes_token_id, mid_yes),
                _book(market.no_token_id, clamp(1.0 - mid_yes, 0.03, 0.97)))
