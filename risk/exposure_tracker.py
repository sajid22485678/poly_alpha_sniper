"""Exposure aggregation from open positions."""
from __future__ import annotations

from collections import defaultdict

from poly_alpha_sniper.core.contracts import Position


class ExposureTracker:
    def __init__(self):
        self.total_usd = 0.0
        self.by_market: dict[str, float] = {}
        self.by_asset: dict[str, float] = {}

    def recompute(self, positions: list[Position],
                  asset_lookup=None) -> dict:
        self.total_usd = 0.0
        by_market: dict[str, float] = defaultdict(float)
        by_asset: dict[str, float] = defaultdict(float)
        for p in positions:
            cost = p.cost_usd
            self.total_usd += cost
            by_market[p.market_id] += cost
            if asset_lookup is not None:
                asset = asset_lookup(p.market_id)
                if asset:
                    by_asset[asset] += cost
        self.by_market = dict(by_market)
        self.by_asset = dict(by_asset)
        return {"total_usd": self.total_usd, "by_market": self.by_market,
                "by_asset": self.by_asset}
