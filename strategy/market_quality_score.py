"""Market quality 0-100: is this market structurally good to trade?"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import (
    MarketInfo, MarketQualityResult, OrderbookSnapshot, clamp)


class MarketQualityScorer:
    WEIGHTS = {"spread": 0.22, "depth": 0.20, "liquidity": 0.15,
               "freshness": 0.15, "mapping": 0.18, "tte_fit": 0.10}

    def __init__(self, cfg):
        self.cfg = cfg

    def score(self, market: MarketInfo, yes_book: Optional[OrderbookSnapshot],
              no_book: Optional[OrderbookSnapshot], now_ms: int) -> MarketQualityResult:
        c: dict[str, float] = {}
        reasons: list[str] = []
        book = yes_book or no_book

        # spread tightness
        max_spread = self.cfg.polymarket.max_spread
        if book is None or book.spread is None:
            c["spread"] = 0.0
            reasons.append("no book / no two-sided quote")
        else:
            c["spread"] = clamp(100.0 * (1.0 - book.spread / max_spread), 0.0, 100.0)
            if book.spread > max_spread * 0.7:
                reasons.append(f"spread {book.spread:.3f} near limit")

        # depth at top 3
        min_depth = self.cfg.microstructure.min_depth_at_ask_usd
        depth = book.depth_usd_at_ask(3) if book is not None else 0.0
        c["depth"] = clamp(100.0 * depth / (2 * min_depth), 0.0, 100.0)
        if depth < min_depth:
            reasons.append(f"ask depth ${depth:.0f} < ${min_depth:.0f}")

        # reported liquidity
        min_liq = self.cfg.polymarket.min_liquidity_usd
        if market.liquidity_usd <= 0:
            c["liquidity"] = 50.0  # unknown: neutral
        else:
            c["liquidity"] = clamp(100.0 * market.liquidity_usd / (4 * min_liq), 0.0, 100.0)
            if market.liquidity_usd < min_liq:
                reasons.append("liquidity below floor")

        # book freshness
        max_stale = self.cfg.polymarket.max_orderbook_staleness_ms
        if book is None:
            c["freshness"] = 0.0
        else:
            age = max(0, now_ms - book.ts_ms)
            c["freshness"] = clamp(100.0 * (1.0 - age / max_stale), 0.0, 100.0)
            if age > max_stale:
                reasons.append(f"book stale {age}ms")

        # mapping confidence
        c["mapping"] = clamp(market.mapping_confidence, 0.0, 100.0)
        if market.mapping_confidence < 95:
            reasons.append("mapping confidence < 95")

        # time-to-expiry fit
        tte = market.seconds_to_expiry(now_ms)
        preferred = self.cfg.ultra_short_expiry.preferred_time_to_expiry_seconds
        if not preferred:
            c["tte_fit"] = 50.0
        else:
            dist = min(abs(tte - p) for p in preferred)
            c["tte_fit"] = clamp(100.0 * (1.0 - dist / 180.0), 0.0, 100.0)

        total = sum(self.WEIGHTS[k] * c[k] for k in self.WEIGHTS)
        return MarketQualityResult(score=round(total, 1), components=c, reasons=reasons)
