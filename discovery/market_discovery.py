"""Automatic discovery of tradable 5-minute crypto markets.

Primary source: Gamma /markets with end-date-window queries (verified to
return the live 5m/15m up-or-down children). Optional CLOB fallback fetcher
kicks in only when Gamma yields zero tradable markets.

Fetchers are injected (async callables) so the pipeline is offline-testable.

One-shot diagnostic against the real API:
    python -m poly_alpha_sniper.discovery.market_discovery
"""
from __future__ import annotations

from collections import Counter
from typing import Awaitable, Callable, Optional

from poly_alpha_sniper.core.contracts import MarketInfo
from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.discovery.crypto_market_classifier import is_crypto_5min_candidate
from poly_alpha_sniper.discovery.market_mapper import map_raw_market
from poly_alpha_sniper.discovery.market_universe_expander import (
    build_gamma_queries, merge_markets)

log = get_logger("discovery")

GammaFetcher = Callable[[dict], Awaitable[list[dict]]]
ClobFetcher = Callable[[], Awaitable[list[dict]]]


class MarketDiscovery:
    def __init__(self, cfg, clock, gamma_fetcher: GammaFetcher,
                 clob_fetcher: Optional[ClobFetcher] = None):
        self.cfg = cfg
        self.clock = clock
        self.fetch = gamma_fetcher
        self.clob_fetch = clob_fetcher
        self.reject_stats: Counter = Counter()
        self.candidates_by_query: dict[str, int] = {}
        self.last_markets: list[MarketInfo] = []
        self.last_raw_sample: list[dict] = []
        self.last_raw_count = 0

    async def refresh(self) -> list[MarketInfo]:
        now = self.clock.now_ms()
        raw_lists: list[list[dict]] = []
        self.candidates_by_query = {}
        for params in build_gamma_queries(self.cfg, now):
            label = params.pop("_label", "query")
            try:
                rows = await self.fetch(params)
                raw_lists.append(rows)
                self.candidates_by_query[label] = len(rows)
            except Exception as exc:  # noqa: BLE001 — one bad query must not kill the sweep
                self.candidates_by_query[label] = -1
                log.warning("gamma_query_failed", extra={"extra": {
                    "label": label, "error": repr(exc)[:120]}})
        merged = merge_markets(raw_lists)
        tradable = self._map_and_filter(merged, now)

        # CLOB fallback only when Gamma produced nothing tradable
        if not tradable and self.clob_fetch is not None:
            try:
                clob_rows = await self.clob_fetch()
                self.candidates_by_query["clob_fallback"] = len(clob_rows)
                merged = merge_markets([merged, clob_rows])
                tradable = self._map_and_filter(merged, now)
            except Exception as exc:  # noqa: BLE001
                self.candidates_by_query["clob_fallback"] = -1
                log.warning("clob_fallback_failed", extra={"extra": {"error": repr(exc)[:120]}})

        self.last_markets = tradable
        log.info("discovery_refresh", extra={"extra": {
            "raw": self.last_raw_count, "tradable": len(tradable),
            "by_query": self.candidates_by_query,
            "rejects": dict(self.reject_stats)}})
        return tradable

    def _map_and_filter(self, merged: list[dict], now: int) -> list[MarketInfo]:
        self.last_raw_count = len(merged)
        self.last_raw_sample = [
            {"title": str(r.get("question") or r.get("title") or "")[:80],
             "slug": str(r.get("slug") or r.get("market_slug") or "")[:60]}
            for r in merged[:20]]
        tradable: list[MarketInfo] = []
        self.reject_stats = Counter()
        for raw in merged:
            market = map_raw_market(raw, now)
            ok, reason = is_crypto_5min_candidate(market, now, self.cfg)
            if not ok:
                self.reject_stats[reason] += 1
                continue
            # fixed_min_shares mode ignores max_trade_usd entirely (see
            # risk/position_sizer.py) -- filtering candidates out by
            # max_trade_usd here would silently starve that mode of markets
            # it is otherwise fully able to size and trade.
            if self.cfg.risk.sizing_mode != "fixed_min_shares" \
                    and market.min_order_size_usd > self.cfg.risk.max_trade_usd:
                self.reject_stats["min_order_size_too_high"] += 1
                continue
            if 0 < market.liquidity_usd < self.cfg.polymarket.min_liquidity_usd:
                self.reject_stats["low_liquidity"] += 1
                continue
            tradable.append(market)
        tradable.sort(key=lambda m: m.expiry_ts_ms)
        return tradable

    # ------------------------------------------------------------------
    def diagnostic_report(self) -> dict:
        now = self.clock.now_ms()
        return {
            "raw_count": self.last_raw_count,
            "candidates_by_query": dict(self.candidates_by_query),
            "mapped_count": len(self.last_markets),
            "rejected_by_reason": dict(self.reject_stats),
            "top_raw": self.last_raw_sample,
            "mapped": [{
                "asset": m.asset,
                "title": m.title[:70],
                "slug": str(m.raw.get("slug", ""))[:60],
                "market_id": m.market_id,
                "condition_id": m.condition_id[:20] + "...",
                "expiry": m.expiry_ts_ms,
                "time_to_expiry_s": round(m.seconds_to_expiry(now), 1),
                "yes_token": m.yes_token_id[:16] + "...",
                "no_token": m.no_token_id[:16] + "...",
                "direction_up_means_yes": m.direction_up_means_yes,
                "market_type": m.market_type.value,
                "min_order_shares": m.raw.get("min_order_shares"),
                "active_tradable": m.active and not m.closed,
                "source": "gamma",
            } for m in self.last_markets[:20]],
        }


async def _diagnose() -> None:
    """One-shot real-API diagnostic (safe public metadata only)."""
    import json
    from poly_alpha_sniper.core.clock import WallClock
    from poly_alpha_sniper.core.config_loader import load_config
    from poly_alpha_sniper.connectors.polymarket_clob_public import PolymarketClobPublic
    from poly_alpha_sniper.connectors.polymarket_gamma import PolymarketGamma

    cfg = load_config()
    gamma = PolymarketGamma(cfg)
    clob = PolymarketClobPublic(cfg)
    disc = MarketDiscovery(cfg, WallClock(), gamma.get_markets,
                           clob_fetcher=clob.get_sampling_markets)
    try:
        await disc.refresh()
        print(json.dumps(disc.diagnostic_report(), indent=2, default=str))
    finally:
        await gamma.close()
        await clob.close()


if __name__ == "__main__":
    import asyncio
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_diagnose())
