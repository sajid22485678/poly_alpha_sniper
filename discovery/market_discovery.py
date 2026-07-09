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
from poly_alpha_sniper.discovery.market_mapper import (
    extract_oracle_anchor_metadata, map_raw_market)
from poly_alpha_sniper.discovery.market_universe_expander import (
    build_gamma_queries, merge_markets)

log = get_logger("discovery")

GammaFetcher = Callable[[dict], Awaitable[list[dict]]]
ClobFetcher = Callable[[], Awaitable[list[dict]]]
EventHydrator = Callable[[dict], Awaitable[Optional[dict]]]


class MarketDiscovery:
    def __init__(self, cfg, clock, gamma_fetcher: GammaFetcher,
                 clob_fetcher: Optional[ClobFetcher] = None,
                 event_hydrator: Optional[EventHydrator] = None):
        self.cfg = cfg
        self.clock = clock
        self.fetch = gamma_fetcher
        self.clob_fetch = clob_fetcher
        self.event_hydrator = event_hydrator
        self.reject_stats: Counter = Counter()
        self.candidates_by_query: dict[str, int] = {}
        self.last_markets: list[MarketInfo] = []
        self.last_raw_sample: list[dict] = []
        self.last_raw_count = 0
        # Cache successful anchors only. Missing anchors are intentionally not
        # cached so a later Gamma hydration can fill price_to_beat mid-window.
        self._oracle_anchor_cache: dict[str, dict] = {}

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
        merged = await self._hydrate_oracle_anchors(merged)
        tradable = self._map_and_filter(merged, now)

        # CLOB fallback only when Gamma produced nothing tradable
        if not tradable and self.clob_fetch is not None:
            try:
                clob_rows = await self.clob_fetch()
                self.candidates_by_query["clob_fallback"] = len(clob_rows)
                merged = merge_markets([merged, clob_rows])
                merged = await self._hydrate_oracle_anchors(merged)
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

    async def _hydrate_oracle_anchors(self, merged: list[dict]) -> list[dict]:
        out: list[dict] = []
        for raw in merged:
            enriched = dict(raw)
            key = self._market_key(enriched)
            price, _source, resolution_url, _diag = extract_oracle_anchor_metadata(enriched)
            if price is not None:
                if key:
                    self._oracle_anchor_cache[key] = self._anchor_cache_payload(
                        enriched, price, resolution_url)
                out.append(enriched)
                continue

            if key and key in self._oracle_anchor_cache:
                out.append(self._apply_cached_anchor(enriched, self._oracle_anchor_cache[key]))
                continue

            if self.event_hydrator is None:
                out.append(enriched)
                continue

            # Always attempt hydration for a missing anchor. Verified against
            # live Gamma (2026-07-10): Polymarket publishes priceToBeat with a
            # per-market delay after each 5-min window opens. Sometimes it lands
            # on the /markets embedded event directly (picked up above); other
            # times the embedded event still has eventMetadata=null but the
            # /events?id= endpoint already exposes the populated metadata -- so
            # the hydration call genuinely recovers anchors and must not be
            # skipped. Missing anchors are intentionally NOT cached, so this
            # retries on every refresh until the anchor appears.
            enriched["_anchor_hydration_attempted"] = True
            event = None
            try:
                event = await self.event_hydrator(enriched)
            except Exception as exc:  # noqa: BLE001
                log.warning("oracle_anchor_hydration_failed", extra={"extra": {
                    "market": key, "error": repr(exc)[:120]}})

            if isinstance(event, dict) and event:
                enriched = self._merge_event(enriched, event)
                price, _source, resolution_url, _diag = extract_oracle_anchor_metadata(enriched)
                success = price is not None
                enriched["_anchor_hydration_attempted"] = True
                enriched["_anchor_hydration_success"] = success
                if success and key:
                    self._oracle_anchor_cache[key] = self._anchor_cache_payload(
                        enriched, price, resolution_url)
            else:
                enriched["_anchor_hydration_success"] = False
            out.append(enriched)
        return out

    @staticmethod
    def _market_key(raw: dict) -> str:
        for key in ("id", "slug", "conditionId", "condition_id", "market_slug"):
            value = raw.get(key)
            if value:
                return str(value)
        return ""

    @staticmethod
    def _anchor_cache_payload(raw: dict, price: float, resolution_url: str) -> dict:
        event_id = ""
        events = raw.get("events")
        if isinstance(events, list):
            for event in events:
                if isinstance(event, dict) and event.get("id"):
                    event_id = str(event.get("id"))
                    break
        return {
            "priceToBeat": price,
            "resolutionSource": resolution_url,
            "eventId": str(raw.get("eventId") or raw.get("event_id") or event_id or ""),
            "slug": str(raw.get("slug") or raw.get("market_slug") or ""),
        }

    @staticmethod
    def _apply_cached_anchor(raw: dict, payload: dict) -> dict:
        enriched = dict(raw)
        if not enriched.get("priceToBeat") and not enriched.get("price_to_beat"):
            enriched["priceToBeat"] = payload.get("priceToBeat")
        if payload.get("resolutionSource") and not enriched.get("resolutionSource"):
            enriched["resolutionSource"] = payload.get("resolutionSource")
        if payload.get("eventId") and not enriched.get("eventId"):
            enriched["eventId"] = payload.get("eventId")
        enriched["_anchor_hydration_attempted"] = False
        enriched["_anchor_hydration_success"] = True
        return enriched

    @staticmethod
    def _merge_event(raw: dict, event: dict) -> dict:
        enriched = dict(raw)
        events = enriched.get("events")
        event_list = list(events) if isinstance(events, list) else []
        if event_list and isinstance(event_list[0], dict):
            event_list[0] = {**event_list[0], **event}
        else:
            event_list.insert(0, event)
        enriched["events"] = event_list
        if event.get("id") and not enriched.get("eventId"):
            enriched["eventId"] = event.get("id")
        if event.get("eventMetadata") and not enriched.get("eventMetadata"):
            enriched["eventMetadata"] = event.get("eventMetadata")
        if event.get("metadata") and not enriched.get("metadata"):
            enriched["metadata"] = event.get("metadata")
        if event.get("resolutionSource") and not enriched.get("resolutionSource"):
            enriched["resolutionSource"] = event.get("resolutionSource")
        return enriched

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
                           clob_fetcher=clob.get_sampling_markets,
                           event_hydrator=gamma.get_event_for_market)
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
