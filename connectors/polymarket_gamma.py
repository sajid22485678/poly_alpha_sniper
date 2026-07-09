"""Polymarket Gamma (public market metadata) REST client."""
from __future__ import annotations

from typing import Any, Optional

import aiohttp

from poly_alpha_sniper.core.contracts import RequestPriority
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("gamma")


class PolymarketGamma:
    def __init__(self, cfg, governor=None):
        self.base = cfg.polymarket.gamma_base_url.rstrip("/")
        self.governor = governor
        self._session: Optional[aiohttp.ClientSession] = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"Accept": "application/json"})
        return self._session

    async def _get(self, path: str, params: dict) -> Any:
        if self.governor is not None:
            await self.governor.acquire("gamma", RequestPriority.DISCOVERY)
        sess = await self._sess()
        async with sess.get(f"{self.base}{path}", params=params) as resp:
            if resp.status == 429 and self.governor is not None:
                self.governor.report_429("gamma")
            resp.raise_for_status()
            return await resp.json()

    async def get_markets(self, params: dict) -> list[dict]:
        """One query-plan page sweep with limit/offset pagination."""
        out: list[dict] = []
        offset = 0
        limit = int(params.get("limit", 100))
        for _page in range(5):  # bounded pagination per plan
            page_params = {k: str(v) for k, v in params.items()}
            page_params["limit"] = str(limit)
            page_params["offset"] = str(offset)
            data = await self._get("/markets", page_params)
            if not isinstance(data, list) or not data:
                break
            out.extend(data)
            if len(data) < limit:
                break
            offset += limit
        return out

    async def get_events(self, params: dict) -> list[dict]:
        data = await self._get("/events", {k: str(v) for k, v in params.items()})
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("events", "data", "items", "results"):
                rows = data.get(key)
                if isinstance(rows, list):
                    return rows
            return [data]
        return []

    async def get_event_for_market(self, raw_market: dict) -> Optional[dict]:
        """Best-effort public Gamma event hydration for a shallow market row.

        Some /markets rows omit eventMetadata, where priceToBeat lives for
        active 5-minute crypto markets. This method only fetches public
        metadata and returns one event dict; it never touches orders or
        private endpoints.
        """
        params_to_try: list[dict] = []
        event_ref = self._first_embedded_event(raw_market)
        for value in (
            raw_market.get("eventId"), raw_market.get("event_id"),
            event_ref.get("id") if event_ref else None,
        ):
            if value:
                params_to_try.append({"id": value})
        for value in (
            raw_market.get("eventSlug"), raw_market.get("event_slug"),
            event_ref.get("slug") if event_ref else None,
        ):
            if value:
                params_to_try.append({"slug": value})

        seen: set[tuple[tuple[str, str], ...]] = set()
        for params in params_to_try:
            key = tuple(sorted((str(k), str(v)) for k, v in params.items()))
            if key in seen:
                continue
            seen.add(key)
            rows = await self.get_events(params)
            if rows:
                return rows[0]
        return None

    @staticmethod
    def _first_embedded_event(raw_market: dict) -> dict:
        events = raw_market.get("events")
        if not isinstance(events, list):
            return {}
        for event in events:
            if isinstance(event, dict):
                return event
        return {}

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
