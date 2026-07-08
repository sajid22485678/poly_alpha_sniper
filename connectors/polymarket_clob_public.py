"""Polymarket CLOB public (no-auth) REST endpoints: books, prices, markets."""
from __future__ import annotations

from typing import Any, Optional

import aiohttp

from poly_alpha_sniper.core.contracts import BookLevel, OrderbookSnapshot, RequestPriority
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("clob_public")


def parse_book(data: dict, token_id: str, ts_ms: int) -> OrderbookSnapshot:
    def levels(raw, reverse: bool) -> list[BookLevel]:
        out = []
        for lvl in raw or []:
            try:
                out.append(BookLevel(float(lvl["price"]), float(lvl["size"])))
            except (KeyError, TypeError, ValueError):
                continue
        out.sort(key=lambda l: l.price, reverse=reverse)
        return out

    return OrderbookSnapshot(
        token_id=token_id,
        bids=levels(data.get("bids"), reverse=True),
        asks=levels(data.get("asks"), reverse=False),
        ts_ms=ts_ms, source="rest")


class PolymarketClobPublic:
    def __init__(self, cfg, clock=None, governor=None):
        self.base = cfg.polymarket.clob_base_url.rstrip("/")
        self.clock = clock
        self.governor = governor
        self._session: Optional[aiohttp.ClientSession] = None

    def _now(self) -> int:
        if self.clock is not None:
            return self.clock.now_ms()
        import time
        return int(time.time() * 1000)

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
                headers={"Accept": "application/json"})
        return self._session

    async def _get(self, path: str, params: dict | None = None) -> Any:
        if self.governor is not None:
            await self.governor.acquire("clob_public", RequestPriority.DISCOVERY)
        sess = await self._sess()
        async with sess.get(f"{self.base}{path}", params=params or {}) as resp:
            if resp.status == 429 and self.governor is not None:
                self.governor.report_429("clob_public")
            resp.raise_for_status()
            return await resp.json()

    async def get_book(self, token_id: str) -> Optional[OrderbookSnapshot]:
        try:
            data = await self._get("/book", {"token_id": token_id})
        except Exception as exc:  # noqa: BLE001
            log.warning("book_fetch_failed", extra={"extra": {"error": repr(exc)[:120]}})
            return None
        return parse_book(data, token_id, self._now())

    async def get_price(self, token_id: str, side: str = "BUY") -> Optional[float]:
        try:
            data = await self._get("/price", {"token_id": token_id, "side": side})
            return float(data.get("price"))
        except Exception:  # noqa: BLE001
            return None

    async def get_midpoint(self, token_id: str) -> Optional[float]:
        try:
            data = await self._get("/midpoint", {"token_id": token_id})
            return float(data.get("mid"))
        except Exception:  # noqa: BLE001
            return None

    async def get_market(self, condition_id: str) -> Optional[dict]:
        try:
            return await self._get(f"/markets/{condition_id}")
        except Exception:  # noqa: BLE001
            return None

    async def get_sampling_markets(self) -> list[dict]:
        try:
            data = await self._get("/sampling-markets")
            return data.get("data", []) if isinstance(data, dict) else []
        except Exception:  # noqa: BLE001
            return []

    async def get_ok(self) -> bool:
        try:
            sess = await self._sess()
            async with sess.get(f"{self.base}/ok") as resp:
                return resp.status < 500
        except Exception:  # noqa: BLE001
            return False

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
