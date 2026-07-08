"""WebSocket-first orderbook mirror with REST bootstrap/fallback.

Freshness engineering (a 1 s staleness budget is unforgiving):
- REST fallback refreshes stale books in PARALLEL (bounded concurrency), not
  sequentially — a sequential pass over dozens of tokens takes ~10 s and
  guarantees permanent staleness.
- Priority tokens (markets near the entry window) refresh first; each pass is
  capped so one slow pass can never starve the next.
- Expired markets must be untracked (app calls untrack_market) or the tracked
  set grows without bound and drags the fresh/total ratio down.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from poly_alpha_sniper.core.contracts import MarketInfo, OrderbookSnapshot
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("orderbook_mirror")

MAX_CONCURRENT_REST = 8
MAX_REFRESH_PER_PASS = 16


class OrderbookMirror:
    def __init__(self, cfg, clock, ws, rest, store):
        self.cfg = cfg
        self.clock = clock
        self.ws = ws
        self.rest = rest
        self.store = store
        self._tracked: set[str] = set()
        self._priority: set[str] = set()
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.last_refresh_ms = 0
        self.rest_refreshes = 0

    async def start(self) -> None:
        if self.ws is not None and self.cfg.polymarket.use_websocket_orderbook_first:
            await self.ws.start()
        self._stop.clear()
        self._task = asyncio.create_task(self._refresh_loop(), name="book_mirror")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
        if self.ws is not None:
            await self.ws.stop()

    async def track_market(self, market: MarketInfo) -> None:
        tokens = [t for t in (market.yes_token_id, market.no_token_id) if t]
        new = [t for t in tokens if t not in self._tracked]
        if not new:
            return
        self._tracked.update(new)
        if self.ws is not None:
            self.ws.subscribe_tokens(new)
        # REST bootstrap so books exist before the first WS snapshot
        if self.rest is not None and self.cfg.polymarket.rest_snapshot_fallback:
            await self._refresh_tokens(new)

    def untrack_market(self, market: MarketInfo) -> None:
        tokens = [market.yes_token_id, market.no_token_id]
        for t in tokens:
            self._tracked.discard(t)
            self._priority.discard(t)
            self.store.drop(t)
        if self.ws is not None:
            self.ws.unsubscribe(tokens)

    def set_priority_tokens(self, tokens: list[str]) -> None:
        """Tokens of markets in/near the entry window — refreshed first."""
        self._priority = {t for t in tokens if t}

    def get(self, token_id: str) -> Optional[OrderbookSnapshot]:
        return self.store.get(token_id)

    def is_fresh(self, token_id: str) -> bool:
        return self.store.is_fresh(token_id)

    def freshness(self) -> tuple[int, int]:
        tracked = list(self._tracked)
        fresh = sum(1 for t in tracked if self.store.is_fresh(t))
        return fresh, len(tracked)

    async def _refresh_tokens(self, tokens: list[str]) -> None:
        if self.rest is None or not tokens:
            return
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REST)

        async def one(token: str) -> None:
            async with semaphore:
                snap = await self.rest.get_book(token)
                if snap is not None:
                    self.store.update_snapshot(snap)
                    self.rest_refreshes += 1

        await asyncio.gather(*(one(t) for t in tokens), return_exceptions=True)

    async def _refresh_loop(self) -> None:
        interval = max(0.1, self.cfg.polymarket.refresh_orderbooks_ms / 1000.0)
        while not self._stop.is_set():
            try:
                if self.rest is not None and self.cfg.polymarket.rest_snapshot_fallback:
                    stale = [t for t in self._tracked if not self.store.is_fresh(t)]
                    # priority (entry-window) tokens first, capped per pass
                    stale.sort(key=lambda t: (t not in self._priority, t))
                    batch = stale[:MAX_REFRESH_PER_PASS]
                    if batch:
                        await self._refresh_tokens(batch)
                        self.last_refresh_ms = self.clock.now_ms()
            except Exception as exc:  # noqa: BLE001
                log.warning("refresh_error", extra={"extra": {"error": repr(exc)[:120]}})
            await asyncio.sleep(interval)
