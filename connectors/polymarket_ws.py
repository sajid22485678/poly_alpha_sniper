"""Polymarket CLOB market-channel WebSocket: book snapshots + price deltas.

Maintains a local book per subscribed token and emits OrderbookSnapshot on
every change. Resubscribes on reconnect. REST fallback lives in
data/orderbook_mirror.py.
"""
from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable

import websockets

from poly_alpha_sniper.core.contracts import BookLevel, OrderbookSnapshot
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("poly_ws")

OnBook = Callable[[OrderbookSnapshot], Awaitable[None]]


class PolymarketWS:
    def __init__(self, cfg, clock, on_book: OnBook):
        self.cfg = cfg
        self.clock = clock
        self.on_book = on_book
        self._tokens: set[str] = set()
        self._books: dict[str, dict[str, dict[float, float]]] = {}
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._resubscribe = asyncio.Event()
        self.connected = False
        self.last_msg_ms = 0

    def subscribe_tokens(self, token_ids: list[str]) -> None:
        new = set(token_ids) - self._tokens
        if new:
            self._tokens.update(new)
            self._resubscribe.set()

    def unsubscribe(self, token_ids: list[str]) -> None:
        for t in token_ids:
            self._tokens.discard(t)
            self._books.pop(t, None)

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="poly_ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        backoff = 2.0
        while not self._stop.is_set():
            if not self._tokens:
                await asyncio.sleep(1.0)
                continue
            try:
                async with websockets.connect(self.cfg.polymarket.ws_url,
                                              ping_interval=10, close_timeout=3) as ws:
                    self.connected = True
                    await ws.send(json.dumps({
                        "assets_ids": sorted(self._tokens), "type": "market"}))
                    log.info("connected", extra={"extra": {"tokens": len(self._tokens)}})
                    backoff = 2.0
                    self._resubscribe.clear()
                    last_resub_ms = self.clock.now_ms()
                    while not self._stop.is_set():
                        # Debounced resubscribe: reconnecting on EVERY new token
                        # (discovery adds some each 20 s sweep) would churn the
                        # socket constantly and leave permanent update gaps.
                        # REST bootstrap covers new tokens until we reconnect.
                        if (self._resubscribe.is_set()
                                and self.clock.now_ms() - last_resub_ms > 45_000):
                            break  # reconnect with the expanded token set
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue
                        await self._handle(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                self.connected = False
                log.warning("ws_error_reconnecting", extra={"extra": {"error": repr(exc)[:150]}})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        self.connected = False

    async def _handle(self, raw) -> None:
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        events = payload if isinstance(payload, list) else [payload]
        for ev in events:
            etype = ev.get("event_type") or ev.get("type")
            token = str(ev.get("asset_id") or ev.get("assetId") or "")
            if not token:
                continue
            self.last_msg_ms = self.clock.now_ms()
            if etype == "book":
                self._books[token] = {
                    "bids": {float(l["price"]): float(l["size"]) for l in ev.get("bids", [])
                             if float(l.get("size", 0)) > 0},
                    "asks": {float(l["price"]): float(l["size"]) for l in ev.get("asks", [])
                             if float(l.get("size", 0)) > 0},
                }
                await self._emit(token)
            elif etype == "price_change":
                book = self._books.setdefault(token, {"bids": {}, "asks": {}})
                for change in ev.get("changes", []):
                    try:
                        price = float(change["price"])
                        size = float(change["size"])
                        side = "bids" if str(change.get("side", "")).upper() == "BUY" else "asks"
                    except (KeyError, TypeError, ValueError):
                        continue
                    if size <= 0:
                        book[side].pop(price, None)
                    else:
                        book[side][price] = size
                await self._emit(token)
            # last_trade_price / tick events are ignored (books drive decisions)

    async def _emit(self, token: str) -> None:
        book = self._books.get(token)
        if book is None:
            return
        snap = OrderbookSnapshot(
            token_id=token,
            bids=sorted((BookLevel(p, s) for p, s in book["bids"].items()),
                        key=lambda l: l.price, reverse=True)[:10],
            asks=sorted((BookLevel(p, s) for p, s in book["asks"].items()),
                        key=lambda l: l.price)[:10],
            ts_ms=self.clock.now_ms(), source="ws")
        await self.on_book(snap)
