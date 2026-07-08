"""Binance spot aggTrade WebSocket feed.

Streams btcusdt@aggTrade etc. into CexTick callbacks with auto-reconnect.
"""
from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable

import websockets

from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("binance_ws")

OnTick = Callable[[CexTick], Awaitable[None]]

WS_BASE = "wss://stream.binance.com:9443/stream?streams="


class BinanceWS:
    exchange = "binance"

    def __init__(self, cfg, clock, on_tick: OnTick):
        self.cfg = cfg
        self.clock = clock
        self.on_tick = on_tick
        self._symbol_to_asset = {v.lower(): k for k, v in cfg.cex.symbols.items()}
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.connected = False
        self.last_msg_ms = 0
        self._reconnect_until_ms = 0

    @property
    def reconnect_recent(self) -> bool:
        return self.clock.now_ms() < self._reconnect_until_ms

    def _url(self) -> str:
        streams = "/".join(f"{v.lower()}@aggTrade" for v in self.cfg.cex.symbols.values())
        return WS_BASE + streams

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="binance_ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        backoff = self.cfg.cex.websocket_reconnect_seconds
        while not self._stop.is_set():
            try:
                async with websockets.connect(self._url(), ping_interval=20,
                                              close_timeout=3) as ws:
                    self.connected = True
                    self._reconnect_until_ms = self.clock.now_ms() + 3000
                    log.info("connected")
                    backoff = self.cfg.cex.websocket_reconnect_seconds
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self._handle(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                self.connected = False
                log.warning("ws_error_reconnecting", extra={"extra": {
                    "error": repr(exc)[:150], "backoff_s": backoff}})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        self.connected = False

    async def _handle(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
            data = msg.get("data", msg)
            if data.get("e") != "aggTrade":
                return
            symbol = str(data.get("s", "")).lower()
            asset = self._symbol_to_asset.get(symbol)
            if asset is None:
                return
            now = self.clock.now_ms()
            self.last_msg_ms = now
            tick = CexTick(asset=asset, exchange=self.exchange,
                           price=float(data["p"]), ts_ms=int(data["T"]),
                           recv_ts_ms=now, volume=float(data.get("q", 0)))
            await self.on_tick(tick)
        except (KeyError, ValueError, TypeError) as exc:
            log.debug("parse_error", extra={"extra": {"error": repr(exc)}})
