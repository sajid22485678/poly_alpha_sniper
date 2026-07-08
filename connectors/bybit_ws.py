"""Bybit v5 public spot trade feed.

IMPORTANT for this deployment: bybit.com can be geo-blocked — on connect
failure the client switches to the bytick.com mirror
(cfg.cex.bybit_ws_fallback_url) and alternates between the two.
"""
from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable

import websockets

from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("bybit_ws")

OnTick = Callable[[CexTick], Awaitable[None]]


class BybitWS:
    exchange = "bybit"

    def __init__(self, cfg, clock, on_tick: OnTick):
        self.cfg = cfg
        self.clock = clock
        self.on_tick = on_tick
        self._symbol_to_asset = {v.upper(): k for k, v in cfg.cex.symbols.items()}
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.connected = False
        self.last_msg_ms = 0
        self._reconnect_until_ms = 0
        self._urls = [cfg.cex.bybit_ws_url, cfg.cex.bybit_ws_fallback_url]
        self._url_idx = 0

    @property
    def reconnect_recent(self) -> bool:
        return self.clock.now_ms() < self._reconnect_until_ms

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="bybit_ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        backoff = self.cfg.cex.websocket_reconnect_seconds
        while not self._stop.is_set():
            url = self._urls[self._url_idx % len(self._urls)]
            try:
                async with websockets.connect(url, ping_interval=20,
                                              close_timeout=3) as ws:
                    self.connected = True
                    self._reconnect_until_ms = self.clock.now_ms() + 3000
                    topics = [f"publicTrade.{v.upper()}" for v in self.cfg.cex.symbols.values()]
                    await ws.send(json.dumps({"op": "subscribe", "args": topics}))
                    log.info("connected", extra={"extra": {"mirror": self._url_idx % 2 == 1}})
                    backoff = self.cfg.cex.websocket_reconnect_seconds
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self._handle(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                self.connected = False
                self._url_idx += 1  # alternate main <-> bytick mirror
                log.warning("ws_error_switching_url", extra={"extra": {
                    "error": repr(exc)[:150], "next_mirror": self._url_idx % 2 == 1}})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        self.connected = False

    async def _handle(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
            if "publicTrade" not in str(msg.get("topic", "")):
                return
            now = self.clock.now_ms()
            self.last_msg_ms = now
            for trade in msg.get("data", []):
                asset = self._symbol_to_asset.get(str(trade.get("s", "")).upper())
                if asset is None:
                    continue
                tick = CexTick(asset=asset, exchange=self.exchange,
                               price=float(trade["p"]), ts_ms=int(trade["T"]),
                               recv_ts_ms=now, volume=float(trade.get("v", 0)))
                await self.on_tick(tick)
        except (KeyError, ValueError, TypeError) as exc:
            log.debug("parse_error", extra={"extra": {"error": repr(exc)}})
