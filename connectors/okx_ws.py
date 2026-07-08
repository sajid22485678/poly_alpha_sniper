"""OKX public trades feed (optional third confirmation exchange)."""
from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable

import websockets

from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("okx_ws")

OnTick = Callable[[CexTick], Awaitable[None]]

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


def _inst_id(symbol: str) -> str:
    """BTCUSDT -> BTC-USDT."""
    if symbol.upper().endswith("USDT"):
        return symbol.upper()[:-4] + "-USDT"
    return symbol.upper()


class OkxWS:
    exchange = "okx"

    def __init__(self, cfg, clock, on_tick: OnTick):
        self.cfg = cfg
        self.clock = clock
        self.on_tick = on_tick
        self._inst_to_asset = {_inst_id(v): k for k, v in cfg.cex.symbols.items()}
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.connected = False
        self.last_msg_ms = 0
        self._reconnect_until_ms = 0

    @property
    def reconnect_recent(self) -> bool:
        return self.clock.now_ms() < self._reconnect_until_ms

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="okx_ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        backoff = self.cfg.cex.websocket_reconnect_seconds
        while not self._stop.is_set():
            try:
                async with websockets.connect(WS_URL, ping_interval=20,
                                              close_timeout=3) as ws:
                    self.connected = True
                    self._reconnect_until_ms = self.clock.now_ms() + 3000
                    args = [{"channel": "trades", "instId": i} for i in self._inst_to_asset]
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
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
                log.warning("ws_error_reconnecting", extra={"extra": {"error": repr(exc)[:150]}})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        self.connected = False

    async def _handle(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
            if msg.get("arg", {}).get("channel") != "trades":
                return
            now = self.clock.now_ms()
            self.last_msg_ms = now
            for trade in msg.get("data", []):
                asset = self._inst_to_asset.get(str(trade.get("instId", "")))
                if asset is None:
                    continue
                tick = CexTick(asset=asset, exchange=self.exchange,
                               price=float(trade["px"]), ts_ms=int(trade["ts"]),
                               recv_ts_ms=now, volume=float(trade.get("sz", 0)))
                await self.on_tick(tick)
        except (KeyError, ValueError, TypeError) as exc:
            log.debug("parse_error", extra={"extra": {"error": repr(exc)}})
