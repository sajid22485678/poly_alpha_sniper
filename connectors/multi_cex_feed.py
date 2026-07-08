"""Fan-in of all CEX feeds into CexState (+ reconnect flags + health)."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.connectors.binance_ws import BinanceWS
from poly_alpha_sniper.connectors.bybit_ws import BybitWS
from poly_alpha_sniper.connectors.okx_ws import OkxWS

log = get_logger("multi_cex_feed")


class MultiCexFeed:
    def __init__(self, cfg, clock, cex_state, enable_okx: bool = True):
        self.cfg = cfg
        self.clock = clock
        self.cex_state = cex_state
        self.feeds = [BinanceWS(cfg, clock, self._on_tick),
                      BybitWS(cfg, clock, self._on_tick)]
        if enable_okx and cfg.cex.optional_exchange == "okx":
            self.feeds.append(OkxWS(cfg, clock, self._on_tick))

    async def _on_tick(self, tick: CexTick) -> None:
        self.cex_state.update(tick)

    async def start(self) -> None:
        for feed in self.feeds:
            await feed.start()
        log.info("feeds_started", extra={"extra": {"n": len(self.feeds)}})

    async def stop(self) -> None:
        for feed in self.feeds:
            await feed.stop()

    def health(self) -> dict:
        now = self.clock.now_ms()
        return {feed.exchange: {
            "connected": feed.connected,
            "staleness_ms": (now - feed.last_msg_ms) if feed.last_msg_ms else -1,
            "reconnect_recent": feed.reconnect_recent,
        } for feed in self.feeds}
