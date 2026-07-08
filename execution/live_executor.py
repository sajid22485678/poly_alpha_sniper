"""Execution clients per mode.

ShadowClobClient: real market data, hypothetical fills, ZERO network orders.
Reuses the simulator's fill model so shadow results are directly comparable
to simulation and (later) live via the shadow/live discrepancy tracker.

LiveExecutor: builds the real authenticated client ONLY when the injected
live_gates_checker returns (True, []). There is no other construction path.
"""
from __future__ import annotations

from typing import Callable, Optional

from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.execution.simulator import BookProvider, SimulatedClobClient

log = get_logger("live_executor")


class ShadowClobClient(SimulatedClobClient):
    """What WOULD have happened — no real orders, ever."""

    def __init__(self, clock, book_provider: BookProvider, fill_latency_ms: int = 250):
        # shadow assumes slightly worse latency than simulation: conservative
        super().__init__(clock, book_provider, fill_latency_ms)
        self.mode_label = "shadow_live"


class LiveExecutor:
    def __init__(self, cfg, secrets, clock, order_manager,
                 live_gates_checker: Callable[[], tuple[bool, list[str]]]):
        self.cfg = cfg
        self.secrets = secrets
        self.clock = clock
        self.order_manager = order_manager
        self.live_gates_checker = live_gates_checker
        self._client = None

    def build_client(self):
        ok, failed = self.live_gates_checker()
        if not ok:
            raise RuntimeError("LIVE BLOCKED — gates failed: " + "; ".join(failed))
        from poly_alpha_sniper.connectors.polymarket_clob_private import create_live_client
        self._client = create_live_client(self.cfg, self.secrets, live_gates_ok=True)
        log.info("live_client_constructed", extra={"extra": {"mode": self.cfg.mode.trading_mode}})
        return self._client

    @property
    def client(self):
        if self._client is None:
            raise RuntimeError("live client not built — gates not passed")
        return self._client
