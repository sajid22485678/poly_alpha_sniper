"""Order manager: lifecycle-tracked submit/cancel with TIF timers and retries.

Every order runs CREATED -> SIGNED -> SUBMITTED before touching the client;
client results map onto OPEN/PARTIAL_FILL/MATCHED; unfilled orders are
cancelled after req.tif_ms. Submit errors follow RetryPolicy (ambiguous
network errors on submits are NOT retried — reconciliation decides).
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Optional

from poly_alpha_sniper.core.contracts import (
    OrderbookSnapshot, OrderRecord, OrderRequest, OrderState)
from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.execution.order_lifecycle import OrderLifecycle
from poly_alpha_sniper.execution.retry_policy import RetryPolicy

log = get_logger("order_manager")

OnUpdate = Callable[[OrderRecord], Awaitable[None]]


class OrderManager:
    def __init__(self, cfg, clock, client, lifecycle: Optional[OrderLifecycle] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 on_update: Optional[OnUpdate] = None):
        self.cfg = cfg
        self.clock = clock
        self.client = client
        self.lifecycle = lifecycle or OrderLifecycle()
        self.retry = retry_policy or RetryPolicy()
        self.on_update = on_update
        self.orders: dict[str, OrderRecord] = {}

    async def _emit(self, record: OrderRecord) -> None:
        if self.on_update is not None:
            try:
                await self.on_update(record)
            except Exception:  # noqa: BLE001
                log.error("on_update_failed")

    async def submit(self, req: OrderRequest, book: Optional[OrderbookSnapshot] = None
                     ) -> OrderRecord:
        now = self.clock.now_ms()
        record = OrderRecord(order_id=req.order_id, token_id=req.token_id,
                             market_id=req.market_id, side=req.side, price=req.price,
                             size_shares=req.size_shares, size_usd=req.size_usd,
                             state=OrderState.CREATED, created_ts_ms=now,
                             updated_ts_ms=now, tier=req.tier.value,
                             signal_id=req.signal_id,
                             exit_reason=req.exit_reason.value if req.exit_reason else "")
        self.orders[req.order_id] = record
        self.lifecycle.transition(record, OrderState.SIGNED, now, "signing")
        self.lifecycle.transition(record, OrderState.SUBMITTED, self.clock.now_ms(), "submitting")
        await self._emit(record)

        attempt = 0
        while True:
            try:
                result = await self.client.place_order(req)
                break
            except ValueError:
                # validation-style failures never retry
                self.lifecycle.transition(record, OrderState.FAILED, self.clock.now_ms(),
                                          "validation error")
                await self._emit(record)
                raise
            except Exception as exc:  # noqa: BLE001
                kind = "rate_limited" if "429" in repr(exc) else "network"
                if self.retry.should_retry("submit", attempt, kind):
                    attempt += 1
                    self.lifecycle.transition(record, OrderState.RETRYING,
                                              self.clock.now_ms(), repr(exc)[:100])
                    await self.clock.sleep(self.retry.backoff_ms(attempt) / 1000.0)
                    self.lifecycle.transition(record, OrderState.SUBMITTED,
                                              self.clock.now_ms(), f"retry {attempt}")
                    continue
                record.error = repr(exc)[:200]
                self.lifecycle.transition(record, OrderState.FAILED, self.clock.now_ms(),
                                          "submit failed (no blind retry)")
                await self._emit(record)
                raise

        # adopt client result
        record.exchange_order_id = result.exchange_order_id
        record.filled_shares = result.filled_shares
        record.avg_fill_price = result.avg_fill_price
        if result.state == OrderState.MATCHED:
            self.lifecycle.transition(record, OrderState.MATCHED, self.clock.now_ms(), "full fill")
        elif result.state == OrderState.PARTIAL_FILL:
            self.lifecycle.transition(record, OrderState.PARTIAL_FILL, self.clock.now_ms(),
                                      f"filled {result.filled_shares}")
            self._schedule_tif_cancel(record, req.tif_ms)
        elif result.state == OrderState.OPEN:
            self.lifecycle.transition(record, OrderState.OPEN, self.clock.now_ms(), "resting")
            self._schedule_tif_cancel(record, req.tif_ms)
        else:
            record.error = result.error
            self.lifecycle.transition(record, OrderState.FAILED, self.clock.now_ms(),
                                      result.error[:100] if result.error else "client failed")
        await self._emit(record)
        return record

    def _schedule_tif_cancel(self, record: OrderRecord, tif_ms: int) -> None:
        if tif_ms <= 0:
            return

        async def _cancel_later():
            await self.clock.sleep(tif_ms / 1000.0)
            live = self.orders.get(record.order_id)
            if live is not None and live.state in (OrderState.OPEN, OrderState.PARTIAL_FILL):
                await self.cancel(record.order_id, note="tif expired")

        try:
            asyncio.get_running_loop().create_task(_cancel_later())
        except RuntimeError:
            pass  # no loop (sync backtest step) — backtest cancels explicitly

    async def cancel(self, order_id: str, note: str = "") -> bool:
        record = self.orders.get(order_id)
        if record is None or record.state.is_terminal:
            return False
        try:
            self.lifecycle.transition(record, OrderState.CANCEL_REQUESTED,
                                      self.clock.now_ms(), note)
        except Exception:  # already moved on (e.g. matched in flight)
            return False
        attempt = 0
        while True:
            try:
                ok = await self.client.cancel_order(order_id)
                break
            except Exception as exc:  # noqa: BLE001
                if self.retry.should_retry("cancel", attempt):
                    attempt += 1
                    await self.clock.sleep(self.retry.backoff_ms(attempt) / 1000.0)
                    continue
                record.error = repr(exc)[:200]
                self.lifecycle.transition(record, OrderState.FAILED, self.clock.now_ms(),
                                          "cancel failed")
                await self._emit(record)
                return False
        state = OrderState.CANCELLED if ok else OrderState.FAILED
        self.lifecycle.transition(record, state, self.clock.now_ms(),
                                  "cancelled" if ok else "cancel rejected")
        await self._emit(record)
        return ok

    async def cancel_all_open(self) -> int:
        n = 0
        for oid, rec in list(self.orders.items()):
            if rec.state in (OrderState.OPEN, OrderState.PARTIAL_FILL):
                if await self.cancel(oid, note="cancel_all"):
                    n += 1
        return n

    def open_orders(self) -> list[OrderRecord]:
        return [r for r in self.orders.values()
                if r.state in (OrderState.OPEN, OrderState.PARTIAL_FILL,
                               OrderState.SUBMITTED)]
