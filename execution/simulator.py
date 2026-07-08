"""Simulated CLOB client — implements the SAME protocol as the live client.

Fill model (conservative, taker-only):
- marketable limit orders fill by walking the provided book: never a better
  price than the book shows, depth-limited partial fills;
- non-marketable orders rest OPEN (cancellable) and do NOT earn passive fills
  (documented pessimistic assumption: we never assume maker fills we cannot
  verify — backtest and shadow inherit this);
- fills are stamped clock.now + fill_latency_ms.
"""
from __future__ import annotations

from typing import Callable, Optional

from poly_alpha_sniper.core.contracts import (
    OrderbookSnapshot, OrderRecord, OrderRequest, OrderState, Outcome, Position)
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("simulator")

BookProvider = Callable[[str], Optional[OrderbookSnapshot]]


class SimulatedClobClient:
    def __init__(self, clock, book_provider: BookProvider, fill_latency_ms: int = 150):
        self.clock = clock
        self.books = book_provider
        self.fill_latency_ms = fill_latency_ms
        self._balance = 0.0
        self._positions: dict[str, Position] = {}
        self._open_orders: dict[str, OrderRecord] = {}
        self.mode_label = "simulation"

    # ------------------------------------------------------------------
    def set_balance(self, usd: float) -> None:
        self._balance = usd

    # ------------------------------------------------------------------
    async def place_order(self, req: OrderRequest) -> OrderRecord:
        now = self.clock.now_ms()
        record = OrderRecord(
            order_id=req.order_id, exchange_order_id=f"sim-{req.order_id[:8]}",
            token_id=req.token_id, market_id=req.market_id, side=req.side,
            price=req.price, size_shares=req.size_shares, size_usd=req.size_usd,
            state=OrderState.SUBMITTED, created_ts_ms=now, updated_ts_ms=now,
            mode=self.mode_label, tier=req.tier.value, signal_id=req.signal_id,
            exit_reason=req.exit_reason.value if req.exit_reason else "")

        if req.side.is_sell:
            pos = self._positions.get(req.token_id)
            owned = pos.shares if pos else 0.0
            if req.size_shares > owned + 1e-9:
                raise ValueError(f"sim: cannot sell {req.size_shares} shares; own {owned}")

        book = self.books(req.token_id)
        filled_shares, avg_price = self._try_fill(req, book)
        fill_ts = now + self.fill_latency_ms

        if filled_shares > 0:
            self._apply_fill_effects(req, filled_shares, avg_price)
            record.filled_shares = filled_shares
            record.avg_fill_price = avg_price
            record.updated_ts_ms = fill_ts
            if filled_shares >= req.size_shares - 1e-9:
                record.state = OrderState.MATCHED
            else:
                record.state = OrderState.PARTIAL_FILL
                self._open_orders[req.order_id] = record
        else:
            record.state = OrderState.OPEN
            self._open_orders[req.order_id] = record
        return record

    def _try_fill(self, req: OrderRequest, book: Optional[OrderbookSnapshot]
                  ) -> tuple[float, float]:
        """Walk the book against a marketable limit. Returns (shares, avg_price)."""
        if book is None:
            return 0.0, 0.0
        levels = book.asks if req.side.is_buy else book.bids
        remaining = req.size_shares
        cost = 0.0
        filled = 0.0
        for lvl in levels:
            marketable = (lvl.price <= req.price + 1e-9) if req.side.is_buy \
                else (lvl.price >= req.price - 1e-9)
            if not marketable:
                break
            take = min(remaining, lvl.size)
            if take <= 0:
                break
            filled += take
            cost += take * lvl.price
            remaining -= take
            if remaining <= 1e-9:
                break
        if filled <= 0:
            return 0.0, 0.0
        return round(filled, 6), cost / filled

    def _apply_fill_effects(self, req: OrderRequest, shares: float, price: float) -> None:
        if req.side.is_buy:
            cost = shares * price
            self._balance -= cost
            pos = self._positions.get(req.token_id)
            if pos is None:
                self._positions[req.token_id] = Position(
                    token_id=req.token_id, market_id=req.market_id,
                    outcome=req.side.outcome, shares=shares, avg_entry_price=price,
                    entry_ts_ms=self.clock.now_ms())
            else:
                total = pos.shares * pos.avg_entry_price + cost
                pos.shares += shares
                pos.avg_entry_price = total / pos.shares
        else:
            pos = self._positions[req.token_id]
            pos.shares -= shares
            pos.realized_pnl += shares * (price - pos.avg_entry_price)
            self._balance += shares * price
            if pos.shares < 1e-6:
                del self._positions[req.token_id]

    # ------------------------------------------------------------------
    async def cancel_order(self, order_id: str) -> bool:
        rec = self._open_orders.pop(order_id, None)
        if rec is None:
            return False
        rec.state = OrderState.CANCELLED
        rec.updated_ts_ms = self.clock.now_ms()
        return True

    async def cancel_all(self) -> int:
        n = len(self._open_orders)
        for oid in list(self._open_orders):
            await self.cancel_order(oid)
        return n

    async def get_open_orders(self) -> list[OrderRecord]:
        return list(self._open_orders.values())

    async def get_balance_usd(self) -> float:
        return self._balance

    async def get_positions(self) -> list[Position]:
        return list(self._positions.values())
