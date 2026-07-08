"""Bulk/targeted cancels, emergency-first."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import OrderState


class CancelManager:
    def __init__(self, order_manager):
        self.om = order_manager

    async def cancel_all_priority(self) -> int:
        """Cancel every open order; exit-orders' cancels first (frees shares)."""
        open_orders = self.om.open_orders()
        open_orders.sort(key=lambda r: 0 if r.exit_reason else 1)
        n = 0
        for rec in open_orders:
            if await self.om.cancel(rec.order_id, note="cancel_all_priority"):
                n += 1
        return n

    async def cancel_for_market(self, market_id: str) -> int:
        n = 0
        for rec in self.om.open_orders():
            if rec.market_id == market_id:
                if await self.om.cancel(rec.order_id, note="cancel_for_market"):
                    n += 1
        return n

    async def cancel_stale(self, older_than_ms: int, now_ms: int) -> int:
        n = 0
        for rec in self.om.open_orders():
            if rec.state == OrderState.OPEN and now_ms - rec.created_ts_ms > older_than_ms:
                if await self.om.cancel(rec.order_id, note="stale"):
                    n += 1
        return n
