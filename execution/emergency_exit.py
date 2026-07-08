"""Emergency exit: cancel everything, then market-cross out of every position.

Any failed close is reported and escalates (caller activates panic; panic
re-entry here is guarded by the in_progress flag).
"""
from __future__ import annotations

from typing import Callable, Optional

from poly_alpha_sniper.core.contracts import ExitDecision, ExitReason, Position
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("emergency_exit")


class EmergencyExit:
    def __init__(self, cfg, order_manager, sell_executor, cancel_manager):
        self.cfg = cfg
        self.order_manager = order_manager
        self.sell_executor = sell_executor
        self.cancel_manager = cancel_manager
        self.in_progress = False

    async def close_everything(self, positions: list[Position],
                               book_lookup: Callable, reason: ExitReason) -> dict:
        if self.in_progress:
            return {"skipped": "already in progress"}
        self.in_progress = True
        closed, failed = [], []
        try:
            cancelled = await self.cancel_manager.cancel_all_priority()
            for pos in positions:
                book = book_lookup(pos.token_id)
                decision = ExitDecision.full(reason, "emergency close")
                try:
                    record = await self.sell_executor.execute_exit(pos, decision, book)
                    if record.filled_shares > 0:
                        closed.append(pos.token_id)
                    else:
                        failed.append((pos.token_id, f"no fill (state {record.state.value})"))
                except Exception as exc:  # noqa: BLE001
                    failed.append((pos.token_id, repr(exc)[:150]))
            summary = {"cancelled_orders": cancelled, "closed": closed,
                       "failed": failed, "all_closed": not failed}
            log.warning("emergency_exit_summary", extra={"extra": summary})
            return summary
        finally:
            self.in_progress = False
