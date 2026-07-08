"""Sell executor: ExitDecision -> priced, validated SELL order."""
from __future__ import annotations

import uuid
from typing import Optional

from poly_alpha_sniper.core.contracts import (
    ExitReason, OrderbookSnapshot, OrderRecord, OrderRequest, OrderSide, Outcome,
    Position, RequestPriority)
from poly_alpha_sniper.core.logger import get_logger
from poly_alpha_sniper.execution.smart_sell_pricer import exit_mode_for_reason, price_exit

log = get_logger("sell_executor")

EMERGENCY_PRIORITY = {ExitReason.EMERGENCY, ExitReason.PANIC, ExitReason.EXPIRY_RISK,
                      ExitReason.STOP_LOSS, ExitReason.KILL_SWITCH,
                      ExitReason.DAILY_LOSS_RISK, ExitReason.MANUAL}


class SellExecutor:
    def __init__(self, cfg, clock, order_manager):
        self.cfg = cfg
        self.clock = clock
        self.order_manager = order_manager

    def build_request(self, position: Position, decision, book: Optional[OrderbookSnapshot]
                      ) -> Optional[OrderRequest]:
        shares = round(position.shares * decision.size_fraction, 2)
        if shares <= 0:
            return None
        mode = exit_mode_for_reason(self.cfg, decision.reason)
        price, note = price_exit(book, self.cfg, mode, decision.reason)
        if price is None:
            return None
        side = OrderSide.SELL_YES if position.outcome == Outcome.YES else OrderSide.SELL_NO
        priority = (RequestPriority.EMERGENCY_EXIT
                    if decision.reason in EMERGENCY_PRIORITY else RequestPriority.NEW_ENTRY)
        return OrderRequest(
            order_id=str(uuid.uuid4()), token_id=position.token_id,
            market_id=position.market_id, side=side, price=price,
            size_shares=shares, size_usd=round(shares * price, 4),
            tif_ms=self.cfg.sell_execution.cancel_if_not_filled_ms,
            priority=priority, reason=f"{note}; {decision.detail}"[:200],
            tier=position.tier, signal_id=position.entry_signal_id,
            exit_reason=decision.reason)

    async def execute_exit(self, position: Position, decision,
                           book: Optional[OrderbookSnapshot]) -> OrderRecord:
        """Submit the exit; if not filled within cancel_if_not_filled_ms, cancel
        and chase one tick toward the bid up to max_chase_ticks (config).
        Emergency-class exits fall back to crossing to the best bid.
        The returned record carries CUMULATIVE filled shares / weighted avg
        price across chase attempts."""
        import uuid as _uuid

        from poly_alpha_sniper.core.contracts import round_to_tick

        req = self.build_request(position, decision, book)
        if req is None:
            raise ValueError("cannot build sell request (no bid or zero size)")
        log.info("exit_order", extra={"extra": {
            "token": position.token_id, "reason": decision.reason.value if decision.reason else "",
            "shares": req.size_shares, "price": req.price}})

        tick = 0.01
        target_shares = req.size_shares
        total_filled = 0.0
        weighted_cost = 0.0
        record = await self.order_manager.submit(req, book)
        total_filled += record.filled_shares
        weighted_cost += record.filled_shares * (record.avg_fill_price or 0.0)

        chases = 0
        max_chases = self.cfg.sell_execution.max_chase_ticks
        floor = self.cfg.sell_execution.min_acceptable_exit_price
        current_price = req.price
        while total_filled < target_shares - 1e-9 and chases < max_chases:
            await self.clock.sleep(self.cfg.sell_execution.cancel_if_not_filled_ms / 1000.0)
            await self.order_manager.cancel(record.order_id, note="chase")
            chases += 1
            new_price = round_to_tick(current_price - tick, tick)
            bid = book.best_bid if book is not None else None
            if bid is not None:
                new_price = max(new_price, min(bid, new_price))
            if new_price < floor:
                break
            current_price = new_price
            remaining = round(target_shares - total_filled, 2)
            if remaining <= 0:
                break
            req = OrderRequest(
                order_id=str(_uuid.uuid4()), token_id=req.token_id,
                market_id=req.market_id, side=req.side, price=current_price,
                size_shares=remaining, size_usd=round(remaining * current_price, 4),
                tif_ms=req.tif_ms, priority=req.priority,
                reason=f"chase {chases}", tier=req.tier,
                signal_id=req.signal_id, exit_reason=req.exit_reason)
            record = await self.order_manager.submit(req, book)
            total_filled += record.filled_shares
            weighted_cost += record.filled_shares * (record.avg_fill_price or 0.0)

        # emergency fallback: cross to the bid for whatever remains
        if (total_filled < target_shares - 1e-9
                and decision.reason in EMERGENCY_PRIORITY
                and book is not None and book.best_bid is not None
                and book.best_bid >= floor):
            await self.order_manager.cancel(record.order_id, note="emergency cross")
            remaining = round(target_shares - total_filled, 2)
            if remaining > 0:
                req = OrderRequest(
                    order_id=str(_uuid.uuid4()), token_id=req.token_id,
                    market_id=req.market_id, side=req.side, price=book.best_bid,
                    size_shares=remaining, size_usd=round(remaining * book.best_bid, 4),
                    tif_ms=req.tif_ms, priority=req.priority,
                    reason="emergency cross to bid", tier=req.tier,
                    signal_id=req.signal_id, exit_reason=req.exit_reason)
                record = await self.order_manager.submit(req, book)
                total_filled += record.filled_shares
                weighted_cost += record.filled_shares * (record.avg_fill_price or 0.0)

        record.filled_shares = round(total_filled, 6)
        record.avg_fill_price = (weighted_cost / total_filled) if total_filled > 0 else 0.0
        return record
