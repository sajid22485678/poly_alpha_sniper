"""Fill/order/position/balance reconciliation.

Order submitted != trade successful — this module is the arbiter. Any mismatch
is returned explicitly; settlement_safety escalates to kill switch + panic.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from poly_alpha_sniper.core.contracts import OrderRecord, OrderState, Position

SHARE_TOLERANCE = 0.01
BALANCE_TOLERANCE = 0.05


@dataclass
class ReconcileResult:
    ok: bool
    mismatches: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)


class FillReconciler:
    def reconcile(self, local_orders: list[OrderRecord],
                  exchange_orders: list[OrderRecord],
                  local_positions: list[Position],
                  exchange_positions: list[Position],
                  balance_local: float, balance_exchange: float) -> ReconcileResult:
        mismatches: list[str] = []

        l_open = {o.order_id: o for o in local_orders
                  if o.state in (OrderState.OPEN, OrderState.PARTIAL_FILL,
                                 OrderState.SUBMITTED)}
        e_by_id: dict[str, OrderRecord] = {}
        for o in exchange_orders:
            e_by_id[o.order_id or o.exchange_order_id] = o

        for key, o in e_by_id.items():
            if key not in l_open and o.exchange_order_id not in {
                    lo.exchange_order_id for lo in local_orders if lo.exchange_order_id}:
                mismatches.append(f"unknown exchange order {key}")
        for oid in l_open:
            if oid not in e_by_id and l_open[oid].exchange_order_id not in e_by_id:
                mismatches.append(f"local open order {oid} missing on exchange")

        l_pos = {p.token_id: p.shares for p in local_positions}
        e_pos = {p.token_id: p.shares for p in exchange_positions}
        for token in set(l_pos) | set(e_pos):
            diff = l_pos.get(token, 0.0) - e_pos.get(token, 0.0)
            if abs(diff) > SHARE_TOLERANCE:
                mismatches.append(
                    f"position mismatch on {token}: local {l_pos.get(token, 0.0):.2f} "
                    f"vs exchange {e_pos.get(token, 0.0):.2f}")

        if abs(balance_local - balance_exchange) > BALANCE_TOLERANCE:
            mismatches.append(
                f"balance mismatch: local ${balance_local:.2f} vs exchange ${balance_exchange:.2f}")

        return ReconcileResult(
            ok=not mismatches, mismatches=mismatches,
            details={"local_open_orders": len(l_open),
                     "exchange_open_orders": len(e_by_id),
                     "local_positions": len(l_pos), "exchange_positions": len(e_pos),
                     "balance_local": balance_local, "balance_exchange": balance_exchange})
