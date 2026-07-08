"""Order lifecycle state machine.

Submitted != successful: only RECONCILED is trusted. Illegal transitions raise
IllegalTransition — silent state corruption is impossible.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import OrderRecord, OrderState

S = OrderState

LEGAL_TRANSITIONS: dict[OrderState, set[OrderState]] = {
    S.CREATED: {S.SIGNED, S.FAILED},
    S.SIGNED: {S.SUBMITTED, S.FAILED},
    S.SUBMITTED: {S.OPEN, S.PARTIAL_FILL, S.MATCHED, S.RETRYING, S.FAILED,
                  S.CANCEL_REQUESTED},
    S.OPEN: {S.PARTIAL_FILL, S.MATCHED, S.CANCEL_REQUESTED, S.FAILED},
    S.PARTIAL_FILL: {S.PARTIAL_FILL, S.MATCHED, S.CANCEL_REQUESTED, S.CANCELLED, S.FAILED},
    S.MATCHED: {S.MINED, S.CONFIRMED, S.FAILED},
    S.MINED: {S.CONFIRMED, S.FAILED},
    S.CONFIRMED: {S.RECONCILED, S.FAILED},
    S.RETRYING: {S.SUBMITTED, S.FAILED},
    S.CANCEL_REQUESTED: {S.CANCELLED, S.MATCHED, S.PARTIAL_FILL, S.FAILED},
    S.CANCELLED: set(),
    S.FAILED: set(),
    S.RECONCILED: set(),
}


class IllegalTransition(RuntimeError):
    pass


class OrderLifecycle:
    def __init__(self):
        self.history: list[tuple[str, str, str, int, str]] = []

    def transition(self, order: OrderRecord, new_state: OrderState,
                   now_ms: int, note: str = "") -> OrderRecord:
        allowed = LEGAL_TRANSITIONS.get(order.state, set())
        if new_state not in allowed:
            raise IllegalTransition(
                f"order {order.order_id}: {order.state.value} -> {new_state.value} not legal "
                f"(allowed: {sorted(s.value for s in allowed)})")
        self.history.append((order.order_id, order.state.value, new_state.value,
                             now_ms, note))
        order.state = new_state
        order.updated_ts_ms = now_ms
        return order

    def history_for(self, order_id: str) -> list[tuple[str, str, str, int, str]]:
        return [h for h in self.history if h[0] == order_id]
