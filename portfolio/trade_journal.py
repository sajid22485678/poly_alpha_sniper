"""Trade journal: rich, secret-free entry/exit records for DB + Telegram."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import (
    ExitDecision, GateResult, OrderRecord, Position, RiskDecision, Signal)


class TradeJournal:
    def __init__(self):
        self.entries: list[dict] = []

    def journal_entry(self, signal: Signal, gate: GateResult, risk: RiskDecision,
                      order: OrderRecord, mode: str) -> dict:
        row = {
            "ts_ms": order.created_ts_ms, "kind": "ENTRY", "mode": mode,
            "signal_id": signal.signal_id, "market_id": signal.market.market_id,
            "market_title": signal.market.title[:120], "asset": signal.asset,
            "side": order.side.value, "price": order.price,
            "size_usd": order.size_usd, "size_shares": order.size_shares,
            "tier": gate.tier.value, "gate_score": gate.score,
            "edge": signal.edge.edge_after_slippage,
            "confidence": signal.fair.confidence,
            "market_quality": signal.market_quality.score,
            "alpha_score": signal.alpha_score, "exit_plan": signal.exit_plan,
            "order_state": order.state.value,
        }
        self.entries.append(row)
        return row

    def journal_exit(self, position: Position, decision: ExitDecision,
                     order: OrderRecord, pnl: float, ts_ms: int) -> dict:
        row = {
            "ts_ms": ts_ms, "kind": "EXIT",
            "market_id": position.market_id, "token_id": position.token_id,
            "outcome": position.outcome.value,
            "reason": decision.reason.value if decision.reason else "",
            "size_fraction": decision.size_fraction, "price": order.avg_fill_price,
            "shares": order.filled_shares, "pnl_usd": round(pnl, 4),
            "detail": decision.detail, "order_state": order.state.value,
        }
        self.entries.append(row)
        return row
