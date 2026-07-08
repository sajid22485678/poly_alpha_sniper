"""Near misses: signals that ALMOST traded — dashboard intelligence."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import Decision, GateResult, Signal


class NearMissLogger:
    def __init__(self, score_threshold: float = 10.0, edge_threshold: float = 0.01,
                 maxlen: int = 200):
        self.score_threshold = score_threshold
        self.edge_threshold = edge_threshold
        self.rows: list[dict] = []
        self.maxlen = maxlen

    def consider(self, signal: Signal, gate: GateResult) -> dict | None:
        if gate.decision == Decision.APPROVE:
            return None
        near = False
        gaps = []
        if gate.decision in (Decision.SHADOW_ONLY, Decision.WAIT):
            near = True
            gaps.append(f"decision={gate.decision.value}")
        if not gate.hard_reject and gate.score >= 100 - self.score_threshold - 20:
            near = True
            gaps.append(f"score={gate.score:.0f}")
        if not near:
            return None
        row = {"ts_ms": signal.ts_ms, "signal_id": signal.signal_id,
               "market_id": signal.market.market_id, "asset": signal.asset,
               "tier": gate.tier.value, "score": gate.score,
               "edge": signal.edge.edge_after_slippage,
               "decision": gate.decision.value, "gap": "; ".join(gaps)}
        self.rows.append(row)
        if len(self.rows) > self.maxlen:
            self.rows = self.rows[-self.maxlen:]
        return row
