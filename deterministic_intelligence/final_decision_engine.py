"""Final decision: gate x risk x order-validation x mode -> executable verdict.

APPROVE is only possible when EVERYTHING passed AND the mode is live.
Shadow/simulation modes cap out at SHADOW_ONLY (executed against the
shadow/sim client), keeping the decision audit identical across modes.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import Decision, GateResult, RiskDecision, Signal, TradingMode


class FinalDecisionEngine:
    def __init__(self, cfg):
        self.cfg = cfg

    def decide(self, signal: Signal, gate: GateResult, risk: RiskDecision,
               validation: RiskDecision, mode: TradingMode) -> Decision:
        if gate.hard_reject or gate.decision == Decision.REJECT:
            return Decision.REJECT
        if gate.decision == Decision.WAIT:
            return Decision.WAIT
        if not risk.approved or not validation.approved:
            return Decision.REJECT
        if gate.decision == Decision.SHADOW_ONLY:
            return Decision.SHADOW_ONLY
        # gate approved:
        if not mode.is_live:
            return Decision.SHADOW_ONLY
        return Decision.APPROVE

    def audit(self, signal: Signal, gate: GateResult, risk: RiskDecision,
              validation: RiskDecision, decision: Decision) -> dict:
        return {
            "signal_id": signal.signal_id, "market_id": signal.market.market_id,
            "tier": gate.tier.value, "gate_decision": gate.decision.value,
            "gate_score": gate.score, "risk_ok": risk.approved,
            "risk_reason": risk.reject_reason, "validation_ok": validation.approved,
            "validation_reason": validation.reject_reason, "final": decision.value,
        }
