"""Balanced Alpha Gate — the tiered deterministic trade gate.

Neither too conservative nor reckless:
- HARD rejects always block (any False in hard_checks).
- SOFT weaknesses only subtract score points; they never auto-reject.
- Decision matrix per master spec:
    A_PLUS -> APPROVE (all non-panic aggression modes)
    A      -> APPROVE in NORMAL/AGGRESSIVE, SHADOW_ONLY in DEFENSIVE
    B      -> SHADOW_ONLY in NORMAL, APPROVE in AGGRESSIVE (config gated),
              REJECT in DEFENSIVE
    C      -> REJECT
    WAIT   -> timing not ideal but recoverable (borderline edge / aging book)
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import (
    AggressionMode, Decision, GateResult, PortfolioSnapshot, Signal, Tier, TradingMode)
from poly_alpha_sniper.deterministic_intelligence.opportunity_tier import (
    classify_tier, min_edge_for_mode)

# soft penalty name -> points subtracted from the 100 base score
SOFT_PENALTIES: dict[str, float] = {
    "edge_below_preferred": 10.0,
    "confidence_below_preferred": 8.0,
    "fakeout_risk_moderate": 8.0,
    "market_quality_mediocre": 6.0,
    "entry_slightly_late": 5.0,
    "depth_thin_but_acceptable": 6.0,
    "spread_wide_but_acceptable": 4.0,
}


class BalancedAlphaGate:
    def __init__(self, cfg):
        self.cfg = cfg

    def evaluate(self, signal: Signal, aggression: AggressionMode, mode: TradingMode,
                 hard_checks: dict[str, bool], portfolio: PortfolioSnapshot) -> GateResult:
        failed = sorted(name for name, ok in hard_checks.items() if not ok)
        if failed:
            return GateResult(
                allow_trade=False, tier=Tier.C, aggression_mode=aggression,
                decision=Decision.REJECT, score=0.0, hard_reject=True,
                failed_checks=failed,
                reason="hard reject: " + ", ".join(failed))

        tier = classify_tier(signal.edge, signal.fair.confidence,
                             signal.market_quality.score, self.cfg, mode)
        score, penalties = self._score(signal, mode)

        if tier == Tier.C:
            # WAIT if the edge is borderline (within 0.005 of the mode floor)
            floor = min_edge_for_mode(self.cfg, mode)
            gap = floor - signal.edge.edge_after_slippage
            if 0 < gap <= 0.005:
                return GateResult(False, Tier.C, aggression, Decision.WAIT, score,
                                  soft_penalties=penalties,
                                  reason=f"edge {signal.edge.edge_after_slippage:.4f} within "
                                         f"{gap:.4f} of floor {floor:.3f} — re-check shortly")
            return GateResult(False, Tier.C, aggression, Decision.REJECT, score,
                              soft_penalties=penalties,
                              reason="tier C: quality too low")

        decision = self._decide(tier, aggression)
        allow = decision == Decision.APPROVE
        reason = f"tier {tier.value} in {aggression.value} -> {decision.value}"
        return GateResult(allow_trade=allow, tier=tier, aggression_mode=aggression,
                          decision=decision, score=score, soft_penalties=penalties,
                          reason=reason)

    def _decide(self, tier: Tier, aggression: AggressionMode) -> Decision:
        if tier == Tier.A_PLUS:
            return Decision.APPROVE
        if tier == Tier.A:
            if aggression == AggressionMode.DEFENSIVE:
                return Decision.SHADOW_ONLY
            return Decision.APPROVE
        # tier B
        if aggression == AggressionMode.AGGRESSIVE:
            if (self.cfg.ultra_short_expiry.allow_b_tier_live_when_aggressive
                    and not self.cfg.ultra_short_expiry.only_trade_a_plus_setups):
                return Decision.APPROVE
            return Decision.SHADOW_ONLY
        if aggression == AggressionMode.DEFENSIVE:
            return Decision.REJECT
        return Decision.SHADOW_ONLY  # NORMAL

    def _score(self, signal: Signal, mode: TradingMode) -> tuple[float, list[str]]:
        score = 100.0
        penalties: list[str] = []
        s = self.cfg.strategy

        def hit(name: str) -> None:
            nonlocal score
            penalties.append(name)
            score -= SOFT_PENALTIES[name]

        if signal.edge.edge_after_slippage < s.preferred_edge_live_micro:
            hit("edge_below_preferred")
        if signal.fair.confidence < s.confidence_preferred_live:
            hit("confidence_below_preferred")
        if signal.shock is not None and 0.4 <= signal.shock.fakeout_risk < 0.8:
            hit("fakeout_risk_moderate")
        if signal.market_quality.score < self.cfg.market_quality.min_score_live_full:
            hit("market_quality_mediocre")
        # timing (ms since shock)
        if signal.shock is not None:
            delay_ms = max(0, signal.ts_ms - signal.shock.ts_ms)
            if s.max_entry_delay_ms * 0.5 < delay_ms <= s.max_entry_delay_ms:
                hit("entry_slightly_late")
        return max(0.0, score), penalties
