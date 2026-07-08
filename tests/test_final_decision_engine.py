from poly_alpha_sniper.core.contracts import (
    AggressionMode, Decision, GateResult, RiskDecision, Tier, TradingMode)
from poly_alpha_sniper.deterministic_intelligence.final_decision_engine import FinalDecisionEngine
from poly_alpha_sniper.tests.helpers import cfg, signal

OK = RiskDecision(approved=True, size_usd=1.0)
BAD = RiskDecision(approved=False, reject_reason="REJECTED_MAX_EXPOSURE")


def _gate(decision=Decision.APPROVE, hard=False, tier=Tier.A):
    return GateResult(allow_trade=decision == Decision.APPROVE, tier=tier,
                      aggression_mode=AggressionMode.NORMAL, decision=decision,
                      score=80.0, hard_reject=hard)


def _fde():
    return FinalDecisionEngine(cfg())


def test_approve_only_in_live():
    d = _fde().decide(signal(), _gate(), OK, OK, TradingMode.LIVE_MICRO)
    assert d == Decision.APPROVE


def test_shadow_mode_caps_at_shadow_only():
    d = _fde().decide(signal(), _gate(), OK, OK, TradingMode.SHADOW_LIVE)
    assert d == Decision.SHADOW_ONLY
    d2 = _fde().decide(signal(), _gate(), OK, OK, TradingMode.SIMULATION)
    assert d2 == Decision.SHADOW_ONLY


def test_hard_reject_wins():
    d = _fde().decide(signal(), _gate(Decision.REJECT, hard=True), OK, OK,
                      TradingMode.LIVE_MICRO)
    assert d == Decision.REJECT


def test_risk_reject_blocks():
    d = _fde().decide(signal(), _gate(), BAD, OK, TradingMode.LIVE_MICRO)
    assert d == Decision.REJECT


def test_validation_reject_blocks():
    d = _fde().decide(signal(), _gate(), OK, BAD, TradingMode.LIVE_MICRO)
    assert d == Decision.REJECT


def test_wait_passthrough():
    d = _fde().decide(signal(), _gate(Decision.WAIT), OK, OK, TradingMode.LIVE_MICRO)
    assert d == Decision.WAIT


def test_gate_shadow_only_respected_in_live():
    d = _fde().decide(signal(), _gate(Decision.SHADOW_ONLY), OK, OK,
                      TradingMode.LIVE_MICRO)
    assert d == Decision.SHADOW_ONLY


def test_audit_shape():
    fde = _fde()
    g = _gate()
    d = fde.decide(signal(), g, OK, OK, TradingMode.SHADOW_LIVE)
    audit = fde.audit(signal(), g, OK, OK, d)
    assert audit["final"] == "SHADOW_ONLY"
    assert audit["tier"] == "A"
