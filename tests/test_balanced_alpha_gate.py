from poly_alpha_sniper.core.contracts import AggressionMode, Decision, Tier, TradingMode
from poly_alpha_sniper.deterministic_intelligence.balanced_alpha_gate import BalancedAlphaGate
from poly_alpha_sniper.tests.helpers import cfg, portfolio_snapshot, signal

ALL_OK = {"mapping_clear": True, "cex_fresh": True, "book_fresh": True,
          "best_bid_ask": True, "spread_ok": True, "no_panic": True,
          "no_kill_switch": True, "frequency_ok": True, "expiry_window_ok": True}


def _gate():
    return BalancedAlphaGate(cfg())


def _eval(sig, aggression=AggressionMode.NORMAL, mode=TradingMode.SHADOW_LIVE,
          checks=None):
    return _gate().evaluate(sig, aggression, mode,
                            dict(checks or ALL_OK), portfolio_snapshot())


def test_hard_reject_always_blocks():
    strong = signal(edge_after_slip=0.15, confidence=95, quality=90)
    checks = dict(ALL_OK, cex_fresh=False)
    g = _eval(strong, checks=checks)
    assert g.hard_reject
    assert g.decision == Decision.REJECT
    assert not g.allow_trade
    assert "cex_fresh" in g.failed_checks
    # even in AGGRESSIVE mode
    g2 = _eval(strong, aggression=AggressionMode.AGGRESSIVE, checks=checks)
    assert g2.hard_reject


def test_a_plus_approved():
    g = _eval(signal(edge_after_slip=0.12, confidence=90, quality=85))
    assert g.tier == Tier.A_PLUS
    assert g.decision == Decision.APPROVE
    assert g.allow_trade


def test_a_approved_in_normal():
    g = _eval(signal(edge_after_slip=0.085, confidence=70, quality=65))
    assert g.tier == Tier.A
    assert g.decision == Decision.APPROVE


def test_a_shadow_only_in_defensive():
    g = _eval(signal(edge_after_slip=0.085, confidence=70, quality=65),
              aggression=AggressionMode.DEFENSIVE)
    assert g.tier == Tier.A
    assert g.decision == Decision.SHADOW_ONLY


def test_b_shadow_only_in_normal():
    g = _eval(signal(edge_after_slip=0.045, confidence=60, quality=55))
    assert g.tier == Tier.B
    assert g.decision == Decision.SHADOW_ONLY
    assert not g.allow_trade


def test_b_approved_in_aggressive():
    g = _eval(signal(edge_after_slip=0.045, confidence=60, quality=55),
              aggression=AggressionMode.AGGRESSIVE)
    assert g.tier == Tier.B
    assert g.decision == Decision.APPROVE


def test_b_rejected_in_defensive():
    g = _eval(signal(edge_after_slip=0.045, confidence=60, quality=55),
              aggression=AggressionMode.DEFENSIVE)
    assert g.decision == Decision.REJECT


def test_c_rejected():
    g = _eval(signal(edge_after_slip=0.01, confidence=30, quality=30))
    assert g.tier == Tier.C
    assert g.decision == Decision.REJECT


def test_soft_penalty_lowers_score_not_reject():
    strong = _eval(signal(edge_after_slip=0.12, confidence=90, quality=85))
    weak = _eval(signal(edge_after_slip=0.085, confidence=68, quality=62, fakeout=0.5))
    assert weak.score < strong.score
    assert weak.soft_penalties
    assert not weak.hard_reject
    assert weak.decision in (Decision.APPROVE, Decision.SHADOW_ONLY)


def test_wait_when_edge_borderline():
    c = cfg()
    floor = max(c.strategy.min_edge_shadow, c.dynamic_edge.hard_min_edge)
    g = _eval(signal(edge_after_slip=floor - 0.003, confidence=30, quality=40))
    assert g.decision == Decision.WAIT


def test_gate_output_shape():
    g = _eval(signal())
    d = g.as_dict()
    for key in ("allow_trade", "tier", "aggression_mode", "decision", "score",
                "hard_reject", "soft_penalties", "failed_checks", "reason"):
        assert key in d
