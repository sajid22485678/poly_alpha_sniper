"""select_sizing_detail_source: prefer whichever of risk/validation actually
carries a sizing_detail dict, not just whichever's reject_reason matches --
see core/app.py. Regression for a latent bug where risk.sizing_detail
(position_sizer.py) was silently dropped whenever risk rejected before an
OrderRequest was ever built (validation becomes a synthetic stand-in with an
empty sizing_detail but a copied reject_reason)."""
from poly_alpha_sniper.core.app import select_sizing_detail_source
from poly_alpha_sniper.core.contracts import RejectReason, RiskDecision


def test_prefers_risk_when_risk_carries_the_detail_and_validation_is_synthetic():
    risk = RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, [],
                        sizing_detail={"tier": "B", "tier_cap_pct": 0.10})
    # synthetic stand-in built by core.app when risk.approved=False and no
    # OrderRequest was ever built -- copies reject_reason, not sizing_detail
    validation = RiskDecision(approved=False, reject_reason=risk.reject_reason)
    source = select_sizing_detail_source(risk, validation)
    assert source is risk
    assert source.sizing_detail["tier"] == "B"


def test_prefers_validation_when_it_carries_the_detail_and_risk_does_not():
    risk = RiskDecision(True, 2.60, "", [])
    validation = RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, [],
                              sizing_detail={"tier": "A", "tier_cap_pct": 0.50})
    source = select_sizing_detail_source(risk, validation)
    assert source is validation


def test_returns_none_when_neither_is_a_sizing_reason():
    risk = RiskDecision(False, 0.0, RejectReason.STALE_ORDERBOOK, [])
    validation = RiskDecision(False, 0.0, RejectReason.STALE_ORDERBOOK, [])
    assert select_sizing_detail_source(risk, validation) is None


def test_returns_none_when_reason_matches_but_detail_is_empty():
    """Both objects can claim a sizing reason with no actual detail (e.g. the
    30% total exposure cap, which intentionally carries no sizing_detail) --
    must not surface an empty dict as if it were real diagnostics."""
    risk = RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, [])
    validation = RiskDecision(False, 0.0, RejectReason.MAX_EXPOSURE, [])
    assert select_sizing_detail_source(risk, validation) is None


def test_covers_all_three_sizing_reasons():
    for reason in (RejectReason.MIN_ORDER_SIZE_TOO_HIGH,
                  RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES,
                  RejectReason.MAX_EXPOSURE):
        risk = RiskDecision(False, 0.0, reason, [], sizing_detail={"x": 1})
        validation = RiskDecision(approved=False, reject_reason=reason)
        assert select_sizing_detail_source(risk, validation) is risk
