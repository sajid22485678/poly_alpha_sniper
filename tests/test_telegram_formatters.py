from poly_alpha_sniper.core.contracts import AggressionMode, Decision, GateResult, Tier
from poly_alpha_sniper.reporting.telegram import format_rejected
from poly_alpha_sniper.tests.helpers import signal


def _clean_approve_gate(score=100.0, tier=Tier.A_PLUS):
    """A gate result for a signal that cleanly passed -- no failed checks, no
    soft penalties. Mirrors what balanced_alpha_gate_results actually showed
    for the real REJECTED_MIN_ORDER_SIZE_TOO_HIGH rows investigated (tier
    A_PLUS/A, score>=90, hard_reject=0, failed_checks='', soft_penalties='')."""
    return GateResult(allow_trade=True, tier=tier, aggression_mode=AggressionMode.NORMAL,
                      decision=Decision.APPROVE, score=score, hard_reject=False,
                      soft_penalties=[], failed_checks=[], reason="tier A_PLUS in NORMAL -> APPROVE")


def _sizing_detail(**overrides):
    d = {"min_shares": 5.0, "ask_price": 0.38, "min_required_usd": 1.90,
        "configured_max_trade_usd": 1.0, "proposed_usd": 1.0,
        "available_cash_usd": 10.0, "shortfall_usd": 0.90}
    d.update(overrides)
    return d


def test_min_order_rejection_does_not_say_edge_confidence():
    """The bug: a signal that already passed edge/confidence (clean APPROVE
    gate, no failed checks) got rejected downstream for min-order sizing, but
    the Telegram message said 'What Must Improve: edge/confidence' -- wrong,
    since edge/confidence were never the blocker."""
    msg = format_rejected(signal(), _clean_approve_gate(), "REJECTED_MIN_ORDER_SIZE_TOO_HIGH",
                          sizing_detail=_sizing_detail())
    assert "edge/confidence" not in msg.lower()


def test_min_order_rejection_shows_full_sizing_breakdown():
    msg = format_rejected(signal(), _clean_approve_gate(), "REJECTED_MIN_ORDER_SIZE_TOO_HIGH",
                          sizing_detail=_sizing_detail())
    assert "Min Required Shares: 5.00" in msg
    assert "Ask Price: 0.3800" in msg
    assert "Min Required USD: $1.90" in msg
    assert "Configured max_trade_usd: $1.00" in msg
    assert "Proposed Size USD: $1.00" in msg
    assert "Available Cash: $10.00" in msg
    assert "Shortfall USD: $0.90" in msg
    assert "Increase max_trade_usd to at least $1.90" in msg
    assert "small-bankroll mode" in msg


def test_non_sizing_rejection_keeps_edge_confidence_language():
    """Every other reject path (weak edge, low quality, etc.) is unaffected --
    only the min-order-size path changes."""
    gate = GateResult(allow_trade=False, tier=Tier.C, aggression_mode=AggressionMode.NORMAL,
                      decision=Decision.REJECT, score=40.0, hard_reject=False,
                      soft_penalties=["edge_below_preferred"], failed_checks=[],
                      reason="tier C: quality too low")
    msg = format_rejected(signal(), gate, "tier C: quality too low")
    assert "What Must Improve: edge_below_preferred" in msg


def test_no_sizing_detail_falls_back_to_gate_based_message():
    gate = GateResult(allow_trade=False, tier=Tier.C, aggression_mode=AggressionMode.NORMAL,
                      decision=Decision.REJECT, score=0.0, hard_reject=True,
                      soft_penalties=[], failed_checks=["book_fresh"], reason="hard reject: book_fresh")
    msg = format_rejected(signal(), gate, "hard reject: book_fresh", sizing_detail=None)
    assert "Failed Checks: book_fresh" in msg


# ---------------------------------------------------------------------------
# WS4: fixed_min_shares insufficient-cash rejection must never say
# "increase max_trade_usd" -- that mode ignores max_trade_usd entirely.
# ---------------------------------------------------------------------------

def _fixed_shares_sizing_detail(**overrides):
    d = {"sizing_mode": "fixed_min_shares", "shares": 5.0, "ask_price": 0.95,
        "required_usd": 4.75, "available_cash_usd": 4.0, "shortfall_usd": 0.75}
    d.update(overrides)
    return d


def test_fixed_min_shares_rejection_never_says_increase_max_trade_usd():
    msg = format_rejected(signal(), _clean_approve_gate(), "REJECTED_INSUFFICIENT_CASH_FOR_5_SHARES",
                          sizing_detail=_fixed_shares_sizing_detail())
    assert "increase max_trade_usd" not in msg.lower()


def test_fixed_min_shares_rejection_excludes_all_forbidden_legacy_phrases():
    msg = format_rejected(signal(), _clean_approve_gate(), "REJECTED_INSUFFICIENT_CASH_FOR_5_SHARES",
                          sizing_detail=_fixed_shares_sizing_detail())
    forbidden = ("Increase max_trade_usd", "small-bankroll mode",
                "Configured max_trade_usd", "Proposed Size USD: $1.00")
    for phrase in forbidden:
        assert phrase not in msg, f"forbidden legacy phrase leaked: {phrase!r}"


def test_fixed_min_shares_rejection_shows_shares_and_cash_breakdown():
    msg = format_rejected(signal(), _clean_approve_gate(), "REJECTED_INSUFFICIENT_CASH_FOR_5_SHARES",
                          sizing_detail=_fixed_shares_sizing_detail())
    assert "Shares: 5.00" in msg
    assert "Ask Price: 0.9500" in msg
    assert "Required USD: $4.75" in msg
    assert "Available Cash: $4.00" in msg
    assert "Shortfall USD: $0.75" in msg
    assert "does not use max_trade_usd" in msg


def test_min_order_rejection_message_unaffected_by_fixed_shares_addition():
    """The pre-existing max_trade_usd-mode message (sizing_detail without
    sizing_mode set) must still work exactly as before."""
    msg = format_rejected(signal(), _clean_approve_gate(), "REJECTED_MIN_ORDER_SIZE_TOO_HIGH",
                          sizing_detail=_sizing_detail())
    assert "Increase max_trade_usd to at least $1.90" in msg
