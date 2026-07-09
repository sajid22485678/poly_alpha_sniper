"""WS5E: loss attribution engine."""
from __future__ import annotations

from poly_alpha_sniper.strategy.loss_attribution import (
    attribute_loss, attribute_skip, build_loss_attribution_summary)


def test_losing_trade_gets_an_attribution_label():
    row = {"reason": "STOP_LOSS", "hold_seconds": 30.0, "pnl_usd": -0.5,
          "market_id": "m1", "detail": "pnl -12% <= -10%"}
    cause = attribute_loss(row)
    assert cause == "cex_lead_failed"
    assert cause != "unattributed"


def test_missing_or_stale_anchor_takes_priority_when_evidenced():
    row = {"reason": "STOP_LOSS", "hold_seconds": 30.0, "pnl_usd": -0.5, "market_id": "m1"}
    oracle_row = {"anchor_quality": "stale", "basis_pct": 0.001}
    assert attribute_loss(row, oracle_row) == "missing_or_stale_oracle_anchor"


def test_basis_unstable_when_evidenced():
    row = {"reason": "STOP_LOSS", "hold_seconds": 30.0, "pnl_usd": -0.5, "market_id": "m1"}
    oracle_row = {"anchor_quality": "good", "basis_pct": 0.05}
    assert attribute_loss(row, oracle_row) == "basis_unstable"


def test_immediate_stop_loss_labeled_entered_too_late():
    row = {"reason": "STOP_LOSS", "hold_seconds": 0.5, "pnl_usd": -0.1, "market_id": "m1"}
    assert attribute_loss(row) == "entered_too_late"


def test_max_hold_labeled_held_too_long():
    row = {"reason": "MAX_HOLD", "hold_seconds": 180.0, "pnl_usd": -0.05, "market_id": "m1"}
    assert attribute_loss(row) == "held_too_long"


def test_unrecognized_shape_falls_back_to_unattributed_not_a_guess():
    row = {"reason": "SOMETHING_NEW", "hold_seconds": 10.0, "pnl_usd": -0.1, "market_id": "m1"}
    assert attribute_loss(row) == "unattributed"


def test_skip_attribution_maps_known_reasons():
    assert attribute_skip("REJECTED_MISSING_ORACLE_ANCHOR") == "missing_oracle_anchor"
    assert attribute_skip("rejected_by_no_shock") == "no_shock"
    assert attribute_skip("rejected_by_no_fresh_cex_price") == "no_fresh_cex_price"
    assert attribute_skip("REJECTED_EV_TOO_LOW") == "ev_too_low"
    assert attribute_skip("REJECTED_MIN_ORDER_SIZE_TOO_HIGH") == "min_order_or_cash"
    assert attribute_skip("REJECTED_INSUFFICIENT_CASH_FOR_5_SHARES") == "min_order_or_cash"
    assert attribute_skip("REJECTED_MAX_EXPOSURE") == "max_exposure"
    assert attribute_skip("REJECTED_TIME_TO_CLOSE_RISK") == "time_to_close_risk"


def test_skip_attribution_unknown_reason_is_other_not_a_guess():
    assert attribute_skip("SOME_BRAND_NEW_REASON") == "other"
    assert attribute_skip("") == "other"


def test_summary_never_fabricates_counts():
    exits = [
        {"reason": "STOP_LOSS", "hold_seconds": 1.0, "pnl_usd": -0.1, "market_id": "m1"},
        {"reason": "TAKE_PROFIT", "hold_seconds": 5.0, "pnl_usd": 0.5, "market_id": "m2"},  # winner, excluded
    ]
    predictions = [
        {"decision": "REJECT", "reject_reason": "rejected_by_no_shock"},
        {"decision": "APPROVE", "reject_reason": ""},  # not a skip, excluded
    ]
    summary = build_loss_attribution_summary(exits, predictions, {}, now_ms=1_000)
    assert summary["losing_trades_analyzed"] == 1
    assert summary["skipped_candidates_analyzed"] == 1
    assert summary["loss_cause_counts"]["entered_too_late"] == 1
    assert summary["skip_cause_counts"]["no_shock"] == 1
    assert sum(summary["loss_cause_counts"].values()) == 1
    assert sum(summary["skip_cause_counts"].values()) == 1
