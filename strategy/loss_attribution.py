"""WS5E: loss attribution engine (research/read-only).

Deterministic, rule-based labeling -- no ML, no guessing beyond what the
already-recorded fields support. Every label is derived from real columns
already in `exits`/`predictions`/`shadow_diagnostics`/`oracle_anchor_log`;
nothing here fabricates a cause that isn't evidenced by the row.
"""
from __future__ import annotations

from typing import Optional

LOSS_CAUSES = (
    "wrong_oracle_anchor", "missing_or_stale_oracle_anchor", "cex_lead_failed",
    "oracle_lag_changed", "basis_unstable", "spread_too_wide", "entered_too_late",
    "entered_too_early", "exited_too_early", "held_too_long", "model_overconfident",
    "book_vanished", "adverse_selection", "min_order_forced_bad_sizing",
    "duplicate_or_exposure_block", "insufficient_cash", "data_quality_failure",
    "unattributed",
)

SKIP_CAUSES = (
    "missing_oracle_anchor", "no_fresh_cex_price", "no_shock", "stale_book",
    "spread", "ev_too_low", "model_uncalibrated", "min_order_or_cash",
    "max_exposure", "time_to_close_risk", "other",
)

# reject_reason (either shadow_diagnostics' rejected_by_* or predictions'
# REJECTED_* form) -> canonical skip cause. Checked as a substring match so
# both naming conventions resolve to the same bucket.
_SKIP_REASON_MAP = {
    "MISSING_ORACLE_ANCHOR": "missing_oracle_anchor",
    "ORACLE_ANCHOR_STALE": "missing_oracle_anchor",
    "ORACLE_SOURCE_UNKNOWN": "missing_oracle_anchor",
    "PRICE_TO_BEAT_MISMATCH": "missing_oracle_anchor",
    "no_fresh_cex_price": "no_fresh_cex_price",
    "no_shock": "no_shock",
    "stale_book": "stale_book",
    "SPREAD": "spread",
    "spread": "spread",
    "EV_TOO_LOW": "ev_too_low",
    "MODEL_UNCALIBRATED": "model_uncalibrated",
    "MIN_ORDER": "min_order_or_cash",
    "INSUFFICIENT_CASH": "min_order_or_cash",
    "MAX_EXPOSURE": "max_exposure",
    "TIME_TO_CLOSE_RISK": "time_to_close_risk",
    # not in the user-specified skip taxonomy list -- closest existing
    # bucket: an untrusted anchor has the same practical effect as a
    # missing one (the trade doesn't happen either way).
    "ORACLE_CEX_BASIS_UNSTABLE": "missing_oracle_anchor",
}


def attribute_skip(reject_reason: str) -> str:
    """Pure. Maps a reject_reason string (either naming convention) to the
    canonical skip-cause taxonomy. Unrecognized reasons -> "other", never
    a guessed specific cause."""
    if not reject_reason:
        return "other"
    text = str(reject_reason)
    for needle, cause in _SKIP_REASON_MAP.items():
        if needle.lower() in text.lower():
            return cause
    return "other"


def attribute_loss(exit_row: dict, oracle_row: Optional[dict] = None) -> str:
    """Pure. Labels ONE losing exit row (pnl_usd < 0 assumed by caller).
    oracle_row is the most recent oracle_anchor_log row for that market, if
    any -- gives the oracle-specific causes real evidence to point to
    instead of guessing. Falls back to "unattributed" rather than picking a
    cause with no supporting evidence."""
    reason = str(exit_row.get("reason") or "")
    hold_s = float(exit_row.get("hold_seconds") or 0.0)
    detail = str(exit_row.get("detail") or "").lower()

    if oracle_row:
        quality = oracle_row.get("anchor_quality")
        basis = oracle_row.get("basis_pct")
        if quality in ("missing", "stale"):
            return "missing_or_stale_oracle_anchor"
        if basis is not None and abs(float(basis)) > 0.02:
            return "basis_unstable"

    if reason == "STOP_LOSS":
        if hold_s < 2.0:
            return "entered_too_late"
        if hold_s > 60.0:
            return "held_too_long"
        return "cex_lead_failed"
    if reason == "MAX_HOLD":
        return "held_too_long"
    if reason in ("PARTIAL_TAKE_PROFIT", "TAKE_PROFIT"):
        # a TAKE_PROFIT-reasoned exit landing net-negative implies fees/
        # slippage ate a real (if small) gain -- adverse selection on fill,
        # not a directional miss.
        return "adverse_selection"
    if "no bid" in detail or "no book" in detail:
        return "book_vanished"
    if reason in ("EXPIRY_RISK",):
        return "exited_too_early"
    return "unattributed"


def build_loss_attribution_summary(exit_rows: list[dict], prediction_rows: list[dict],
                                   oracle_rows_by_market: dict[str, dict],
                                   now_ms: int) -> dict:
    """Aggregate counts for both losing trades and skipped candidates.
    oracle_rows_by_market: market_id -> most recent oracle_anchor_log row
    for that market (caller builds this, e.g. one query result reduced by
    market_id -> latest ts_ms)."""
    losses = [r for r in exit_rows if float(r.get("pnl_usd") or 0.0) < 0]
    loss_counts: dict[str, int] = {c: 0 for c in LOSS_CAUSES}
    loss_examples: dict[str, dict] = {}
    for row in losses:
        cause = attribute_loss(row, oracle_rows_by_market.get(row.get("market_id")))
        loss_counts[cause] = loss_counts.get(cause, 0) + 1
        loss_examples.setdefault(cause, row)

    skipped = [r for r in prediction_rows if str(r.get("decision") or "") == "REJECT"]
    skip_counts: dict[str, int] = {c: 0 for c in SKIP_CAUSES}
    for row in skipped:
        cause = attribute_skip(str(row.get("reject_reason") or ""))
        skip_counts[cause] = skip_counts.get(cause, 0) + 1

    return {
        "generated_ts_ms": now_ms,
        "losing_trades_analyzed": len(losses),
        "loss_cause_counts": loss_counts,
        "loss_examples": {k: v for k, v in loss_examples.items()},
        "skipped_candidates_analyzed": len(skipped),
        "skip_cause_counts": skip_counts,
        "note": (
            "Deterministic rule-based labeling only -- every label is derived from "
            "already-recorded fields (exit reason/hold time/oracle anchor quality/"
            "basis, or reject_reason). 'unattributed'/'other' means no rule matched, "
            "not that the cause is unknowable."),
    }
