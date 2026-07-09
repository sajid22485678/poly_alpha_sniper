"""WS5C: point-in-time replay engine (research/read-only).

Replays the bot's OWN recorded shadow-trading history (predictions, exits,
oracle_anchor_log) chronologically, re-scoring each decision with a
challenger formula using ONLY fields already recorded on that same
historical row -- never anything from after that moment.

No-lookahead guarantee, by construction:
- Each `predictions` row is a real point-in-time record: fair_probability,
  edge, market_price, tier, decision, reject_reason were all computed BY
  THE LIVE BOT at that exact ts_ms, using only data it had then. Re-scoring
  a challenger formula against those same stored inputs cannot leak future
  information -- there is no "peek ahead" field being read.
- `resolved_outcome` (final Up/Down result) and any `exits` row for that
  market are used ONLY in the final scoring/comparison step, never fed back
  into the challenger's per-row decision. See `_score_row` -- it takes no
  outcome argument.
- oracle_anchor_log rows are joined to a prediction by market_id AND
  ts_ms <= prediction.ts_ms (the most recent anchor evaluation at or before
  that moment) -- never a later one.

This is a DIFFERENT tool from backtest/replay_engine.py (which replays
synthetic CSV tick data through the live pipeline for parameter-tuning
backtests). This module replays REAL recorded history for champion-vs-
challenger comparison; it makes no live network/DB-write calls.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from poly_alpha_sniper.strategy.oracle_ev import compute_oracle_ev


@dataclass
class ReplayFrame:
    """One point-in-time decision frame -- exactly the fields WS5C's spec
    requires, all sourced from a single historical predictions row (+ the
    most recent oracle_anchor_log row known at that moment, if any)."""
    timestamp: int
    market_id: str
    asset: str
    window_start: Optional[int]
    window_end: Optional[int]
    time_remaining: Optional[float]
    price_to_beat_known_then: Optional[float]
    cex_price_known_then: Optional[float]
    book_price_known_then: Optional[float]
    spread_known_then: Optional[float]
    implied_probability: Optional[float]     # market_price at that moment
    model_probability: Optional[float]       # fair_probability at that moment
    edge: Optional[float]
    champion_decision: str
    champion_reject_reason: str
    final_result: Optional[str] = None       # filled in ONLY by the caller, after the fact, for scoring


def _latest_anchor_at_or_before(anchor_rows_by_market: dict[str, list[dict]],
                                market_id: str, ts_ms: int) -> Optional[dict]:
    rows = anchor_rows_by_market.get(market_id) or []
    candidates = [r for r in rows if (r.get("ts_ms") or 0) <= ts_ms]
    if not candidates:
        return None
    return max(candidates, key=lambda r: r.get("ts_ms") or 0)


def build_frames(prediction_rows: list[dict], oracle_rows: list[dict]) -> list[ReplayFrame]:
    """Pure. Builds one ReplayFrame per prediction row, joined to the most
    recent oracle_anchor_log row known AT OR BEFORE that prediction's
    timestamp -- never a later one."""
    by_market: dict[str, list[dict]] = {}
    for row in oracle_rows:
        by_market.setdefault(row.get("market_id"), []).append(row)

    frames = []
    for pred in prediction_rows:
        ts = int(pred.get("ts_ms") or 0)
        mid = pred.get("market_id")
        anchor = _latest_anchor_at_or_before(by_market, mid, ts)
        frames.append(ReplayFrame(
            timestamp=ts, market_id=mid, asset=pred.get("asset"),
            window_start=anchor.get("window_start_ts_ms") if anchor else None,
            window_end=anchor.get("window_end_ts_ms") if anchor else None,
            time_remaining=anchor.get("time_remaining_seconds") if anchor else None,
            price_to_beat_known_then=anchor.get("price_to_beat") if anchor else None,
            cex_price_known_then=pred.get("cex_price"),
            book_price_known_then=pred.get("polymarket_price"),
            spread_known_then=None,  # not columns on predictions; honestly omitted rather than guessed
            implied_probability=pred.get("polymarket_price"),
            model_probability=pred.get("fair_probability"),
            edge=pred.get("edge_after_slippage") or pred.get("edge"),
            champion_decision=str(pred.get("decision") or ""),
            champion_reject_reason=str(pred.get("reject_reason") or ""),
            final_result=pred.get("resolved_outcome") or None,
        ))
    return frames


def _score_row(frame: ReplayFrame, cfg) -> dict:
    """Challenger (oracle-aware EV) score for ONE frame, using ONLY fields
    already on that frame -- no outcome argument, by design (no-lookahead)."""
    if frame.price_to_beat_known_then is None:
        return {"challenger_decision": "REJECT", "challenger_reason": "REJECTED_MISSING_ORACLE_ANCHOR", "ev": None}
    if frame.model_probability is None or frame.implied_probability is None:
        return {"challenger_decision": "REJECT", "challenger_reason": "REJECTED_DATA_QUALITY", "ev": None}

    result = compute_oracle_ev(
        probability_of_payout=frame.model_probability,
        executable_price=frame.implied_probability,
        fee_rate=cfg.oracle_ev.fee_rate, slippage_buffer=cfg.oracle_ev.slippage_buffer,
        adverse_selection_buffer=cfg.oracle_ev.adverse_selection_buffer)
    if result.ev < cfg.oracle_ev.min_ev_threshold:
        return {"challenger_decision": "REJECT", "challenger_reason": "REJECTED_EV_TOO_LOW", "ev": result.ev}
    return {"challenger_decision": "APPROVE", "challenger_reason": "", "ev": result.ev}


def replay(prediction_rows: list[dict], oracle_rows: list[dict], cfg) -> dict:
    """Champion-vs-challenger comparison over real recorded history.
    Champion = what the bot actually decided (already in the row).
    Challenger = oracle-aware EV re-score of that same row's stored inputs.
    """
    frames = build_frames(prediction_rows, oracle_rows)
    n = len(frames)
    agree = 0
    challenger_would_reject_that_champion_approved = 0
    challenger_would_approve_that_champion_rejected = 0
    missing_anchor_count = 0

    for frame in frames:
        if frame.price_to_beat_known_then is None:
            missing_anchor_count += 1
        score = _score_row(frame, cfg)
        champion_approved = frame.champion_decision in ("APPROVE", "SHADOW_ONLY")
        challenger_approved = score["challenger_decision"] == "APPROVE"
        if champion_approved == challenger_approved:
            agree += 1
        elif champion_approved and not challenger_approved:
            challenger_would_reject_that_champion_approved += 1
        elif challenger_approved and not champion_approved:
            challenger_would_approve_that_champion_rejected += 1

    return {
        "no_lookahead_guarantee": (
            "every challenger score is computed from fields already recorded on that "
            "historical predictions row at decision time, joined only to oracle_anchor_log "
            "rows with ts_ms <= the prediction's own ts_ms -- see module docstring"),
        "frames_analyzed": n,
        "missing_oracle_anchor_count": missing_anchor_count,
        "missing_oracle_anchor_pct": round(100 * missing_anchor_count / n, 1) if n else 0.0,
        "champion_challenger_agree_count": agree,
        "champion_challenger_agree_pct": round(100 * agree / n, 1) if n else 0.0,
        "challenger_stricter_count": challenger_would_reject_that_champion_approved,
        "challenger_looser_count": challenger_would_approve_that_champion_rejected,
        "note": (
            "This compares DECISIONS (approve/reject), not PnL -- with only "
            f"{n} recorded predictions and most markets never having an oracle anchor "
            "logged yet (this logging is new as of this session), a PnL/Brier/calibration "
            "comparison would be statistically meaningless right now. Re-run this once "
            "oracle_anchor_log has accumulated real history from live shadow trading."
            if missing_anchor_count == n or n < 30 else
            "Sample may still be small -- treat comparative PnL/calibration numbers as "
            "directional, not conclusive."),
    }
