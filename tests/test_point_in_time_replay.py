"""WS5C: point-in-time replay engine -- no-lookahead is the core guarantee."""
from __future__ import annotations

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.backtest.point_in_time_replay import build_frames, replay


def _prediction(ts_ms, market_id="m1", asset="BTC", fair=0.70, price=0.60,
                decision="APPROVE", reject_reason=""):
    return {"ts_ms": ts_ms, "market_id": market_id, "asset": asset,
           "fair_probability": fair, "polymarket_price": price, "cex_price": 100_000.0,
           "edge_after_slippage": fair - price, "decision": decision, "reject_reason": reject_reason}


def _oracle_row(ts_ms, market_id="m1", price_to_beat=100_000.0, window_end=None):
    return {"ts_ms": ts_ms, "market_id": market_id, "price_to_beat": price_to_beat,
           "window_start_ts_ms": ts_ms - 10_000, "window_end_ts_ms": window_end or ts_ms + 290_000,
           "time_remaining_seconds": 250.0, "anchor_quality": "good"}


def test_frame_only_uses_anchor_at_or_before_its_own_timestamp():
    """The critical no-lookahead property: a prediction at t=1000 must never
    see an oracle_anchor_log row recorded at t=2000, even though that later
    row exists in the same query result set."""
    pred = _prediction(ts_ms=1000)
    early_anchor = _oracle_row(ts_ms=500, price_to_beat=99_000.0)
    late_anchor = _oracle_row(ts_ms=2000, price_to_beat=105_000.0)  # would leak if lookahead existed

    frames = build_frames([pred], [early_anchor, late_anchor])

    assert len(frames) == 1
    assert frames[0].price_to_beat_known_then == 99_000.0  # the EARLY one, never the late one


def test_frame_finds_no_anchor_when_only_later_ones_exist():
    pred = _prediction(ts_ms=1000)
    only_later = _oracle_row(ts_ms=5000, price_to_beat=99_000.0)

    frames = build_frames([pred], [only_later])

    assert frames[0].price_to_beat_known_then is None


def test_final_result_is_never_read_by_the_scoring_function():
    """resolved_outcome must only ever be attached to the frame for a LATER
    caller-driven scoring step -- the replay() aggregate itself never
    branches on it while producing champion/challenger agreement stats."""
    pred = _prediction(ts_ms=1000)
    pred["resolved_outcome"] = "UP"  # present, but must not influence the frame's own decision fields
    anchor = _oracle_row(ts_ms=900)

    frames = build_frames([pred], [anchor])

    assert frames[0].final_result == "UP"
    assert frames[0].champion_decision == "APPROVE"  # unaffected by final_result


def test_replay_reports_missing_anchor_honestly():
    preds = [_prediction(ts_ms=i * 1000) for i in range(5)]
    result = replay(preds, [], load_config())
    assert result["missing_oracle_anchor_count"] == 5
    assert result["frames_analyzed"] == 5
    assert "no_lookahead_guarantee" in result


def test_replay_agreement_counts_are_internally_consistent():
    preds = [_prediction(ts_ms=i * 1000, decision="APPROVE") for i in range(5)]
    anchors = [_oracle_row(ts_ms=i * 1000 - 100) for i in range(5)]
    result = replay(preds, anchors, load_config())
    total = (result["champion_challenger_agree_count"]
            + result["challenger_stricter_count"]
            + result["challenger_looser_count"])
    assert total == result["frames_analyzed"]
