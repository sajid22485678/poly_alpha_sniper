import pytest

from poly_alpha_sniper.dashboard import metrics


EXITS = [{"ts_ms": 1_000_000 + i * 60_000, "pnl_usd": p, "price": 0.6, "shares": 1.6,
          "tier": t}
         for i, (p, t) in enumerate([(0.12, "A_PLUS"), (-0.09, "A"), (0.05, "A"),
                                     (0.10, "B"), (-0.06, "B"), (0.15, "A_PLUS")])]
PNL = [{"ts_ms": 1_000_000 + i * 60_000, "realized_pnl_usd": e["pnl_usd"]}
       for i, e in enumerate(EXITS)]
PREDS = [{"ts_ms": 1_000_000 + i * 30_000, "edge_after_slippage": 0.06,
          "tier": ["A_PLUS", "A", "B", "C"][i % 4], "decision": "REJECT" if i % 3 == 0 else "SHADOW_ONLY",
          "reject_reason": "REJECTED_SPREAD_TOO_WIDE" if i % 3 == 0 else "",
          "aggression_mode": "NORMAL"} for i in range(12)]


def test_winrate():
    assert metrics.winrate(EXITS) == pytest.approx(4 / 6, abs=1e-3)


def test_profit_factor():
    pf = metrics.profit_factor(EXITS)
    assert pf == pytest.approx((0.12 + 0.05 + 0.10 + 0.15) / (0.09 + 0.06), rel=1e-3)


def test_expectancy():
    assert metrics.expectancy(EXITS) == pytest.approx(sum(e["pnl_usd"] for e in EXITS) / 6,
                                                      abs=1e-4)


def test_equity_curve_and_roi():
    curve = metrics.equity_curve(PNL, starting=10.0)
    assert curve[-1]["equity"] == pytest.approx(10.27)
    assert metrics.compounded_roi(PNL, 10.0) == pytest.approx(2.7, abs=0.01)


def test_max_drawdown():
    dd = metrics.max_drawdown(PNL, 10.0)
    assert dd["usd"] >= 0.09  # the -0.09 dip


def test_empty_rows_no_crash():
    assert metrics.winrate([]) == 0.0
    assert metrics.profit_factor([]) == 0.0
    assert metrics.expectancy([]) == 0.0
    assert metrics.equity_curve([], 10.0) == []
    assert metrics.max_drawdown([], 10.0) == {"usd": 0.0, "pct": 0.0}
    assert metrics.avg_edge([]) == 0.0
    assert metrics.tier_distribution([]) == {}


def test_tier_distribution():
    dist = metrics.tier_distribution(PREDS)
    assert dist["A_PLUS"] == 3
    assert dist["C"] == 3


def test_pnl_by_tier():
    by_tier = metrics.pnl_by_tier(EXITS)
    assert by_tier["A_PLUS"] == pytest.approx(0.27)
    assert by_tier["B"] == pytest.approx(0.04)


def test_b_allowed_vs_skipped():
    gate_rows = [{"tier": "B", "decision": "SHADOW_ONLY"},
                 {"tier": "B", "decision": "APPROVE"},
                 {"tier": "B", "decision": "REJECT"},
                 {"tier": "A", "decision": "APPROVE"}]
    out = metrics.b_allowed_vs_skipped(gate_rows)
    assert out == {"total_b": 3, "approved": 1, "shadow_only": 1, "rejected": 1}


def test_reject_breakdown():
    out = metrics.reject_breakdown(PREDS)
    assert out.get("REJECTED_SPREAD_TOO_WIDE") == 4


def test_frequency_warnings():
    over = metrics.frequency_vs_target(EXITS, target_per_hour=1, max_per_hour=2)
    assert over["warning"] == "OVERTRADING"
    calm = metrics.frequency_vs_target([], 5, 12)
    assert calm["warning"] == ""


def test_open_positions_split():
    rows = [{"outcome": "YES"}, {"outcome": "NO"}, {"outcome": "YES"}]
    yes, no = metrics.open_positions_split(rows)
    assert len(yes) == 2 and len(no) == 1


def test_min_order_sizing_rows_computes_shortfall_from_existing_columns():
    preds = [
        {"ts_ms": 2000, "asset": "ETH", "market_title": "ETH test", "tier": "A_PLUS",
         "edge_after_slippage": 0.35, "confidence": 95.0, "polymarket_price": 0.38,
         "decision": "REJECT", "reject_reason": "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"},
        {"ts_ms": 1000, "asset": "SOL", "market_title": "SOL test", "tier": "A",
         "edge_after_slippage": 0.66, "confidence": 95.0, "polymarket_price": 0.25,
         "decision": "REJECT", "reject_reason": "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"},
        {"ts_ms": 1500, "asset": "BTC", "market_title": "BTC test", "tier": "C",
         "edge_after_slippage": 0.01, "confidence": 60.0, "polymarket_price": 0.7,
         "decision": "REJECT", "reject_reason": "hard reject: book_fresh"},
    ]
    rows = metrics.min_order_sizing_rows(preds, max_trade_usd=1.0)
    assert len(rows) == 2  # the book_fresh reject is excluded
    assert rows[0]["asset"] == "ETH"  # sorted newest first
    assert rows[0]["min_required_usd"] == pytest.approx(1.90)   # 5 * 0.38
    assert rows[0]["shortfall_usd"] == pytest.approx(0.90)      # 1.90 - 1.0
    assert rows[1]["min_required_usd"] == pytest.approx(1.25)   # 5 * 0.25
    assert rows[1]["shortfall_usd"] == pytest.approx(0.25)      # 1.25 - 1.0


def test_min_order_sizing_rows_empty_when_no_rejects():
    assert metrics.min_order_sizing_rows([], max_trade_usd=1.0) == []


# ---------------------------------------------------------------------------
# Premium dashboard additions
# ---------------------------------------------------------------------------

def test_today_pnl_only_sums_current_utc_day():
    now = 1_800_000_000_000  # arbitrary fixed "now" for determinism
    day_start = now - (now % 86_400_000)
    rows = [
        {"ts_ms": day_start - 1000, "realized_pnl_usd": 5.0},   # yesterday: excluded
        {"ts_ms": day_start + 1000, "realized_pnl_usd": 0.5},   # today
        {"ts_ms": now, "realized_pnl_usd": 0.25},               # today
    ]
    assert metrics.today_pnl(rows, now_ms=now) == pytest.approx(0.75)


def test_today_pnl_empty_rows():
    assert metrics.today_pnl([], now_ms=1_800_000_000_000) == 0.0


def test_all_time_pnl_sums_everything():
    rows = [{"realized_pnl_usd": 1.0}, {"realized_pnl_usd": -0.3}, {"realized_pnl_usd": 0.2}]
    assert metrics.all_time_pnl(rows) == pytest.approx(0.9)


def test_unified_reject_breakdown_maps_both_stages_to_canonical_buckets():
    diag_rows = [
        {"reason": "rejected_by_no_shock"}, {"reason": "rejected_by_no_shock"},
        {"reason": "rejected_by_no_fresh_cex_price"},
        {"reason": "rejected_by_stale_book"},
    ]
    preds = [
        {"decision": "REJECT", "reject_reason": "REJECTED_MAX_EXPOSURE"},
        {"decision": "REJECT", "reject_reason": "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"},
        {"decision": "REJECT", "reject_reason": "hard reject: spread_ok"},
        {"decision": "REJECT", "reject_reason": "hard reject: book_fresh"},
        {"decision": "SHADOW_ONLY", "reject_reason": ""},  # not a reject -- excluded
    ]
    out = metrics.unified_reject_breakdown(diag_rows, preds)
    b = out["buckets"]
    assert b["no_shock"] == 2
    assert b["no_fresh_cex_price"] == 1
    assert b["stale_book"] == 2          # 1 from diag + 1 from "hard reject: book_fresh"
    assert b["max_exposure"] == 1
    assert b["min_order"] == 1
    assert b["spread"] == 1
    assert out["total"] == 8


def test_unified_reject_breakdown_never_drops_unmapped_reasons():
    diag_rows = [{"reason": "rejected_by_price_window_not_ready"}]
    out = metrics.unified_reject_breakdown(diag_rows, [])
    assert out["buckets"]["other"] == 1
    assert out["total"] == 1


def test_unified_reject_breakdown_empty_inputs():
    out = metrics.unified_reject_breakdown([], [])
    assert out["total"] == 0
    assert all(v == 0 for v in out["buckets"].values())


def test_latest_market_state_unavailable_when_no_predictions():
    assert metrics.latest_market_state([]) == {
        "available": False, "snapshot_kind": "historical_last_signal"}


def test_latest_market_state_staleness_labeling():
    """The Signal Engine snapshot is HISTORICAL (last predictions row, which
    can be hours old). With now_ms it must self-describe its age and label
    itself stale past 10 min so an old 'hard reject: book_fresh' can never be
    read as the current blocker."""
    preds = [{"ts_ms": 1_000_000, "asset": "BTC", "decision": "REJECT",
              "reject_reason": "hard reject: book_fresh"}]
    fresh = metrics.latest_market_state(preds, now_ms=1_000_000 + 60_000)
    assert fresh["snapshot_kind"] == "historical_last_signal"
    assert fresh["age_ms"] == 60_000
    assert fresh["is_stale"] is False
    assert fresh["staleness_label"] is None

    stale = metrics.latest_market_state(preds, now_ms=1_000_000 + 3 * 3600_000)
    assert stale["is_stale"] is True
    assert stale["staleness_label"] == metrics.STALE_SIGNAL_LABEL
    # without now_ms (legacy v1/v2 dashboard callers) nothing breaks
    legacy = metrics.latest_market_state(preds)
    assert legacy["age_ms"] is None and legacy["is_stale"] is False


def test_latest_market_state_uses_most_recent_row_and_diag():
    preds = [
        {"ts_ms": 1000, "asset": "BTC", "market_title": "old", "decision": "REJECT"},
        {"ts_ms": 5000, "asset": "ETH", "market_title": "ETH Up or Down",
         "direction": "UP", "polymarket_price": 0.42, "fair_probability": 0.55,
         "edge_after_slippage": 0.13, "confidence": 90, "tier": "A",
         "decision": "APPROVE", "reject_reason": ""},
    ]
    diag = {"cex_selected_source": {"ETH": "okx"}, "cex_freshest_age_ms": {"ETH": 120},
           "latest_prices": {"ETH": 1730.5}, "fresh_books": 18, "total_books": 30,
           "last_block_reason": "SOL:rejected_by_no_shock"}
    out = metrics.latest_market_state(preds, diag)
    assert out["available"] is True
    assert out["asset"] == "ETH"  # picked ts_ms=5000, not the older BTC row
    assert out["decision_label"] == "ENTER"
    assert out["cex_selected_source"] == "okx"
    assert out["cex_price"] == pytest.approx(1730.5)


def test_latest_market_state_decision_labels():
    base = {"ts_ms": 1, "asset": "BTC"}
    assert metrics.latest_market_state([{**base, "decision": "REJECT"}])["decision_label"] == "SKIP"
    assert metrics.latest_market_state([{**base, "decision": "WAIT"}])["decision_label"] == "WAIT"
    assert metrics.latest_market_state([{**base, "decision": "SHADOW_ONLY"}])["decision_label"] == "ENTER (shadow)"


def test_min_order_summary_counts_and_latest():
    preds = [
        {"ts_ms": 1000, "asset": "ETH", "polymarket_price": 0.38,
         "decision": "REJECT", "reject_reason": "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"},
        {"ts_ms": 2000, "asset": "SOL", "polymarket_price": 0.25,
         "decision": "REJECT", "reject_reason": "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"},
    ]
    out = metrics.min_order_summary(preds, max_trade_usd=1.0)
    assert out["blocked_count"] == 2
    assert out["latest"]["asset"] == "SOL"  # newest ts_ms


def test_min_order_summary_empty_when_none_blocked():
    out = metrics.min_order_summary([], max_trade_usd=1.0)
    assert out == {"blocked_count": 0, "latest": None}
