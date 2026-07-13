import pytest

from poly_alpha_sniper.lite_frequency_v4.metrics import (
    build_metrics,
    compound_preview,
    frequency_window,
    performance_metrics,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store
from tests.test_frequency_v4_store import (
    NOW,
    create_entry,
    entry_payload,
    seed_candidate_entry_context,
    seed_market_window,
    seed_session,
)


def test_partial_rolling_interval_never_extrapolates_entries_per_hour(tmp_path):
    store = V4Store(tmp_path / "partial.db")
    try:
        seed_session(store, started=NOW-30*60_000)
        context = seed_market_window(store, open_ts=NOW-5*60_000)
        store.update_window_funnel(
            context["window_id"], NOW-4*60_000,
            positive_edge=1, first_positive_edge_ts_ms=NOW-4*60_000,
            execution_attempts=1, actual_entry=1, entry_ts_ms=NOW-3*60_000,
        )
        metric = frequency_window(store, NOW, hours=1)
        assert metric["actual_entries"] == 1
        assert metric["observed_hours"] == pytest.approx(0.5)
        assert metric["complete_interval"] is False
        assert metric["entries_per_hour"] is None
        assert metric["rate_status"] == "INSUFFICIENT_DURATION_NO_EXTRAPOLATION"
    finally:
        store.close()


def test_session_counts_evidence_from_window_opened_before_runtime_start(tmp_path):
    store = V4Store(tmp_path / "mid-window-session.db")
    try:
        session_start = NOW - 60_000
        seed_session(store, started=session_start)
        context = seed_market_window(store, open_ts=NOW-200_000)
        store.update_window_funnel(
            context["window_id"], NOW-10_000,
            positive_edge=1, first_positive_edge_ts_ms=NOW-30_000,
            execution_attempts=1, actual_entry=1, entry_ts_ms=NOW-20_000,
        )

        metric = frequency_window(
            store, NOW, hours=0, session_id=context["session_id"],
            full_session=True,
        )

        assert metric["from_ts_ms"] == session_start
        assert metric["available_asset_windows"] == 1
        assert metric["positive_edge_windows"] == 1
        assert metric["actual_entries"] == 1
        assert metric["coverage_pct"] == 100.0
        assert metric["entries_per_hour"] is None
    finally:
        store.close()


def test_capacity_coverage_positive_windows_and_bottleneck_are_distinct(tmp_path):
    store = V4Store(tmp_path / "capacity.db")
    try:
        seed_session(store, started=NOW-2*3_600_000)
        contexts = [
            seed_market_window(
                store, asset=asset, open_ts=NOW-300_000, suffix=str(index)
            )
            for index, asset in enumerate(("BTC", "ETH", "SOL"), start=1)
        ]
        store.update_window_funnel(
            contexts[0]["window_id"], NOW-200_000, positive_edge=1,
            first_positive_edge_ts_ms=NOW-250_000, execution_attempts=1,
            actual_entry=1, entry_ts_ms=NOW-200_000,
        )
        store.update_window_funnel(
            contexts[1]["window_id"], NOW-100_000, positive_edge=1,
            first_positive_edge_ts_ms=NOW-200_000, execution_attempts=1,
            missed_opportunity=1, final_blocker="edge_expired",
        )
        store.update_window_funnel(
            contexts[2]["window_id"], NOW-100_000,
            final_blocker="no_positive_fee_net_edge",
        )
        metric = frequency_window(store, NOW, hours=1)
        assert metric["complete_interval"] is True
        assert metric["theoretical_max_trades_per_hour"] == 36
        assert metric["coverage_required_for_19_per_hour_pct"] == pytest.approx(52.777778)
        assert metric["available_asset_windows"] == 3
        assert metric["eligible_windows"] == 3
        assert metric["positive_edge_windows"] == 2
        assert metric["actual_entries"] == 1
        assert metric["missed_opportunities"] == 1
        assert metric["entries_per_hour"] == 1.0
        assert metric["coverage_pct"] == pytest.approx(100/3)
        assert metric["positive_edge_conversion_pct"] == 50.0
        assert metric["bottlenecks"] == {
            "edge_expired": 1, "no_positive_fee_net_edge": 1,
        }
        assert metric["quota_override"] is False
    finally:
        store.close()


def _terminal_trade(store, *, asset, open_ts, suffix, gross_pnl, net_pnl):
    context = seed_market_window(
        store, asset=asset, open_ts=open_ts, suffix=suffix
    )
    evidence = seed_candidate_entry_context(store, context, seq=1)
    entry_id = create_entry(
        store, entry_payload(context, evidence, idem=f"entry-{suffix}")
    )
    position = store.open_positions()[0]
    observed_outcome = "YES" if gross_pnl > 0 else "NO"
    store.record_resolution_attempt({
        "entry_id": entry_id, "attempt_no": 1,
        "attempt_ts_ms": context["close_ts"]+1,
        "source": "POLYMARKET_OFFICIAL", "result": "RESOLVED",
        "observed_outcome": observed_outcome,
        "evidence_hash": f"evidence-{suffix}", "verified": True,
    })
    store.close_position({
        "position_id": position["position_id"],
        "exit_ts_ms": context["close_ts"]+1,
        "exit_source": "OFFICIAL_RESOLUTION",
        "shares": 5.0,
        "payout_usd": max(0.0, gross_pnl+2.45),
        "gross_pnl": gross_pnl,
        "exit_fee": 0.0,
        "net_pnl": net_pnl,
        "evidence_verified": True,
        "resolution_outcome": observed_outcome,
        "reason": "official_resolution",
    })
    return entry_id


def test_performance_breakdowns_fee_net_math_drawdown_and_preview(tmp_path):
    store = V4Store(tmp_path / "performance.db")
    try:
        seed_session(store, started=NOW-10_000_000)
        _terminal_trade(store, asset="BTC", open_ts=NOW-900_000, suffix="btc1",
                        gross_pnl=2.55, net_pnl=2.50)
        _terminal_trade(store, asset="ETH", open_ts=NOW-600_000, suffix="eth1",
                        gross_pnl=-2.45, net_pnl=-2.50)
        _terminal_trade(store, asset="SOL", open_ts=NOW-300_000, suffix="sol1",
                        gross_pnl=2.55, net_pnl=2.50)
        performance = performance_metrics(store)
        verified = performance["verified_terminal"]
        assert verified["count"] == 3
        assert verified["net_pnl"] == pytest.approx(2.5)
        assert verified["fees"] == pytest.approx(0.15)
        assert verified["profit_factor"] == pytest.approx(2.0)
        assert verified["expectancy"] == pytest.approx(2.5/3)
        assert verified["max_drawdown"] == pytest.approx(2.5)
        assert set(performance["by_asset"]) == {"BTC", "ETH", "SOL"}
        assert performance["by_side"]["YES"]["count"] == 3
        assert performance["by_model"]["lead_lag_impulse"]["count"] == 3
        assert performance["by_edge_bucket"][">=0.020"]["count"] == 3
        preview = compound_preview(store, starting_equity_usd=13.0)
        assert preview["fixed_share_equity_usd"] == pytest.approx(15.5)
        assert preview["sample_size"] == 3
        assert preview["risk_of_ruin_estimate"] is None
        assert preview["influences_sizing"] is False
    finally:
        store.close()


def test_build_metrics_separates_funnel_and_keeps_300_trade_gate_closed(tmp_path):
    store = V4Store(tmp_path / "gate.db")
    try:
        seed_session(store, started=NOW-4*3_600_000)
        context = seed_market_window(store, open_ts=NOW-300_000)
        store.update_window_funnel(
            context["window_id"], NOW-200_000, positive_edge=1,
            first_positive_edge_ts_ms=NOW-250_000, execution_attempts=2,
            actual_entry=0, missed_opportunity=1, final_blocker="chase_rejected",
        )
        store.record_reject({
            "session_id": context["session_id"], "window_id": context["window_id"],
            "reject_ts_ms": NOW-100_000, "taxonomy": "EXECUTION",
            "reason": "chase_rejected", "recoverable": False,
        })
        metrics = build_metrics(store, NOW)
        assert metrics["funnel"]["available_asset_windows"] == 1
        assert metrics["funnel"]["unique_positive_edge_opportunities"] == 1
        assert metrics["funnel"]["execution_attempts"] == 2
        assert metrics["funnel"]["actual_entries"] == 0
        assert set(metrics["frequency"]) == {"1h", "3h", "6h", "12h", "session"}
        gate = metrics["acceptance_gate"]
        assert gate["verdict"] == "READY_FOR_MORE_SHADOW"
        assert gate["frequency_target_status"] == "FREQUENCY_TARGET_NOT_YET_PROVEN"
        assert gate["required_verified_terminal_trades"] == 300
        assert "fewer_than_300_verified_terminal_trades" in gate["blockers"]
        assert gate["live_enablement_authorized"] is False
    finally:
        store.close()
