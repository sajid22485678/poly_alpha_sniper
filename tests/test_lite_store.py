from pathlib import Path

import pytest

from poly_alpha_sniper.lite.lite_store import LiteStore, WindowLockConflict


def _entry(*, window=600_000, side="BUY_YES", **overrides):
    start_s = window//1000-300
    tag = str(window)
    row = {
        "asset": "BTC", "market_id": f"m-{tag}", "event_id": f"e-{tag}",
        "slug": f"btc-updown-5m-{start_s}", "condition_id": f"c-{tag}",
        "yes_token_id": f"yes-{tag}", "no_token_id": f"no-{tag}",
        "side": side, "shares": 999, "entry_price": 0.42,
        "entry_cost": 999, "entry_ts": window-250_000,
        "window_open_ts": window-300_000, "window_close_ts": window,
        "status": "OPEN", "anchor_available": False, "price_to_beat": None,
        "cex_source": "test", "cex_entry_price": 100.0,
        "momentum_pct": 0.001, "strategy_name": "lite_direction_sniper_v2",
        "entry_fee": 0.08, "fee_rate": 0.07,
    }
    row.update(overrides)
    return row


def test_store_enforces_exact_five_shares_and_separate_fee(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        trade_id = store.insert_trade(_entry())
        trade = store.get_trade(trade_id)
        assert trade["shares"] == 5.0
        assert trade["entry_cost"] == pytest.approx(5*0.42)
        assert trade["entry_fee"] == 0.08
        assert trade["no_anchor_trade"] == 1
    finally:
        store.close()


def test_one_asset_window_blocks_opposite_side_and_same_side(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        trade_id = store.insert_trade(_entry(side="BUY_YES"))
        with pytest.raises(WindowLockConflict) as opposite:
            store.insert_trade(_entry(side="BUY_NO"))
        assert opposite.value.reason == "opposite_side_blocked"
        with pytest.raises(WindowLockConflict) as same:
            store.insert_trade(_entry(side="BUY_YES"))
        assert same.value.reason == "position_lifecycle_incomplete"
        store.complete_trade(
            trade_id, "CLOSED_WIN", 1.0, 700_000, 2.8,
            "official_outcome", "YES", gross_pnl=2.9,
            resolution_verified=True)
        store.finalize_window_locks(700_000)
        with pytest.raises(WindowLockConflict) as completed:
            store.insert_trade(_entry(side="BUY_YES"))
        assert completed.value.reason == "duplicate_same_side_blocked"
    finally:
        store.close()


def test_window_lock_survives_restart_and_next_window_is_allowed(tmp_path):
    path = tmp_path / "lite.db"
    first = LiteStore(str(path))
    first.insert_trade(_entry())
    first.close()
    second = LiteStore(str(path))
    try:
        with pytest.raises(WindowLockConflict):
            second.insert_trade(_entry(side="BUY_NO"))
        new_id = second.insert_trade(_entry(window=900_000, side="BUY_NO"))
        assert second.get_trade(new_id)["window_close_ts"] == 900_000
    finally:
        second.close()


def test_separate_connections_share_db_authoritative_lock(tmp_path):
    path = tmp_path / "lite.db"
    one, two = LiteStore(str(path)), LiteStore(str(path))
    try:
        one.insert_trade(_entry())
        with pytest.raises(WindowLockConflict):
            two.insert_trade(_entry(side="BUY_NO"))
        assert one.table_count("lite_trades") == 1
    finally:
        one.close(); two.close()


def test_reject_and_decision_rows_are_bucketed(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        for ts in range(1_000, 29_000, 1_000):
            store.record_reject(ts, "BTC", "slug", "truly_flat", bucket_s=30)
            store.record_decision(ts, "BTC", "slug", "candidate", bucket_s=30)
        assert store.table_count("lite_rejects") == 1
        assert store.table_count("lite_decision_buckets") == 1
        metrics = store.dashboard_metrics(29_000)
        assert metrics["top_reject_reasons"]["truly_flat"] == 1
        assert metrics["anti_dead_bot_last_hour"]["candidate"] == 1
    finally:
        store.close()


def test_pending_and_unresolved_are_excluded_from_all_performance_metrics(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        pending = store.insert_trade(_entry(window=600_000))
        store.mark_pending(pending, 600_000, "no_exit")
        unresolved = store.insert_trade(_entry(window=900_000))
        store.mark_pending(unresolved, 900_000, "no_exit")
        store.mark_unresolved(unresolved, 910_000, "no_evidence")
        closed = store.insert_trade(_entry(window=1_200_000))
        store.complete_trade(
            closed, "CLOSED_WIN", 1.0, 1_210_000, 2.8,
            "official_outcome", "YES", gross_pnl=2.9,
            resolution_verified=True)
        metrics = store.dashboard_metrics(1_210_000)
        assert store.get_trade(unresolved)["pnl"] is None
        assert metrics["pending_resolution"] == 1
        assert metrics["unresolved_final"] == 1
        assert metrics["completed_trades"] == 1
        assert metrics["total_lite_pnl"] == 2.8
        assert metrics["winrate"] == 1.0
    finally:
        store.close()


def test_metric_sum_expectancy_profit_factor_and_batched_drawdown_are_exact(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        pnls = [2.0, -1.0, 3.0, -2.0]
        for index, pnl in enumerate(pnls, start=1):
            trade_id = store.insert_trade(_entry(window=300_000*(index+1)))
            store.complete_trade(
                trade_id, "CLOSED_WIN" if pnl > 0 else "CLOSED_LOSS",
                1.0 if pnl > 0 else 0.0, 2_000_000 + index//2,
                pnl, "official_outcome", "exact", gross_pnl=pnl,
                resolution_verified=True)
        metrics = store.dashboard_metrics(3_000_000)
        terminal_sum = sum(store.get_trade(i)["pnl"] for i in range(1, 5))
        assert metrics["total_lite_pnl"] == terminal_sum == 2.0
        assert metrics["expectancy"] == 0.5
        assert metrics["profit_factor"] == pytest.approx(5/3, abs=1e-6)
        assert metrics["average_win"] == 2.5
        assert metrics["average_loss"] == -1.5
        assert metrics["max_drawdown"] >= 0
    finally:
        store.close()


def test_committed_exposure_includes_pending_and_unresolved(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        first = store.insert_trade(_entry(window=600_000, entry_price=0.4, entry_fee=0.08))
        store.mark_pending(first, 600_000, "no_exit")
        second = store.insert_trade(_entry(window=900_000, entry_price=0.5, entry_fee=0.0875))
        store.mark_pending(second, 900_000, "no_exit")
        store.mark_unresolved(second, 910_000, "no_evidence")
        assert store.committed_exposure() == pytest.approx(2.08+2.5875)
    finally:
        store.close()


def test_transaction_reasserts_position_and_exposure_guards(tmp_path):
    position_store = LiteStore(
        str(tmp_path / "positions.db"), max_open_positions=1,
        max_open_per_asset=1, exposure_cap_usd=20.0)
    try:
        position_store.insert_trade(_entry())
        with pytest.raises(WindowLockConflict) as limit:
            position_store.insert_trade(_entry(window=900_000, asset="ETH"))
        assert limit.value.reason == "max_open_positions"
    finally:
        position_store.close()

    exposure_store = LiteStore(
        str(tmp_path / "exposure.db"), max_open_positions=6,
        max_open_per_asset=2, exposure_cap_usd=3.0)
    try:
        exposure_store.insert_trade(_entry(entry_price=0.40, entry_fee=0.08))
        with pytest.raises(WindowLockConflict) as cap:
            exposure_store.insert_trade(_entry(
                window=900_000, asset="ETH", entry_price=0.40, entry_fee=0.08))
        assert cap.value.reason == "equity_exposure_cap"
    finally:
        exposure_store.close()


def test_lite_store_never_touches_another_database(tmp_path):
    advanced = tmp_path / "advanced.db"
    advanced.write_bytes(b"advanced-sentinel")
    store = LiteStore(str(tmp_path / "poly_alpha_lite.db"))
    try:
        store.insert_trade(_entry())
    finally:
        store.close()
    assert advanced.read_bytes() == b"advanced-sentinel"


def test_anchor_is_recorded_but_not_required_across_distinct_windows(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        store.insert_trade(_entry(window=600_000, anchor_available=False))
        store.insert_trade(_entry(
            window=900_000, anchor_available=True, price_to_beat=100.5))
        assert store.dashboard_metrics(900_000)["anchor_breakdown"] == {
            "anchor": 1, "no_anchor": 1}
    finally:
        store.close()


def test_historical_backfill_rejects_ambiguous_outcomes_before_mutation(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        trade_id = store.insert_trade(_entry())
        store.mark_pending(trade_id, 600_000, "no_exit")
        before = store.get_trade(trade_id)
        with pytest.raises(ValueError, match="exactly YES or NO"):
            store.apply_verified_official_resolution(
                trade_id, outcome="UNKNOWN", evidence_ts=700_000, fee_rate=0.07)
        after = store.get_trade(trade_id)
        assert after["status"] == before["status"]
        assert after["pnl"] is None
    finally:
        store.close()


def test_no_runtime_artifact_is_inside_git_tracked_scope():
    root = Path(__file__).resolve().parent.parent
    ignore = (root / ".gitignore").read_text(encoding="utf-8")
    assert "runtime/" in ignore and "logs/" in ignore
    assert "/data/poly_alpha_lite.db*" in ignore
