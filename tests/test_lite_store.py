from pathlib import Path
import sqlite3

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
        completed_lock = store.get_window_lock("BTC", 600_000)
        assert completed_lock["status"] == "COMPLETE"
        completed_updated_ts = completed_lock["last_updated_ts"]
        store.finalize_window_locks(800_000)
        assert store.get_window_lock("BTC", 600_000)["last_updated_ts"] == completed_updated_ts
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
        assert metrics["top_reject_reasons"]["truly_flat"] == 28
        assert metrics["anti_dead_bot_last_hour"]["candidate"] == 28
        assert metrics["candidate_evaluations_last_hour"] == 28
    finally:
        store.close()


def test_dashboard_active_locks_excludes_expired_windows(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        rows = [
            ("BTC", "expired", 90_000, "expired-key"),
            ("ETH", "future", 120_000, "future-key"),
        ]
        store._conn.executemany(
            """INSERT INTO lite_window_locks(
               asset,slug,market_id,event_id,condition_id,window_open_ts,
               window_close_ts,side,status,lifecycle_status,
               direction_decision_ts,idempotency_key,last_updated_ts)
               VALUES(?,?,?, ?,?, ?,?,'BUY_YES','DIRECTION_LOCKED',
                      'DIRECTION_LOCKED',?,?,?)""",
            [
                (asset, slug, f"m-{slug}", f"e-{slug}", f"c-{slug}",
                 close_ts-300_000, close_ts, 10_000, key, 10_000)
                for asset, slug, close_ts, key in rows
            ],
        )
        store._conn.commit()

        metrics = store.dashboard_metrics(100_000)

        assert metrics["asset_window_locks"]["active"] == 1
        assert metrics["asset_window_locks"]["by_status"]["DIRECTION_LOCKED"] == 2
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


def test_additive_migration_extends_existing_window_lock_schema(tmp_path):
    path = tmp_path / "old-lock.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE lite_window_locks (
            asset TEXT NOT NULL, slug TEXT NOT NULL, market_id TEXT NOT NULL,
            event_id TEXT NOT NULL, condition_id TEXT NOT NULL,
            window_open_ts INTEGER NOT NULL, window_close_ts INTEGER NOT NULL,
            side TEXT NOT NULL, status TEXT NOT NULL, lifecycle_status TEXT NOT NULL,
            direction_decision_ts INTEGER NOT NULL, direction_output TEXT,
            direction_score REAL, yes_score REAL, no_score REAL,
            score_difference REAL, confidence REAL, direction_reason TEXT,
            return_10s REAL, return_30s REAL, return_60s REAL,
            tick_return REAL, volatility REAL, entry_state TEXT,
            initial_ask REAL, target_price REAL, max_chase_price REAL,
            deadline_ts INTEGER, last_reevaluate_ts INTEGER,
            expected_improvement REAL, actual_improvement REAL,
            wait_duration_ms INTEGER NOT NULL DEFAULT 0,
            missed_opportunity INTEGER NOT NULL DEFAULT 0,
            chase_prevented INTEGER NOT NULL DEFAULT 0,
            final_entry_reason TEXT, trade_id INTEGER,
            idempotency_key TEXT NOT NULL, last_updated_ts INTEGER NOT NULL,
            PRIMARY KEY(asset,window_close_ts), UNIQUE(idempotency_key)
        );
        """)
    connection.close()
    store = LiteStore(str(path))
    try:
        lock_columns = {
            row[1] for row in store._conn.execute(
                "PRAGMA table_info(lite_window_locks)").fetchall()}
        trade_columns = {
            row[1] for row in store._conn.execute(
                "PRAGMA table_info(lite_trades)").fetchall()}
        assert {"fair_probability_yes", "net_edge_yes", "maker_start_ts",
                "pullback_condition"} <= lock_columns
        assert {"runtime_commit", "model_version", "exit_now_value",
                "maker_fill_assumed"} <= trade_columns
    finally:
        store.close()


def test_fair_value_telemetry_round_trips_and_assumed_maker_fill_fails(tmp_path):
    store = LiteStore(str(tmp_path / "telemetry.db"))
    try:
        trade_id = store.insert_trade(_entry(
            strategy_name="lite_fair_value_edge_v3",
            runtime_commit="a"*40, model_version="paired_book_fair_value_v1",
            market_probability_yes=0.50, fair_probability_yes=0.56,
            fair_probability_no=0.44, executable_yes_price=0.48,
            executable_no_price=0.53, net_edge_yes=0.02,
            net_edge_no=-0.08, selected_net_edge=0.02,
            execution_state="CROSS_SPREAD", maker_fill_assumed=False,
            pullback_start_ts=None, pullback_condition=None))
        row = store.get_trade(trade_id)
        assert row["fair_probability_yes"] == pytest.approx(0.56)
        assert row["net_edge_yes"] == pytest.approx(0.02)
        assert row["maker_fill_assumed"] == 0
        with pytest.raises(ValueError, match="assumed maker fill"):
            store.insert_trade(_entry(
                window=900_000, maker_fill_assumed=True))
    finally:
        store.close()


def test_management_evidence_is_bounded_and_persisted(tmp_path):
    store = LiteStore(str(tmp_path / "management.db"))
    try:
        trade_id = store.insert_trade(_entry())
        store.update_trade_management(
            trade_id, 500_000, exit_now_value=1.2,
            hold_expected_value=2.4, exit_fair_probability=0.50,
            thesis_status="CONTINUING", reason="hold_ev_superior")
        row = store.get_trade(trade_id)
        assert row["exit_now_value"] == pytest.approx(1.2)
        assert row["hold_expected_value"] == pytest.approx(2.4)
        assert row["management_reason"] == "hold_ev_superior"
        with pytest.raises(ValueError, match="management evidence"):
            store.update_trade_management(
                trade_id, 501_000, exit_now_value=6.0,
                hold_expected_value=2.0, exit_fair_probability=0.5,
                thesis_status="CONTINUING", reason="invalid")
    finally:
        store.close()


def test_verified_metrics_require_execution_and_resolution_evidence(tmp_path):
    store = LiteStore(str(tmp_path / "verified.db"))
    try:
        verified = store.insert_trade(_entry(
            window=600_000, execution_verified=True))
        store.complete_trade(
            verified, "CLOSED_WIN", 1.0, 610_000, 1.0,
            "official_outcome", "exact", resolution_verified=True)
        unverified_resolution = store.insert_trade(_entry(
            window=900_000, execution_verified=True))
        store.complete_trade(
            unverified_resolution, "CLOSED_BOOK_EXIT", 0.5, 850_000, 1.0,
            "book_exit", "legacy", resolution_verified=False)
        metrics = store.dashboard_metrics(1_000_000)
        assert metrics["completed_trades"] == 2
        assert metrics["verified_metrics"]["count"] == 1
        assert metrics["verified_realized_pnl"] == pytest.approx(1.0)
    finally:
        store.close()
