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


def _market(window=600_000):
    start = window - 300_000
    return {
        "asset": "BTC", "market_id": f"m-{window}", "event_id": f"e-{window}",
        "slug": f"btc-updown-5m-{start//1000}", "condition_id": f"c-{window}",
        "window_open_ts": start, "window_close_ts": window,
    }


def _direction(*, selected_edge=0.03, yes_price=0.47, no_price=0.54,
               fair_yes=0.55, cex_adjustment=0.01, lead_lag_status="LEADING",
               reason="initial_edge"):
    return {
        "output": "BUY_YES", "side": "BUY_YES", "reason": reason,
        "executable_yes_price": yes_price, "executable_no_price": no_price,
        "net_edge_yes": selected_edge, "net_edge_no": -0.10,
        "selected_net_edge": selected_edge,
        "fair_probability_yes": fair_yes, "fair_probability_no": 1.0-fair_yes,
        "cex_adjustment": cex_adjustment, "lead_lag_status": lead_lag_status,
    }


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
        INSERT INTO lite_window_locks(
            asset,slug,market_id,event_id,condition_id,window_open_ts,
            window_close_ts,side,status,lifecycle_status,direction_decision_ts,
            direction_output,entry_state,idempotency_key,last_updated_ts)
        VALUES('BTC','historical','m','e','c',300000,600000,'BUY_YES',
               'SKIPPED','SKIPPED',350000,'BUY_YES','SKIPPED','historical-key',
               354321);
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
                "pullback_condition", "initial_executable_yes_price",
                "final_selected_net_edge"} <= lock_columns
        assert {"runtime_commit", "model_version", "exit_now_value",
                "maker_fill_assumed", "initial_fair_probability_yes",
                "final_lead_lag_status"} <= trade_columns
        historical = store.get_window_lock("BTC", 600_000)
        assert historical["direction_decision_ts"] == 350_000
        assert historical["last_updated_ts"] == 354_321
        assert historical["final_executable_yes_price"] is None
        assert historical["final_selected_net_edge"] is None
    finally:
        store.close()


def test_trade_migration_preserves_rows_and_leaves_historical_finals_null(tmp_path):
    path = tmp_path / "old-trades.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE lite_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL, market_id TEXT NOT NULL, event_id TEXT NOT NULL,
            slug TEXT NOT NULL, condition_id TEXT NOT NULL,
            side TEXT NOT NULL, entry_ts INTEGER NOT NULL,
            window_open_ts INTEGER, window_close_ts INTEGER NOT NULL,
            status TEXT NOT NULL, pnl REAL, gross_pnl REAL
        );
        INSERT INTO lite_trades(
            asset,market_id,event_id,slug,condition_id,side,entry_ts,
            window_open_ts,window_close_ts,status,pnl,gross_pnl)
        VALUES('BTC','m','e','historical','c','BUY_YES',350000,
               300000,600000,'CLOSED_WIN',1.25,1.25);
        """)
    connection.close()

    store = LiteStore(str(path))
    try:
        historical = store.get_trade(1)
        assert store.table_count("lite_trades") == 1
        assert historical["entry_ts"] == 350_000
        assert historical["window_open_ts"] == 300_000
        assert historical["window_close_ts"] == 600_000
        assert historical["final_executable_yes_price"] is None
        assert historical["final_net_edge_yes"] is None
        assert historical["final_selected_net_edge"] is None
        assert historical["final_fair_probability_yes"] is None
        assert historical["final_cex_adjustment"] is None
        assert historical["final_lead_lag_status"] is None
        assert store._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
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


def test_terminal_skip_persists_actual_wait_and_distinct_final_telemetry(tmp_path):
    store = LiteStore(str(tmp_path / "terminal-skip.db"))
    try:
        initial = _direction()
        reserved, _, _ = store.reserve_window_direction(
            _market(), initial, now_ms=100_000)
        assert reserved
        store.update_window_lock(
            "BTC", 600_000, status="MAKER_WAIT", maker_start_ts=100_000,
            maker_deadline_ts=104_000, maker_wait_ms=0, wait_duration_ms=0)
        final = _direction(
            selected_edge=0.004, yes_price=0.49, no_price=0.52,
            fair_yes=0.53, cex_adjustment=0.006,
            lead_lag_status="NO_NEW_TICK", reason="edge_below_entry_threshold")

        store.mark_window_skipped(
            "BTC", 600_000, 104_500, "maker_edge_expired",
            chase_prevented=True, final_direction=final)

        lock = store.get_window_lock("BTC", 600_000)
        assert lock["wait_duration_ms"] == 4_500
        assert lock["maker_wait_ms"] == 4_500
        assert lock["maker_fill_assumed"] == 0
        assert lock["executable_yes_price"] == pytest.approx(0.47)
        assert lock["selected_net_edge"] == pytest.approx(0.03)
        assert lock["initial_executable_yes_price"] == pytest.approx(0.47)
        assert lock["initial_selected_net_edge"] == pytest.approx(0.03)
        assert lock["initial_entry_reason"] == "initial_edge"
        assert lock["final_executable_yes_price"] == pytest.approx(0.49)
        assert lock["final_executable_no_price"] == pytest.approx(0.52)
        assert lock["final_net_edge_yes"] == pytest.approx(0.004)
        assert lock["final_selected_net_edge"] == pytest.approx(0.004)
        assert lock["final_fair_probability_yes"] == pytest.approx(0.53)
        assert lock["final_cex_adjustment"] == pytest.approx(0.006)
        assert lock["final_lead_lag_status"] == "NO_NEW_TICK"
        assert lock["final_entry_reason"] == "maker_edge_expired"
    finally:
        store.close()


def test_terminal_cross_copies_initials_and_persists_final_recomputation(tmp_path):
    store = LiteStore(str(tmp_path / "terminal-cross.db"))
    try:
        initial = _direction()
        reserved, _, _ = store.reserve_window_direction(
            _market(), initial, now_ms=100_000)
        assert reserved
        store.update_window_lock(
            "BTC", 600_000, status="MAKER_WAIT", maker_start_ts=100_000,
            maker_deadline_ts=104_000)
        final = _direction(
            selected_edge=0.012, yes_price=0.48, no_price=0.53,
            fair_yes=0.545, cex_adjustment=0.008,
            lead_lag_status="NO_NEW_TICK", reason="final_cross")
        trade_id = store.insert_trade(_entry(
            entry_ts=104_500, maker_start_ts=100_000,
            maker_deadline_ts=104_000, wait_duration_ms=0, maker_wait_ms=0,
            final_entry_reason="maker_expired_cross_edge_valid", **{
                field: final[field] for field in (
                    "executable_yes_price", "executable_no_price", "net_edge_yes",
                    "net_edge_no", "selected_net_edge", "fair_probability_yes",
                    "fair_probability_no", "cex_adjustment", "lead_lag_status")
            }))

        trade = store.get_trade(trade_id)
        lock = store.get_window_lock("BTC", 600_000)
        assert trade["wait_duration_ms"] == trade["maker_wait_ms"] == 4_500
        assert trade["maker_fill_assumed"] == 0
        assert trade["initial_executable_yes_price"] == pytest.approx(0.47)
        assert trade["initial_selected_net_edge"] == pytest.approx(0.03)
        assert trade["final_executable_yes_price"] == pytest.approx(0.48)
        assert trade["final_net_edge_yes"] == pytest.approx(0.012)
        assert trade["final_selected_net_edge"] == pytest.approx(0.012)
        assert trade["final_fair_probability_yes"] == pytest.approx(0.545)
        assert trade["final_cex_adjustment"] == pytest.approx(0.008)
        assert trade["final_lead_lag_status"] == "NO_NEW_TICK"
        assert lock["wait_duration_ms"] == lock["maker_wait_ms"] == 4_500
        assert lock["initial_selected_net_edge"] == pytest.approx(0.03)
        assert lock["final_selected_net_edge"] == pytest.approx(0.012)
        assert lock["final_entry_reason"] == "maker_expired_cross_edge_valid"
    finally:
        store.close()


def test_expired_unentered_wait_is_reconciled_after_rollover(tmp_path):
    store = LiteStore(str(tmp_path / "expired-wait.db"))
    try:
        reserved, _, _ = store.reserve_window_direction(
            _market(), _direction(), now_ms=590_000)
        assert reserved
        store.update_window_lock(
            "BTC", 600_000, status="MAKER_WAIT", maker_start_ts=590_000,
            maker_deadline_ts=594_000, maker_wait_ms=0, wait_duration_ms=0)

        store.finalize_window_locks(600_500)

        lock = store.get_window_lock("BTC", 600_000)
        assert lock["status"] == lock["lifecycle_status"] == "SKIPPED"
        assert lock["final_entry_reason"] == "expired_market"
        assert lock["maker_wait_ms"] == lock["wait_duration_ms"] == 10_500
        assert lock["maker_fill_assumed"] == 0
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
