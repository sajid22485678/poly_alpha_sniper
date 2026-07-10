from pathlib import Path

from poly_alpha_sniper.lite.lite_store import LiteStore


def _entry(**overrides):
    row = {
        "asset": "BTC",
        "market_id": "m1",
        "event_id": "e1",
        "slug": "btc-updown-5m-300",
        "condition_id": "c1",
        "yes_token_id": "yes-1",
        "no_token_id": "no-1",
        "side": "BUY_YES",
        "shares": 999,
        "entry_price": 0.42,
        "entry_cost": 999,
        "entry_ts": 350_000,
        "window_close_ts": 600_000,
        "status": "OPEN",
        "anchor_available": False,
        "price_to_beat": None,
        "cex_source": "test",
        "cex_entry_price": 100.0,
        "momentum_pct": 0.001,
        "strategy_name": "lite_momentum_v1",
    }
    row.update(overrides)
    return row


def test_store_enforces_five_shares_and_informational_cost(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        trade_id = store.insert_trade(_entry())
        trade = store.get_trade(trade_id)
        assert trade["shares"] == 5.0
        assert trade["entry_cost"] == 5 * 0.42
        assert trade["no_anchor_trade"] == 1
    finally:
        store.close()


def test_reject_rows_are_bucketed_and_do_not_flood_db(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        for ts in range(1_000, 29_000, 1_000):
            store.record_reject(ts, "BTC", "slug", "no_momentum", bucket_s=30)
        assert store.table_count("lite_rejects") == 1
        metrics = store.dashboard_metrics(29_000)
        assert metrics["top_reject_reasons"]["no_momentum"] == 28
    finally:
        store.close()


def test_unresolved_has_null_pnl_and_is_excluded_from_performance(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        unresolved_id = store.insert_trade(_entry(market_id="unresolved"))
        store.mark_pending(unresolved_id, 610_000, "no_exit_book")
        store.mark_unresolved(unresolved_id, 700_000, "no_official_evidence")
        closed_id = store.insert_trade(_entry(market_id="closed", entry_ts=351_000))
        store.complete_trade(
            trade_id=closed_id,
            status="CLOSED_WIN",
            exit_price=1.0,
            exit_ts=700_000,
            pnl=2.9,
            resolution_source="official_outcome",
            resolution_reason="YES",
        )
        metrics = store.dashboard_metrics(700_000)
        assert store.get_trade(unresolved_id)["pnl"] is None
        assert metrics["unresolved_final"] == 1
        assert metrics["completed_trades"] == 1
        assert metrics["winrate"] == 1.0
        assert metrics["expectancy"] == 2.9
    finally:
        store.close()


def test_lite_store_never_touches_another_database(tmp_path):
    baseline = tmp_path / "advanced.db"
    baseline.write_bytes(b"advanced-sentinel")
    lite = tmp_path / "poly_alpha_lite.db"
    store = LiteStore(str(lite))
    try:
        store.insert_trade(_entry())
    finally:
        store.close()
    assert baseline.read_bytes() == b"advanced-sentinel"
    assert lite.exists()


def test_anchor_presence_is_recorded_but_never_required(tmp_path):
    store = LiteStore(str(tmp_path / "lite.db"))
    try:
        store.insert_trade(_entry(market_id="no-anchor", anchor_available=False))
        store.insert_trade(_entry(
            market_id="anchor", entry_ts=351_000, anchor_available=True,
            price_to_beat=100.5,
        ))
        metrics = store.dashboard_metrics(400_000)
        assert metrics["anchor_breakdown"] == {"anchor": 1, "no_anchor": 1}
    finally:
        store.close()
