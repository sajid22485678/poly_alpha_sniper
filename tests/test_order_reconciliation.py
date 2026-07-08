import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.storage.backup_manager import BackupManager
from poly_alpha_sniper.storage.migrations import run_migrations
from poly_alpha_sniper.storage.order_reconciliation import (
    DEFAULT_CUTOFF_AGE_MS, reconcile_stale_orders)
from poly_alpha_sniper.storage.sqlite_store import SqliteStore

NOW_MS = 1_752_000_000_000


def _store(tmp_path):
    s = SqliteStore(str(tmp_path / "test.db"))
    run_migrations(s)
    return s


def _backup_manager(tmp_path, store):
    cfg = load_config()
    clock = SimClock(NOW_MS)
    return BackupManager(cfg, clock, store.path, str(tmp_path / "bk"))


def _order_row(order_id, state, created_ts_ms, filled_shares=0.0):
    return {"order_id": order_id, "state": state, "created_ts_ms": created_ts_ms,
           "updated_ts_ms": created_ts_ms, "filled_shares": filled_shares,
           "token_id": "tok", "market_id": "m1", "side": "BUY_YES",
           "price": 0.02, "size_shares": 50.0, "size_usd": 1.0, "mode": "shadow_live"}


def test_stale_zero_fill_open_order_is_reconciled(tmp_path):
    s = _store(tmp_path)
    old_ts = NOW_MS - DEFAULT_CUTOFF_AGE_MS - 1000
    s.insert("orders", _order_row("stuck-1", "OPEN", old_ts))
    bm = _backup_manager(tmp_path, s)

    result = reconcile_stale_orders(s, bm, NOW_MS)

    assert result["touched"] == ["stuck-1"]
    assert result["backup_path"] is not None
    row = s.query("SELECT * FROM orders WHERE order_id='stuck-1'")[0]
    assert row["state"] == "RECONCILED"
    assert "RECONCILED_STALE_NO_FILL" in row["error"]
    assert row["filled_shares"] == 0.0  # never rewritten as if it filled
    s.close()


def test_recent_open_order_not_touched(tmp_path):
    s = _store(tmp_path)
    recent_ts = NOW_MS - 2000  # well within the resting window
    s.insert("orders", _order_row("fresh-1", "OPEN", recent_ts))
    bm = _backup_manager(tmp_path, s)

    result = reconcile_stale_orders(s, bm, NOW_MS)

    assert result["touched"] == []
    row = s.query("SELECT * FROM orders WHERE order_id='fresh-1'")[0]
    assert row["state"] == "OPEN"
    s.close()


def test_matched_order_never_touched_regardless_of_age(tmp_path):
    s = _store(tmp_path)
    old_ts = NOW_MS - DEFAULT_CUTOFF_AGE_MS - 1000
    s.insert("orders", _order_row("filled-1", "MATCHED", old_ts, filled_shares=5.26))
    bm = _backup_manager(tmp_path, s)

    result = reconcile_stale_orders(s, bm, NOW_MS)

    assert result["touched"] == []
    row = s.query("SELECT * FROM orders WHERE order_id='filled-1'")[0]
    assert row["state"] == "MATCHED"
    s.close()


def test_no_rows_deleted(tmp_path):
    s = _store(tmp_path)
    old_ts = NOW_MS - DEFAULT_CUTOFF_AGE_MS - 1000
    s.insert("orders", _order_row("stuck-1", "OPEN", old_ts))
    bm = _backup_manager(tmp_path, s)

    before = len(s.query("SELECT * FROM orders"))
    reconcile_stale_orders(s, bm, NOW_MS)
    after = len(s.query("SELECT * FROM orders"))

    assert before == after == 1
    s.close()


def test_idempotent_second_run_touches_nothing(tmp_path):
    s = _store(tmp_path)
    old_ts = NOW_MS - DEFAULT_CUTOFF_AGE_MS - 1000
    s.insert("orders", _order_row("stuck-1", "OPEN", old_ts))
    bm = _backup_manager(tmp_path, s)

    first = reconcile_stale_orders(s, bm, NOW_MS)
    second = reconcile_stale_orders(s, bm, NOW_MS + 60_000)

    assert first["touched"] == ["stuck-1"]
    assert second["touched"] == []
    s.close()


def test_no_backup_taken_when_nothing_stale(tmp_path):
    s = _store(tmp_path)
    bm = _backup_manager(tmp_path, s)

    result = reconcile_stale_orders(s, bm, NOW_MS)

    assert result == {"touched": [], "backup_path": None}
    assert bm.list_backups() == []  # backup_now() was never called
    s.close()


def test_incident_report_logged_via_insert_fn(tmp_path):
    s = _store(tmp_path)
    old_ts = NOW_MS - DEFAULT_CUTOFF_AGE_MS - 1000
    s.insert("orders", _order_row("stuck-1", "OPEN", old_ts))
    bm = _backup_manager(tmp_path, s)

    reconcile_stale_orders(s, bm, NOW_MS, insert_fn=s.insert)

    reports = s.query("SELECT * FROM incident_reports")
    assert len(reports) == 1
    assert reports[0]["kind"] == "stale_order_reconciliation"
    assert "stuck-1" in reports[0]["detail"]
    s.close()
