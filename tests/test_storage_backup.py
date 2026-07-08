import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.storage.backup_manager import BackupManager
from poly_alpha_sniper.storage.migrations import TABLES, run_migrations
from poly_alpha_sniper.storage.sqlite_store import SqliteStore


def _store(tmp_path):
    s = SqliteStore(str(tmp_path / "test.db"))
    run_migrations(s)
    return s


def test_migrations_create_all_tables(tmp_path):
    s = _store(tmp_path)
    rows = s.query("SELECT name FROM sqlite_master WHERE type='table'")
    names = {r["name"] for r in rows}
    for table in TABLES:
        assert table in names, f"missing table {table}"
    assert "schema_migrations" in names
    s.close()


def test_migrations_idempotent(tmp_path):
    s = _store(tmp_path)
    run_migrations(s)  # second run must not raise
    s.close()


def test_insert_query_roundtrip(tmp_path):
    s = _store(tmp_path)
    s.insert("predictions", {"ts_ms": 123, "asset": "BTC", "edge": 0.07,
                             "tier": "A", "unknown_key_ignored": "x"})
    rows = s.query("SELECT * FROM predictions")
    assert len(rows) == 1
    assert rows[0]["asset"] == "BTC"
    assert rows[0]["edge"] == pytest.approx(0.07)
    s.close()


def test_secret_key_insert_refused(tmp_path):
    s = _store(tmp_path)
    with pytest.raises(ValueError):
        s.insert("errors", {"ts_ms": 1, "api_secret": "oops"})
    with pytest.raises(ValueError):
        s.insert("errors", {"ts_ms": 1, "private_key": "oops"})
    s.close()


def test_backup_prune_restore(tmp_path):
    s = _store(tmp_path)
    s.insert("pnl", {"ts_ms": 1, "realized_pnl_usd": 1.5, "equity_usd": 11.5})
    s.close()
    cfg = load_config()
    cfg.runtime.keep_backups = 3
    clock = SimClock(1_000_000)
    bm = BackupManager(cfg, clock, str(tmp_path / "test.db"), str(tmp_path / "bk"))
    for _ in range(5):
        clock.advance_ms(1000)
        bm.backup_now()
    assert len(bm.list_backups()) <= 3

    with pytest.raises(PermissionError):
        bm.restore(str(bm.list_backups()[-1]))  # no confirm
    restored = bm.restore(str(bm.list_backups()[-1]), confirm=True)
    s2 = SqliteStore(str(restored))
    rows = s2.query("SELECT * FROM pnl")
    assert len(rows) == 1
    s2.close()
