"""Dashboard liveness: same DB path as runtime, fresh reads, no false demo."""
import json
import time

from poly_alpha_sniper.dashboard.db_reader import (
    DashboardData, demo_data, read_runtime_state, resolve_db_path)
from poly_alpha_sniper.storage.db import default_sqlite_path
from poly_alpha_sniper.storage.migrations import run_migrations
from poly_alpha_sniper.storage.sqlite_store import SqliteStore


def test_dashboard_resolves_same_path_as_runtime_default():
    assert resolve_db_path(env={}) == default_sqlite_path()


def test_dashboard_resolves_env_url_like_runtime():
    # the exact .env default shipped in .env.example
    path = resolve_db_path(env={"DATABASE_URL": "sqlite:///storage/poly_alpha_sniper.db"})
    assert path == default_sqlite_path()
    # absolute URLs pass through
    abs_path = resolve_db_path(env={"DATABASE_URL": "sqlite:///D:/tmp/x.db"})
    assert abs_path.replace("\\", "/").endswith("D:/tmp/x.db")


def _live_db(tmp_path):
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    return path, store


def test_live_but_empty_db_is_not_demo(tmp_path):
    path, store = _live_db(tmp_path)
    store.close()
    data = DashboardData(path)
    assert data.has_data is True  # schema exists -> LIVE empty state, not demo
    assert data.predictions() == []
    assert "no runtime activity" in data.why_no_predictions() or \
           "waiting" in data.why_no_predictions()


def test_fresh_instance_sees_new_rows(tmp_path):
    path, store = _live_db(tmp_path)
    first = DashboardData(path)
    assert first.count("predictions") == 0
    store.insert("predictions", {"ts_ms": 111, "asset": "BTC", "decision": "REJECT",
                                 "reject_reason": "REJECTED_SPREAD_TOO_WIDE"})
    # dashboard re-instantiates per rerun -> new connection sees the row
    second = DashboardData(path)
    assert second.count("predictions") == 1
    assert second.latest_ts("predictions") == 111
    store.close()


def test_db_info_contract(tmp_path):
    path, store = _live_db(tmp_path)
    store.insert("signals", {"ts_ms": 222, "asset": "ETH", "kind": "shock"})
    data = DashboardData(path)
    info = data.db_info()
    assert info["exists"] and info["readable"]
    assert info["path"] == path
    assert info["last_write_ms"] > 0
    assert info["row_counts"]["signals"] == 1
    assert info["last_ts"]["signals"] == 222
    assert "shadow_diagnostics" in info["row_counts"]
    store.close()


def test_why_no_predictions_uses_diagnostics(tmp_path):
    path, store = _live_db(tmp_path)
    store.insert("shadow_diagnostics", {
        "ts_ms": 333, "asset": "BTC", "reason": "rejected_by_no_shock",
        "detail": "ret2=+0.00010 z=+0.30", "fresh_books": 10, "total_books": 12})
    data = DashboardData(path)
    why = data.why_no_predictions()
    assert "rejected_by_no_shock" in why
    store.close()


def test_read_runtime_state(tmp_path):
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({
        "heartbeat_ts_ms": 999, "mode": "shadow_live",
        "diagnostics": {"fresh_books": 9, "total_books": 12,
                        "prediction_loop_iterations": 42}}))
    state = read_runtime_state(str(state_file))
    assert state["heartbeat_ts_ms"] == 999
    assert state["diagnostics"]["prediction_loop_iterations"] == 42
    assert read_runtime_state(str(tmp_path / "missing.json")) == {}


def test_demo_only_when_file_missing(tmp_path):
    data = DashboardData(str(tmp_path / "nope.db"))
    assert data.has_data is False
    d = demo_data()
    assert d["label"][0]["label"] == "DEMO DATA — NOT REAL BOT DATA"


def test_dashboard_source_auto_refresh_and_no_secrets():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "dashboard" / "app.py"
           ).read_text(encoding="utf-8")
    assert "st.rerun()" in src                      # auto-refresh present
    assert "resolve_db_path" in src                 # project-root path resolution
    assert "use_container_width" not in src         # deprecated API gone
    for forbidden in ("place_order", "cancel_order", "PRIVATE_KEY",
                      "API_SECRET", "BOT_TOKEN"):
        assert forbidden not in src
