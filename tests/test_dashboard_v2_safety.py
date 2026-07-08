"""Dashboard v2 safety checks: explicit live-OFF labeling, no fake data when
a real DB exists, the Hermes panel never touches .env, and the project-wide
safety invariants (mode/dry_run/LIVE_TRADING_ENABLED) still hold after the
v2 rebuild."""
import re
from pathlib import Path

import pytest

from poly_alpha_sniper.core.config_loader import TradingMode, load_config, load_secrets
from poly_alpha_sniper.dashboard.db_reader import DashboardData, demo_data
from poly_alpha_sniper.dashboard.process_health import check_single_instance, wal_status
from poly_alpha_sniper.storage.migrations import run_migrations
from poly_alpha_sniper.storage.sqlite_store import SqliteStore

PROJECT_ROOT = Path(__file__).resolve().parent.parent
APP_SOURCE = (PROJECT_ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
PROCESS_HEALTH_SOURCE = (PROJECT_ROOT / "dashboard" / "process_health.py").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Explicit "live: OFF" labeling (not a bare "live_enabled" badge)
# ---------------------------------------------------------------------------

def test_dashboard_uses_dedicated_live_status_badge():
    assert "live_status_badge(" in APP_SOURCE


def test_status_badges_call_does_not_bury_live_enabled_as_a_bare_flag():
    """The generic status_badges({...}) call must not carry a bare
    'live_enabled' key -- that ambiguous badge was the exact thing v2 was
    asked to stop doing. The dedicated live_status_badge() call (checked
    above) is the only place 'live' status should be rendered."""
    call_match = re.search(r"status_badges\(\{(.*?)\}\)", APP_SOURCE, re.DOTALL)
    assert call_match, "status_badges({...}) call not found"
    assert '"live_enabled"' not in call_match.group(1)


def test_live_status_badge_component_renders_off_in_red_by_default():
    from poly_alpha_sniper.dashboard.components import live_status_badge
    import inspect
    src = inspect.getsource(live_status_badge)
    assert "pas-badge-off" in src  # red class
    assert "LIVE: OFF" in src


# ---------------------------------------------------------------------------
# Hermes panel / process_health never touch .env or secrets
# ---------------------------------------------------------------------------

def test_hermes_panel_source_never_reads_env_or_secrets():
    forbidden = ("load_secrets(", "load_dotenv_file(", "Secrets(")
    # Isolate the Hermes panel section specifically, not the whole file
    # (auth.py's check_auth() legitimately calls load_secrets elsewhere).
    idx = APP_SOURCE.index("HERMES AGENT PANEL")
    hermes_section = APP_SOURCE[idx:]
    for term in forbidden:
        assert term not in hermes_section, f"Hermes panel calls {term!r}"


def test_process_health_module_never_touches_env_or_secrets():
    forbidden = ("load_secrets", "load_dotenv_file", "Secrets(", "os.environ")
    for term in forbidden:
        assert term not in PROCESS_HEALTH_SOURCE


def test_process_health_module_never_calls_process_control():
    """Read-only introspection only -- no kill/terminate/suspend calls."""
    forbidden = (".kill(", ".terminate(", ".suspend(", ".send_signal(")
    for term in forbidden:
        assert term not in PROCESS_HEALTH_SOURCE


def test_dashboard_never_calls_order_placement_or_process_control():
    forbidden = (".place_order(", ".cancel_order(", ".cancel_all(", ".kill(", ".terminate(")
    for term in forbidden:
        assert term not in APP_SOURCE


# ---------------------------------------------------------------------------
# No fake data when a real (non-demo) DB exists
# ---------------------------------------------------------------------------

def test_live_db_with_rows_is_not_flagged_demo(tmp_path):
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    store.insert("predictions", {"ts_ms": 1000, "asset": "BTC", "decision": "REJECT",
                                 "reject_reason": "REJECTED_SPREAD_TOO_WIDE"})
    store.close()
    data = DashboardData(path)
    assert data.has_data is True  # real DB -> never falls back to demo_data()


def test_demo_data_is_unambiguously_labeled():
    d = demo_data()
    assert "DEMO" in d["label"][0]["label"]
    for row in d["predictions"]:
        assert row.get("source") == "DEMO"
    for row in d["exits"]:
        assert row.get("source") == "DEMO"


def test_process_health_never_fabricates_a_count_it_could_not_observe():
    """If psutil scanning fails, available must be False, not a fabricated
    zero/duplicate result."""
    result = check_single_instance()
    if not result["available"]:
        assert result["count"] == 0 and result["likely_duplicate"] is False
    else:
        assert isinstance(result["count"], int) and result["count"] == len(result["matches"])


def test_wal_status_reports_actual_file_state(tmp_path):
    db = tmp_path / "x.db"
    db.write_text("", encoding="utf-8")
    result = wal_status(str(db))
    assert result["wal_present"] is False  # no -wal file created yet
    (tmp_path / "x.db-wal").write_bytes(b"1234")
    result2 = wal_status(str(db))
    assert result2["wal_present"] is True
    assert result2["wal_size_bytes"] == 4


# ---------------------------------------------------------------------------
# Project-wide safety invariants (still true after the v2 rebuild)
# ---------------------------------------------------------------------------

def test_mode_is_still_shadow_live_or_simulation():
    cfg = load_config()
    assert cfg.mode.trading_mode in ("shadow_live", "simulation")
    assert TradingMode(cfg.mode.trading_mode).is_live is False


def test_dry_run_is_still_true():
    cfg = load_config()
    assert cfg.mode.dry_run is True


def test_live_trading_enabled_is_still_false():
    secrets = load_secrets()
    assert secrets.live_trading_enabled is False


def test_dashboard_app_still_has_no_secrets_or_order_calls():
    """Regression guard on the pre-existing v1 static check, re-affirmed
    after the v2 rebuild."""
    for forbidden in ("place_order", "cancel_order", "PRIVATE_KEY", "API_SECRET", "BOT_TOKEN"):
        assert forbidden not in APP_SOURCE
