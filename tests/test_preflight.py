import pytest

from poly_alpha_sniper.core.config_loader import Secrets, load_config
from poly_alpha_sniper.core.startup_preflight import run_preflight


@pytest.fixture(autouse=True)
def _no_real_lock_probe(monkeypatch):
    """Preflight's duplicate-process probe reads runtime/live.lock — a REAL
    running bot must not fail the test suite."""
    from poly_alpha_sniper.core.process_lock import ProcessLock
    monkeypatch.setattr(ProcessLock, "_read", lambda self: None)


def _named(result):
    return {name: ok for name, ok, _ in result.checks}


async def test_offline_preflight_default_config_passes():
    cfg = load_config()
    secrets = Secrets(env={"DASHBOARD_AUTH_ENABLED": "false"})
    cfg.dashboard.auth_enabled = False
    cfg.telegram.enabled = False
    result = await run_preflight(cfg, secrets, offline_ok=True)
    named = _named(result)
    assert named["config_valid"]
    assert named["mode_valid"]
    assert named["database_writable"]
    assert named["logs_writable"]
    assert named["backup_writable"]
    assert result.ok


async def test_dashboard_auth_without_password_fails():
    cfg = load_config()
    cfg.telegram.enabled = False
    secrets = Secrets(env={"DASHBOARD_AUTH_ENABLED": "true"})
    result = await run_preflight(cfg, secrets, offline_ok=True)
    named = _named(result)
    assert named["dashboard_auth"] is False
    assert result.ok is False


async def test_live_mode_without_env_flags_fails_preflight():
    cfg = load_config()
    cfg.mode.trading_mode = "live_micro"
    cfg.mode.dry_run = False
    cfg.telegram.enabled = False
    cfg.dashboard.auth_enabled = False
    secrets = Secrets(env={"DASHBOARD_AUTH_ENABLED": "false"})
    result = await run_preflight(cfg, secrets, offline_ok=True)
    named = _named(result)
    assert named["live_env_gates"] is False
    assert result.ok is False


async def test_summary_text_lists_failures():
    cfg = load_config()
    cfg.telegram.enabled = False
    cfg.dashboard.auth_enabled = False
    secrets = Secrets(env={"DASHBOARD_AUTH_ENABLED": "false"})
    result = await run_preflight(cfg, secrets, offline_ok=True)
    text = result.summary_text()
    assert "PREFLIGHT" in text
    assert "config_valid" in text
