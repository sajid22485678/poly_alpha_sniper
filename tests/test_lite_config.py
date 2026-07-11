"""Safety and sizing contract for the isolated Lite lane."""
from pathlib import Path
import json
import pytest

from poly_alpha_sniper.lite.lite_bot import LiteRuntimeFiles
from poly_alpha_sniper.lite.lite_config import (
    FIXED_SHARES, LITE_CLOB_BASE_URL, LITE_DB_PATH, LITE_EXPORT_DIR,
    LITE_GAMMA_BASE_URL, LITE_RUNTIME_DIR,
    LiteConfig, load_lite_config,
)
from poly_alpha_sniper.lite.lite_risk import assess_live_small_exposure


def test_lite_defaults_are_hard_locked_shadow_only():
    cfg = load_lite_config(path="Z:/definitely/missing/config_lite.yaml")
    assert cfg.mode == "lite_shadow"
    assert cfg.dry_run is True
    assert cfg.live_enabled is False
    assert cfg.fixed_order_shares == FIXED_SHARES == 5.0


def test_lite_disables_usd_cap_and_old_trigger_gates():
    cfg = LiteConfig()
    assert cfg.use_max_trade_usd is False
    assert cfg.max_trade_usd is None
    assert cfg.require_fired is False
    assert cfg.require_imbalance is False
    assert cfg.require_oracle is False


def test_unsafe_yaml_cannot_change_safety_or_size(tmp_path):
    path = tmp_path / "lite.yaml"
    path.write_text(
        """lite_shadow:
  mode: live_full
  dry_run: false
  live_enabled: true
  fixed_order_shares: 999
  use_max_trade_usd: true
  max_trade_usd: 99999
  require_oracle: true
  db_path: C:/wrong/advanced.db
  runtime_dir: C:/wrong/runtime
  export_dir: C:/wrong/export
  live_kill_switch_engaged: false
""",
        encoding="utf-8",
    )
    cfg = load_lite_config(str(path))
    assert (cfg.mode, cfg.dry_run, cfg.live_enabled) == ("lite_shadow", True, False)
    assert cfg.fixed_order_shares == 5.0
    assert cfg.use_max_trade_usd is False
    assert cfg.max_trade_usd is None
    assert cfg.require_oracle is False
    assert cfg.db_path == LITE_DB_PATH
    assert cfg.runtime_dir == LITE_RUNTIME_DIR
    assert cfg.export_dir == LITE_EXPORT_DIR
    assert cfg.live_kill_switch_engaged is True


def test_lite_default_db_is_dedicated_requested_path():
    cfg = LiteConfig()
    assert Path(cfg.db_path).as_posix().endswith("/data/poly_alpha_lite.db")


def test_thirteen_dollar_profile_cannot_exceed_seventy_five_percent():
    first = assess_live_small_exposure(
        entry_price=0.9, committed_exposure_usd=0, equity_usd=13,
        exposure_cap_pct=0.75, fee_buffer_usd=0.02)
    assert first.allowed
    second = assess_live_small_exposure(
        entry_price=0.9, committed_exposure_usd=first.proposed_commitment_usd,
        equity_usd=13, exposure_cap_pct=0.75, fee_buffer_usd=0.02)
    assert second.allowed
    third = assess_live_small_exposure(
        entry_price=0.2,
        committed_exposure_usd=first.proposed_commitment_usd+second.proposed_commitment_usd,
        equity_usd=13, exposure_cap_pct=0.75, fee_buffer_usd=0.02)
    assert third.allowed is False
    assert third.reason == "equity_exposure_cap"


def test_live_small_profile_has_hard_kill_switch_but_no_live_order_surface():
    cfg = load_lite_config(path="Z:/definitely/missing/config_lite.yaml")
    assert cfg.live_kill_switch_engaged is True
    assert cfg.live_enabled is False and cfg.dry_run is True


def test_invalid_yaml_fails_closed_instead_of_silent_fallback(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("lite_shadow: [", encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid Lite config"):
        load_lite_config(str(path))


@pytest.mark.parametrize(
    "body",
    [
        "lite_shadow:\n  unknown_switch: true\n",
        "lite_shadow:\n  momentum_min_pct: .nan\n",
        "lite_shadow:\n  max_open_positions: '6'\n",
        "lite_shadow:\n  max_spread: 2\n",
    ],
)
def test_unknown_nonfinite_wrong_type_and_out_of_range_config_fail_closed(tmp_path, body):
    path = tmp_path / "invalid.yaml"
    path.write_text(body, encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid Lite config"):
        load_lite_config(str(path))


def test_public_data_endpoints_and_required_assets_are_hard_locked(tmp_path):
    path = tmp_path / "endpoints.yaml"
    path.write_text(
        """lite_shadow:
  assets: [DOGE]
  momentum_windows_s: [1]
  gamma_base_url: http://localhost/private
  clob_base_url: http://localhost/private
""", encoding="utf-8")
    cfg = load_lite_config(str(path))
    assert cfg.assets == ["BTC", "ETH", "SOL"]
    assert cfg.momentum_windows_s == [10, 30, 60]
    assert cfg.gamma_base_url == LITE_GAMMA_BASE_URL
    assert cfg.clob_base_url == LITE_CLOB_BASE_URL


def test_lite_has_no_advanced_order_or_usd_cap_validator_imports():
    root = Path(__file__).resolve().parent.parent / "lite"
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    for forbidden in (
        "order_validator", "position_sizer", "OrderManager", "LiveExecutor",
        "polymarket_clob_private", "load_secrets", "place_order", "cancel_order",
    ):
        assert forbidden not in source
    # The compatibility name may occur only in Lite config/tests; no strategy,
    # broker, resolver, store, or bot is allowed to consult it.
    for path in root.glob("*.py"):
        if path.name == "lite_config.py":
            continue
        assert "max_trade_usd" not in path.read_text(encoding="utf-8")


def test_lite_runtime_files_are_nested_and_never_touch_advanced_state(tmp_path):
    advanced_heartbeat = tmp_path / "heartbeat.json"
    advanced_heartbeat.write_text('{"advanced":true}', encoding="utf-8")
    cfg = LiteConfig(runtime_dir=str(tmp_path / "lite_shadow"))
    runtime = LiteRuntimeFiles(cfg.runtime_dir, cfg)
    runtime.acquire()
    try:
        state = runtime.publish({
            "open_positions": 0, "db_path": cfg.db_path,
            "mode": "live_full", "dry_run": False, "live_enabled": True,
        })
        assert state["mode"] == "lite_shadow"
        assert state["dry_run"] is True
        assert state["live_enabled"] is False
        assert json.loads(runtime.lock_path.read_text(encoding="utf-8"))["pid"] == runtime.pid
    finally:
        runtime.release({"open_positions": 0, "db_path": cfg.db_path})
    assert advanced_heartbeat.read_text(encoding="utf-8") == '{"advanced":true}'
    assert not runtime.lock_path.exists()


def test_lite_scripts_validate_windows_venv_redirector_child():
    root = Path(__file__).resolve().parent.parent
    start = (root / "scripts/start_lite_shadow.bat").read_text(encoding="utf-8")
    stop = (root / "scripts/stop_lite_shadow.ps1").read_text(encoding="utf-8")
    status = (root / "scripts/status_lite_shadow.ps1").read_text(encoding="utf-8")
    assert "sys._base_executable" in start
    assert "lock.pid -eq $process.Id" not in start
    assert "lite.lite_bot" in start
    for source in (stop, status):
        assert "sys._base_executable" in source
        assert "ExpectedExecutables" in source
        assert "lite\\.lite_bot" in source


def test_runtime_os_guard_prevents_concurrent_lite_owner(tmp_path):
    cfg = LiteConfig(runtime_dir=str(tmp_path / "lite_shadow"))
    first = LiteRuntimeFiles(cfg.runtime_dir, cfg)
    second = LiteRuntimeFiles(cfg.runtime_dir, cfg)
    first.acquire()
    try:
        with pytest.raises(RuntimeError, match="OS process guard"):
            second.acquire()
    finally:
        first.release({"open_positions": 0, "db_path": cfg.db_path})


def test_runtime_stop_request_requires_exact_launch_nonce(tmp_path):
    cfg = LiteConfig(runtime_dir=str(tmp_path / "lite_shadow"))
    runtime = LiteRuntimeFiles(cfg.runtime_dir, cfg)
    runtime.acquire()
    try:
        runtime.stop_path.write_text(json.dumps({
            "mode": "lite_shadow", "target_pid": runtime.pid,
            "launch_nonce": "0"*32,
        }), encoding="utf-8")
        assert runtime.stop_requested() is False
        runtime.stop_path.write_text(json.dumps({
            "mode": "lite_shadow", "target_pid": runtime.pid,
            "launch_nonce": runtime.launch_nonce,
        }), encoding="utf-8")
        assert runtime.stop_requested() is True
    finally:
        runtime.release({"open_positions": 0, "db_path": cfg.db_path})


def test_lite_scripts_correlate_launch_nonce_and_detect_orphans():
    root = Path(__file__).resolve().parent.parent
    start = (root / "scripts/start_lite_shadow.bat").read_text(encoding="utf-8")
    status = (root / "scripts/status_lite_shadow.ps1").read_text(encoding="utf-8")
    stop = (root / "scripts/stop_lite_shadow.ps1").read_text(encoding="utf-8")
    assert "POLY_ALPHA_LITE_LAUNCH_NONCE" in start
    assert "$ownedLaunch" in start
    assert "orphanLiteProcesses" in status
    assert "ownedLiteProcessIds" in status
    assert "ParentProcessId" in status
    assert "nonceCrosscheck" in status
    assert "launch_nonce" in stop
