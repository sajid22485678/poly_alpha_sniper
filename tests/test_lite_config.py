"""Safety and sizing contract for the isolated Lite lane."""
from pathlib import Path
import json

from poly_alpha_sniper.lite.lite_bot import LiteRuntimeFiles
from poly_alpha_sniper.lite.lite_config import FIXED_SHARES, LiteConfig, load_lite_config


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
""",
        encoding="utf-8",
    )
    cfg = load_lite_config(str(path))
    assert (cfg.mode, cfg.dry_run, cfg.live_enabled) == ("lite_shadow", True, False)
    assert cfg.fixed_order_shares == 5.0
    assert cfg.use_max_trade_usd is False
    assert cfg.max_trade_usd is None
    assert cfg.require_oracle is False


def test_lite_default_db_is_dedicated_requested_path():
    cfg = LiteConfig()
    assert Path(cfg.db_path).as_posix().endswith("/data/poly_alpha_lite.db")


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
