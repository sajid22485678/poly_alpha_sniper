from pathlib import Path

import pytest

from poly_alpha_sniper.lite_frequency_v4.config import (
    FIXED_SHARES,
    FREQUENCY_V4_DB_PATH,
    FREQUENCY_V4_EXPORT_DIR,
    FREQUENCY_V4_RUNTIME_DIR,
    MODE,
    STRATEGY_ID,
    FrequencyV4Config,
    load_frequency_v4_config,
    validate_frequency_v4_config,
)
from poly_alpha_sniper.lite_frequency_v4.contracts import (
    CexObservation,
    SourceEvent,
)


def test_defaults_are_permanently_shadow_only_and_isolated():
    cfg = load_frequency_v4_config("Z:/missing/frequency-v4.yaml")
    assert cfg.strategy_id == STRATEGY_ID == "lite_frequency_v4"
    assert cfg.mode == MODE == "lite_frequency_v4_shadow"
    assert cfg.dry_run is True
    assert cfg.live_enabled is False
    assert cfg.real_orders_possible is False
    assert cfg.live_adapter_present is False
    assert cfg.kill_switch_engaged is True
    assert cfg.fixed_shares == FIXED_SHARES == 5.0
    assert cfg.db_path == FREQUENCY_V4_DB_PATH
    assert cfg.runtime_dir == FREQUENCY_V4_RUNTIME_DIR
    assert cfg.export_dir == FREQUENCY_V4_EXPORT_DIR
    assert "poly_alpha_lite.db" not in cfg.db_path
    assert "runtime/lite_shadow" not in Path(cfg.runtime_dir).as_posix()


def test_unsafe_yaml_cannot_change_identity_safety_paths_or_size(tmp_path):
    path = tmp_path / "unsafe.yaml"
    path.write_text(
        """lite_frequency_v4_shadow:
  strategy_id: wrong
  mode: live_full
  enabled: false
  dry_run: false
  live_enabled: true
  real_orders_possible: true
  live_adapter_present: true
  kill_switch_engaged: false
  fixed_shares: 999
  db_path: C:/wrong.db
  runtime_dir: C:/wrong-runtime
  export_dir: C:/wrong-export
  required_assets: [DOGE]
  discover_additional_assets: false
  exact_window_seconds: 900
  strong_cross_edge: 0.025
""",
        encoding="utf-8",
    )
    cfg = load_frequency_v4_config(str(path))
    assert cfg.safety_state == {
        "strategy_id": "lite_frequency_v4",
        "mode": "lite_frequency_v4_shadow",
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "kill_switch_engaged": True,
        "fixed_shares": 5.0,
    }
    assert (cfg.db_path, cfg.runtime_dir, cfg.export_dir) == (
        FREQUENCY_V4_DB_PATH, FREQUENCY_V4_RUNTIME_DIR,
        FREQUENCY_V4_EXPORT_DIR)
    assert cfg.required_assets == ["BTC", "ETH", "SOL"]
    assert cfg.discover_additional_assets is True
    assert cfg.exact_window_seconds == 300
    assert cfg.strong_cross_edge == 0.025  # a transparent tunable did apply


def test_initial_tiers_and_maker_timing_match_v4_mission():
    cfg = FrequencyV4Config()
    assert cfg.strong_cross_edge == 0.020
    assert cfg.medium_maker_edge == 0.010
    assert cfg.weak_observe_edge == 0.005
    assert (cfg.maker_observation_min_ms,
            cfg.maker_observation_default_ms,
            cfg.maker_observation_max_ms) == (500, 1000, 1500)
    # 100% of the authoritative cohort equity may be committed.
    assert cfg.exposure_cap_pct == 1.0
    assert cfg.exposure_cap_usd == 130.0


def test_persistence_controls_are_explicit_bounded_and_do_not_change_strategy():
    cfg = FrequencyV4Config()
    assert cfg.critical_queue_capacity == 2_048
    assert cfg.telemetry_queue_capacity == 20_000
    assert cfg.critical_command_timeout_s == 15.0
    assert cfg.telemetry_batch_size == 512
    assert cfg.telemetry_flush_interval_ms == 250
    assert cfg.writer_heartbeat_interval_ms == 1_000
    assert cfg.checkpoint_wal_size_trigger_bytes == 32 * 1024 * 1024
    assert cfg.retention_chunk_size == 250
    validate_frequency_v4_config(cfg)
    assert (cfg.strong_cross_edge, cfg.medium_maker_edge,
            cfg.weak_observe_edge, cfg.fixed_shares) == (0.020, 0.010, 0.005, 5.0)


@pytest.mark.parametrize("body", [
    "lite_frequency_v4_shadow:\n  unknown_field: 1\n",
    "lite_frequency_v4_shadow:\n  strong_cross_edge: .nan\n",
    "lite_frequency_v4_shadow:\n  weak_observe_edge: 0.03\n  strong_cross_edge: 0.02\n",
    "lite_frequency_v4_shadow:\n  maker_observation_min_ms: 499\n",
    "lite_frequency_v4_shadow:\n  maker_observation_max_ms: 1501\n",
    "lite_frequency_v4_shadow:\n  rest_recovery_min_ms: 751\n",
    "lite_frequency_v4_shadow:\n  fair_probability_ceiling: 1.0\n",
    "lite_frequency_v4_shadow:\n  max_open_positions: false\n",
    "lite_frequency_v4_shadow:\n  critical_queue_capacity: 0\n",
    "lite_frequency_v4_shadow:\n  telemetry_batch_size: 1000\n  telemetry_queue_capacity: 100\n",
    "lite_frequency_v4_shadow:\n  critical_command_timeout_s: 1\n",
    "lite_frequency_v4_shadow:\n  checkpoint_wal_size_trigger_bytes: 100\n",
])
def test_nonfinite_unknown_wrong_type_and_out_of_range_fail_closed(tmp_path, body):
    path = tmp_path / "bad.yaml"
    path.write_text(body, encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid Frequency V4 config"):
        load_frequency_v4_config(str(path))


def test_direct_config_mutation_is_rejected_by_validator():
    cfg = FrequencyV4Config(live_enabled=True)
    with pytest.raises(RuntimeError, match="safety lock"):
        validate_frequency_v4_config(cfg)


def test_contracts_preserve_connection_epoch_and_separate_timestamps():
    event = SourceEvent(
        source="okx", channel="trades", event_type="trade",
        event_key="okx:1", payload_hash="a" * 64,
        provider_ts_ms=1000, receipt_ts_ms=1001,
        receipt_monotonic_ns=99, connection_epoch=3,
    )
    tick = CexObservation(
        provider="okx", asset="btc", instrument="BTC-USDT", price=100.0,
        provider_ts_ms=1000, receipt_ts_ms=1001,
        receipt_monotonic_ns=99, event_id="1", event_type="trade",
        side="BUY", connection_epoch=3,
    )
    assert event.to_dict()["connection_epoch"] == 3
    assert tick.asset == "BTC" and tick.side == "buy"
    assert tick.to_dict()["provider_ts_ms"] != tick.to_dict()["receipt_ts_ms"]


def test_v4_foundation_has_no_legacy_or_authenticated_runtime_imports():
    root = Path(__file__).resolve().parent.parent / "lite_frequency_v4"
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    assert "poly_alpha_sniper.lite" not in source
    for forbidden in (
        "load_secrets", "polymarket_clob_private", "OrderManager",
        "LiveExecutor", "place_order(", "cancel_order(",
    ):
        assert forbidden not in source
