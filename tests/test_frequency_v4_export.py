import json
from pathlib import Path

import pytest

from poly_alpha_sniper.lite_frequency_v4.config import FrequencyV4Config
from poly_alpha_sniper.lite_frequency_v4.export import (
    EXPORT_FILENAME,
    assert_v4_safety,
    build_frequency_v4_dashboard,
    write_frequency_v4_dashboard,
)
from poly_alpha_sniper.lite_frequency_v4.store import V4Store
from tests.test_frequency_v4_store import NOW, seed_market_window, seed_session


def _store_with_health(tmp_path):
    store = V4Store(tmp_path / "poly_alpha_frequency_v4.db")
    session = seed_session(store, started=NOW-2*3_600_000)
    context = seed_market_window(store, open_ts=NOW-300_000)
    store.record_source_health({
        "session_id": session,
        "source": "OKX",
        "channel": "tickers",
        "sample_ts_ms": NOW,
        "status": "HEALTHY",
        "connected": True,
        "hydrated": True,
        "heartbeat_age_ms": 20,
        "ping_age_ms": 10,
        "last_provider_ts_ms": NOW-5,
        "last_receipt_ts_ms": NOW-2,
        "freshness_ms": 5,
        "reconnect_count": 0,
        "sequence_gap_count": 0,
        "duplicate_count": 0,
        "future_count": 0,
        "regressed_count": 0,
        "rest_recovery_status": "READY",
    })
    store.record_runtime_health({
        "session_id": session,
        "sample_ts_ms": NOW,
        "heartbeat_ts_ms": NOW,
        "pid": 12345,
        "state": "RUNNING",
        "loop_lag_ms": 4,
        "db_writes_per_min": 12,
        "db_size_bytes": store.database_size_bytes(),
        "open_positions": 0,
    })
    return store, session, context


def test_export_snapshot_is_v4_only_and_surfaces_safety_health_capacity(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state={
                "session_id": session, "pid": 12345,
                "launch_nonce": "nonce-session-v4", "heartbeat_ts_ms": NOW,
                "current_commit": "c"*40, "config_hash": "d"*64,
                "state": "RUNNING", "process_ownership_valid": True,
                "orphan_processes": 0,
            },
        )
        assert payload["strategy_id"] == "lite_frequency_v4"
        assert payload["mode"] == "lite_frequency_v4_shadow"
        assert payload["dry_run"] is True
        assert payload["live_enabled"] is False
        assert payload["real_orders_possible"] is False
        assert payload["live_adapter_present"] is False
        assert payload["kill_switch_engaged"] is True
        assert payload["fixed_shares"] == 5.0
        assert payload["current_commit"] == "c"*40
        assert payload["heartbeat_ts_ms"] == NOW
        assert payload["safety"]["dry_run"] is True
        assert payload["safety"]["live_enabled"] is False
        assert payload["safety"]["real_orders_possible"] is False
        assert payload["safety"]["live_adapter_present"] is False
        assert payload["safety"]["kill_switch_engaged"] is True
        assert payload["safety"]["fixed_shares"] == 5.0
        assert payload["safety"]["quota_can_override_economic_gate"] is False
        assert payload["runtime"]["current_commit"] == "c"*40
        assert payload["runtime"]["launch_nonce_fingerprint"]
        assert "nonce-session-v4" not in json.dumps(payload)
        assert payload["source_policy"]["primary_cex"] == "OKX"
        assert payload["sources"][0]["status"] == "HEALTHY"
        assert payload["universe"]["exact_duration_ms"] == 300_000
        assert payload["market_universe"] == payload["universe"]
        assert payload["frequency"]["1h"]["theoretical_max_trades_per_hour"] == 12
        assert payload["funnel"]["raw_events"] == 0
        assert payload["funnel"]["available_asset_windows"] == 1
        assert payload["performance"]["verified_terminal"]["count"] == 0
        assert payload["pnl"]["count"] == 0
        assert payload["compound_preview"]["influences_sizing"] is False
        assert payload["compounding_preview"] == payload["compound_preview"]
        assert payload["exposure"] == payload["positions"]
        assert payload["integrity"] == {
            "sqlite_integrity": "ok", "foreign_key_violations": 0,
            "conflicts": 0, "duplicates": 0, "unresolved_final": 0,
            "maker_fill_assumed_count": 0,
        }
        assert payload["database"]["path_name"] == "poly_alpha_frequency_v4.db"
        assert payload["database"]["legacy_data_included"] is False
        assert payload["acceptance_gate"]["live_enablement_authorized"] is False
    finally:
        store.close()


@pytest.mark.parametrize(
    "unsafe",
    [
        {"mode": "lite_shadow"},
        {"dry_run": False},
        {"live_enabled": True},
        {"real_orders_possible": True},
        {"live_adapter_present": True},
        {"kill_switch_engaged": False},
        {"fixed_shares": 6.0},
    ],
)
def test_export_fails_closed_for_any_unsafe_configuration(unsafe):
    baseline = {
        "strategy_id": "lite_frequency_v4",
        "mode": "lite_frequency_v4_shadow",
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "kill_switch_engaged": True,
        "fixed_shares": 5.0,
    }
    baseline.update(unsafe)
    with pytest.raises(RuntimeError, match="safety lock failed"):
        assert_v4_safety(baseline)


def test_partial_config_cannot_inherit_safe_defaults_silently():
    with pytest.raises(RuntimeError, match="missing required fields"):
        assert_v4_safety({})


def test_atomic_export_writes_only_caller_v4_json_and_no_temp_file(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    output = tmp_path / "readonly_v4"
    try:
        result = write_frequency_v4_dashboard(
            store, output, now_ms=NOW, config=FrequencyV4Config(),
            session_id=session,
            runtime_state={"session_id": session, "heartbeat_ts_ms": NOW},
        )
        path = Path(result["path"])
        assert path == output / EXPORT_FILENAME
        decoded = json.loads(path.read_text(encoding="utf-8"))
        assert decoded["mode"] == "lite_frequency_v4_shadow"
        assert decoded["database"]["legacy_data_included"] is False
        assert sorted(child.name for child in output.iterdir()) == [EXPORT_FILENAME]
        assert not list(output.glob("*.tmp"))
        assert result["bytes"] == path.stat().st_size
    finally:
        store.close()


def test_export_module_has_no_legacy_store_or_execution_imports():
    source = (Path(__file__).resolve().parent.parent /
              "lite_frequency_v4" / "export.py").read_text(encoding="utf-8")
    for forbidden in (
        "lite.lite_store", "storage.sqlite_store", "reporting.agent_export",
        "place_order", "cancel_order", "polymarket_clob_private",
    ):
        assert forbidden not in source
