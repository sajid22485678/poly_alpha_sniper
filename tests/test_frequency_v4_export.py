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


def _healthy_runtime_state(session: str) -> dict:
    return {
        "session_id": session, "pid": 12345,
        "launch_nonce": "nonce-session-v4", "heartbeat_ts_ms": NOW,
        "current_commit": "c"*40, "config_hash": "d"*64,
        "state": "RUNNING", "process_ownership_valid": True,
        "orphan_processes": 0, "execution_blocked_reason": "",
        "persistence": {
            "critical": {
                "state": "HEALTHY", "queue_depth": 0,
                "queue_capacity": 2048, "timeout_count": 0,
                "unconfirmed_command_count": 0,
            },
            "telemetry": {
                "state": "HEALTHY", "queue_depth": 0,
                "queue_capacity": 20000, "rows_dropped": 0,
                "failed_batches": 0,
                "raw_telemetry_loss_count": 0,
                "critical_evidence_incomplete_count": 0,
            },
            "operational_reads": {"state": "RUNNING"},
            "reporting": {"state": "RUNNING"},
            "maintenance": {"state": "RUNNING"},
            "runtime_io": {"state": "RUNNING"},
        },
    }


def test_non_finite_values_never_blank_the_whole_export(tmp_path):
    """One infinite number must not freeze the operator's only view.

    The export is written with allow_nan=False, so a single non-finite float
    aborted the entire write and left the dashboard serving its last good file
    while the runtime kept running -- a stale page with no failure indication.
    A model with no losing trade has an infinite profit factor, so this is a
    legitimate value that must be exported as null instead.
    """
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["model_health"] = {
            "enabled": True,
            "quarantined_models": [],
            "model_statistics": {
                "no_losses_yet": {
                    "model_name": "no_losses_yet",
                    "observations": 12,
                    "profit_factor": float("inf"),
                    "loss_asymmetry": float("inf"),
                    "expectancy": 1.25,
                    "net_pnl": 15.0,
                    "quarantined": False,
                    "reason": "",
                },
            },
        }
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state,
        )
        stats = payload["model_health"]["model_statistics"]["no_losses_yet"]
        assert stats["profit_factor"] is None
        assert stats["loss_asymmetry"] is None
        # Finite neighbours are untouched.
        assert stats["expectancy"] == 1.25
        assert stats["observations"] == 12
        # The payload is now strictly encodable, which is what the writer does.
        encoded = json.dumps(payload, allow_nan=False)
        assert "Infinity" not in encoded
        assert "NaN" not in encoded
    finally:
        store.close()


def test_inflight_critical_command_does_not_block_operational_ready(tmp_path):
    """An outstanding command inside its deadline is pipelining, not a fault."""
    store, session, _ = _store_with_health(tmp_path)
    config = FrequencyV4Config()
    try:
        inflight = _healthy_runtime_state(session)
        inflight["persistence"]["critical"].update({
            "unconfirmed_command_count": 4,
            "unconfirmed_command_oldest_age_ms": 25,
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=config, session_id=session,
            runtime_state=inflight,
        )
        assert payload["persistence"]["critical_blocked_reasons"] == []
        assert payload["persistence"]["operational_ready"] is True

        overdue = _healthy_runtime_state(session)
        overdue["persistence"]["critical"].update({
            "unconfirmed_command_count": 4,
            "unconfirmed_command_oldest_age_ms": (
                config.writer_failure_timeout_ms + 1),
        })
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=config, session_id=session,
            runtime_state=overdue,
        )
        assert "unconfirmed_critical_command" in (
            payload["persistence"]["critical_blocked_reasons"])
        assert payload["persistence"]["operational_ready"] is False

        # A writer that cannot report the age stays fail-closed.
        unknown = _healthy_runtime_state(session)
        unknown["persistence"]["critical"]["unconfirmed_command_count"] = 1
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=config, session_id=session,
            runtime_state=unknown,
        )
        assert "unconfirmed_critical_command" in (
            payload["persistence"]["critical_blocked_reasons"])
    finally:
        store.close()


def test_telemetry_blocker_clears_once_the_recovery_window_passes(tmp_path):
    """telemetry_batch_failure must not latch after the lane recovers."""
    store, session, _ = _store_with_health(tmp_path)
    config = FrequencyV4Config()
    window = config.writer_failure_timeout_ms
    try:
        def payload_for(last_failure_offset_ms: int) -> dict:
            state = _healthy_runtime_state(session)
            state["persistence"]["telemetry"].update({
                # Lifetime totals stay large and honest throughout.
                "failed_batches": 6_037,
                "rows_dropped": 211_867,
                "raw_telemetry_loss_count": 217_904,
                "last_failure_ts_ms": NOW - last_failure_offset_ms,
            })
            return build_frequency_v4_dashboard(
                store, now_ms=NOW, config=config, session_id=session,
                runtime_state=state,
            )

        inside = payload_for(window - 1)["persistence"]
        assert "telemetry_batch_failure" in inside["blocked_reasons"]
        assert inside["operational_ready"] is False
        assert inside["telemetry_recovery"]["recent_failure"] is True

        outside = payload_for(window + 1)["persistence"]
        assert "telemetry_batch_failure" not in outside["blocked_reasons"]
        assert outside["operational_ready"] is True
        assert outside["telemetry_recovery"]["recent_failure"] is False
        # Lifetime counters remain reported honestly after recovery.
        assert outside["telemetry_recovery"][
            "lifetime_telemetry_failures"] == 6_037
        assert outside["telemetry_recovery"][
            "lifetime_raw_telemetry_loss"] == 217_904
    finally:
        store.close()


def test_export_separates_runtime_heartbeat_age_from_export_age(tmp_path):
    """A stale export must never make a fresh runtime look stale."""
    store, session, _ = _store_with_health(tmp_path)
    try:
        state = _healthy_runtime_state(session)
        state["heartbeat_ts_ms"] = NOW - 400
        state["persistence"]["critical"]["heartbeat_ts_ms"] = NOW - 100
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=state,
        )
        # export_age is zero at generation; the reader ages it from
        # generated_ts_ms.  The runtime heartbeat is measured independently and
        # takes the freshest observed signal.
        assert payload["export_age_ms"] == 0
        assert payload["generated_ts_ms"] == NOW
        assert payload["runtime_heartbeat_age_ms"] == 0
        assert payload["runtime_heartbeat_ts_ms"] == NOW
        assert payload["heartbeat_age_ms"] == 400
    finally:
        store.close()


def test_export_snapshot_is_v4_only_and_surfaces_safety_health_capacity(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(), session_id=session,
            runtime_state=_healthy_runtime_state(session),
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
        assert payload["database"]["wal_autocheckpoint"] == 0
        assert payload["persistence"]["operational_ready"] is True
        assert payload["persistence"]["critical_execution_ready"] is True
        assert payload["persistence"]["critical_blocked_reasons"] == []
        assert payload["persistence"]["connection_ownership"] == {
            "critical_writer": "dedicated_writer_thread",
            "telemetry": "dedicated_aggregator_and_writer_connection",
            "operational_reads": "dedicated_operational_read_only_worker",
            "reporting": "dedicated_report_read_only_worker",
            "maintenance": "dedicated_maintenance_worker",
        }
        assert payload["effective_config"]["critical_queue_capacity"] == 2048
        assert payload["effective_config"]["maintenance_max_rows_per_pass"] == 4000
        encoded = json.dumps(payload).lower()
        for forbidden in ("gamma_base_url", "clob_ws_url", "nonce-session-v4", "api_key"):
            assert forbidden not in encoded
        assert payload["acceptance_gate"]["live_enablement_authorized"] is False
    finally:
        store.close()


def test_export_separates_critical_block_from_lossy_raw_telemetry(tmp_path):
    store, session, _ = _store_with_health(tmp_path)
    try:
        # Lifetime raw-telemetry loss with NO recent failure must NOT latch the
        # dashboard degraded: the lossy lane has recovered, the critical lane is
        # healthy, and historical counters are informational only.  The lifetime
        # totals stay exposed in telemetry_recovery for auditability.
        recovered = _healthy_runtime_state(session)
        recovered["persistence"]["telemetry"]["rows_dropped"] = 3
        recovered["persistence"]["telemetry"]["raw_telemetry_loss_count"] = 3
        recovered["persistence"]["telemetry"]["failed_batches"] = 1
        recovered["persistence"]["telemetry"]["last_failure_ts_ms"] = 0
        recovered["persistence"]["telemetry"]["last_overflow_ts_ms"] = 0
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(),
            session_id=session, runtime_state=recovered,
        )
        assert payload["persistence"]["critical_execution_ready"] is True
        assert payload["persistence"]["operational_ready"] is True
        assert payload["persistence"]["telemetry_recovery"]["lifetime_raw_telemetry_loss"] == 3
        assert payload["persistence"]["telemetry_recovery"]["recent_failure"] is False
        assert "raw_telemetry_loss" not in payload["persistence"]["blocked_reasons"]

        # A failure within the recent window IS an active degradation: the lane
        # is currently unhealthy and operational_ready must reflect it.
        active = _healthy_runtime_state(session)
        active["persistence"]["telemetry"]["failed_batches"] = 5
        active["persistence"]["telemetry"]["last_failure_ts_ms"] = NOW - 1_000
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(),
            session_id=session, runtime_state=active,
        )
        assert payload["persistence"]["critical_execution_ready"] is True
        assert payload["persistence"]["operational_ready"] is False
        assert "telemetry_batch_failure" in payload["persistence"]["blocked_reasons"]
        assert payload["persistence"]["telemetry_recovery"]["recent_failure"] is True

        latched = _healthy_runtime_state(session)
        latched["execution_blocked_reason"] = "critical_command_failed"
        payload = build_frequency_v4_dashboard(
            store, now_ms=NOW, config=FrequencyV4Config(),
            session_id=session, runtime_state=latched,
        )
        assert payload["persistence"]["critical_execution_ready"] is False
        assert any(
            reason.endswith("critical_command_failed")
            for reason in payload["persistence"]["critical_blocked_reasons"]
        )
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


def test_atomic_export_writes_only_caller_v4_json_and_no_temp_file(tmp_path, monkeypatch):
    # C1.H uses strict canonical containment: the guard accepts writes only
    # under canonical_export_dir().  Tests repoint the canonical root at
    # tmp_path via the single monkeypatchable derivation rather than
    # loosening the guard.
    from poly_alpha_sniper.lite_frequency_v4 import export as export_mod
    monkeypatch.setattr(export_mod, "canonical_export_dir", lambda: tmp_path)
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


# --- C1.H export guard regression suite -----------------------------------
# Strict canonical containment (design a).  The guard must accept writes only
# under canonical_export_dir() and refuse every other path.  Tests repoint
# the canonical root at tmp_path via the single monkeypatchable derivation.

from poly_alpha_sniper.lite_frequency_v4 import export as _export_mod  # noqa: E402


def _point_canonical_at(monkeypatch, root: Path) -> None:
    """Repoint canonical_export_dir() at ``root`` for hermetic testing."""
    monkeypatch.setattr(_export_mod, "canonical_export_dir", lambda: root)


def test_c1h_canonical_destination_accepted(tmp_path, monkeypatch):
    # A path directly under the canonical dir is accepted.
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    resolved = _export_mod.assert_canonical_export_path(canon / "out.json")
    assert resolved == (canon / "out.json").resolve()


def test_c1h_canonical_subdirectory_accepted(tmp_path, monkeypatch):
    canon = tmp_path / "canon"
    _point_canonical_at(monkeypatch, canon)
    nested = canon / "sub" / "deep"
    resolved = _export_mod.assert_canonical_export_path(nested)
    # Directory input appends EXPORT_FILENAME.
    assert resolved == (nested / _export_mod.EXPORT_FILENAME).resolve()


def test_c1h_canonical_dir_root_itself_accepted(tmp_path, monkeypatch):
    # The canonical dir itself is accepted (path is root or descendant).
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    resolved = _export_mod.assert_canonical_export_path(canon)
    assert resolved == (canon / _export_mod.EXPORT_FILENAME).resolve()


def test_c1h_live_production_source_tree_refused(tmp_path, monkeypatch):
    """The live production repo root must be refused from a staging test."""
    _point_canonical_at(monkeypatch, tmp_path)
    live_repo = Path(r"D:\claude\poly_alpha_sniper")
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(live_repo)


def test_c1h_live_production_source_subdir_refused(tmp_path, monkeypatch):
    _point_canonical_at(monkeypatch, tmp_path)
    live_sub = Path(r"D:\claude\poly_alpha_sniper\lite_frequency_v4")
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(live_sub)


def test_c1h_arbitrary_path_refused(tmp_path, monkeypatch):
    _point_canonical_at(monkeypatch, tmp_path)
    arbitrary = Path(r"C:\Windows\Temp\anywhere")
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(arbitrary)


def test_c1h_sibling_directory_refused(tmp_path, monkeypatch):
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    sibling = tmp_path / "sibling"
    sibling.mkdir()
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(sibling)


def test_c1h_parent_directory_refused(tmp_path, monkeypatch):
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    # tmp_path is the PARENT of the canonical dir; must be refused.
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(tmp_path)


def test_c1h_nested_unauthorized_directory_refused(tmp_path, monkeypatch):
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    nested_unauth = tmp_path / "other_root" / "deep" / "tree"
    nested_unauth.mkdir(parents=True)
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(nested_unauth)


def test_c1h_dotdot_relative_escape_refused(tmp_path, monkeypatch):
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    # A path that uses .. to climb ABOVE the canonical dir resolves outside
    # it and must be refused (relative-path escape).
    escape = canon / ".." / "escaped.json"
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(escape)


def test_c1h_absolute_path_with_traversal_refused(tmp_path, monkeypatch):
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    # An absolute path that embeds .. to leave the canonical dir.
    escape = (canon / "sub" / ".." / ".." / "escape.json").resolve()
    with pytest.raises(ValueError, match="outside the canonical export directory"):
        _export_mod.assert_canonical_export_path(escape)


def test_c1h_case_and_separator_normalization_handled(tmp_path, monkeypatch):
    # Windows: drive letter and path comparison are case-insensitive; both
    # / and \ must compare equal.  The canonical path with different case
    # and separators is still recognized as canonical.
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    # Build a path under canon but with mixed separators/case on the drive.
    resolved_canon = str(canon.resolve())
    mixed = resolved_canon.replace("\\", "/") + "/report.JSON"
    resolved = _export_mod.assert_canonical_export_path(mixed)
    # .JSON (uppercase) is treated as a .json file (suffix check is case-insensitive)
    assert resolved.name == "report.JSON"


def test_c1h_write_path_uses_guard(tmp_path, monkeypatch):
    """write_frequency_v4_dashboard routes through the guard and refuses."""
    canon = tmp_path / "canon"
    canon.mkdir()
    _point_canonical_at(monkeypatch, canon)
    store, session, _ = _store_with_health(tmp_path)
    try:
        # Writing outside the canonical dir must fail before any file is built.
        outside = tmp_path / "outside"
        outside.mkdir()
        with pytest.raises(ValueError, match="outside the canonical export directory"):
            write_frequency_v4_dashboard(
                store, outside, now_ms=NOW, config=FrequencyV4Config(),
                session_id=session,
                runtime_state={"session_id": session, "heartbeat_ts_ms": NOW},
            )
        # And no file may have been written there.
        assert not list(outside.iterdir())
    finally:
        store.close()


def test_c1h_no_hardcoded_production_path_in_module():
    """C1.H exists to remove hardcoded production paths; none may survive."""
    source = (Path(__file__).resolve().parent.parent /
              "lite_frequency_v4" / "export.py").read_text(encoding="utf-8")
    # The live production path must not be hardcoded anywhere in export.py.
    assert r"D:\claude\poly_alpha_sniper" not in source
    assert "poly_alpha_sniper" not in source


def test_c1h_canonical_dir_is_single_source_of_truth():
    """canonical_export_dir() is the only canonical-path derivation; no
    dead _CANONICAL_EXPORT_DIR import or _production_live_export_dir()."""
    source = (Path(__file__).resolve().parent.parent /
              "lite_frequency_v4" / "export.py").read_text(encoding="utf-8")
    assert "_production_live_export_dir" not in source
    assert "_CANONICAL_EXPORT_DIR" not in source
