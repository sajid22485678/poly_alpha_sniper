from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


def read(name: str) -> str:
    return (SCRIPTS / name).read_text(encoding="utf-8")


def test_runtime_launcher_targets_only_exact_v4_module_and_paths():
    start = read("start_lite_frequency_v4_shadow.ps1")
    status = read("status_lite_frequency_v4_shadow.ps1")
    stop = read("stop_lite_frequency_v4_shadow.ps1")
    combined = "\n".join((start, status, stop)).lower()
    assert "-m\",\"lite_frequency_v4.bot" in start
    assert "runtime\\lite_frequency_v4_shadow" in status
    assert "data\\poly_alpha_frequency_v4.db" in status
    assert "poly_alpha_lite.db" not in combined
    assert "lite.lite_bot" not in combined
    assert "runtime\\live" not in combined
    assert ".env" not in combined
    assert "api_key" not in combined
    assert "private_key" not in combined
    assert "process_create_time" in stop
    assert "TryParse([string]$heartbeat.ts_ms" in status


def test_dashboard_scripts_are_isolated_to_port_8504():
    texts = "\n".join(read(name) for name in (
        "start_frequency_v4_dashboard.ps1",
        "status_frequency_v4_dashboard.ps1",
        "stop_frequency_v4_dashboard.ps1",
    ))
    lowered = texts.lower()
    assert "8504" in texts
    assert "8503" not in texts
    assert "dashboard_frequency_v4" in lowered
    assert "dashboard_v3" not in lowered
    assert "Get-NetTCPConnection -State Listen -LocalPort 8504" in read(
        "start_frequency_v4_dashboard.ps1"
    )


def test_restart_and_batch_wrappers_delegate_only_to_v4_scripts():
    restart = read("restart_lite_frequency_v4_shadow.ps1").lower()
    batch = read("start_lite_frequency_v4_shadow.bat").lower()
    assert "stop_lite_frequency_v4_shadow.ps1" in restart
    assert "start_lite_frequency_v4_shadow.ps1" in restart
    assert "start_lite_frequency_v4_shadow.ps1" in batch
    assert "lite_shadow" not in restart.replace("lite_frequency_v4_shadow", "")

# C1 launcher readiness timeout regressions
def test_launcher_allows_long_integrity_startup_without_force_kill():
    start = read("start_lite_frequency_v4_shadow.ps1")
    lowered = start.lower()
    compact = "".join(lowered.split())

    assert "[validaterange(5,1800)]" in compact
    assert "$readytimeoutseconds=600" in compact
    assert "stop-process" not in lowered
    assert "[bool]$state.integrity_ok" in lowered
    assert "was not killed" in lowered
    assert "no process was killed" in lowered
    assert "exit 3" in lowered


def test_launcher_readiness_requires_correlated_safe_fresh_heartbeat():
    start = read("start_lite_frequency_v4_shadow.ps1").lower()

    required = (
        "$lock.launch_nonce",
        "$state.launch_nonce",
        "$heartbeat.launch_nonce",
        "$state.pid",
        "$heartbeat.pid",
        "$heartbeatagems",
        "$state.current_commit",
        "$heartbeat.current_commit",
        "$state.db_path",
        "$state.integrity_ok",
        "$state.dry_run",
        "$state.live_enabled",
        "$state.real_orders_possible",
        "$state.kill_switch_engaged",
        "$state.fixed_shares",
    )

    for token in required:
        assert token in start

    assert "$heartbeatagems -le 15000" in start
    assert "refusing a duplicate" in start


def test_dashboard_reader_rejects_legacy_invalid_and_future_exports():
    route = (
        ROOT
        / "dashboard_frequency_v4"
        / "src"
        / "app"
        / "api"
        / "snapshot"
        / "route.ts"
    ).read_text(encoding="utf-8")
    assert "snapshot.schema_version !== 3" in route
    assert 'error: "unsupported_export_schema"' in route
    assert 'error: "invalid_export_timestamp"' in route
    assert 'error: "future_export_timestamp"' in route
    assert "Number.isSafeInteger(generatedTsMs)" in route
    assert "MAX_FUTURE_SKEW_MS" in route
    assert "info.mtimeMs" not in route


def test_dashboard_runtime_session_card_is_terminal_state_aware():
    dashboard = (
        ROOT
        / "dashboard_frequency_v4"
        / "src"
        / "components"
        / "V4Dashboard.tsx"
    ).read_text(encoding="utf-8")
    assert 'runtimeState === "STOPPED" || runtimeState === "FAILED"' in dashboard
    assert "expectedOpenSessions = terminalRuntime ? 0 : 1" in dashboard
    assert "current_session_open" in dashboard
