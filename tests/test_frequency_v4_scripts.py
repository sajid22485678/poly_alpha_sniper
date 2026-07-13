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
