"""WS2: safety checks for the auto-export loop scripts. Static source
analysis (matching tests/test_dashboard_v2_safety.py's approach) rather
than actually running PowerShell in the test suite -- the scripts were
already live-verified manually (start/status/stop cycle, BOM-free JSON,
vault copy) during development; these tests lock in the safety properties
that must never regress."""
from __future__ import annotations

import re

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = PROJECT_ROOT / "scripts"


def _strip_comments(text: str) -> str:
    """Strip PowerShell/batch comment lines (# ... / REM ...) so safety-
    explaining prose (e.g. 'never touches .env') doesn't trip a naive
    substring check for the very terms it forbids."""
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or stripped.upper().startswith("REM "):
            continue
        lines.append(line)
    return "\n".join(lines)


LOOP = (SCRIPTS / "auto_export_loop.ps1").read_text(encoding="utf-8")
START_BAT = (SCRIPTS / "start_auto_export_loop.bat").read_text(encoding="utf-8")
STOP = (SCRIPTS / "stop_auto_export_loop.ps1").read_text(encoding="utf-8")
STATUS = (SCRIPTS / "status_auto_export_loop.ps1").read_text(encoding="utf-8")
INSTALL = (SCRIPTS / "install_auto_export_startup_task.ps1").read_text(encoding="utf-8")
UNINSTALL = (SCRIPTS / "uninstall_auto_export_startup_task.ps1").read_text(encoding="utf-8")

LOOP_CODE = _strip_comments(LOOP)
START_BAT_CODE = _strip_comments(START_BAT)
STOP_CODE = _strip_comments(STOP)
STATUS_CODE = _strip_comments(STATUS)
INSTALL_CODE = _strip_comments(INSTALL)
UNINSTALL_CODE = _strip_comments(UNINSTALL)


def test_all_six_required_files_exist():
    for name in ("auto_export_loop.ps1", "start_auto_export_loop.bat",
                "stop_auto_export_loop.ps1", "status_auto_export_loop.ps1",
                "install_auto_export_startup_task.ps1", "uninstall_auto_export_startup_task.ps1"):
        assert (SCRIPTS / name).exists(), f"missing {name}"


def test_loop_uses_the_python_exporter_directly():
    assert "export_agent_readonly.py" in LOOP
    assert ".venv\\Scripts\\python.exe" in LOOP or "python.exe" in LOOP


def test_loop_sleeps_ten_seconds_by_default():
    assert "[int]$IntervalSeconds = 10" in LOOP
    assert "Start-Sleep -Seconds $IntervalSeconds" in LOOP


def test_loop_is_persistent_not_spawned_per_export():
    """The loop must be its own while(true) with an internal sleep, not a
    script that runs once and relies on an external scheduler to fire it
    every 10s."""
    assert "while ($true)" in LOOP


def test_loop_does_not_crash_permanently_on_one_failed_export():
    assert "consecutive_failures" in LOOP
    assert "does not re-throw" in LOOP or "catch" in LOOP
    # the retry sleep must be OUTSIDE the try/catch's failure path, i.e. the
    # loop keeps going -- verified structurally: no `exit`/`return`/`break`
    # inside the catch block that would end the loop after one failure.
    catch_block = LOOP.split("} catch {", 1)[1].split("}", 1)[0]
    assert "exit" not in catch_block
    assert "break" not in catch_block


def test_loop_never_overwrites_vault_files_with_empty_content():
    assert "Length -gt 0" in LOOP
    assert LOOP.count("Length -gt 0") >= 2  # both report + daily note copies gated


def test_loop_status_json_has_all_required_fields():
    required = ("running", "pid", "last_started_at", "last_finished_at",
               "last_success_at", "last_error_at", "last_error_message",
               "interval_seconds", "exports_completed", "consecutive_failures",
               "lock_active", "log_path", "status_path")
    for field in required:
        assert field in LOOP, f"status field {field} missing from auto_export_loop.ps1"


def test_loop_writes_bom_free_json():
    """PowerShell's Set-Content -Encoding utf8 writes a BOM that breaks
    Node's JSON.parse in Dashboard V3 -- must use the BOM-free writer."""
    assert "UTF8Encoding $false" in LOOP or "UTF8Encoding($false)" in LOOP.replace(" ", "")


def test_start_bat_does_not_block_and_has_no_pause():
    # a bare `pause` command line, not the word "pause" appearing in prose
    assert not re.search(r"^\s*pause\s*$", START_BAT_CODE, re.IGNORECASE | re.MULTILINE)
    assert "Start-Process" in START_BAT  # detached, so the .bat returns immediately


def test_stop_script_only_targets_powershell_processes():
    """Stale/reused-PID protection: refuses to Stop-Process anything that
    isn't actually a powershell/pwsh process."""
    assert 'notin @("powershell", "pwsh")' in STOP
    assert "refusing to stop it" in STOP


def test_stop_script_never_references_the_bot_dashboard_or_other_processes():
    forbidden = ("core.app", "dashboard\\app.py", "streamlit", "node.exe", "next dev")
    for term in forbidden:
        assert term not in STOP


def test_stop_script_patches_status_after_forced_kill():
    assert "running = $false" in STOP or "running=$false" in STOP.replace(" ", "")


def test_status_script_is_read_only():
    forbidden = ("Stop-Process", "Remove-Item", "Set-Content", "Start-Process")
    for term in forbidden:
        assert term not in STATUS


def test_install_task_only_starts_the_loop_at_logon_not_the_exporter_directly():
    assert "auto_export_loop.ps1" in INSTALL
    assert "export_agent_readonly.py" not in INSTALL
    assert "AtLogOn" in INSTALL


def test_uninstall_task_does_not_stop_a_running_loop():
    """Removing the scheduled task must not itself kill an already-running
    loop process -- that's stop_auto_export_loop.ps1's job."""
    assert "Stop-Process" not in UNINSTALL


def test_no_script_touches_env_or_secrets():
    forbidden = (".env", "PRIVATE_KEY", "API_SECRET", "API_KEY", "BOT_TOKEN", "TELEGRAM_")
    for src in (LOOP_CODE, START_BAT_CODE, STOP_CODE, STATUS_CODE, INSTALL_CODE, UNINSTALL_CODE):
        for term in forbidden:
            assert term not in src


def test_no_script_places_or_cancels_orders():
    forbidden = ("place_order", "cancel_order", "clob.polymarket.com", "gamma-api.polymarket.com")
    for src in (LOOP_CODE, START_BAT_CODE, STOP_CODE, STATUS_CODE, INSTALL_CODE, UNINSTALL_CODE):
        for term in forbidden:
            assert term not in src
