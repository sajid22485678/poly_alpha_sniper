"""Safety checks for the Hermes read-only workspace at
D:\\claude\\agent_readonly\\hermes_poly_workspace\\ (outside this repo --
these tests read that fixed location directly, matching where the workspace
is actually deployed). Skipped gracefully if that location doesn't exist in
a given environment rather than failing the whole suite."""
from pathlib import Path

import pytest

from poly_alpha_sniper.core.logger import redact_text

WORKSPACE = Path("D:/claude/agent_readonly/hermes_poly_workspace")

pytestmark = pytest.mark.skipif(not WORKSPACE.exists(),
                                reason="Hermes workspace not present in this environment")

REQUIRED_FILES = [
    "README.md", "HERMES_SYSTEM_PROMPT.md", "allowed_paths.md", "forbidden_paths.md",
    "daily_operator_task.md", "live_readiness_task.md", "anomaly_watch_task.md",
    "output_templates/poly_daily_brief.md", "output_templates/poly_anomaly_brief.md",
]

REQUIRED_FORBIDDEN_MENTIONS = [
    ".env", "private key", "seed phrase", "Telegram", "OpenRouter", "Polymarket API secret",
    "start_shadow.bat", "start_dashboard.bat",
]


def _all_workspace_files():
    return [p for p in WORKSPACE.rglob("*") if p.is_file()]


def test_all_required_files_exist():
    for rel in REQUIRED_FILES:
        assert (WORKSPACE / rel).exists(), f"missing required workspace file: {rel}"


def test_workspace_contains_no_secrets():
    """Every file in the workspace passes through the same redaction filter
    the bot's structured logger uses -- if redaction would change the text,
    something secret-shaped is present and must not be here."""
    offenders = []
    for f in _all_workspace_files():
        text = f.read_text(encoding="utf-8")
        if redact_text(text) != text:
            offenders.append(str(f))
    assert offenders == [], f"secret-shaped content found in: {offenders}"


def test_workspace_contains_no_literal_env_values():
    """No file should contain what looks like an actual KEY=VALUE .env
    assignment (as opposed to prose that merely mentions ".env" as a
    concept, which is expected and fine)."""
    import re
    env_assignment = re.compile(
        r"^(TELEGRAM_BOT_TOKEN|TELEGRAM_CHAT_ID|POLYMARKET_PRIVATE_KEY|POLYMARKET_API_KEY|"
        r"POLYMARKET_API_SECRET|POLYMARKET_API_PASSPHRASE|DASHBOARD_PASSWORD)=\S+", re.MULTILINE)
    for f in _all_workspace_files():
        text = f.read_text(encoding="utf-8")
        assert not env_assignment.search(text), f"{f} contains a literal env assignment"


def test_forbidden_paths_file_documents_required_entries():
    content = (WORKSPACE / "forbidden_paths.md").read_text(encoding="utf-8")
    for term in REQUIRED_FORBIDDEN_MENTIONS:
        assert term in content, f"forbidden_paths.md does not mention {term!r}"


def test_allowed_paths_scopes_write_access_to_hermes_reports_only():
    content = (WORKSPACE / "allowed_paths.md").read_text(encoding="utf-8")
    assert "06_Poly_Hermes_Reports" in content
    # must not claim write access anywhere else in the vault
    assert "only this folder" in content.lower() or "and only this folder" in content.lower()


def test_system_prompt_states_fixed_verdict_rules():
    content = (WORKSPACE / "HERMES_SYSTEM_PROMPT.md").read_text(encoding="utf-8")
    for phrase in ("INVESTIGATE BEFORE LIVE", "NOT LIVE READY",
                   "CONTINUE SHADOW — SAMPLE TOO SMALL", "CONTINUE SHADOW"):
        assert phrase in content


def test_system_prompt_never_permits_live_recommendation():
    import re
    content = (WORKSPACE / "HERMES_SYSTEM_PROMPT.md").read_text(encoding="utf-8")
    # tolerate markdown emphasis markers (**never**) between the words
    pattern = re.compile(r"never\W*recommend\W*(enabling\W*)?live", re.IGNORECASE)
    assert pattern.search(content), "system prompt must explicitly forbid recommending live trading"
    assert 'say "ready for live"' in content.lower() or "ready for live" in content.lower()


def test_no_execute_permission_language_present():
    content = (WORKSPACE / "HERMES_SYSTEM_PROMPT.md").read_text(encoding="utf-8")
    assert "never execute" in content.lower() or "no execution capability" in content.lower()
