"""Safety checks for the LAN mobile-dashboard access layer (proxy auth +
scripts). Static source analysis -- the scripts were live-validated manually
(0.0.0.0 bind, 401 without password / 200 with, POST -> 405, firewall
Private/LocalSubnet only). These tests lock in the safety properties that must
never regress: LAN-only, auth-gated, read-only, no public exposure, no trading
ports, and no touching of the bot/exporter/.env."""
from __future__ import annotations

import re
from pathlib import Path


def _strip_ts_comments(src: str) -> str:
    """Remove /* */ block comments and // line comments so safety-explaining
    prose (which names the very things it forbids) doesn't trip a substring
    scan -- only real code is checked."""
    src = re.sub(r"/\*(?:.|\n)*?\*/", "", src)
    return "\n".join(l for l in src.splitlines() if not l.strip().startswith("//"))


def _strip_script_comments(src: str) -> str:
    """Strip PowerShell/batch comment lines (# ... / REM ...)."""
    out = []
    for line in src.splitlines():
        s = line.strip()
        if s.startswith("#") or s.upper().startswith("REM ") or s.upper() == "REM":
            continue
        out.append(line)
    return "\n".join(out)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = PROJECT_ROOT / "scripts"
PROXY = PROJECT_ROOT / "dashboard_v3" / "src" / "proxy.ts"
DOCS = PROJECT_ROOT / "docs" / "mobile_dashboard_lan_access.md"

START = (SCRIPTS / "start_dashboard_v3_lan.bat").read_text(encoding="utf-8")
STATUS = (SCRIPTS / "status_dashboard_v3_lan.ps1").read_text(encoding="utf-8")
STOP = (SCRIPTS / "stop_dashboard_v3_lan.ps1").read_text(encoding="utf-8")
PRINT_URL = (SCRIPTS / "print_mobile_dashboard_url.ps1").read_text(encoding="utf-8")
FW_ALLOW = (SCRIPTS / "allow_dashboard_v3_lan_firewall.ps1").read_text(encoding="utf-8")
FW_DISABLE = (SCRIPTS / "disable_dashboard_v3_lan_firewall.ps1").read_text(encoding="utf-8")


def test_all_lan_files_exist():
    for p in (PROXY, DOCS, SCRIPTS / "start_dashboard_v3_lan.bat",
              SCRIPTS / "status_dashboard_v3_lan.ps1", SCRIPTS / "stop_dashboard_v3_lan.ps1",
              SCRIPTS / "print_mobile_dashboard_url.ps1",
              SCRIPTS / "allow_dashboard_v3_lan_firewall.ps1",
              SCRIPTS / "disable_dashboard_v3_lan_firewall.ps1"):
        assert p.exists(), f"missing {p}"


# --- proxy (Basic Auth) safety -------------------------------------------

def test_proxy_gates_on_password_and_returns_401():
    src = PROXY.read_text(encoding="utf-8")
    assert "DASHBOARD_LAN_PASSWORD" in src
    assert "401" in src
    assert "WWW-Authenticate" in src


def test_proxy_is_read_only_no_write_or_mutation():
    code = _strip_ts_comments(PROXY.read_text(encoding="utf-8"))
    # a gate only -- must not introduce any write/order capability
    for forbidden in ("place_order", "cancel_order", "fetch(", "writeFile", "POST"):
        assert forbidden not in code, f"proxy CODE references {forbidden!r}"


def test_proxy_never_reads_dotenv_file_or_secret_keys():
    code = _strip_ts_comments(PROXY.read_text(encoding="utf-8"))
    # process.env.DASHBOARD_LAN_PASSWORD (a user-chosen LAN viewing password)
    # is allowed and is the ONLY env var read. Reading the .env FILE, using
    # dotenv, or touching any real secret key is not.
    for forbidden in ("dotenv", "readFileSync", 'readFile(".env', "PRIVATE_KEY",
                      "API_SECRET", "API_KEY", "TELEGRAM", "BOT_TOKEN", "POLYMARKET"):
        assert forbidden not in code, f"proxy CODE references {forbidden!r}"
    assert "DASHBOARD_LAN_PASSWORD" in code  # the only env var it reads
    # exactly one process.env access -> it can't be scraping other secrets
    assert code.count("process.env") == 1


# --- start script: LAN + auth-required, never touches bot ------------------

def test_start_binds_lan_and_requires_password():
    assert "dev:lan" in START or "0.0.0.0" in START
    # refuses to start LAN mode without a password (batch: if "%VAR%"=="")
    assert '"%DASHBOARD_LAN_PASSWORD%"==""' in START
    assert "aborted" in START.lower() or "abort" in START.lower()


def test_start_does_not_touch_bot_or_exporter_or_secrets():
    low = _strip_script_comments(START).lower()
    for forbidden in ("main.py", "--mode live", "live_micro", "live_full", ".env",
                      "start_shadow", "stop_auto_export", "place_order"):
        assert forbidden not in low, f"start script CODE references {forbidden!r}"


def test_no_public_tunnel_or_port_forward_anywhere():
    """No file may INVOKE a public tunnel or port-forward tool. (The docs are
    allowed -- indeed required -- to WARN against them; those warnings live in
    prose, so we check for actual command invocations, not the bare word.)"""
    tunnel_invocations = (
        re.compile(r"\bngrok\b\s+\w", re.I),          # e.g. "ngrok http 8503"
        re.compile(r"\bcloudflared\b\s+\w", re.I),    # e.g. "cloudflared tunnel ..."
        re.compile(r"\blocaltunnel\b\s+\w", re.I),
        re.compile(r"portproxy", re.I),               # netsh interface portproxy
        re.compile(r"UPnP", re.I),
    )
    for name, src in (("start", START), ("status", STATUS), ("stop", STOP),
                      ("print_url", PRINT_URL), ("fw_allow", FW_ALLOW), ("fw_disable", FW_DISABLE)):
        for pat in tunnel_invocations:
            assert not pat.search(src), f"{name} invokes a tunnel/forward tool: {pat.pattern}"


# --- firewall: dashboard port only, Private profile, LocalSubnet -----------

def test_firewall_allow_opens_only_dashboard_port_private_localsubnet():
    assert "8503" in FW_ALLOW
    assert "-Profile Private" in FW_ALLOW
    assert "LocalSubnet" in FW_ALLOW
    assert "Inbound" in FW_ALLOW
    # never open the Public profile or any other/trading port
    assert "-Profile Public" not in FW_ALLOW
    assert "-Profile Any" not in FW_ALLOW
    for trading_port in ("clob", "gamma", "443", "8501"):
        assert trading_port not in FW_ALLOW


def test_firewall_scripts_require_admin_and_are_named_correctly():
    for src in (FW_ALLOW, FW_DISABLE):
        assert "Administrator" in src
        assert "Poly Alpha Sniper Dashboard V3 LAN" in src
    assert "New-NetFirewallRule" in FW_ALLOW
    assert "Remove-NetFirewallRule" in FW_DISABLE
    # disable script must not create/allow anything
    assert "New-NetFirewallRule" not in FW_DISABLE


# --- stop script: only the dashboard node process, never the bot -----------

def test_stop_only_targets_dashboard_node_not_bot():
    assert "8503" in STOP
    assert "dashboard_v3" in STOP
    for forbidden in ("main.py", "python.exe", "shadow_live", "auto_export"):
        assert forbidden not in STOP, f"stop script references {forbidden!r}"
    # must verify it's a node process before stopping (no blind kill)
    assert "node" in STOP


def test_docs_state_lan_only_and_keep_auth():
    doc = DOCS.read_text(encoding="utf-8").lower()
    assert "lan" in doc
    assert "same wi-fi" in doc or "same network" in doc
    assert "password" in doc
    assert "do not port-forward" in doc or "do not port forward" in doc
    assert "shadow" in doc  # reaffirms shadow-mode safety
