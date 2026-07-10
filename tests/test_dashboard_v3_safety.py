"""Dashboard v3 (Next.js control room, dashboard_v3/) safety checks: reads
only the allow-listed exporter JSON files, no .env/secret access, no order
placement/cancellation surface, no write/mutation endpoints, handles missing
export files gracefully, and the project-wide safety invariants
(mode/dry_run/LIVE_TRADING_ENABLED) still hold. Mirrors
tests/test_dashboard_v2_safety.py's static-scan approach but over the v3
TypeScript/TSX source instead of the Streamlit app.py.

v3 lives entirely under dashboard_v3/ (a separate Next.js project) and never
imports the Python bot code, so these are plain text/source scans rather
than Python-level imports."""
import re
from pathlib import Path

import pytest

from poly_alpha_sniper.core.config_loader import TradingMode, load_config, load_secrets

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_V3_ROOT = PROJECT_ROOT / "dashboard_v3"
SRC_ROOT = DASHBOARD_V3_ROOT / "src"

pytestmark = pytest.mark.skipif(not SRC_ROOT.exists(), reason="dashboard_v3 not present in this environment")


def _source_files():
    return sorted(SRC_ROOT.rglob("*.ts")) + sorted(SRC_ROOT.rglob("*.tsx"))


def _strip_ts_comments(text: str) -> str:
    """Strip /* block */ and // line comments so safety-explaining prose
    (e.g. a docstring that says 'never reads process.env') doesn't trip a
    naive substring check for the very terms it forbids. No string literal
    in this codebase contains '//' (verified: no URLs in source), so a
    plain '//' truncation is safe here."""
    without_blocks = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    lines = [line.split("//", 1)[0] for line in without_blocks.splitlines()]
    return "\n".join(lines)


def _all_source_text() -> str:
    return "\n".join(_strip_ts_comments(f.read_text(encoding="utf-8")) for f in _source_files())


def _route_source() -> str:
    return (SRC_ROOT / "app" / "api" / "snapshot" / "route.ts").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Reads only the allow-listed exporter files
# ---------------------------------------------------------------------------

def test_api_route_hardcodes_required_and_optional_allowlisted_files():
    src = _route_source()
    for filename in (
        "dashboard_snapshot.json", "latest_status.json", "trade_summary.json",
        "reject_breakdown.json", "auto_export_status.json", "lite_dashboard.json",
    ):
        assert filename in src, f"API route does not reference {filename}"


def test_missing_lite_export_never_marks_advanced_core_data_missing():
    src = _route_source()
    start = src.index("for (const [key, result]")
    end = src.index("file_ages_ms.auto_export_status")
    assert "liteDashboard" not in src[start:end]
    assert "lite_dashboard_missing: liteDashboard.missing" in src


def test_api_route_never_builds_a_path_from_request_input():
    """No path-traversal surface: the route must never read a filename out
    of a query param, header, or request body."""
    src = _route_source()
    forbidden = ("request.url", "searchParams", "req.query", "req.body", "req.headers")
    for term in forbidden:
        assert term not in src, f"API route derives a path from request input via {term!r}"


def test_no_source_file_reads_outside_the_agent_readonly_export_dir():
    """Every readFile/readFileSync call in the v3 source must resolve under
    the fixed agent_readonly export directory -- no other filesystem reads
    exist anywhere in the app."""
    read_calls = re.compile(r"readFile(?:Sync)?\s*\(")
    for f in _source_files():
        text = f.read_text(encoding="utf-8")
        if read_calls.search(text):
            assert "agent_readonly" in text or "EXPORT_DIR" in text, (
                f"{f} calls readFile but doesn't reference the fixed export dir"
            )


# ---------------------------------------------------------------------------
# No .env / secrets access
# ---------------------------------------------------------------------------

def test_no_source_file_touches_env_or_secret_looking_terms():
    """Bare '.env' is intentionally excluded: HermesPanel's permissions
    table legitimately displays the label 'Can access .env / secrets: NO'
    as informational UI copy, same as forbidden_paths.md documenting the
    term it forbids (see test_hermes_workspace.py). Actual file access is
    covered separately by test_no_source_file_reads_outside_the_agent_readonly_export_dir,
    which requires every readFile call to resolve under the export dir.

    Exception for process.env: the LAN auth proxy (proxy.ts) reads exactly one
    env var -- process.env.DASHBOARD_LAN_PASSWORD, a user-chosen LAN VIEWING
    password (not a stored secret / API key) used only to Basic-Auth-gate the
    read-only dashboard for phone access. That single, named, non-secret read
    is allowed; any other process.env use, and every secret-looking key, is
    still forbidden in every source file including proxy.ts."""
    secret_terms = (
        "dotenv", "PRIVATE_KEY", "API_SECRET", "API_KEY",
        "BOT_TOKEN", "POLYMARKET_", "TELEGRAM_", "OPENROUTER_", "DASHBOARD_PASSWORD",
    )
    for f in _source_files():
        text = _strip_ts_comments(f.read_text(encoding="utf-8"))
        for term in secret_terms:
            assert not re.search(re.escape(term), text), f"{f} references forbidden term {term!r}"
        env_hits = re.findall(r"process\.env\.\w+", text)
        if f.name == "proxy.ts":
            assert env_hits == ["process.env.DASHBOARD_LAN_PASSWORD"], \
                f"proxy.ts may only read DASHBOARD_LAN_PASSWORD, saw {env_hits}"
        else:
            assert "process.env" not in text, f"{f} references forbidden term 'process.env'"


# ---------------------------------------------------------------------------
# No order placement/cancellation, no execution surface
# ---------------------------------------------------------------------------

def test_no_source_file_references_order_placement_or_cancellation():
    forbidden = ("place_order", "placeOrder", "cancel_order", "cancelOrder", "cancel_all", "cancelAll")
    text = _all_source_text()
    for term in forbidden:
        assert term not in text, f"v3 source references forbidden execution term {term!r}"


def test_no_source_file_shells_out_or_spawns_processes():
    forbidden = ("child_process", "execSync", "spawn(", "exec(")
    text = _all_source_text()
    for term in forbidden:
        assert term not in text, f"v3 source references process-spawning term {term!r}"


# ---------------------------------------------------------------------------
# No write/mutation endpoints -- GET only
# ---------------------------------------------------------------------------

def test_api_route_exports_only_a_get_handler():
    src = _route_source()
    for verb in ("POST", "PUT", "DELETE", "PATCH"):
        assert f"export async function {verb}" not in src and f"export function {verb}" not in src, (
            f"API route exports a {verb} handler -- v3 must be GET-only"
        )
    assert "export async function GET" in src


def test_no_api_route_exists_outside_the_single_snapshot_endpoint():
    api_dir = SRC_ROOT / "app" / "api"
    route_files = sorted(api_dir.rglob("route.ts")) if api_dir.exists() else []
    assert [str(p) for p in route_files] == [str(SRC_ROOT / "app" / "api" / "snapshot" / "route.ts")]


def test_no_source_file_calls_a_filesystem_write_function():
    forbidden = ("writeFile", "writeFileSync", "unlink", "rm(", "rmSync", "appendFile")
    text = _all_source_text()
    for term in forbidden:
        assert term not in text, f"v3 source calls a filesystem write/delete function {term!r}"


# ---------------------------------------------------------------------------
# Missing export files handled gracefully; stale/sample-size warnings exist
# ---------------------------------------------------------------------------

def test_api_route_catches_missing_files_instead_of_throwing():
    src = _route_source()
    assert "try {" in src and "catch" in src
    assert "missing" in src.lower()


def test_status_banner_shows_stale_data_warning():
    src = (SRC_ROOT / "components" / "StatusBanner.tsx").read_text(encoding="utf-8")
    assert "STALE" in src
    assert "STALE_THRESHOLD_MS" in src


def test_status_banner_shows_no_data_yet_state():
    src = (SRC_ROOT / "components" / "StatusBanner.tsx").read_text(encoding="utf-8")
    assert "No data yet" in src


def test_kpi_grid_or_footer_surfaces_sample_size_note():
    text = _all_source_text()
    assert "sample_size_note" in text or "30" in (SRC_ROOT / "components" / "SafetyFooter.tsx").read_text(encoding="utf-8")


def test_types_file_never_invents_fields_not_in_the_python_exporter():
    """Sanity guard: the fabricated-looking reference metrics (Kelly,
    Markov persistence, token/s, sent-count) must never appear as real
    fields in the shared type definitions -- only in prose explaining why
    they're NOT IMPLEMENTED."""
    types_src = (SRC_ROOT / "lib" / "types.ts").read_text(encoding="utf-8")
    forbidden_fields = ("kelly_fraction", "markov_persistence", "toks_per_sec", "sent_count", "calmar")
    for term in forbidden_fields:
        assert term not in types_src.lower()


# ---------------------------------------------------------------------------
# Project-wide safety invariants (still true with dashboard_v3 present)
# ---------------------------------------------------------------------------

def test_mode_is_still_shadow_live_or_simulation():
    cfg = load_config()
    assert cfg.mode.trading_mode in ("shadow_live", "simulation")
    assert TradingMode(cfg.mode.trading_mode).is_live is False


def test_dry_run_is_still_true():
    cfg = load_config()
    assert cfg.mode.dry_run is True


def test_live_trading_enabled_is_still_false():
    secrets = load_secrets()
    assert secrets.live_trading_enabled is False


def test_dashboard_v1_v2_app_untouched_still_has_no_secrets_or_order_calls():
    """Regression guard: building v3 must not have touched dashboard/app.py."""
    app_source = (PROJECT_ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
    for forbidden in ("place_order", "cancel_order", "PRIVATE_KEY", "API_SECRET", "BOT_TOKEN"):
        assert forbidden not in app_source


def test_scripts_v3_bat_does_not_touch_v1_v2_start_script():
    v1_bat = (PROJECT_ROOT / "scripts" / "start_dashboard.bat").read_text(encoding="utf-8")
    assert "8501" in v1_bat
    v3_bat_path = PROJECT_ROOT / "scripts" / "start_dashboard_v3.bat"
    assert v3_bat_path.exists()
    v3_bat = v3_bat_path.read_text(encoding="utf-8")
    assert "dashboard_v3" in v3_bat
    assert "8501" not in v3_bat
