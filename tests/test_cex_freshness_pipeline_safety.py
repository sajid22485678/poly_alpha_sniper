"""Static safety scan for the CEX-freshness-pipeline fix's new modules --
matches the pattern in tests/test_auto_export_loop_safety.py: read-only,
never touches .env/secrets, never places/cancels an order."""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

NEW_FILES = (
    PROJECT_ROOT / "strategy" / "shock_near_miss.py",
    PROJECT_ROOT / "tools" / "replay_no_shock_near_miss.py",
    PROJECT_ROOT / "risk" / "exposure_cap.py",
)

FORBIDDEN = (".env", "PRIVATE_KEY", "API_SECRET", "API_KEY", "BOT_TOKEN",
            "TELEGRAM_", "place_order", "cancel_order",
            "clob.polymarket.com", "gamma-api.polymarket.com")


def test_new_files_exist():
    for path in NEW_FILES:
        assert path.exists(), f"missing {path}"


def test_new_files_never_reference_secrets_or_order_endpoints():
    for path in NEW_FILES:
        src = path.read_text(encoding="utf-8")
        for term in FORBIDDEN:
            assert term not in src, f"{path.name} references forbidden term {term!r}"


def test_replay_tool_is_read_only():
    src = (PROJECT_ROOT / "tools" / "replay_no_shock_near_miss.py").read_text(encoding="utf-8")
    forbidden_writes = ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP TABLE")
    for term in forbidden_writes:
        assert term not in src


def test_shock_near_miss_module_never_imports_shock_detector():
    """The near-miss module must be pure diagnostics -- no import of the
    actual ShockDetector class (mentioning it in a docstring is fine; only
    an actual import would let this module influence the real decision)."""
    src = (PROJECT_ROOT / "strategy" / "shock_near_miss.py").read_text(encoding="utf-8")
    assert "import ShockDetector" not in src
    assert "from poly_alpha_sniper.strategy.shock_detector import" not in src


def test_core_app_never_references_env_or_live_trading_flag():
    """core/app.py must never read .env directly or reference the
    LIVE_TRADING_ENABLED flag -- live-gating is config_validator.py's job,
    via the Secrets object, never inline in the trade loop."""
    src = (PROJECT_ROOT / "core" / "app.py").read_text(encoding="utf-8")
    assert ".env" not in src
    assert "LIVE_TRADING_ENABLED" not in src
