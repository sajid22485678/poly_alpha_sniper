"""Secret-safety proofs: redaction, DB refusal, no AI imports, tools exist."""
import logging
import re
from pathlib import Path

import pytest

from poly_alpha_sniper.core.logger import JsonFormatter, RedactionFilter, redact_obj, redact_text

PROJECT_ROOT = Path(__file__).resolve().parent.parent

FAKE_PRIVATE_KEY = "0x" + "ab12" * 16                       # 64 hex chars
FAKE_TG_TOKEN = "123456789:AAF" + "x" * 30
FAKE_SECRET_LINE = "api_secret=SuperSecretValue123"


def test_redact_scrubs_private_key():
    assert FAKE_PRIVATE_KEY not in redact_text(f"key is {FAKE_PRIVATE_KEY} ok")


def test_redact_scrubs_telegram_token():
    assert FAKE_TG_TOKEN not in redact_text(f"token {FAKE_TG_TOKEN} used")


def test_redact_scrubs_kv_secrets():
    out = redact_text(f"config: {FAKE_SECRET_LINE}")
    assert "SuperSecretValue123" not in out


def test_redact_obj_masks_secretish_keys():
    obj = {"api_key": "abc123456", "nested": {"passphrase": "p" * 12},
           "price": 0.55, "note": f"pk {FAKE_PRIVATE_KEY}"}
    cleaned = redact_obj(obj)
    assert cleaned["api_key"] == "[REDACTED]"
    assert cleaned["nested"]["passphrase"] == "[REDACTED]"
    assert cleaned["price"] == 0.55
    assert FAKE_PRIVATE_KEY not in cleaned["note"]


def test_logging_pipeline_redacts(tmp_path):
    logger = logging.getLogger("pas.secret_test")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logfile = tmp_path / "test.log"
    handler = logging.FileHandler(logfile, encoding="utf-8")
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RedactionFilter())
    logger.addHandler(handler)
    try:
        logger.info("connecting with key %s", FAKE_PRIVATE_KEY)
        logger.info("payload", extra={"extra": {"private_key": FAKE_PRIVATE_KEY,
                                                "detail": FAKE_SECRET_LINE}})
        handler.flush()
        content = logfile.read_text(encoding="utf-8")
        assert FAKE_PRIVATE_KEY not in content
        assert "SuperSecretValue123" not in content
        assert "[REDACTED]" in content
    finally:
        logger.removeHandler(handler)
        handler.close()


def test_incident_report_redacts():
    from poly_alpha_sniper.reporting.incident_report import build_incident
    incident = build_incident("test", {"api_secret": "topsecret999",
                                       "note": f"key {FAKE_PRIVATE_KEY}"}, 1000)
    assert "topsecret999" not in incident["detail"]
    assert FAKE_PRIVATE_KEY not in incident["detail"]


def test_sqlite_refuses_secret_keys(tmp_path):
    from poly_alpha_sniper.storage.migrations import run_migrations
    from poly_alpha_sniper.storage.sqlite_store import SqliteStore
    s = SqliteStore(str(tmp_path / "x.db"))
    run_migrations(s)
    with pytest.raises(ValueError):
        s.insert("errors", {"ts_ms": 1, "api_secret": "boom"})
    s.close()


def test_secrets_repr_never_reveals_values():
    from poly_alpha_sniper.core.config_loader import Secrets
    s = Secrets(env={"POLYMARKET_PRIVATE_KEY": FAKE_PRIVATE_KEY,
                     "TELEGRAM_BOT_TOKEN": FAKE_TG_TOKEN})
    assert FAKE_PRIVATE_KEY not in repr(s)
    assert FAKE_TG_TOKEN not in str(s)


FORBIDDEN_IMPORTS = re.compile(
    r"^\s*(import|from)\s+(openai|anthropic|deepseek|openrouter|langchain|"
    r"transformers|litellm|google\.generativeai)\b", re.M)


def test_no_ai_modules_imported_anywhere():
    """Master rule: deterministic bot — no runtime AI, no AI SDKs."""
    offenders = []
    for py in PROJECT_ROOT.rglob("*.py"):
        if ".venv" in py.parts:
            continue
        try:
            text = py.read_text(encoding="utf-8")
        except OSError:
            continue
        if FORBIDDEN_IMPORTS.search(text):
            offenders.append(str(py))
    assert offenders == []


def test_no_ai_env_vars_required():
    env_example = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    for banned in ("OPENAI", "ANTHROPIC", "DEEPSEEK", "OPENROUTER"):
        assert banned not in env_example.upper()


def test_env_example_has_no_real_values():
    env_example = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    for line in env_example.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key in ("LIVE_TRADING_ENABLED", "I_UNDERSTAND_REAL_MONEY_RISK",
                   "MAX_REAL_TRADE_USD", "DASHBOARD_AUTH_ENABLED",
                   "DATABASE_URL", "BOT_TIMEZONE", "LOG_LEVEL"):
            continue  # safe defaults allowed
        assert value == "", f"{key} must be an empty placeholder in .env.example"


def test_env_example_defaults_are_safe():
    env_example = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    assert "LIVE_TRADING_ENABLED=false" in env_example
    assert "I_UNDERSTAND_REAL_MONEY_RISK=false" in env_example


def test_helper_tools_exist_and_import():
    import poly_alpha_sniper.tools.test_telegram  # noqa: F401
    import poly_alpha_sniper.tools.test_polymarket_auth  # noqa: F401
    import poly_alpha_sniper.tools.create_polymarket_api_credentials as cred_tool

    # credential tool must never print the private key value
    cred_source = Path(cred_tool.__file__).read_text(encoding="utf-8")
    for line in cred_source.splitlines():
        if "print(" in line:
            assert "PRIVATE_KEY" not in line or "never printed" in line

    # auth tool must never CALL order-placing/cancelling methods
    auth_source = (PROJECT_ROOT / "tools" / "test_polymarket_auth.py").read_text(encoding="utf-8")
    for forbidden_call in (".place_order(", ".cancel(", ".cancel_order(",
                           ".cancel_all(", ".post_order(", ".create_order("):
        assert forbidden_call not in auth_source, f"auth tool calls {forbidden_call}"
