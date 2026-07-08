"""Env-file loading: canonical precedence, robust parsing, no secret leaks."""
import os

from poly_alpha_sniper.core.config_loader import Secrets, load_dotenv_file, parse_env_file


def _write(tmp_path, content: str, encoding="utf-8"):
    p = tmp_path / ".env"
    p.write_bytes(content.encode(encoding))
    return p


def test_basic_parse(tmp_path):
    p = _write(tmp_path, "TELEGRAM_CHAT_ID=566689426\nTELEGRAM_BOT_TOKEN=12:abc\n")
    values = parse_env_file(p)
    assert values["TELEGRAM_CHAT_ID"] == "566689426"
    assert values["TELEGRAM_BOT_TOKEN"] == "12:abc"


def test_bom_tolerated(tmp_path):
    p = _write(tmp_path, "﻿TELEGRAM_CHAT_ID=566689426\n")
    assert parse_env_file(p)["TELEGRAM_CHAT_ID"] == "566689426"


def test_utf16_tolerated(tmp_path):
    p = _write(tmp_path, "TELEGRAM_CHAT_ID=566689426\r\n", encoding="utf-16")
    assert parse_env_file(p)["TELEGRAM_CHAT_ID"] == "566689426"


def test_quotes_stripped_content_verbatim(tmp_path):
    p = _write(tmp_path, 'DASHBOARD_PASSWORD="p#ss w0rd"\n' + "TELEGRAM_CHAT_ID='566689426'\n")
    values = parse_env_file(p)
    assert values["DASHBOARD_PASSWORD"] == "p#ss w0rd"  # '#' inside quotes kept
    assert values["TELEGRAM_CHAT_ID"] == "566689426"


def test_inline_comment_stripped_unquoted(tmp_path):
    p = _write(tmp_path, "TELEGRAM_CHAT_ID=566689426  # my personal chat\n")
    assert parse_env_file(p)["TELEGRAM_CHAT_ID"] == "566689426"


def test_export_prefix_and_blank_lines(tmp_path):
    p = _write(tmp_path, "\n# comment\nexport LOG_LEVEL=DEBUG\n\n")
    assert parse_env_file(p)["LOG_LEVEL"] == "DEBUG"


def test_missing_file_empty(tmp_path):
    assert parse_env_file(tmp_path / "nope.env") == {}


def test_env_file_overrides_stale_process_env(tmp_path, monkeypatch):
    """THE fix: a stale shell variable can never supersede the project .env."""
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999_stale_value")
    p = _write(tmp_path, "TELEGRAM_CHAT_ID=566689426\n")
    values = load_dotenv_file(p)
    assert values["TELEGRAM_CHAT_ID"] == "566689426"
    assert os.environ["TELEGRAM_CHAT_ID"] == "566689426"  # os.environ synced too


def test_fill_only_mode_available(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "keep_me")
    p = _write(tmp_path, "TELEGRAM_CHAT_ID=566689426\n")
    load_dotenv_file(p, override=False)
    assert os.environ["TELEGRAM_CHAT_ID"] == "keep_me"


def test_secrets_merged_view_prefers_file(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "stale")
    monkeypatch.setenv("BOT_TIMEZONE", "UTC")  # only in process env
    file_values = {"TELEGRAM_CHAT_ID": "566689426"}
    merged = dict(os.environ)
    merged.update(file_values)
    s = Secrets(env=merged)
    assert s.get("TELEGRAM_CHAT_ID") == "566689426"
    assert s.bot_timezone == "UTC"  # process env still fallback for absent keys


def test_explicit_env_dict_is_sole_source(monkeypatch):
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "process_value")
    s = Secrets(env={})
    assert s.get("TELEGRAM_CHAT_ID") == ""  # explicit dict ignores os.environ
