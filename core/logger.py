"""Structured JSON logging with mandatory secret redaction.

Every module gets its logger via get_logger(name). Log records are emitted as
single-line JSON. A redaction filter scrubs anything that looks like a secret:
values of known secret env vars, private keys, API keys/passphrases, bearer
and auth headers, and long hex strings that look like keys/signatures.

NEVER add a code path that bypasses the redaction filter.
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import re
import sys
from pathlib import Path
from typing import Any

_SECRET_ENV_KEYS = (
    "TELEGRAM_BOT_TOKEN",
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
    "DASHBOARD_PASSWORD",
)

_PATTERNS = [
    re.compile(r"0x[a-fA-F0-9]{40,}"),                      # long hex (keys/sigs)
    re.compile(r"\b[a-fA-F0-9]{64}\b"),                     # raw 32-byte hex
    re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{30,}\b"),         # telegram bot token
    re.compile(r"(?i)(bearer|authorization)[\"':\s=]+[^\s\"']{8,}"),
    re.compile(r"(?i)(api[_-]?key|api[_-]?secret|passphrase|private[_-]?key|password)"
               r"[\"':\s=]+[^\s\"']{4,}"),
]

_REDACTED = "[REDACTED]"


def _secret_values() -> list[str]:
    vals = []
    for k in _SECRET_ENV_KEYS:
        v = os.environ.get(k, "")
        if v and len(v) >= 4:
            vals.append(v)
    return vals


def redact_text(text: str) -> str:
    """Scrub secrets from arbitrary text. Safe on any string."""
    if not text:
        return text
    out = text
    for v in _secret_values():
        out = out.replace(v, _REDACTED)
    for pat in _PATTERNS:
        out = pat.sub(_REDACTED, out)
    return out


def redact_obj(obj: Any) -> Any:
    """Recursively scrub secrets from dicts/lists/strings."""
    if isinstance(obj, str):
        return redact_text(obj)
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if isinstance(k, str) and re.search(
                    r"(?i)(secret|passphrase|private|password|token|api[_-]?key|auth)", k):
                cleaned[k] = _REDACTED
            else:
                cleaned[k] = redact_obj(v)
        return cleaned
    if isinstance(obj, (list, tuple)):
        return [redact_obj(x) for x in obj]
    return obj


class RedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.msg = redact_text(str(record.msg))
            if record.args:
                record.args = tuple(redact_obj(a) for a in record.args)
            extra = getattr(record, "extra", None)
            if extra is not None:
                record.extra = redact_obj(extra)
        except Exception:
            record.msg = "[REDACTION_FAILURE - message suppressed]"
            record.args = ()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "ms": int(record.msecs),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = redact_text(self.formatException(record.exc_info))
        try:
            return json.dumps(payload, default=str, ensure_ascii=False)
        except Exception:
            return json.dumps({"level": "ERROR", "msg": "log_serialization_failure"})


_configured = False


def configure(log_dir: str = "logs", level: str = "INFO",
              rotation_mb: int = 50, keep_days: int = 14) -> None:
    """Idempotent root logging setup: JSON to stdout + rotating file."""
    global _configured
    if _configured:
        return
    _configured = True
    root = logging.getLogger("pas")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.propagate = False

    fmt = JsonFormatter()
    redact = RedactionFilter()

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.addFilter(redact)
    root.addHandler(sh)

    try:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            Path(log_dir) / "poly_alpha_sniper.log",
            maxBytes=rotation_mb * 1024 * 1024, backupCount=max(1, keep_days),
            encoding="utf-8")
        fh.setFormatter(fmt)
        fh.addFilter(redact)
        root.addHandler(fh)
    except OSError:
        pass  # file logging is best-effort; stdout always works


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"pas.{name}")
    if not logging.getLogger("pas").handlers:
        configure(level=os.environ.get("LOG_LEVEL", "INFO"))
    return logger
