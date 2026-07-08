"""Incident reports — always redacted, persisted, and renderable."""
from __future__ import annotations

import json

from poly_alpha_sniper.core.logger import redact_obj

SEVERITIES = ("INFO", "WARNING", "CRITICAL")


def build_incident(kind: str, detail: dict, ts_ms: int,
                   severity: str = "CRITICAL") -> dict:
    if severity not in SEVERITIES:
        severity = "CRITICAL"
    return {
        "ts_ms": ts_ms,
        "kind": str(kind)[:80],
        "severity": severity,
        "detail": json.dumps(redact_obj(detail), default=str)[:4000],
    }


def save_incident(store, incident: dict) -> None:
    store.insert("incident_reports", incident)


def render_text(incident: dict) -> str:
    return (f"INCIDENT [{incident.get('severity')}] {incident.get('kind')}\n"
            f"ts_ms: {incident.get('ts_ms')}\n"
            f"detail: {incident.get('detail', '')[:800]}")
