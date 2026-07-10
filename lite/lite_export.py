"""Separate read-only dashboard export for the Lite shadow lane."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

LITE_WARNING = "LITE SHADOW ONLY — NOT BASELINE, NOT LIVE READINESS, NOT REAL FUNDS"


def assert_lite_safety(cfg) -> None:
    if cfg.mode != "lite_shadow" or cfg.dry_run is not True or cfg.live_enabled is not False:
        raise RuntimeError("Lite safety lock failed: mode/dry_run/live_enabled")


def build_lite_dashboard(
    store,
    cfg,
    now_ms: int,
    runtime_state: Optional[dict[str, Any]] = None,
    cex_feed_state: Optional[dict[str, Any]] = None,
    current_market_by_asset: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build Lite metrics only; no advanced DB/state is accepted or queried."""
    assert_lite_safety(cfg)
    runtime_state = runtime_state or {}
    payload = store.dashboard_metrics(int(now_ms))
    return {
        "generated_ts_ms": int(now_ms),
        "mode": "lite_shadow",
        "dry_run": True,
        "live_enabled": False,
        "warning": LITE_WARNING,
        "heartbeat_ts_ms": runtime_state.get("heartbeat_ts_ms"),
        **payload,
        "cex_feed_state": cex_feed_state or {},
        "current_market_by_asset": current_market_by_asset or {},
    }


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def write_lite_dashboard(
    store,
    cfg,
    now_ms: int,
    runtime_state: Optional[dict[str, Any]] = None,
    cex_feed_state: Optional[dict[str, Any]] = None,
    current_market_by_asset: Optional[dict[str, Any]] = None,
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    payload = build_lite_dashboard(
        store=store,
        cfg=cfg,
        now_ms=now_ms,
        runtime_state=runtime_state,
        cex_feed_state=cex_feed_state,
        current_market_by_asset=current_market_by_asset,
    )
    path = Path(output_dir or cfg.export_dir) / "lite_dashboard.json"
    encoded = json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False)
    _atomic_write(path, encoded)
    return {"path": str(path), "payload": payload}
