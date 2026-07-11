"""Separate read-only dashboard export for the Lite shadow lane."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from .lite_config import (
    LITE_CLOB_BASE_URL, LITE_DB_PATH, LITE_EXPORT_DIR, LITE_GAMMA_BASE_URL,
    LITE_RUNTIME_DIR,
)

LITE_WARNING = "LITE SHADOW ONLY — SEPARATE FROM ADVANCED READINESS — NO REAL ORDERS"


def assert_lite_safety(cfg) -> None:
    if cfg.mode != "lite_shadow" or cfg.dry_run is not True or cfg.live_enabled is not False:
        raise RuntimeError("Lite safety lock failed: mode/dry_run/live_enabled")
    if Path(cfg.db_path).resolve() != Path(LITE_DB_PATH).resolve():
        raise RuntimeError("Lite safety lock failed: dedicated DB path")
    if Path(cfg.runtime_dir).resolve() != Path(LITE_RUNTIME_DIR).resolve():
        raise RuntimeError("Lite safety lock failed: dedicated runtime path")
    if Path(cfg.export_dir).resolve() != Path(LITE_EXPORT_DIR).resolve():
        raise RuntimeError("Lite safety lock failed: dedicated export path")
    if cfg.gamma_base_url != LITE_GAMMA_BASE_URL or cfg.clob_base_url != LITE_CLOB_BASE_URL:
        raise RuntimeError("Lite safety lock failed: canonical public endpoints")
    if list(cfg.assets) != ["BTC", "ETH", "SOL"]:
        raise RuntimeError("Lite safety lock failed: required assets")
    if float(cfg.fixed_order_shares) != 5.0:
        raise RuntimeError("Lite safety lock failed: fixed five-share sizing")
    if cfg.live_kill_switch_engaged is not True:
        raise RuntimeError("Lite safety lock failed: live kill switch")


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
    risk = store.risk_snapshot(int(now_ms)) if hasattr(store, "risk_snapshot") else {
        "today_realized_pnl": payload.get("today_lite_pnl", 0.0),
        "consecutive_losses": 0,
        "committed_exposure_usd": payload.get("committed_exposure_usd", 0.0),
    }
    equity = float(cfg.live_small_equity_usd)
    cap = round(equity * float(cfg.equity_exposure_cap_pct), 10)
    committed = float(risk.get("committed_exposure_usd") or 0.0)
    live_guard_reasons = []
    if bool(cfg.live_kill_switch_engaged):
        live_guard_reasons.append("live_kill_switch_engaged")
    if float(risk.get("today_realized_pnl") or 0.0) <= -abs(
            float(cfg.max_daily_realized_loss_usd)):
        live_guard_reasons.append("max_daily_realized_loss")
    if int(risk.get("consecutive_losses") or 0) >= int(cfg.max_consecutive_losses):
        live_guard_reasons.append("max_consecutive_losses")
    return {
        "generated_ts_ms": int(now_ms),
        "mode": "lite_shadow",
        "dry_run": True,
        "live_enabled": False,
        "warning": LITE_WARNING,
        "heartbeat_ts_ms": runtime_state.get("heartbeat_ts_ms"),
        "current_commit": runtime_state.get("current_commit", "UNKNOWN"),
        **payload,
        "live_small_preview": {
            "fixed_shares": 5.0,
            "equity_source": "configured_shadow_preview",
            "equity_usd": equity,
            "exposure_cap_pct": float(cfg.equity_exposure_cap_pct),
            "exposure_cap_usd": cap,
            "committed_exposure_usd": round(committed, 10),
            "available_balance_usd": round(max(0.0, equity-committed), 10),
            "fee_buffer_usd": float(cfg.fee_buffer_usd),
            "max_daily_realized_loss_usd": float(cfg.max_daily_realized_loss_usd),
            "max_consecutive_losses": int(cfg.max_consecutive_losses),
            "consecutive_losses": int(risk.get("consecutive_losses") or 0),
            "kill_switch_engaged": bool(cfg.live_kill_switch_engaged),
            "guard_reasons": live_guard_reasons,
        },
        "live_readiness_verdict": "READY_FOR_MORE_SHADOW",
        "live_readiness_blockers": [
            "corrected_historical_expectancy_and_holdout_are_negative",
            "forward_verified_optimizer_holdout_not_yet_available",
            "authenticated_live_execution_adapter_intentionally_absent",
            "partial_fill_and_order_reconciliation_not_forward_validated",
        ],
        "real_orders_possible": False,
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
