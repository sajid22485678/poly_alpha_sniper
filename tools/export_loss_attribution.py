"""WS5E: read-only export of the loss attribution summary.

Reads the bot's DB read-only (same DashboardData path the dashboard uses),
writes loss_attribution_summary.json to the agent_export output dir. Places
no orders, touches no secrets, writes nothing back to the bot's DB.

Usage:
    .venv\\Scripts\\python.exe tools\\export_loss_attribution.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import json

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.dashboard.db_reader import DashboardData, resolve_db_path
from poly_alpha_sniper.strategy.loss_attribution import build_loss_attribution_summary


def _latest_oracle_rows_by_market(data: DashboardData) -> dict:
    rows = data.recent("oracle_anchor_log", 5000)
    latest: dict[str, dict] = {}
    for row in rows:
        mid = row.get("market_id")
        if mid and (mid not in latest or (row.get("ts_ms") or 0) > (latest[mid].get("ts_ms") or 0)):
            latest[mid] = row
    return latest


def main() -> None:
    cfg = load_config()
    now_ms = int(time.time() * 1000)
    data = DashboardData(resolve_db_path())

    exits = data.exits()
    predictions = data.predictions()
    oracle_by_market = _latest_oracle_rows_by_market(data)

    summary = build_loss_attribution_summary(exits, predictions, oracle_by_market, now_ms)

    out_dir = Path(cfg.agent_export.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "loss_attribution_summary.json"
    tmp_path = out_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    tmp_path.replace(out_path)

    print(f"wrote {out_path}")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
