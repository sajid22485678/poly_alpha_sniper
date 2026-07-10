"""Read-only dashboard data access + clearly-labeled demo fallback.

Path resolution is anchored to the PROJECT ROOT (never the shell's working
directory), so the dashboard reads the exact same SQLite file as the runtime
no matter how Streamlit was launched. Every DashboardData instantiation opens
a FRESH read-only connection — no cross-rerun caching.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from poly_alpha_sniper.core.config_loader import PROJECT_ROOT


def resolve_db_path(env: Optional[dict] = None) -> str:
    """Resolve DATABASE_URL exactly like the runtime does (project-root
    anchored; `storage/` remapped to `storage_data/`)."""
    import os
    src = env if env is not None else os.environ
    url = src.get("DATABASE_URL", "") or "sqlite:///storage/poly_alpha_sniper.db"
    if url.startswith("sqlite:///"):
        raw = url[len("sqlite:///"):]
        if raw.replace("\\", "/").startswith("storage/"):
            raw = "storage_data/" + raw.replace("\\", "/")[len("storage/"):]
        p = Path(raw)
        if not p.is_absolute():
            p = PROJECT_ROOT / p
        return str(p)
    # non-sqlite URLs: dashboard falls back to the default sqlite location
    return str(PROJECT_ROOT / "storage_data" / "poly_alpha_sniper.db")


def read_runtime_state(path: Optional[str] = None) -> dict:
    """runtime/state.json written by the bot (heartbeat + diagnostics)."""
    p = Path(path) if path else (PROJECT_ROOT / "runtime" / "state.json")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


_COUNT_TABLES = ("predictions", "signals", "near_misses", "shadow_diagnostics",
                 "orders", "positions", "exits", "health_logs", "errors",
                 "balanced_alpha_gate_results")


class DashboardData:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self.has_data = False
        try:
            if Path(self.db_path).exists():
                uri = f"file:{Path(self.db_path).as_posix()}?mode=ro"
                self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
                self._conn.row_factory = sqlite3.Row
                # live DB = file exists and schema is readable. NEVER show demo
                # data just because a young runtime has no predictions yet.
                probe = self.safe_query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='predictions'")
                self.has_data = bool(probe)
        except sqlite3.Error:
            self._conn = None
            self.has_data = False

    def safe_query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        if self._conn is None:
            return []
        try:
            cur = self._conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

    # ------------------------------------------------------------------
    def recent(self, table: str, limit: int = 100) -> list[dict]:
        if table not in _KNOWN_TABLES:
            return []
        return self.safe_query(f"SELECT * FROM {table} ORDER BY ts_ms DESC LIMIT ?",
                               (limit,))

    def predictions(self, limit: int = 500) -> list[dict]:
        return self.recent("predictions", limit)

    def exits(self, limit: int = 200) -> list[dict]:
        return self.recent("exits", limit)

    def orders(self, limit: int = 200) -> list[dict]:
        return self.recent("orders", limit)

    def failed_orders(self, limit: int = 100) -> list[dict]:
        return self.safe_query(
            "SELECT * FROM orders WHERE state='FAILED' ORDER BY ts_ms DESC LIMIT ?",
            (limit,))

    def pnl_series(self, limit: int = 1000) -> list[dict]:
        return self.safe_query("SELECT * FROM pnl ORDER BY ts_ms ASC LIMIT ?", (limit,))

    def gate_results(self, limit: int = 300) -> list[dict]:
        return self.recent("balanced_alpha_gate_results", limit)

    def diagnostics(self, limit: int = 100) -> list[dict]:
        """shadow_diagnostics rows (may be absent on pre-v2 runtimes)."""
        return self.recent("shadow_diagnostics", limit)

    def count(self, table: str) -> int:
        rows = self.safe_query(f"SELECT COUNT(*) AS n FROM {table}") \
            if table in _KNOWN_TABLES else []
        return int(rows[0]["n"]) if rows else 0

    def latest_ts(self, table: str) -> Optional[int]:
        rows = self.safe_query(f"SELECT MAX(ts_ms) AS t FROM {table}") \
            if table in _KNOWN_TABLES else []
        return int(rows[0]["t"]) if rows and rows[0]["t"] is not None else None

    def db_info(self) -> dict:
        p = Path(self.db_path)
        exists = p.exists()
        # WAL mode: writes land in the -wal sidecar; the main file's mtime only
        # moves on checkpoint. "Last write" = max across db/-wal/-shm.
        last_write = 0
        if exists:
            for candidate in (p, Path(str(p) + "-wal"), Path(str(p) + "-shm")):
                try:
                    if candidate.exists():
                        last_write = max(last_write, int(candidate.stat().st_mtime * 1000))
                except OSError:
                    continue
        info: dict[str, Any] = {
            "path": str(p),
            "exists": exists,
            "size_bytes": p.stat().st_size if exists else 0,
            "last_write_ms": last_write,
            "readable": self._conn is not None,
            "checked_at_ms": int(time.time() * 1000),
            "row_counts": {}, "last_ts": {},
        }
        for table in _COUNT_TABLES:
            info["row_counts"][table] = self.count(table)
            info["last_ts"][table] = self.latest_ts(table)
        return info

    def why_no_predictions(self) -> str:
        """Human answer for an empty/quiet predictions table."""
        diag = self.diagnostics(limit=10)
        if diag:
            reasons = {}
            for d in diag:
                reasons[d.get("reason", "?")] = reasons.get(d.get("reason", "?"), 0) + 1
            top = max(reasons.items(), key=lambda kv: kv[1])[0]
            latest = diag[0]
            return (f"pipeline alive, waiting for a tradable trigger — recent blocks: {top} "
                    f"(latest: {latest.get('reason')} {latest.get('detail', '')[:80]})")
        preds = self.predictions(limit=5)
        if preds:
            return "predictions exist — check the tables below"
        if self.count("health_logs") > 0 or self.count("signals") > 0:
            return ("runtime alive, no shock-triggered opportunity yet "
                    "(diagnostics rows appear after the next bot restart)")
        return "no runtime activity recorded yet — is the bot running?"


_KNOWN_TABLES = {
    "predictions", "signals", "orders", "order_lifecycle", "fills", "positions",
    "exits", "pnl", "health_logs", "errors", "fill_quality", "panic_events",
    "incident_reports", "rate_limit_usage", "near_misses", "watchdog_events",
    "database_backups", "calibration_results", "reconciliation_events",
    "shadow_live_discrepancy", "latency_metrics", "market_memory",
    "balanced_alpha_gate_results", "telegram_commands", "tuning_changes",
    "market_snapshots", "shadow_diagnostics", "feature_store",
    "experimental_probe_trades",
}

DEMO_LABEL = "DEMO DATA — NOT REAL BOT DATA"


def demo_data() -> dict[str, list[dict]]:
    """Clearly-fake rows shown ONLY when no database file exists."""
    base_ts = 1_752_000_000_000
    preds = [{"ts_ms": base_ts + i * 60_000, "asset": ["BTC", "ETH", "SOL"][i % 3],
              "market_id": f"demo-m{i}", "market_title": f"[DEMO] Bitcoin Up or Down #{i}",
              "direction": "UP" if i % 2 else "DOWN", "edge": 0.05 + (i % 5) * 0.01,
              "edge_after_slippage": 0.04 + (i % 5) * 0.01,
              "fair_probability": 0.55 + (i % 4) * 0.05, "confidence": 65 + (i % 4) * 8,
              "market_quality": 55 + (i % 5) * 8, "alpha_score": 50 + (i % 5) * 9,
              "tier": ["A_PLUS", "A", "B", "C"][i % 4],
              "aggression_mode": "NORMAL",
              "decision": ["SHADOW_ONLY", "REJECT"][i % 2],
              "reject_reason": "" if i % 2 == 0 else "REJECTED_SPREAD_TOO_WIDE",
              "mode": "shadow_live", "source": "DEMO"} for i in range(40)]
    exits = [{"ts_ms": base_ts + i * 300_000, "market_id": f"demo-m{i}",
              "reason": ["TAKE_PROFIT", "STOP_LOSS", "EXPIRY_RISK"][i % 3],
              "pnl_usd": [0.12, -0.09, 0.05][i % 3], "hold_seconds": 90 + i * 5,
              "price": 0.6, "shares": 1.6, "source": "DEMO"} for i in range(12)]
    pnl = [{"ts_ms": base_ts + i * 300_000, "realized_pnl_usd": [0.12, -0.09, 0.05][i % 3],
            "equity_usd": 10 + 0.03 * i, "source": "DEMO"} for i in range(12)]
    return {"label": [{"label": DEMO_LABEL}], "predictions": preds,
            "exits": exits, "pnl": pnl}
