"""Dedicated SQLite persistence for Poly Alpha Lite.

This module never opens the advanced feature store.  Trade sizing is enforced
again at the storage boundary so a caller cannot persist a non-five-share Lite
entry or a USD-cap-derived size.
"""
from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .lite_config import FIXED_SHARES

TRADE_COLUMNS = (
    "asset", "market_id", "event_id", "slug", "condition_id",
    "yes_token_id", "no_token_id", "side", "shares", "entry_price",
    "entry_cost", "entry_ts", "window_close_ts", "status", "exit_price",
    "exit_ts", "pnl", "resolution_source", "resolution_reason", "retry_count",
    "anchor_available", "price_to_beat", "no_anchor_trade", "cex_source",
    "cex_entry_price", "momentum_pct", "strategy_name",
)


class LiteStore:
    def __init__(self, db_path: str):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), timeout=10, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS lite_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    yes_token_id TEXT NOT NULL,
                    no_token_id TEXT NOT NULL,
                    side TEXT NOT NULL CHECK(side IN ('BUY_YES','BUY_NO')),
                    shares REAL NOT NULL CHECK(shares = 5.0),
                    entry_price REAL NOT NULL,
                    entry_cost REAL NOT NULL,
                    entry_ts INTEGER NOT NULL,
                    window_close_ts INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    exit_price REAL,
                    exit_ts INTEGER,
                    pnl REAL,
                    resolution_source TEXT,
                    resolution_reason TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    anchor_available INTEGER NOT NULL DEFAULT 0,
                    price_to_beat REAL,
                    no_anchor_trade INTEGER NOT NULL DEFAULT 1,
                    cex_source TEXT,
                    cex_entry_price REAL,
                    momentum_pct REAL,
                    strategy_name TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lite_trades_status
                    ON lite_trades(status, window_close_ts);
                CREATE INDEX IF NOT EXISTS idx_lite_trades_guard
                    ON lite_trades(asset, window_close_ts, side);
                CREATE TABLE IF NOT EXISTS lite_rejects (
                    timestamp_bucket INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    reject_reason TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(timestamp_bucket, asset, slug, reject_reason)
                );
                """
            )

    @staticmethod
    def _row_dict(row: Optional[sqlite3.Row]) -> Optional[dict]:
        return dict(row) if row is not None else None

    def insert_trade(self, row: dict[str, Any]) -> int:
        payload = {key: row.get(key) for key in TRADE_COLUMNS}
        price = float(payload["entry_price"])
        if not 0 < price <= 1:
            raise ValueError("Lite entry price must be in (0, 1]")
        payload["shares"] = FIXED_SHARES
        payload["entry_cost"] = FIXED_SHARES * price
        payload["status"] = str(payload.get("status") or "OPEN")
        payload["retry_count"] = int(payload.get("retry_count") or 0)
        payload["anchor_available"] = int(bool(payload.get("anchor_available")))
        payload["no_anchor_trade"] = int(not bool(payload["anchor_available"]))
        payload["strategy_name"] = str(payload.get("strategy_name") or "lite_momentum_v1")
        columns = ",".join(TRADE_COLUMNS)
        marks = ",".join("?" for _ in TRADE_COLUMNS)
        with self._lock, self._conn:
            cursor = self._conn.execute(
                f"INSERT INTO lite_trades ({columns}) VALUES ({marks})",
                tuple(payload[column] for column in TRADE_COLUMNS),
            )
            return int(cursor.lastrowid)

    def record_reject(
        self,
        ts_ms: int,
        asset: str,
        slug: str,
        reject_reason: str,
        bucket_s: int = 30,
    ) -> None:
        bucket_ms = max(1, int(bucket_s)) * 1000
        timestamp_bucket = int(ts_ms) // bucket_ms * bucket_ms
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO lite_rejects(timestamp_bucket,asset,slug,reject_reason,count)
                VALUES(?,?,?,?,1)
                ON CONFLICT(timestamp_bucket,asset,slug,reject_reason)
                DO UPDATE SET count=count+1
                """,
                (timestamp_bucket, str(asset), str(slug or ""), str(reject_reason)),
            )

    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        with self._lock:
            return [dict(row) for row in self._conn.execute(sql, tuple(params)).fetchall()]

    def open_positions(self) -> list[dict]:
        return self._rows("SELECT * FROM lite_trades WHERE status='OPEN' ORDER BY entry_ts")

    def positions_for_window(self, window_close_ts: int) -> list[dict]:
        return self._rows(
            "SELECT * FROM lite_trades WHERE window_close_ts=? ORDER BY entry_ts",
            (int(window_close_ts),),
        )

    def get_trade(self, trade_id: int) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM lite_trades WHERE id=?", (int(trade_id),)).fetchone()
        return self._row_dict(row)

    def mark_pending(self, trade_id: int, now_ms: int, reason: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET status='PENDING_RESOLUTION', exit_ts=?, pnl=NULL,
                   resolution_source='unresolved', resolution_reason=? WHERE id=? AND status='OPEN'""",
                (int(now_ms), str(reason), int(trade_id)),
            )

    def pending_trades_due(self, now_ms: int, retry_s: float) -> list[dict]:
        cutoff = int(now_ms - max(0.0, float(retry_s)) * 1000)
        return self._rows(
            """SELECT * FROM lite_trades WHERE status='PENDING_RESOLUTION'
               AND COALESCE(exit_ts,window_close_ts)<=? ORDER BY window_close_ts""",
            (cutoff,),
        )

    def complete_trade(
        self,
        trade_id: int,
        status: str,
        exit_price: Optional[float],
        exit_ts: int,
        pnl: Optional[float],
        resolution_source: str,
        resolution_reason: str,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET status=?,exit_price=?,exit_ts=?,pnl=?,
                   resolution_source=?,resolution_reason=? WHERE id=?
                   AND status IN ('OPEN','PENDING_RESOLUTION')""",
                (str(status), exit_price, int(exit_ts), pnl, str(resolution_source),
                 str(resolution_reason), int(trade_id)),
            )

    def note_resolution_retry(self, trade_id: int, now_ms: int, reason: str) -> int:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET retry_count=retry_count+1,exit_ts=?,
                   resolution_source='unresolved',resolution_reason=?
                   WHERE id=? AND status='PENDING_RESOLUTION'""",
                (int(now_ms), str(reason), int(trade_id)),
            )
            row = self._conn.execute(
                "SELECT retry_count FROM lite_trades WHERE id=?", (int(trade_id),)
            ).fetchone()
        return int(row[0]) if row else 0

    def mark_unresolved(self, trade_id: int, now_ms: int, reason: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET status='UNRESOLVED_FINAL',exit_price=NULL,
                   exit_ts=?,pnl=NULL,resolution_source='unresolved',resolution_reason=?
                   WHERE id=? AND status='PENDING_RESOLUTION'""",
                (int(now_ms), str(reason), int(trade_id)),
            )

    def last_trade_ts(self) -> Optional[int]:
        with self._lock:
            row = self._conn.execute("SELECT MAX(entry_ts) FROM lite_trades").fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def dashboard_metrics(self, now_ms: int) -> dict[str, Any]:
        # Build from UTC first so synthetic pre-1970-ish unit-test timestamps
        # remain portable on Windows' local-time implementation.
        local_now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).astimezone()
        midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_ms = int(midnight.timestamp() * 1000)
        with self._lock:
            conn = self._conn
            def scalar(sql: str, params: tuple = ()) -> Any:
                row = conn.execute(sql, params).fetchone()
                return row[0] if row else None

            open_count = int(scalar("SELECT COUNT(*) FROM lite_trades WHERE status='OPEN'") or 0)
            completed = int(scalar("SELECT COUNT(*) FROM lite_trades WHERE pnl IS NOT NULL") or 0)
            pending = int(scalar("SELECT COUNT(*) FROM lite_trades WHERE status='PENDING_RESOLUTION'") or 0)
            unresolved = int(scalar("SELECT COUNT(*) FROM lite_trades WHERE status='UNRESOLVED_FINAL'") or 0)
            total_pnl = float(scalar("SELECT COALESCE(SUM(pnl),0) FROM lite_trades WHERE pnl IS NOT NULL") or 0)
            today_pnl = float(scalar(
                "SELECT COALESCE(SUM(pnl),0) FROM lite_trades WHERE pnl IS NOT NULL AND exit_ts>=?",
                (today_start_ms,),
            ) or 0)
            wins = int(scalar("SELECT COUNT(*) FROM lite_trades WHERE pnl>0") or 0)
            gross_win = float(scalar("SELECT COALESCE(SUM(pnl),0) FROM lite_trades WHERE pnl>0") or 0)
            gross_loss = abs(float(scalar("SELECT COALESCE(SUM(pnl),0) FROM lite_trades WHERE pnl<0") or 0))
            entries_asset = {
                str(row[0]): int(row[1]) for row in conn.execute(
                    "SELECT asset,COUNT(*) FROM lite_trades GROUP BY asset"
                ).fetchall()
            }
            entries_side = {
                str(row[0]): int(row[1]) for row in conn.execute(
                    "SELECT side,COUNT(*) FROM lite_trades GROUP BY side"
                ).fetchall()
            }
            anchor = int(scalar("SELECT COUNT(*) FROM lite_trades WHERE anchor_available=1") or 0)
            no_anchor = int(scalar("SELECT COUNT(*) FROM lite_trades WHERE no_anchor_trade=1") or 0)
            source_counts = {
                str(row[0]): int(row[1]) for row in conn.execute(
                    """SELECT COALESCE(resolution_source,''),COUNT(*) FROM lite_trades
                       WHERE status!='OPEN' GROUP BY COALESCE(resolution_source,'')"""
                ).fetchall()
            }
            rejects = {
                str(row[0]): int(row[1]) for row in conn.execute(
                    """SELECT reject_reason,SUM(count) AS total FROM lite_rejects
                       GROUP BY reject_reason ORDER BY total DESC LIMIT 10"""
                ).fetchall()
            }
            recent = [dict(row) for row in conn.execute(
                "SELECT * FROM lite_trades ORDER BY entry_ts DESC LIMIT 20"
            ).fetchall()]
            cutoff = int(now_ms) - 60_000
            recent_trade_writes = int(scalar(
                "SELECT COUNT(*) FROM lite_trades WHERE entry_ts>=? OR exit_ts>=?", (cutoff, cutoff)
            ) or 0)
            recent_reject_writes = int(scalar(
                "SELECT COALESCE(SUM(count),0) FROM lite_rejects WHERE timestamp_bucket>=?", (cutoff,),
            ) or 0)

        db_size = 0
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(f"{self.path}{suffix}")
            if candidate.exists():
                db_size += candidate.stat().st_size

        return {
            "open_positions": open_count,
            "completed_trades": completed,
            "pending_resolution": pending,
            "unresolved_final": unresolved,
            "total_lite_pnl": round(total_pnl, 10),
            "today_lite_pnl": round(today_pnl, 10),
            "winrate": round(wins / completed, 6) if completed else None,
            "profit_factor": round(gross_win / gross_loss, 6) if gross_loss > 0 else None,
            "expectancy": round(total_pnl / completed, 10) if completed else None,
            "entries_by_asset": entries_asset,
            "entries_by_side": entries_side,
            "anchor_breakdown": {"anchor": anchor, "no_anchor": no_anchor},
            "resolution_source_breakdown": {
                "book_exit": source_counts.get("book_exit", 0),
                "official_outcome": source_counts.get("official_outcome", 0),
                "unresolved": unresolved,
            },
            "top_reject_reasons": rejects,
            "last_20_trades": recent,
            "db_diagnostics": {
                "size_bytes": db_size,
                "writes_per_min": recent_trade_writes + recent_reject_writes,
            },
        }

    def table_count(self, table: str) -> int:
        if table not in ("lite_trades", "lite_rejects"):
            raise ValueError("unknown Lite table")
        with self._lock:
            row = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0])

    def close(self) -> None:
        with self._lock:
            self._conn.close()
