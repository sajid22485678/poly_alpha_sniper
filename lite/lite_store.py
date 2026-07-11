"""Dedicated, restart-safe SQLite persistence for Poly Alpha Lite.

The database is the authority for the one-entry-per-asset/window invariant,
resolution retries, net accounting, and forward decision telemetry.  It never
opens or imports the advanced bot's feature store.
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .lite_config import FIXED_SHARES
from .lite_risk import idempotent_intent_key, taker_fee


TERMINAL_STATUSES = ("CLOSED_BOOK_EXIT", "CLOSED_WIN", "CLOSED_LOSS")
COMMITTED_STATUSES = (
    "OPEN", "EXIT_PENDING", "PENDING_RESOLUTION",
    "UNRESOLVED_RETRYING", "UNRESOLVED_FINAL",
)

TRADE_COLUMNS = (
    "asset", "market_id", "event_id", "slug", "condition_id",
    "yes_token_id", "no_token_id", "side", "shares", "entry_price",
    "entry_cost", "entry_ts", "window_open_ts", "window_close_ts", "status",
    "exit_price", "exit_ts", "pnl", "gross_pnl", "entry_fee", "fee_buffer", "exit_fee",
    "fee_rate", "resolution_source", "resolution_reason", "retry_count",
    "last_attempt_at", "last_error", "next_attempt_at", "resolution_verified",
    "anchor_available", "price_to_beat", "no_anchor_trade", "cex_source",
    "cex_entry_price", "momentum_pct", "strategy_name", "entry_mode",
    "direction_decision_ts", "direction_score", "yes_score", "no_score",
    "confidence", "direction_reason", "expected_improvement",
    "actual_improvement", "wait_duration_ms", "missed_opportunity",
    "chase_prevented", "final_entry_reason", "entry_book_ts",
    "entry_book_received_ts", "entry_book_age_ms", "entry_book_hash",
    "entry_best_bid", "entry_best_ask", "entry_fill_shares",
    "entry_ask_depth_shares", "entry_spread", "entry_fill_levels",
    "entry_worst_price", "exit_book_ts",
    "exit_book_received_ts", "exit_book_age_ms", "exit_book_hash",
    "exit_best_bid", "exit_best_ask", "exit_fill_shares",
    "exit_bid_depth_shares", "exit_spread", "exit_fill_levels",
    "exit_worst_price", "execution_verified",
    "accounting_version", "legacy_note",
)

_MIGRATION_COLUMNS = {
    "window_open_ts": "INTEGER",
    "gross_pnl": "REAL",
    "entry_fee": "REAL NOT NULL DEFAULT 0",
    "fee_buffer": "REAL NOT NULL DEFAULT 0",
    "exit_fee": "REAL NOT NULL DEFAULT 0",
    "fee_rate": "REAL NOT NULL DEFAULT 0",
    "last_attempt_at": "INTEGER",
    "last_error": "TEXT",
    "next_attempt_at": "INTEGER",
    "resolution_verified": "INTEGER NOT NULL DEFAULT 0",
    "entry_mode": "TEXT",
    "direction_decision_ts": "INTEGER",
    "direction_score": "REAL",
    "yes_score": "REAL",
    "no_score": "REAL",
    "confidence": "REAL",
    "direction_reason": "TEXT",
    "expected_improvement": "REAL",
    "actual_improvement": "REAL",
    "wait_duration_ms": "INTEGER",
    "missed_opportunity": "INTEGER NOT NULL DEFAULT 0",
    "chase_prevented": "INTEGER NOT NULL DEFAULT 0",
    "final_entry_reason": "TEXT",
    "entry_book_ts": "INTEGER",
    "entry_book_received_ts": "INTEGER",
    "entry_book_age_ms": "INTEGER",
    "entry_book_hash": "TEXT",
    "entry_best_bid": "REAL",
    "entry_best_ask": "REAL",
    "entry_fill_shares": "REAL",
    "entry_ask_depth_shares": "REAL",
    "entry_spread": "REAL",
    "entry_fill_levels": "TEXT",
    "entry_worst_price": "REAL",
    "exit_book_ts": "INTEGER",
    "exit_book_received_ts": "INTEGER",
    "exit_book_age_ms": "INTEGER",
    "exit_book_hash": "TEXT",
    "exit_best_bid": "REAL",
    "exit_best_ask": "REAL",
    "exit_fill_shares": "REAL",
    "exit_bid_depth_shares": "REAL",
    "exit_spread": "REAL",
    "exit_fill_levels": "TEXT",
    "exit_worst_price": "REAL",
    "execution_verified": "INTEGER NOT NULL DEFAULT 0",
    "accounting_version": "INTEGER NOT NULL DEFAULT 1",
    "legacy_note": "TEXT",
}


class WindowLockConflict(RuntimeError):
    def __init__(self, reason: str, lock: Optional[dict] = None):
        super().__init__(reason)
        self.reason = reason
        self.lock = lock or {}


def _value(obj: Any, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


class LiteStore:
    def __init__(self, db_path: str, *, max_open_positions: Optional[int] = None,
                 max_open_per_asset: Optional[int] = None,
                 exposure_cap_usd: Optional[float] = None):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._bucket_seen: set[tuple[str, int, str, str, str]] = set()
        self.max_open_positions = (int(max_open_positions)
                                   if max_open_positions is not None else None)
        self.max_open_per_asset = (int(max_open_per_asset)
                                   if max_open_per_asset is not None else None)
        self.exposure_cap_usd = (float(exposure_cap_usd)
                                 if exposure_cap_usd is not None else None)
        self._conn = sqlite3.connect(str(self.path), timeout=10, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS lite_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset TEXT NOT NULL, market_id TEXT NOT NULL,
                    event_id TEXT NOT NULL, slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL, yes_token_id TEXT NOT NULL,
                    no_token_id TEXT NOT NULL,
                    side TEXT NOT NULL CHECK(side IN ('BUY_YES','BUY_NO')),
                    shares REAL NOT NULL CHECK(shares = 5.0),
                    entry_price REAL NOT NULL, entry_cost REAL NOT NULL,
                    entry_ts INTEGER NOT NULL, window_open_ts INTEGER,
                    window_close_ts INTEGER NOT NULL, status TEXT NOT NULL,
                    exit_price REAL, exit_ts INTEGER, pnl REAL, gross_pnl REAL,
                    entry_fee REAL NOT NULL DEFAULT 0,
                    fee_buffer REAL NOT NULL DEFAULT 0,
                    exit_fee REAL NOT NULL DEFAULT 0,
                    fee_rate REAL NOT NULL DEFAULT 0,
                    resolution_source TEXT, resolution_reason TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at INTEGER, last_error TEXT, next_attempt_at INTEGER,
                    resolution_verified INTEGER NOT NULL DEFAULT 0,
                    anchor_available INTEGER NOT NULL DEFAULT 0,
                    price_to_beat REAL, no_anchor_trade INTEGER NOT NULL DEFAULT 1,
                    cex_source TEXT, cex_entry_price REAL, momentum_pct REAL,
                    strategy_name TEXT NOT NULL, entry_mode TEXT,
                    direction_decision_ts INTEGER, direction_score REAL,
                    yes_score REAL, no_score REAL, confidence REAL,
                    direction_reason TEXT, expected_improvement REAL,
                    actual_improvement REAL, wait_duration_ms INTEGER,
                    missed_opportunity INTEGER NOT NULL DEFAULT 0,
                    chase_prevented INTEGER NOT NULL DEFAULT 0,
                    final_entry_reason TEXT, entry_book_ts INTEGER,
                    entry_book_received_ts INTEGER, entry_book_age_ms INTEGER,
                    entry_book_hash TEXT, entry_best_bid REAL, entry_best_ask REAL,
                    entry_fill_shares REAL, entry_ask_depth_shares REAL,
                    entry_spread REAL, entry_fill_levels TEXT,
                    entry_worst_price REAL, exit_book_ts INTEGER,
                    exit_book_received_ts INTEGER, exit_book_age_ms INTEGER,
                    exit_book_hash TEXT, exit_best_bid REAL, exit_best_ask REAL,
                    exit_fill_shares REAL, exit_bid_depth_shares REAL,
                    exit_spread REAL, exit_fill_levels TEXT,
                    exit_worst_price REAL, execution_verified INTEGER NOT NULL DEFAULT 0,
                    accounting_version INTEGER NOT NULL DEFAULT 1, legacy_note TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_lite_trades_status
                    ON lite_trades(status, window_close_ts);
                CREATE INDEX IF NOT EXISTS idx_lite_trades_guard
                    ON lite_trades(asset, window_close_ts, side);
                CREATE TABLE IF NOT EXISTS lite_rejects (
                    timestamp_bucket INTEGER NOT NULL, asset TEXT NOT NULL,
                    slug TEXT NOT NULL, reject_reason TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(timestamp_bucket,asset,slug,reject_reason)
                );
                CREATE TABLE IF NOT EXISTS lite_decision_buckets (
                    timestamp_bucket INTEGER NOT NULL, asset TEXT NOT NULL,
                    slug TEXT NOT NULL, decision TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(timestamp_bucket,asset,slug,decision)
                );
                CREATE TABLE IF NOT EXISTS lite_window_locks (
                    asset TEXT NOT NULL, slug TEXT NOT NULL, market_id TEXT NOT NULL,
                    event_id TEXT NOT NULL, condition_id TEXT NOT NULL,
                    window_open_ts INTEGER NOT NULL, window_close_ts INTEGER NOT NULL,
                    side TEXT NOT NULL CHECK(side IN ('BUY_YES','BUY_NO')),
                    status TEXT NOT NULL, lifecycle_status TEXT NOT NULL,
                    direction_decision_ts INTEGER NOT NULL, direction_output TEXT,
                    direction_score REAL, yes_score REAL, no_score REAL,
                    score_difference REAL, confidence REAL, direction_reason TEXT,
                    return_10s REAL, return_30s REAL, return_60s REAL,
                    tick_return REAL, volatility REAL, entry_state TEXT,
                    initial_ask REAL, target_price REAL, max_chase_price REAL,
                    deadline_ts INTEGER, last_reevaluate_ts INTEGER,
                    expected_improvement REAL, actual_improvement REAL,
                    wait_duration_ms INTEGER NOT NULL DEFAULT 0,
                    missed_opportunity INTEGER NOT NULL DEFAULT 0,
                    chase_prevented INTEGER NOT NULL DEFAULT 0,
                    final_entry_reason TEXT, trade_id INTEGER,
                    idempotency_key TEXT NOT NULL, last_updated_ts INTEGER NOT NULL,
                    PRIMARY KEY(asset,window_close_ts),
                    UNIQUE(idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS idx_lite_window_locks_state
                    ON lite_window_locks(status,window_close_ts);
                CREATE TABLE IF NOT EXISTS lite_resolution_attempts (
                    trade_id INTEGER NOT NULL, attempt_no INTEGER NOT NULL,
                    attempt_ts INTEGER NOT NULL, result TEXT NOT NULL,
                    error TEXT, resolution_source TEXT,
                    PRIMARY KEY(trade_id,attempt_no)
                );
                """
            )
            existing = {
                str(row[1]) for row in self._conn.execute("PRAGMA table_info(lite_trades)")
            }
            for name, definition in _MIGRATION_COLUMNS.items():
                if name not in existing:
                    self._conn.execute(
                        f'ALTER TABLE lite_trades ADD COLUMN "{name}" {definition}')
            self._conn.execute(
                """UPDATE lite_trades SET window_open_ts=window_close_ts-300000
                   WHERE window_open_ts IS NULL""")
            self._conn.execute(
                """UPDATE lite_trades SET gross_pnl=pnl
                   WHERE pnl IS NOT NULL AND gross_pnl IS NULL""")
            self._seed_historical_locks()

    def _seed_historical_locks(self) -> None:
        self._conn.execute(
            """
            INSERT OR IGNORE INTO lite_window_locks(
                asset,slug,market_id,event_id,condition_id,window_open_ts,
                window_close_ts,side,status,lifecycle_status,direction_decision_ts,
                direction_output,entry_state,trade_id,idempotency_key,last_updated_ts)
            SELECT t.asset,t.slug,t.market_id,t.event_id,t.condition_id,
                   COALESCE(t.window_open_ts,t.window_close_ts-300000),
                   t.window_close_ts,t.side,'HISTORICAL_ENTERED',t.status,t.entry_ts,
                   t.side,'HISTORICAL',t.id,
                   lower(hex(randomblob(32))),t.entry_ts
            FROM lite_trades t
            JOIN (
                SELECT asset,window_close_ts,MIN(id) AS first_id
                FROM lite_trades GROUP BY asset,window_close_ts
            ) first ON first.first_id=t.id
            """
        )

    @staticmethod
    def _row_dict(row: Optional[sqlite3.Row]) -> Optional[dict]:
        return dict(row) if row is not None else None

    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        with self._lock:
            return [dict(row) for row in self._conn.execute(sql, tuple(params)).fetchall()]

    @staticmethod
    def _conflict_reason(lock: dict, requested_side: str,
                         requested_identity: Optional[dict] = None) -> str:
        if requested_identity:
            for key in ("slug", "market_id", "event_id", "condition_id", "window_open_ts"):
                if str(lock.get(key)) != str(requested_identity.get(key)):
                    return "asset_window_already_entered"
        if str(lock.get("side")) != str(requested_side):
            return "opposite_side_blocked"
        if str(lock.get("lifecycle_status")) in COMMITTED_STATUSES:
            return "position_lifecycle_incomplete"
        return "duplicate_same_side_blocked"

    def get_window_lock(self, asset: str, window_close_ts: int) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM lite_window_locks WHERE asset=? AND window_close_ts=?",
                (str(asset).upper(), int(window_close_ts)),
            ).fetchone()
        return self._row_dict(row)

    def reserve_window_direction(self, market, direction, now_ms: int) -> tuple[bool, str, dict]:
        asset = str(_value(market, "asset", "")).upper()
        close_ts = int(_value(market, "window_close_s", 0) * 1000
                       if _value(market, "window_close_ts", None) is None
                       else _value(market, "window_close_ts"))
        open_ts = int(_value(market, "window_start_s", 0) * 1000
                      if _value(market, "window_open_ts", None) is None
                      else _value(market, "window_open_ts"))
        side = str(_value(direction, "side", ""))
        identity = {
            "slug": str(_value(market, "slug", "")),
            "market_id": str(_value(market, "market_id", "")),
            "event_id": str(_value(market, "event_id", "")),
            "condition_id": str(_value(market, "condition_id", "")),
            "window_open_ts": open_ts,
        }
        key = idempotent_intent_key(
            asset=asset, window_close_ts=close_ts,
            side=side, **identity)
        values = (
            asset, identity["slug"], identity["market_id"], identity["event_id"],
            identity["condition_id"], open_ts, close_ts, side, "DIRECTION_LOCKED",
            "DIRECTION_LOCKED", int(now_ms), str(_value(direction, "output", side)),
            _value(direction, "direction_score"), _value(direction, "yes_score"),
            _value(direction, "no_score"), _value(direction, "score_difference"),
            _value(direction, "confidence"), str(_value(direction, "reason", "")),
            _value(direction, "return_10s"), _value(direction, "return_30s"),
            _value(direction, "return_60s"), _value(direction, "tick_return"),
            _value(direction, "volatility"), "DIRECTION_LOCKED", key, int(now_ms),
        )
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                existing = self._conn.execute(
                    "SELECT * FROM lite_window_locks WHERE asset=? AND window_close_ts=?",
                    (asset, close_ts),
                ).fetchone()
                if existing is not None:
                    lock = dict(existing)
                    self._conn.rollback()
                    return False, self._conflict_reason(lock, side, identity), lock
                self._conn.execute(
                    """INSERT INTO lite_window_locks(
                       asset,slug,market_id,event_id,condition_id,window_open_ts,
                       window_close_ts,side,status,lifecycle_status,
                       direction_decision_ts,direction_output,direction_score,
                       yes_score,no_score,score_difference,confidence,direction_reason,
                       return_10s,return_30s,return_60s,tick_return,volatility,
                       entry_state,idempotency_key,last_updated_ts)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return True, "direction_locked", self.get_window_lock(asset, close_ts) or {}

    def update_window_lock(self, asset: str, window_close_ts: int, **fields) -> None:
        allowed = {
            "status", "lifecycle_status", "entry_state", "initial_ask",
            "target_price", "max_chase_price", "deadline_ts", "last_reevaluate_ts",
            "expected_improvement", "actual_improvement", "wait_duration_ms",
            "missed_opportunity", "chase_prevented", "final_entry_reason",
            "last_updated_ts", "trade_id",
        }
        updates = {key: value for key, value in fields.items() if key in allowed}
        if not updates:
            return
        marks = ",".join(f'"{key}"=?' for key in updates)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE lite_window_locks SET {marks} WHERE asset=? AND window_close_ts=?",
                (*updates.values(), str(asset).upper(), int(window_close_ts)),
            )

    def mark_window_skipped(self, asset: str, window_close_ts: int, now_ms: int,
                            reason: str, *, missed: bool = False,
                            chase_prevented: bool = False) -> None:
        self.update_window_lock(
            asset, window_close_ts, status="SKIPPED", lifecycle_status="SKIPPED",
            entry_state="SKIPPED", final_entry_reason=str(reason),
            missed_opportunity=int(bool(missed)),
            chase_prevented=int(bool(chase_prevented)), last_updated_ts=int(now_ms),
        )

    def insert_trade(self, row: dict[str, Any]) -> int:
        payload = {key: row.get(key) for key in TRADE_COLUMNS}
        price = float(payload["entry_price"])
        if not 0 < price < 1:
            raise ValueError("Lite entry price must be in (0, 1)")
        payload["shares"] = FIXED_SHARES
        payload["entry_cost"] = FIXED_SHARES * price
        payload["window_open_ts"] = int(
            payload.get("window_open_ts") or int(payload["window_close_ts"]) - 300_000)
        payload["status"] = str(payload.get("status") or "OPEN")
        payload["retry_count"] = int(payload.get("retry_count") or 0)
        payload["anchor_available"] = int(bool(payload.get("anchor_available")))
        payload["no_anchor_trade"] = int(not bool(payload["anchor_available"]))
        payload["strategy_name"] = str(payload.get("strategy_name") or "lite_direction_v2")
        payload["entry_fee"] = float(payload.get("entry_fee") or 0.0)
        payload["fee_buffer"] = max(0.0, float(payload.get("fee_buffer") or 0.0))
        payload["exit_fee"] = float(payload.get("exit_fee") or 0.0)
        payload["fee_rate"] = float(payload.get("fee_rate") or 0.0)
        payload["resolution_verified"] = int(bool(payload.get("resolution_verified")))
        payload["missed_opportunity"] = int(bool(payload.get("missed_opportunity")))
        payload["chase_prevented"] = int(bool(payload.get("chase_prevented")))
        payload["execution_verified"] = int(bool(payload.get("execution_verified")))
        payload["accounting_version"] = int(payload.get("accounting_version") or 2)
        asset = str(payload["asset"]).upper()
        payload["asset"] = asset
        close_ts = int(payload["window_close_ts"])
        identity = {key: payload[key] for key in (
            "slug", "market_id", "event_id", "condition_id", "window_open_ts")}
        columns = ",".join(TRADE_COLUMNS)
        marks = ",".join("?" for _ in TRADE_COLUMNS)
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                marks_committed = ",".join("?" for _ in COMMITTED_STATUSES)
                if self.max_open_positions is not None:
                    open_count = int(self._conn.execute(
                        f"SELECT COUNT(*) FROM lite_trades WHERE status IN ({marks_committed})",
                        COMMITTED_STATUSES).fetchone()[0])
                    if open_count >= self.max_open_positions:
                        raise WindowLockConflict("max_open_positions")
                if self.max_open_per_asset is not None:
                    asset_count = int(self._conn.execute(
                        f"""SELECT COUNT(*) FROM lite_trades
                            WHERE asset=? AND status IN ({marks_committed})""",
                        (asset, *COMMITTED_STATUSES)).fetchone()[0])
                    if asset_count >= self.max_open_per_asset:
                        raise WindowLockConflict("max_open_per_asset")
                if self.exposure_cap_usd is not None:
                    committed = float(self._conn.execute(
                        f"""SELECT COALESCE(SUM(entry_cost+COALESCE(entry_fee,0)
                            +COALESCE(fee_buffer,0)),0) FROM lite_trades
                            WHERE status IN ({marks_committed})""",
                        COMMITTED_STATUSES).fetchone()[0] or 0.0)
                    proposed = (float(payload["entry_cost"])
                                + float(payload.get("entry_fee") or 0.0)
                                + float(payload.get("fee_buffer") or 0.0))
                    if committed + proposed > self.exposure_cap_usd + 1e-9:
                        raise WindowLockConflict("equity_exposure_cap")
                existing_row = self._conn.execute(
                    "SELECT * FROM lite_window_locks WHERE asset=? AND window_close_ts=?",
                    (asset, close_ts),
                ).fetchone()
                if existing_row is None:
                    key = idempotent_intent_key(
                        asset=asset, window_close_ts=close_ts,
                        side=str(payload["side"]), **identity)
                    self._conn.execute(
                        """INSERT INTO lite_window_locks(
                           asset,slug,market_id,event_id,condition_id,window_open_ts,
                           window_close_ts,side,status,lifecycle_status,
                           direction_decision_ts,direction_output,entry_state,
                           idempotency_key,last_updated_ts)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (asset, payload["slug"], payload["market_id"], payload["event_id"],
                         payload["condition_id"], payload["window_open_ts"], close_ts,
                         payload["side"], "DIRECTION_LOCKED", "DIRECTION_LOCKED",
                         int(payload["direction_decision_ts"] or payload["entry_ts"]),
                         payload["side"], "DIRECTION_LOCKED", key, int(payload["entry_ts"])),
                    )
                else:
                    existing = dict(existing_row)
                    if existing.get("trade_id") is not None or existing.get("status") not in (
                            "DIRECTION_LOCKED", "WAIT_FOR_PULLBACK"):
                        raise WindowLockConflict(
                            self._conflict_reason(existing, str(payload["side"]), identity),
                            existing)
                    reason = self._conflict_reason(existing, str(payload["side"]), identity)
                    if reason in ("opposite_side_blocked", "asset_window_already_entered"):
                        raise WindowLockConflict(reason, existing)
                cursor = self._conn.execute(
                    f"INSERT INTO lite_trades ({columns}) VALUES ({marks})",
                    tuple(payload[column] for column in TRADE_COLUMNS),
                )
                trade_id = int(cursor.lastrowid)
                self._conn.execute(
                    """UPDATE lite_window_locks SET status='ENTERED',
                       lifecycle_status='OPEN',entry_state=?,trade_id=?,
                       actual_improvement=?,wait_duration_ms=?,missed_opportunity=?,
                       chase_prevented=?,final_entry_reason=?,last_updated_ts=?
                       WHERE asset=? AND window_close_ts=?""",
                    (str(payload.get("entry_mode") or "ENTER_NOW"), trade_id,
                     payload.get("actual_improvement"), payload.get("wait_duration_ms") or 0,
                     payload.get("missed_opportunity") or 0,
                     payload.get("chase_prevented") or 0,
                     payload.get("final_entry_reason"), int(payload["entry_ts"]),
                     asset, close_ts),
                )
                self._conn.commit()
                return trade_id
            except Exception:
                self._conn.rollback()
                raise

    def record_reject(self, ts_ms: int, asset: str, slug: str,
                      reject_reason: str, bucket_s: int = 30) -> None:
        self._record_bucket("lite_rejects", "reject_reason", ts_ms, asset, slug,
                            reject_reason, bucket_s)

    def record_decision(self, ts_ms: int, asset: str, slug: str,
                        decision: str, bucket_s: int = 30) -> None:
        self._record_bucket("lite_decision_buckets", "decision", ts_ms, asset, slug,
                            decision, bucket_s)

    def _record_bucket(self, table: str, field: str, ts_ms: int, asset: str,
                       slug: str, value: str, bucket_s: int) -> None:
        if table not in ("lite_rejects", "lite_decision_buckets"):
            raise ValueError("invalid Lite bucket table")
        bucket_ms = max(1, int(bucket_s)) * 1000
        timestamp_bucket = int(ts_ms) // bucket_ms * bucket_ms
        cache_key = (table, timestamp_bucket, str(asset), str(slug or ""), str(value))
        with self._lock, self._conn:
            if cache_key in self._bucket_seen:
                return
            self._conn.execute(
                f"""INSERT INTO {table}(timestamp_bucket,asset,slug,{field},count)
                    VALUES(?,?,?,?,1)
                    ON CONFLICT(timestamp_bucket,asset,slug,{field}) DO NOTHING""",
                (timestamp_bucket, str(asset), str(slug or ""), str(value)),
            )
            self._bucket_seen.add(cache_key)
            # Retain only this and the immediately prior bucket.  This bounds
            # memory while preserving the no-repeat-write guarantee.
            cutoff = timestamp_bucket - bucket_ms
            self._bucket_seen = {
                key for key in self._bucket_seen if key[1] >= cutoff
            }

    def open_positions(self) -> list[dict]:
        return self._rows(
            "SELECT * FROM lite_trades WHERE status IN ('OPEN','EXIT_PENDING') ORDER BY entry_ts")

    def committed_positions(self) -> list[dict]:
        marks = ",".join("?" for _ in COMMITTED_STATUSES)
        return self._rows(
            f"SELECT * FROM lite_trades WHERE status IN ({marks}) ORDER BY entry_ts",
            COMMITTED_STATUSES)

    def committed_exposure(self) -> float:
        marks = ",".join("?" for _ in COMMITTED_STATUSES)
        with self._lock:
            row = self._conn.execute(
                f"""SELECT COALESCE(SUM(entry_cost+COALESCE(entry_fee,0)+COALESCE(fee_buffer,0)),0)
                    FROM lite_trades WHERE status IN ({marks})""",
                COMMITTED_STATUSES).fetchone()
        return float(row[0] or 0.0)

    def positions_for_window(self, window_close_ts: int) -> list[dict]:
        return self._rows(
            "SELECT * FROM lite_trades WHERE window_close_ts=? ORDER BY entry_ts",
            (int(window_close_ts),))

    def get_trade(self, trade_id: int) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM lite_trades WHERE id=?", (int(trade_id),)).fetchone()
        return self._row_dict(row)

    def mark_exit_pending(self, trade_id: int, now_ms: int, reason: str = "exit_due") -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET status='EXIT_PENDING',last_error=?
                   WHERE id=? AND status='OPEN'""", (str(reason), int(trade_id)))
            self._sync_lock_lifecycle(trade_id, "EXIT_PENDING", now_ms)

    def mark_pending(self, trade_id: int, now_ms: int, reason: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET status='PENDING_RESOLUTION',pnl=NULL,
                   resolution_source='unresolved',resolution_reason=?,last_error=?,
                   next_attempt_at=? WHERE id=?
                   AND status IN ('OPEN','EXIT_PENDING')""",
                (str(reason), str(reason), int(now_ms), int(trade_id)))
            self._sync_lock_lifecycle(trade_id, "PENDING_RESOLUTION", now_ms)

    def resolution_trades_due(self, now_ms: int, limit: int = 12) -> list[dict]:
        return self._rows(
            """SELECT * FROM lite_trades
               WHERE status IN ('PENDING_RESOLUTION','UNRESOLVED_RETRYING','UNRESOLVED_FINAL')
                 AND COALESCE(next_attempt_at,window_close_ts)<=?
               ORDER BY COALESCE(next_attempt_at,window_close_ts),window_close_ts
               LIMIT ?""", (int(now_ms), max(1, int(limit))))

    def pending_trades_due(self, now_ms: int, retry_s: float = 0) -> list[dict]:
        del retry_s
        return self.resolution_trades_due(now_ms, 1000)

    def begin_resolution_attempt(self, trade_id: int, now_ms: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET status='UNRESOLVED_RETRYING',
                   last_attempt_at=? WHERE id=? AND status IN
                   ('PENDING_RESOLUTION','UNRESOLVED_RETRYING','UNRESOLVED_FINAL')""",
                (int(now_ms), int(trade_id)))
            self._sync_lock_lifecycle(trade_id, "UNRESOLVED_RETRYING", now_ms)

    def note_resolution_retry(self, trade_id: int, now_ms: int, reason: str,
                              base_s: float = 30.0, cap_s: float = 300.0,
                              max_retries: int = 30,
                              final_recheck_s: float = 3600.0) -> int:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT retry_count FROM lite_trades WHERE id=?", (int(trade_id),)).fetchone()
            retry = int(row[0] if row else 0) + 1
            final = retry >= int(max_retries)
            delay_s = float(final_recheck_s) if final else min(
                float(cap_s), float(base_s) * (2 ** min(retry - 1, 8)))
            status = "UNRESOLVED_FINAL" if final else "UNRESOLVED_RETRYING"
            self._conn.execute(
                """UPDATE lite_trades SET status=?,retry_count=?,last_attempt_at=?,
                   last_error=?,next_attempt_at=?,resolution_source='unresolved',
                   resolution_reason=?,pnl=NULL WHERE id=?""",
                (status, retry, int(now_ms), str(reason),
                 int(now_ms + delay_s * 1000), str(reason), int(trade_id)))
            self._conn.execute(
                """INSERT OR REPLACE INTO lite_resolution_attempts(
                   trade_id,attempt_no,attempt_ts,result,error,resolution_source)
                   VALUES(?,?,?,?,?,'official_outcome')""",
                (int(trade_id), retry, int(now_ms), status, str(reason)))
            self._sync_lock_lifecycle(trade_id, status, now_ms)
        return retry

    def mark_unresolved(self, trade_id: int, now_ms: int, reason: str) -> None:
        self.note_resolution_retry(
            trade_id, now_ms, reason, max_retries=1, final_recheck_s=3600.0)

    def complete_trade(self, trade_id: int, status: str,
                       exit_price: Optional[float], exit_ts: int,
                       pnl: Optional[float], resolution_source: str,
                       resolution_reason: str, *, gross_pnl: Optional[float] = None,
                       exit_fee: float = 0.0, resolution_verified: bool = False,
                       exit_evidence: Optional[dict] = None,
                       entry_fee: Optional[float] = None,
                       fee_rate: Optional[float] = None) -> None:
        if status not in TERMINAL_STATUSES:
            raise ValueError("invalid Lite terminal status")
        evidence = exit_evidence or {}
        fill_levels = evidence.get("fill_levels")
        serialized_levels = (json.dumps(fill_levels, separators=(",", ":"))
                             if fill_levels is not None else None)
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET status=?,exit_price=?,exit_ts=?,pnl=?,
                    gross_pnl=?,exit_fee=?,entry_fee=COALESCE(?,entry_fee),
                    fee_rate=COALESCE(?,fee_rate),resolution_source=?,resolution_reason=?,
                   resolution_verified=?,last_error=NULL,next_attempt_at=NULL,
                   exit_book_ts=?,exit_book_received_ts=?,exit_book_age_ms=?,
                   exit_book_hash=?,exit_best_bid=?,exit_best_ask=?,
                    exit_fill_shares=?,exit_bid_depth_shares=?,exit_spread=?,
                    exit_fill_levels=?,exit_worst_price=?,
                   accounting_version=2 WHERE id=? AND status IN
                   ('OPEN','EXIT_PENDING','PENDING_RESOLUTION',
                    'UNRESOLVED_RETRYING','UNRESOLVED_FINAL')""",
                (status, exit_price, int(exit_ts), pnl,
                  gross_pnl if gross_pnl is not None else pnl, float(exit_fee),
                  entry_fee, fee_rate,
                  str(resolution_source), str(resolution_reason),
                 int(bool(resolution_verified)), evidence.get("book_ts"),
                 evidence.get("received_ts"), evidence.get("age_ms"),
                 evidence.get("book_hash"), evidence.get("best_bid"),
                  evidence.get("best_ask"), evidence.get("fill_shares"),
                  evidence.get("bid_depth_shares"), evidence.get("spread"),
                  serialized_levels, evidence.get("worst_price"),
                  int(trade_id)))
            self._sync_lock_lifecycle(trade_id, status, exit_ts)

    def apply_verified_official_resolution(self, trade_id: int, *, outcome: str,
                                           evidence_ts: int, fee_rate: float,
                                           reason: str = "exact_gamma_backfill") -> bool:
        if str(outcome) not in ("YES", "NO"):
            raise ValueError("official outcome must be exactly YES or NO")
        if (isinstance(evidence_ts, bool) or int(evidence_ts) <= 0
                or not math.isfinite(float(fee_rate)) or not 0 <= float(fee_rate) <= 1):
            raise ValueError("invalid official-resolution evidence")
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM lite_trades WHERE id=?", (int(trade_id),)).fetchone()
            if row is None:
                return False
            trade = dict(row)
            if trade["status"] not in (
                    "PENDING_RESOLUTION", "UNRESOLVED_RETRYING",
                    "UNRESOLVED_FINAL", "CLOSED_BOOK_EXIT"):
                return False
            won = ((trade["side"] == "BUY_YES" and outcome == "YES") or
                   (trade["side"] == "BUY_NO" and outcome == "NO"))
            gross = FIXED_SHARES * ((1.0 if won else 0.0) - float(trade["entry_price"]))
            entry_fee = taker_fee(FIXED_SHARES, float(trade["entry_price"]), fee_rate)
            net = gross - entry_fee
            previous = f"previous={trade['status']}:{trade.get('resolution_source') or ''}"
            status = "CLOSED_WIN" if won else "CLOSED_LOSS"
            self._conn.execute(
                """UPDATE lite_trades SET status=?,exit_price=?,exit_ts=?,gross_pnl=?,
                   entry_fee=?,exit_fee=0,fee_rate=?,pnl=?,resolution_source='official_outcome',
                   resolution_reason=?,resolution_verified=1,last_error=NULL,
                   next_attempt_at=NULL,accounting_version=2,legacy_note=? WHERE id=?""",
                (status, 1.0 if won else 0.0, int(evidence_ts), gross, entry_fee,
                 float(fee_rate), net, str(reason), previous, int(trade_id)))
            self._sync_lock_lifecycle(trade_id, status, evidence_ts)
        return True

    def apply_legacy_book_fee(self, trade_id: int, fee_rate: float) -> bool:
        if not math.isfinite(float(fee_rate)) or not 0 <= float(fee_rate) <= 1:
            raise ValueError("invalid legacy fee rate")
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM lite_trades WHERE id=? AND status='CLOSED_BOOK_EXIT'",
                (int(trade_id),)).fetchone()
            if row is None:
                return False
            trade = dict(row)
            gross = float(trade["gross_pnl"] if trade["gross_pnl"] is not None else trade["pnl"])
            entry_fee = taker_fee(FIXED_SHARES, float(trade["entry_price"]), fee_rate)
            exit_fee = taker_fee(FIXED_SHARES, float(trade["exit_price"]), fee_rate)
            self._conn.execute(
                """UPDATE lite_trades SET gross_pnl=?,entry_fee=?,exit_fee=?,fee_rate=?,
                   pnl=?,accounting_version=2,legacy_note=? WHERE id=?""",
                (gross, entry_fee, exit_fee, float(fee_rate),
                 gross - entry_fee - exit_fee,
                 "legacy_book_quote_depth_not_reconstructable", int(trade_id)))
        return True

    def _sync_lock_lifecycle(self, trade_id: int, lifecycle: str, now_ms: int) -> None:
        self._conn.execute(
            """UPDATE lite_window_locks SET lifecycle_status=?,last_updated_ts=?
               WHERE trade_id=?""", (str(lifecycle), int(now_ms), int(trade_id)))

    def finalize_window_locks(self, now_ms: int) -> None:
        marks = ",".join("?" for _ in TERMINAL_STATUSES)
        with self._lock, self._conn:
            self._conn.execute(
                f"""UPDATE lite_window_locks SET status='COMPLETE',
                    lifecycle_status='COMPLETE',last_updated_ts=?
                    WHERE trade_id IN (
                        SELECT id FROM lite_trades WHERE status IN ({marks})
                    ) AND window_close_ts<=?""",
                (int(now_ms), *TERMINAL_STATUSES, int(now_ms)))

    def last_trade_ts(self) -> Optional[int]:
        with self._lock:
            row = self._conn.execute("SELECT MAX(entry_ts) FROM lite_trades").fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def risk_snapshot(self, now_ms: int) -> dict[str, Any]:
        metrics = self.dashboard_metrics(now_ms, include_rows=False)
        terminal = self._rows(
            """SELECT pnl FROM lite_trades WHERE status IN
               ('CLOSED_BOOK_EXIT','CLOSED_WIN','CLOSED_LOSS') AND pnl IS NOT NULL
               ORDER BY exit_ts DESC,id DESC""")
        streak = 0
        for row in terminal:
            if float(row["pnl"]) < 0:
                streak += 1
            else:
                break
        return {
            "today_realized_pnl": metrics["today_lite_pnl"],
            "consecutive_losses": streak,
            "committed_exposure_usd": self.committed_exposure(),
        }

    @staticmethod
    def _performance(rows: list[dict]) -> dict[str, Any]:
        values = [float(row["pnl"]) for row in rows if row.get("pnl") is not None]
        wins = [value for value in values if value > 0]
        losses = [value for value in values if value < 0]
        by_exit: dict[int, float] = defaultdict(float)
        for row in rows:
            if row.get("pnl") is not None and row.get("exit_ts") is not None:
                by_exit[int(row["exit_ts"])] += float(row["pnl"])
        cumulative = peak = drawdown = 0.0
        for exit_ts in sorted(by_exit):
            cumulative += by_exit[exit_ts]
            peak = max(peak, cumulative)
            drawdown = max(drawdown, peak - cumulative)
        hold = [
            (int(row["exit_ts"]) - int(row["entry_ts"])) / 1000.0
            for row in rows if row.get("exit_ts") is not None
        ]
        total = sum(values)
        gross_profit = sum(wins)
        gross_loss = -sum(losses)
        return {
            "count": len(values), "pnl": round(total, 10),
            "winrate": round(len(wins) / len(values), 6) if values else None,
            "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss else None,
            "expectancy": round(total / len(values), 10) if values else None,
            "gross_profit": round(gross_profit, 10),
            "gross_loss": round(gross_loss, 10),
            "average_win": round(gross_profit / len(wins), 10) if wins else None,
            "average_loss": round(sum(losses) / len(losses), 10) if losses else None,
            "max_drawdown": round(drawdown, 10),
            "average_hold_seconds": round(sum(hold) / len(hold), 6) if hold else None,
        }

    def dashboard_metrics(self, now_ms: int, *, include_rows: bool = True) -> dict[str, Any]:
        local_now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).astimezone()
        midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_ms = int(midnight.timestamp() * 1000)
        all_rows = self._rows("SELECT * FROM lite_trades ORDER BY exit_ts,id")
        terminal = [row for row in all_rows if row["status"] in TERMINAL_STATUSES
                    and row.get("pnl") is not None]
        verified = [row for row in terminal if bool(row.get("execution_verified"))]
        perf = self._performance(terminal)
        verified_perf = self._performance(verified)
        statuses: dict[str, int] = defaultdict(int)
        for row in all_rows:
            statuses[str(row["status"])] += 1
        entries_asset: dict[str, int] = defaultdict(int)
        entries_side: dict[str, int] = defaultdict(int)
        for row in all_rows:
            entries_asset[str(row["asset"])] += 1
            entries_side[str(row["side"])] += 1

        def terminal_breakdown(field: str) -> dict[str, dict[str, float | int]]:
            grouped: dict[str, list[dict]] = defaultdict(list)
            for row in terminal:
                grouped[str(row.get(field) or "UNKNOWN")].append(row)
            return {
                key: {"trades": len(rows), "pnl": round(sum(float(r["pnl"]) for r in rows), 10)}
                for key, rows in sorted(grouped.items())
            }

        with self._lock:
            conn = self._conn
            rejects = {str(row[0]): int(row[1]) for row in conn.execute(
                """SELECT reject_reason,SUM(count) total FROM lite_rejects
                   GROUP BY reject_reason ORDER BY total DESC LIMIT 12""").fetchall()}
            source_counts = {str(row[0] or ""): int(row[1]) for row in conn.execute(
                """SELECT resolution_source,COUNT(*) FROM lite_trades
                   WHERE status IN ('CLOSED_BOOK_EXIT','CLOSED_WIN','CLOSED_LOSS')
                   GROUP BY resolution_source""").fetchall()}
            cutoff = int(now_ms) - 3_600_000
            decisions = {str(row[0]): int(row[1]) for row in conn.execute(
                """SELECT decision,SUM(count) FROM lite_decision_buckets
                   WHERE timestamp_bucket>=? GROUP BY decision""", (cutoff,)).fetchall()}
            conflicts = int(conn.execute(
                """SELECT COUNT(*) FROM (
                   SELECT asset,window_close_ts FROM lite_trades
                   GROUP BY asset,window_close_ts HAVING COUNT(DISTINCT side)>1)""").fetchone()[0])
            lock_counts = {str(row[0]): int(row[1]) for row in conn.execute(
                "SELECT status,COUNT(*) FROM lite_window_locks GROUP BY status").fetchall()}
            active_locks = int(conn.execute(
                """SELECT COUNT(*) FROM lite_window_locks WHERE status IN
                   ('DIRECTION_LOCKED','WAIT_FOR_PULLBACK','ENTERED')""").fetchone()[0])
            last_error_row = conn.execute(
                """SELECT last_error FROM lite_trades WHERE last_error IS NOT NULL
                   AND last_error!='' ORDER BY COALESCE(last_attempt_at,entry_ts) DESC LIMIT 1""").fetchone()
            recent_trade_writes = int(conn.execute(
                """SELECT COUNT(*) FROM lite_trades
                   WHERE entry_ts>=? OR COALESCE(exit_ts,0)>=? OR COALESCE(last_attempt_at,0)>=?""",
                (int(now_ms)-60_000,)*3).fetchone()[0])
            recent_bucket_writes = sum(int(row[0] or 0) for row in (
                conn.execute("SELECT SUM(count) FROM lite_rejects WHERE timestamp_bucket>=?",
                             (int(now_ms)-60_000,)).fetchone(),
                conn.execute("SELECT SUM(count) FROM lite_decision_buckets WHERE timestamp_bucket>=?",
                             (int(now_ms)-60_000,)).fetchone(),
            ))

        db_size = sum(
            candidate.stat().st_size for suffix in ("", "-wal", "-shm")
            if (candidate := Path(f"{self.path}{suffix}")).exists())
        today_pnl = sum(
            float(row["pnl"]) for row in terminal
            if row.get("exit_ts") is not None and int(row["exit_ts"]) >= today_start_ms)
        anchor = sum(bool(row.get("anchor_available")) for row in all_rows)
        recent = sorted(all_rows, key=lambda row: int(row["entry_ts"]), reverse=True)[:20]
        return {
            "open_positions": statuses.get("OPEN", 0) + statuses.get("EXIT_PENDING", 0),
            "exit_pending": statuses.get("EXIT_PENDING", 0),
            "completed_trades": perf["count"],
            "verified_completed_trades": verified_perf["count"],
            "pending_resolution": statuses.get("PENDING_RESOLUTION", 0),
            "unresolved_retrying": statuses.get("UNRESOLVED_RETRYING", 0),
            "unresolved_final": statuses.get("UNRESOLVED_FINAL", 0),
            "total_lite_pnl": perf["pnl"],
            "verified_realized_pnl": verified_perf["pnl"],
            "today_lite_pnl": round(today_pnl, 10),
            "winrate": perf["winrate"], "profit_factor": perf["profit_factor"],
            "expectancy": perf["expectancy"], "gross_profit": perf["gross_profit"],
            "gross_loss": perf["gross_loss"], "average_win": perf["average_win"],
            "average_loss": perf["average_loss"], "max_drawdown": perf["max_drawdown"],
            "average_hold_seconds": perf["average_hold_seconds"],
            "verified_metrics": verified_perf,
            "entries_by_asset": dict(sorted(entries_asset.items())),
            "entries_by_side": dict(sorted(entries_side.items())),
            "performance_by_asset": terminal_breakdown("asset"),
            "performance_by_side": terminal_breakdown("side"),
            "performance_by_entry_mode": terminal_breakdown("entry_mode"),
            "anchor_breakdown": {"anchor": anchor, "no_anchor": len(all_rows)-anchor},
            "resolution_source_breakdown": {
                "book_exit": source_counts.get("book_exit", 0),
                "official_outcome": source_counts.get("official_outcome", 0),
                "unresolved": statuses.get("UNRESOLVED_FINAL", 0),
            },
            "historical_both_side_conflicts": conflicts,
            "asset_window_locks": {"active": active_locks, "by_status": lock_counts},
            "anti_dead_bot_last_hour": decisions,
            "top_reject_reasons": rejects,
            "committed_exposure_usd": round(self.committed_exposure(), 10),
            "last_error": str(last_error_row[0]) if last_error_row else None,
            "last_20_trades": recent if include_rows else [],
            "db_diagnostics": {
                "size_bytes": db_size,
                "writes_per_min": recent_trade_writes + recent_bucket_writes,
            },
        }

    def table_count(self, table: str) -> int:
        allowed = {
            "lite_trades", "lite_rejects", "lite_decision_buckets",
            "lite_window_locks", "lite_resolution_attempts",
        }
        if table not in allowed:
            raise ValueError("unknown Lite table")
        with self._lock:
            row = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0])

    def close(self) -> None:
        with self._lock:
            self._conn.close()
