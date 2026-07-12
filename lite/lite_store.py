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
    "runtime_commit", "model_version", "return_5s", "acceleration",
    "window_return", "reliability", "cex_adjustment",
    "lead_lag_adjustment", "market_probability_yes",
    "fair_probability_yes", "fair_probability_no", "calibration_bucket",
    "executable_yes_price", "executable_no_price", "estimated_yes_fee",
    "estimated_no_fee", "execution_buffer_yes", "execution_buffer_no",
    "uncertainty_buffer", "net_edge_yes", "net_edge_no",
    "selected_net_edge", "edge_bucket", "lead_lag_status", "cex_move_ts",
    "poly_book_ts", "lead_lag_ms", "poly_response", "execution_state",
    "maker_price", "maker_start_ts", "maker_deadline_ts", "maker_wait_ms",
    "maker_fill_assumed", "maker_fill_model", "lock_ts",
    "initial_executable_yes_price", "initial_executable_no_price",
    "initial_net_edge_yes", "initial_net_edge_no", "initial_selected_net_edge",
    "initial_fair_probability_yes", "initial_fair_probability_no",
    "initial_cex_adjustment", "initial_lead_lag_status", "initial_entry_reason",
    "final_executable_yes_price", "final_executable_no_price",
    "final_net_edge_yes", "final_net_edge_no", "final_selected_net_edge",
    "final_fair_probability_yes", "final_fair_probability_no",
    "final_cex_adjustment", "final_lead_lag_status",
    "pullback_start_ts", "pullback_condition", "wait_deadline_ts",
    "last_management_ts", "exit_now_value", "hold_expected_value",
    "exit_fair_probability", "thesis_status", "management_reason",
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
    "runtime_commit": "TEXT",
    "model_version": "TEXT",
    "return_5s": "REAL",
    "acceleration": "REAL",
    "window_return": "REAL",
    "reliability": "REAL",
    "cex_adjustment": "REAL",
    "lead_lag_adjustment": "REAL",
    "market_probability_yes": "REAL",
    "fair_probability_yes": "REAL",
    "fair_probability_no": "REAL",
    "calibration_bucket": "TEXT",
    "executable_yes_price": "REAL",
    "executable_no_price": "REAL",
    "estimated_yes_fee": "REAL",
    "estimated_no_fee": "REAL",
    "execution_buffer_yes": "REAL",
    "execution_buffer_no": "REAL",
    "uncertainty_buffer": "REAL",
    "net_edge_yes": "REAL",
    "net_edge_no": "REAL",
    "selected_net_edge": "REAL",
    "edge_bucket": "TEXT",
    "lead_lag_status": "TEXT",
    "cex_move_ts": "INTEGER",
    "poly_book_ts": "INTEGER",
    "lead_lag_ms": "INTEGER",
    "poly_response": "REAL",
    "execution_state": "TEXT",
    "maker_price": "REAL",
    "maker_start_ts": "INTEGER",
    "maker_deadline_ts": "INTEGER",
    "maker_wait_ms": "INTEGER",
    "maker_fill_assumed": "INTEGER NOT NULL DEFAULT 0",
    "maker_fill_model": "TEXT",
    "lock_ts": "INTEGER",
    "initial_executable_yes_price": "REAL",
    "initial_executable_no_price": "REAL",
    "initial_net_edge_yes": "REAL",
    "initial_net_edge_no": "REAL",
    "initial_selected_net_edge": "REAL",
    "initial_fair_probability_yes": "REAL",
    "initial_fair_probability_no": "REAL",
    "initial_cex_adjustment": "REAL",
    "initial_lead_lag_status": "TEXT",
    "initial_entry_reason": "TEXT",
    "final_executable_yes_price": "REAL",
    "final_executable_no_price": "REAL",
    "final_net_edge_yes": "REAL",
    "final_net_edge_no": "REAL",
    "final_selected_net_edge": "REAL",
    "final_fair_probability_yes": "REAL",
    "final_fair_probability_no": "REAL",
    "final_cex_adjustment": "REAL",
    "final_lead_lag_status": "TEXT",
    "pullback_start_ts": "INTEGER",
    "pullback_condition": "TEXT",
    "wait_deadline_ts": "INTEGER",
    "last_management_ts": "INTEGER",
    "exit_now_value": "REAL",
    "hold_expected_value": "REAL",
    "exit_fair_probability": "REAL",
    "thesis_status": "TEXT",
    "management_reason": "TEXT",
}

_LOCK_MIGRATION_COLUMNS = {
    "return_5s": "REAL",
    "acceleration": "REAL",
    "window_return": "REAL",
    "reliability": "REAL",
    "cex_adjustment": "REAL",
    "lead_lag_adjustment": "REAL",
    "market_probability_yes": "REAL",
    "fair_probability_yes": "REAL",
    "fair_probability_no": "REAL",
    "calibration_bucket": "TEXT",
    "executable_yes_price": "REAL",
    "executable_no_price": "REAL",
    "net_edge_yes": "REAL",
    "net_edge_no": "REAL",
    "selected_net_edge": "REAL",
    "edge_bucket": "TEXT",
    "lead_lag_status": "TEXT",
    "cex_move_ts": "INTEGER",
    "poly_book_ts": "INTEGER",
    "lead_lag_ms": "INTEGER",
    "poly_response": "REAL",
    "execution_state": "TEXT",
    "maker_price": "REAL",
    "maker_start_ts": "INTEGER",
    "maker_deadline_ts": "INTEGER",
    "maker_wait_ms": "INTEGER NOT NULL DEFAULT 0",
    "maker_fill_assumed": "INTEGER NOT NULL DEFAULT 0",
    "initial_net_edge": "REAL",
    "initial_executable_yes_price": "REAL",
    "initial_executable_no_price": "REAL",
    "initial_net_edge_yes": "REAL",
    "initial_net_edge_no": "REAL",
    "initial_selected_net_edge": "REAL",
    "initial_fair_probability_yes": "REAL",
    "initial_fair_probability_no": "REAL",
    "initial_cex_adjustment": "REAL",
    "initial_lead_lag_status": "TEXT",
    "initial_entry_reason": "TEXT",
    "final_executable_yes_price": "REAL",
    "final_executable_no_price": "REAL",
    "final_net_edge_yes": "REAL",
    "final_net_edge_no": "REAL",
    "final_selected_net_edge": "REAL",
    "final_fair_probability_yes": "REAL",
    "final_fair_probability_no": "REAL",
    "final_cex_adjustment": "REAL",
    "final_lead_lag_status": "TEXT",
    "lock_ts": "INTEGER",
    "pullback_start_ts": "INTEGER",
    "pullback_condition": "TEXT",
    "wait_deadline_ts": "INTEGER",
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


_DECISION_TELEMETRY_FIELDS = (
    "executable_yes_price", "executable_no_price",
    "net_edge_yes", "net_edge_no", "selected_net_edge",
    "fair_probability_yes", "fair_probability_no",
    "cex_adjustment", "lead_lag_status",
)


def _decision_telemetry(direction: Any, prefix: str) -> dict[str, Any]:
    return {
        f"{prefix}_{field}": _value(direction, field)
        for field in _DECISION_TELEMETRY_FIELDS
    }


class LiteStore:
    def __init__(self, db_path: str, *, max_open_positions: Optional[int] = None,
                 max_open_per_asset: Optional[int] = None,
                 exposure_cap_usd: Optional[float] = None):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._bucket_pending: dict[
            tuple[str, int, str, str, str], int
        ] = defaultdict(int)
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
                    accounting_version INTEGER NOT NULL DEFAULT 1, legacy_note TEXT,
                    runtime_commit TEXT, model_version TEXT, return_5s REAL,
                    acceleration REAL, window_return REAL, reliability REAL,
                    cex_adjustment REAL, lead_lag_adjustment REAL,
                    market_probability_yes REAL, fair_probability_yes REAL,
                    fair_probability_no REAL, calibration_bucket TEXT,
                    executable_yes_price REAL, executable_no_price REAL,
                    estimated_yes_fee REAL, estimated_no_fee REAL,
                    execution_buffer_yes REAL, execution_buffer_no REAL,
                    uncertainty_buffer REAL, net_edge_yes REAL, net_edge_no REAL,
                    selected_net_edge REAL, edge_bucket TEXT, lead_lag_status TEXT,
                    cex_move_ts INTEGER, poly_book_ts INTEGER, lead_lag_ms INTEGER,
                    poly_response REAL, execution_state TEXT, maker_price REAL,
                    maker_start_ts INTEGER, maker_deadline_ts INTEGER,
                    maker_wait_ms INTEGER, maker_fill_assumed INTEGER NOT NULL DEFAULT 0,
                    maker_fill_model TEXT, lock_ts INTEGER,
                    initial_executable_yes_price REAL,
                    initial_executable_no_price REAL,
                    initial_net_edge_yes REAL, initial_net_edge_no REAL,
                    initial_selected_net_edge REAL,
                    initial_fair_probability_yes REAL,
                    initial_fair_probability_no REAL,
                    initial_cex_adjustment REAL, initial_lead_lag_status TEXT,
                    initial_entry_reason TEXT,
                    final_executable_yes_price REAL, final_executable_no_price REAL,
                    final_net_edge_yes REAL, final_net_edge_no REAL,
                    final_selected_net_edge REAL, final_fair_probability_yes REAL,
                    final_fair_probability_no REAL, final_cex_adjustment REAL,
                    final_lead_lag_status TEXT,
                    pullback_start_ts INTEGER, pullback_condition TEXT,
                    wait_deadline_ts INTEGER, last_management_ts INTEGER,
                    exit_now_value REAL, hold_expected_value REAL,
                    exit_fair_probability REAL, thesis_status TEXT,
                    management_reason TEXT
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
                    return_5s REAL, acceleration REAL, window_return REAL,
                    reliability REAL, cex_adjustment REAL,
                    lead_lag_adjustment REAL, market_probability_yes REAL,
                    fair_probability_yes REAL,
                    fair_probability_no REAL, calibration_bucket TEXT,
                    executable_yes_price REAL, executable_no_price REAL,
                    net_edge_yes REAL, net_edge_no REAL, selected_net_edge REAL,
                    edge_bucket TEXT, lead_lag_status TEXT, cex_move_ts INTEGER,
                    poly_book_ts INTEGER, lead_lag_ms INTEGER, poly_response REAL,
                    execution_state TEXT, maker_price REAL, maker_start_ts INTEGER,
                    maker_deadline_ts INTEGER, maker_wait_ms INTEGER NOT NULL DEFAULT 0,
                    maker_fill_assumed INTEGER NOT NULL DEFAULT 0,
                    initial_net_edge REAL, lock_ts INTEGER,
                    initial_executable_yes_price REAL,
                    initial_executable_no_price REAL,
                    initial_net_edge_yes REAL, initial_net_edge_no REAL,
                    initial_selected_net_edge REAL,
                    initial_fair_probability_yes REAL,
                    initial_fair_probability_no REAL,
                    initial_cex_adjustment REAL, initial_lead_lag_status TEXT,
                    initial_entry_reason TEXT,
                    final_executable_yes_price REAL, final_executable_no_price REAL,
                    final_net_edge_yes REAL, final_net_edge_no REAL,
                    final_selected_net_edge REAL, final_fair_probability_yes REAL,
                    final_fair_probability_no REAL, final_cex_adjustment REAL,
                    final_lead_lag_status TEXT,
                    pullback_start_ts INTEGER, pullback_condition TEXT,
                    wait_deadline_ts INTEGER,
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
            existing_locks = {
                str(row[1]) for row in self._conn.execute(
                    "PRAGMA table_info(lite_window_locks)")
            }
            for name, definition in _LOCK_MIGRATION_COLUMNS.items():
                if name not in existing_locks:
                    self._conn.execute(
                        f'ALTER TABLE lite_window_locks ADD COLUMN "{name}" {definition}')
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
        lock_payload = {
            "asset": asset, "slug": identity["slug"],
            "market_id": identity["market_id"], "event_id": identity["event_id"],
            "condition_id": identity["condition_id"], "window_open_ts": open_ts,
            "window_close_ts": close_ts, "side": side,
            "status": "DIRECTION_LOCKED", "lifecycle_status": "DIRECTION_LOCKED",
            "direction_decision_ts": int(now_ms),
            "direction_output": str(_value(direction, "output", side)),
            "direction_score": _value(direction, "direction_score"),
            "yes_score": _value(direction, "yes_score"),
            "no_score": _value(direction, "no_score"),
            "score_difference": _value(direction, "score_difference"),
            "confidence": _value(direction, "confidence"),
            "direction_reason": str(_value(direction, "reason", "")),
            "return_5s": _value(direction, "return_5s"),
            "return_10s": _value(direction, "return_10s"),
            "return_30s": _value(direction, "return_30s"),
            "return_60s": _value(direction, "return_60s"),
            "tick_return": _value(direction, "tick_return"),
            "volatility": _value(direction, "volatility"),
            "acceleration": _value(direction, "acceleration"),
            "window_return": _value(direction, "window_return"),
            "reliability": _value(direction, "reliability"),
            "cex_adjustment": _value(direction, "cex_adjustment"),
            "lead_lag_adjustment": _value(direction, "lead_lag_adjustment"),
            "market_probability_yes": _value(direction, "market_probability_yes"),
            "fair_probability_yes": _value(direction, "fair_probability_yes"),
            "fair_probability_no": _value(direction, "fair_probability_no"),
            "calibration_bucket": _value(direction, "calibration_bucket"),
            "executable_yes_price": _value(direction, "executable_yes_price"),
            "executable_no_price": _value(direction, "executable_no_price"),
            "net_edge_yes": _value(direction, "net_edge_yes"),
            "net_edge_no": _value(direction, "net_edge_no"),
            "selected_net_edge": _value(direction, "selected_net_edge"),
            "initial_net_edge": _value(direction, "selected_net_edge"),
            "edge_bucket": _value(direction, "edge_bucket"),
            "lead_lag_status": _value(direction, "lead_lag_status"),
            "cex_move_ts": _value(direction, "cex_move_ts"),
            "poly_book_ts": _value(direction, "poly_book_ts"),
            "lead_lag_ms": _value(direction, "lead_lag_ms"),
            "poly_response": _value(direction, "poly_response"),
            "entry_state": "DIRECTION_LOCKED", "execution_state": "EDGE_IDENTIFIED",
            "maker_wait_ms": 0, "maker_fill_assumed": 0,
            **_decision_telemetry(direction, "initial"),
            "initial_entry_reason": str(_value(direction, "reason", "")),
            "lock_ts": int(now_ms), "idempotency_key": key,
            "last_updated_ts": int(now_ms),
        }
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
                columns = ",".join(lock_payload)
                marks = ",".join("?" for _ in lock_payload)
                self._conn.execute(
                    f"INSERT INTO lite_window_locks ({columns}) VALUES ({marks})",
                    tuple(lock_payload.values()),
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
            "last_updated_ts", "trade_id", "market_probability_yes",
            "fair_probability_yes", "fair_probability_no", "calibration_bucket",
            "executable_yes_price", "executable_no_price", "net_edge_yes",
            "net_edge_no", "selected_net_edge", "edge_bucket",
            "lead_lag_status", "cex_move_ts", "poly_book_ts", "lead_lag_ms",
            "poly_response", "execution_state", "maker_price", "maker_start_ts",
            "maker_deadline_ts", "maker_wait_ms", "maker_fill_assumed",
            "final_executable_yes_price", "final_executable_no_price",
            "final_net_edge_yes", "final_net_edge_no", "final_selected_net_edge",
            "final_fair_probability_yes", "final_fair_probability_no",
            "final_cex_adjustment", "final_lead_lag_status",
            "lock_ts", "pullback_start_ts",
            "pullback_condition", "wait_deadline_ts", "return_5s",
            "acceleration", "window_return", "reliability", "cex_adjustment",
            "lead_lag_adjustment",
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
                            chase_prevented: bool = False,
                            final_direction: Any = None) -> None:
        lock = self.get_window_lock(asset, window_close_ts)
        fields = {
            "status": "SKIPPED", "lifecycle_status": "SKIPPED",
            "entry_state": "SKIPPED", "final_entry_reason": str(reason),
            "missed_opportunity": int(bool(missed)),
            "chase_prevented": int(bool(chase_prevented)),
            "maker_fill_assumed": 0, "last_updated_ts": int(now_ms),
        }
        if final_direction is not None:
            fields.update(_decision_telemetry(final_direction, "final"))
        maker_start_ts = lock.get("maker_start_ts") if lock is not None else None
        if maker_start_ts is not None:
            actual_wait_ms = max(0, int(now_ms) - int(maker_start_ts))
            fields["wait_duration_ms"] = actual_wait_ms
            fields["maker_wait_ms"] = actual_wait_ms
        self.update_window_lock(asset, window_close_ts, **fields)

    def insert_trade(self, row: dict[str, Any]) -> int:
        payload = {key: row.get(key) for key in TRADE_COLUMNS}
        for field in _DECISION_TELEMETRY_FIELDS:
            final_field = f"final_{field}"
            if payload.get(final_field) is None:
                payload[final_field] = row.get(field)
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
        payload["maker_fill_assumed"] = int(bool(payload.get("maker_fill_assumed")))
        if payload["maker_fill_assumed"]:
            raise ValueError("Lite cannot persist an assumed maker fill")
        payload["maker_wait_ms"] = int(payload.get("maker_wait_ms") or 0)
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
                existing_lock = dict(existing_row) if existing_row is not None else {}
                for field in _DECISION_TELEMETRY_FIELDS:
                    initial_field = f"initial_{field}"
                    if payload.get(initial_field) is None:
                        payload[initial_field] = (
                            existing_lock.get(initial_field)
                            if existing_lock.get(initial_field) is not None
                            else existing_lock.get(field)
                            if existing_lock.get(field) is not None
                            else row.get(field)
                        )
                if payload.get("initial_entry_reason") is None:
                    payload["initial_entry_reason"] = (
                        existing_lock.get("initial_entry_reason")
                        if existing_lock.get("initial_entry_reason") is not None
                        else existing_lock.get("direction_reason")
                        if existing_lock.get("direction_reason") is not None
                        else row.get("direction_reason")
                    )
                maker_start_ts = payload.get("maker_start_ts")
                if maker_start_ts is None:
                    maker_start_ts = existing_lock.get("maker_start_ts")
                    payload["maker_start_ts"] = maker_start_ts
                if maker_start_ts is not None:
                    actual_wait_ms = max(
                        0, int(payload["entry_ts"]) - int(maker_start_ts))
                    payload["wait_duration_ms"] = actual_wait_ms
                    payload["maker_wait_ms"] = actual_wait_ms
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
                            "DIRECTION_LOCKED", "WAIT_FOR_PULLBACK", "MAKER_WAIT"):
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
                       chase_prevented=?,final_entry_reason=?,execution_state=?,
                       maker_price=?,maker_start_ts=?,maker_deadline_ts=?,
                       maker_wait_ms=?,maker_fill_assumed=0,last_updated_ts=?
                       WHERE asset=? AND window_close_ts=?""",
                    (str(payload.get("entry_mode") or "ENTER_NOW"), trade_id,
                     payload.get("actual_improvement"), payload.get("wait_duration_ms") or 0,
                     payload.get("missed_opportunity") or 0,
                     payload.get("chase_prevented") or 0,
                     payload.get("final_entry_reason"), payload.get("execution_state"),
                     payload.get("maker_price"), payload.get("maker_start_ts"),
                     payload.get("maker_deadline_ts"), payload.get("maker_wait_ms") or 0,
                     int(payload["entry_ts"]),
                      asset, close_ts),
                )
                initial_columns = [
                    f"initial_{field}" for field in _DECISION_TELEMETRY_FIELDS
                ] + ["initial_entry_reason"]
                final_columns = [
                    f"final_{field}" for field in _DECISION_TELEMETRY_FIELDS
                ]
                initial_updates = ",".join(
                    f'"{column}"=COALESCE("{column}",?)'
                    for column in initial_columns)
                final_updates = ",".join(
                    f'"{column}"=?' for column in final_columns)
                self._conn.execute(
                    f"""UPDATE lite_window_locks
                        SET {initial_updates},{final_updates}
                        WHERE asset=? AND window_close_ts=?""",
                    tuple(payload.get(column) for column in initial_columns)
                    + tuple(payload.get(column) for column in final_columns)
                    + (asset, close_ts),
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
            self._bucket_pending[cache_key] += 1
            # One batched upsert per rolled bucket preserves exact evaluation
            # counts without restoring a write on every three-second scan.
            stale = [key for key in self._bucket_pending
                     if key[1] < timestamp_bucket]
            self._flush_bucket_counts(stale)

    def _flush_bucket_counts(
            self, keys: Optional[Iterable[tuple[str, int, str, str, str]]] = None,
    ) -> None:
        selected = list(self._bucket_pending if keys is None else keys)
        for key in selected:
            count = int(self._bucket_pending.pop(key, 0))
            if count <= 0:
                continue
            table, timestamp_bucket, asset, slug, value = key
            if table == "lite_rejects":
                field = "reject_reason"
            elif table == "lite_decision_buckets":
                field = "decision"
            else:  # defensive: pending keys are created only above
                raise ValueError("invalid pending Lite bucket")
            self._conn.execute(
                f"""INSERT INTO {table}(timestamp_bucket,asset,slug,{field},count)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(timestamp_bucket,asset,slug,{field})
                    DO UPDATE SET count=count+excluded.count""",
                (timestamp_bucket, asset, slug, value, count),
            )

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

    def update_trade_management(
            self, trade_id: int, now_ms: int, *,
            exit_now_value: Optional[float],
            hold_expected_value: Optional[float],
            exit_fair_probability: Optional[float],
            thesis_status: str, reason: str,
    ) -> None:
        numeric = (exit_now_value, hold_expected_value, exit_fair_probability)
        for index, value in enumerate(numeric):
            if value is None:
                continue
            parsed = float(value)
            maximum = 1.0 if index == 2 else FIXED_SHARES
            if not math.isfinite(parsed) or not 0.0 <= parsed <= maximum:
                raise ValueError("invalid Lite management evidence")
        allowed_thesis = {
            "CONTINUING", "INVALIDATED", "DATA_INVALID", "RESOLUTION_PENDING",
        }
        if str(thesis_status) not in allowed_thesis or not str(reason):
            raise ValueError("invalid Lite management reason")
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE lite_trades SET last_management_ts=?,exit_now_value=?,
                   hold_expected_value=?,exit_fair_probability=?,thesis_status=?,
                   management_reason=? WHERE id=? AND status IN ('OPEN','EXIT_PENDING')""",
                (int(now_ms), exit_now_value, hold_expected_value,
                 exit_fair_probability, str(thesis_status), str(reason),
                 int(trade_id)),
            )

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
                    ) AND window_close_ts<=?
                    AND (status!='COMPLETE' OR lifecycle_status!='COMPLETE')""",
                (int(now_ms), *TERMINAL_STATUSES, int(now_ms)))
            self._conn.execute(
                """UPDATE lite_window_locks SET status='SKIPPED',
                   lifecycle_status='SKIPPED',entry_state='SKIPPED',
                   execution_state='DATA_INVALID',final_entry_reason='expired_market',
                   maker_wait_ms=CASE WHEN maker_start_ts IS NULL THEN maker_wait_ms
                       ELSE MAX(0,?-maker_start_ts) END,
                   wait_duration_ms=CASE WHEN maker_start_ts IS NULL THEN wait_duration_ms
                       ELSE MAX(0,?-maker_start_ts) END,
                   maker_fill_assumed=0,last_updated_ts=?
                   WHERE trade_id IS NULL AND window_close_ts<=?
                     AND status IN ('DIRECTION_LOCKED','MAKER_WAIT','WAIT_FOR_PULLBACK')""",
                (int(now_ms), int(now_ms), int(now_ms), int(now_ms)))

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
        verified = [row for row in terminal
                    if bool(row.get("execution_verified"))
                    and bool(row.get("resolution_verified"))]
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
            for (table, bucket, _asset, _slug, value), count in self._bucket_pending.items():
                if bucket < cutoff:
                    continue
                if table == "lite_decision_buckets":
                    decisions[value] = decisions.get(value, 0) + int(count)
                elif table == "lite_rejects":
                    rejects[value] = rejects.get(value, 0) + int(count)
            conflicts = int(conn.execute(
                """SELECT COUNT(*) FROM (
                   SELECT asset,window_close_ts FROM lite_trades
                   GROUP BY asset,window_close_ts HAVING COUNT(DISTINCT side)>1)""").fetchone()[0])
            lock_counts = {str(row[0]): int(row[1]) for row in conn.execute(
                "SELECT status,COUNT(*) FROM lite_window_locks GROUP BY status").fetchall()}
            active_locks = int(conn.execute(
                """SELECT COUNT(*) FROM lite_window_locks WHERE status IN
                   ('DIRECTION_LOCKED','WAIT_FOR_PULLBACK','MAKER_WAIT','ENTERED')
                   AND window_close_ts>?""", (int(now_ms),)).fetchone()[0])
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
            recent_bucket_events = recent_bucket_writes + sum(
                int(count) for (_table, bucket, _asset, _slug, _value), count
                in self._bucket_pending.items() if bucket >= int(now_ms)-60_000)

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
            "candidate_evaluations_last_hour": decisions.get("candidate", 0),
            "valid_markets_last_hour": decisions.get("valid_market", 0),
            "positive_edge_events_last_hour": decisions.get("positive_edge", 0),
            "no_edge_skips_last_hour": decisions.get("NO_TRADE_TRULY_NO_EDGE", 0),
            "maker_observations_last_hour": decisions.get("MAKER_WAIT", 0),
            "cross_spread_entries_last_hour": decisions.get("CROSS_SPREAD", 0),
            "top_reject_reasons": rejects,
            "committed_exposure_usd": round(self.committed_exposure(), 10),
            "last_error": str(last_error_row[0]) if last_error_row else None,
            "last_20_trades": recent if include_rows else [],
            "db_diagnostics": {
                "size_bytes": db_size,
                "writes_per_min": recent_trade_writes,
                "bucket_events_per_min": recent_bucket_events,
            },
        }

    def table_count(self, table: str) -> int:
        allowed = {
            "lite_trades", "lite_rejects", "lite_decision_buckets",
            "lite_window_locks", "lite_resolution_attempts",
        }
        if table not in allowed:
            raise ValueError("unknown Lite table")
        with self._lock, self._conn:
            pending = [key for key in self._bucket_pending if key[0] == table]
            self._flush_bucket_counts(pending)
            row = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0])

    def close(self) -> None:
        with self._lock:
            self._flush_bucket_counts()
            self._conn.commit()
            self._conn.close()
