"""SQLite implementation of the contracts.Store protocol.

Thread-safe (single connection + lock), WAL mode for crash safety.
`insert` silently drops row keys that are not columns of the target table so
callers can pass rich dicts without schema coupling.

SECURITY: any row key that looks like a secret raises ValueError — secrets
must never reach the database (defense in depth on top of the logger filter).
"""
from __future__ import annotations

import re
import sqlite3
import threading
from pathlib import Path
from typing import Any

_SECRET_KEY_RE = re.compile(r"(?i)(private_key|api_secret|passphrase|password|bot_token|auth_header|signed_payload)")


class SqliteStore:
    def __init__(self, path: str):
        self._path = str(path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False, timeout=10)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        self._columns_cache: dict[str, list[str]] = {}

    @property
    def path(self) -> str:
        return self._path

    def _columns(self, table: str) -> list[str]:
        if table not in self._columns_cache:
            rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
            self._columns_cache[table] = [r["name"] for r in rows]
        return self._columns_cache[table]

    def insert(self, table: str, row: dict[str, Any]) -> None:
        for key in row:
            if _SECRET_KEY_RE.search(str(key)):
                raise ValueError(f"refusing to store secret-like key {key!r} in database")
        cols = [c for c in self._columns(table) if c in row]
        if not cols:
            return
        placeholders = ",".join("?" for _ in cols)
        sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
        values = tuple(_coerce(row[c]) for c in cols)
        with self._lock:
            self._conn.execute(sql, values)
            self._conn.commit()

    def executemany_rows(self, table: str, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            self.insert(table, row)

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()
        self._columns_cache.clear()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
                self._conn.close()
            except sqlite3.Error:
                pass


def _coerce(v: Any) -> Any:
    if isinstance(v, bool):
        return int(v)
    if v is None or isinstance(v, (int, float, str, bytes)):
        return v
    if hasattr(v, "value"):
        return v.value
    return str(v)
