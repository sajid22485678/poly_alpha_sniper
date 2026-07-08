"""Optional Postgres adapter implementing the Store protocol.

Requires `pip install psycopg[binary]`. SQLite is the default; this adapter
exists so larger deployments can switch by changing DATABASE_URL only.
"""
from __future__ import annotations

import re
import threading
from typing import Any

_SECRET_KEY_RE = re.compile(r"(?i)(private_key|api_secret|passphrase|password|bot_token|auth_header|signed_payload)")


class PostgresStore:
    def __init__(self, dsn: str):
        try:
            import psycopg  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dep
            raise RuntimeError(
                "Postgres support requires: pip install 'psycopg[binary]'") from exc
        self._psycopg = psycopg
        self._conn = psycopg.connect(dsn, autocommit=True)
        self._lock = threading.Lock()
        self._columns_cache: dict[str, list[str]] = {}

    @property
    def path(self) -> str:
        return ""

    def _columns(self, table: str) -> list[str]:
        if table not in self._columns_cache:
            with self._lock, self._conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
                    (table,))
                self._columns_cache[table] = [r[0] for r in cur.fetchall()]
        return self._columns_cache[table]

    def insert(self, table: str, row: dict[str, Any]) -> None:
        for key in row:
            if _SECRET_KEY_RE.search(str(key)):
                raise ValueError(f"refusing to store secret-like key {key!r}")
        cols = [c for c in self._columns(table) if c in row]
        if not cols:
            return
        sql = (f"INSERT INTO {table} ({','.join(cols)}) "
               f"VALUES ({','.join('%s' for _ in cols)})")
        with self._lock, self._conn.cursor() as cur:
            cur.execute(sql, tuple(row[c] for c in cols))

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        sql = sql.replace("?", "%s")
        with self._lock, self._conn.cursor() as cur:
            cur.execute(sql, params)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]

    def execute(self, sql: str, params: tuple = ()) -> None:
        sql = sql.replace("?", "%s")
        with self._lock, self._conn.cursor() as cur:
            cur.execute(sql, params)
        self._columns_cache.clear()

    def close(self) -> None:
        self._conn.close()
