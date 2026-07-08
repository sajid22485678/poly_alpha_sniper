"""Store factory from DATABASE_URL.

NOTE: the default URL `sqlite:///storage/poly_alpha_sniper.db` is remapped to
`<project_root>/storage_data/poly_alpha_sniper.db` because `storage/` is the
python package directory — the data file must not live inside the package.
"""
from __future__ import annotations

from pathlib import Path

from poly_alpha_sniper.core.config_loader import PROJECT_ROOT
from poly_alpha_sniper.core.contracts import Store
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("db")


def default_sqlite_path() -> str:
    return str(PROJECT_ROOT / "storage_data" / "poly_alpha_sniper.db")


def get_store(database_url: str = "") -> Store:
    url = database_url or "sqlite:///storage/poly_alpha_sniper.db"
    if url.startswith("sqlite:///"):
        raw = url[len("sqlite:///"):]
        # remap package dir -> data dir
        if raw.replace("\\", "/").startswith("storage/"):
            raw = "storage_data/" + raw.replace("\\", "/")[len("storage/"):]
        p = Path(raw)
        if not p.is_absolute():
            p = PROJECT_ROOT / p
        from poly_alpha_sniper.storage.sqlite_store import SqliteStore
        return SqliteStore(str(p))
    if url.startswith(("postgres://", "postgresql://")):
        from poly_alpha_sniper.storage.postgres_store import PostgresStore
        return PostgresStore(url)
    raise ValueError(f"unsupported DATABASE_URL scheme: {url.split(':', 1)[0]}")
