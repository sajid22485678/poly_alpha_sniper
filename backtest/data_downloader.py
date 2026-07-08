"""Shared async download helpers (chunked GET, retry, resumable CSV append)."""
from __future__ import annotations

import asyncio
import csv
from pathlib import Path

import aiohttp


async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict,
                     retries: int = 3) -> list | dict:
    for attempt in range(retries):
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientError:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(1.5 * (attempt + 1))
    return []


def append_rows(path: str, rows: list[dict], fieldnames: list[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    new_file = not p.exists() or p.stat().st_size == 0
    with p.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def last_ts_ms(path: str) -> int:
    """Resume point: last ts_ms in an existing CSV (0 when absent)."""
    p = Path(path)
    if not p.exists():
        return 0
    last = 0
    try:
        with p.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    last = max(last, int(row.get("ts_ms") or 0))
                except ValueError:
                    continue
    except OSError:
        return 0
    return last
