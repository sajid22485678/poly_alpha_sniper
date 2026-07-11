"""Evidence-gated historical audit/backfill for the dedicated Lite DB.

Default mode is read-only. ``--apply`` requires an exact rollback backup and
SHA-256, then updates only rows whose direct Gamma market and event association
both match every stored identity field.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lite.lite_config import LITE_DB_PATH
from lite.lite_market import LiteGammaClient
from lite.lite_resolver import (
    event_market_row,
    market_fee_rate,
    official_outcome_from_market_row,
    validate_market_identity,
)
from lite.lite_store import LiteStore


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_rows(path: Path) -> list[dict]:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(
            "SELECT * FROM lite_trades ORDER BY id")]
    finally:
        connection.close()


def _evidence_timestamp(row: dict, fallback: int) -> int:
    for key in ("closedTime", "umaEndDate", "updatedAt"):
        value = row.get(key)
        if not value:
            continue
        try:
            text = str(value).replace(" ", "T").replace("Z", "+00:00")
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except (TypeError, ValueError):
            continue
    return int(fallback)


def _identity(trade: dict) -> dict:
    close_ts = int(trade["window_close_ts"])
    return {
        "market_id": str(trade["market_id"]),
        "slug": str(trade["slug"]),
        "condition_id": str(trade["condition_id"]),
        "window_open_ts": int(trade.get("window_open_ts") or close_ts - 300_000),
        "window_close_ts": close_ts,
        "yes_token_id": str(trade["yes_token_id"]),
        "no_token_id": str(trade["no_token_id"]),
    }


async def _audit(rows: list[dict]) -> list[dict]:
    gamma = LiteGammaClient("https://gamma-api.polymarket.com")
    semaphore = asyncio.Semaphore(8)
    market_cache: dict[str, asyncio.Task] = {}
    event_cache: dict[str, asyncio.Task] = {}

    async def limited(call, identity: str):
        async with semaphore:
            try:
                return await call(identity)
            except Exception:
                return None

    async def cached(cache: dict[str, asyncio.Task], identity: str, call):
        if identity not in cache:
            cache[identity] = asyncio.create_task(limited(call, identity))
        return await cache[identity]

    async def fetch(trade: dict) -> dict:
        market_id, event_id = str(trade["market_id"]), str(trade["event_id"])
        direct, event = await asyncio.gather(
            cached(market_cache, market_id, gamma.get_market),
            cached(event_cache, event_id, gamma.get_event),
        )
        identity = _identity(trade)
        ok, reason = validate_market_identity(direct, **identity)
        associated = None
        if ok:
            associated, reason = event_market_row(
                event, event_id=event_id, **identity)
        outcome = None
        if associated is not None:
            direct_outcome, direct_reason = official_outcome_from_market_row(
                direct, **identity)
            outcome, reason = official_outcome_from_market_row(
                associated, **identity)
            if direct_outcome != outcome:
                outcome, reason = None, f"direct_event_outcome_mismatch:{direct_reason}"
        rate, fee_source = market_fee_rate(associated)
        exact = associated is not None and outcome in ("YES", "NO") \
            and fee_source == "exact_fee_schedule"
        return {
            "trade_id": int(trade["id"]), "asset": trade["asset"],
            "side": trade["side"], "status_before": trade["status"],
            "market_id": market_id, "event_id": event_id,
            "slug": trade["slug"], "condition_id": trade["condition_id"],
            "exact": exact, "reason": reason, "outcome": outcome,
            "fee_rate": rate, "fee_source": fee_source,
            "evidence_ts": _evidence_timestamp(
                associated or {}, int(trade["window_close_ts"])),
            "post_close_book_exit": (
                trade["status"] == "CLOSED_BOOK_EXIT"
                and int(trade.get("exit_ts") or 0) >= int(trade["window_close_ts"])),
        }

    try:
        return list(await asyncio.gather(*(fetch(row) for row in rows)))
    finally:
        await gamma.close()


def _conflicts(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (
            row["asset"], row["slug"], row["market_id"], row["event_id"],
            row["condition_id"], row["window_close_ts"],
        )
        grouped.setdefault(key, []).append(row)
    out = []
    for key, members in grouped.items():
        if len({row["side"] for row in members}) < 2:
            continue
        out.append({
            "asset": key[0], "slug": key[1], "market_id": key[2],
            "event_id": key[3], "condition_id": key[4],
            "window_close_ts": key[5],
            "buy_yes_ids": [row["id"] for row in members if row["side"] == "BUY_YES"],
            "buy_no_ids": [row["id"] for row in members if row["side"] == "BUY_NO"],
        })
    return sorted(out, key=lambda row: (row["window_close_ts"], row["asset"]))


async def main_async(args) -> int:
    db_path = Path(args.db).resolve()
    baseline_path = Path(args.baseline_db).resolve()
    if db_path != Path(LITE_DB_PATH).resolve():
        raise RuntimeError("refusing non-canonical Lite DB target")
    if not baseline_path.is_file():
        raise RuntimeError("immutable baseline DB is missing")
    if args.apply:
        expected = str(args.baseline_sha256).lower()
        actual = _sha256(baseline_path).lower()
        if not expected or actual != expected:
            raise RuntimeError("rollback backup SHA-256 mismatch")

    baseline_rows = _read_rows(baseline_path)
    current_rows = _read_rows(db_path)
    evidence = await _audit(current_rows)
    by_id = {row["trade_id"]: row for row in evidence}
    unresolved = [row for row in current_rows if row["status"] == "UNRESOLVED_FINAL"]
    eligible_unresolved = [row for row in unresolved if by_id[row["id"]]["exact"]]
    post_close = [row for row in current_rows
                  if by_id[row["id"]]["post_close_book_exit"]]
    eligible_post_close = [row for row in post_close if by_id[row["id"]]["exact"]]

    applied = {"official_backfills": 0, "post_close_repaired": 0,
               "legacy_book_fees": 0}
    after_metrics = None
    if args.apply:
        store = LiteStore(str(db_path))
        try:
            official_ids = {int(row["id"]) for row in eligible_unresolved + eligible_post_close}
            for trade in eligible_unresolved + eligible_post_close:
                proof = by_id[int(trade["id"])]
                changed = store.apply_verified_official_resolution(
                    int(trade["id"]), outcome=str(proof["outcome"]),
                    evidence_ts=int(proof["evidence_ts"]),
                    fee_rate=float(proof["fee_rate"]),
                    reason="exact_gamma_market_event_backfill")
                if changed:
                    if trade in eligible_post_close:
                        applied["post_close_repaired"] += 1
                    else:
                        applied["official_backfills"] += 1
            for trade in current_rows:
                if (trade["status"] == "CLOSED_BOOK_EXIT"
                        and int(trade["id"]) not in official_ids
                        and by_id[int(trade["id"])]["exact"]):
                    if store.apply_legacy_book_fee(
                            int(trade["id"]), float(by_id[int(trade["id"])]["fee_rate"])):
                        applied["legacy_book_fees"] += 1
            store.finalize_window_locks(int(time.time() * 1000))
            after_metrics = store.dashboard_metrics(int(time.time() * 1000))
        finally:
            store.close()

    report = {
        "generated_ts_ms": int(time.time() * 1000),
        "mode": "APPLY" if args.apply else "READ_ONLY",
        "baseline_db": str(baseline_path),
        "baseline_sha256": _sha256(baseline_path),
        "baseline_trade_count": len(baseline_rows),
        "baseline_conflicts": _conflicts(baseline_rows),
        "baseline_conflict_count": len(_conflicts(baseline_rows)),
        "current_trade_count_before": len(current_rows),
        "unresolved_before": len(unresolved),
        "unresolved_exact_backfill_eligible": len(eligible_unresolved),
        "post_close_book_exits_before": len(post_close),
        "post_close_exact_repair_eligible": len(eligible_post_close),
        "evidence_failures": [row for row in evidence if not row["exact"]],
        "unresolved_audit": [by_id[int(row["id"])] for row in unresolved],
        "applied": applied,
        "after_metrics": after_metrics,
    }
    output = Path(args.report).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({
        "report": str(output), "mode": report["mode"],
        "unresolved_before": len(unresolved),
        "unresolved_eligible": len(eligible_unresolved),
        "post_close_book_exits": len(post_close), "applied": applied,
        "evidence_failures": len(report["evidence_failures"]),
    }, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=LITE_DB_PATH)
    parser.add_argument("--baseline-db", required=True)
    parser.add_argument("--baseline-sha256", default="")
    parser.add_argument("--report", required=True)
    parser.add_argument("--apply", action="store_true")
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
