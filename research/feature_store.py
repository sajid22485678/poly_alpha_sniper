"""RESEARCH / SHADOW-ONLY: point-in-time feature snapshots for every scanned
candidate. Never places orders, never affects the baseline trading decision or
live readiness.

Why: replay/challenger/drift research previously had to re-derive features
from scattered, throttled diagnostic text rows. This records one wide,
append-only row per (asset, scan) — throttled — with the exact values the
pipeline saw at that moment, so backtests are lookahead-free by construction:
a row only ever contains what was known when it was written.

Rules:
- append-only; historical rows are never mutated (scoring_version marks eras).
- lane="baseline" for the production-like shadow pipeline; experimental
  research writes lane="experimental" so stats can never mix.
- no secrets: only market/CEX/book/diagnostic values ever enter a row.
"""
from __future__ import annotations

import json
from typing import Optional

# One row per asset per THROTTLE_MS keeps volume ~ 3 assets * 8640 rows/day at
# 10s -- rich enough for research, small enough for SQLite.
THROTTLE_MS = 10_000


def should_record(last_recorded_ms: Optional[int], now_ms: int,
                  block_reason: Optional[str], near_miss_tier: str = "") -> bool:
    """Throttle ALWAYS applies, with one exception: an actual FIRED shock
    (rare by construction under the v2 AND-gate scoring) records immediately.

    Post-mortem (2026-07-10): the original exemption was block_reason is None
    ("reached shock evaluation"), which is the NORMAL state of every healthy
    scan, not a rare event -- it bypassed the throttle on ~every iteration and
    flooded the DB with ~110k rows/lane/hour (2+ GB/day incl. backups)."""
    del block_reason  # kept in the signature for call-site clarity only
    if near_miss_tier == "FIRED":
        return True
    return last_recorded_ms is None or (now_ms - last_recorded_ms) >= THROTTLE_MS


def build_scan_feature_row(*, asset: str, view, market, anchor, freshness: str,
                           block_reason: Optional[str], near_miss, book,
                           scoring_version: str, now_ms: int,
                           lane: str = "baseline") -> dict:
    """Assemble the feature row from whatever the scan actually has in hand.
    Every argument except asset/now_ms may be None; missing values stay NULL
    in the table -- never fabricated."""
    stats = view.primary if (view is not None and view.primary is not None) else None
    raw = market.raw if (market is not None and isinstance(market.raw, dict)) else {}
    anchor_diag = raw.get("oracle_anchor_diagnostics") \
        if isinstance(raw.get("oracle_anchor_diagnostics"), dict) else {}
    row = {
        "ts_ms": now_ms,
        "asset": asset,
        "lane": lane,
        "market_slug": raw.get("slug") or "",
        "market_id": market.market_id if market is not None else "",
        "event_id": str(raw.get("event_id") or ""),
        "condition_id": market.condition_id if market is not None else "",
        "yes_token_id": market.yes_token_id if market is not None else "",
        "no_token_id": market.no_token_id if market is not None else "",
        "time_to_close_s": market.seconds_to_expiry(now_ms) if market is not None else None,
        "price_to_beat": market.price_to_beat if market is not None else None,
        "anchor_status": ("available" if (anchor is not None and anchor.available)
                          else ("missing" if anchor is not None else "not_evaluated")),
        "anchor_source": (anchor.oracle_source if anchor is not None else "") or "",
        "anchor_missing_reason": str(anchor_diag.get("missing_reason") or ""),
        "cex_source": stats.exchange if stats else "",
        "cex_age_ms": stats.staleness_ms if stats else None,
        "cex_freshness": freshness,
        "cex_price": stats.price if stats else None,
        "ret_1s": stats.returns.get(1, 0.0) if stats else None,
        "ret_2s": stats.returns.get(2, 0.0) if stats else None,
        "ret_5s": stats.returns.get(5, 0.0) if stats else None,
        "volatility": stats.volatility if stats else None,
        "zscore": stats.zscore if stats else None,
        "shock_score": near_miss.shock_score if near_miss is not None else None,
        "ret_score": near_miss.ret_score if near_miss is not None else None,
        "zscore_score": near_miss.zscore_score if near_miss is not None else None,
        "near_miss_tier": near_miss.tier if near_miss is not None else "",
        "scoring_version": scoring_version,
        "book_bid": book.best_bid if book is not None else None,
        "book_ask": book.best_ask if book is not None else None,
        "spread": book.spread if book is not None else None,
        "book_age_ms": max(0, now_ms - book.ts_ms) if book is not None else None,
        "depth_usd": ((book.depth_usd_at_bid(1) or 0.0) + (book.depth_usd_at_ask(1) or 0.0))
                     if book is not None else None,
        "blocker": block_reason or "",
        "decision": "",   # filled only by rows written at decision time (future use)
        "edge": None,
        "ev": None,
        "tier": "",
        "extra": json.dumps({"direction": near_miss.direction if near_miss is not None else None},
                            default=str)[:500],
    }
    return row
