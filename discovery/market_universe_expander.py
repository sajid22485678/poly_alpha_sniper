"""Gamma query plans for the short-duration crypto universe.

Verified against live Gamma behavior (2026-07-08):
- THE reliable filter is the end-date WINDOW: `end_date_min`/`end_date_max`
  (ISO). It returns exactly the live 5m/15m children, e.g.
  "Bitcoin Up or Down - July 8, 6:00AM-6:05AM ET" slug=btc-updown-5m-<unixts>.
- `slug=` is an EXACT match on Gamma (fragment queries return nothing).
- Default listings ordered by endDate surface stale never-closed markets and
  long-dated events — useless for 5-minute discovery.
Each query carries a `_label` (diagnostics); strip it before sending.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

DISCOVERY_WINDOW_MIN = 20  # subscribe up to 20 minutes before expiry


def build_gamma_queries(cfg, now_ms: int) -> list[dict]:
    now = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)
    lo = (now - timedelta(seconds=60)).isoformat()          # clock-skew guard
    hi = (now + timedelta(minutes=DISCOVERY_WINDOW_MIN)).isoformat()
    queries: list[dict] = [
        {"_label": "end_window", "closed": "false", "active": "true",
         "limit": 100, "end_date_min": lo, "end_date_max": hi},
        # second overlapping window: markets created moments ago sometimes lag
        # the first page; a slightly shifted window catches boundary cases.
        {"_label": "end_window_far", "closed": "false", "active": "true",
         "limit": 100,
         "end_date_min": (now + timedelta(minutes=DISCOVERY_WINDOW_MIN)).isoformat(),
         "end_date_max": (now + timedelta(minutes=2 * DISCOVERY_WINDOW_MIN)).isoformat()},
    ]
    return queries


def merge_markets(result_lists: list[list[dict]]) -> list[dict]:
    """Dedupe raw markets by id / conditionId / slug / token ids."""
    seen: set[str] = set()
    merged: list[dict] = []
    for lst in result_lists:
        for raw in lst or []:
            keys = [str(raw.get("id") or ""),
                    str(raw.get("conditionId") or raw.get("condition_id") or ""),
                    str(raw.get("slug") or raw.get("market_slug") or "")]
            token_raw = raw.get("clobTokenIds") or raw.get("tokens") or ""
            keys.append(str(token_raw)[:120])
            key = next((k for k in keys if k), "")
            if not key:
                continue
            if any(k and k in seen for k in keys):
                continue
            for k in keys:
                if k:
                    seen.add(k)
            merged.append(raw)
    return merged
