"""Oracle anchor autopsy: per-asset, per-window (current + next) evidence of
exactly WHERE price_to_beat was looked for, what was present, and why it is
missing when it is missing. Pure diagnostics -- never a gate, never fabricates.

Purpose: end the "is it upstream delay or our bug?" ambiguity. For every
BTC/ETH/SOL current and next 5-minute market this records the full field-
presence map (every schema path the extractor supports), the final anchor
value/source, the precise missing reason, hydration/retry state, and whether
the row's window exactly matches the clock. Verified live (2026-07-10):
Polymarket publishes priceToBeat per-market with a delay after each window
opens -- UPSTREAM_NOT_PUBLISHED is expected early in a window and honest.
"""
from __future__ import annotations

import re
from typing import Optional

from poly_alpha_sniper.discovery.market_mapper import extract_oracle_anchor_metadata

WINDOW_S = 300  # 5-minute windows

_SLUG_RE = re.compile(r"^([a-z]+)-updown-5m-(\d{9,11})$")


def _parse_slug(slug: str) -> Optional[tuple[str, int]]:
    """slug 'btc-updown-5m-1783624500' -> ('BTC', window_start_epoch_s)."""
    m = _SLUG_RE.match(str(slug or ""))
    if not m:
        return None
    return m.group(1).upper(), int(m.group(2))


def _json_like(value) -> dict:
    import json
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _field_presence(raw: dict) -> dict:
    """Booleans for every schema path the extractor supports -- so a schema
    change shows up as 'present but unrecognized' instead of a mystery."""
    events = raw.get("events") if isinstance(raw.get("events"), list) else []
    ev0 = events[0] if events and isinstance(events[0], dict) else {}
    ev0_meta = _json_like(ev0.get("eventMetadata"))
    meta_top = _json_like(raw.get("eventMetadata"))
    meta_json = _json_like(raw.get("metadata"))
    return {
        "priceToBeat_present": "priceToBeat" in raw,
        "price_to_beat_present": "price_to_beat" in raw,
        "eventMetadata_present": raw.get("eventMetadata") is not None,
        "eventMetadata_priceToBeat_present": "priceToBeat" in meta_top,
        "eventMetadata_price_to_beat_present": "price_to_beat" in meta_top,
        "events_count": len(events),
        "events0_eventMetadata_present": ev0.get("eventMetadata") is not None,
        "events0_eventMetadata_priceToBeat_present": "priceToBeat" in ev0_meta,
        "events0_eventMetadata_price_to_beat_present": "price_to_beat" in ev0_meta,
        "metadata_json_present": raw.get("metadata") is not None,
        "metadata_json_priceToBeat_present": "priceToBeat" in meta_json,
        "metadata_json_price_to_beat_present": "price_to_beat" in meta_json,
    }


def _window_report(raw: Optional[dict], which: str, asset: str, now_ms: int,
                   retry_counts: dict, last_success: dict) -> dict:
    last = last_success.get(asset)
    base = {
        "asset": asset,
        "current_or_next": which,
        "last_successful_anchor_for_asset": last,
        "last_successful_anchor_age_s": (round((now_ms - last["ts_ms"]) / 1000)
                                         if last else None),
    }
    if raw is None:
        base.update({
            "slug": None, "final_anchor_available": False,
            "final_missing_reason": "NOT_RECORDED",
            "note": f"no {which}-window market row for {asset} in this refresh",
        })
        return base
    price, _src, _url, diag = extract_oracle_anchor_metadata(raw)
    parsed = _parse_slug(raw.get("slug") or "")
    window_start_s = parsed[1] if parsed else None
    now_s = now_ms // 1000
    exact = (window_start_s is not None and
             ((which == "current" and window_start_s <= now_s < window_start_s + WINDOW_S)
              or (which == "next" and window_start_s >= now_s)))
    missing = diag.get("missing_reason") or ""
    if price is None and not exact:
        missing = "EXPIRED_OR_WRONG_WINDOW"
    base.update({
        "slug": raw.get("slug"),
        "event_id": diag.get("event_id"),
        "market_id": str(raw.get("id") or ""),
        "condition_id": str(raw.get("conditionId") or ""),
        "title": str(raw.get("question") or "")[:60],
        "open_time_s": window_start_s,
        "close_time_s": window_start_s + WINDOW_S if window_start_s else None,
        "seconds_since_open": (now_s - window_start_s) if window_start_s else None,
        "seconds_until_close": ((window_start_s + WINDOW_S) - now_s) if window_start_s else None,
        "hydration_attempted": bool(raw.get("_anchor_hydration_attempted", False)),
        "hydration_success": bool(raw.get("_anchor_hydration_success", False)),
        "fields_checked": diag.get("fields_checked", []),
        "field_presence": _field_presence(raw),
        "final_anchor_available": price is not None,
        "final_price_to_beat": price,
        # exact hit path recorded by the extractor itself -- never inferred
        "final_anchor_source_path": diag.get("source_path") or None,
        "final_missing_reason": "" if price is not None else missing,
        "retry_count": retry_counts.get(str(raw.get("id") or raw.get("slug") or ""), 0),
        "exact_window_match": exact,
    })
    return base


def build_anchor_autopsy(merged_raw: list[dict], assets: list[str], now_ms: int,
                         retry_counts: Optional[dict] = None,
                         last_success: Optional[dict] = None) -> dict:
    """Per-asset current+next window anchor autopsy from the post-hydration
    raw discovery rows. Rows that don't parse as {asset}-updown-5m-{start}
    are ignored (not crypto 5-min markets)."""
    retry_counts = retry_counts or {}
    last_success = last_success or {}
    now_s = now_ms // 1000
    by_asset: dict[str, dict[str, dict]] = {a: {} for a in assets}
    for raw in merged_raw:
        if not isinstance(raw, dict):
            continue
        parsed = _parse_slug(raw.get("slug") or "")
        if not parsed or parsed[0] not in by_asset:
            continue
        asset, start_s = parsed
        if start_s <= now_s < start_s + WINDOW_S:
            by_asset[asset]["current"] = raw
        elif start_s >= now_s:
            nxt = by_asset[asset].get("next")
            nxt_parsed = _parse_slug(nxt.get("slug")) if nxt else None
            if nxt is None or (nxt_parsed and start_s < nxt_parsed[1]):
                by_asset[asset]["next"] = raw  # the NEAREST future window

    out = {"generated_ts_ms": now_ms, "assets": {}}
    for asset in assets:
        out["assets"][asset] = {
            "current": _window_report(by_asset[asset].get("current"), "current",
                                      asset, now_ms, retry_counts, last_success),
            "next": _window_report(by_asset[asset].get("next"), "next",
                                   asset, now_ms, retry_counts, last_success),
        }
    return out
