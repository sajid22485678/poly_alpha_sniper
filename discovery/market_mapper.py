"""Map raw Gamma/CLOB market dicts into validated contracts.MarketInfo.

Handles BOTH payload shapes:
- Gamma /markets: question, slug, conditionId, outcomes + clobTokenIds as JSON
  strings, endDate, orderMinSize (SHARES), orderPriceMinTickSize,
  liquidityNum, acceptingOrders, eventStartTime...
- CLOB /markets|/sampling-markets: question, market_slug, condition_id,
  tokens: [{token_id, outcome}], end_date_iso, minimum_order_size (SHARES),
  minimum_tick_size, accepting_orders...

IMPORTANT SEMANTICS (verified against live payloads 2026-07-08):
- `orderMinSize` / `minimum_order_size` are in SHARES (typically 5), NOT USD.
  The USD cost of the minimum depends on price, so discovery only rejects on
  a conservative floor (min_shares * tick); the ORDER VALIDATOR enforces the
  real share minimum against the actual order.
- The market's OWN endDate always wins; stale parent/series/event dates in
  raw["events"] never expire a live child market.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from poly_alpha_sniper.core.contracts import MarketInfo, MarketType
from poly_alpha_sniper.deterministic_intelligence.token_mapping_validator import (
    _as_list, validate_token_mapping)
from poly_alpha_sniper.discovery.threshold_parser import parse_market_text


def _iso_to_ms(value: str) -> int:
    if not value:
        return 0
    try:
        v = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def _num(raw: dict, *keys, default: float = 0.0) -> float:
    for k in keys:
        v = raw.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return default


_PRICE_TO_BEAT_KEYS = ("priceToBeat", "price_to_beat")
_RESOLUTION_URL_KEYS = (
    "resolutionSource", "resolution_source_url", "resolutionSourceUrl",
    "resolution_source", "resolutionUrl",
)


def _json_obj(value) -> dict:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    text = value.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _valid_price(value) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


def _events(raw: dict) -> list[dict]:
    events = raw.get("events")
    if not isinstance(events, list):
        return []
    return [ev for ev in events if isinstance(ev, dict)]


def _metadata_containers(raw: dict) -> list[tuple[str, dict]]:
    containers: list[tuple[str, dict]] = [("", raw)]
    for key in ("eventMetadata", "metadata"):
        meta = _json_obj(raw.get(key))
        if meta:
            containers.append((key, meta))
    for idx, event in enumerate(_events(raw)):
        prefix = f"events[{idx}]"
        containers.append((prefix, event))
        for key in ("eventMetadata", "metadata"):
            meta = _json_obj(event.get(key))
            if meta:
                containers.append((f"{prefix}.{key}", meta))
    return containers


def _field_name(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


def _extract_resolution_source_url(raw: dict) -> str:
    for prefix, container in _metadata_containers(raw):
        del prefix
        for key in _RESOLUTION_URL_KEYS:
            value = container.get(key)
            if value:
                return str(value)
    return ""


def _event_id(raw: dict) -> str:
    for key in ("eventId", "event_id", "eventID"):
        if raw.get(key):
            return str(raw.get(key))
    for event in _events(raw):
        for key in ("id", "eventId", "event_id"):
            if event.get(key):
                return str(event.get(key))
    return ""


def extract_oracle_anchor_metadata(raw: dict) -> tuple[float | None, str, str, dict]:
    """Extract Polymarket's price-to-beat anchor from known Gamma schema
    variants without ever substituting a URL or CEX price for the anchor.

    Observed active-market payloads can be shallow or hydrated, and metadata
    may arrive as a JSON object or JSON string. Missing anchors are not cached
    here; callers decide retry policy.
    """
    fields_checked: list[str] = []
    price: float | None = None
    for prefix, container in _metadata_containers(raw):
        for key in _PRICE_TO_BEAT_KEYS:
            field = _field_name(prefix, key)
            fields_checked.append(field)
            if key not in container:
                continue
            price = _valid_price(container.get(key))
            if price is not None:
                break
        if price is not None:
            break

    resolution_source_url = _extract_resolution_source_url(raw)
    source = "polymarket_event_metadata" if price is not None else ""
    status = "available" if price is not None else "missing"
    diag = {
        "market_id": str(raw.get("id") or raw.get("slug") or raw.get("conditionId") or ""),
        "event_id": _event_id(raw),
        "slug": str(raw.get("slug") or raw.get("market_slug") or ""),
        "hydration_attempted": bool(raw.get("_anchor_hydration_attempted", False)),
        "hydration_success": bool(raw.get("_anchor_hydration_success", False)),
        "fields_checked": fields_checked,
        "price_to_beat": price,
        "resolution_source_url": resolution_source_url,
        "final_anchor_status": status,
    }
    return price, source, resolution_source_url, diag


def _extract_price_to_beat(raw: dict) -> tuple[float | None, str, str]:
    price, source, resolution_source_url, _diag = extract_oracle_anchor_metadata(raw)
    return price, source, resolution_source_url


def normalize_raw_market(raw: dict) -> dict:
    """Normalize a CLOB-shaped market dict into Gamma-like keys (in-place safe:
    returns a shallow copy when translation is needed)."""
    if "tokens" not in raw or "clobTokenIds" in raw:
        return raw
    tokens = raw.get("tokens") or []
    if not isinstance(tokens, list) or not tokens or not isinstance(tokens[0], dict):
        return raw
    out = dict(raw)
    out.setdefault("clobTokenIds", [str(t.get("token_id") or "") for t in tokens])
    out.setdefault("outcomes", [str(t.get("outcome") or "") for t in tokens])
    out.setdefault("slug", raw.get("market_slug") or "")
    out.setdefault("conditionId", raw.get("condition_id") or "")
    out.setdefault("endDate", raw.get("end_date_iso") or "")
    out.setdefault("orderMinSize", raw.get("minimum_order_size"))
    out.setdefault("orderPriceMinTickSize", raw.get("minimum_tick_size"))
    out.setdefault("acceptingOrders", raw.get("accepting_orders", True))
    out.setdefault("negRisk", raw.get("neg_risk", False))
    out.setdefault("id", raw.get("condition_id") or "")
    return out


def map_raw_market(raw: dict, now_ms: int) -> MarketInfo:
    raw = normalize_raw_market(raw)
    title = str(raw.get("question") or raw.get("title") or "")
    slug = str(raw.get("slug") or "")
    description = str(raw.get("description") or "")[:500]
    market_id = str(raw.get("id") or raw.get("slug") or raw.get("conditionId") or "")

    # slug participates in parsing: "btc-updown-5m-..." identifies the asset
    parsed = parse_market_text(title, f"{slug} {description}")
    mapping = validate_token_mapping(raw, parsed)
    token_ids = _as_list(raw.get("clobTokenIds") or raw.get("clob_token_ids"))
    yes_token = str(token_ids[0]) if len(token_ids) >= 1 else ""
    no_token = str(token_ids[1]) if len(token_ids) >= 2 else ""

    # the MARKET's own end date always wins (never a parent/series/event date)
    expiry_ms = _iso_to_ms(str(raw.get("endDate") or raw.get("endDateIso")
                               or raw.get("end_date_iso") or ""))

    tick = _num(raw, "orderPriceMinTickSize", "minimum_tick_size", default=0.01) or 0.01
    # SHARES-based minimum (verified: orderMinSize=5 means 5 shares)
    min_order_shares = _num(raw, "orderMinSize", "minimum_order_size", default=5.0) or 5.0
    # conservative-permissive USD floor for discovery; the order validator
    # enforces the true share minimum against the live price.
    min_order_usd_floor = round(min_order_shares * tick, 4)

    accepting = bool(raw.get("acceptingOrders", True))
    active = (bool(raw.get("active", True)) and not bool(raw.get("archived", False))
              and accepting)
    closed = bool(raw.get("closed", False))

    reject = ""
    if not parsed.ok:
        reject = parsed.reject_reason
    elif not mapping.ok:
        reject = mapping.reject_reason
    elif not yes_token or not no_token:
        reject = "MISSING_TOKEN_IDS"
    elif expiry_ms <= 0:
        reject = "MISSING_EXPIRY"
    elif not accepting:
        reject = "NOT_ACCEPTING_ORDERS"

    price_to_beat, price_to_beat_source, resolution_source_url, anchor_diag = \
        extract_oracle_anchor_metadata(raw)

    return MarketInfo(
        market_id=market_id,
        condition_id=str(raw.get("conditionId") or raw.get("condition_id") or ""),
        title=title,
        asset=parsed.asset,
        market_type=parsed.market_type if parsed.ok else MarketType.UNKNOWN,
        threshold=parsed.threshold,
        direction_up_means_yes=mapping.direction_up_means_yes,
        yes_token_id=yes_token, no_token_id=no_token,
        expiry_ts_ms=expiry_ms,
        tick_size=tick,
        min_order_size_usd=min_order_usd_floor,
        neg_risk=bool(raw.get("negRisk") or raw.get("neg_risk") or False),
        active=active, closed=closed,
        liquidity_usd=_num(raw, "liquidityNum", "liquidity"),
        volume_24h_usd=_num(raw, "volume24hr", "volume24hrClob", "volume"),
        mapping_confidence=min(parsed.confidence, mapping.confidence) if parsed.ok else 0.0,
        parse_reject_reason=reject,
        price_to_beat=price_to_beat,
        price_to_beat_source=price_to_beat_source,
        resolution_source_url=resolution_source_url,
        raw={
            "slug": slug,
            "min_order_shares": min_order_shares,
            "event_start_ts_ms": _iso_to_ms(str(raw.get("eventStartTime") or "")),
            "best_bid": _num(raw, "bestBid", default=-1.0),
            "best_ask": _num(raw, "bestAsk", default=-1.0),
            "spread": _num(raw, "spread", default=-1.0),
            "accepting_orders": accepting,
            "neg_risk": bool(raw.get("negRisk") or False),
            "event_id": anchor_diag.get("event_id", ""),
            "oracle_anchor_diagnostics": anchor_diag,
        },
    )
