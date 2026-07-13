"""Exact-identity official settlement helpers for Frequency V4.

Gamma market/event responses are treated as settlement evidence only after
the immutable market, event, condition, token mapping, and five-minute window
all corroborate.  A resolution-source URL is never outcome evidence.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Optional


POSITIVE_LABELS = {"yes", "up", "above", "higher"}
NEGATIVE_LABELS = {"no", "down", "below", "lower"}
FIXED_SHARES = 5.0


def _value(obj: Any, name: str, default: Any = None) -> Any:
    return obj.get(name, default) if isinstance(obj, dict) else getattr(obj, name, default)


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _first(row: dict[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value is not None and str(value):
            return str(value)
    return ""


def _slug_window(slug: str) -> tuple[Optional[int], Optional[int]]:
    try:
        start_s = int(str(slug).rsplit("-", 1)[1])
    except (IndexError, TypeError, ValueError):
        return None, None
    if start_s <= 0 or start_s % 300:
        return None, None
    return start_s * 1000, (start_s + 300) * 1000


def validate_market_identity(row: Optional[dict[str, Any]], identity: Any) -> tuple[bool, str]:
    if not isinstance(row, dict):
        return False, "no_market_row"
    expected = {
        "market_id": str(_value(identity, "market_id", "") or ""),
        "slug": str(_value(identity, "slug", "") or ""),
        "condition_id": str(_value(identity, "condition_id", "") or ""),
    }
    actual = {
        "market_id": _first(row, "id", "market_id"),
        "slug": _first(row, "slug", "market_slug"),
        "condition_id": _first(row, "conditionId", "condition_id"),
    }
    for field in expected:
        if not expected[field] or actual[field] != expected[field]:
            return False, f"{field}_mismatch"
    actual_open, actual_close = _slug_window(actual["slug"])
    if actual_open != int(_value(identity, "window_open_ms", 0) or 0):
        return False, "window_open_mismatch"
    if actual_close != int(_value(identity, "window_close_ms", 0) or 0):
        return False, "window_close_mismatch"

    labels = [str(item).strip().lower() for item in _as_list(row.get("outcomes"))]
    tokens = [str(item) for item in _as_list(row.get("clobTokenIds"))]
    if len(labels) != 2 or len(tokens) != 2 or len(set(tokens)) != 2:
        return False, "token_mapping_unparseable"
    yes_indexes = [index for index, label in enumerate(labels) if label in POSITIVE_LABELS]
    no_indexes = [index for index, label in enumerate(labels) if label in NEGATIVE_LABELS]
    if len(yes_indexes) != 1 or len(no_indexes) != 1:
        return False, "token_mapping_unparseable"
    if tokens[yes_indexes[0]] != str(_value(identity, "yes_token_id", "") or ""):
        return False, "yes_token_mismatch"
    if tokens[no_indexes[0]] != str(_value(identity, "no_token_id", "") or ""):
        return False, "no_token_mismatch"
    return True, "identity_match"


def event_market_row(event: Optional[dict[str, Any]], identity: Any) -> tuple[Optional[dict[str, Any]], str]:
    if not isinstance(event, dict):
        return None, "no_event_row"
    if _first(event, "id", "eventId", "event_id") != str(_value(identity, "event_id", "") or ""):
        return None, "event_id_mismatch"
    event_slug = _first(event, "slug", "eventSlug")
    if event_slug and event_slug != str(_value(identity, "slug", "") or ""):
        return None, "event_slug_mismatch"
    markets = event.get("markets")
    if not isinstance(markets, list):
        return None, "event_markets_unparseable"
    last_reason = "event_market_missing"
    for candidate in markets:
        valid, reason = validate_market_identity(candidate if isinstance(candidate, dict) else None, identity)
        if valid:
            return candidate, "identity_match"
        last_reason = reason
    return None, last_reason


def official_outcome(row: Optional[dict[str, Any]], identity: Any) -> tuple[Optional[str], str]:
    valid, reason = validate_market_identity(row, identity)
    if not valid:
        return None, reason
    assert isinstance(row, dict)
    if row.get("closed") is not True:
        return None, "market_not_closed_yet"
    labels = [str(item).strip().lower() for item in _as_list(row.get("outcomes"))]
    raw_prices = _as_list(row.get("outcomePrices"))
    if len(labels) != 2 or len(raw_prices) != 2:
        return None, "outcome_unparseable"
    try:
        prices = [float(raw_prices[0]), float(raw_prices[1])]
    except (TypeError, ValueError, OverflowError):
        return None, "outcome_unparseable"
    if not all(math.isfinite(value) and 0.0 <= value <= 1.0 for value in prices):
        return None, "outcome_unparseable"
    yes_indexes = [index for index, label in enumerate(labels) if label in POSITIVE_LABELS]
    no_indexes = [index for index, label in enumerate(labels) if label in NEGATIVE_LABELS]
    if len(yes_indexes) != 1 or len(no_indexes) != 1:
        return None, "outcome_unparseable"
    yes_price, no_price = prices[yes_indexes[0]], prices[no_indexes[0]]
    if yes_price >= 0.99 and no_price <= 0.01:
        return "YES", "resolved"
    if no_price >= 0.99 and yes_price <= 0.01:
        return "NO", "resolved"
    return None, "outcome_prices_not_degenerate"


def official_pnl(
    *, side: str, entry_price: float, entry_fee: float, winning_outcome: str,
    shares: float = FIXED_SHARES,
) -> tuple[float, float, bool]:
    size = float(shares)
    price = float(entry_price)
    fee = float(entry_fee)
    if (
        size != FIXED_SHARES
        or side not in {"BUY_YES", "BUY_NO"}
        or winning_outcome not in {"YES", "NO"}
        or not all(math.isfinite(value) for value in (size, price, fee))
        or not 0.0 < price < 1.0
        or fee < 0.0
    ):
        raise ValueError("invalid official settlement inputs")
    won = (side == "BUY_YES" and winning_outcome == "YES") or (
        side == "BUY_NO" and winning_outcome == "NO"
    )
    payout_price = 1.0 if won else 0.0
    pnl = size * payout_price - (size * price + fee)
    return round(pnl, 10), payout_price, won


@dataclass(frozen=True)
class ResolutionEvidence:
    verified: bool
    outcome: Optional[str]
    reason: str
    direct_reason: str
    event_reason: str


def corroborated_resolution(
    *, identity: Any, direct_market: Optional[dict[str, Any]], event: Optional[dict[str, Any]],
) -> ResolutionEvidence:
    direct_outcome, direct_reason = official_outcome(direct_market, identity)
    related_market, event_reason = event_market_row(event, identity)
    event_outcome: Optional[str] = None
    if related_market is not None:
        event_outcome, event_outcome_reason = official_outcome(related_market, identity)
        if event_outcome is None:
            event_reason = event_outcome_reason
    if direct_outcome is None:
        return ResolutionEvidence(False, None, direct_reason, direct_reason, event_reason)
    if event_outcome is None:
        return ResolutionEvidence(False, None, event_reason, direct_reason, event_reason)
    if direct_outcome != event_outcome:
        return ResolutionEvidence(False, None, "resolution_conflict", direct_reason, event_reason)
    return ResolutionEvidence(True, direct_outcome, "official_corroborated", direct_reason, event_reason)
