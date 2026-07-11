"""Public, deterministic market discovery for the isolated Lite shadow lane.

Only the exact current five-minute slug is accepted.  A response row is never
trusted merely because it was the first Gamma result: asset, window, market
state, outcome labels, and token ids all have to agree.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

WINDOW_S = 300


@dataclass(frozen=True)
class LiteMarket:
    asset: str
    slug: str
    market_id: str
    event_id: str
    condition_id: str
    yes_token_id: str  # Up/Yes token
    no_token_id: str   # Down/No token
    window_start_s: int
    window_close_s: int
    anchor_available: bool
    price_to_beat: Optional[float]

    def seconds_to_close(self, now_ms: int) -> float:
        return self.window_close_s - now_ms / 1000.0


def current_window_start_s(now_ms: int) -> int:
    return int(now_ms // 1000) // WINDOW_S * WINDOW_S


def current_window_slug(asset: str, now_ms: int) -> str:
    return f"{asset.lower()}-updown-5m-{current_window_start_s(now_ms)}"


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _as_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _event_rows(raw: dict) -> list[dict]:
    events = raw.get("events")
    return [row for row in events if isinstance(row, dict)] if isinstance(events, list) else []


def _event_id(raw: dict) -> str:
    for key in ("eventId", "event_id", "eventID"):
        if raw.get(key):
            return str(raw[key])
    for event in _event_rows(raw):
        for key in ("id", "eventId", "event_id"):
            if event.get(key):
                return str(event[key])
    return ""


def _optional_anchor(raw: dict) -> Optional[float]:
    """Read only recognized numeric metadata fields; never use a URL/CEX."""
    containers = [raw]
    for key in ("eventMetadata", "metadata"):
        meta = _as_dict(raw.get(key))
        if meta:
            containers.append(meta)
    for event in _event_rows(raw):
        containers.append(event)
        for key in ("eventMetadata", "metadata"):
            meta = _as_dict(event.get(key))
            if meta:
                containers.append(meta)
    for container in containers:
        for key in ("priceToBeat", "price_to_beat"):
            try:
                value = float(container.get(key))
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
    return None


def parse_market_row(
    asset: str,
    raw: Optional[dict],
    expected_slug: Optional[str] = None,
) -> tuple[Optional[LiteMarket], str]:
    """Return an exact, unambiguous market or a Lite reject reason."""
    if not isinstance(raw, dict) or not raw.get("id"):
        return None, "no_market"
    slug = str(raw.get("slug") or "")
    prefix = f"{asset.lower()}-updown-5m-"
    if not slug.startswith(prefix) or (expected_slug and slug != expected_slug):
        return None, "no_market"
    try:
        start_s = int(slug[len(prefix):])
    except (TypeError, ValueError):
        return None, "no_market"
    if start_s <= 0 or start_s % WINDOW_S != 0:
        return None, "no_market"
    state = {
        "closed": raw.get("closed"),
        "active": raw.get("active"),
        "archived": raw.get("archived"),
        "acceptingOrders": raw.get("acceptingOrders"),
    }
    if any(value is None for value in state.values()):
        return None, "market_state_invalid"
    if (state["closed"] is not False or state["active"] is not True
            or state["archived"] is not False
            or state["acceptingOrders"] is not True):
        return None, "expired_market"

    tokens = [str(v) for v in _as_list(raw.get("clobTokenIds"))]
    outcomes = [str(v).strip().lower() for v in _as_list(raw.get("outcomes"))]
    if len(tokens) != 2 or len(outcomes) != 2 or any(not token for token in tokens) \
            or tokens[0] == tokens[1]:
        return None, "invalid_token"
    up_indexes = [i for i, value in enumerate(outcomes) if value in ("up", "yes")]
    down_indexes = [i for i, value in enumerate(outcomes) if value in ("down", "no")]
    if len(up_indexes) != 1 or len(down_indexes) != 1 or up_indexes[0] == down_indexes[0]:
        return None, "invalid_token"

    condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
    event_id = _event_id(raw)
    if not condition_id or not event_id:
        return None, "no_market"
    anchor = _optional_anchor(raw)
    return LiteMarket(
        asset=asset.upper(),
        slug=slug,
        market_id=str(raw["id"]),
        event_id=event_id,
        condition_id=condition_id,
        yes_token_id=tokens[up_indexes[0]],
        no_token_id=tokens[down_indexes[0]],
        window_start_s=start_s,
        window_close_s=start_s + WINDOW_S,
        anchor_available=anchor is not None,
        price_to_beat=anchor,
    ), ""


class LiteMarketFinder:
    """Small current-window cache around an injected public Gamma fetcher."""

    def __init__(self, gamma_get_markets, cache_s: float = 2.0):
        self._fetch = gamma_get_markets
        self._cache_s = float(cache_s)
        self._cache: dict[str, tuple[float, LiteMarket]] = {}

    async def current_market(self, asset: str, now_ms: int) -> tuple[Optional[LiteMarket], str]:
        asset = asset.upper()
        slug = current_window_slug(asset, now_ms)
        cached = self._cache.get(asset)
        if cached and cached[1].slug == slug and now_ms / 1000.0 - cached[0] < self._cache_s:
            return cached[1], ""
        try:
            rows = await self._fetch({"slug": slug, "limit": 10})
        except Exception:  # one public metadata failure rejects this scan
            return None, "no_market"
        exact_rows = [
            row for row in (rows or [])
            if isinstance(row, dict) and str(row.get("slug") or "") == slug
        ]
        if len(exact_rows) != 1:
            return None, "ambiguous_market" if exact_rows else "no_market"
        market, reason = parse_market_row(asset, exact_rows[0], expected_slug=slug)
        if market is None:
            return None, reason
        if market.window_start_s != current_window_start_s(now_ms):
            return None, "expired_market"
        if market.seconds_to_close(now_ms) <= 0:
            return None, "expired_market"
        self._cache[asset] = (now_ms / 1000.0, market)
        return market, ""


class LiteGammaClient:
    """Minimal public Gamma client; no auth headers, credentials, or writes."""

    def __init__(self, base_url: str, session_factory=None):
        self.base_url = base_url.rstrip("/")
        self._session_factory = session_factory
        self._session = None

    async def _sess(self):
        if self._session is None or getattr(self._session, "closed", False):
            if self._session_factory is not None:
                self._session = self._session_factory()
            else:
                import aiohttp
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=6),
                    headers={"Accept": "application/json"},
                )
        return self._session

    async def get_markets(self, params: dict) -> list[dict]:
        session = await self._sess()
        async with session.get(f"{self.base_url}/markets", params=params) as response:
            if response.status != 200:
                return []
            data = await response.json()
            return data if isinstance(data, list) else []

    async def get_market(self, market_id: str) -> Optional[dict]:
        """Fetch one exact market, including closed short-lived markets."""
        if not str(market_id or "").isdigit():
            return None
        session = await self._sess()
        async with session.get(f"{self.base_url}/markets/{market_id}") as response:
            if response.status != 200:
                return None
            data = await response.json()
            return data if isinstance(data, dict) else None

    async def get_event(self, event_id: str) -> Optional[dict]:
        """Fetch the stored event so market-to-event identity is provable."""
        if not str(event_id or "").isdigit():
            return None
        session = await self._sess()
        async with session.get(f"{self.base_url}/events/{event_id}") as response:
            if response.status != 200:
                return None
            data = await response.json()
            return data if isinstance(data, dict) else None

    async def close(self) -> None:
        if self._session is not None and not getattr(self._session, "closed", True):
            await self._session.close()
