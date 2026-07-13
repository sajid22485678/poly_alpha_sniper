"""Exact, generic discovery of five-minute crypto Up/Down markets.

Required assets receive direct current/next slug queries.  A separate broad
``end_date_min`` query is what permits newly listed crypto assets to join the
universe without a code change.  Fifteen-minute and other durations are
reported but never admitted to Frequency V4 execution statistics.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
import re
from typing import Any, Awaitable, Callable, Iterable, Optional

from .config import FrequencyV4Config
from .contracts import AnchorStatus, FIVE_MINUTES_MS, MarketIdentity


EXACT_SLUG = re.compile(
    r"^(?P<asset>[a-z0-9]+)-updown-5m-(?P<start>[1-9][0-9]{8,})$",
    re.IGNORECASE,
)
DURATION_SLUG = re.compile(
    r"^(?P<asset>[a-z0-9]+)-updown-(?P<minutes>[1-9][0-9]*)m-"
    r"(?P<start>[1-9][0-9]{8,})$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class DiscoveryRequest:
    name: str
    params: dict[str, Any]
    required_asset: str = ""
    window_open_ms: Optional[int] = None
    broad: bool = False


@dataclass(frozen=True, slots=True)
class AnchorEvidence:
    status: AnchorStatus
    price_to_beat: Optional[float]
    field_path: str = ""


@dataclass(frozen=True, slots=True)
class MarketParseResult:
    market: Optional[MarketIdentity]
    reason: str
    slug: str = ""
    duration_seconds: Optional[int] = None

    @property
    def eligible(self) -> bool:
        return self.market is not None and not self.reason


@dataclass(frozen=True, slots=True)
class DiscoveryBatch:
    generated_ts_ms: int
    eligible_markets: tuple[MarketIdentity, ...] = ()
    rejected: tuple[MarketParseResult, ...] = ()
    ignored_duration_counts: dict[str, int] = field(default_factory=dict)
    request_count: int = 0
    row_count: int = 0

    @property
    def assets(self) -> tuple[str, ...]:
        return tuple(sorted({market.asset for market in self.eligible_markets}))


def window_open_ms(now_ms: int, *, offset_windows: int = 0) -> int:
    current = int(now_ms) // FIVE_MINUTES_MS * FIVE_MINUTES_MS
    return current + int(offset_windows) * FIVE_MINUTES_MS


def exact_slug(asset: str, open_ms: int) -> str:
    return f"{str(asset).lower()}-updown-5m-{int(open_ms)//1000}"


def _iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z")


def build_discovery_requests(
        cfg: FrequencyV4Config, now_ms: int) -> tuple[DiscoveryRequest, ...]:
    requests: list[DiscoveryRequest] = []
    # Query both current and immediately upcoming exact slugs for the minimum
    # guaranteed universe.  Broad discovery remains an independent path.
    for asset in cfg.required_assets:
        for offset in (0, 1):
            opening = window_open_ms(now_ms, offset_windows=offset)
            requests.append(DiscoveryRequest(
                name=f"required_{str(asset).lower()}_{offset}",
                params={"slug": exact_slug(asset, opening), "limit": 10},
                required_asset=str(asset).upper(),
                window_open_ms=opening,
            ))
    requests.append(DiscoveryRequest(
        name="broad_exact_five_minute_crypto",
        params={
            "active": "true",
            "closed": "false",
            "end_date_min": _iso_utc(int(now_ms)),
            "end_date_max": _iso_utc(int(now_ms) + cfg.discovery_lookahead_s * 1000),
            "order": "endDate",
            "ascending": "true",
            "limit": int(cfg.broad_discovery_limit),
        },
        broad=True,
    ))
    return tuple(requests)


# Naming aliases used by schedulers/tests.
build_market_queries = build_discovery_requests


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _timestamp_ms(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
            return None
        integer = int(parsed)
        return integer if integer >= 100_000_000_000 else integer * 1000
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        integer = int(text)
        return integer if integer >= 100_000_000_000 else integer * 1000
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return None
        return int(dt.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _event_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    rows = raw.get("events")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _event_id(raw: dict[str, Any]) -> tuple[str, str]:
    identities: set[str] = set()
    for key in ("eventId", "event_id", "eventID"):
        if raw.get(key) is not None and str(raw.get(key)):
            identities.add(str(raw[key]))
    for event in _event_rows(raw):
        for key in ("id", "eventId", "event_id"):
            if event.get(key) is not None and str(event.get(key)):
                identities.add(str(event[key]))
    if not identities:
        return "", "event_id_missing"
    if len(identities) != 1:
        return "", "event_identity_ambiguous"
    return next(iter(identities)), ""


def _anchor_containers(raw: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = [("market", raw)]
    for key in ("eventMetadata", "metadata"):
        parsed = _as_dict(raw.get(key))
        if parsed:
            out.append((f"market.{key}", parsed))
    for index, event in enumerate(_event_rows(raw)):
        out.append((f"events[{index}]", event))
        for key in ("eventMetadata", "metadata"):
            parsed = _as_dict(event.get(key))
            if parsed:
                out.append((f"events[{index}].{key}", parsed))
    return out


def extract_anchor_evidence(
        raw: dict[str, Any], *, now_ms: int,
        window_open_ms_value: int) -> AnchorEvidence:
    del now_ms, window_open_ms_value  # null publication state is explicit evidence
    containers = _anchor_containers(raw)
    for path, container in containers:
        for key in ("priceToBeat", "price_to_beat"):
            if key not in container:
                continue
            value = container.get(key)
            if value is None or (isinstance(value, str) and not value.strip()):
                return AnchorEvidence(
                    AnchorStatus.NOT_YET_PUBLISHED, None, f"{path}.{key}")
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return AnchorEvidence(
                    AnchorStatus.PARSE_FAILED, None, f"{path}.{key}")
            if not math.isfinite(parsed) or parsed <= 0.0:
                return AnchorEvidence(
                    AnchorStatus.PARSE_FAILED, None, f"{path}.{key}")
            return AnchorEvidence(
                AnchorStatus.ANCHORED, parsed, f"{path}.{key}")

    for path, container in containers:
        explicit = str(container.get("anchorStatus") or
                       container.get("anchor_status") or "").strip().lower()
        if explicit in ("pending", "unpublished", "not_published", "not yet published"):
            return AnchorEvidence(AnchorStatus.NOT_YET_PUBLISHED, None,
                                  f"{path}.anchorStatus")
        if explicit in ("none", "unanchored", "not_required"):
            return AnchorEvidence(AnchorStatus.UNANCHORED, None,
                                  f"{path}.anchorStatus")
        for key in ("anchorRequired", "anchor_required", "hasPriceToBeat"):
            if key in container and container.get(key) is False:
                return AnchorEvidence(AnchorStatus.UNANCHORED, None,
                                      f"{path}.{key}")
    return AnchorEvidence(AnchorStatus.FIELD_MISSING, None, "")


def _duration_seconds(slug: str) -> Optional[int]:
    match = DURATION_SLUG.fullmatch(slug)
    return int(match.group("minutes")) * 60 if match else None


def parse_market_row(
        raw: Any, *, now_ms: int,
        minimum_remaining_s: float = 0.0) -> MarketParseResult:
    if not isinstance(raw, dict):
        return MarketParseResult(None, "market_row_invalid")
    slug = str(raw.get("slug") or "")
    match = EXACT_SLUG.fullmatch(slug)
    duration = _duration_seconds(slug)
    if match is None:
        return MarketParseResult(
            None, "wrong_duration" if duration is not None else "slug_invalid",
            slug=slug, duration_seconds=duration)
    open_ms = int(match.group("start")) * 1000
    if open_ms % FIVE_MINUTES_MS != 0:
        return MarketParseResult(None, "window_alignment_invalid", slug, 300)
    close_ms = open_ms + FIVE_MINUTES_MS
    end_value = None
    for name in ("endDate", "end_date", "endTime", "end_time"):
        if raw.get(name) is not None:
            end_value = raw.get(name)
            break
    parsed_end = _timestamp_ms(end_value)
    if parsed_end is None:
        return MarketParseResult(None, "end_date_missing_or_invalid", slug, 300)
    if parsed_end != close_ms:
        return MarketParseResult(None, "window_close_mismatch", slug, 300)
    if close_ms - int(now_ms) < int(float(minimum_remaining_s) * 1000):
        return MarketParseResult(None, "insufficient_remaining_time", slug, 300)

    state_names = ("active", "closed", "archived", "acceptingOrders")
    if any(type(raw.get(name)) is not bool for name in state_names):
        return MarketParseResult(None, "market_state_invalid", slug, 300)
    if (raw["active"] is not True or raw["closed"] is not False
            or raw["archived"] is not False
            or raw["acceptingOrders"] is not True):
        return MarketParseResult(None, "market_not_executable", slug, 300)

    market_id = str(raw.get("id") or raw.get("market_id") or "")
    condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
    if not market_id:
        return MarketParseResult(None, "market_id_missing", slug, 300)
    event_id, event_reason = _event_id(raw)
    if event_reason:
        return MarketParseResult(None, event_reason, slug, 300)
    if not condition_id:
        return MarketParseResult(None, "condition_id_missing", slug, 300)

    tokens = [str(value) for value in _as_list(raw.get("clobTokenIds"))]
    outcomes = [str(value).strip().lower() for value in _as_list(raw.get("outcomes"))]
    if (len(tokens) != 2 or any(not token for token in tokens)
            or tokens[0] == tokens[1]):
        return MarketParseResult(None, "token_pair_invalid", slug, 300)
    if len(outcomes) != 2:
        return MarketParseResult(None, "outcome_pair_invalid", slug, 300)
    positive = [index for index, value in enumerate(outcomes)
                if value in ("up", "yes")]
    negative = [index for index, value in enumerate(outcomes)
                if value in ("down", "no")]
    if len(positive) != 1 or len(negative) != 1 or positive[0] == negative[0]:
        return MarketParseResult(None, "outcome_pair_invalid", slug, 300)

    anchor = extract_anchor_evidence(
        raw, now_ms=int(now_ms), window_open_ms_value=open_ms)
    try:
        market = MarketIdentity(
            asset=match.group("asset").upper(),
            slug=slug,
            market_id=market_id,
            event_id=event_id,
            condition_id=condition_id,
            yes_token_id=tokens[positive[0]],
            no_token_id=tokens[negative[0]],
            window_open_ms=open_ms,
            window_close_ms=close_ms,
            anchor_status=anchor.status,
            price_to_beat=anchor.price_to_beat,
            active=raw["active"],
            accepting_orders=raw["acceptingOrders"],
            closed=raw["closed"],
            archived=raw["archived"],
        )
    except (TypeError, ValueError):
        return MarketParseResult(None, "market_identity_invalid", slug, 300)
    return MarketParseResult(market, "", slug, 300)


parse_exact_market = parse_market_row


def discover_from_rows(
        rows: Iterable[Any], *, now_ms: int,
        minimum_remaining_s: float = 0.0,
        request_count: int = 0) -> DiscoveryBatch:
    raw_rows = list(rows)
    parsed = [parse_market_row(
        row, now_ms=int(now_ms), minimum_remaining_s=minimum_remaining_s)
        for row in raw_rows]
    rejected = [result for result in parsed if not result.eligible]
    duration_counts: dict[str, int] = {}
    for result in rejected:
        if result.reason == "wrong_duration" and result.duration_seconds is not None:
            key = f"{result.duration_seconds // 60}m"
            duration_counts[key] = duration_counts.get(key, 0) + 1

    # The same market returned by direct and broad requests is one row.  Two
    # distinct market identities claiming one asset/window are ambiguous and
    # all are rejected fail-closed.
    unique_market_ids: dict[tuple[str, str], MarketIdentity] = {}
    for result in parsed:
        if result.market is not None:
            unique_market_ids[(result.market.market_id,
                               result.market.condition_id)] = result.market
    grouped: dict[tuple[str, int], list[MarketIdentity]] = {}
    for market in unique_market_ids.values():
        grouped.setdefault((market.asset, market.window_open_ms), []).append(market)
    eligible: list[MarketIdentity] = []
    for markets in grouped.values():
        if len(markets) == 1:
            eligible.append(markets[0])
        else:
            rejected.extend(MarketParseResult(
                None, "duplicate_market_identity", market.slug, 300)
                for market in markets)
    eligible.sort(key=lambda market: (
        market.window_open_ms, market.asset, market.market_id))
    return DiscoveryBatch(
        generated_ts_ms=int(now_ms),
        eligible_markets=tuple(eligible),
        rejected=tuple(rejected),
        ignored_duration_counts=dict(sorted(duration_counts.items())),
        request_count=int(request_count),
        row_count=len(raw_rows),
    )


class GammaMarketDiscovery:
    """Background discovery around an injected unauthenticated GET function."""

    def __init__(
            self, get_markets: Callable[[dict[str, Any]], Awaitable[list[dict]]],
            cfg: FrequencyV4Config):
        self._get_markets = get_markets
        self.cfg = cfg

    async def discover(self, now_ms: int) -> DiscoveryBatch:
        requests = build_discovery_requests(self.cfg, int(now_ms))
        results = await asyncio.gather(
            *(self._get_markets(request.params) for request in requests),
            return_exceptions=True,
        )
        rows: list[Any] = []
        failures: list[MarketParseResult] = []
        for request, result in zip(requests, results):
            if isinstance(result, BaseException):
                failures.append(MarketParseResult(
                    None, f"discovery_request_failed:{request.name}"))
            elif isinstance(result, list):
                rows.extend(result)
            else:
                failures.append(MarketParseResult(
                    None, f"discovery_response_invalid:{request.name}"))
        batch = discover_from_rows(
            rows, now_ms=int(now_ms),
            minimum_remaining_s=float(self.cfg.minimum_remaining_s),
            request_count=len(requests),
        )
        if not failures:
            return batch
        return DiscoveryBatch(
            generated_ts_ms=batch.generated_ts_ms,
            eligible_markets=batch.eligible_markets,
            rejected=batch.rejected + tuple(failures),
            ignored_duration_counts=batch.ignored_duration_counts,
            request_count=batch.request_count,
            row_count=batch.row_count,
        )
