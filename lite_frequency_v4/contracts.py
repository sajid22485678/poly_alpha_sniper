"""Typed, serializable evidence contracts for Frequency V4.

All epoch timestamps use integer milliseconds.  Receipt monotonic timestamps
use ``time.monotonic_ns()`` semantics and are never compared with epoch time.
Protocol ordering and freshness dispositions live in the ingestion layer; the
contracts preserve the original evidence needed to make those decisions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
import math
from typing import Any, Mapping, Optional


FIVE_MINUTES_MS = 300_000


class AnchorStatus(str, Enum):
    ANCHORED = "anchored"
    UNANCHORED = "unanchored"
    FIELD_MISSING = "anchor_field_missing"
    PARSE_FAILED = "anchor_parse_failed"
    NOT_YET_PUBLISHED = "anchor_not_yet_published"


class Direction(str, Enum):
    YES = "YES"
    NO = "NO"
    NONE = "NONE"


class EntrySide(str, Enum):
    BUY_YES = "BUY_YES"
    BUY_NO = "BUY_NO"


def _finite(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be finite")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def _probability(value: Any, name: str) -> float:
    parsed = _finite(value, name)
    if not 0.0 < parsed < 1.0:
        raise ValueError(f"{name} must be strictly between zero and one")
    return parsed


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


class EvidenceContract:
    def to_dict(self) -> dict[str, Any]:
        return _jsonable(self)


@dataclass(frozen=True, slots=True)
class MarketIdentity(EvidenceContract):
    asset: str
    slug: str
    market_id: str
    event_id: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    window_open_ms: int
    window_close_ms: int
    anchor_status: AnchorStatus = AnchorStatus.FIELD_MISSING
    price_to_beat: Optional[float] = None
    active: bool = True
    accepting_orders: bool = True
    closed: bool = False
    archived: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset", str(self.asset).upper())
        if not all(str(value) for value in (
                self.asset, self.slug, self.market_id, self.event_id,
                self.condition_id, self.yes_token_id, self.no_token_id)):
            raise ValueError("market identity fields are required")
        if self.yes_token_id == self.no_token_id:
            raise ValueError("YES and NO token ids must be distinct")
        if isinstance(self.window_open_ms, bool) or isinstance(self.window_close_ms, bool):
            raise ValueError("market window timestamps must be integers")
        if int(self.window_close_ms) - int(self.window_open_ms) != FIVE_MINUTES_MS:
            raise ValueError("market identity must be an exact five-minute window")
        if int(self.window_open_ms) <= 0:
            raise ValueError("market window must be positive")
        status = (self.anchor_status if isinstance(self.anchor_status, AnchorStatus)
                  else AnchorStatus(str(self.anchor_status)))
        object.__setattr__(self, "anchor_status", status)
        if self.price_to_beat is not None:
            anchor = _finite(self.price_to_beat, "price_to_beat")
            if anchor <= 0.0:
                raise ValueError("price_to_beat must be positive")
            object.__setattr__(self, "price_to_beat", anchor)
        if status is AnchorStatus.ANCHORED and self.price_to_beat is None:
            raise ValueError("anchored markets require price_to_beat")
        if status is not AnchorStatus.ANCHORED and self.price_to_beat is not None:
            raise ValueError("non-anchored status cannot carry price_to_beat")
        if any(type(value) is not bool for value in (
                self.active, self.accepting_orders, self.closed, self.archived)):
            raise ValueError("market state fields must be strict booleans")

    @property
    def duration_ms(self) -> int:
        return int(self.window_close_ms) - int(self.window_open_ms)

    @property
    def window_key(self) -> str:
        return f"{self.asset}:{int(self.window_open_ms)}"

    @property
    def anchored(self) -> bool:
        return self.anchor_status is AnchorStatus.ANCHORED

    def token_for_side(self, side: EntrySide | str) -> str:
        parsed = side if isinstance(side, EntrySide) else EntrySide(str(side))
        return self.yes_token_id if parsed is EntrySide.BUY_YES else self.no_token_id


@dataclass(frozen=True, slots=True)
class BookLevel(EvidenceContract):
    price: float
    shares: float

    def __post_init__(self) -> None:
        price = _finite(self.price, "book price")
        shares = _finite(self.shares, "book shares")
        if not 0.0 < price < 1.0 or shares <= 0.0:
            raise ValueError("book level must have price in (0,1) and positive shares")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "shares", shares)


@dataclass(frozen=True, slots=True)
class Sweep(EvidenceContract):
    side: str
    shares: float
    notional: float
    vwap: float
    worst_price: float
    levels: tuple[BookLevel, ...]

    def __post_init__(self) -> None:
        shares = _finite(self.shares, "sweep shares")
        notional = _finite(self.notional, "sweep notional")
        vwap = _finite(self.vwap, "sweep vwap")
        worst = _finite(self.worst_price, "sweep worst_price")
        if shares <= 0.0 or notional <= 0.0 or not 0.0 < vwap < 1.0:
            raise ValueError("invalid sweep economics")
        if not 0.0 < worst < 1.0 or not self.levels:
            raise ValueError("sweep requires consumed levels")
        if abs(sum(level.shares for level in self.levels) - shares) > 1e-9:
            raise ValueError("sweep levels do not fill the requested shares")
        if abs(sum(level.price * level.shares for level in self.levels) - notional) > 1e-9:
            raise ValueError("sweep levels do not match notional")
        object.__setattr__(self, "shares", shares)
        object.__setattr__(self, "notional", notional)
        object.__setattr__(self, "vwap", vwap)
        object.__setattr__(self, "worst_price", worst)


@dataclass(frozen=True, slots=True)
class BookState(EvidenceContract):
    token_id: str
    condition_id: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    provider_ts_ms: int
    receipt_ts_ms: int
    receipt_monotonic_ns: int
    source: str
    event_id: str
    payload_hash: str
    market_id: str = ""
    sequence: Optional[int] = None
    connection_epoch: int = 0
    min_order_size: Optional[float] = None
    tick_size: Optional[float] = None
    neg_risk: Optional[bool] = None
    hydrated: bool = True

    def __post_init__(self) -> None:
        if not self.token_id or not self.condition_id or not self.source:
            raise ValueError("book provenance is incomplete")
        if int(self.provider_ts_ms) <= 0 or int(self.receipt_ts_ms) <= 0:
            raise ValueError("book timestamps must be positive")
        if int(self.provider_ts_ms) > int(self.receipt_ts_ms):
            raise ValueError("future book timestamp is forbidden")
        if int(self.receipt_monotonic_ns) < 0 or int(self.connection_epoch) < 0:
            raise ValueError("book monotonic time/connection epoch is invalid")
        if self.sequence is not None and int(self.sequence) < 0:
            raise ValueError("book sequence is invalid")
        if tuple(self.bids) != tuple(sorted(self.bids, key=lambda item: item.price, reverse=True)):
            raise ValueError("bids must be best-price first")
        if tuple(self.asks) != tuple(sorted(self.asks, key=lambda item: item.price)):
            raise ValueError("asks must be best-price first")
        if self.best_bid is not None and self.best_ask is not None \
                and self.best_bid > self.best_ask:
            raise ValueError("crossed book is invalid")
        if self.min_order_size is not None:
            minimum = _finite(self.min_order_size, "min_order_size")
            if minimum <= 0.0:
                raise ValueError("min_order_size must be positive")
            object.__setattr__(self, "min_order_size", minimum)
        if self.tick_size is not None:
            tick = _finite(self.tick_size, "tick_size")
            if not 0.0 < tick < 1.0:
                raise ValueError("tick_size must be in (0,1)")
            object.__setattr__(self, "tick_size", tick)
        if self.neg_risk is not None and type(self.neg_risk) is not bool:
            raise ValueError("neg_risk must be a strict boolean")

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    def age_ms(self, now_ms: int) -> int:
        return int(now_ms) - int(self.provider_ts_ms)


@dataclass(frozen=True, slots=True)
class SourceEvent(EvidenceContract):
    source: str
    channel: str
    event_type: str
    event_key: str
    payload_hash: str
    provider_ts_ms: int
    receipt_ts_ms: int
    receipt_monotonic_ns: int
    sequence: Optional[int] = None
    connection_epoch: int = 0
    asset: str = ""
    market_id: str = ""
    condition_id: str = ""
    token_id: str = ""
    window_open_ms: Optional[int] = None
    payload_json: str = ""

    def __post_init__(self) -> None:
        if not self.source or not self.channel or not self.event_type or not self.event_key:
            raise ValueError("source-event identity is required")
        if int(self.receipt_ts_ms) <= 0 or int(self.receipt_monotonic_ns) < 0:
            raise ValueError("source-event receipt timestamps are invalid")
        if int(self.provider_ts_ms) < 0:
            raise ValueError("source-event provider timestamp is invalid")
        if self.sequence is not None and int(self.sequence) < 0:
            raise ValueError("source-event sequence is invalid")
        if int(self.connection_epoch) < 0:
            raise ValueError("connection epoch is invalid")
        object.__setattr__(self, "asset", str(self.asset).upper())


@dataclass(frozen=True, slots=True)
class CexObservation(EvidenceContract):
    provider: str
    asset: str
    instrument: str
    price: float
    provider_ts_ms: int
    receipt_ts_ms: int
    receipt_monotonic_ns: int
    event_id: str
    event_type: str = "ticker"
    side: str = ""
    sequence: Optional[int] = None
    connection_epoch: int = 0
    bid: Optional[float] = None
    ask: Optional[float] = None
    size: Optional[float] = None
    unchanged: bool = False
    classification: str = "NEW_TICK"

    def __post_init__(self) -> None:
        if (not self.provider or not self.asset or not self.instrument
                or not self.event_id or not self.event_type):
            raise ValueError("CEX observation identity is required")
        price = _finite(self.price, "CEX price")
        if price <= 0.0:
            raise ValueError("CEX price must be positive")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "asset", str(self.asset).upper())
        if int(self.provider_ts_ms) <= 0 or int(self.receipt_ts_ms) <= 0:
            raise ValueError("CEX timestamps must be positive")
        if int(self.receipt_monotonic_ns) < 0 or int(self.connection_epoch) < 0:
            raise ValueError("CEX monotonic time/connection epoch is invalid")
        for name in ("bid", "ask", "size"):
            value = getattr(self, name)
            if value is None:
                continue
            parsed = _finite(value, name)
            if parsed <= 0.0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, parsed)
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("CEX bid cannot exceed ask")
        normalized_side = str(self.side or "").lower()
        if normalized_side not in ("", "buy", "sell"):
            raise ValueError("CEX side must be buy, sell, or empty")
        object.__setattr__(self, "side", normalized_side)
        if type(self.unchanged) is not bool:
            raise ValueError("unchanged must be a strict boolean")


@dataclass(frozen=True, slots=True)
class CexFeatures(EvidenceContract):
    asset: str
    provider: str
    provider_ts_ms: int
    receipt_ts_ms: int
    latest_move_ts_ms: Optional[int]
    evidence_age_ms: int
    returns: Mapping[int, Optional[float]] = field(default_factory=dict)
    tick_return: Optional[float] = None
    acceleration: Optional[float] = None
    volatility: Optional[float] = None
    window_open_price: Optional[float] = None
    window_return: Optional[float] = None
    sample_count: int = 0
    classification: str = "NO_HISTORY"
    valid: bool = False
    invalidation_reason: str = ""
    evidence_event_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset", str(self.asset).upper())
        if int(self.evidence_age_ms) < 0 or int(self.sample_count) < 0:
            raise ValueError("CEX feature age/count is invalid")
        for value in self.returns.values():
            if value is not None:
                _finite(value, "CEX return")
        for name in ("tick_return", "acceleration", "volatility", "window_open_price", "window_return"):
            value = getattr(self, name)
            if value is not None:
                _finite(value, name)
        if type(self.valid) is not bool:
            raise ValueError("CEX feature valid flag must be boolean")


@dataclass(frozen=True, slots=True)
class ModelOutput(EvidenceContract):
    model_name: str
    family: str
    direction: Direction
    raw_score: float
    estimated_probability_yes: float
    evidence_age_ms: int
    confidence: float
    reliability: float
    invalidation_reason: str
    expected_net_edge: float
    contribution: float
    calibrated: bool = False
    correlation_group: str = ""
    evidence_event_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.model_name or not self.family:
            raise ValueError("model identity is required")
        direction = self.direction if isinstance(self.direction, Direction) else Direction(str(self.direction))
        object.__setattr__(self, "direction", direction)
        score = _finite(self.raw_score, "raw_score")
        contribution = _finite(self.contribution, "contribution")
        if not -1.0 <= score <= 1.0 or not -1.0 <= contribution <= 1.0:
            raise ValueError("model score/contribution must be bounded")
        _probability(self.estimated_probability_yes, "estimated_probability_yes")
        for name in ("confidence", "reliability"):
            parsed = _finite(getattr(self, name), name)
            if not 0.0 <= parsed <= 1.0:
                raise ValueError(f"{name} must be in [0,1]")
        _finite(self.expected_net_edge, "expected_net_edge")
        if int(self.evidence_age_ms) < 0:
            raise ValueError("model evidence age is invalid")
        if type(self.calibrated) is not bool:
            raise ValueError("calibrated must be a strict boolean")


@dataclass(frozen=True, slots=True)
class FairValueSide(EvidenceContract):
    side: EntrySide
    fair_probability: float
    executable_vwap: Optional[float]
    worst_consumed_price: Optional[float]
    spread: Optional[float]
    depth_shares: float
    estimated_fee: float
    execution_buffer: float
    latency_buffer: float
    uncertainty_buffer: float
    net_edge: Optional[float]
    evidence_age_ms: int
    valid: bool
    invalidation_reason: str = ""

    def __post_init__(self) -> None:
        side = self.side if isinstance(self.side, EntrySide) else EntrySide(str(self.side))
        object.__setattr__(self, "side", side)
        _probability(self.fair_probability, "fair_probability")
        for name in ("executable_vwap", "worst_consumed_price", "spread", "net_edge"):
            value = getattr(self, name)
            if value is None:
                continue
            parsed = _finite(value, name)
            if name in ("executable_vwap", "worst_consumed_price") and not 0.0 < parsed < 1.0:
                raise ValueError(f"{name} must be in (0,1)")
            if name == "spread" and parsed < 0.0:
                raise ValueError("spread cannot be negative")
        for name in ("depth_shares", "estimated_fee", "execution_buffer",
                     "latency_buffer", "uncertainty_buffer"):
            if _finite(getattr(self, name), name) < 0.0:
                raise ValueError(f"{name} cannot be negative")
        if int(self.evidence_age_ms) < 0:
            raise ValueError("fair-value evidence age is invalid")
        if type(self.valid) is not bool:
            raise ValueError("fair-value valid flag must be boolean")
        if self.valid and (self.executable_vwap is None or self.net_edge is None):
            raise ValueError("valid fair-value side requires executable price and edge")


@dataclass(frozen=True, slots=True)
class FairValueResult(EvidenceContract):
    calculated_ts_ms: int
    phase: str
    fair_probability_yes: float
    fair_probability_no: float
    yes: FairValueSide
    no: FairValueSide
    selected_side: Optional[EntrySide]
    selected_net_edge: Optional[float]
    coherent: bool
    model_uncalibrated: bool = True
    evidence_event_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        yes = _probability(self.fair_probability_yes, "fair_probability_yes")
        no = _probability(self.fair_probability_no, "fair_probability_no")
        if abs((yes + no) - 1.0) > 1e-9:
            raise ValueError("YES and NO probabilities must be coherent")
        if self.yes.side is not EntrySide.BUY_YES or self.no.side is not EntrySide.BUY_NO:
            raise ValueError("fair-value sides are mislabeled")
        if self.selected_side is not None:
            side = (self.selected_side if isinstance(self.selected_side, EntrySide)
                    else EntrySide(str(self.selected_side)))
            object.__setattr__(self, "selected_side", side)
            selected = self.yes if side is EntrySide.BUY_YES else self.no
            if not selected.valid:
                raise ValueError("selected fair-value side is invalid")
        if self.selected_net_edge is not None:
            _finite(self.selected_net_edge, "selected_net_edge")
        if type(self.coherent) is not bool or type(self.model_uncalibrated) is not bool:
            raise ValueError("fair-value flags must be strict booleans")


@dataclass(frozen=True, slots=True)
class CandidateEvaluation(EvidenceContract):
    candidate_id: str
    market: MarketIdentity
    evaluation_ts_ms: int
    evaluation_monotonic_ns: int
    regime: str
    model_outputs: tuple[ModelOutput, ...]
    initial_fair_value: FairValueResult
    final_fair_value: Optional[FairValueResult]
    selected_side: Optional[EntrySide]
    execution_tier: str
    decision: str
    reason: str
    positive_edge: bool
    source_event_ids: tuple[str, ...] = ()
    book_event_ids: tuple[str, ...] = ()
    cex_event_ids: tuple[str, ...] = ()
    maker_start_ts_ms: Optional[int] = None
    maker_deadline_ts_ms: Optional[int] = None
    maker_fill_assumed: bool = False
    chase_rejected: bool = False
    missed_opportunity: bool = False

    def __post_init__(self) -> None:
        if not self.candidate_id or not self.execution_tier or not self.decision or not self.reason:
            raise ValueError("candidate identity/decision fields are required")
        if int(self.evaluation_ts_ms) <= 0 or int(self.evaluation_monotonic_ns) < 0:
            raise ValueError("candidate timestamps are invalid")
        if self.selected_side is not None:
            side = (self.selected_side if isinstance(self.selected_side, EntrySide)
                    else EntrySide(str(self.selected_side)))
            object.__setattr__(self, "selected_side", side)
        if any(type(value) is not bool for value in (
                self.positive_edge, self.maker_fill_assumed,
                self.chase_rejected, self.missed_opportunity)):
            raise ValueError("candidate flags must be strict booleans")
        if self.maker_fill_assumed:
            raise ValueError("Frequency V4 never assumes a maker fill")
        if self.maker_start_ts_ms is not None and self.maker_deadline_ts_ms is not None \
                and int(self.maker_deadline_ts_ms) < int(self.maker_start_ts_ms):
            raise ValueError("maker deadline cannot predate maker start")
