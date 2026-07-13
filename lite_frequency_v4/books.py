"""Strict direct-token order-book normalization and five-share sweeps."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import time
from typing import Any, Iterable, Optional

from .config import FIXED_SHARES
from .contracts import BookLevel, BookState, MarketIdentity, Sweep


class NoBookReason(str, Enum):
    OK = "ok"
    YES_BOOK_MISSING = "yes_book_missing"
    NO_BOOK_MISSING = "no_book_missing"
    BOTH_BOOKS_MISSING = "both_books_missing"
    EMPTY_LEVELS = "empty_levels"
    HTTP_FAILURE = "http_failure"
    WS_NOT_HYDRATED = "websocket_not_hydrated"
    STALE_SNAPSHOT = "stale_snapshot"
    ROLLOVER_GAP = "rollover_gap"
    WRONG_TOKEN = "wrong_token"
    WRONG_MARKET = "wrong_market"
    INSUFFICIENT_DEPTH = "insufficient_depth"
    INVALID_PAYLOAD = "invalid_payload"
    INVALID_TIMESTAMP = "invalid_timestamp"
    FUTURE_TIMESTAMP = "future_timestamp"
    REGRESSED_TIMESTAMP = "regressed_timestamp"
    DUPLICATE_EVENT = "duplicate_event"
    OUT_OF_ORDER_SEQUENCE = "out_of_order_sequence"
    SEQUENCE_GAP = "sequence_gap"
    CROSSED_BOOK = "crossed_book"
    MINIMUM_ORDER_SIZE = "minimum_order_size"


@dataclass(frozen=True, slots=True)
class BookNormalization:
    book: Optional[BookState]
    reason: NoBookReason
    detail: str = ""
    sequence_gap: bool = False
    duplicate: bool = False

    @property
    def valid(self) -> bool:
        return self.book is not None and self.reason is NoBookReason.OK


@dataclass(frozen=True, slots=True)
class PairedBookValidation:
    valid: bool
    reason: NoBookReason
    yes_book: Optional[BookState]
    no_book: Optional[BookState]
    pair_skew_ms: Optional[int] = None


def _payload(data: Any) -> Optional[dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    nested = data.get("data")
    return nested if isinstance(nested, dict) else data


def _positive_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _integer(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text or any(character not in "0123456789" for character in text):
        return None
    try:
        parsed = int(text)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _parse_levels(raw: Any, *, bids: bool) -> tuple[Optional[tuple[BookLevel, ...]], str]:
    if raw is None:
        return (), ""
    if not isinstance(raw, list):
        return None, "levels_not_a_list"
    aggregated: dict[float, float] = {}
    for item in raw:
        try:
            if isinstance(item, dict):
                price, shares = float(item["price"]), float(item["size"])
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                price, shares = float(item[0]), float(item[1])
            else:
                return None, "level_shape_invalid"
        except (KeyError, TypeError, ValueError, OverflowError):
            return None, "level_value_invalid"
        if (not math.isfinite(price) or not math.isfinite(shares)
                or not 0.0 < price < 1.0 or shares <= 0.0):
            return None, "level_bounds_invalid"
        aggregated[price] = aggregated.get(price, 0.0) + shares
    levels = tuple(
        BookLevel(price, shares)
        for price, shares in sorted(
            aggregated.items(), key=lambda item: item[0], reverse=bids)
    )
    return levels, ""


def _payload_digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_book(
        data: Any, *, expected_token_id: str, expected_condition_id: str,
        receipt_ts_ms: int, receipt_monotonic_ns: Optional[int] = None,
        source: str = "clob_rest", expected_market_id: str = "",
        hydrated: bool = True, connection_epoch: int = 0,
        previous_provider_ts_ms: Optional[int] = None,
        previous_sequence: Optional[int] = None,
        previous_payload_hash: str = "",
        now_ms: Optional[int] = None, max_age_ms: Optional[int] = None,
) -> BookNormalization:
    payload = _payload(data)
    if payload is None:
        return BookNormalization(None, NoBookReason.INVALID_PAYLOAD)
    token = str(expected_token_id or "")
    condition = str(expected_condition_id or "")
    if not token or not condition:
        return BookNormalization(None, NoBookReason.WRONG_MARKET,
                                 "expected identity missing")
    returned_tokens = {
        str(payload.get(name)) for name in
        ("asset_id", "assetId", "token_id", "token", "asset")
        if payload.get(name) is not None and str(payload.get(name))
    }
    if not returned_tokens or returned_tokens != {token}:
        return BookNormalization(None, NoBookReason.WRONG_TOKEN)
    returned_condition = str(
        payload.get("market") or payload.get("condition_id") or
        payload.get("conditionId") or "")
    if returned_condition != condition:
        return BookNormalization(None, NoBookReason.WRONG_MARKET,
                                 "condition_id mismatch")
    returned_market_id = str(payload.get("market_id") or payload.get("marketId") or "")
    if expected_market_id and returned_market_id and returned_market_id != str(expected_market_id):
        return BookNormalization(None, NoBookReason.WRONG_MARKET,
                                 "market_id mismatch")
    if not hydrated:
        return BookNormalization(None, NoBookReason.WS_NOT_HYDRATED)

    provider_ts = _integer(
        payload.get("timestamp") if payload.get("timestamp") is not None
        else payload.get("provider_ts_ms"))
    try:
        receipt_ts = int(receipt_ts_ms)
        monotonic_ns = (time.monotonic_ns() if receipt_monotonic_ns is None
                        else int(receipt_monotonic_ns))
    except (TypeError, ValueError, OverflowError):
        return BookNormalization(None, NoBookReason.INVALID_TIMESTAMP)
    if provider_ts is None or provider_ts <= 0 or receipt_ts <= 0 or monotonic_ns < 0:
        return BookNormalization(None, NoBookReason.INVALID_TIMESTAMP)
    if provider_ts > receipt_ts:
        return BookNormalization(None, NoBookReason.FUTURE_TIMESTAMP)
    if previous_provider_ts_ms is not None and provider_ts < int(previous_provider_ts_ms):
        return BookNormalization(None, NoBookReason.REGRESSED_TIMESTAMP)
    effective_now = receipt_ts if now_ms is None else int(now_ms)
    if provider_ts > effective_now:
        return BookNormalization(None, NoBookReason.FUTURE_TIMESTAMP)
    if max_age_ms is not None and effective_now - provider_ts > int(max_age_ms):
        return BookNormalization(None, NoBookReason.STALE_SNAPSHOT)

    sequence = _integer(payload.get("sequence"))
    if previous_sequence is not None and sequence is not None:
        if sequence <= int(previous_sequence):
            return BookNormalization(
                None, NoBookReason.OUT_OF_ORDER_SEQUENCE,
                duplicate=sequence == int(previous_sequence))
        if sequence > int(previous_sequence) + 1:
            return BookNormalization(
                None, NoBookReason.SEQUENCE_GAP, sequence_gap=True)

    payload_hash = _payload_digest(payload)
    if (previous_payload_hash and payload_hash == previous_payload_hash
            and previous_provider_ts_ms is not None
            and provider_ts == int(previous_provider_ts_ms)):
        return BookNormalization(
            None, NoBookReason.DUPLICATE_EVENT, duplicate=True)

    bids, bid_error = _parse_levels(payload.get("bids"), bids=True)
    asks, ask_error = _parse_levels(payload.get("asks"), bids=False)
    if bids is None or asks is None:
        return BookNormalization(
            None, NoBookReason.INVALID_PAYLOAD, bid_error or ask_error)
    if not bids and not asks:
        return BookNormalization(None, NoBookReason.EMPTY_LEVELS)
    if bids and asks and bids[0].price > asks[0].price:
        return BookNormalization(None, NoBookReason.CROSSED_BOOK)

    raw_minimum = payload.get("min_order_size")
    minimum = _positive_float(raw_minimum)
    if raw_minimum is not None and minimum is None:
        return BookNormalization(None, NoBookReason.MINIMUM_ORDER_SIZE)
    if minimum is not None and minimum > FIXED_SHARES:
        return BookNormalization(None, NoBookReason.MINIMUM_ORDER_SIZE)
    raw_tick = payload.get("tick_size")
    tick_size = _positive_float(raw_tick)
    if raw_tick is not None and (tick_size is None or tick_size >= 1.0):
        return BookNormalization(None, NoBookReason.INVALID_PAYLOAD,
                                 "tick_size invalid")
    raw_neg_risk = payload.get("neg_risk")
    if raw_neg_risk is None:
        neg_risk = None
    elif type(raw_neg_risk) is bool:
        neg_risk = raw_neg_risk
    elif str(raw_neg_risk).strip().lower() in ("true", "false"):
        neg_risk = str(raw_neg_risk).strip().lower() == "true"
    else:
        return BookNormalization(None, NoBookReason.INVALID_PAYLOAD,
                                 "neg_risk invalid")
    upstream_event_id = str(
        payload.get("event_id") or payload.get("hash") or payload_hash)
    try:
        book = BookState(
            token_id=token,
            condition_id=condition,
            market_id=str(expected_market_id or returned_market_id),
            bids=bids,
            asks=asks,
            provider_ts_ms=provider_ts,
            receipt_ts_ms=receipt_ts,
            receipt_monotonic_ns=monotonic_ns,
            source=str(source),
            event_id=upstream_event_id,
            payload_hash=payload_hash,
            sequence=sequence,
            connection_epoch=int(connection_epoch),
            min_order_size=minimum,
            tick_size=tick_size,
            neg_risk=neg_risk,
            hydrated=True,
        )
    except (TypeError, ValueError) as exc:
        return BookNormalization(None, NoBookReason.INVALID_PAYLOAD,
                                 type(exc).__name__)
    return BookNormalization(book, NoBookReason.OK)


def normalize_book_state(*args, **kwargs) -> Optional[BookState]:
    """Compatibility convenience: return only a valid state or ``None``."""
    return normalize_book(*args, **kwargs).book


parse_book = normalize_book


def sweep_levels(levels: Iterable[BookLevel], shares: float, *, side: str) -> Optional[Sweep]:
    try:
        requested = float(shares)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(requested) or requested <= 0.0:
        return None
    remaining = requested
    consumed: list[BookLevel] = []
    notional = 0.0
    for level in levels:
        filled = min(remaining, float(level.shares))
        if filled <= 0.0:
            continue
        consumed.append(BookLevel(level.price, filled))
        notional += float(level.price) * filled
        remaining -= filled
        if remaining <= 1e-12:
            break
    if remaining > 1e-12 or not consumed:
        return None
    return Sweep(
        side=str(side),
        shares=requested,
        notional=notional,
        vwap=notional / requested,
        worst_price=consumed[-1].price,
        levels=tuple(consumed),
    )


def five_share_buy_sweep(book: Optional[BookState]) -> Optional[Sweep]:
    if book is None or (book.min_order_size is not None
                        and book.min_order_size > FIXED_SHARES):
        return None
    return sweep_levels(book.asks, FIXED_SHARES, side="BUY")


def five_share_sell_sweep(book: Optional[BookState]) -> Optional[Sweep]:
    if book is None or (book.min_order_size is not None
                        and book.min_order_size > FIXED_SHARES):
        return None
    return sweep_levels(book.bids, FIXED_SHARES, side="SELL")


exact_five_share_buy_sweep = five_share_buy_sweep
exact_five_share_sell_sweep = five_share_sell_sweep


def classify_book_pair(
        market: MarketIdentity, yes_book: Optional[BookState],
        no_book: Optional[BookState], *, now_ms: int,
        max_age_ms: int, max_pair_skew_ms: int) -> PairedBookValidation:
    if yes_book is None and no_book is None:
        return PairedBookValidation(False, NoBookReason.BOTH_BOOKS_MISSING, None, None)
    if yes_book is None:
        return PairedBookValidation(False, NoBookReason.YES_BOOK_MISSING, None, no_book)
    if no_book is None:
        return PairedBookValidation(False, NoBookReason.NO_BOOK_MISSING, yes_book, None)
    if (yes_book.token_id != market.yes_token_id
            or no_book.token_id != market.no_token_id):
        return PairedBookValidation(False, NoBookReason.WRONG_TOKEN, yes_book, no_book)
    if (yes_book.condition_id != market.condition_id
            or no_book.condition_id != market.condition_id):
        return PairedBookValidation(False, NoBookReason.WRONG_MARKET, yes_book, no_book)
    if not yes_book.hydrated or not no_book.hydrated:
        return PairedBookValidation(False, NoBookReason.WS_NOT_HYDRATED, yes_book, no_book)
    if (yes_book.age_ms(now_ms) < 0 or no_book.age_ms(now_ms) < 0):
        return PairedBookValidation(False, NoBookReason.FUTURE_TIMESTAMP, yes_book, no_book)
    if (yes_book.age_ms(now_ms) > int(max_age_ms)
            or no_book.age_ms(now_ms) > int(max_age_ms)):
        return PairedBookValidation(False, NoBookReason.STALE_SNAPSHOT, yes_book, no_book)
    skew = abs(yes_book.provider_ts_ms - no_book.provider_ts_ms)
    if skew > int(max_pair_skew_ms):
        return PairedBookValidation(False, NoBookReason.STALE_SNAPSHOT,
                                    yes_book, no_book, skew)
    if five_share_buy_sweep(yes_book) is None or five_share_buy_sweep(no_book) is None:
        return PairedBookValidation(False, NoBookReason.INSUFFICIENT_DEPTH,
                                    yes_book, no_book, skew)
    return PairedBookValidation(True, NoBookReason.OK, yes_book, no_book, skew)


def compact_book_evidence(
        book: BookState, sweep: Optional[Sweep] = None,
        *, max_levels: int = 10) -> dict[str, Any]:
    """Compact telemetry while retaining every level consumed by a fill."""
    limit = max(1, int(max_levels))
    evidence = {
        "token_id": book.token_id,
        "condition_id": book.condition_id,
        "market_id": book.market_id,
        "provider_ts_ms": book.provider_ts_ms,
        "receipt_ts_ms": book.receipt_ts_ms,
        "receipt_monotonic_ns": book.receipt_monotonic_ns,
        "source": book.source,
        "event_id": book.event_id,
        "payload_hash": book.payload_hash,
        "sequence": book.sequence,
        "connection_epoch": book.connection_epoch,
        "best_bid": book.best_bid,
        "best_ask": book.best_ask,
        "spread": book.spread,
        "min_order_size": book.min_order_size,
        "tick_size": book.tick_size,
        "neg_risk": book.neg_risk,
        "bids": [level.to_dict() for level in book.bids[:limit]],
        "asks": [level.to_dict() for level in book.asks[:limit]],
    }
    if sweep is not None:
        evidence["sweep"] = sweep.to_dict()
    return evidence


def multi_level_imbalance(book: BookState, levels: int = 5) -> Optional[float]:
    depth = max(1, int(levels))
    bids = sum(level.shares for level in book.bids[:depth])
    asks = sum(level.shares for level in book.asks[:depth])
    total = bids + asks
    return (bids - asks) / total if total > 0.0 else None


def microprice(book: BookState) -> Optional[float]:
    if not book.bids or not book.asks:
        return None
    bid, ask = book.bids[0], book.asks[0]
    total = bid.shares + ask.shares
    if total <= 0.0:
        return None
    # Opposite-side quantity weights the executable top prices.
    return (ask.price * bid.shares + bid.price * ask.shares) / total
