"""Fee-complete paired-book fair value and hard positive-EV gate."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import math
from typing import Optional

from .config import FrequencyV4Config
from .contracts import (
    BookLevel,
    BookState,
    EntrySide,
    FairValueResult,
    FairValueSide,
    MarketIdentity,
    Sweep,
)
from .edge_models import EnsembleResult


FEE_QUANTUM = Decimal("0.00001")
FIXED_SHARES = 5.0


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def exact_sweep(book: BookState, *, buy: bool, shares: float = FIXED_SHARES) -> Optional[Sweep]:
    if float(shares) != FIXED_SHARES or not book.hydrated:
        return None
    levels = book.asks if buy else book.bids
    remaining = float(shares)
    consumed: list[BookLevel] = []
    notional = 0.0
    for level in levels:
        take = min(remaining, float(level.shares))
        if take <= 0.0:
            continue
        consumed.append(BookLevel(float(level.price), take))
        notional += float(level.price) * take
        remaining -= take
        if remaining <= 1e-9:
            return Sweep(
                side="BUY" if buy else "SELL",
                shares=FIXED_SHARES,
                notional=notional,
                vwap=notional / FIXED_SHARES,
                worst_price=consumed[-1].price,
                levels=tuple(consumed),
            )
    return None


def sweep_fee(sweep: Sweep, fee_rate: float = 0.07) -> float:
    rate = Decimal(str(fee_rate))
    if not rate.is_finite() or rate < 0:
        raise ValueError("invalid fee rate")
    total = Decimal("0")
    for level in sweep.levels:
        price = Decimal(str(level.price))
        shares = Decimal(str(level.shares))
        total += shares * rate * price * (Decimal("1") - price)
    return float(total.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP))


def compact_book_evidence(book: BookState, sweep: Optional[Sweep]) -> dict:
    return {
        "token_id": book.token_id,
        "condition_id": book.condition_id,
        "provider_ts_ms": int(book.provider_ts_ms),
        "receipt_ts_ms": int(book.receipt_ts_ms),
        "receipt_monotonic_ns": int(book.receipt_monotonic_ns),
        "source": book.source,
        "event_id": book.event_id,
        "payload_hash": book.payload_hash,
        "connection_epoch": int(book.connection_epoch),
        "hydrated": bool(book.hydrated),
        "best_bid": book.best_bid,
        "best_ask": book.best_ask,
        "spread": book.spread,
        "min_order_size": book.min_order_size,
        "tick_size": book.tick_size,
        "bid_depth_shares": sum(level.shares for level in book.bids),
        "ask_depth_shares": sum(level.shares for level in book.asks),
        "sweep": sweep.to_dict() if sweep is not None else None,
    }


@dataclass(frozen=True, slots=True)
class EconomicCalculation:
    result: FairValueResult
    yes_sweep: Optional[Sweep]
    no_sweep: Optional[Sweep]
    yes_fee_total: Optional[float]
    no_fee_total: Optional[float]
    invalid_reasons: tuple[str, ...]


def _book_reason(book: Optional[BookState], *, identity: MarketIdentity, token_id: str,
                 now_ms: int, cfg: FrequencyV4Config) -> str:
    if book is None:
        return "book_missing"
    if book.token_id != token_id:
        return "wrong_token"
    if book.condition_id != identity.condition_id:
        return "wrong_market"
    if not book.hydrated:
        return "websocket_not_hydrated"
    if book.provider_ts_ms > now_ms or book.receipt_ts_ms > now_ms:
        return "future_book"
    if book.age_ms(now_ms) > cfg.book_max_age_ms:
        return "stale_snapshot"
    if not book.bids or not book.asks:
        return "empty_levels"
    if book.spread is None or book.spread < 0.0 or book.spread > cfg.max_spread:
        return "spread_invalid"
    if book.min_order_size is None:
        return "minimum_order_size_missing"
    if float(book.min_order_size) > FIXED_SHARES:
        return "minimum_order_size"
    return ""


def _side(
    *, side: EntrySide, probability: float, book: Optional[BookState], sweep: Optional[Sweep],
    book_reason: str, ensemble: EnsembleResult, now_ms: int, cfg: FrequencyV4Config,
) -> tuple[FairValueSide, Optional[float]]:
    spread = book.spread if book is not None else None
    age = max(0, int(now_ms) - int(book.provider_ts_ms)) if book is not None else cfg.book_max_age_ms + 1
    depth = sum(level.shares for level in book.asks) if book is not None else 0.0
    if book_reason:
        return FairValueSide(
            side=side,
            fair_probability=probability,
            executable_vwap=None,
            worst_consumed_price=None,
            spread=spread,
            depth_shares=depth,
            estimated_fee=0.0,
            execution_buffer=0.0,
            latency_buffer=0.0,
            uncertainty_buffer=0.0,
            net_edge=None,
            evidence_age_ms=max(0, age),
            valid=False,
            invalidation_reason=book_reason,
        ), None
    if sweep is None:
        return FairValueSide(
            side=side,
            fair_probability=probability,
            executable_vwap=None,
            worst_consumed_price=None,
            spread=spread,
            depth_shares=depth,
            estimated_fee=0.0,
            execution_buffer=0.0,
            latency_buffer=0.0,
            uncertainty_buffer=0.0,
            net_edge=None,
            evidence_age_ms=max(0, age),
            valid=False,
            invalidation_reason="insufficient_five_share_depth",
        ), None
    total_fee = sweep_fee(sweep, cfg.crypto_taker_fee_rate)
    fee_per_share = total_fee / FIXED_SHARES
    best_ask = book.best_ask if book is not None else sweep.vwap
    slippage = max(0.0, sweep.vwap - float(best_ask))
    execution_buffer = cfg.execution_buffer_base + 0.10 * float(spread or 0.0) + 0.50 * slippage
    volatility = max(
        [abs(float(row.raw_score)) * 0.001 for row in ensemble.outputs if not row.invalidation_reason]
        or [0.0]
    )
    latency_buffer = cfg.latency_buffer_base + min(0.02, age / 1_000.0 * max(volatility, 0.00025))
    uncertainty_buffer = cfg.uncertainty_buffer_base + (1.0 - ensemble.reliability) * 0.015
    if ensemble.model_uncalibrated:
        uncertainty_buffer += 0.002
    uncertainty_buffer = min(0.05, uncertainty_buffer)
    net_edge = (
        probability - sweep.vwap - fee_per_share - execution_buffer
        - latency_buffer - uncertainty_buffer
    )
    values = (probability, sweep.vwap, total_fee, execution_buffer,
              latency_buffer, uncertainty_buffer, net_edge)
    if not all(math.isfinite(value) for value in values):
        raise ValueError("non-finite fair-value economics")
    return FairValueSide(
        side=side,
        fair_probability=probability,
        executable_vwap=sweep.vwap,
        worst_consumed_price=sweep.worst_price,
        spread=spread,
        depth_shares=depth,
        estimated_fee=fee_per_share,
        execution_buffer=execution_buffer,
        latency_buffer=latency_buffer,
        uncertainty_buffer=uncertainty_buffer,
        net_edge=net_edge,
        evidence_age_ms=age,
        valid=True,
        invalidation_reason="",
    ), total_fee


def calculate_economics(
    *, identity: MarketIdentity, ensemble: EnsembleResult,
    yes_book: Optional[BookState], no_book: Optional[BookState], now_ms: int,
    cfg: FrequencyV4Config, phase: str = "INITIAL",
) -> EconomicCalculation:
    fair_yes = _clamp(
        ensemble.fair_probability_yes,
        cfg.fair_probability_floor,
        cfg.fair_probability_ceiling,
    )
    fair_no = 1.0 - fair_yes
    # Floating point complement is normalized once to keep exact coherence.
    fair_yes = 1.0 - fair_no

    yes_reason = _book_reason(
        yes_book, identity=identity, token_id=identity.yes_token_id,
        now_ms=now_ms, cfg=cfg,
    )
    no_reason = _book_reason(
        no_book, identity=identity, token_id=identity.no_token_id,
        now_ms=now_ms, cfg=cfg,
    )
    if not yes_reason and not no_reason and yes_book is not None and no_book is not None:
        if abs(yes_book.provider_ts_ms - no_book.provider_ts_ms) > cfg.max_book_pair_skew_ms:
            yes_reason = no_reason = "paired_book_timestamp_skew"
        elif yes_book.connection_epoch != no_book.connection_epoch:
            yes_reason = no_reason = "paired_book_connection_epoch_mismatch"

    yes_sweep = exact_sweep(yes_book, buy=True) if yes_book is not None and not yes_reason else None
    no_sweep = exact_sweep(no_book, buy=True) if no_book is not None and not no_reason else None
    yes_side, yes_fee = _side(
        side=EntrySide.BUY_YES, probability=fair_yes, book=yes_book,
        sweep=yes_sweep, book_reason=yes_reason, ensemble=ensemble,
        now_ms=now_ms, cfg=cfg,
    )
    no_side, no_fee = _side(
        side=EntrySide.BUY_NO, probability=fair_no, book=no_book,
        sweep=no_sweep, book_reason=no_reason, ensemble=ensemble,
        now_ms=now_ms, cfg=cfg,
    )
    valid_sides = [side for side in (yes_side, no_side) if side.valid and side.net_edge is not None]
    selected = max(valid_sides, key=lambda side: float(side.net_edge)) if valid_sides else None
    selected_side = selected.side if selected is not None else None
    selected_edge = float(selected.net_edge) if selected is not None else None
    evidence_ids = tuple(dict.fromkeys(
        event_id for event_id in (
            *(row.evidence_event_ids for row in ensemble.outputs),
            (yes_book.event_id,) if yes_book is not None else (),
            (no_book.event_id,) if no_book is not None else (),
        ) for event_id in event_id if event_id
    ))
    result = FairValueResult(
        calculated_ts_ms=int(now_ms),
        phase=str(phase).upper(),
        fair_probability_yes=fair_yes,
        fair_probability_no=fair_no,
        yes=yes_side,
        no=no_side,
        selected_side=selected_side,
        selected_net_edge=selected_edge,
        coherent=True,
        model_uncalibrated=ensemble.model_uncalibrated,
        evidence_event_ids=evidence_ids,
    )
    reasons = tuple(reason for reason in (yes_reason, no_reason) if reason)
    return EconomicCalculation(result, yes_sweep, no_sweep, yes_fee, no_fee, reasons)


def positive_ev(result: FairValueResult) -> bool:
    return bool(result.selected_side is not None
                and result.selected_net_edge is not None
                and result.selected_net_edge > 0.0)
