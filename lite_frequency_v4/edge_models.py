"""Deterministic, auditable Frequency V4 multi-edge ensemble.

The seven families operate on point-in-time evidence only.  Correlated CEX
families are capped as one group, so repeated views of the same move cannot be
silently counted as independent alpha.  Outputs remain explicitly
uncalibrated until a forward calibration sample exists.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import math
from typing import Iterable, Optional

from .contracts import (
    BookState,
    CexFeatures,
    Direction,
    MarketIdentity,
    ModelOutput,
)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _sign(value: float, epsilon: float = 1e-12) -> int:
    return 1 if value > epsilon else -1 if value < -epsilon else 0


def _direction(score: float) -> Direction:
    return Direction.YES if score > 0.0 else Direction.NO if score < 0.0 else Direction.NONE


def _mid(book: Optional[BookState]) -> Optional[float]:
    if book is None or book.best_bid is None or book.best_ask is None:
        return None
    return (book.best_bid + book.best_ask) / 2.0


def _depth(levels: Iterable, limit: int = 5) -> float:
    return sum(float(level.shares) for level in tuple(levels)[:limit])


def _buy_vwap(book: Optional[BookState], shares: float = 5.0) -> Optional[float]:
    if book is None or not book.hydrated or shares <= 0.0:
        return None
    remaining = float(shares)
    notional = 0.0
    for level in book.asks:
        used = min(remaining, float(level.shares))
        notional += used * float(level.price)
        remaining -= used
        if remaining <= 1e-9:
            return notional / shares
    return None


def _base_probability(yes: Optional[BookState], no: Optional[BookState]) -> float:
    yes_mid, no_mid = _mid(yes), _mid(no)
    estimates: list[float] = []
    if yes_mid is not None:
        estimates.append(yes_mid)
    if no_mid is not None:
        estimates.append(1.0 - no_mid)
    return _clamp(sum(estimates) / len(estimates), 0.001, 0.999) if estimates else 0.5


def _rough_edge(probability_yes: float, yes: Optional[BookState], no: Optional[BookState],
                direction: Direction, fee_rate: float = 0.07) -> float:
    if direction is Direction.NONE:
        return 0.0
    price = _buy_vwap(yes if direction is Direction.YES else no)
    if price is None:
        return -1.0
    probability = probability_yes if direction is Direction.YES else 1.0 - probability_yes
    fee_per_share = fee_rate * price * (1.0 - price)
    return probability - price - fee_per_share - 0.013


@dataclass(frozen=True, slots=True)
class BookFeaturePoint:
    provider_ts_ms: int
    receipt_ts_ms: int
    yes_mid: float
    no_mid: float
    yes_bid_depth: float
    yes_ask_depth: float
    no_bid_depth: float
    no_ask_depth: float
    yes_trade_flow: float = 0.0
    no_trade_flow: float = 0.0


@dataclass(frozen=True, slots=True)
class ModelContext:
    market: MarketIdentity
    now_ms: int
    cex: CexFeatures
    yes_book: Optional[BookState]
    no_book: Optional[BookState]
    book_history: tuple[BookFeaturePoint, ...] = ()
    yes_trade_flow: float = 0.0
    no_trade_flow: float = 0.0
    polymarket_response_ms: Optional[int] = None

    @property
    def remaining_ms(self) -> int:
        return int(self.market.window_close_ms) - int(self.now_ms)

    @property
    def elapsed_ms(self) -> int:
        return int(self.now_ms) - int(self.market.window_open_ms)

    @property
    def baseline_probability_yes(self) -> float:
        return _base_probability(self.yes_book, self.no_book)

    @property
    def evidence_age_ms(self) -> int:
        ages = [max(0, int(self.cex.evidence_age_ms))]
        for book in (self.yes_book, self.no_book):
            if book is not None:
                ages.append(max(0, book.age_ms(self.now_ms)))
        return max(ages)


@dataclass(frozen=True, slots=True)
class EnsembleResult:
    regime: str
    fair_probability_yes: float
    reliability: float
    outputs: tuple[ModelOutput, ...]
    model_uncalibrated: bool = True


class DeterministicEdgeEnsemble:
    MODEL_VERSION = "frequency_v4_ensemble_1"

    def __init__(self, *, probability_floor: float = 0.001,
                 probability_ceiling: float = 0.999,
                 max_adjustment: float = 0.15):
        self.floor = float(probability_floor)
        self.ceiling = float(probability_ceiling)
        self.max_adjustment = float(max_adjustment)
        if not 0.0 < self.floor < self.ceiling < 1.0:
            raise ValueError("invalid probability bounds")
        if not 0.0 < self.max_adjustment < 0.5:
            raise ValueError("invalid adjustment cap")

    @staticmethod
    def _invalid(name: str, family: str, ctx: ModelContext, reason: str,
                 group: str) -> ModelOutput:
        return ModelOutput(
            model_name=name,
            family=family,
            direction=Direction.NONE,
            raw_score=0.0,
            estimated_probability_yes=ctx.baseline_probability_yes,
            evidence_age_ms=ctx.evidence_age_ms,
            confidence=0.0,
            reliability=0.0,
            invalidation_reason=reason,
            expected_net_edge=0.0,
            contribution=0.0,
            calibrated=False,
            correlation_group=group,
            evidence_event_ids=tuple(ctx.cex.evidence_event_ids),
        )

    def _output(self, *, name: str, family: str, group: str, score: float,
                confidence: float, reliability: float, ctx: ModelContext,
                reason: str = "") -> ModelOutput:
        bounded = _clamp(score, -1.0, 1.0)
        probability = _clamp(
            ctx.baseline_probability_yes + bounded * self.max_adjustment,
            self.floor,
            self.ceiling,
        )
        direction = _direction(bounded)
        return ModelOutput(
            model_name=name,
            family=family,
            direction=direction,
            raw_score=bounded,
            estimated_probability_yes=probability,
            evidence_age_ms=ctx.evidence_age_ms,
            confidence=_clamp(confidence, 0.0, 1.0),
            reliability=_clamp(reliability, 0.0, 1.0),
            invalidation_reason=reason,
            expected_net_edge=_rough_edge(probability, ctx.yes_book, ctx.no_book, direction),
            contribution=0.0,
            calibrated=False,
            correlation_group=group,
            evidence_event_ids=tuple(ctx.cex.evidence_event_ids),
        )

    def lead_lag_impulse(self, ctx: ModelContext) -> ModelOutput:
        name, family, group = "cex_lead_lag_impulse", "lead_lag", "cex_directional"
        if not ctx.cex.valid:
            return self._invalid(name, family, ctx, ctx.cex.invalidation_reason or "cex_invalid", group)
        short = next((ctx.cex.returns.get(window) for window in (1, 3, 5, 10)
                      if ctx.cex.returns.get(window) is not None), None)
        if short is None or abs(float(short)) < 0.00003:
            return self._invalid(name, family, ctx, "no_evidenced_impulse", group)
        acceleration = float(ctx.cex.acceleration or 0.0)
        volatility = max(float(ctx.cex.volatility or 0.0), 0.00005)
        impulse = float(short) + 0.35 * acceleration
        normalized = math.tanh(impulse / (volatility * 2.5))
        lag_ms = ctx.polymarket_response_ms
        if lag_ms is None:
            lag_weight = 0.55
        elif lag_ms < 0:
            return self._invalid(name, family, ctx, "regressed_response_timestamp", group)
        else:
            lag_weight = _clamp(lag_ms / 2_000.0, 0.15, 1.0)
        score = normalized * lag_weight
        confidence = min(1.0, abs(impulse) / (volatility * 3.0))
        reliability = confidence * max(0.0, 1.0 - ctx.cex.evidence_age_ms / 2_000.0)
        return self._output(name=name, family=family, group=group, score=score,
                            confidence=confidence, reliability=reliability, ctx=ctx)

    def trend_continuation(self, ctx: ModelContext) -> ModelOutput:
        name, family, group = "short_horizon_trend", "trend_continuation", "cex_directional"
        if not ctx.cex.valid:
            return self._invalid(name, family, ctx, ctx.cex.invalidation_reason or "cex_invalid", group)
        values = [float(value) for window in (1, 3, 5, 10, 15, 30)
                  if (value := ctx.cex.returns.get(window)) is not None]
        if len(values) < 3:
            return self._invalid(name, family, ctx, "insufficient_multi_horizon_history", group)
        dominant = _sign(sum(values))
        persistence = sum(1 for value in values if _sign(value) == dominant) / len(values)
        if dominant == 0 or persistence < 0.66:
            return self._invalid(name, family, ctx, "one_tick_or_inconsistent_trend", group)
        volatility = max(float(ctx.cex.volatility or 0.0), 0.00005)
        weighted = sum(value / (index + 1) for index, value in enumerate(values))
        score = math.tanh(weighted / (volatility * 3.0)) * persistence
        confidence = _clamp((persistence - 0.5) * 2.0, 0.0, 1.0)
        reliability = confidence * min(1.0, ctx.cex.sample_count / 12.0)
        return self._output(name=name, family=family, group=group, score=score,
                            confidence=confidence, reliability=reliability, ctx=ctx)

    def liquidity_sweep_reversal(self, ctx: ModelContext) -> ModelOutput:
        name, family, group = "liquidity_sweep_reversal", "sweep_reversal", "reversal"
        history = [point for point in ctx.book_history
                   if point.provider_ts_ms <= ctx.now_ms and point.receipt_ts_ms <= ctx.now_ms]
        if len(history) < 3:
            return self._invalid(name, family, ctx, "insufficient_book_recovery_history", group)
        oldest, middle, latest = history[-3], history[-2], history[-1]
        displacement = middle.yes_mid - oldest.yes_mid
        recovery = latest.yes_mid - middle.yes_mid
        if abs(displacement) < 0.015 or _sign(recovery) != -_sign(displacement):
            return self._invalid(name, family, ctx, "no_confirmed_reversal", group)
        recovered_fraction = abs(recovery) / max(abs(displacement), 1e-9)
        if recovered_fraction < 0.20:
            return self._invalid(name, family, ctx, "recovery_not_confirmed", group)
        cex_tick = float(ctx.cex.tick_return or 0.0)
        reversal_direction = -_sign(displacement)
        if _sign(cex_tick) not in (0, reversal_direction):
            return self._invalid(name, family, ctx, "unresolved_momentum", group)
        depleted = ((middle.yes_ask_depth < oldest.yes_ask_depth * 0.75)
                    if displacement > 0 else
                    (middle.yes_bid_depth < oldest.yes_bid_depth * 0.75))
        replenished = ((latest.yes_bid_depth > middle.yes_bid_depth * 1.10)
                       if reversal_direction > 0 else
                       (latest.yes_ask_depth > middle.yes_ask_depth * 1.10))
        if not (depleted or replenished):
            return self._invalid(name, family, ctx, "no_sweep_or_replenishment_evidence", group)
        score = reversal_direction * min(1.0, recovered_fraction)
        confidence = min(1.0, 0.45 + 0.35 * recovered_fraction + (0.2 if replenished else 0.0))
        return self._output(name=name, family=family, group=group, score=score,
                            confidence=confidence, reliability=confidence * 0.8, ctx=ctx)

    def window_open_displacement(self, ctx: ModelContext) -> ModelOutput:
        name, family, group = "window_open_displacement", "window_open", "cex_directional"
        if not ctx.cex.valid or ctx.cex.window_open_price is None or ctx.cex.window_return is None:
            return self._invalid(name, family, ctx, "window_open_reference_missing", group)
        if ctx.elapsed_ms < 1_000 or ctx.elapsed_ms >= 300_000:
            return self._invalid(name, family, ctx, "elapsed_time_outside_window", group)
        move = float(ctx.cex.window_return)
        volatility = max(float(ctx.cex.volatility or 0.0), 0.00005)
        elapsed_weight = _clamp(ctx.elapsed_ms / 45_000.0, 0.15, 1.0)
        close_decay = _clamp(ctx.remaining_ms / 20_000.0, 0.25, 1.0)
        score = math.tanh(move / (volatility * 4.0)) * elapsed_weight * close_decay
        confidence = min(1.0, abs(move) / (volatility * 5.0))
        return self._output(name=name, family=family, group=group, score=score,
                            confidence=confidence, reliability=confidence * 0.85, ctx=ctx)

    def order_book_microstructure(self, ctx: ModelContext) -> ModelOutput:
        name, family, group = "order_book_microstructure", "microstructure", "book"
        yes, no = ctx.yes_book, ctx.no_book
        if yes is None or no is None or not yes.hydrated or not no.hydrated:
            return self._invalid(name, family, ctx, "paired_book_not_hydrated", group)
        if yes.spread is None or no.spread is None:
            return self._invalid(name, family, ctx, "empty_book_levels", group)
        yes_bid, yes_ask = _depth(yes.bids), _depth(yes.asks)
        no_bid, no_ask = _depth(no.bids), _depth(no.asks)
        if min(yes_bid + yes_ask, no_bid + no_ask) <= 0.0:
            return self._invalid(name, family, ctx, "empty_book_depth", group)
        yes_imbalance = (yes_bid - yes_ask) / (yes_bid + yes_ask)
        no_imbalance = (no_bid - no_ask) / (no_bid + no_ask)
        yes_micro = ((yes.best_ask * yes_bid + yes.best_bid * yes_ask) /
                     (yes_bid + yes_ask))
        no_micro = ((no.best_ask * no_bid + no.best_bid * no_ask) /
                    (no_bid + no_ask))
        paired_micro_yes = (yes_micro + (1.0 - no_micro)) / 2.0
        flow = math.tanh((ctx.yes_trade_flow - ctx.no_trade_flow) / 25.0)
        score = math.tanh((paired_micro_yes - ctx.baseline_probability_yes) / 0.02)
        score = _clamp(0.55 * score + 0.25 * ((yes_imbalance - no_imbalance) / 2.0)
                       + 0.20 * flow, -1.0, 1.0)
        spread_quality = max(0.0, 1.0 - (yes.spread + no.spread) / 0.20)
        confidence = min(1.0, abs(score) * 0.7 + spread_quality * 0.3)
        return self._output(name=name, family=family, group=group, score=score,
                            confidence=confidence, reliability=confidence * spread_quality, ctx=ctx)

    def paired_book_parity(self, ctx: ModelContext) -> ModelOutput:
        name, family, group = "paired_book_fair_value_parity", "paired_parity", "parity"
        yes_price, no_price = _buy_vwap(ctx.yes_book), _buy_vwap(ctx.no_book)
        if yes_price is None or no_price is None:
            return self._invalid(name, family, ctx, "paired_five_share_depth_missing", group)
        complement_yes = 1.0 - no_price
        normalized_yes = _clamp(
            (yes_price + complement_yes) / 2.0, self.floor, self.ceiling)
        # Gross pair discount is symmetric by identity; subtracting the two
        # raw discounts would therefore be identically zero.  Direction comes
        # only from the coherent normalized probability after side-specific
        # executable prices and nonlinear fees are applied.
        yes_fee = 0.07 * yes_price * (1.0 - yes_price)
        no_fee = 0.07 * no_price * (1.0 - no_price)
        yes_net = normalized_yes - yes_price - yes_fee
        no_net = (1.0 - normalized_yes) - no_price - no_fee
        directional_discount = yes_net - no_net
        pair_gap = 1.0 - yes_price - no_price
        score = math.tanh(directional_discount / 0.025)
        confidence = min(
            1.0, abs(directional_discount) / 0.02 + abs(pair_gap) / 0.08)
        output = self._output(name=name, family=family, group=group, score=score,
                              confidence=confidence, reliability=confidence * 0.75, ctx=ctx)
        probability = _clamp(
            0.6 * normalized_yes + 0.4 * output.estimated_probability_yes,
            self.floor,
            self.ceiling,
        )
        return replace(
            output,
            estimated_probability_yes=probability,
            expected_net_edge=_rough_edge(probability, ctx.yes_book, ctx.no_book, output.direction),
        )

    def late_window_dominance(self, ctx: ModelContext) -> ModelOutput:
        name, family, group = "late_window_probability_dominance", "late_dominance", "cex_directional"
        if not 10_000 <= ctx.remaining_ms <= 60_000:
            return self._invalid(name, family, ctx, "not_in_late_window", group)
        if not ctx.cex.valid or ctx.cex.window_return is None or ctx.cex.evidence_age_ms > 750:
            return self._invalid(name, family, ctx, "late_window_evidence_unsafe", group)
        if any(book is None or book.age_ms(ctx.now_ms) > 750 for book in (ctx.yes_book, ctx.no_book)):
            return self._invalid(name, family, ctx, "late_window_book_latency", group)
        move = float(ctx.cex.window_return)
        volatility = max(float(ctx.cex.volatility or 0.0), 0.00005)
        dominance = abs(move) / volatility
        if dominance < 1.25:
            return self._invalid(name, family, ctx, "late_dominance_insufficient", group)
        urgency = _clamp((60_000 - ctx.remaining_ms) / 50_000.0, 0.0, 1.0)
        score = _sign(move) * math.tanh(dominance / 4.0) * (0.5 + 0.5 * urgency)
        confidence = _clamp((dominance - 1.0) / 4.0, 0.0, 0.95)
        reliability = confidence * max(0.0, 1.0 - ctx.cex.evidence_age_ms / 1_000.0)
        return self._output(name=name, family=family, group=group, score=score,
                            confidence=confidence, reliability=reliability, ctx=ctx)

    def evaluate(
        self, ctx: ModelContext, *,
        quarantined_models: Optional[frozenset[str]] = None,
    ) -> EnsembleResult:
        outputs = (
            self.lead_lag_impulse(ctx),
            self.trend_continuation(ctx),
            self.liquidity_sweep_reversal(ctx),
            self.window_open_displacement(ctx),
            self.order_book_microstructure(ctx),
            self.paired_book_parity(ctx),
            self.late_window_dominance(ctx),
        )
        # Fail-closed model-health quarantine: a quarantined model's output is
        # marked invalid so it contributes nothing to the ensemble probability
        # and cannot drive an entry.  ``invalidation_reason`` is the existing
        # validity filter used by every consumer of ``outputs``.
        quarantine = frozenset(quarantined_models or ())
        if quarantine:
            outputs = tuple(
                replace(output, invalidation_reason=(
                    output.invalidation_reason
                    or f"model_health_quarantined:{output.model_name}"
                )) if output.model_name in quarantine else output
                for output in outputs
            )
        volatility = float(ctx.cex.volatility or 0.0)
        widest_spread = max(
            [book.spread or 1.0 for book in (ctx.yes_book, ctx.no_book) if book is not None]
            or [1.0]
        )
        if ctx.remaining_ms <= 60_000:
            regime = "LATE_WINDOW"
        elif widest_spread > 0.08:
            regime = "ILLIQUID"
        elif volatility > 0.0015:
            regime = "HIGH_VOLATILITY"
        else:
            regime = "NORMAL"

        group_caps = {
            "cex_directional": 0.09 if regime == "ILLIQUID" else 0.12,
            "reversal": 0.06 if regime == "HIGH_VOLATILITY" else 0.04,
            "book": 0.03 if regime == "ILLIQUID" else 0.05,
            "parity": 0.04,
        }
        grouped: dict[str, list[ModelOutput]] = {}
        for output in outputs:
            grouped.setdefault(output.correlation_group or output.family, []).append(output)

        contributions: dict[str, float] = {}
        for group, rows in grouped.items():
            valid = [row for row in rows if not row.invalidation_reason and row.reliability > 0.0]
            if not valid:
                continue
            denominator = sum(row.reliability for row in valid)
            score = sum(row.raw_score * row.reliability for row in valid) / denominator
            # A confirmed reversal gates rather than adds another trend view.
            if group == "cex_directional" and any(
                    row.correlation_group == "reversal" and not row.invalidation_reason
                    for row in outputs):
                score *= 0.55
            total = _clamp(score * group_caps.get(group, 0.03),
                           -group_caps.get(group, 0.03), group_caps.get(group, 0.03))
            for row in valid:
                contributions[row.model_name] = total * row.reliability / denominator

        total_adjustment = _clamp(sum(contributions.values()),
                                  -self.max_adjustment, self.max_adjustment)
        probability = _clamp(ctx.baseline_probability_yes + total_adjustment,
                             self.floor, self.ceiling)
        revised: list[ModelOutput] = []
        for output in outputs:
            contribution = contributions.get(output.model_name, 0.0)
            revised.append(replace(
                output,
                contribution=_clamp(contribution, -1.0, 1.0),
                expected_net_edge=_rough_edge(
                    output.estimated_probability_yes,
                    ctx.yes_book,
                    ctx.no_book,
                    output.direction,
                ),
            ))
        active = [row for row in revised if not row.invalidation_reason and row.reliability > 0.0]
        reliability = (sum(row.reliability for row in active) / len(active)) if active else 0.0
        return EnsembleResult(
            regime=regime,
            fair_probability_yes=probability,
            reliability=_clamp(reliability, 0.0, 1.0),
            outputs=tuple(revised),
            model_uncalibrated=True,
        )
