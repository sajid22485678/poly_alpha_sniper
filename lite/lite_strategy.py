"""Deterministic fair-value, edge, timing, and exit decisions for Lite.

The strategy is deliberately small and inspectable.  Paired direct-token books
form the market prior; point-in-time CEX evidence can move that prior only by a
hard bound.  Entries use verified five-share taker sweeps.  Maker behavior is
observational only because public book data cannot prove that a hypothetical
resting order filled.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

from .lite_book import LiteBookQuote, LiteBookSweep
from .lite_config import FIXED_SHARES
from .lite_risk import sweep_taker_fee


MODEL_VERSION = "paired_book_fair_value_v1"
DIRECTION_OUTPUTS = {
    "BUY_YES", "BUY_NO", "BRIEF_CONFIRMATION_WAIT",
    "NO_TRADE_DATA_INVALID", "NO_TRADE_TRULY_NO_EDGE",
}


def _value(obj: Any, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


@dataclass(frozen=True)
class LeadLagEvidence:
    status: str = "NO_HISTORY"
    valid: bool = True
    cex_move_ts: Optional[int] = None
    poly_book_ts: Optional[int] = None
    lead_lag_ms: Optional[int] = None
    cex_move: Optional[float] = None
    poly_response: Optional[float] = None
    direction_consistent: Optional[bool] = None
    probability_adjustment: float = 0.0
    reason: str = "no_previous_point_in_time_observation"


@dataclass(frozen=True)
class DirectionDecision:
    # Legacy fields remain first for compatibility with small test fixtures.
    output: str
    side: Optional[str]
    yes_score: float
    no_score: float
    score_difference: float
    confidence: float
    reason: str
    direction_score: float = 0.0
    return_10s: Optional[float] = None
    return_30s: Optional[float] = None
    return_60s: Optional[float] = None
    tick_return: Optional[float] = None
    volatility: Optional[float] = None
    evidenced_momentum: Optional[float] = None
    return_5s: Optional[float] = None
    acceleration: Optional[float] = None
    window_return: Optional[float] = None
    market_probability_yes: float = 0.5
    fair_probability_yes: float = 0.5
    fair_probability_no: float = 0.5
    executable_yes_price: Optional[float] = None
    executable_no_price: Optional[float] = None
    estimated_yes_fee: float = 0.0
    estimated_no_fee: float = 0.0
    execution_buffer_yes: float = 0.0
    execution_buffer_no: float = 0.0
    uncertainty_buffer: float = 0.0
    net_edge_yes: Optional[float] = None
    net_edge_no: Optional[float] = None
    selected_net_edge: Optional[float] = None
    calibration_bucket: str = "50-60"
    edge_bucket: str = "NO_EDGE"
    reliability: float = 0.0
    cex_adjustment: float = 0.0
    model_version: str = MODEL_VERSION
    lead_lag_status: str = "NO_HISTORY"
    cex_move_ts: Optional[int] = None
    poly_book_ts: Optional[int] = None
    lead_lag_ms: Optional[int] = None
    poly_response: Optional[float] = None
    lead_lag_adjustment: float = 0.0


@dataclass(frozen=True)
class EntryTimingDecision:
    action: str
    reason: str
    entry_price: Optional[float] = None
    target_price: Optional[float] = None
    max_chase_price: Optional[float] = None
    deadline_ts: Optional[int] = None
    expected_improvement: float = 0.0
    actual_improvement: float = 0.0
    wait_duration_ms: int = 0
    missed_opportunity: bool = False
    chase_prevented: bool = False
    execution_state: str = ""
    maker_price: Optional[float] = None
    maker_start_ts: Optional[int] = None
    maker_fill_assumed: bool = False


@dataclass(frozen=True)
class LiteDecision:
    accepted: bool
    reject_reason: str
    side: Optional[str] = None
    token_id: str = ""
    shares: float = FIXED_SHARES
    entry_price: Optional[float] = None
    entry_cost: Optional[float] = None
    momentum_pct: Optional[float] = None
    direction_output: str = "NO_TRADE_DATA_INVALID"
    entry_action: str = ""
    direction_score: float = 0.0
    yes_score: float = 0.0
    no_score: float = 0.0
    confidence: float = 0.0
    direction_reason: str = ""
    target_price: Optional[float] = None
    max_chase_price: Optional[float] = None
    deadline_ts: Optional[int] = None
    expected_improvement: float = 0.0
    actual_improvement: float = 0.0
    wait_duration_ms: int = 0
    missed_opportunity: bool = False
    chase_prevented: bool = False
    entry_fee: float = 0.0
    book_evidence: Optional[dict] = None
    market_probability_yes: float = 0.5
    return_5s: Optional[float] = None
    acceleration: Optional[float] = None
    window_return: Optional[float] = None
    reliability: float = 0.0
    cex_adjustment: float = 0.0
    lead_lag_adjustment: float = 0.0
    fair_probability_yes: float = 0.5
    fair_probability_no: float = 0.5
    executable_yes_price: Optional[float] = None
    executable_no_price: Optional[float] = None
    estimated_yes_fee: float = 0.0
    estimated_no_fee: float = 0.0
    execution_buffer_yes: float = 0.0
    execution_buffer_no: float = 0.0
    uncertainty_buffer: float = 0.0
    net_edge_yes: Optional[float] = None
    net_edge_no: Optional[float] = None
    selected_net_edge: Optional[float] = None
    calibration_bucket: str = "50-60"
    edge_bucket: str = "NO_EDGE"
    lead_lag_status: str = "NO_HISTORY"
    cex_move_ts: Optional[int] = None
    poly_book_ts: Optional[int] = None
    lead_lag_ms: Optional[int] = None
    poly_response: Optional[float] = None
    execution_state: str = ""
    maker_price: Optional[float] = None
    maker_deadline_ts: Optional[int] = None
    maker_start_ts: Optional[int] = None
    maker_wait_ms: int = 0
    maker_fill_assumed: bool = False
    maker_fill_model: str = "observational_no_fill_claim"
    pullback_start_ts: Optional[int] = None
    pullback_condition: str = ""
    model_version: str = MODEL_VERSION


@dataclass(frozen=True)
class ExitHoldDecision:
    action: str
    reason: str
    exit_now_value: Optional[float]
    hold_expected_value: Optional[float]
    exit_fair_probability: Optional[float]
    thesis_status: str
    exit_price: Optional[float] = None
    exit_fee: float = 0.0


class LiteStrategy:
    def __init__(self, cfg):
        self.cfg = cfg

    @staticmethod
    def _finite(value) -> Optional[float]:
        try:
            result = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return result if math.isfinite(result) else None

    @staticmethod
    def _calibration_bucket(probability: float) -> str:
        lower = min(90, int(_clamp(probability, 0.0, 1.0) * 10) * 10)
        return f"{lower:02d}-{lower + 10:02d}"

    @staticmethod
    def _edge_bucket(edge: Optional[float]) -> str:
        if edge is None or edge <= 0:
            return "NO_EDGE"
        if edge < 0.01:
            return "0-1pct"
        if edge < 0.03:
            return "1-3pct"
        return "3pct_plus"

    def _quote_reason(self, quote: Optional[LiteBookQuote], *, token_id: str,
                      condition_id: str, now_ms: int) -> str:
        if quote is None:
            return "no_book"
        if str(quote.token_id) != str(token_id):
            return "wrong_token_pairing"
        if (not quote.condition_id or quote.condition_id != str(condition_id)
                or not quote.book_hash or quote.source_ts_ms is None):
            return "wrong_condition_pairing"
        if quote.is_future(now_ms):
            return "future_book"
        if quote.is_stale(now_ms, int(self.cfg.book_max_age_ms)):
            return "stale_book"
        if quote.hash_reused:
            return "reused_book_snapshot"
        if quote.min_order_size is None:
            return "minimum_order_size_missing"
        try:
            minimum = float(quote.min_order_size)
        except (TypeError, ValueError, OverflowError):
            return "minimum_order_size"
        if not math.isfinite(minimum) or minimum <= 0 or FIXED_SHARES < minimum:
            return "minimum_order_size"
        spread = self._finite(quote.spread)
        if spread is None or spread < 0 or spread > float(self.cfg.max_spread):
            return "spread_too_wide" if spread is not None else "no_book"
        if quote.buy_sweep(FIXED_SHARES) is None:
            return "insufficient_five_share_depth"
        return ""

    def _paired_costs(
            self, market, yes_book: Optional[LiteBookQuote],
            no_book: Optional[LiteBookQuote], now_ms: int,
    ) -> tuple[Optional[dict], str]:
        condition_id = str(_value(market, "condition_id", "") or "")
        yes_token = str(_value(market, "yes_token_id", "") or "")
        no_token = str(_value(market, "no_token_id", "") or "")
        if not condition_id or not yes_token or not no_token or yes_token == no_token:
            return None, "invalid_market_identity"
        for quote, token in ((yes_book, yes_token), (no_book, no_token)):
            reason = self._quote_reason(
                quote, token_id=token, condition_id=condition_id, now_ms=now_ms)
            if reason:
                return None, reason
        assert yes_book is not None and no_book is not None
        skew = abs(yes_book.effective_ts_ms() - no_book.effective_ts_ms())
        if skew > int(self.cfg.max_book_pair_skew_ms):
            return None, "paired_book_timestamp_skew"
        yes_sweep = yes_book.buy_sweep(FIXED_SHARES)
        no_sweep = no_book.buy_sweep(FIXED_SHARES)
        assert yes_sweep is not None and no_sweep is not None
        yes_fee = sweep_taker_fee(yes_sweep, float(self.cfg.crypto_taker_fee_rate))
        no_fee = sweep_taker_fee(no_sweep, float(self.cfg.crypto_taker_fee_rate))
        yes_all_in = (float(yes_sweep.notional) + yes_fee) / FIXED_SHARES
        no_all_in = (float(no_sweep.notional) + no_fee) / FIXED_SHARES
        total = yes_all_in + no_all_in
        if (not all(math.isfinite(value) for value in (
                yes_all_in, no_all_in, total)) or total <= 0
                or not 0 < yes_sweep.vwap < 1 or not 0 < no_sweep.vwap < 1):
            return None, "invalid_paired_book_cost"
        market_probability_yes = _clamp(yes_all_in / total, 0.01, 0.99)
        yes_buffer = (float(self.cfg.execution_buffer_base)
                      + float(self.cfg.execution_buffer_spread_fraction)
                      * float(yes_book.spread or 0.0))
        no_buffer = (float(self.cfg.execution_buffer_base)
                     + float(self.cfg.execution_buffer_spread_fraction)
                     * float(no_book.spread or 0.0))
        return {
            "yes_sweep": yes_sweep,
            "no_sweep": no_sweep,
            "yes_fee": yes_fee,
            "no_fee": no_fee,
            "yes_all_in": yes_all_in,
            "no_all_in": no_all_in,
            "market_probability_yes": market_probability_yes,
            "yes_buffer": yes_buffer,
            "no_buffer": no_buffer,
            "poly_book_ts": max(
                yes_book.effective_ts_ms(), no_book.effective_ts_ms()),
        }, ""

    def detect_lead_lag(
            self, *, cex_price: float, cex_move_ts: Optional[int],
            market_probability_yes: float, poly_book_ts: int,
            previous_observation: Optional[dict], now_ms: int,
    ) -> LeadLagEvidence:
        if not previous_observation:
            return LeadLagEvidence(poly_book_ts=int(poly_book_ts))
        previous_price = self._finite(previous_observation.get("cex_price"))
        previous_probability = self._finite(
            previous_observation.get("market_probability_yes"))
        previous_cex_ts = previous_observation.get("cex_ts_ms")
        previous_poly_ts = previous_observation.get("poly_book_ts")
        try:
            move_ts = int(cex_move_ts) if cex_move_ts is not None else None
            previous_cex_ts = int(previous_cex_ts)
            previous_poly_ts = int(previous_poly_ts)
        except (TypeError, ValueError, OverflowError):
            return LeadLagEvidence(
                status="INVALID_HISTORY", valid=False,
                poly_book_ts=int(poly_book_ts), reason="invalid_previous_observation")
        if (previous_price is None or previous_price <= 0
                or previous_probability is None or move_ts is None):
            return LeadLagEvidence(
                status="NO_NEW_MOVE", poly_book_ts=int(poly_book_ts),
                reason="cex_move_not_evidenced")
        if (move_ts <= previous_cex_ts or int(poly_book_ts) < previous_poly_ts
                or move_ts > int(now_ms) or int(poly_book_ts) > int(now_ms)):
            return LeadLagEvidence(
                status="OUT_OF_ORDER", valid=False, cex_move_ts=move_ts,
                poly_book_ts=int(poly_book_ts), reason="future_or_out_of_order_lag_data")
        cex_move = (float(cex_price) - previous_price) / previous_price
        poly_response = float(market_probability_yes) - previous_probability
        if abs(cex_move) < float(self.cfg.lead_lag_min_cex_move):
            return LeadLagEvidence(
                status="MOVE_BELOW_THRESHOLD", cex_move_ts=move_ts,
                poly_book_ts=int(poly_book_ts), cex_move=cex_move,
                poly_response=poly_response, reason="cex_move_below_lag_threshold")
        if int(poly_book_ts) < move_ts:
            return LeadLagEvidence(
                status="BOOK_PREDATES_MOVE", valid=False, cex_move_ts=move_ts,
                poly_book_ts=int(poly_book_ts), cex_move=cex_move,
                poly_response=poly_response, reason="stale_apparent_edge")
        lag_ms = int(poly_book_ts) - move_ts
        if lag_ms > int(self.cfg.lead_lag_max_ms):
            return LeadLagEvidence(
                status="LAG_TOO_OLD", valid=False, cex_move_ts=move_ts,
                poly_book_ts=int(poly_book_ts), lead_lag_ms=lag_ms,
                cex_move=cex_move, poly_response=poly_response,
                reason="stale_lead_lag_signal")
        sign = 1.0 if cex_move > 0 else -1.0
        aligned_response = sign * poly_response
        expected = min(
            float(self.cfg.lead_lag_max_adjustment),
            float(self.cfg.lead_lag_max_adjustment)
            * abs(cex_move) / float(self.cfg.lead_lag_min_cex_move),
        )
        residual = max(0.0, expected - max(0.0, aligned_response))
        consistent = aligned_response >= 0
        adjustment = sign * residual if consistent else 0.0
        return LeadLagEvidence(
            status="LEAD_DETECTED" if adjustment else "POLY_CAUGHT_UP",
            cex_move_ts=move_ts, poly_book_ts=int(poly_book_ts),
            lead_lag_ms=lag_ms, cex_move=cex_move,
            poly_response=poly_response, direction_consistent=consistent,
            probability_adjustment=adjustment,
            reason=("bounded_unanswered_cex_move" if adjustment
                    else "poly_response_not_lagging"),
        )

    def choose_direction(
            self, features: dict, *, cex_price: Optional[float], market=None,
            yes_book: Optional[LiteBookQuote] = None,
            no_book: Optional[LiteBookQuote] = None,
            cex_age_ms: Optional[int] = None,
            previous_observation: Optional[dict] = None,
            now_ms: Optional[int] = None,
    ) -> DirectionDecision:
        """Estimate coherent fair value, fee-net edges, and one output state."""
        cex = self._finite(cex_price)
        if now_ms is None:
            source_now = _value(features, "receipt_ts_ms", None)
            now_ms = int(source_now) if source_now is not None else 0
        try:
            age = int(cex_age_ms)
            receipt_age = int(features.get("receipt_age_ms", age))
        except (TypeError, ValueError, OverflowError):
            age = receipt_age = -1
        if (cex is None or cex <= 0 or age < 0 or receipt_age < 0
                or age > int(self.cfg.cex_max_age_ms)
                or receipt_age > int(self.cfg.cex_max_age_ms)):
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                "stale_or_invalid_cex")
        if market is None:
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                "invalid_market_identity")
        costs, pair_reason = self._paired_costs(
            market, yes_book, no_book, int(now_ms))
        if costs is None:
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                pair_reason)

        returns = features.get("returns", {}) if isinstance(features, dict) else {}
        def feature(name: str, window: Optional[int] = None) -> Optional[float]:
            value = features.get(name) if isinstance(features, dict) else None
            if value is None and window is not None and isinstance(returns, dict):
                value = returns.get(window, returns.get(str(window)))
            return self._finite(value)

        r5 = feature("return_5s", 5)
        r10 = feature("return_10s", 10)
        r30 = feature("return_30s", 30)
        r60 = feature("return_60s", 60)
        tick = feature("tick_return")
        acceleration = feature("acceleration")
        volatility = feature("volatility")
        window_return = feature("window_return")
        weighted = [(r5, 0.35), (r10, 0.30), (r30, 0.20), (r60, 0.15)]
        available = [(value, weight) for value, weight in weighted if value is not None]
        if not available:
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                "missing_evidenced_cex_returns", return_5s=r5,
                return_10s=r10, return_30s=r30, return_60s=r60,
                tick_return=tick, volatility=volatility)
        weighted_return = (
            sum(value * weight for value, weight in available)
            / sum(weight for _, weight in available))
        threshold = float(self.cfg.momentum_min_pct)
        direction_score = weighted_return / threshold
        if acceleration is not None:
            direction_score += 0.15 * acceleration / threshold
        if tick is not None:
            direction_score += 0.10 * tick / threshold
        if window_return is not None:
            direction_score += 0.20 * _clamp(window_return / threshold, -3.0, 3.0)
        if bool(_value(market, "anchor_available", False)):
            anchor = self._finite(_value(market, "price_to_beat", None))
            if anchor is not None and anchor > 0:
                scale = max(threshold, abs(volatility or 0.0))
                anchor_signal = (cex - anchor) / anchor / scale
                direction_score += 0.25 * _clamp(anchor_signal, -3.0, 3.0)
        direction_score = _clamp(direction_score, -6.0, 6.0)

        try:
            seconds_to_close = float(_value(market, "seconds_to_close")(int(now_ms)))
        except Exception:
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                "invalid_time_remaining")
        if not math.isfinite(seconds_to_close) or seconds_to_close <= 0:
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                "expired_market")
        freshness = _clamp(
            1.0 - max(age, receipt_age) / float(self.cfg.cex_max_age_ms),
            0.0, 1.0)
        time_weight = _clamp(1.0 - seconds_to_close / 600.0, 0.5, 1.0)
        completeness = len(available) / 4.0
        reliability = _clamp(
            0.50 + 0.15 * completeness
            + (0.10 if window_return is not None else 0.0)
            + (0.10 if bool(_value(market, "anchor_available", False)) else 0.0),
            0.50, 0.85)
        cex_adjustment = (
            float(self.cfg.fair_value_max_adjustment)
            * math.tanh(direction_score / float(self.cfg.fair_value_signal_scale))
            * freshness * time_weight * reliability)

        lead = self.detect_lead_lag(
            cex_price=cex,
            cex_move_ts=features.get("latest_move_ts_ms"),
            market_probability_yes=float(costs["market_probability_yes"]),
            poly_book_ts=int(costs["poly_book_ts"]),
            previous_observation=previous_observation,
            now_ms=int(now_ms),
        )
        if not lead.valid:
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None,
                float(costs["market_probability_yes"]),
                1.0 - float(costs["market_probability_yes"]), 0.0, 0.0,
                lead.reason, direction_score=direction_score,
                return_5s=r5, return_10s=r10, return_30s=r30,
                return_60s=r60, tick_return=tick, volatility=volatility,
                evidenced_momentum=weighted_return, acceleration=acceleration,
                window_return=window_return,
                market_probability_yes=float(costs["market_probability_yes"]),
                fair_probability_yes=float(costs["market_probability_yes"]),
                fair_probability_no=1.0-float(costs["market_probability_yes"]),
                poly_book_ts=lead.poly_book_ts,
                cex_move_ts=lead.cex_move_ts,
                lead_lag_ms=lead.lead_lag_ms,
                poly_response=lead.poly_response,
                lead_lag_status=lead.status)
        lag_adjustment = lead.probability_adjustment
        # A lead-lag observation may enhance only the already evidenced CEX
        # direction; it can never reverse or create a thesis by itself.
        if cex_adjustment == 0 or lag_adjustment * cex_adjustment < 0:
            lag_adjustment = 0.0
        fair_yes = _clamp(
            float(costs["market_probability_yes"])
            + cex_adjustment + lag_adjustment, 0.01, 0.99)
        fair_no = 1.0 - fair_yes
        uncertainty = (
            float(self.cfg.fair_value_uncertainty_buffer)
            + (1.0 - reliability) * 0.01)
        yes_edge = (
            fair_yes - float(costs["yes_all_in"])
            - float(costs["yes_buffer"]) - uncertainty)
        no_edge = (
            fair_no - float(costs["no_all_in"])
            - float(costs["no_buffer"]) - uncertainty)
        best_side = "BUY_YES" if yes_edge > no_edge else "BUY_NO"
        best_edge = max(yes_edge, no_edge)
        if best_edge <= 0:
            output, side, reason = (
                "NO_TRADE_TRULY_NO_EDGE", None, "no_positive_fee_net_edge")
        elif best_edge < float(self.cfg.min_net_edge) or abs(yes_edge-no_edge) < 1e-9:
            output, side, reason = (
                "BRIEF_CONFIRMATION_WAIT", None, "edge_below_entry_threshold")
        else:
            output, side, reason = best_side, best_side, "strongest_positive_fee_net_edge"
        assert output in DIRECTION_OUTPUTS
        difference = fair_yes - fair_no
        return DirectionDecision(
            output=output, side=side, yes_score=fair_yes, no_score=fair_no,
            score_difference=difference, confidence=abs(difference), reason=reason,
            direction_score=direction_score, return_5s=r5, return_10s=r10,
            return_30s=r30, return_60s=r60, tick_return=tick,
            volatility=volatility, evidenced_momentum=weighted_return,
            acceleration=acceleration, window_return=window_return,
            market_probability_yes=float(costs["market_probability_yes"]),
            fair_probability_yes=fair_yes, fair_probability_no=fair_no,
            executable_yes_price=float(costs["yes_sweep"].vwap),
            executable_no_price=float(costs["no_sweep"].vwap),
            estimated_yes_fee=float(costs["yes_fee"]),
            estimated_no_fee=float(costs["no_fee"]),
            execution_buffer_yes=float(costs["yes_buffer"]),
            execution_buffer_no=float(costs["no_buffer"]),
            uncertainty_buffer=uncertainty, net_edge_yes=yes_edge,
            net_edge_no=no_edge, selected_net_edge=best_edge,
            calibration_bucket=self._calibration_bucket(fair_yes),
            edge_bucket=self._edge_bucket(best_edge), reliability=reliability,
            cex_adjustment=cex_adjustment,
            lead_lag_status=lead.status, cex_move_ts=lead.cex_move_ts,
            poly_book_ts=lead.poly_book_ts, lead_lag_ms=lead.lead_lag_ms,
            poly_response=lead.poly_response, lead_lag_adjustment=lag_adjustment,
        )

    @staticmethod
    def observation(direction: DirectionDecision, *, cex_price: float,
                    features: dict) -> Optional[dict]:
        try:
            cex_ts = int(features.get("provider_ts_ms"))
            poly_ts = int(direction.poly_book_ts)
            price = float(cex_price)
            probability = float(direction.market_probability_yes)
        except (TypeError, ValueError, OverflowError):
            return None
        if not all(math.isfinite(value) for value in (price, probability)):
            return None
        return {
            "cex_price": price, "cex_ts_ms": cex_ts,
            "market_probability_yes": probability, "poly_book_ts": poly_ts,
        }

    def optimize_entry(self, direction: DirectionDecision,
                       book: Optional[LiteBookQuote], market, now_ms: int,
                       *, lock: Optional[dict] = None) -> EntryTimingDecision:
        """Observe a bounded maker opportunity, then conservatively cross."""
        if direction.side not in ("BUY_YES", "BUY_NO"):
            return EntryTimingDecision("SKIP", direction.reason)
        token_id = (str(_value(market, "yes_token_id", ""))
                    if direction.side == "BUY_YES"
                    else str(_value(market, "no_token_id", "")))
        condition_id = str(_value(market, "condition_id", "") or "")
        reason = self._quote_reason(
            book, token_id=token_id, condition_id=condition_id, now_ms=now_ms)
        if reason:
            return EntryTimingDecision("SKIP", reason)
        assert book is not None
        sweep = book.buy_sweep(FIXED_SHARES)
        assert sweep is not None
        fill = float(sweep.vwap)
        selected_edge = (direction.net_edge_yes if direction.side == "BUY_YES"
                         else direction.net_edge_no)
        if selected_edge is None or selected_edge <= 0:
            return EntryTimingDecision("SKIP", "edge_gone", chase_prevented=True,
                                       execution_state="EDGE_GONE")
        try:
            seconds_to_close = float(_value(market, "seconds_to_close")(now_ms))
        except Exception:
            return EntryTimingDecision("SKIP", "invalid_time_remaining")
        if seconds_to_close < float(self.cfg.time_to_close_min_s):
            return EntryTimingDecision("SKIP", "price_window")

        if lock:
            if str(lock.get("side") or "") != direction.side:
                return EntryTimingDecision(
                    "SKIP", "thesis_invalidated_no_reversal",
                    execution_state="EDGE_GONE")
            start = int(lock.get("maker_start_ts")
                        or lock.get("direction_decision_ts") or now_ms)
            deadline = int(lock.get("maker_deadline_ts")
                           or lock.get("deadline_ts") or start)
            maker_price = self._finite(lock.get("maker_price"))
            initial = self._finite(lock.get("initial_ask")) or fill
            max_chase = self._finite(lock.get("max_chase_price")) or initial
            waited = max(0, int(now_ms) - start)
            if now_ms < deadline:
                return EntryTimingDecision(
                    "MAKER_WAIT", "observational_maker_wait", None,
                    maker_price, max_chase, deadline,
                    max(0.0, initial-(maker_price or initial)), 0.0, waited,
                    execution_state="MAKER_WAIT", maker_price=maker_price,
                    maker_start_ts=start, maker_fill_assumed=False)
            if selected_edge < float(self.cfg.min_cross_edge):
                return EntryTimingDecision(
                    "SKIP", "maker_edge_expired", None, maker_price,
                    max_chase, deadline, wait_duration_ms=waited,
                    missed_opportunity=True, chase_prevented=True,
                    execution_state="EDGE_GONE", maker_price=maker_price,
                    maker_start_ts=start, maker_fill_assumed=False)
            if fill > max_chase + 1e-12:
                return EntryTimingDecision(
                    "SKIP", "chase_price_negative_ev", None, maker_price,
                    max_chase, deadline, wait_duration_ms=waited,
                    chase_prevented=True, execution_state="EDGE_GONE",
                    maker_price=maker_price, maker_start_ts=start,
                    maker_fill_assumed=False)
            return EntryTimingDecision(
                "CROSS_SPREAD", "maker_expired_cross_edge_valid", fill,
                maker_price, max_chase, deadline,
                max(0.0, initial-(maker_price or initial)), initial-fill, waited,
                missed_opportunity=True, execution_state="CROSS_SPREAD",
                maker_price=maker_price, maker_start_ts=start,
                maker_fill_assumed=False)

        best_bid = self._finite(book.best_bid)
        maker_price = best_bid if best_bid is not None and best_bid > 0 else None
        improvement = max(0.0, fill-(maker_price or fill))
        enough_time = seconds_to_close > (
            float(self.cfg.time_to_close_min_s)
            + float(self.cfg.maker_wait_s) + float(self.cfg.scan_interval_s))
        selected_fair = (direction.fair_probability_yes
                         if direction.side == "BUY_YES"
                         else direction.fair_probability_no)
        selected_fee = (direction.estimated_yes_fee
                        if direction.side == "BUY_YES"
                        else direction.estimated_no_fee) / FIXED_SHARES
        selected_buffer = (direction.execution_buffer_yes
                           if direction.side == "BUY_YES"
                           else direction.execution_buffer_no)
        economic_cap = (
            selected_fair - selected_fee - selected_buffer
            - direction.uncertainty_buffer - float(self.cfg.min_cross_edge))
        max_chase = min(
            0.999, fill + float(self.cfg.max_chase_worsening), economic_cap)
        if (maker_price is not None and improvement > 0 and enough_time
                and maker_price <= economic_cap + 1e-12):
            deadline = int(now_ms + float(self.cfg.maker_wait_s) * 1000)
            return EntryTimingDecision(
                "MAKER_WAIT", "maker_opportunity_observed", None,
                maker_price, max_chase, deadline, improvement, 0.0, 0,
                execution_state="MAKER_WAIT", maker_price=maker_price,
                maker_start_ts=int(now_ms), maker_fill_assumed=False)
        if selected_edge >= float(self.cfg.min_cross_edge) and fill <= max_chase + 1e-12:
            return EntryTimingDecision(
                "CROSS_SPREAD", "immediate_cross_edge_valid", fill,
                maker_price, max_chase, now_ms, execution_state="CROSS_SPREAD",
                maker_price=maker_price, maker_start_ts=None,
                maker_fill_assumed=False)
        return EntryTimingDecision(
            "SKIP", "edge_below_cross_buffer", None, maker_price,
            max_chase, now_ms, chase_prevented=True,
            execution_state="EDGE_GONE", maker_price=maker_price,
            maker_fill_assumed=False)

    @staticmethod
    def _same_window(position, market) -> bool:
        if str(_value(position, "asset", "")).upper() != str(
                _value(market, "asset", "")).upper():
            return False
        return int(_value(position, "window_close_ts", -1)) == int(
            _value(market, "window_close_s", 0) * 1000)

    def build_entry_decision(
            self, market, direction: DirectionDecision,
            timing: EntryTimingDecision, book: LiteBookQuote, now_ms: int,
    ) -> LiteDecision:
        if timing.action != "CROSS_SPREAD" or timing.entry_price is None:
            raise ValueError("entry decision requires an evidenced taker cross")
        if timing.maker_fill_assumed:
            raise ValueError("Lite cannot assume a maker fill")
        sweep = book.buy_sweep(FIXED_SHARES)
        if sweep is None or abs(float(sweep.vwap)-float(timing.entry_price)) > 1e-9:
            raise ValueError("entry sweep changed before decision binding")
        fee = sweep_taker_fee(sweep, float(self.cfg.crypto_taker_fee_rate))
        evidence = {
            "token_id": book.token_id, "condition_id": book.condition_id,
            "book_ts": book.effective_ts_ms(), "received_ts": book.received_ts_ms,
            "age_ms": book.age_ms(now_ms), "book_hash": book.book_hash,
            "best_bid": book.best_bid, "best_ask": book.best_ask,
            "fill_shares": sweep.shares, "ask_depth_shares": book.total_ask_shares,
            "fill_vwap": sweep.vwap, "worst_price": sweep.worst_price,
            "fill_levels": [list(level) for level in sweep.levels],
            "spread": book.spread, "min_order_size": book.min_order_size,
            "hash_reused": book.hash_reused,
        }
        token_id = (str(_value(market, "yes_token_id", ""))
                    if direction.side == "BUY_YES"
                    else str(_value(market, "no_token_id", "")))
        return LiteDecision(
            accepted=True, reject_reason="opened", side=direction.side,
            token_id=token_id, shares=FIXED_SHARES,
            entry_price=float(timing.entry_price),
            entry_cost=FIXED_SHARES*float(timing.entry_price),
            momentum_pct=direction.evidenced_momentum,
            direction_output=direction.output, entry_action="CROSS_SPREAD",
            direction_score=direction.direction_score,
            yes_score=direction.yes_score, no_score=direction.no_score,
            confidence=direction.confidence, direction_reason=direction.reason,
            target_price=timing.target_price,
            max_chase_price=timing.max_chase_price,
            deadline_ts=timing.deadline_ts,
            expected_improvement=timing.expected_improvement,
            actual_improvement=timing.actual_improvement,
            wait_duration_ms=timing.wait_duration_ms,
            missed_opportunity=timing.missed_opportunity,
            chase_prevented=timing.chase_prevented,
            entry_fee=fee, book_evidence=evidence,
            market_probability_yes=direction.market_probability_yes,
            return_5s=direction.return_5s,
            acceleration=direction.acceleration,
            window_return=direction.window_return,
            reliability=direction.reliability,
            cex_adjustment=direction.cex_adjustment,
            lead_lag_adjustment=direction.lead_lag_adjustment,
            fair_probability_yes=direction.fair_probability_yes,
            fair_probability_no=direction.fair_probability_no,
            executable_yes_price=direction.executable_yes_price,
            executable_no_price=direction.executable_no_price,
            estimated_yes_fee=direction.estimated_yes_fee,
            estimated_no_fee=direction.estimated_no_fee,
            execution_buffer_yes=direction.execution_buffer_yes,
            execution_buffer_no=direction.execution_buffer_no,
            uncertainty_buffer=direction.uncertainty_buffer,
            net_edge_yes=direction.net_edge_yes,
            net_edge_no=direction.net_edge_no,
            selected_net_edge=direction.selected_net_edge,
            calibration_bucket=direction.calibration_bucket,
            edge_bucket=direction.edge_bucket,
            lead_lag_status=direction.lead_lag_status,
            cex_move_ts=direction.cex_move_ts,
            poly_book_ts=direction.poly_book_ts,
            lead_lag_ms=direction.lead_lag_ms,
            poly_response=direction.poly_response,
            execution_state="CROSS_SPREAD",
            maker_price=timing.maker_price,
            maker_deadline_ts=timing.deadline_ts,
            maker_start_ts=timing.maker_start_ts,
            maker_wait_ms=timing.wait_duration_ms,
            maker_fill_assumed=False,
        )

    def decide_exit_or_hold(
            self, trade, direction: DirectionDecision,
            sweep: Optional[LiteBookSweep], exit_fee: float, now_ms: int,
            *, book_reason: str = "",
    ) -> ExitHoldDecision:
        close_ts = int(_value(trade, "window_close_ts", 0) or 0)
        if close_ts <= 0 or int(now_ms) >= close_ts:
            return ExitHoldDecision(
                "HOLD_DATA_INVALID", "post_close_book_forbidden", None, None,
                None, "RESOLUTION_PENDING")
        if direction.output == "NO_TRADE_DATA_INVALID":
            return ExitHoldDecision(
                "HOLD_DATA_INVALID", direction.reason, None, None, None,
                "DATA_INVALID")
        if sweep is None:
            return ExitHoldDecision(
                "HOLD_DATA_INVALID", book_reason or "no_executable_exit", None,
                None, None, "DATA_INVALID")
        if not math.isfinite(float(exit_fee)) or exit_fee < 0:
            return ExitHoldDecision(
                "HOLD_DATA_INVALID", "invalid_exit_fee", None, None, None,
                "DATA_INVALID")
        side = str(_value(trade, "side", ""))
        if side not in ("BUY_YES", "BUY_NO"):
            return ExitHoldDecision(
                "HOLD_DATA_INVALID", "invalid_trade_side", None, None, None,
                "DATA_INVALID")
        owned_probability = (direction.fair_probability_yes
                             if side == "BUY_YES"
                             else direction.fair_probability_no)
        aligned_score = (direction.direction_score
                         if side == "BUY_YES" else -direction.direction_score)
        thesis_invalid = aligned_score <= -float(self.cfg.thesis_invalidation_score)
        thesis_status = "INVALIDATED" if thesis_invalid else "CONTINUING"
        exit_now = float(sweep.notional) - float(exit_fee)
        hold_probability = max(
            0.0, float(owned_probability)-float(self.cfg.exit_hold_uncertainty))
        hold_value = FIXED_SHARES * hold_probability
        margin = float(self.cfg.exit_value_margin_usd)
        dominates = exit_now >= hold_value + margin
        invalidated_and_superior = thesis_invalid and exit_now >= hold_value
        if dominates or invalidated_and_superior:
            return ExitHoldDecision(
                "EXIT_NOW",
                "exit_value_dominates_hold_ev" if dominates
                else "thesis_invalidated_exit_superior",
                exit_now, hold_value, float(owned_probability), thesis_status,
                exit_price=float(sweep.vwap), exit_fee=float(exit_fee))
        return ExitHoldDecision(
            "HOLD", "hold_ev_superior_to_executable_bid", exit_now,
            hold_value, float(owned_probability), thesis_status,
            exit_price=float(sweep.vwap), exit_fee=float(exit_fee))

    def evaluate(
            self, market, yes_book: Optional[LiteBookQuote],
            no_book: Optional[LiteBookQuote], cex_price: Optional[float],
            cex_age_ms: Optional[int], cex_source: str, momentum_values,
            now_ms: int, open_positions, *, window_lock: Optional[dict] = None,
            direction_decision: Optional[DirectionDecision] = None,
            previous_observation: Optional[dict] = None,
    ) -> LiteDecision:
        """Compatibility wrapper; runtime passes its already-computed decision."""
        del cex_source
        if market is None:
            return LiteDecision(False, "no_market")
        if isinstance(momentum_values, dict) and "returns" in momentum_values:
            features = momentum_values
        elif isinstance(momentum_values, dict):
            features = {
                "returns": momentum_values,
                **{f"return_{window}s": momentum_values.get(
                    window, momentum_values.get(str(window)))
                   for window in (5, 10, 30, 60)},
            }
        else:
            values = list(momentum_values or [])
            windows = list(getattr(
                self.cfg, "momentum_windows_s", (5, 10, 30, 60)))
            mapped = {window: values[min(index, len(values)-1)]
                      for index, window in enumerate(windows) if values}
            features = {"returns": mapped,
                        **{f"return_{window}s": mapped.get(window)
                           for window in (5, 10, 30, 60)}}
        direction = direction_decision or self.choose_direction(
            features, cex_price=cex_price, market=market,
            yes_book=yes_book, no_book=no_book, cex_age_ms=cex_age_ms,
            previous_observation=previous_observation, now_ms=now_ms)
        if direction.side is None:
            reason = {
                "BRIEF_CONFIRMATION_WAIT": "brief_confirmation_wait",
                "NO_TRADE_TRULY_NO_EDGE": "no_net_edge",
                "NO_TRADE_DATA_INVALID": "data_invalid",
            }[direction.output]
            return LiteDecision(
                False, reason, direction_output=direction.output,
                direction_score=direction.direction_score,
                yes_score=direction.yes_score, no_score=direction.no_score,
                confidence=direction.confidence,
                direction_reason=direction.reason,
                fair_probability_yes=direction.fair_probability_yes,
                fair_probability_no=direction.fair_probability_no,
                net_edge_yes=direction.net_edge_yes,
                net_edge_no=direction.net_edge_no)
        rows = (list(open_positions.values()) if isinstance(open_positions, dict)
                else list(open_positions or []))
        same_window = [row for row in rows if self._same_window(row, market)]
        if same_window:
            reason = ("opposite_side_blocked" if any(
                str(_value(row, "side", "")) != direction.side
                for row in same_window) else "duplicate_same_side_blocked")
            return LiteDecision(False, reason, side=direction.side,
                                direction_output=direction.output)
        active = [row for row in rows if str(_value(row, "status", "")) in (
            "OPEN", "EXIT_PENDING", "PENDING_RESOLUTION", "UNRESOLVED_RETRYING",
            "UNRESOLVED_FINAL")]
        if len(active) >= int(self.cfg.max_open_positions):
            return LiteDecision(False, "max_open_positions", side=direction.side,
                                direction_output=direction.output)
        if sum(str(_value(row, "asset", "")).upper()
               == str(_value(market, "asset", "")).upper() for row in active
               ) >= int(self.cfg.max_open_per_asset):
            return LiteDecision(False, "max_open_positions", side=direction.side,
                                direction_output=direction.output)
        book = yes_book if direction.side == "BUY_YES" else no_book
        timing = self.optimize_entry(
            direction, book, market, now_ms, lock=window_lock)
        if timing.action != "CROSS_SPREAD":
            return LiteDecision(
                False, timing.reason, side=direction.side,
                token_id=str(_value(
                    market, "yes_token_id" if direction.side == "BUY_YES"
                    else "no_token_id", "")),
                direction_output=direction.output, entry_action=timing.action,
                direction_score=direction.direction_score,
                yes_score=direction.yes_score, no_score=direction.no_score,
                confidence=direction.confidence,
                direction_reason=direction.reason,
                target_price=timing.target_price,
                max_chase_price=timing.max_chase_price,
                deadline_ts=timing.deadline_ts,
                expected_improvement=timing.expected_improvement,
                actual_improvement=timing.actual_improvement,
                wait_duration_ms=timing.wait_duration_ms,
                missed_opportunity=timing.missed_opportunity,
                chase_prevented=timing.chase_prevented,
                execution_state=timing.execution_state,
                maker_price=timing.maker_price,
                maker_deadline_ts=timing.deadline_ts,
                maker_start_ts=timing.maker_start_ts,
                maker_wait_ms=timing.wait_duration_ms,
                maker_fill_assumed=False)
        assert book is not None
        return self.build_entry_decision(
            market, direction, timing, book, now_ms)
