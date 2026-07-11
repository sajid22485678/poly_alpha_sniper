"""Deterministic single-direction and bounded sniper timing for Lite.

Direction selection and entry-price timing are separate pure decisions.  A
runtime persists the chosen direction before using the timing result, so a
pullback wait or restart can never reverse the asset/window thesis.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

from .lite_book import LiteBookQuote
from .lite_config import FIXED_SHARES
from .lite_risk import sweep_taker_fee


DIRECTION_OUTPUTS = {
    "BUY_YES", "BUY_NO", "BRIEF_CONFIRMATION_WAIT",
    "NO_TRADE_DATA_INVALID", "NO_TRADE_TRULY_FLAT",
}


def _value(obj: Any, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


@dataclass(frozen=True)
class DirectionDecision:
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


class LiteStrategy:
    def __init__(self, cfg):
        self.cfg = cfg

    @staticmethod
    def _finite(value) -> Optional[float]:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        return result if math.isfinite(result) else None

    def choose_direction(self, features: dict, *, cex_price: Optional[float],
                         market=None, yes_book: Optional[LiteBookQuote] = None,
                         no_book: Optional[LiteBookQuote] = None,
                         cex_age_ms: Optional[int] = None) -> DirectionDecision:
        """Compute competing YES/NO evidence and exactly one output state."""
        cex = self._finite(cex_price)
        returns = features.get("returns", {}) if isinstance(features, dict) else {}
        r10 = self._finite(features.get("return_10s", returns.get(10))) if isinstance(features, dict) else None
        r30 = self._finite(features.get("return_30s", returns.get(30))) if isinstance(features, dict) else None
        r60 = self._finite(features.get("return_60s", returns.get(60))) if isinstance(features, dict) else None
        tick = self._finite(features.get("tick_return")) if isinstance(features, dict) else None
        volatility = self._finite(features.get("volatility")) if isinstance(features, dict) else None
        available = [(r10, 0.50), (r30, 0.30), (r60, 0.20)]
        available = [(value, weight) for value, weight in available if value is not None]
        age_invalid = False
        if cex_age_ms is not None:
            try:
                age = int(cex_age_ms)
                age_invalid = age < 0 or age > int(self.cfg.cex_max_age_ms)
            except (TypeError, ValueError):
                age_invalid = True
        if cex is None or cex <= 0 or not available or age_invalid:
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                "missing_cex_or_evidenced_returns", return_10s=r10,
                return_30s=r30, return_60s=r60, tick_return=tick,
                volatility=volatility)

        weight_total = sum(weight for _, weight in available)
        weighted_return = sum(value * weight for value, weight in available) / weight_total
        try:
            configured_threshold = abs(float(self.cfg.momentum_min_pct))
            score_min = float(self.cfg.direction_score_min)
            score_flat = float(self.cfg.direction_score_flat)
        except (TypeError, ValueError, OverflowError):
            configured_threshold = score_min = score_flat = float("nan")
        if (not all(math.isfinite(value) for value in (
                configured_threshold, score_min, score_flat))
                or configured_threshold <= 0 or score_min <= 0
                or score_flat < 0 or score_flat >= score_min):
            return DirectionDecision(
                "NO_TRADE_DATA_INVALID", None, 0.5, 0.5, 0.0, 0.0,
                "invalid_direction_configuration", return_10s=r10,
                return_30s=r30, return_60s=r60, tick_return=tick,
                volatility=volatility)
        threshold = configured_threshold
        direction_score = weighted_return / threshold
        if r10 is not None and r30 is not None:
            direction_score += 0.20 * ((r10 - r30 / 3.0) / threshold)
        if tick is not None:
            direction_score += 0.10 * (tick / threshold)
        # Price context is deliberately small: it can break a near-tie but can
        # never overpower evidenced CEX direction.
        if yes_book is not None and no_book is not None:
            if yes_book.best_ask is not None and no_book.best_ask is not None:
                direction_score += 0.05 * (float(no_book.best_ask) - float(yes_book.best_ask))
        if market is not None and bool(_value(market, "anchor_available", False)):
            anchor = self._finite(_value(market, "price_to_beat", None))
            if anchor and anchor > 0:
                direction_score += 0.10 * max(-2.0, min(2.0, (cex-anchor)/anchor/threshold))

        normalized = max(-1.0, min(1.0, direction_score / 3.0))
        yes_score = 0.5 + normalized / 2.0
        no_score = 0.5 - normalized / 2.0
        difference = yes_score - no_score
        confidence = abs(difference)
        values = [abs(value) for value, _ in available]
        flat = (max(values) < threshold
                and (volatility is None or volatility <= threshold / 2.0))
        if flat and abs(direction_score) <= score_flat:
            output, side, reason = "NO_TRADE_TRULY_FLAT", None, "evidenced_returns_truly_flat"
        elif abs(direction_score) < score_min:
            output, side, reason = (
                "BRIEF_CONFIRMATION_WAIT", None, "yes_no_scores_nearly_equal")
        elif direction_score > 0:
            output, side, reason = "BUY_YES", "BUY_YES", "weighted_cex_evidence_up"
        else:
            output, side, reason = "BUY_NO", "BUY_NO", "weighted_cex_evidence_down"
        assert output in DIRECTION_OUTPUTS
        return DirectionDecision(
            output, side, yes_score, no_score, difference, confidence, reason,
            direction_score, r10, r30, r60, tick, volatility, weighted_return)

    def optimize_entry(self, direction: DirectionDecision,
                       book: Optional[LiteBookQuote], market, now_ms: int,
                       *, lock: Optional[dict] = None) -> EntryTimingDecision:
        """Choose ENTER_NOW, WAIT_FOR_PULLBACK, or SKIP for a locked side."""
        if direction.side not in ("BUY_YES", "BUY_NO"):
            return EntryTimingDecision("SKIP", direction.reason)
        token_id = (str(_value(market, "yes_token_id", ""))
                    if direction.side == "BUY_YES"
                    else str(_value(market, "no_token_id", "")))
        if book is None:
            return EntryTimingDecision("SKIP", "no_book")
        if str(book.token_id) != token_id:
            return EntryTimingDecision("SKIP", "invalid_token")
        condition_id = str(_value(market, "condition_id", "") or "")
        if (not book.condition_id or not condition_id
                or book.condition_id != condition_id or not book.book_hash
                or book.source_ts_ms is None):
            return EntryTimingDecision("SKIP", "invalid_market_book")
        if book.is_future(now_ms):
            return EntryTimingDecision("SKIP", "future_book")
        if book.is_stale(now_ms, int(self.cfg.book_max_age_ms)):
            return EntryTimingDecision("SKIP", "stale_book")
        if book.hash_reused:
            return EntryTimingDecision("SKIP", "reused_book_snapshot")
        if book.min_order_size is None:
            return EntryTimingDecision("SKIP", "minimum_order_size_missing")
        if FIXED_SHARES < float(book.min_order_size):
            return EntryTimingDecision("SKIP", "minimum_order_size")
        if book.spread is None or book.spread < 0:
            return EntryTimingDecision("SKIP", "no_book")
        if float(book.spread) > float(self.cfg.max_spread):
            return EntryTimingDecision("SKIP", "spread_too_wide")
        sweep = book.buy_sweep(FIXED_SHARES)
        if sweep is None:
            return EntryTimingDecision("SKIP", "insufficient_five_share_depth")
        fill = float(sweep.vwap)
        if not 0 < fill < 1:
            return EntryTimingDecision("SKIP", "no_book")
        seconds_to_close = float(_value(market, "seconds_to_close")(now_ms))
        if seconds_to_close < float(self.cfg.time_to_close_min_s):
            return EntryTimingDecision("SKIP", "price_window")

        if lock:
            locked_side = str(lock.get("side") or "")
            if locked_side != direction.side:
                return EntryTimingDecision("SKIP", "thesis_invalidated")
            initial = float(lock.get("initial_ask") or fill)
            target = float(lock.get("target_price") or initial)
            max_chase = float(lock.get("max_chase_price") or initial)
            deadline = int(lock.get("deadline_ts") or now_ms)
            waited = max(0, int(now_ms) - int(lock.get("direction_decision_ts") or now_ms))
            if fill <= target + 1e-12:
                return EntryTimingDecision(
                    "ENTER_NOW", "pullback_target_met", fill, target, max_chase,
                    deadline, max(0.0, initial-target), max(0.0, initial-fill), waited)
            if now_ms >= deadline:
                if fill <= max_chase + 1e-12:
                    return EntryTimingDecision(
                        "ENTER_NOW", "pullback_missed_enter_before_deadline", fill,
                        target, max_chase, deadline, max(0.0, initial-target),
                        initial-fill, waited, missed_opportunity=True)
                return EntryTimingDecision(
                    "SKIP", "max_chase_exceeded", None, target, max_chase,
                    deadline, max(0.0, initial-target), initial-fill, waited,
                    chase_prevented=True)
            return EntryTimingDecision(
                "WAIT_FOR_PULLBACK", "awaiting_bounded_pullback", None, target,
                max_chase, deadline, max(0.0, initial-target), 0.0, waited)

        extended = abs(direction.direction_score) >= float(self.cfg.extension_score)
        expensive = fill >= 0.60
        enough_time = seconds_to_close > (
            float(self.cfg.time_to_close_min_s) + float(self.cfg.pullback_wait_s) + 2.0)
        if extended and expensive and enough_time:
            improvement = min(float(self.cfg.pullback_target_improvement), fill-0.001)
            target = max(0.001, fill-improvement)
            max_chase = min(0.999, fill+float(self.cfg.max_chase_worsening))
            deadline = min(
                int(now_ms + float(self.cfg.pullback_wait_s)*1000),
                int(_value(market, "window_close_s")*1000
                    - float(self.cfg.time_to_close_min_s)*1000))
            return EntryTimingDecision(
                "WAIT_FOR_PULLBACK", "strong_but_temporarily_extended", None,
                target, max_chase, deadline, improvement)
        return EntryTimingDecision("ENTER_NOW", "strong_continuation_enter_now", fill)

    @staticmethod
    def _same_window(position, market) -> bool:
        if str(_value(position, "asset", "")).upper() != str(_value(market, "asset", "")).upper():
            return False
        return int(_value(position, "window_close_ts", -1)) == int(
            _value(market, "window_close_s", 0) * 1000)

    def evaluate(self, market, yes_book: Optional[LiteBookQuote],
                 no_book: Optional[LiteBookQuote], cex_price: Optional[float],
                 cex_age_ms: Optional[int], cex_source: str,
                 momentum_values, now_ms: int, open_positions,
                 *, window_lock: Optional[dict] = None) -> LiteDecision:
        """Compatibility orchestration wrapper used by the runtime/tests."""
        del cex_source
        if market is None:
            return LiteDecision(False, "no_market")
        try:
            seconds_to_close = float(market.seconds_to_close(now_ms))
        except Exception:
            return LiteDecision(False, "no_market")
        if seconds_to_close <= 0:
            return LiteDecision(False, "expired_market")
        if not (float(self.cfg.time_to_close_min_s) <= seconds_to_close
                <= float(self.cfg.time_to_close_max_s)):
            return LiteDecision(False, "price_window")
        try:
            age = int(cex_age_ms)
            cex = float(cex_price)
        except (TypeError, ValueError):
            return LiteDecision(False, "cex_stale")
        if not math.isfinite(cex) or cex <= 0 or age < 0 or age > int(self.cfg.cex_max_age_ms):
            return LiteDecision(False, "cex_stale")

        if isinstance(momentum_values, dict) and "returns" in momentum_values:
            features = momentum_values
        elif isinstance(momentum_values, dict):
            features = {
                "returns": momentum_values,
                **{f"return_{window}s": momentum_values.get(window, momentum_values.get(str(window)))
                   for window in (10, 30, 60)},
            }
        else:
            values = list(momentum_values or [])
            windows = list(getattr(self.cfg, "momentum_windows_s", (10, 30, 60)))
            mapped = {window: values[min(index, len(values)-1)]
                      for index, window in enumerate(windows) if values}
            features = {"returns": mapped, **{f"return_{w}s": mapped.get(w) for w in (10,30,60)}}
        direction = self.choose_direction(
            features, cex_price=cex, market=market, yes_book=yes_book,
            no_book=no_book, cex_age_ms=age)
        if direction.side is None:
            reason = {
                "BRIEF_CONFIRMATION_WAIT": "brief_confirmation_wait",
                "NO_TRADE_TRULY_FLAT": "truly_flat",
                "NO_TRADE_DATA_INVALID": "data_invalid",
            }[direction.output]
            return LiteDecision(
                False, reason, direction_output=direction.output,
                direction_score=direction.direction_score,
                yes_score=direction.yes_score, no_score=direction.no_score,
                confidence=direction.confidence, direction_reason=direction.reason)

        rows = list(open_positions.values()) if isinstance(open_positions, dict) else list(open_positions or [])
        same_window = [row for row in rows if self._same_window(row, market)]
        if same_window:
            if any(str(_value(row, "side", "")) != direction.side for row in same_window):
                reason = "opposite_side_blocked"
            else:
                reason = "duplicate_same_side_blocked"
            return LiteDecision(False, reason, side=direction.side,
                                direction_output=direction.output)
        active = [row for row in rows if str(_value(row, "status", "")) in (
            "OPEN", "EXIT_PENDING", "PENDING_RESOLUTION", "UNRESOLVED_RETRYING",
            "UNRESOLVED_FINAL")]
        if len(active) >= int(self.cfg.max_open_positions):
            return LiteDecision(False, "max_open_positions", side=direction.side,
                                direction_output=direction.output)
        if sum(str(_value(row, "asset", "")).upper() == str(market.asset).upper()
               for row in active) >= int(self.cfg.max_open_per_asset):
            return LiteDecision(False, "max_open_positions", side=direction.side,
                                direction_output=direction.output)
        book = yes_book if direction.side == "BUY_YES" else no_book
        timing = self.optimize_entry(direction, book, market, now_ms, lock=window_lock)
        token_id = str(market.yes_token_id if direction.side == "BUY_YES" else market.no_token_id)
        if timing.action != "ENTER_NOW":
            return LiteDecision(
                False, timing.reason, direction.side, token_id,
                direction_output=direction.output, entry_action=timing.action,
                direction_score=direction.direction_score,
                yes_score=direction.yes_score, no_score=direction.no_score,
                confidence=direction.confidence, direction_reason=direction.reason,
                target_price=timing.target_price, max_chase_price=timing.max_chase_price,
                deadline_ts=timing.deadline_ts,
                expected_improvement=timing.expected_improvement,
                actual_improvement=timing.actual_improvement,
                wait_duration_ms=timing.wait_duration_ms,
                missed_opportunity=timing.missed_opportunity,
                chase_prevented=timing.chase_prevented)
        assert book is not None
        sweep = book.buy_sweep(FIXED_SHARES)
        assert sweep is not None and timing.entry_price is not None
        price = float(timing.entry_price)
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
        return LiteDecision(
            True, "opened", direction.side, token_id, FIXED_SHARES, price,
            FIXED_SHARES*price, direction.evidenced_momentum,
            direction.output, "ENTER_NOW", direction.direction_score,
            direction.yes_score, direction.no_score, direction.confidence,
            direction.reason, timing.target_price, timing.max_chase_price,
            timing.deadline_ts, timing.expected_improvement,
            timing.actual_improvement, timing.wait_duration_ms,
            timing.missed_opportunity, timing.chase_prevented, fee, evidence)
