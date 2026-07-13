"""Event-driven tier router and evidence-bound shadow entry builder.

There is intentionally no exchange client, order placement method, wallet,
signer, or cancellation surface in this module.  A CROSS_SPREAD action means
"persist the evidenced simulated five-share sweep" only.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
import json
import math
from typing import Optional

from .config import FrequencyV4Config
from .contracts import CandidateEvaluation, EntrySide, MarketIdentity
from .economics import EconomicCalculation


class ExecutionState(str, Enum):
    TRACKING = "TRACKING"
    MAKER_OBSERVE = "MAKER_OBSERVE"
    CONFIRM_ONLY = "CONFIRM_ONLY"
    CROSSED = "CROSSED"
    SKIPPED = "SKIPPED"
    SAFETY_FAILED = "SAFETY_FAILED"


class RouterAction(str, Enum):
    NO_ACTION = "NO_ACTION"
    START_OBSERVATION = "START_OBSERVATION"
    CONTINUE_OBSERVING = "CONTINUE_OBSERVING"
    CROSS_SPREAD = "CROSS_SPREAD"
    SKIP = "SKIP"
    SAFETY_FAIL = "SAFETY_FAIL"


def candidate_key(identity: MarketIdentity, calculation: EconomicCalculation,
                  event_ts_ms: int) -> str:
    fair = calculation.result
    payload = {
        "window": identity.window_key,
        "market": identity.market_id,
        "condition": identity.condition_id,
        "side": fair.selected_side.value if fair.selected_side else None,
        "yes": round(fair.fair_probability_yes, 12),
        "edge": round(float(fair.selected_net_edge or 0.0), 12),
        "events": list(fair.evidence_event_ids),
        "ts": int(event_ts_ms),
    }
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def tier_for_edge(edge: Optional[float], cfg: FrequencyV4Config) -> str:
    if edge is None or not math.isfinite(float(edge)) or float(edge) <= 0.0:
        return "NO_POSITIVE_EDGE"
    if edge >= cfg.strong_cross_edge:
        return "STRONG_CROSS"
    if edge >= cfg.medium_maker_edge:
        return "MEDIUM_MAKER"
    if edge >= cfg.weak_observe_edge:
        return "WEAK_CONFIRM"
    return "BELOW_ENTRY_CLASSIFICATION"


@dataclass(slots=True)
class WindowExecution:
    identity: MarketIdentity
    side: EntrySide
    state: ExecutionState
    tier: str
    candidate_id: str
    initial: EconomicCalculation
    maker_start_ts_ms: int
    maker_deadline_ts_ms: int
    maker_start_monotonic_ns: int
    maker_min_monotonic_ns: int
    maker_deadline_monotonic_ns: int
    maker_target: Optional[float]
    initial_executable_price: float
    initial_edge: float
    price_touched: bool = False
    update_count: int = 0
    last_update_ts_ms: int = 0
    final: Optional[EconomicCalculation] = None
    final_reason: str = ""
    maker_fill_assumed: bool = False

    @property
    def window_key(self) -> str:
        return self.identity.window_key


@dataclass(frozen=True, slots=True)
class RouterDecision:
    action: RouterAction
    reason: str
    tier: str
    side: Optional[EntrySide]
    calculation: EconomicCalculation
    state: Optional[WindowExecution]
    actual_observation_ms: int = 0
    chase_worsening: Optional[float] = None
    maker_fill_assumed: bool = False

    def __post_init__(self) -> None:
        if self.maker_fill_assumed:
            raise ValueError("Frequency V4 never assumes a maker fill")


class ExecutionRouter:
    """One state machine per exact asset/window; deadlines use monotonic time."""

    IMMEDIATE_SAFETY_REASONS = {
        "wrong_token", "wrong_market", "future_book", "stale_snapshot",
        "websocket_not_hydrated", "paired_book_timestamp_skew",
        "paired_book_connection_epoch_mismatch", "minimum_order_size",
    }

    def __init__(self, cfg: FrequencyV4Config):
        self.cfg = cfg
        self._active: dict[str, WindowExecution] = {}

    def active(self, identity: MarketIdentity) -> Optional[WindowExecution]:
        return self._active.get(identity.window_key)

    def restore(self, state: WindowExecution) -> None:
        if state.identity.window_key in self._active:
            raise ValueError("duplicate window controller")
        self._active[state.identity.window_key] = state

    def safety_fail(self, identity: MarketIdentity,
                    calculation: EconomicCalculation, *, reason: str,
                    monotonic_ns: int) -> RouterDecision:
        """Terminate an active observation on an external evidence failure.

        The data-orchestration layer owns provider freshness.  This explicit
        hook lets loss of all fresh CEX evidence fail an active maker state
        immediately without pretending the economic calculation itself came
        from safe evidence.
        """

        active = self._active.pop(identity.window_key, None)
        elapsed = 0
        side = calculation.result.selected_side
        tier = tier_for_edge(calculation.result.selected_net_edge, self.cfg)
        if active is not None:
            active.state = ExecutionState.SAFETY_FAILED
            active.final = calculation
            active.final_reason = str(reason)
            side = active.side
            tier = active.tier
            elapsed = max(
                0, (int(monotonic_ns) - active.maker_start_monotonic_ns) // 1_000_000
            )
        return RouterDecision(
            RouterAction.SAFETY_FAIL, str(reason), tier, side,
            calculation, active, int(elapsed), maker_fill_assumed=False,
        )

    @staticmethod
    def _selected_price(calculation: EconomicCalculation) -> Optional[float]:
        side = calculation.result.selected_side
        if side is EntrySide.BUY_YES and calculation.yes_sweep is not None:
            return calculation.yes_sweep.vwap
        if side is EntrySide.BUY_NO and calculation.no_sweep is not None:
            return calculation.no_sweep.vwap
        return None

    @staticmethod
    def _maker_target(calculation: EconomicCalculation) -> Optional[float]:
        selected = (calculation.result.yes if calculation.result.selected_side is EntrySide.BUY_YES
                    else calculation.result.no if calculation.result.selected_side is EntrySide.BUY_NO
                    else None)
        if selected is None or selected.executable_vwap is None:
            return None
        # Observational only: use the selected book's best implied passive price
        # (executable VWAP minus one visible spread), never claim a fill there.
        return max(0.001, selected.executable_vwap - float(selected.spread or 0.0))

    @staticmethod
    def _selected_side_result(calculation: EconomicCalculation):
        return (calculation.result.yes if calculation.result.selected_side is EntrySide.BUY_YES
                else calculation.result.no if calculation.result.selected_side is EntrySide.BUY_NO
                else None)

    def _safety_failure(self, calculation: EconomicCalculation) -> Optional[str]:
        for reason in calculation.invalid_reasons:
            if reason in self.IMMEDIATE_SAFETY_REASONS:
                return reason
        return None

    def on_candidate(
        self, *, identity: MarketIdentity, calculation: EconomicCalculation,
        now_ms: int, monotonic_ns: int,
    ) -> RouterDecision:
        if int(now_ms) >= identity.window_close_ms:
            active = self._active.pop(identity.window_key, None)
            if active is not None:
                active.state = ExecutionState.SAFETY_FAILED
                active.final = calculation
                active.final_reason = "window_closed"
            return RouterDecision(RouterAction.SAFETY_FAIL, "window_closed", "CLOSED", None,
                                  calculation, active)
        active = self._active.get(identity.window_key)
        safety = self._safety_failure(calculation)
        if safety and active is not None:
            self._active.pop(identity.window_key, None)
            active.state = ExecutionState.SAFETY_FAILED
            active.final = calculation
            active.final_reason = safety
            return RouterDecision(RouterAction.SAFETY_FAIL, safety, active.tier, active.side,
                                  calculation, active,
                                  max(0, (monotonic_ns-active.maker_start_monotonic_ns)//1_000_000))
        if active is not None:
            return self._update_active(active, calculation, now_ms, monotonic_ns)

        side = calculation.result.selected_side
        edge = calculation.result.selected_net_edge
        tier = tier_for_edge(edge, self.cfg)
        if side is None or edge is None or edge <= 0.0:
            return RouterDecision(RouterAction.NO_ACTION, "no_positive_fee_net_edge", tier,
                                  side, calculation, None)
        price = self._selected_price(calculation)
        if price is None:
            return RouterDecision(RouterAction.NO_ACTION, "no_executable_five_share_sweep", tier,
                                  side, calculation, None)
        if tier == "STRONG_CROSS":
            return RouterDecision(RouterAction.CROSS_SPREAD, "strong_edge_immediate_cross", tier,
                                  side, calculation, None)
        if tier == "BELOW_ENTRY_CLASSIFICATION":
            # This is intentionally non-terminal; later events may improve the
            # same window.  A frequency quota cannot turn it into an entry.
            return RouterDecision(RouterAction.NO_ACTION, "positive_edge_below_weak_threshold",
                                  tier, side, calculation, None)

        duration_ms = self.cfg.maker_observation_default_ms
        state = WindowExecution(
            identity=identity,
            side=side,
            state=(ExecutionState.MAKER_OBSERVE if tier == "MEDIUM_MAKER"
                   else ExecutionState.CONFIRM_ONLY),
            tier=tier,
            candidate_id=candidate_key(identity, calculation, now_ms),
            initial=calculation,
            maker_start_ts_ms=int(now_ms),
            maker_deadline_ts_ms=int(now_ms) + duration_ms,
            maker_start_monotonic_ns=int(monotonic_ns),
            maker_min_monotonic_ns=int(monotonic_ns) + self.cfg.maker_observation_min_ms * 1_000_000,
            maker_deadline_monotonic_ns=int(monotonic_ns) + duration_ms * 1_000_000,
            maker_target=self._maker_target(calculation),
            initial_executable_price=float(price),
            initial_edge=float(edge),
            last_update_ts_ms=int(now_ms),
        )
        self._active[identity.window_key] = state
        return RouterDecision(RouterAction.START_OBSERVATION, "maker_observation_started", tier,
                              side, calculation, state)

    def _update_active(self, active: WindowExecution, calculation: EconomicCalculation,
                       now_ms: int, monotonic_ns: int) -> RouterDecision:
        active.update_count += 1
        active.last_update_ts_ms = int(now_ms)
        active.final = calculation
        selected_side = calculation.result.selected_side
        selected = self._selected_side_result(calculation)
        current_price = self._selected_price(calculation)
        if (selected_side is active.side and selected is not None
                and active.maker_target is not None and selected.executable_vwap is not None
                and selected.executable_vwap <= active.maker_target + 1e-12):
            active.price_touched = True

        elapsed_ms = max(0, int((monotonic_ns-active.maker_start_monotonic_ns)//1_000_000))
        eligible_time = monotonic_ns >= active.maker_min_monotonic_ns
        expired = monotonic_ns >= active.maker_deadline_monotonic_ns
        same_side = selected_side is active.side
        edge = calculation.result.selected_net_edge if same_side else None
        clears_cross = edge is not None and edge >= self.cfg.strong_cross_edge
        worsening = (None if current_price is None else
                     float(current_price) - active.initial_executable_price)
        chase_ok = worsening is not None and worsening <= self.cfg.max_chase_worsening + 1e-12

        if eligible_time and clears_cross and chase_ok:
            self._active.pop(active.window_key, None)
            active.state = ExecutionState.CROSSED
            active.final_reason = "fresh_recompute_clears_cross"
            return RouterDecision(RouterAction.CROSS_SPREAD,
                                  "fresh_recompute_clears_cross", active.tier,
                                  active.side, calculation, active, elapsed_ms, worsening)
        if eligible_time and clears_cross and not chase_ok:
            self._active.pop(active.window_key, None)
            active.state = ExecutionState.SKIPPED
            active.final_reason = "chase_rejected"
            return RouterDecision(RouterAction.SKIP, "chase_rejected", active.tier,
                                  active.side, calculation, active, elapsed_ms, worsening)
        if expired:
            self._active.pop(active.window_key, None)
            active.state = ExecutionState.SKIPPED
            if not same_side:
                reason = "selected_side_changed"
            elif edge is None or edge <= 0.0:
                reason = "edge_expired"
            elif edge < self.cfg.strong_cross_edge:
                reason = "final_edge_below_cross_threshold"
            else:
                reason = "no_safe_cross"
            active.final_reason = reason
            return RouterDecision(RouterAction.SKIP, reason, active.tier, active.side,
                                  calculation, active, elapsed_ms, worsening)
        # A transient economic dip is deliberately not terminal before the
        # observation deadline.  Only genuine safety failures end early.
        return RouterDecision(RouterAction.CONTINUE_OBSERVING,
                              "awaiting_observation_deadline", active.tier,
                              active.side, calculation, active, elapsed_ms, worsening)


def candidate_contract(
    *, identity: MarketIdentity, models, regime: str,
    calculation: EconomicCalculation, decision: RouterDecision,
    now_ms: int, monotonic_ns: int,
) -> CandidateEvaluation:
    state = decision.state
    key = state.candidate_id if state is not None else candidate_key(identity, calculation, now_ms)
    return CandidateEvaluation(
        candidate_id=key,
        market=identity,
        evaluation_ts_ms=int(now_ms),
        evaluation_monotonic_ns=int(monotonic_ns),
        regime=regime,
        model_outputs=tuple(models),
        initial_fair_value=(state.initial.result if state is not None else calculation.result),
        final_fair_value=(calculation.result if state is not None else None),
        selected_side=decision.side,
        execution_tier=decision.tier,
        decision=decision.action.value,
        reason=decision.reason,
        positive_edge=bool(calculation.result.selected_net_edge is not None
                           and calculation.result.selected_net_edge > 0.0),
        source_event_ids=tuple(calculation.result.evidence_event_ids),
        book_event_ids=tuple(event for event in calculation.result.evidence_event_ids if event),
        cex_event_ids=tuple(dict.fromkeys(event for model in models for event in model.evidence_event_ids)),
        maker_start_ts_ms=state.maker_start_ts_ms if state is not None else None,
        maker_deadline_ts_ms=state.maker_deadline_ts_ms if state is not None else None,
        maker_fill_assumed=False,
        chase_rejected=decision.reason == "chase_rejected",
        missed_opportunity=(decision.action is RouterAction.SKIP
                            and calculation.result.selected_net_edge is not None
                            and calculation.result.selected_net_edge > 0.0),
    )


class ShadowEntryBuilder:
    """Create an exact simulated-entry row; the store remains authoritative."""

    def __init__(self, cfg: FrequencyV4Config):
        self.cfg = cfg

    def build(self, *, identity: MarketIdentity, decision: RouterDecision,
              candidate_id: int | str, window_lock_id: int,
              runtime_session_id: str, current_commit: str, now_ms: int) -> dict:
        if decision.action is not RouterAction.CROSS_SPREAD or decision.side is None:
            raise ValueError("only an evidenced cross action can create a shadow entry")
        calc = decision.calculation
        side_result = calc.result.yes if decision.side is EntrySide.BUY_YES else calc.result.no
        sweep = calc.yes_sweep if decision.side is EntrySide.BUY_YES else calc.no_sweep
        fee = calc.yes_fee_total if decision.side is EntrySide.BUY_YES else calc.no_fee_total
        if (sweep is None or fee is None or sweep.shares != 5.0
                or not side_result.valid or side_result.net_edge is None
                or side_result.net_edge <= 0.0
                or side_result.executable_vwap != sweep.vwap):
            raise ValueError("entry evidence is incomplete or not positive EV")
        idempotency = sha256(
            "|".join((
                "lite_frequency_v4", identity.asset, identity.slug,
                identity.market_id, identity.event_id, identity.condition_id,
                str(identity.window_open_ms), decision.side.value, "5",
            )).encode()
        ).hexdigest()
        book = calc.yes_sweep if decision.side is EntrySide.BUY_YES else calc.no_sweep
        book_state = None
        # The exact book snapshot is linked separately by the orchestrator;
        # this compact consumed evidence is repeated on the entry for audit.
        if book is not None:
            book_state = book.to_dict()
        return {
            "runtime_session_id": runtime_session_id,
            "window_lock_id": int(window_lock_id),
            "candidate_id": int(candidate_id) if str(candidate_id).isdigit() else None,
            "strategy_id": "lite_frequency_v4",
            "mode": "lite_frequency_v4_shadow",
            "idempotency_key": idempotency,
            "asset": identity.asset,
            "market_id": identity.market_id,
            "event_id": identity.event_id,
            "condition_id": identity.condition_id,
            "slug": identity.slug,
            "window_open_ms": identity.window_open_ms,
            "window_close_ms": identity.window_close_ms,
            "side": decision.side.value,
            "token_id": identity.token_for_side(decision.side),
            "shares": 5.0,
            "entry_ts_ms": int(now_ms),
            "entry_mode": ("CROSS_SPREAD" if decision.state is None
                           else "CROSS_SPREAD_AFTER_OBSERVATION"),
            "entry_price": sweep.vwap,
            "worst_consumed_price": sweep.worst_price,
            "entry_notional": sweep.notional,
            "entry_fee": float(fee),
            "entry_cost": sweep.notional + float(fee),
            "initial_net_edge": (decision.state.initial.result.selected_net_edge
                                 if decision.state is not None else side_result.net_edge),
            "final_net_edge": side_result.net_edge,
            "fair_probability": side_result.fair_probability,
            "execution_buffer": side_result.execution_buffer,
            "latency_buffer": side_result.latency_buffer,
            "uncertainty_buffer": side_result.uncertainty_buffer,
            "maker_fill_assumed": 0,
            "maker_observation_ms": decision.actual_observation_ms,
            "fill_levels_json": json.dumps(book_state, sort_keys=True, separators=(",", ":")),
            "runtime_commit": current_commit,
            "status": "OPEN",
            "execution_verified": 1,
        }
