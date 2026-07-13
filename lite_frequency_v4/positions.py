"""Deterministic executable-exit versus hold-to-resolution management."""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Optional

from .config import FrequencyV4Config
from .contracts import BookState, EntrySide, FairValueResult, MarketIdentity, Sweep
from .economics import exact_sweep, sweep_fee


FIXED_SHARES = 5.0
EXIT_VALUE_MARGIN_USD = 0.02


def _value(value: Any, name: str, default: Any = None) -> Any:
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


@dataclass(frozen=True, slots=True)
class ManagementDecision:
    action: str
    reason: str
    decision_ts_ms: int
    entry_id: int
    side: EntrySide
    remaining_ms: int
    thesis_state: str
    fair_probability: float
    executable_exit_vwap: Optional[float]
    executable_exit_value: Optional[float]
    hold_expected_value: float
    exit_fee: Optional[float]
    uncertainty_usd: float
    spread: Optional[float]
    depth_shares: float
    evidence_age_ms: Optional[int]
    sweep: Optional[Sweep]

    @property
    def exit_selected(self) -> bool:
        return self.action == "EXIT_BOOK"


def evaluate_exit_vs_hold(
    *, entry: Any, identity: MarketIdentity, fair_value: FairValueResult,
    owned_book: Optional[BookState], now_ms: int, cfg: FrequencyV4Config,
) -> ManagementDecision:
    side = EntrySide(str(_value(entry, "side")))
    entry_id = int(_value(entry, "id", _value(entry, "entry_id", 0)) or 0)
    remaining = int(identity.window_close_ms) - int(now_ms)
    side_fair = fair_value.yes if side is EntrySide.BUY_YES else fair_value.no
    probability = side_fair.fair_probability
    uncertainty_usd = FIXED_SHARES * (
        side_fair.uncertainty_buffer + side_fair.latency_buffer
    )
    hold_value = FIXED_SHARES * probability - uncertainty_usd
    thesis_state = (
        "INVALIDATED" if probability + 1e-12 < float(_value(entry, "entry_price", 0.0))
        else "WEAKENED" if probability < float(_value(entry, "fair_probability", probability))
        else "INTACT"
    )

    def result(action: str, reason: str, *, sweep: Optional[Sweep] = None,
               fee: Optional[float] = None) -> ManagementDecision:
        value = None if sweep is None or fee is None else sweep.notional - fee
        return ManagementDecision(
            action=action,
            reason=reason,
            decision_ts_ms=int(now_ms),
            entry_id=entry_id,
            side=side,
            remaining_ms=remaining,
            thesis_state=thesis_state,
            fair_probability=probability,
            executable_exit_vwap=sweep.vwap if sweep is not None else None,
            executable_exit_value=value,
            hold_expected_value=hold_value,
            exit_fee=fee,
            uncertainty_usd=uncertainty_usd,
            spread=owned_book.spread if owned_book is not None else None,
            depth_shares=(sum(level.shares for level in owned_book.bids)
                          if owned_book is not None else 0.0),
            evidence_age_ms=(owned_book.age_ms(now_ms) if owned_book is not None else None),
            sweep=sweep,
        )

    # Official settlement is the only authoritative post-close path.  A late
    # stale book can never become an exit price.
    if remaining <= 0:
        return result("HOLD_OFFICIAL_RESOLUTION", "post_close_book_exit_forbidden")
    if owned_book is None:
        return result("HOLD", "owned_token_book_missing")
    expected_token = identity.yes_token_id if side is EntrySide.BUY_YES else identity.no_token_id
    if owned_book.token_id != expected_token:
        return result("HOLD", "wrong_owned_token_book")
    if owned_book.condition_id != identity.condition_id:
        return result("HOLD", "wrong_market_book")
    if not owned_book.hydrated:
        return result("HOLD", "owned_book_not_hydrated")
    if owned_book.provider_ts_ms >= identity.window_close_ms:
        return result("HOLD_OFFICIAL_RESOLUTION", "post_close_book_evidence_forbidden")
    if owned_book.provider_ts_ms > now_ms or owned_book.receipt_ts_ms > now_ms:
        return result("HOLD", "future_owned_book")
    if owned_book.age_ms(now_ms) > cfg.book_max_age_ms:
        return result("HOLD", "stale_owned_book")
    if owned_book.spread is None or owned_book.spread > cfg.max_spread:
        return result("HOLD", "owned_book_spread_invalid")
    sweep = exact_sweep(owned_book, buy=False)
    if sweep is None:
        return result("HOLD", "insufficient_exit_depth")
    fee = sweep_fee(sweep, cfg.crypto_taker_fee_rate)
    exit_value = sweep.notional - fee
    if not all(math.isfinite(value) for value in (exit_value, hold_value, fee)):
        return result("HOLD", "nonfinite_management_economics")
    if exit_value > hold_value + EXIT_VALUE_MARGIN_USD:
        return result("EXIT_BOOK", "exit_value_dominates_hold", sweep=sweep, fee=fee)
    # Invalidation alone does not force a sale; it merely lowers hold value.
    # The executable comparison above still decides.
    return result("HOLD", "hold_value_not_worse", sweep=sweep, fee=fee)


def management_record(decision: ManagementDecision, *, runtime_session_id: str) -> dict:
    return {
        "runtime_session_id": runtime_session_id,
        "entry_id": decision.entry_id,
        "decision_ts_ms": decision.decision_ts_ms,
        "action": decision.action,
        "reason": decision.reason,
        "thesis_state": decision.thesis_state,
        "remaining_ms": decision.remaining_ms,
        "fair_probability": decision.fair_probability,
        "executable_exit_vwap": decision.executable_exit_vwap,
        "executable_exit_value": decision.executable_exit_value,
        "hold_expected_value": decision.hold_expected_value,
        "exit_fee": decision.exit_fee,
        "uncertainty_usd": decision.uncertainty_usd,
        "spread": decision.spread,
        "depth_shares": decision.depth_shares,
        "evidence_age_ms": decision.evidence_age_ms,
        "evidence_json": (decision.sweep.to_dict() if decision.sweep is not None else {}),
    }

def exit_record(decision: ManagementDecision, entry: Any, *, runtime_session_id: str) -> dict:
    if not decision.exit_selected or decision.sweep is None or decision.exit_fee is None:
        raise ValueError("management decision did not select an executable exit")
    entry_cost = float(_value(entry, "entry_cost"))
    proceeds = decision.sweep.notional - decision.exit_fee
    pnl = proceeds - entry_cost
    return {
        "runtime_session_id": runtime_session_id,
        "entry_id": decision.entry_id,
        "exit_ts_ms": decision.decision_ts_ms,
        "exit_source": "BOOK_PRE_CLOSE",
        "exit_reason": decision.reason,
        "exit_price": decision.sweep.vwap,
        "exit_worst_price": decision.sweep.worst_price,
        "exit_notional": decision.sweep.notional,
        "exit_fee": decision.exit_fee,
        "payout": proceeds,
        "net_pnl": pnl,
        "resolution_verified": 0,
        "evidence_json": decision.sweep.to_dict(),
    }
