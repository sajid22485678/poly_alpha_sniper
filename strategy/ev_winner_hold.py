"""WS5D: EV/thesis-based winner-hold exit engine -- CHALLENGER ONLY.

Problem this addresses: the champion ExitEngine's fixed take-profit
(tier_exit_rules[tier].take_profit_pct) sells a winner the instant it
crosses a fixed percentage, regardless of how much further the fair-value
model thinks it could run. This engine instead holds while the thesis
(fair value / oracle anchor / momentum) stays intact, and only exits on a
real invalidation signal.

NOT wired into core.app.App's live trade loop -- gated by
cfg.ev_thesis_exit.enabled (default False, see core/config_loader.py) and
only ever instantiated by the replay/research tooling
(backtest/point_in_time_replay.py). Must not become primary unless a
replay run shows it beats the champion on risk-adjusted metrics (spec
requirement) -- that comparison is the replay engine's job, not this
module's.

Mirrors strategy.exit_engine.ExitEngine's evaluate() signature so the
replay engine can swap champion/challenger interchangeably, plus one extra
optional `oracle_anchor` parameter this engine actually uses.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from poly_alpha_sniper.core.contracts import (
    ExitDecision, ExitReason, FairProbability, MarketInfo, MultiCexView,
    OracleAnchor, OrderbookSnapshot, Outcome, PortfolioSnapshot, Position)


@dataclass
class HoldMetrics:
    ev_exit_now: Optional[float]
    ev_hold_to_close: Optional[float]
    ev_partial_exit: Optional[float]
    winner_continuation_score: float   # 0-100, higher = stronger case to keep holding
    hold_to_resolution_probability: Optional[float]
    fair_value_decay: Optional[float]  # fair now minus a neutral 0.5 baseline; NOT vs entry (not tracked)
    thesis_flip_score: float           # 0-100, higher = thesis looks broken


def _fair_p_for_position(position: Position, market: Optional[MarketInfo],
                         fair: Optional[FairProbability]) -> Optional[float]:
    if fair is None or market is None:
        return None
    if position.outcome == Outcome.YES:
        return fair.p_up if market.direction_up_means_yes else fair.p_down
    return fair.p_down if market.direction_up_means_yes else fair.p_up


def compute_hold_metrics(position: Position, market: Optional[MarketInfo],
                         book: Optional[OrderbookSnapshot], fair: Optional[FairProbability],
                         view: Optional[MultiCexView], oracle_anchor: Optional[OracleAnchor],
                         fee_rate: float, slippage_buffer: float) -> HoldMetrics:
    """Pure. Every field is None when the inputs needed to compute it are
    missing -- never a fabricated number standing in for "not available"."""
    mark = book.best_bid if book is not None else None
    fair_p = _fair_p_for_position(position, market, fair)

    ev_exit_now = None
    if fair_p is not None and mark is not None:
        ev_exit_now = fair_p - mark - fee_rate - slippage_buffer

    ev_hold_to_close = None
    if fair_p is not None and position.avg_entry_price > 0:
        # expected payout if held to resolution ($1/share if right, $0 if
        # not) vs cost basis already paid -- no further trading costs since
        # holding to resolution doesn't cross the book again.
        ev_hold_to_close = fair_p * 1.0 - position.avg_entry_price

    ev_partial_exit = None
    if ev_exit_now is not None and ev_hold_to_close is not None:
        ev_partial_exit = (ev_exit_now + ev_hold_to_close) / 2.0

    momentum_against = 0.0
    if view is not None and view.primary is not None and market is not None:
        held_up = (position.outcome == Outcome.YES) == market.direction_up_means_yes
        m = view.primary.momentum
        momentum_against = max(0.0, -m if held_up else m)

    basis_against = 0.0
    if oracle_anchor is not None and oracle_anchor.oracle_vs_cex_basis is not None and market is not None:
        held_up = (position.outcome == Outcome.YES) == market.direction_up_means_yes
        basis = oracle_anchor.oracle_vs_cex_basis
        # basis = (cex - anchor)/anchor; if we hold "up" a NEGATIVE basis
        # (cex has fallen below anchor) argues against the thesis.
        basis_against = max(0.0, -basis if held_up else basis)

    thesis_flip_score = min(100.0, (momentum_against * 60.0) + (basis_against * 4000.0))

    winner_continuation_score = 0.0
    if fair_p is not None:
        winner_continuation_score = max(0.0, min(100.0, fair_p * 100.0 - thesis_flip_score * 0.5))

    fair_value_decay = (fair_p - 0.5) if fair_p is not None else None

    return HoldMetrics(
        ev_exit_now=ev_exit_now, ev_hold_to_close=ev_hold_to_close,
        ev_partial_exit=ev_partial_exit,
        winner_continuation_score=round(winner_continuation_score, 2),
        hold_to_resolution_probability=fair_p,
        fair_value_decay=fair_value_decay,
        thesis_flip_score=round(thesis_flip_score, 2))


class EvWinnerHoldEngine:
    """Challenger exit engine. `cfg` must have both `.exit`/`.microstructure`/
    `.ultra_short_expiry`/`.risk` (reused as safety floors, same as the
    champion) and `.ev_thesis_exit` (this engine's own knobs)."""

    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock

    def evaluate(self, position: Position, market: Optional[MarketInfo],
                 book: Optional[OrderbookSnapshot], fair: Optional[FairProbability],
                 view: Optional[MultiCexView], portfolio: PortfolioSnapshot,
                 panic: bool = False, kill: bool = False,
                 oracle_anchor: Optional[OracleAnchor] = None) -> ExitDecision:
        if panic:
            return ExitDecision.full(ExitReason.PANIC, "panic mode active")
        if kill:
            return ExitDecision.full(ExitReason.KILL_SWITCH, "kill switch active")

        cfg = self.cfg.ev_thesis_exit
        now = self.clock.now_ms()
        mark = book.best_bid if book is not None else None

        # near-close oracle uncertainty too high -- same hard floor the
        # champion uses, held-to-resolution is not a license to hold past expiry.
        if market is not None:
            tte = market.seconds_to_expiry(now)
            if tte <= self.cfg.ultra_short_expiry.force_exit_before_expiry_seconds:
                return ExitDecision.full(ExitReason.EXPIRY_RISK,
                                         f"{tte:.0f}s to expiry -- oracle uncertainty too high to hold further")

        # stop-loss / invalidation -- hard safety floor, always enforced
        # even in "hold the winner" mode.
        if mark is not None and position.avg_entry_price > 0:
            pnl_pct = (mark - position.avg_entry_price) / position.avg_entry_price
            sl = self.cfg.exit.stop_loss_pct
            if pnl_pct <= -sl:
                return ExitDecision.full(ExitReason.STOP_LOSS,
                                         f"pnl {pnl_pct:.1%} <= -{sl:.0%} (safety floor, not overridden by hold logic)")

        metrics = compute_hold_metrics(
            position, market, book, fair, view, oracle_anchor,
            self.cfg.oracle_ev.fee_rate, self.cfg.oracle_ev.slippage_buffer)

        # book illiquid / spread too wide -- can't trust the fair-value
        # comparison against a spread this wide.
        if book is not None and book.spread is not None:
            if book.spread > self.cfg.microstructure.max_spread * 1.5:
                return ExitDecision.full(ExitReason.ORDERBOOK_FLIP,
                                         f"spread {book.spread:.3f} too wide to hold reliably")

        # fair value fell below executable bid + safety margin -> thesis invalidated
        if metrics.hold_to_resolution_probability is not None and mark is not None:
            if metrics.hold_to_resolution_probability < mark + cfg.min_ev_to_hold:
                return ExitDecision.full(ExitReason.THESIS_INVALIDATED,
                                         f"fair {metrics.hold_to_resolution_probability:.3f} < "
                                         f"bid {mark:.3f} + margin {cfg.min_ev_to_hold}")

        # oracle basis flipped against the held direction
        if cfg.exit_on_oracle_basis_flip and oracle_anchor is not None \
                and oracle_anchor.oracle_vs_cex_basis is not None and market is not None:
            held_up = (position.outcome == Outcome.YES) == market.direction_up_means_yes
            basis = oracle_anchor.oracle_vs_cex_basis
            against = -basis if held_up else basis
            if against > self.cfg.oracle_ev.max_basis_abs_pct:
                return ExitDecision.full(ExitReason.ORACLE_BASIS_FLIP,
                                         f"oracle-vs-cex basis {basis:+.4f} flipped against held side")

        # CEX momentum flips strongly against the position (thesis flip) --
        # stricter threshold than the champion's OPPOSITE_SIGNAL trigger
        # (0.5) since this engine deliberately tolerates more noise.
        if cfg.exit_on_thesis_flip and view is not None and view.primary is not None and market is not None:
            m = view.primary.momentum
            held_up = (position.outcome == Outcome.YES) == market.direction_up_means_yes
            against = -m if held_up else m
            if against > 0.65:
                return ExitDecision.full(ExitReason.OPPOSITE_SIGNAL,
                                         f"momentum {m:.2f} strongly against held side (thesis flip)")

        # Hold: model probability remains high, fair value above bid, oracle
        # thesis valid, time remaining still supports the expected payout.
        return ExitDecision.none()
