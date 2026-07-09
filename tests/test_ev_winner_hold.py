"""WS5D: EV/thesis-based winner-hold exit engine (challenger only)."""
from __future__ import annotations

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import ExitReason, FairProbability, Outcome
from poly_alpha_sniper.strategy.ev_winner_hold import EvWinnerHoldEngine, compute_hold_metrics
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg, market, portfolio_snapshot, position, view


def _engine():
    return EvWinnerHoldEngine(cfg(), SimClock(NOW_MS))


def test_challenger_never_wired_into_live_config_by_default():
    """cfg.ev_thesis_exit.enabled must default False -- this engine is
    challenger-only until a replay proves it beats the champion."""
    assert cfg().ev_thesis_exit.enabled is False


def test_does_not_exit_a_winner_past_fixed_tp_if_thesis_still_valid():
    """The whole point of WS5D: a position beyond the champion's fixed
    take-profit % must NOT auto-exit here if fair value still comfortably
    clears the bid -- this is exactly what the champion ExitEngine would do
    differently (it would TAKE_PROFIT immediately)."""
    engine = _engine()
    pos = position(entry=0.20, shares=10.0, entry_ts_ms=NOW_MS - 5_000)  # far beyond any tier's TP%
    m = market(expiry_ms=NOW_MS + 200_000)
    b = book(bid=0.45, ask=0.47)  # position is deep in profit (0.20 -> 0.45)
    fair = FairProbability(p_up=0.60, p_down=0.40, confidence=80.0)
    v = view(asset="BTC", momentum=0.05)  # no strong momentum against
    snap = portfolio_snapshot()

    decision = engine.evaluate(pos, m, b, fair, v, snap, oracle_anchor=None)

    assert not decision.should_exit


def test_exits_on_thesis_invalidated_when_fair_value_falls_below_bid():
    engine = _engine()
    pos = position(entry=0.20, shares=10.0, entry_ts_ms=NOW_MS - 5_000)
    m = market(expiry_ms=NOW_MS + 200_000)
    b = book(bid=0.45, ask=0.47)
    fair = FairProbability(p_up=0.40, p_down=0.60, confidence=80.0)  # fair (0.40) now below bid (0.45)
    v = view(asset="BTC", momentum=0.0)
    snap = portfolio_snapshot()

    decision = engine.evaluate(pos, m, b, fair, v, snap, oracle_anchor=None)

    assert decision.should_exit
    assert decision.reason == ExitReason.THESIS_INVALIDATED


def test_stop_loss_floor_still_enforced_even_in_hold_mode():
    """Holding through noise must never override the hard stop-loss safety
    floor -- this engine is not a license to ignore real losses."""
    engine = _engine()
    pos = position(entry=0.60, shares=10.0, entry_ts_ms=NOW_MS - 5_000)
    m = market(expiry_ms=NOW_MS + 200_000)
    b = book(bid=0.40, ask=0.42)  # -33% -- well past any stop_loss_pct
    fair = FairProbability(p_up=0.55, p_down=0.45, confidence=80.0)
    v = view(asset="BTC", momentum=0.0)
    snap = portfolio_snapshot()

    decision = engine.evaluate(pos, m, b, fair, v, snap, oracle_anchor=None)

    assert decision.should_exit
    assert decision.reason == ExitReason.STOP_LOSS


def test_exits_on_panic_regardless_of_thesis():
    engine = _engine()
    pos = position()
    decision = engine.evaluate(pos, market(), book(), None, None, portfolio_snapshot(),
                               panic=True, oracle_anchor=None)
    assert decision.should_exit
    assert decision.reason == ExitReason.PANIC


def test_exits_near_expiry_regardless_of_thesis():
    engine = _engine()
    pos = position(entry_ts_ms=NOW_MS - 5_000)
    m = market(expiry_ms=NOW_MS + 2_000)  # inside the force-exit window
    fair = FairProbability(p_up=0.90, p_down=0.10, confidence=95.0)
    decision = engine.evaluate(pos, m, book(bid=0.80, ask=0.82), fair, None,
                               portfolio_snapshot(), oracle_anchor=None)
    assert decision.should_exit
    assert decision.reason == ExitReason.EXPIRY_RISK


def test_compute_hold_metrics_never_fabricates_when_inputs_missing():
    pos = position()
    metrics = compute_hold_metrics(pos, None, None, None, None, None,
                                   fee_rate=0.0, slippage_buffer=0.01)
    assert metrics.ev_exit_now is None
    assert metrics.ev_hold_to_close is None
    assert metrics.hold_to_resolution_probability is None


def test_compute_hold_metrics_returns_real_numbers_when_inputs_present():
    pos = position(entry=0.20, outcome=Outcome.YES)
    m = market()
    b = book(bid=0.45, ask=0.47)
    fair = FairProbability(p_up=0.60, p_down=0.40, confidence=80.0)
    metrics = compute_hold_metrics(pos, m, b, fair, None, None, fee_rate=0.0, slippage_buffer=0.01)
    assert metrics.ev_exit_now is not None
    assert metrics.ev_hold_to_close is not None
    assert metrics.hold_to_resolution_probability == 0.60
