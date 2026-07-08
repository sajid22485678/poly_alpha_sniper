from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import ExitReason, FairProbability, Tier
from poly_alpha_sniper.strategy.exit_engine import ExitEngine
from poly_alpha_sniper.tests.helpers import (
    NOW_MS, book, cfg, market, portfolio_snapshot, position, view)


def _engine(c=None):
    return ExitEngine(c or cfg(), SimClock(NOW_MS))


def _fair(p_up=0.75):
    return FairProbability(p_up=p_up, p_down=1 - p_up, confidence=80)


def _eval(pos=None, bk=None, fair=None, v=None, panic=False, kill=False, m=None,
          engine=None):
    e = engine or _engine()
    return e.evaluate(pos or position(), m if m is not None else market(),
                      bk, fair, v, portfolio_snapshot(), panic=panic, kill=kill)


def test_take_profit_fires():
    # entry 0.60, bid 0.68 -> +13.3% >= 12% (tier A)
    d = _eval(bk=book(bid=0.68, ask=0.70), fair=_fair(0.80), v=view(momentum=0.8))
    assert d.should_exit
    assert d.reason in (ExitReason.TAKE_PROFIT, ExitReason.PARTIAL_TAKE_PROFIT)


def test_stop_loss_fires():
    # entry 0.60, bid 0.54 -> -10% <= -9% (tier A)
    d = _eval(bk=book(bid=0.54, ask=0.56), fair=_fair(0.75), v=view(momentum=0.5))
    assert d.should_exit
    assert d.reason == ExitReason.STOP_LOSS


def test_expiry_force_exit_outranks_tp():
    m = market(expiry_ms=NOW_MS + 15_000)  # 15s left < 20s force window
    d = _eval(bk=book(bid=0.68, ask=0.70), fair=_fair(0.80), v=view(), m=m)
    assert d.should_exit
    assert d.reason == ExitReason.EXPIRY_RISK


def test_panic_outranks_everything():
    d = _eval(bk=book(bid=0.68, ask=0.70), panic=True)
    assert d.reason == ExitReason.PANIC
    assert d.priority == 1


def test_kill_switch_exit():
    d = _eval(bk=book(bid=0.62, ask=0.64), kill=True)
    assert d.reason == ExitReason.KILL_SWITCH


def test_edge_decay_fires():
    # bid 0.62, fair for YES 0.622 -> gap 0.002 < 0.007 tier-A floor
    d = _eval(bk=book(bid=0.62, ask=0.64), fair=_fair(0.622), v=view(momentum=0.3))
    assert d.should_exit
    assert d.reason == ExitReason.EDGE_DECAY


def test_opposite_shock_fires():
    d = _eval(bk=book(bid=0.62, ask=0.64), fair=_fair(0.75),
              v=view(momentum=-0.8, ret2=-0.004))
    assert d.should_exit
    assert d.reason == ExitReason.OPPOSITE_SIGNAL


def test_max_hold_fires():
    clock = SimClock(NOW_MS)
    e = ExitEngine(cfg(), clock)
    pos = position(entry_ts_ms=NOW_MS - 200_000)  # held 200s > 150s tier A
    d = e.evaluate(pos, market(), book(bid=0.61, ask=0.63), _fair(0.72),
                   view(momentum=0.4), portfolio_snapshot())
    assert d.should_exit
    assert d.reason == ExitReason.MAX_HOLD


def test_healthy_position_no_exit():
    d = _eval(bk=book(bid=0.63, ask=0.65), fair=_fair(0.75), v=view(momentum=0.6))
    assert not d.should_exit


def test_tier_b_tighter_than_a_plus():
    c = cfg()
    engine = _engine(c)
    # +9% pnl: fires TP for tier B (8%) but not for A_PLUS (14%)
    bk = book(bid=0.654, ask=0.674)
    d_b = engine.evaluate(position(tier=Tier.B), market(), bk, _fair(0.75),
                          view(momentum=0.6), portfolio_snapshot())
    assert d_b.should_exit and d_b.reason in (ExitReason.TAKE_PROFIT,
                                              ExitReason.PARTIAL_TAKE_PROFIT)
    d_ap = _engine(c).evaluate(position(tier=Tier.A_PLUS), market(), bk, _fair(0.75),
                               view(momentum=0.6), portfolio_snapshot())
    assert not (d_ap.should_exit and d_ap.reason == ExitReason.TAKE_PROFIT)


def test_no_book_only_time_triggers():
    d = _eval(bk=None, fair=None, v=None)
    assert not d.should_exit  # young position, no book: nothing fires
    m = market(expiry_ms=NOW_MS + 10_000)
    d2 = _eval(bk=None, m=m)
    assert d2.reason == ExitReason.EXPIRY_RISK
