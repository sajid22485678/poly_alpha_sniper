from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import AggressionMode
from poly_alpha_sniper.deterministic_intelligence.adaptive_aggression import AdaptiveAggression
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg, portfolio_snapshot


def _aa(clock=None):
    return AdaptiveAggression(cfg(), clock or SimClock(NOW_MS))


def test_default_is_normal():
    aa = _aa()
    assert aa.current == AggressionMode.NORMAL


def test_loss_streak_triggers_defensive():
    aa = _aa()
    aa.record_trade(-0.10, 0.06, -0.05, 80)
    aa.record_trade(-0.12, 0.06, -0.06, 80)
    mode = aa.evaluate(portfolio_snapshot(consecutive_losses=2))
    assert mode == AggressionMode.DEFENSIVE
    assert "loss_streak" in aa.reason


def test_defensive_cooldown_holds():
    clock = SimClock(NOW_MS)
    aa = _aa(clock)
    aa.record_trade(-0.10, 0.06, -0.05, 80)
    aa.record_trade(-0.12, 0.06, -0.06, 80)
    aa.evaluate(portfolio_snapshot(consecutive_losses=2))
    # wins later, but cooldown not elapsed
    for _ in range(10):
        aa.record_trade(0.15, 0.06, 0.07, 90)
    clock.advance_ms(60_000)  # 1 min < 30 min cooldown
    assert aa.evaluate(portfolio_snapshot()) == AggressionMode.DEFENSIVE
    clock.advance_ms(31 * 60_000)
    assert aa.evaluate(portfolio_snapshot()) != AggressionMode.DEFENSIVE


def test_good_rolling_performance_triggers_aggressive():
    aa = _aa()
    for _ in range(20):
        aa.record_trade(0.12, 0.06, 0.06, 90)  # all wins, full realization
    mode = aa.evaluate(portfolio_snapshot(equity=12.0))
    assert mode == AggressionMode.AGGRESSIVE


def test_insufficient_samples_stay_normal():
    aa = _aa()
    for _ in range(5):
        aa.record_trade(0.12, 0.06, 0.06, 90)
    assert aa.evaluate(portfolio_snapshot()) == AggressionMode.NORMAL


def test_drawdown_triggers_defensive():
    aa = _aa()
    for _ in range(10):
        aa.record_trade(0.05, 0.06, 0.04, 90)
    snap = portfolio_snapshot(equity=8.0)
    snap.equity_ath_usd = 10.0  # 20% drawdown > 15% threshold
    assert aa.evaluate(snap) == AggressionMode.DEFENSIVE


def test_bad_fill_quality_triggers_defensive():
    aa = _aa()
    for _ in range(6):
        aa.record_trade(0.05, 0.06, 0.05, 30)  # awful fills
    assert aa.evaluate(portfolio_snapshot()) == AggressionMode.DEFENSIVE
