from poly_alpha_sniper.strategy.profit_lock_rules import evaluate_equity_locks
from poly_alpha_sniper.tests.helpers import cfg


def test_equity_up_50_pct_locks_profit():
    r = evaluate_equity_locks(cfg(), equity=15.0, starting=10.0, ath=15.0,
                              today_start_equity=12.0)
    assert "LOCK_PROFIT" in r["actions"]


def test_drawdown_from_ath_switches_defensive():
    r = evaluate_equity_locks(cfg(), equity=8.0, starting=10.0, ath=10.0,
                              today_start_equity=10.0)
    assert "SWITCH_DEFENSIVE" in r["actions"]


def test_equity_doubles_reduces_risk_one_day():
    r = evaluate_equity_locks(cfg(), equity=21.0, starting=10.0, ath=21.0,
                              today_start_equity=10.0)
    assert "REDUCE_RISK_TODAY" in r["actions"]
    assert "LOCK_PROFIT" in r["actions"]  # also up >50%


def test_no_triggers():
    r = evaluate_equity_locks(cfg(), equity=10.5, starting=10.0, ath=10.5,
                              today_start_equity=10.0)
    assert r["actions"] == []


def test_disabled():
    c = cfg()
    c.profit_lock.enabled = False
    r = evaluate_equity_locks(c, equity=30.0, starting=10.0, ath=30.0,
                              today_start_equity=10.0)
    assert r["actions"] == []
