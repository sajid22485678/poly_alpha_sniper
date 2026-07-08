from poly_alpha_sniper.strategy.capital_scaling import current_max_trade_usd
from poly_alpha_sniper.tests.helpers import cfg


def test_tiers_map_correctly():
    c = cfg()
    c.capital_scaling.auto_increase_size = True
    good = dict(live_trade_count=150, rolling_pf=1.5, fill_quality_ok=True)
    assert current_max_trade_usd(c, 15, **good).recommended_cap_usd == 1
    assert current_max_trade_usd(c, 30, **good).recommended_cap_usd == 2
    assert current_max_trade_usd(c, 75, **good).recommended_cap_usd == 5
    assert current_max_trade_usd(c, 200, **good).recommended_cap_usd == 10.0  # 5% of 200


def test_auto_increase_off_pins_active_cap():
    c = cfg()  # auto_increase_size false by default
    d = current_max_trade_usd(c, 75, live_trade_count=150, rolling_pf=1.5,
                              fill_quality_ok=True)
    assert d.active_cap_usd == c.risk.max_trade_usd == 1
    assert d.recommended_cap_usd == 5


def test_requirement_gates():
    c = cfg()
    c.capital_scaling.auto_increase_size = True
    d = current_max_trade_usd(c, 75, live_trade_count=10, rolling_pf=1.5,
                              fill_quality_ok=True)
    assert not d.scale_allowed
    assert "live trades" in d.reason
    d2 = current_max_trade_usd(c, 75, live_trade_count=150, rolling_pf=1.0,
                               fill_quality_ok=True)
    assert not d2.scale_allowed
    d3 = current_max_trade_usd(c, 75, live_trade_count=150, rolling_pf=1.5,
                               fill_quality_ok=False)
    assert not d3.scale_allowed


def test_disabled_scaling():
    c = cfg()
    c.capital_scaling.enabled = False
    d = current_max_trade_usd(c, 75, 150, 1.5, True)
    assert d.active_cap_usd == c.risk.max_trade_usd
