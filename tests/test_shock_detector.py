from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import Direction, MultiCexView
from poly_alpha_sniper.strategy.shock_detector import ShockDetector
from poly_alpha_sniper.tests.helpers import NOW_MS, cfg, stats, view


def _detector(clock=None):
    return ShockDetector(cfg(), clock or SimClock(NOW_MS))


def test_clean_up_shock_detected():
    d = _detector()
    s = d.detect(view(ret2=0.003, zscore=3.5))
    assert s is not None
    assert s.direction == Direction.UP
    assert s.zscore == 3.5
    assert s.confirming_exchanges >= 1
    assert s.reason


def test_down_shock_direction():
    d = _detector()
    s = d.detect(view(ret2=-0.003, zscore=-3.5, momentum=-0.7))
    assert s is not None
    assert s.direction == Direction.DOWN


def test_small_move_ignored():
    d = _detector()
    assert d.detect(view(ret2=0.0003, zscore=0.5)) is None


def test_low_zscore_ignored():
    d = _detector()
    assert d.detect(view(ret2=0.003, zscore=1.0)) is None


def test_stale_data_ignored():
    d = _detector()
    assert d.detect(view(ret2=0.003, zscore=3.5, fresh=False)) is None


def test_cooldown_suppresses_duplicate():
    clock = SimClock(NOW_MS)
    d = _detector(clock)
    assert d.detect(view(ret2=0.003, zscore=3.5)) is not None
    clock.advance_ms(2000)  # inside 20s cooldown
    assert d.detect(view(ret2=0.003, zscore=3.5)) is None
    clock.advance_ms(25_000)
    assert d.detect(view(ret2=0.003, zscore=3.5)) is not None


def test_exchange_disagreement_suppresses():
    v = view(ret2=0.003, zscore=3.5)
    v.direction_agreement = False
    assert _detector().detect(v) is None


def test_extreme_fakeout_suppressed():
    st = stats(ret2=-0.002, zscore=-6.0, momentum=-0.5)
    st.returns[30] = 0.02   # huge up move over 30s, 2s reversing hard -> fakeout
    v = MultiCexView(asset="BTC", primary=st, per_exchange={"binance": st},
                     confirming_exchanges=2, direction_agreement=True)
    assert _detector().detect(v) is None
