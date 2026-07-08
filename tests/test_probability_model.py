import pytest

from poly_alpha_sniper.core.contracts import MarketType
from poly_alpha_sniper.strategy.probability_model import ProbabilityModel
from poly_alpha_sniper.tests.helpers import NOW_MS, book, cfg, market, stats


def _model():
    return ProbabilityModel(cfg())


def test_up_momentum_raises_p_up():
    m = market()
    fair = _model().fair(stats(momentum=0.8, ret2=0.003), m,
                         book(), book("tok_no", bid=0.38, ask=0.40), NOW_MS)
    assert fair.p_up > 0.5
    assert fair.p_up + fair.p_down == pytest.approx(1.0, abs=0.05)


def test_down_momentum_lowers_p_up():
    m = market()
    fair = _model().fair(stats(momentum=-0.8, ret2=-0.003), m,
                         book(), book("tok_no", bid=0.38, ask=0.40), NOW_MS)
    assert fair.p_up < 0.5


def test_clamps_hold():
    c = cfg()
    m = market()
    fair = ProbabilityModel(c).fair(stats(momentum=1.0, ret2=0.02, zscore=8.0), m,
                                    book(), None, NOW_MS)
    assert c.probability_model.clamp_min <= fair.p_up <= c.probability_model.clamp_max
    assert c.probability_model.clamp_min <= fair.p_down <= c.probability_model.clamp_max


def test_threshold_market_uses_distance_model():
    m = market(market_type=MarketType.THRESHOLD, threshold=99_000.0)
    # price 100k well above 99k strike with little time left -> p_up high
    fair = _model().fair(stats(price=100_000.0, vol=0.0001), m, book(), None, NOW_MS)
    assert fair.p_up > 0.8
    assert "distance" in fair.explanation


def test_confidence_range_and_freshness_penalty():
    m = market()
    fresh = _model().fair(stats(), m, book(), book("tok_no"), NOW_MS)
    stale = _model().fair(stats(fresh=False), m, book(), book("tok_no"), NOW_MS)
    assert 0 <= fresh.confidence <= 100
    assert stale.confidence < fresh.confidence


def test_flipped_token_mapping_orients_p_up():
    m_normal = market(up_means_yes=True)
    m_flipped = market(up_means_yes=False)
    st = stats(momentum=0.8, ret2=0.003)
    fair_n = _model().fair(st, m_normal, book(), None, NOW_MS)
    fair_f = _model().fair(st, m_flipped, book(), None, NOW_MS)
    # p_up (prob of UP move) should be >0.5 in both orientations
    assert fair_n.p_up > 0.5
    assert fair_f.p_up > 0.5
