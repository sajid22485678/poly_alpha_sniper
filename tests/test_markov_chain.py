"""Tests for research/markov_chain.py (shadow-only Markov state tracker)."""
import math

import pytest

from poly_alpha_sniper.research.markov_chain import (
    STATES,
    MarkovTransitionTracker,
    classify_state,
)


def _classify(**kw):
    base = dict(ret_2s=0.0, zscore=0.0, shock_score=0.0,
                direction_flips_30s=0, time_to_close_s=600.0, vol=0.001)
    base.update(kw)
    return classify_state(**base)


def test_final_window_overrides_everything():
    assert _classify(time_to_close_s=45.0) == "FINAL_WINDOW"
    assert _classify(time_to_close_s=10.0, shock_score=2.0) == "FINAL_WINDOW_CHAOS"
    assert _classify(time_to_close_s=10.0, vol=0.05) == "FINAL_WINDOW_CHAOS"


def test_shock_direction_by_ret_sign_with_zscore_tiebreak():
    assert _classify(shock_score=1.5, ret_2s=0.01) == "SHOCK_UP"
    assert _classify(shock_score=1.5, ret_2s=-0.01) == "SHOCK_DOWN"
    # ret_2s == 0 falls back to zscore sign
    assert _classify(shock_score=1.0, ret_2s=0.0, zscore=-2.0) == "SHOCK_DOWN"
    assert _classify(shock_score=1.0, ret_2s=0.0, zscore=2.0) == "SHOCK_UP"


def test_buildup_and_choppy_and_quiet():
    assert _classify(shock_score=0.7, ret_2s=0.002) == "BUILDUP_UP"
    assert _classify(shock_score=0.5, ret_2s=-0.002) == "BUILDUP_DOWN"
    assert _classify(direction_flips_30s=3) == "CHOPPY"
    assert _classify() == "QUIET"


def test_reversal_requires_prev_shock_and_opposing_ret():
    assert _classify(ret_2s=-0.01, prev_state="SHOCK_UP") == "REVERSAL"
    assert _classify(ret_2s=0.01, prev_state="SHOCK_DOWN") == "REVERSAL"
    # same-direction continuation after a shock is not a reversal
    assert _classify(ret_2s=0.01, prev_state="SHOCK_UP") == "QUIET"
    # no prior shock -> no reversal
    assert _classify(ret_2s=-0.01, prev_state="QUIET") == "QUIET"


def test_non_finite_inputs_return_neutral_quiet():
    assert _classify(ret_2s=float("nan"), shock_score=5.0) == "QUIET"
    assert _classify(vol=float("inf"), time_to_close_s=1.0) == "QUIET"


def test_classify_is_deterministic():
    kw = dict(ret_2s=0.003, zscore=1.2, shock_score=0.8,
              direction_flips_30s=1, time_to_close_s=300.0, vol=0.004)
    results = {classify_state(**kw) for _ in range(50)}
    assert results == {"BUILDUP_UP"}


def test_empty_tracker_uniform_rows_and_neutral_probs():
    t = MarkovTransitionTracker()
    assert t.n_observations == 0
    m = t.transition_matrix()
    n = len(STATES)
    for s in STATES:
        assert sum(m[s].values()) == pytest.approx(1.0)
        for p in m[s].values():
            assert p == pytest.approx(1.0 / n)
    # neutral continuation/reversal stay within clamp bounds
    for s in STATES:
        assert 0.01 <= t.continuation_probability(s) <= 0.99
        assert 0.01 <= t.reversal_probability(s) <= 0.99


def test_rows_sum_to_one_after_observations():
    t = MarkovTransitionTracker()
    t.observe("QUIET", "BUILDUP_UP")
    t.observe("BUILDUP_UP", "SHOCK_UP")
    t.observe("SHOCK_UP", "REVERSAL")
    assert t.n_observations == 3
    m = t.transition_matrix()
    for s in STATES:
        assert sum(m[s].values()) == pytest.approx(1.0)
    # observed cell dominates its row: (1+1)/(1+9) vs 1/(1+9)
    assert m["QUIET"]["BUILDUP_UP"] == pytest.approx(2.0 / 10.0)
    assert m["QUIET"]["QUIET"] == pytest.approx(1.0 / 10.0)


def test_continuation_probability_high_and_clamped():
    t = MarkovTransitionTracker()
    for _ in range(1000):
        t.observe("SHOCK_UP", "SHOCK_UP")
    p = t.continuation_probability("SHOCK_UP")
    # raw family mass approx (1001 + 1) / 1009 > 0.99 -> clamped ceiling
    assert p == pytest.approx(0.99)
    # continuation counts a shock decaying into same-direction buildup
    t2 = MarkovTransitionTracker()
    t2.observe("SHOCK_UP", "BUILDUP_UP")
    assert t2.continuation_probability("SHOCK_UP") > \
        t2.continuation_probability("SHOCK_DOWN")


def test_reversal_probability_reflects_opposite_family():
    t = MarkovTransitionTracker()
    for _ in range(50):
        t.observe("SHOCK_UP", "SHOCK_DOWN")
    p_rev = t.reversal_probability("SHOCK_UP")
    p_cont = t.continuation_probability("SHOCK_UP")
    assert p_rev > 0.8
    assert p_cont < 0.1
    assert 0.01 <= p_rev <= 0.99


def test_unknown_state_raises():
    t = MarkovTransitionTracker()
    with pytest.raises(ValueError):
        t.observe("QUIET", "NOT_A_STATE")
    with pytest.raises(ValueError):
        t.observe("BOGUS", "QUIET")
    with pytest.raises(ValueError):
        t.continuation_probability("BOGUS")
    with pytest.raises(ValueError):
        t.reversal_probability("BOGUS")


def test_tracker_deterministic_given_same_observations():
    seq = [("QUIET", "BUILDUP_UP"), ("BUILDUP_UP", "SHOCK_UP"),
           ("SHOCK_UP", "SHOCK_UP"), ("SHOCK_UP", "REVERSAL"),
           ("REVERSAL", "QUIET")]
    a, b = MarkovTransitionTracker(), MarkovTransitionTracker()
    for prev, nxt in seq:
        a.observe(prev, nxt)
        b.observe(prev, nxt)
    assert a.transition_matrix() == b.transition_matrix()
    for s in STATES:
        assert a.continuation_probability(s) == b.continuation_probability(s)
        assert a.reversal_probability(s) == b.reversal_probability(s)
