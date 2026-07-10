"""strategy/shock_near_miss.py: no_shock near-miss diagnostics -- pure,
read-only, must never influence the actual shock decision."""
from poly_alpha_sniper.core.contracts import CexWindowStats, MultiCexView
from poly_alpha_sniper.strategy.shock_near_miss import (
    NEAR_MISS_SCORE_THRESHOLD, compute_shock_near_miss)
from poly_alpha_sniper.tests.helpers import cfg


def _view(ret2=0.0, zscore=0.0, fresh=True, staleness_ms=100):
    stats = CexWindowStats(
        asset="BTC", exchange="bybit", price=100_000.0, ts_ms=0,
        returns={1: 0.0, 2: ret2, 3: 0.0, 5: 0.0, 10: 0.0, 15: 0.0, 30: 0.0},
        volatility=0.0002, zscore=zscore, momentum=0.0, impulse=0.0,
        fresh=fresh, staleness_ms=staleness_ms, reconnect_recent=False)
    return MultiCexView(asset="BTC", primary=stats, per_exchange={"bybit": stats},
                        confirming_exchanges=1, direction_agreement=True,
                        max_deviation_pct=0.0, any_stale=not fresh)


def test_none_view_returns_none():
    assert compute_shock_near_miss(None, cfg()) is None


def test_far_from_threshold_is_not_near_miss():
    view = _view(ret2=0.0001, zscore=0.2)  # well below shock_min_abs_return=0.0012, shock_min_zscore=2.0
    nm = compute_shock_near_miss(view, cfg())
    assert nm.shock_score < NEAR_MISS_SCORE_THRESHOLD
    assert nm.is_near_miss is False


def test_close_to_threshold_on_both_legs_is_near_miss():
    # The detector is an AND-gate (return AND z-score), so a near-miss needs
    # BOTH legs close: ret2=0.0010/0.0012 -> 0.833, z=1.6/2.0 -> 0.8;
    # min() = 0.8, within [0.7, 1.0).
    view = _view(ret2=0.0010, zscore=1.6)
    nm = compute_shock_near_miss(view, cfg())
    assert nm.zscore_score == 0.8
    assert nm.shock_score >= NEAR_MISS_SCORE_THRESHOLD
    assert nm.is_near_miss is True


def test_z_spike_alone_on_tiny_move_is_not_a_near_miss():
    """The exact quiet-market failure the FIRED mislabeling came from:
    volatility collapses, z explodes (z=10) on an economically-nothing move
    (0.03% vs the 0.12% floor). The detector would never fire (AND-gate), so
    the board must not label this FIRED/near-miss either."""
    view = _view(ret2=0.0003, zscore=10.0)
    nm = compute_shock_near_miss(view, cfg())
    assert nm.zscore_score == 2.0            # z leg maxed out...
    assert nm.shock_score < NEAR_MISS_SCORE_THRESHOLD  # ...but score follows the weak leg
    assert nm.tier == "ROUTINE_NO_SHOCK"
    assert nm.is_near_miss is False


def test_at_or_above_threshold_is_not_a_near_miss_its_a_real_shock_candidate():
    """score >= 1.0 means a real shock likely already fired -- near_miss is
    specifically the "almost but not quite" band, not "at or past it"."""
    view = _view(ret2=0.0020, zscore=3.0)  # both well past their floors
    nm = compute_shock_near_miss(view, cfg())
    assert nm.shock_score >= 1.0
    assert nm.is_near_miss is False


def test_direction_reflects_sign_of_trigger_return():
    up = compute_shock_near_miss(_view(ret2=0.002, zscore=1.0), cfg())
    down = compute_shock_near_miss(_view(ret2=-0.002, zscore=1.0), cfg())
    assert up.direction == "UP"
    assert down.direction == "DOWN"


def test_detail_suffix_is_parseable_text():
    nm = compute_shock_near_miss(_view(ret2=0.0006, zscore=1.6), cfg())
    text = nm.detail_suffix()
    assert "shock_score=" in text
    assert "near_miss=" in text
