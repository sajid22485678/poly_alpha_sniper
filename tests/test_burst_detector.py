"""Tests for the shadow-only Hawkes-style burst detector."""
import pytest

from poly_alpha_sniper.research.burst_detector import BurstDetector


def test_empty_detector_is_neutral():
    d = BurstDetector()
    assert d.intensity(1_000_000) == 0.0
    assert d.cluster_score(1_000_000) == 0.0
    assert d.impulse_count(1_000_000, 10.0) == 0
    assert d.time_since_last_impulse_s(1_000_000) is None
    assert d.burst_active(1_000_000) is False
    snap = d.snapshot(1_000_000)
    assert snap["intensity"] == 0.0
    assert snap["time_since_last_impulse_s"] is None


def test_single_impulse_decays_by_half_each_half_life():
    d = BurstDetector(decay_half_life_s=10.0)
    d.record_impulse(0, magnitude=1.0)
    assert d.intensity(0) == pytest.approx(1.0)
    assert d.intensity(10_000) == pytest.approx(0.5)
    assert d.intensity(20_000) == pytest.approx(0.25)


def test_multiple_impulses_sum_and_decay_independently():
    d = BurstDetector(decay_half_life_s=10.0)
    d.record_impulse(0, magnitude=2.0)
    d.record_impulse(10_000, magnitude=1.0)
    # At t=10s: first has decayed to 1.0, second is fresh at 1.0.
    assert d.intensity(10_000) == pytest.approx(2.0)


def test_pruning_drops_events_older_than_ten_half_lives():
    d = BurstDetector(decay_half_life_s=10.0)
    d.record_impulse(0, magnitude=1.0)
    # 101 half-lives seconds later -> well past the 100s prune horizon.
    assert d.intensity(101_000) == 0.0
    assert d.impulse_count(101_000, 1_000.0) == 0
    # Last-impulse age survives pruning: truthful, not fabricated as None.
    assert d.time_since_last_impulse_s(101_000) == pytest.approx(101.0)


def test_impulse_count_respects_window():
    d = BurstDetector(decay_half_life_s=10.0)
    d.record_impulse(0)
    d.record_impulse(25_000)
    d.record_impulse(29_000)
    now = 30_000
    assert d.impulse_count(now, 10.0) == 2  # events at 25s and 29s
    assert d.impulse_count(now, 30.0) == 3
    assert d.impulse_count(now, -5.0) == 0  # nonsense window -> explicit zero


def test_cluster_score_saturating_map_and_bounds():
    d = BurstDetector(decay_half_life_s=10.0)
    d.record_impulse(0, magnitude=2.0)
    # intensity 2 -> 2 / (2 + 2) = 0.5
    assert d.cluster_score(0) == pytest.approx(0.5)
    # Extreme magnitude: score approaches but never exceeds 1.
    d.record_impulse(0, magnitude=1e9)
    score = d.cluster_score(0)
    assert 0.99 < score <= 1.0


def test_burst_active_threshold_boundary():
    d = BurstDetector(decay_half_life_s=10.0)
    d.record_impulse(0, magnitude=2.0)  # score exactly 0.5
    assert d.burst_active(0, threshold=0.5) is True
    assert d.burst_active(0, threshold=0.51) is False
    # After several half-lives the burst dies out.
    assert d.burst_active(50_000, threshold=0.5) is False


def test_invalid_magnitudes_are_ignored():
    d = BurstDetector()
    d.record_impulse(0, magnitude=0.0)
    d.record_impulse(0, magnitude=-3.0)
    d.record_impulse(0, magnitude=float("nan"))
    d.record_impulse(0, magnitude=float("inf"))
    assert d.intensity(0) == 0.0
    assert d.time_since_last_impulse_s(0) is None


def test_future_timestamp_clamped_not_amplified():
    d = BurstDetector(decay_half_life_s=10.0)
    d.record_impulse(60_000, magnitude=1.0)  # 60s in the "future" vs now=0
    # Negative age is clamped to 0: contributes at most raw magnitude.
    assert d.intensity(0) == pytest.approx(1.0)
    assert d.time_since_last_impulse_s(0) == 0.0


def test_out_of_order_recording_matches_in_order():
    a = BurstDetector(decay_half_life_s=10.0)
    b = BurstDetector(decay_half_life_s=10.0)
    for ts in (0, 5_000, 9_000):
        a.record_impulse(ts, magnitude=1.5)
    for ts in (9_000, 0, 5_000):
        b.record_impulse(ts, magnitude=1.5)
    assert a.snapshot(12_000) == b.snapshot(12_000)


def test_invalid_half_life_rejected():
    with pytest.raises(ValueError):
        BurstDetector(decay_half_life_s=0.0)
    with pytest.raises(ValueError):
        BurstDetector(decay_half_life_s=-1.0)
    with pytest.raises(ValueError):
        BurstDetector(decay_half_life_s=float("nan"))


def test_deterministic_replay_same_snapshot():
    def build():
        d = BurstDetector(decay_half_life_s=7.5)
        for i in range(20):
            d.record_impulse(i * 1_300, magnitude=0.5 + (i % 3))
        return d.snapshot(30_000)

    assert build() == build()
