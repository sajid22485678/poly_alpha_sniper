from poly_alpha_sniper.strategy.calibration import brier_score, bucketize
from poly_alpha_sniper.strategy.calibration_report import build_report, render_text


def _rows(pred: float, wins: int, losses: int, tier="A", asset="BTC"):
    rows = []
    for i in range(wins):
        rows.append({"ts_ms": 1000 + i, "asset": asset, "tier": tier,
                     "market_title": "Bitcoin Up or Down",
                     "fair_probability": pred, "resolved_outcome": "WIN"})
    for i in range(losses):
        rows.append({"ts_ms": 2000 + i, "asset": asset, "tier": tier,
                     "market_title": "Bitcoin Up or Down",
                     "fair_probability": pred, "resolved_outcome": "LOSS"})
    return rows


def test_overconfident_flagged():
    # predicts 70% but wins only 52% -> overconfident (master example)
    rows = _rows(0.70, wins=52, losses=48)
    report = build_report(rows)
    assert report["overconfident"] is True
    assert report["underconfident"] is False
    assert "OVERCONFIDENT" in render_text(report)


def test_well_calibrated_not_flagged():
    rows = _rows(0.70, wins=70, losses=30)
    report = build_report(rows)
    assert report["overconfident"] is False
    assert report["underconfident"] is False


def test_brier_sane():
    perfect = [(1.0, True)] * 10
    awful = [(1.0, False)] * 10
    assert brier_score(perfect) == 0.0
    assert brier_score(awful) == 1.0


def test_empty_rows_no_crash():
    report = build_report([])
    assert report["n"] == 0
    assert "No resolved predictions" in render_text(report)


def test_bucket_structure():
    buckets = bucketize([(0.05, False), (0.55, True), (0.95, True)], 10)
    assert len(buckets) == 10
    assert sum(b["n"] for b in buckets) == 3


def test_slices_present():
    rows = _rows(0.7, 30, 20, tier="A", asset="BTC") + _rows(0.6, 10, 10, tier="B", asset="ETH")
    report = build_report(rows)
    assert "BTC" in report["by_asset"]
    assert "B" in report["by_tier"]
    assert report["drift"]
