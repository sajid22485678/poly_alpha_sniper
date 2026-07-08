"""Backtest engine: shared-logic replay, compounding semantics, metrics."""
import csv

import pytest

from poly_alpha_sniper.backtest.metrics import compute_metrics, render_report
from poly_alpha_sniper.backtest.monte_carlo import run_monte_carlo
from poly_alpha_sniper.backtest.replay_engine import ReplayEngine
from poly_alpha_sniper.backtest.walk_forward import evaluate_folds
from poly_alpha_sniper.core.config_loader import load_config

BASE_TS = 1_752_000_000_000


def _write_ticks(path) -> None:
    """35 minutes of BTC ticks with engineered shocks that then trend on."""
    rows = []
    price = 100_000.0
    ts = BASE_TS
    for i in range(35 * 60 * 2):  # 2 ticks/second? no: 0.5s cadence, 4200 ticks
        ts += 500
        cycle = i % 360  # 3-minute pattern
        if cycle < 6:
            price += 90.0     # sharp +0.54% ramp over 3s  -> shock
        elif cycle < 200:
            price += 2.0      # gentle continuation (lag edge pays)
        else:
            price -= 3.2      # drift back down
        # small deterministic oscillation for non-zero vol
        wiggle = 4.0 if i % 2 else -4.0
        rows.append({"ts_ms": ts, "asset": "BTC", "price": round(price + wiggle, 2),
                     "exchange": "replay"})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ts_ms", "asset", "price", "exchange"])
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="module")
def replay_result(tmp_path_factory):
    path = tmp_path_factory.mktemp("bt") / "cex.csv"
    _write_ticks(path)
    cfg = load_config(profile_override="backtest_5min")
    engine = ReplayEngine(cfg, str(path), seed=7)
    return engine.run_both()


def test_replay_produces_activity(replay_result):
    compounded = replay_result["compounded"]
    assert compounded["predictions"], "no predictions generated"
    assert compounded["trades"], "no trades executed"
    assert replay_result["metrics"]["synthetic_odds"] is True
    assert "RESEARCH ONLY" in replay_result["metrics"]["data_label"]


def test_compounding_uses_realized_only(replay_result):
    compounded = replay_result["compounded"]
    cum = 10.0 + sum(t["pnl"] for t in compounded["trades"])
    assert compounded["equity"] == pytest.approx(cum, abs=0.05)


def test_fixed_size_stake_constant(replay_result):
    fixed = replay_result["fixed"]
    sizes = [t["size_usd"] for t in fixed["trades"] if t.get("size_usd")]
    assert sizes, "fixed run produced no sized trades"
    assert max(sizes) <= 1.0 + 1e-9  # max_trade_usd cap


def test_metrics_contract(replay_result):
    m = replay_result["metrics"]
    for key in ("starting_equity", "ending_equity", "roi_pct", "total_pnl",
                "max_drawdown_usd", "max_drawdown_pct", "profit_factor", "winrate",
                "total_trades", "avg_trade_size", "profit_by_asset",
                "profit_by_direction", "tier_stats", "aggression_mode_stats",
                "b_if_taken_pnl", "b_if_skipped_pnl", "fixed_vs_compounded",
                "answers", "edge_realization", "reject_counts"):
        assert key in m, f"metrics missing {key}"
    assert set(m["answers"]) == {"q1_b_tier_value", "q2_best_aggression_mode",
                                 "q3_best_max_trades_per_hour",
                                 "q4_best_edge_threshold", "q5_tiers_to_live_enable"}
    assert "BACKTEST REPORT" in render_report(m)


def test_sizing_warning_fires():
    trades = [{"ts_ms": 1, "pnl": 1.0, "size_usd": 1.0, "tier": "A",
               "aggression_mode": "NORMAL", "asset": "BTC", "direction": "UP",
               "outcome": "YES", "market_id": "m"}]
    from collections import Counter
    m = compute_metrics(trades, [], Counter(), 10.0,
                        fixed_result={"roi_pct": -1.0, "total_pnl": -0.1},
                        compounded_result={"roi_pct": 5.0, "total_pnl": 0.5})
    assert m["sizing_warning"] is True
    assert "sizing-dependent" in m["sizing_warning_text"]


def test_walk_forward_rules():
    good = [{"ts_ms": d * 86_400_000 + i * 3_600_000, "pnl": 0.1 if i % 3 else -0.05}
            for d in range(6) for i in range(8)]
    verdict = evaluate_folds(good)
    assert verdict["accept"], verdict["reasons"]

    few = good[:10]
    v2 = evaluate_folds(few)
    assert not v2["accept"]
    assert any("too few" in r for r in v2["reasons"])

    lucky = [{"ts_ms": 0 * 86_400_000 + i, "pnl": 5.0} for i in range(20)]
    lucky += [{"ts_ms": d * 86_400_000, "pnl": 0.01} for d in range(1, 5) for _ in range(4)]
    v3 = evaluate_folds(lucky)
    assert any("single day" in r for r in v3["reasons"])

    v4 = evaluate_folds(good, fixed_roi=-2.0, compounded_roi=5.0)
    assert any("sizing dependent" in r for r in v4["reasons"])


def test_monte_carlo_outputs():
    trades = [{"pnl": 0.1 if i % 3 else -0.07, "size_usd": 1.0} for i in range(60)]
    out = run_monte_carlo(trades, 10.0, n=200, seed=42)
    for key in ("prob_profit", "median_ending", "p5_ending",
                "worst_drawdown_usd", "risk_of_ruin"):
        assert key in out
    assert 0.0 <= out["prob_profit"] <= 1.0
    assert out["p5_ending"] <= out["median_ending"]
    # determinism
    out2 = run_monte_carlo(trades, 10.0, n=200, seed=42)
    assert out == out2


def test_monte_carlo_empty_trades():
    out = run_monte_carlo([], 10.0, n=10)
    assert out["n_paths"] == 0
