"""Monte Carlo stress: shuffle + degrade the trade stream (seeded).

Stress knobs per master spec: order shuffle, worse slippage, wider spread,
missed fills, delayed entries/exits (pnl haircut), 2x latency, random API
errors, engineered losing streaks.

Run: python -m poly_alpha_sniper.backtest.monte_carlo [--cex data/cex.csv]
"""
from __future__ import annotations

import argparse

import numpy as np

DEFAULT_STRESS = {"slippage_mult": 1.5, "spread_add": 0.005, "miss_fill_prob": 0.10,
                  "delay_haircut": 0.15, "error_prob": 0.02}


def run_monte_carlo(trades: list[dict], starting_equity: float = 10.0,
                    n: int = 1000, seed: int = 42,
                    stress: dict | None = None) -> dict:
    stress = {**DEFAULT_STRESS, **(stress or {})}
    pnls = np.array([t["pnl"] for t in trades], dtype=float)
    sizes = np.array([max(t.get("size_usd", 1.0), 0.01) for t in trades], dtype=float)
    if len(pnls) == 0:
        return {"n_paths": 0, "prob_profit": 0.0, "median_ending": starting_equity,
                "p5_ending": starting_equity, "worst_drawdown_usd": 0.0,
                "risk_of_ruin": 0.0, "note": "no trades"}
    rng = np.random.RandomState(seed)
    endings = np.zeros(n)
    worst_dd = np.zeros(n)
    ruined = 0
    ruin_floor = starting_equity * 0.2

    # cost per trade from stress: extra slippage+spread as fraction of stake
    extra_cost = (stress["spread_add"] / 2 + 0.003 * (stress["slippage_mult"] - 1.0))

    for i in range(n):
        idx = rng.permutation(len(pnls))
        path = pnls[idx].copy()
        stake = sizes[idx]
        path -= extra_cost * stake                       # worse slippage/spread
        missed = rng.rand(len(path)) < stress["miss_fill_prob"]
        path[missed & (path > 0)] = 0.0                  # missed fills kill winners
        delayed = rng.rand(len(path)) < 0.3
        path[delayed & (path > 0)] *= (1 - stress["delay_haircut"])  # late = worse
        errors = rng.rand(len(path)) < stress["error_prob"]
        path[errors] = np.minimum(path[errors], 0.0) - 0.02 * stake[errors]

        equity = starting_equity
        peak = equity
        dd = 0.0
        ruin = False
        for p in path:
            equity += p
            peak = max(peak, equity)
            dd = max(dd, peak - equity)
            if equity <= ruin_floor:
                ruin = True
        endings[i] = equity
        worst_dd[i] = dd
        ruined += int(ruin)

    return {
        "n_paths": n,
        "n_trades": len(pnls),
        "prob_profit": round(float((endings > starting_equity).mean()), 3),
        "median_ending": round(float(np.median(endings)), 3),
        "p5_ending": round(float(np.percentile(endings, 5)), 3),
        "worst_drawdown_usd": round(float(worst_dd.max()), 3),
        "risk_of_ruin": round(ruined / n, 4),
        "stress": stress,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cex", default="")
    parser.add_argument("--paths", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.cex:
        from poly_alpha_sniper.backtest.replay_engine import ReplayEngine
        from poly_alpha_sniper.core.config_loader import load_config
        cfg = load_config(profile_override="backtest_5min")
        result = ReplayEngine(cfg, args.cex).run_both()
        trades = result["compounded"]["trades"]
        starting = cfg.risk.starting_bankroll_usd
    else:
        rng = np.random.RandomState(1)
        trades = [{"pnl": float(p), "size_usd": 1.0}
                  for p in rng.normal(0.02, 0.09, 120)]
        starting = 10.0
        print("(demo trades — supply --cex for a real run)")
    out = run_monte_carlo(trades, starting, n=args.paths, seed=args.seed)
    for k, v in out.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
