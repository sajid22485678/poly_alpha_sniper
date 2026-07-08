"""Backtest CLI.

Run: python -m poly_alpha_sniper.backtest.replay --cex data/cex.csv [--poly data/poly.csv]
"""
from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="poly_alpha_sniper backtest")
    parser.add_argument("--cex", required=True, help="CEX ticks CSV (ts_ms,asset,price)")
    parser.add_argument("--poly", default="", help="optional real Polymarket books CSV")
    parser.add_argument("--profile", default="backtest_5min")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    from poly_alpha_sniper.backtest.metrics import render_report
    from poly_alpha_sniper.backtest.replay_engine import ReplayEngine
    from poly_alpha_sniper.core.config_loader import load_config

    cfg = load_config(profile_override=args.profile)
    engine = ReplayEngine(cfg, args.cex, args.poly, seed=args.seed)
    if engine.synthetic:
        print(">>> No Polymarket book history supplied — using synthetic delayed "
              "odds. RESULTS ARE RESEARCH ONLY. <<<\n")
    result = engine.run_both()
    print(render_report(result["metrics"]))


if __name__ == "__main__":
    main()
