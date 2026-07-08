"""Adaptive aggression: DEFENSIVE <-> NORMAL <-> AGGRESSIVE from rolling stats.

Rules (deterministic, master spec):
- DEFENSIVE on loss streak / drawdown / poor rolling PF / bad fill quality.
  Holds for defensive_cooldown_minutes before it can upgrade again.
- AGGRESSIVE only with enough samples AND good PF AND winrate AND edge
  realization AND low drawdown AND good fills.
- NORMAL otherwise (and the starting default from config/profile).
"""
from __future__ import annotations

from collections import deque

from poly_alpha_sniper.core.contracts import AggressionMode, PortfolioSnapshot


class AdaptiveAggression:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self._trades: deque[dict] = deque(maxlen=20)
        self._predictions: deque[dict] = deque(maxlen=50)
        self._mode = AggressionMode(cfg.adaptive_aggression.default_mode)
        self._defensive_until_ms = 0
        self.reason = "startup default"

    # ------------------------------------------------------------------
    @property
    def current(self) -> AggressionMode:
        return self._mode

    def record_trade(self, pnl_usd: float, edge_expected: float,
                     edge_realized: float, fill_quality: float) -> None:
        self._trades.append({"pnl": pnl_usd, "edge_expected": edge_expected,
                             "edge_realized": edge_realized, "fill_quality": fill_quality})

    def record_prediction(self, edge: float, confidence: float) -> None:
        self._predictions.append({"edge": edge, "confidence": confidence})

    # ------------------------------------------------------------------
    def rolling_stats(self) -> dict:
        trades = list(self._trades)
        n = len(trades)
        wins = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] <= 0]
        gross_win = sum(t["pnl"] for t in wins)
        gross_loss = abs(sum(t["pnl"] for t in losses))
        pf = (gross_win / gross_loss) if gross_loss > 1e-9 else (2.0 if gross_win > 0 else 1.0)
        winrate = len(wins) / n if n else 0.0
        expectancy = sum(t["pnl"] for t in trades) / n if n else 0.0
        exp_edges = [t["edge_expected"] for t in trades if t["edge_expected"] > 0]
        realization = 0.0
        if exp_edges:
            realized = [t["edge_realized"] for t in trades if t["edge_expected"] > 0]
            realization = max(-1.0, min(2.0, sum(realized) / max(sum(exp_edges), 1e-9)))
        fq = sum(t["fill_quality"] for t in trades) / n if n else 100.0
        streak = 0
        for t in reversed(trades):
            if t["pnl"] <= 0:
                streak += 1
            else:
                break
        return {"n": n, "profit_factor": pf, "winrate": winrate, "expectancy": expectancy,
                "edge_realization": realization, "fill_quality": fq, "loss_streak": streak}

    def evaluate(self, portfolio: PortfolioSnapshot) -> AggressionMode:
        a = self.cfg.adaptive_aggression
        if not a.enabled:
            return self._mode
        stats = self.rolling_stats()
        now = self.clock.now_ms()
        drawdown_pct = 0.0
        if portfolio.equity_ath_usd > 0:
            drawdown_pct = max(0.0, (portfolio.equity_ath_usd - portfolio.equity_usd)
                               / portfolio.equity_ath_usd)

        # --- DEFENSIVE triggers -------------------------------------------
        defensive_reasons = []
        streak = max(stats["loss_streak"], portfolio.consecutive_losses)
        if streak >= a.defensive_loss_streak:
            defensive_reasons.append(f"loss_streak={streak}")
        if drawdown_pct >= a.defensive_max_drawdown_pct:
            defensive_reasons.append(f"drawdown={drawdown_pct:.1%}")
        if stats["n"] >= 5 and stats["profit_factor"] < a.defensive_min_profit_factor:
            defensive_reasons.append(f"pf={stats['profit_factor']:.2f}")
        if stats["n"] >= 5 and stats["fill_quality"] < 50:
            defensive_reasons.append(f"fill_quality={stats['fill_quality']:.0f}")
        if defensive_reasons:
            self._mode = AggressionMode.DEFENSIVE
            self._defensive_until_ms = now + int(a.defensive_cooldown_minutes * 60_000)
            self.reason = "defensive: " + ", ".join(defensive_reasons)
            return self._mode

        # cooling off after a defensive episode
        if now < self._defensive_until_ms:
            self._mode = AggressionMode.DEFENSIVE
            self.reason = "defensive cooldown active"
            return self._mode

        # --- AGGRESSIVE requirements --------------------------------------
        if (stats["n"] >= a.min_rolling_trades_for_aggressive
                and stats["profit_factor"] >= a.aggressive_min_profit_factor
                and stats["winrate"] >= a.aggressive_min_winrate
                and stats["edge_realization"] >= a.aggressive_min_edge_realization
                and drawdown_pct < a.defensive_max_drawdown_pct / 2
                and stats["fill_quality"] >= 70):
            self._mode = AggressionMode.AGGRESSIVE
            self.reason = (f"aggressive: pf={stats['profit_factor']:.2f} "
                           f"wr={stats['winrate']:.0%} er={stats['edge_realization']:.2f}")
            return self._mode

        self._mode = AggressionMode.NORMAL
        self.reason = f"normal: n={stats['n']} pf={stats['profit_factor']:.2f}"
        return self._mode
