"""Backtest metrics: every field from the master spec + the 5 answers."""
from __future__ import annotations

from collections import Counter, defaultdict

SIZING_WARNING = ("WARNING: compounded result is positive but fixed-size result is "
                  "not — performance may be sizing-dependent, NOT true edge.")


def _base_stats(trades: list[dict], starting: float) -> dict:
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_win, gross_loss = sum(wins), abs(sum(losses))
    equity = starting
    peak = starting
    dd_usd = dd_pct = 0.0
    for p in pnls:
        equity += p
        peak = max(peak, equity)
        dd_usd = max(dd_usd, peak - equity)
        if peak > 0:
            dd_pct = max(dd_pct, (peak - equity) / peak * 100)
    return {
        "starting_equity": starting,
        "ending_equity": round(starting + sum(pnls), 4),
        "roi_pct": round(sum(pnls) / starting * 100, 2) if starting else 0.0,
        "total_pnl": round(sum(pnls), 4),
        "max_drawdown_usd": round(dd_usd, 4),
        "max_drawdown_pct": round(dd_pct, 2),
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss > 1e-9
                         else (2.0 if gross_win else 0.0),
        "winrate": round(len(wins) / len(pnls), 4) if pnls else 0.0,
        "expectancy": round(sum(pnls) / len(pnls), 4) if pnls else 0.0,
        "total_trades": len(trades),
    }


def _by(trades: list[dict], key: str) -> dict[str, dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        groups[str(t.get(key) or "?")].append(t)
    return {k: _base_stats(v, 10.0) for k, v in sorted(groups.items())}


def compute_metrics(trades: list[dict], predictions: list[dict],
                    rejects: Counter, starting_equity: float,
                    fixed_result: dict | None = None,
                    compounded_result: dict | None = None,
                    synthetic_odds: bool = False) -> dict:
    sizes = [t.get("size_usd", 0.0) for t in trades] or [0.0]
    slippage_in = [t.get("entry_slippage_bps", 0.0) for t in trades]
    slippage_out = [t.get("exit_slippage_bps", 0.0) for t in trades]
    hours = Counter(int((t.get("ts_ms") or 0) // 3_600_000) % 24 for t in trades)
    per_hour_pnl: dict[int, float] = defaultdict(float)
    for t in trades:
        per_hour_pnl[int((t.get("ts_ms") or 0) // 3_600_000) % 24] += t["pnl"]

    tier_stats = _by(trades, "tier")
    mode_stats = _by(trades, "aggression_mode")

    b_taken = [t for t in trades if t.get("tier") == "B"]
    b_hypo = [p for p in predictions
              if p.get("tier") == "B" and p.get("decision") == "SHADOW_ONLY"
              and p.get("hypothetical_pnl") is not None]
    b_if_taken_pnl = round(sum(p["hypothetical_pnl"] for p in b_hypo)
                           + sum(t["pnl"] for t in b_taken), 4)
    b_if_skipped_pnl = round(sum(t["pnl"] for t in b_taken), 4)  # only forced ones

    metrics = {
        **_base_stats(trades, starting_equity),
        "avg_trade_size": round(sum(sizes) / len(sizes), 4),
        "largest_trade_size": round(max(sizes), 4),
        "smallest_trade_size": round(min(sizes), 4),
        "daily_stops": int(rejects.get("REJECTED_DAILY_LOSS_CAP", 0) > 0),
        "rejected_min_order_size": rejects.get("REJECTED_MIN_ORDER_SIZE_TOO_HIGH", 0),
        "rejected_spread_liquidity": (rejects.get("REJECTED_SPREAD_TOO_WIDE", 0)
                                      + rejects.get("REJECTED_LIQUIDITY_TOO_THIN", 0)),
        "reject_counts": dict(rejects),
        "failed_fill_count": sum(1 for t in trades if t.get("failed_fill")),
        "failed_sell_count": sum(1 for t in trades if t.get("failed_sell")),
        "avg_entry_slippage_bps": round(sum(slippage_in) / len(slippage_in), 2)
                                  if slippage_in else 0.0,
        "avg_exit_slippage_bps": round(sum(slippage_out) / len(slippage_out), 2)
                                 if slippage_out else 0.0,
        "profit_by_asset": {k: round(v["total_pnl"], 4) for k, v in _by(trades, "asset").items()},
        "profit_by_direction": {k: round(v["total_pnl"], 4)
                                for k, v in _by(trades, "direction").items()},
        "profit_by_outcome": {k: round(v["total_pnl"], 4)
                              for k, v in _by(trades, "outcome").items()},
        "per_hour_trades": dict(hours),
        "per_hour_pnl": {k: round(v, 4) for k, v in per_hour_pnl.items()},
        "per_market": {k: round(v["total_pnl"], 4)
                       for k, v in list(_by(trades, "market_id").items())[:30]},
        "edge_realization": _edge_realization(trades),
        "fill_quality_avg": round(sum(t.get("fill_quality", 100.0) for t in trades)
                                  / len(trades), 1) if trades else 0.0,
        "tier_stats": {k: {m: v[m] for m in ("total_trades", "profit_factor",
                                             "winrate", "max_drawdown_usd",
                                             "expectancy", "total_pnl")}
                       for k, v in tier_stats.items()},
        "aggression_mode_stats": {k: {m: v[m] for m in ("total_trades", "profit_factor",
                                                        "winrate", "total_pnl")}
                                  for k, v in mode_stats.items()},
        "b_if_taken_pnl": b_if_taken_pnl,
        "b_if_skipped_pnl": b_if_skipped_pnl,
        "b_hypothetical_count": len(b_hypo),
        "predictions": len(predictions),
        "synthetic_odds": synthetic_odds,
        "data_label": "RESEARCH ONLY — synthetic delayed odds" if synthetic_odds
                      else "real odds data",
    }

    if fixed_result is not None and compounded_result is not None:
        metrics["fixed_vs_compounded"] = {
            "fixed_roi_pct": fixed_result.get("roi_pct"),
            "compounded_roi_pct": compounded_result.get("roi_pct"),
            "fixed_pnl": fixed_result.get("total_pnl"),
            "compounded_pnl": compounded_result.get("total_pnl"),
        }
        metrics["sizing_warning"] = bool(
            (compounded_result.get("roi_pct") or 0) > 0
            and (fixed_result.get("roi_pct") or 0) <= 0)
        if metrics["sizing_warning"]:
            metrics["sizing_warning_text"] = SIZING_WARNING

    metrics["answers"] = _answers(metrics, tier_stats, mode_stats)
    return metrics


def _edge_realization(trades: list[dict]) -> float:
    exp = sum(t.get("expected_edge", 0.0) for t in trades if t.get("expected_edge", 0) > 0)
    if exp <= 1e-9:
        return 0.0
    real = sum(t["pnl"] / t["size_usd"] for t in trades
               if t.get("expected_edge", 0) > 0 and t.get("size_usd", 0) > 0)
    return round(real / exp, 3)


def _answers(m: dict, tier_stats: dict, mode_stats: dict) -> dict:
    b = tier_stats.get("B", {})
    q1 = ("B-tier adds profit" if (m["b_if_taken_pnl"] > m["b_if_skipped_pnl"]
                                   and b.get("profit_factor", 0) > 1.0)
          else "B-tier mostly adds noise — keep it shadow-only/aggressive-gated")
    best_mode = max(mode_stats.items(), key=lambda kv: kv[1]["total_pnl"])[0] \
        if mode_stats else "NORMAL"
    hours = m.get("per_hour_trades", {})
    busiest = max(hours.values()) if hours else 0
    q3 = (f"observed peak {busiest} trades/hour; config caps "
          "(12/h live) were " + ("binding" if busiest >= 12 else "not binding"))
    er = m.get("edge_realization", 0.0)
    q4 = ("current edge floors look right (realization "
          f"{er:.2f})" if er >= 0.5 else
          f"raise min edge — realization only {er:.2f} of predicted")
    live_tiers = [t for t, s in tier_stats.items()
                  if s.get("profit_factor", 0) >= 1.1 and s.get("total_trades", 0) >= 5]
    q5 = f"tiers with PF>=1.1 and n>=5: {live_tiers or ['insufficient data']}"
    return {
        "q1_b_tier_value": q1,
        "q2_best_aggression_mode": best_mode,
        "q3_best_max_trades_per_hour": q3,
        "q4_best_edge_threshold": q4,
        "q5_tiers_to_live_enable": q5,
    }


def render_report(m: dict) -> str:
    lines = ["=" * 60, "BACKTEST REPORT" + ("  [" + m["data_label"] + "]"
                                            if m.get("synthetic_odds") else ""),
             "=" * 60]
    for key in ("starting_equity", "ending_equity", "roi_pct", "total_pnl",
                "max_drawdown_usd", "max_drawdown_pct", "profit_factor", "winrate",
                "expectancy", "total_trades", "avg_trade_size",
                "avg_entry_slippage_bps", "avg_exit_slippage_bps",
                "edge_realization", "fill_quality_avg", "predictions"):
        lines.append(f"{key:>28}: {m.get(key)}")
    lines.append(f"{'profit_by_asset':>28}: {m.get('profit_by_asset')}")
    lines.append(f"{'profit_by_direction':>28}: {m.get('profit_by_direction')}")
    lines.append(f"{'reject_counts':>28}: {dict(list(m.get('reject_counts', {}).items())[:6])}")
    lines.append("-" * 60)
    lines.append("TIER STATS:")
    for tier, s in m.get("tier_stats", {}).items():
        lines.append(f"  {tier}: {s}")
    lines.append(f"B if taken: ${m.get('b_if_taken_pnl')} vs skipped: "
                 f"${m.get('b_if_skipped_pnl')} (n={m.get('b_hypothetical_count')})")
    lines.append("AGGRESSION MODES:")
    for mode, s in m.get("aggression_mode_stats", {}).items():
        lines.append(f"  {mode}: {s}")
    if "fixed_vs_compounded" in m:
        lines.append(f"fixed vs compounded: {m['fixed_vs_compounded']}")
        if m.get("sizing_warning"):
            lines.append("!!! " + m.get("sizing_warning_text", ""))
    lines.append("-" * 60)
    lines.append("ANSWERS:")
    for k, v in m.get("answers", {}).items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)
