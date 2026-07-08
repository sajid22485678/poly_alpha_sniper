"""Health report builder (dict + text)."""
from __future__ import annotations


def build_health(runtime_state: dict, cex_health: dict, poly_health: dict,
                 portfolio, governor_usage: dict, panic: bool, kill: bool,
                 mode: str) -> dict:
    cex_summary = {ex: ("ok" if h.get("connected") else "DOWN")
                   for ex, h in (cex_health or {}).items()}
    report = {
        "mode": mode,
        "paused": runtime_state.get("paused", False),
        "panic": panic,
        "kill_switch": kill,
        "aggression": runtime_state.get("aggression_mode", "?"),
        "equity_usd": round(portfolio.equity_usd, 2),
        "realized_today_usd": round(portfolio.realized_pnl_today_usd, 2),
        "open_positions": portfolio.open_positions,
        "exposure_usd": round(portfolio.total_exposure_usd, 2),
        "cex": cex_summary,
        "poly": poly_health,
        "rate_limits": {k: v for k, v in list((governor_usage or {}).items())[:6]},
    }
    lines = [f"HEALTH mode={mode} panic={panic} kill={kill}",
             f"equity=${report['equity_usd']} today=${report['realized_today_usd']} "
             f"open={report['open_positions']}",
             "cex: " + ", ".join(f"{k}={v}" for k, v in cex_summary.items())]
    report["text"] = "\n".join(lines)
    return report
