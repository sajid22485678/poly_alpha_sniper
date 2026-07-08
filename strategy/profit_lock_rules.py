"""Equity-level profit lock rules (cfg.profit_lock). Pure function."""
from __future__ import annotations


def evaluate_equity_locks(cfg, equity: float, starting: float, ath: float,
                          today_start_equity: float) -> dict:
    actions: list[str] = []
    reasons: list[str] = []
    p = cfg.profit_lock
    if not p.enabled or starting <= 0:
        return {"actions": [], "reason": "profit lock disabled"}

    up_pct = (equity - starting) / starting * 100.0
    if up_pct >= p.lock_profit_when_equity_up_pct:
        actions.append("LOCK_PROFIT")
        reasons.append(f"equity up {up_pct:.0f}% >= {p.lock_profit_when_equity_up_pct}% "
                       f"-> mentally reserve {p.lock_profit_pct}% of gains")

    if p.if_equity_doubles_reduce_risk_one_day and today_start_equity > 0 \
            and equity >= 2 * today_start_equity:
        actions.append("REDUCE_RISK_TODAY")
        reasons.append("equity doubled within the day -> reduce risk for one day")

    if ath > 0:
        dd_pct = (ath - equity) / ath * 100.0
        if dd_pct >= p.if_drawdown_from_ath_pct and p.switch_to_defensive:
            actions.append("SWITCH_DEFENSIVE")
            reasons.append(f"drawdown from ATH {dd_pct:.0f}% >= {p.if_drawdown_from_ath_pct}%")

    return {"actions": actions, "reason": "; ".join(reasons) if reasons else "no lock triggered"}
