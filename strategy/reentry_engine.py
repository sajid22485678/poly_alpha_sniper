"""Re-entry policy per market (frequency config)."""
from __future__ import annotations


def can_reenter(market_id: str, history: list[dict], cfg, now_ms: int) -> tuple[bool, str]:
    """history: prior completed trades in this market: [{'win': bool, 'ts_ms': int}]."""
    tf = cfg.trade_frequency
    past = [h for h in history if h.get("market_id", market_id) == market_id]
    if len(past) > tf.max_reentries_same_market:
        return False, f"max re-entries {tf.max_reentries_same_market} reached"
    if past and tf.allow_reentry_after_profit and not past[-1].get("win", False):
        return False, "last trade in market was a loss — re-entry requires prior profit"
    return True, "ok"
