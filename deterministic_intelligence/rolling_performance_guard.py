"""Belt-and-braces performance guard independent of adaptive aggression."""
from __future__ import annotations


def ok_to_trade(stats: dict) -> tuple[bool, str]:
    """stats: output of AdaptiveAggression.rolling_stats()."""
    n = stats.get("n", 0)
    if n >= 10 and stats.get("profit_factor", 1.0) < 0.75:
        return False, f"rolling profit factor {stats['profit_factor']:.2f} < 0.75 over {n} trades"
    if stats.get("loss_streak", 0) >= 4:
        return False, f"loss streak {stats['loss_streak']} >= 4"
    return True, "ok"
