"""Detect loss clusters (k losses within m minutes)."""
from __future__ import annotations


def cluster(trades: list[tuple[int, float]], k: int = 3,
            window_min: float = 15.0) -> tuple[bool, str]:
    """trades: [(ts_ms, pnl_usd)]."""
    losses = [t for t, pnl in trades if pnl < 0]
    if len(losses) < k:
        return False, f"{len(losses)} losses total"
    losses.sort()
    window_ms = int(window_min * 60_000)
    for i in range(len(losses) - k + 1):
        if losses[i + k - 1] - losses[i] <= window_ms:
            return True, f"{k} losses within {window_min:.0f} min"
    return False, "no cluster"
