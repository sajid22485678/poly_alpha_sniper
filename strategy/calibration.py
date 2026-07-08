"""Calibration math: predicted probability vs realized outcome."""
from __future__ import annotations


def bucketize(preds: list[tuple[float, bool]], n_buckets: int = 10) -> list[dict]:
    buckets: list[dict] = []
    for i in range(n_buckets):
        lo, hi = i / n_buckets, (i + 1) / n_buckets
        members = [(p, w) for p, w in preds if lo <= p < hi or (i == n_buckets - 1 and p == 1.0)]
        n = len(members)
        mean_pred = sum(p for p, _ in members) / n if n else 0.0
        winrate = sum(1 for _, w in members if w) / n if n else 0.0
        buckets.append({"bucket_lo": lo, "bucket_hi": hi, "n": n,
                        "mean_pred": round(mean_pred, 4),
                        "winrate": round(winrate, 4),
                        "gap": round(mean_pred - winrate, 4) if n else 0.0})
    return buckets


def brier_score(preds: list[tuple[float, bool]]) -> float:
    if not preds:
        return 0.0
    return sum((p - (1.0 if w else 0.0)) ** 2 for p, w in preds) / len(preds)


def calibration_error(buckets: list[dict]) -> float:
    """Weighted mean absolute gap across populated buckets (ECE)."""
    total = sum(b["n"] for b in buckets)
    if total == 0:
        return 0.0
    return sum(abs(b["gap"]) * b["n"] for b in buckets) / total


def confidence_flags(buckets: list[dict], threshold: float = 0.05) -> dict:
    total = sum(b["n"] for b in buckets)
    if total == 0:
        return {"overconfident": False, "underconfident": False}
    weighted_gap = sum(b["gap"] * b["n"] for b in buckets) / total
    return {"overconfident": weighted_gap > threshold,
            "underconfident": weighted_gap < -threshold}
