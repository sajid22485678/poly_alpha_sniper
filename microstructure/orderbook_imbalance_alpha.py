"""Imbalance-change alpha hint: building bid pressure suggests UP repricing."""
from __future__ import annotations


def imbalance_signal(imbalance_now: float, imbalance_prev: float) -> dict:
    delta = imbalance_now - imbalance_prev
    if abs(imbalance_now) < 0.15 and abs(delta) < 0.1:
        return {"direction_hint": "NONE", "strength": 0.0}
    direction = "UP" if (imbalance_now > 0 or delta > 0.15) else "DOWN"
    strength = min(1.0, abs(imbalance_now) * 0.7 + abs(delta) * 0.6)
    return {"direction_hint": direction, "strength": round(strength, 3)}
