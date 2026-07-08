"""Structural sanity of a fully-built Signal before it reaches the gate."""
from __future__ import annotations

import math

from poly_alpha_sniper.core.contracts import Signal


def _bad(x: float) -> bool:
    return x is None or math.isnan(x) or math.isinf(x)


def check_signal(signal: Signal) -> tuple[bool, list[str]]:
    issues: list[str] = []
    if not signal.market.market_id:
        issues.append("missing market_id")
    if not signal.signal_id:
        issues.append("missing signal_id")
    if _bad(signal.fair.confidence) or not (0 <= signal.fair.confidence <= 100):
        issues.append("confidence out of range")
    for name, p in (("fair_p", signal.edge.fair_probability),
                    ("market_price", signal.edge.market_price)):
        if _bad(p) or not (0.0 <= p <= 1.0):
            issues.append(f"{name} out of [0,1]")
    for name, e in (("raw_edge", signal.edge.raw_edge),
                    ("edge_after_slippage", signal.edge.edge_after_slippage)):
        if _bad(e) or not (-1.0 <= e <= 1.0):
            issues.append(f"{name} out of [-1,1]")
    if signal.seconds_to_expiry <= 0:
        issues.append("expiry not in future")
    expected_side = signal.market.side_for_direction(signal.direction)
    if signal.side != expected_side:
        issues.append(f"side {signal.side.value} inconsistent with direction "
                      f"{signal.direction.value} (expected {expected_side.value})")
    return (not issues), issues
