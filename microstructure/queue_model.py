"""Passive fill probability: exponential consumption of queue ahead."""
from __future__ import annotations

import math


def passive_fill_probability(depth_ahead_usd: float, expected_flow_usd_per_s: float,
                             timeout_s: float) -> float:
    """P(queue ahead is consumed within timeout) under Poisson-ish flow."""
    if depth_ahead_usd <= 0:
        return 1.0
    if expected_flow_usd_per_s <= 0 or timeout_s <= 0:
        return 0.0
    expected_consumed = expected_flow_usd_per_s * timeout_s
    return 1.0 - math.exp(-expected_consumed / depth_ahead_usd)
