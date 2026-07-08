"""Deterministic session (UTC hour) multipliers.

Quiet Asia hours (roughly 00-06 UTC) get slightly stricter edge requirements;
US/EU overlap (13-21 UTC) runs at baseline. Multipliers only ever tighten
(edge_mult >= 1.0) or shrink size (size_mult <= 1.0).
"""
from __future__ import annotations

_TABLE: dict[int, tuple[float, float]] = {}
for h in range(24):
    if 0 <= h < 6:          # quiet Asia
        _TABLE[h] = (1.15, 0.9)
    elif 6 <= h < 12:       # EU morning
        _TABLE[h] = (1.05, 1.0)
    elif 12 <= h < 21:      # US/EU overlap — best liquidity
        _TABLE[h] = (1.0, 1.0)
    else:                   # late US
        _TABLE[h] = (1.10, 1.0)


def session_multipliers(hour_utc: int) -> dict:
    edge_mult, size_mult = _TABLE[hour_utc % 24]
    return {"edge_mult": edge_mult, "size_mult": size_mult}
