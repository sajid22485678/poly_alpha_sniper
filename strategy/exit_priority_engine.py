"""Stable ordering + dedup of exit decisions (one action per token/cycle)."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import ExitDecision, Position


def order_exits(pairs: list[tuple[Position, ExitDecision]]
                ) -> list[tuple[Position, ExitDecision]]:
    best: dict[str, tuple[Position, ExitDecision]] = {}
    for pos, dec in pairs:
        cur = best.get(pos.token_id)
        if cur is None or dec.priority < cur[1].priority:
            best[pos.token_id] = (pos, dec)
    out = list(best.values())
    out.sort(key=lambda p: (p[1].priority, -abs(p[0].unrealized_pnl())))
    return out
