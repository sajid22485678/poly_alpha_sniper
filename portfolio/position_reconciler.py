"""Local vs exchange position diffing."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import Position


def diff_positions(local: list[Position], exchange: list[Position],
                   tolerance: float = 0.01) -> dict:
    l_by_token = {p.token_id: p for p in local}
    e_by_token = {p.token_id: p for p in exchange}
    mismatches: list[str] = []
    missing_local = [t for t in e_by_token if t not in l_by_token
                     and e_by_token[t].shares > tolerance]
    missing_exchange = [t for t in l_by_token if t not in e_by_token
                        and l_by_token[t].shares > tolerance]
    share_diffs: dict[str, float] = {}
    for token, lp in l_by_token.items():
        ep = e_by_token.get(token)
        if ep is None:
            continue
        diff = abs(lp.shares - ep.shares)
        if diff > tolerance:
            share_diffs[token] = round(lp.shares - ep.shares, 4)
    for t in missing_local:
        mismatches.append(f"exchange holds {e_by_token[t].shares:.2f} of {t} unknown locally")
    for t in missing_exchange:
        mismatches.append(f"local holds {l_by_token[t].shares:.2f} of {t} not on exchange")
    for t, d in share_diffs.items():
        mismatches.append(f"share diff {d:+.4f} on {t}")
    return {"ok": not mismatches, "mismatches": mismatches,
            "missing_local": missing_local, "missing_exchange": missing_exchange,
            "share_diffs": share_diffs}
