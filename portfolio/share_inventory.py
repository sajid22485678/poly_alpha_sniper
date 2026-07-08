"""Thin share-inventory view over Portfolio."""
from __future__ import annotations


def shares_owned(portfolio, token_id: str) -> float:
    pos = portfolio.get(token_id)
    return pos.shares if pos is not None else 0.0


def can_sell(portfolio, token_id: str, shares: float) -> tuple[bool, float]:
    owned = shares_owned(portfolio, token_id)
    return shares > 0 and shares <= owned + 1e-9, owned
