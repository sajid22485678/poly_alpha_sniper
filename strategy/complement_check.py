"""YES/NO complement consistency (report-only)."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import OrderbookSnapshot


def complement_gap(yes_book: Optional[OrderbookSnapshot],
                   no_book: Optional[OrderbookSnapshot]) -> dict:
    out = {"sum_asks": None, "sum_bids": None, "arb": False, "gap": 0.0}
    if yes_book is None or no_book is None:
        return out
    ya, na = yes_book.best_ask, no_book.best_ask
    yb, nb = yes_book.best_bid, no_book.best_bid
    if ya is not None and na is not None:
        out["sum_asks"] = ya + na
        if ya + na < 0.99:
            out["arb"] = True
            out["gap"] = 0.99 - (ya + na)
    if yb is not None and nb is not None:
        out["sum_bids"] = yb + nb
        if yb + nb > 1.01:
            out["arb"] = True
            out["gap"] = max(out["gap"], (yb + nb) - 1.01)
    return out
