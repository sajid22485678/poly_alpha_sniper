"""Adverse selection: hitting a quote that predates a large CEX move means
the counterparty simply has not repriced — but so might the whole book, and
our fill becomes the exit liquidity when it does. Block toxic combinations."""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import clamp


def adverse_selection_risk(book_age_ms: float, cex_move_since_book_pct: float,
                           cfg) -> tuple[bool, float, str]:
    move = abs(cex_move_since_book_pct)
    age_factor = clamp(book_age_ms / max(cfg.polymarket.max_orderbook_staleness_ms, 1), 0.0, 2.0)
    move_factor = clamp(move / 0.003, 0.0, 2.0)  # 0.3% move = full factor
    risk = clamp(0.35 * age_factor + 0.45 * move_factor, 0.0, 1.0)
    blocked = bool(cfg.microstructure.adverse_selection_block and risk > 0.7)
    detail = f"book_age={book_age_ms:.0f}ms move_since_book={move:.3%} risk={risk:.2f}"
    return blocked, risk, detail
