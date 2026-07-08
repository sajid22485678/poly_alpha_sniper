"""Market-level feasibility for a small bankroll."""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import MarketInfo, OrderbookSnapshot


def market_ok_for_small_bankroll(market: MarketInfo, book: Optional[OrderbookSnapshot],
                                 cfg) -> tuple[bool, str]:
    if market.min_order_size_usd > cfg.risk.max_trade_usd:
        return False, (f"min order ${market.min_order_size_usd:.2f} > "
                       f"max trade ${cfg.risk.max_trade_usd:.2f}")
    if cfg.micro_bankroll_mode.reject_wide_spread and book is not None \
            and book.spread is not None and book.spread > cfg.microstructure.max_spread:
        return False, f"spread {book.spread:.3f} too wide for small bankroll"
    return True, "ok"
