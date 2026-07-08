"""Signal engine: shock + market + books -> fully-formed Signal.

Deterministic signal_id (uuid5 over market/side/shock-ts) so replays produce
identical ids. Alpha score blends edge, confidence and market quality.
"""
from __future__ import annotations

import uuid
from typing import Optional

from poly_alpha_sniper.core.contracts import (
    MarketInfo, MultiCexView, OrderbookSnapshot, Outcome, Shock, Signal, Tier, clamp)
from poly_alpha_sniper.deterministic_intelligence.opportunity_tier import classify_tier
from poly_alpha_sniper.strategy.edge_engine import compute_edge
from poly_alpha_sniper.strategy.trade_quality_gate import trade_quality

_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


class SignalEngine:
    def __init__(self, cfg, clock, probability_model, quality_scorer):
        self.cfg = cfg
        self.clock = clock
        self.prob_model = probability_model
        self.quality_scorer = quality_scorer

    def build_signal(self, shock: Shock, market: MarketInfo, view: MultiCexView,
                     yes_book: Optional[OrderbookSnapshot],
                     no_book: Optional[OrderbookSnapshot], now_ms: int) -> Optional[Signal]:
        if view is None or view.primary is None:
            return None
        side = market.side_for_direction(shock.direction)
        book = yes_book if side.outcome == Outcome.YES else no_book
        if book is None:
            return None

        fair = self.prob_model.fair(view.primary, market, yes_book, no_book, now_ms, shock)
        fair_for_side = fair.p_up if (side.outcome == Outcome.YES) == market.direction_up_means_yes \
            else fair.p_down
        # orient: fair probability that THIS side's token pays out 1.
        # BUY_YES pays when the YES condition happens.
        if side.outcome == Outcome.YES:
            fair_p = fair.p_up if market.direction_up_means_yes else fair.p_down
        else:
            fair_p = fair.p_down if market.direction_up_means_yes else fair.p_up
        del fair_for_side

        size_hint = clamp(self.cfg.risk.max_trade_usd, self.cfg.risk.min_trade_usd,
                          self.cfg.risk.max_trade_usd)
        edge = compute_edge(side, fair_p, book, self.cfg, size_hint, fair.confidence)
        if edge is None or edge.raw_edge <= 0:
            return None

        quality = self.quality_scorer.score(market, yes_book, no_book, now_ms)
        tq_score, _ok_by_mode = trade_quality(edge, quality,
                                              max(0, now_ms - shock.ts_ms), self.cfg)
        alpha = clamp(0.5 * clamp(edge.edge_after_slippage, 0.0, 0.15) / 0.15 * 100.0
                      + 0.3 * fair.confidence + 0.2 * quality.score, 0.0, 100.0)

        tier = classify_tier(edge, fair.confidence, quality.score, self.cfg,
                             self.cfg.trading_mode)
        rules = self.cfg.tier_exit_rules.get(tier.value)
        exit_plan = (f"TP {rules.take_profit_pct:.0%} / SL {rules.stop_loss_pct:.0%} / "
                     f"maxhold {rules.max_hold_seconds:.0f}s / "
                     f"force-exit {self.cfg.ultra_short_expiry.force_exit_before_expiry_seconds:.0f}s pre-expiry"
                     if rules else "full close before expiry")

        signal_id = str(uuid.uuid5(_NAMESPACE, f"{market.market_id}|{side.value}|{shock.ts_ms}"))
        return Signal(
            signal_id=signal_id, ts_ms=now_ms, asset=shock.asset, market=market,
            side=side, direction=shock.direction, shock=shock, fair=fair, edge=edge,
            market_quality=quality, trade_quality=tq_score, alpha_score=alpha,
            tier=tier, exit_plan=exit_plan,
            seconds_to_expiry=market.seconds_to_expiry(now_ms))
