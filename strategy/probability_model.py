"""Deterministic ensemble-logistic fair probability model.

p_up = probability that the market's POSITIVE (YES-side) condition resolves
true, oriented via market.direction_up_means_yes. Components per config
weights: momentum, lag (recent short return), orderbook imbalance, time decay
toward the market prior, mean-reversion penalty on extreme z-scores.

For THRESHOLD markets with a known strike, the distance-to-strike model is
the core estimate (config: use_distance_to_strike_if_available).
"""
from __future__ import annotations

import math
from typing import Optional

from poly_alpha_sniper.core.contracts import (
    CexWindowStats, FairProbability, MarketInfo, MarketType, OrderbookSnapshot, Shock, clamp)
from poly_alpha_sniper.strategy.distance_to_strike_model import prob_above_threshold


def _book_imbalance(book: Optional[OrderbookSnapshot], levels: int = 3) -> float:
    if book is None:
        return 0.0
    bid = book.depth_usd_at_bid(levels)
    ask = book.depth_usd_at_ask(levels)
    total = bid + ask
    return (bid - ask) / total if total > 0 else 0.0


class ProbabilityModel:
    def __init__(self, cfg):
        self.cfg = cfg

    def fair(self, stats: CexWindowStats, market: MarketInfo,
             yes_book: Optional[OrderbookSnapshot], no_book: Optional[OrderbookSnapshot],
             now_ms: int, shock: Optional[Shock] = None) -> FairProbability:
        w = self.cfg.probability_model
        components: dict[str, float] = {}

        # market prior from YES mid (fallback 0.5)
        prior = 0.5
        if yes_book is not None and yes_book.mid is not None:
            prior = clamp(yes_book.mid, w.clamp_min, w.clamp_max)
        components["prior"] = prior

        tte_s = max(market.seconds_to_expiry(now_ms), 1.0)

        if (market.market_type == MarketType.THRESHOLD and market.threshold
                and w.use_distance_to_strike_if_available):
            p_above, _p_below, conf = prob_above_threshold(
                stats.price, market.threshold, tte_s, stats.volatility,
                stats.momentum, stats.zscore, w.mean_reversion_penalty)
            components["distance_to_strike"] = p_above
            # p of the UP/ABOVE condition; orient to YES token
            p_up_condition = p_above
            explanation = "distance-to-strike core"
        else:
            # UP_DOWN market: probability the asset is up vs window open at expiry.
            momentum_term = w.momentum_weight * stats.momentum
            lag_term = w.lag_weight * math.tanh(stats.returns.get(2, 0.0) / 0.0015)
            imb = _book_imbalance(yes_book)
            imbalance_term = w.orderbook_imbalance_weight * imb
            mean_rev = 0.0
            if abs(stats.zscore) > 3.0:
                mean_rev = (w.mean_reversion_penalty
                            * (abs(stats.zscore) - 3.0) / 3.0
                            * (1 if stats.momentum > 0 else -1))
            x = 4.0 * (momentum_term + lag_term + imbalance_term - mean_rev)
            p_move = 1.0 / (1.0 + math.exp(-x))
            components.update({"momentum": momentum_term, "lag": lag_term,
                               "imbalance": imbalance_term, "mean_reversion": mean_rev,
                               "logistic": p_move})
            # blend toward prior as expiry approaches (prior increasingly reflects
            # locked-in price distance); time_decay_weight scales the pull.
            prior_pull = w.time_decay_weight * (1.0 - clamp(tte_s / 300.0, 0.0, 1.0))
            p_up_condition = (1 - prior_pull) * p_move + prior_pull * prior
            conf = self._confidence(stats, yes_book, no_book)
            explanation = "ensemble logistic (momentum/lag/imbalance)"

        p_yes = p_up_condition if market.direction_up_means_yes else 1.0 - p_up_condition
        p_yes = clamp(p_yes, w.clamp_min, w.clamp_max)
        p_up = p_yes if market.direction_up_means_yes else 1.0 - p_yes

        return FairProbability(
            p_up=clamp(p_up, w.clamp_min, w.clamp_max),
            p_down=clamp(1.0 - p_up, w.clamp_min, w.clamp_max),
            confidence=conf, explanation=explanation, components=components)

    def _confidence(self, stats: CexWindowStats,
                    yes_book: Optional[OrderbookSnapshot],
                    no_book: Optional[OrderbookSnapshot]) -> float:
        conf = 60.0
        if stats.fresh:
            conf += 15.0
        else:
            conf -= 30.0
        if stats.volatility > 1e-6:
            conf += 10.0
        if abs(stats.momentum) > 0.5:
            conf += 10.0
        if stats.reconnect_recent:
            conf -= 15.0
        if yes_book is None or no_book is None:
            conf -= 10.0
        return clamp(conf, 0.0, 100.0)
