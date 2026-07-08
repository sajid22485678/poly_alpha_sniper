"""Exit engine — all triggers, priority-ordered, shared across every mode.

Priority (contracts.EXIT_PRIORITY): panic/emergency > expiry > stop-loss >
kill-switch/daily-loss > edge-decay > opposite-signal/book-flip/momentum-fade/
max-hold > profit-lock > rebalance > take-profit.

Tier exit rules come from cfg.tier_exit_rules[tier]; missing book limits the
engine to time/expiry/panic triggers (fail-safe).
"""
from __future__ import annotations

from typing import Optional

from poly_alpha_sniper.core.contracts import (
    ExitDecision, ExitReason, FairProbability, MarketInfo, MultiCexView,
    OrderbookSnapshot, Outcome, PortfolioSnapshot, Position)
from poly_alpha_sniper.strategy.momentum_model import momentum_fading
from poly_alpha_sniper.strategy.profit_lock_exit import ProfitLockTracker


class ExitEngine:
    def __init__(self, cfg, clock):
        self.cfg = cfg
        self.clock = clock
        self.profit_lock = ProfitLockTracker(cfg)

    def _tier_rules(self, position: Position):
        return self.cfg.tier_exit_rules.get(position.tier.value) or None

    def evaluate(self, position: Position, market: Optional[MarketInfo],
                 book: Optional[OrderbookSnapshot], fair: Optional[FairProbability],
                 view: Optional[MultiCexView], portfolio: PortfolioSnapshot,
                 panic: bool = False, kill: bool = False) -> ExitDecision:
        now = self.clock.now_ms()

        if panic:
            return ExitDecision.full(ExitReason.PANIC, "panic mode active")
        if kill:
            return ExitDecision.full(ExitReason.KILL_SWITCH, "kill switch active")

        # expiry risk (works without a book)
        if market is not None:
            tte = market.seconds_to_expiry(now)
            if tte <= self.cfg.ultra_short_expiry.force_exit_before_expiry_seconds:
                return ExitDecision.full(ExitReason.EXPIRY_RISK,
                                         f"{tte:.0f}s to expiry <= force-exit window")

        rules = self._tier_rules(position)
        tp = rules.take_profit_pct if rules else self.cfg.exit.take_profit_pct
        sl = rules.stop_loss_pct if rules else self.cfg.exit.stop_loss_pct
        max_hold = rules.max_hold_seconds if rules else self.cfg.exit.max_hold_seconds
        edge_floor = rules.close_when_edge_below if rules else self.cfg.exit.close_when_edge_below

        hold_s = (now - position.entry_ts_ms) / 1000.0 if position.entry_ts_ms else 0.0

        mark = book.best_bid if book is not None else None
        pnl_pct = None
        if mark is not None and position.avg_entry_price > 0:
            pnl_pct = (mark - position.avg_entry_price) / position.avg_entry_price

        # stop loss
        if pnl_pct is not None and pnl_pct <= -sl:
            return ExitDecision.full(ExitReason.STOP_LOSS,
                                     f"pnl {pnl_pct:.1%} <= -{sl:.0%}")

        # daily loss risk: projected realized loss if we stopped out here
        if pnl_pct is not None and pnl_pct < 0:
            cap = min(self.cfg.risk.max_daily_loss_usd,
                      portfolio.equity_usd * self.cfg.risk.max_daily_loss_pct_equity)
            projected = portfolio.realized_pnl_today_usd + position.shares * (mark - position.avg_entry_price)
            if projected <= -cap:
                return ExitDecision.full(ExitReason.DAILY_LOSS_RISK,
                                         f"projected day pnl {projected:.2f} breaches cap {-cap:.2f}")

        # edge decay: our fair value for the held side barely above market
        if fair is not None and mark is not None and market is not None:
            if position.outcome == Outcome.YES:
                fair_p = fair.p_up if market.direction_up_means_yes else fair.p_down
            else:
                fair_p = fair.p_down if market.direction_up_means_yes else fair.p_up
            if (fair_p - mark) < edge_floor:
                return ExitDecision.full(ExitReason.EDGE_DECAY,
                                         f"fair {fair_p:.3f} - bid {mark:.3f} < {edge_floor}")

        # opposite shock / momentum against the position
        if view is not None and view.primary is not None and market is not None \
                and self.cfg.exit.exit_on_opposite_shock:
            m = view.primary.momentum
            held_up = (position.outcome == Outcome.YES) == market.direction_up_means_yes
            against = -m if held_up else m
            if against > 0.5:
                return ExitDecision.full(ExitReason.OPPOSITE_SIGNAL,
                                         f"momentum {m:.2f} against held side")

        # orderbook flip against the position
        if book is not None and self.cfg.exit.exit_on_orderbook_flip:
            bid_d = book.depth_usd_at_bid(3)
            ask_d = book.depth_usd_at_ask(3)
            if bid_d + ask_d > 0 and bid_d / max(ask_d, 1e-9) < 0.4:
                return ExitDecision.full(ExitReason.ORDERBOOK_FLIP,
                                         f"bid/ask depth {bid_d:.0f}/{ask_d:.0f} flipped against position")

        # momentum fade on a small winner
        if (view is not None and view.primary is not None and pnl_pct is not None
                and self.cfg.exit.exit_on_momentum_decay
                and 0 < pnl_pct < tp * 0.5 and hold_s > 20
                and momentum_fading(view.primary.returns)):
            return ExitDecision.full(ExitReason.MOMENTUM_FADE,
                                     f"momentum fading with pnl {pnl_pct:.1%}")

        # max hold
        if hold_s >= max_hold:
            return ExitDecision.full(ExitReason.MAX_HOLD, f"held {hold_s:.0f}s >= {max_hold:.0f}s")

        # profit lock (trailing)
        if pnl_pct is not None and self.cfg.exit.profit_lock_enabled:
            self.profit_lock.update(position.token_id, pnl_pct)
            if self.profit_lock.should_lock(position.token_id, pnl_pct, tp):
                return ExitDecision.full(ExitReason.PROFIT_LOCK,
                                         f"trailing lock: peak {self.profit_lock.peak(position.token_id):.1%}, "
                                         f"now {pnl_pct:.1%}")

        # take profit (partial when feasible)
        if pnl_pct is not None and pnl_pct >= tp:
            from poly_alpha_sniper.strategy.partial_exit_engine import plan_partial_exit
            min_order = self.cfg.risk.min_trade_usd
            shares, is_full = plan_partial_exit(position, book, min_order)
            if is_full or not self.cfg.sell_execution.allow_partial_exit:
                return ExitDecision.full(ExitReason.TAKE_PROFIT, f"pnl {pnl_pct:.1%} >= {tp:.0%}")
            fraction = shares / position.shares if position.shares > 0 else 1.0
            return ExitDecision.partial(ExitReason.PARTIAL_TAKE_PROFIT, fraction,
                                        f"partial TP at {pnl_pct:.1%}")

        return ExitDecision.none()
