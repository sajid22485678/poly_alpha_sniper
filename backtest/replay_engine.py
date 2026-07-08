"""Replay/backtest engine — drives the SAME production pipeline on history.

No duplicate logic: CexState, ShockDetector, SignalEngine, BalancedAlphaGate,
AdaptiveAggression, TradeFrequencyController, RiskManager, validate_order,
SimulatedClobClient (depth-limited fills, no better-than-book), ExitEngine,
SellExecutor and Portfolio are the exact objects live trading uses, run on a
SimClock.

Data: CEX ticks CSV (ts_ms,asset,price[,exchange]). Real Polymarket book
history CSV is optional; without it, synthetic delayed odds are used and the
result is labeled RESEARCH ONLY.

Fixed-size vs compounded: the sizing function is identical; fixed mode pins
the equity/cash inputs at the starting bankroll so the stake never grows.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from typing import Optional

import pandas as pd

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.contracts import (
    CexTick, Decision, MarketInfo, MarketType, Outcome, PortfolioSnapshot,
    RequestPriority, TradingMode)
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("replay")

CYCLE_MS = 250          # replay decision cadence
WINDOW_S = 300          # synthetic 5-minute markets
FILL_LATENCY_MS = 250   # pessimistic entry latency in replay


class ReplayEngine:
    def __init__(self, cfg, cex_csv: str, poly_csv: str = "", seed: int = 7,
                 stress: Optional[dict] = None):
        self.cfg = cfg
        self.cex_csv = cex_csv
        self.poly_csv = poly_csv
        self.seed = seed
        self.stress = stress or {}
        self.synthetic = not poly_csv

    # ------------------------------------------------------------------
    def _load_ticks(self) -> pd.DataFrame:
        df = pd.read_csv(self.cex_csv)
        required = {"ts_ms", "asset", "price"}
        if not required.issubset(df.columns):
            raise ValueError(f"cex csv must have columns {required}")
        if "exchange" not in df.columns:
            df["exchange"] = "replay"
        df = df.sort_values("ts_ms").reset_index(drop=True)
        return df

    def _make_markets(self, df: pd.DataFrame) -> list[dict]:
        """Rolling 5-min UP/DOWN market per asset per window."""
        markets: list[dict] = []
        t0, t1 = int(df.ts_ms.min()), int(df.ts_ms.max())
        start = (t0 // (WINDOW_S * 1000)) * (WINDOW_S * 1000)
        for asset in sorted(df.asset.unique()):
            asset_df = df[df.asset == asset]
            w = start
            while w + WINDOW_S * 1000 <= t1:
                window = asset_df[(asset_df.ts_ms >= w)
                                  & (asset_df.ts_ms < w + WINDOW_S * 1000)]
                if len(window) >= 5:
                    open_price = float(window.iloc[0].price)
                    close_price = float(window.iloc[-1].price)
                    mid = f"replay-{asset}-{w}"
                    markets.append({
                        "market": MarketInfo(
                            market_id=mid, condition_id=mid,
                            title=f"{asset} Up or Down (replay window)",
                            asset=asset, market_type=MarketType.UP_DOWN,
                            direction_up_means_yes=True,
                            yes_token_id=f"{mid}-yes", no_token_id=f"{mid}-no",
                            expiry_ts_ms=w + WINDOW_S * 1000, tick_size=0.01,
                            min_order_size_usd=1.0, active=True,
                            liquidity_usd=1000.0, mapping_confidence=100.0),
                        "open_price": open_price, "close_price": close_price,
                        "resolved_up": close_price > open_price})
                w += WINDOW_S * 1000
        return markets

    # ------------------------------------------------------------------
    def execute(self, fixed_size: bool = False) -> dict:
        return asyncio.run(self._run(fixed_size))

    async def _run(self, fixed_size: bool) -> dict:
        from poly_alpha_sniper.backtest.synthetic_poly_odds import SyntheticPolyOdds
        from poly_alpha_sniper.data.cex_state import CexState
        from poly_alpha_sniper.data.orderbook_state import OrderbookStore
        from poly_alpha_sniper.deterministic_intelligence.adaptive_aggression import AdaptiveAggression
        from poly_alpha_sniper.deterministic_intelligence.balanced_alpha_gate import BalancedAlphaGate
        from poly_alpha_sniper.deterministic_intelligence.final_decision_engine import FinalDecisionEngine
        from poly_alpha_sniper.deterministic_intelligence.signal_sanity_checker import check_signal
        from poly_alpha_sniper.deterministic_intelligence.trade_frequency_controller import TradeFrequencyController
        from poly_alpha_sniper.execution.order_lifecycle import OrderLifecycle
        from poly_alpha_sniper.execution.order_manager import OrderManager
        from poly_alpha_sniper.execution.order_side import shares_for_usd
        from poly_alpha_sniper.execution.order_validator import validate_order
        from poly_alpha_sniper.execution.sell_executor import SellExecutor
        from poly_alpha_sniper.execution.simulator import SimulatedClobClient
        from poly_alpha_sniper.execution.smart_limit_pricer import price_entry
        from poly_alpha_sniper.portfolio.positions import Portfolio
        from poly_alpha_sniper.risk.kill_switch import KillSwitch
        from poly_alpha_sniper.risk.panic_mode import PanicMode
        from poly_alpha_sniper.risk.risk_manager import RiskManager
        from poly_alpha_sniper.strategy.exit_engine import ExitEngine
        from poly_alpha_sniper.strategy.market_quality_score import MarketQualityScorer
        from poly_alpha_sniper.strategy.probability_model import ProbabilityModel
        from poly_alpha_sniper.strategy.shock_detector import ShockDetector
        from poly_alpha_sniper.strategy.signal_engine import SignalEngine
        import uuid
        from poly_alpha_sniper.core.contracts import OrderRequest

        df = self._load_ticks()
        market_specs = self._make_markets(df)
        clock = SimClock(int(df.ts_ms.min()))
        starting = self.cfg.risk.starting_bankroll_usd

        cex_state = CexState(self.cfg, clock)
        books = OrderbookStore(self.cfg, clock)
        odds = SyntheticPolyOdds(seed=self.seed,
                                 lag_ms=int(self.stress.get("lag_ms", 1500)),
                                 base_spread=0.02 + self.stress.get("spread_add", 0.0),
                                 noise_bp=30.0)
        kill, panic = KillSwitch(), PanicMode()
        portfolio = Portfolio(self.cfg, clock)
        risk = RiskManager(self.cfg, clock, kill, panic)
        client = SimulatedClobClient(clock, books.get, fill_latency_ms=int(
            FILL_LATENCY_MS * self.stress.get("latency_mult", 1.0)))
        client.set_balance(starting)
        order_manager = OrderManager(self.cfg, clock, client, OrderLifecycle())
        sell_executor = SellExecutor(self.cfg, clock, order_manager)
        shock_detector = ShockDetector(self.cfg, clock)
        prob_model = ProbabilityModel(self.cfg)
        quality = MarketQualityScorer(self.cfg)
        signal_engine = SignalEngine(self.cfg, clock, prob_model, quality)
        gate = BalancedAlphaGate(self.cfg)
        aggression = AdaptiveAggression(self.cfg, clock)
        freq = TradeFrequencyController(self.cfg, clock)
        final = FinalDecisionEngine(self.cfg)
        exit_engine = ExitEngine(self.cfg, clock)

        mode = TradingMode.SIMULATION
        trades: list[dict] = []
        predictions: list[dict] = []
        rejects: Counter = Counter()
        entry_meta: dict[str, dict] = {}   # token_id -> entry context
        spec_by_id = {s["market"].market_id: s for s in market_specs}
        active: list[dict] = []
        pending_specs = sorted(market_specs, key=lambda s: s["market"].expiry_ts_ms)
        next_cycle = clock.now_ms()

        def snap() -> PortfolioSnapshot:
            s = portfolio.snapshot(clock.now_ms())
            if fixed_size:
                # fixed-size run: sizing sees a frozen bankroll
                s.equity_usd = starting
                s.available_cash_usd = max(s.available_cash_usd, 0.0) \
                    if s.available_cash_usd < starting else starting
            return s

        def vol_for(asset: str) -> float:
            st = cex_state.stats(asset)
            return st.volatility if st else 0.0

        async def refresh_books(now: int) -> None:
            for entry in active:
                m = entry["market"]
                yes, no = odds.books_for(m, entry["open_price"], now, vol_for(m.asset))
                books.update_snapshot(yes)
                books.update_snapshot(no)

        async def settle_expired(now: int) -> None:
            for entry in [e for e in active if e["market"].expiry_ts_ms <= now]:
                active.remove(entry)
                m = entry["market"]
                for token, won_if_up in ((m.yes_token_id, True), (m.no_token_id, False)):
                    pos = portfolio.get(token)
                    if pos is None:
                        continue
                    won = entry["resolved_up"] == won_if_up
                    meta = entry_meta.pop(token, {})
                    realized = portfolio.settle_resolution(token, won, now)
                    if realized is not None:
                        trades.append(_trade_row(now, m, pos, realized, meta,
                                                 aggression.current.value,
                                                 reason="RESOLUTION"))
                        freq.record_result(m.asset, m.market_id, realized > 0)
                        aggression.record_trade(realized, meta.get("expected_edge", 0.0),
                                                realized / max(meta.get("size_usd", 1), 1e-9),
                                                100.0)
                # hypothetical B outcomes resolve here too
                for p in predictions:
                    if (p.get("market_id") == m.market_id
                            and p.get("hypothetical_pending")):
                        won = entry["resolved_up"] == (p["side"] == "BUY_YES")
                        stake = 1.0
                        entry_price = p["entry_price"]
                        p["hypothetical_pnl"] = round(
                            stake / entry_price * (1.0 - entry_price) if won
                            else -stake, 4)
                        p["hypothetical_pending"] = False

        async def manage_exits(now: int) -> None:
            for pos in portfolio.open_positions():
                m_spec = spec_by_id.get(pos.market_id)
                if m_spec is None:
                    continue
                m = m_spec["market"]
                book = books.get(pos.token_id)
                if book is not None:
                    portfolio.mark(pos.token_id, book.best_bid, book.best_ask)
                st = cex_state.stats(m.asset)
                fair = prob_model.fair(st, m, books.get(m.yes_token_id),
                                       books.get(m.no_token_id), now) if st else None
                decision = exit_engine.evaluate(pos, m, book, fair,
                                                cex_state.multi_view(m.asset),
                                                portfolio.snapshot(now),
                                                panic=panic.is_active,
                                                kill=kill.is_active)
                if not decision.should_exit or book is None:
                    continue
                try:
                    record = await sell_executor.execute_exit(pos, decision, book)
                except ValueError:
                    trades and trades[-1].update(failed_sell=True)
                    continue
                if record.filled_shares > 0:
                    price = record.avg_fill_price
                    pnl = record.filled_shares * (price - pos.avg_entry_price)
                    meta = entry_meta.get(pos.token_id, {})
                    from poly_alpha_sniper.core.contracts import FillRecord, OrderSide
                    fill = FillRecord(order_id=record.order_id, token_id=pos.token_id,
                                      market_id=pos.market_id,
                                      side=OrderSide.SELL_YES if pos.outcome == Outcome.YES
                                      else OrderSide.SELL_NO,
                                      price=price, size_shares=record.filled_shares,
                                      ts_ms=now)
                    portfolio.apply_fill(fill)
                    portfolio.record_trade_result(pnl > 0)
                    freq.record_result(m.asset, m.market_id, pnl > 0)
                    aggression.record_trade(pnl, meta.get("expected_edge", 0.0),
                                            pnl / max(meta.get("size_usd", 1), 1e-9),
                                            90.0)
                    if portfolio.get(pos.token_id) is None:
                        entry_meta.pop(pos.token_id, None)
                    trades.append(_trade_row(now, m, pos, pnl, meta,
                                             aggression.current.value,
                                             reason=decision.reason.value
                                             if decision.reason else ""))

        async def scan_entries(now: int) -> None:
            for asset in self.cfg.assets:
                view = cex_state.multi_view(asset)
                if view is None:
                    continue
                shock = shock_detector.detect(view)
                if shock is None:
                    continue
                candidates = [e for e in active if e["market"].asset == asset
                              and self._in_window(e["market"], now)]
                for entry in candidates[:2]:
                    await try_enter(shock, entry, view, now)

        async def try_enter(shock, entry, view, now) -> None:
            m = entry["market"]
            yes_book, no_book = books.get(m.yes_token_id), books.get(m.no_token_id)
            signal = signal_engine.build_signal(shock, m, view, yes_book, no_book, now)
            if signal is None:
                return
            ok, _issues = check_signal(signal)
            if not ok:
                return
            can, _why = freq.can_trade(m.asset, m.market_id, mode)
            s = snap()
            checks = {
                "mapping_clear": True, "cex_fresh": view.primary.fresh,
                "book_fresh": yes_book is not None, "best_bid_ask": True,
                "spread_ok": (yes_book.spread or 1) <= self.cfg.microstructure.max_spread,
                "no_panic": not panic.is_active, "no_kill_switch": not kill.is_active,
                "frequency_ok": can, "expiry_window_ok": True,
            }
            g = gate.evaluate(signal, aggression.current, mode, checks, s)
            risk_d = risk.check_entry(signal, s, mode)
            book = yes_book if signal.side.outcome == Outcome.YES else no_book
            price, _note = price_entry(book, signal.side,
                                       self.cfg.execution_pricing.default_mode, self.cfg)
            validation = None
            req = None
            if risk_d.approved and price:
                req = OrderRequest(order_id=str(uuid.uuid4()),
                                   token_id=m.token_for(signal.side.outcome),
                                   market_id=m.market_id, side=signal.side,
                                   price=price,
                                   size_shares=shares_for_usd(risk_d.size_usd, price),
                                   size_usd=risk_d.size_usd, tier=signal.tier,
                                   signal_id=signal.signal_id)
                validation = validate_order(req, book, m, s, self.cfg, now)
            else:
                from poly_alpha_sniper.core.contracts import RiskDecision
                validation = RiskDecision(False, reject_reason=risk_d.reject_reason
                                          or "no_price")
            decision = final.decide(signal, g, risk_d, validation, mode)
            pred = {"ts_ms": now, "market_id": m.market_id, "asset": m.asset,
                    "tier": g.tier.value, "decision": decision.value,
                    "side": signal.side.value, "entry_price": signal.edge.market_price,
                    "edge": signal.edge.edge_after_slippage,
                    "aggression_mode": aggression.current.value,
                    "reject_reason": (risk_d.reject_reason or validation.reject_reason
                                      or (g.reason if g.hard_reject else ""))
                    if decision == Decision.REJECT else ""}
            if g.tier.value == "B" and decision == Decision.SHADOW_ONLY:
                pred["hypothetical_pending"] = True
                pred["hypothetical_pnl"] = None
            predictions.append(pred)
            if decision == Decision.REJECT:
                rejects[pred["reject_reason"] or "REJECTED_OTHER"] += 1
                return
            if decision not in (Decision.APPROVE, Decision.SHADOW_ONLY) or req is None:
                return
            if decision == Decision.SHADOW_ONLY and not (
                    g.decision in (Decision.APPROVE,) or mode in (
                        TradingMode.SIMULATION, TradingMode.SHADOW_LIVE)):
                return
            if g.decision not in (Decision.APPROVE, Decision.SHADOW_ONLY):
                return
            if g.decision == Decision.SHADOW_ONLY and g.tier.value == "B":
                return  # B stays hypothetical in NORMAL — counterfactual tracked
            # stress: random missed fills
            miss_p = self.stress.get("miss_fill_prob", 0.0)
            if miss_p > 0:
                import numpy as np
                rng = np.random.RandomState(self.seed + now % 1000)
                if rng.rand() < miss_p:
                    trades.append({"ts_ms": now, "market_id": m.market_id,
                                   "asset": m.asset, "pnl": 0.0, "size_usd": 0.0,
                                   "tier": g.tier.value, "failed_fill": True,
                                   "aggression_mode": aggression.current.value,
                                   "direction": signal.direction.value,
                                   "outcome": signal.side.outcome.value})
                    return
            try:
                record = await order_manager.submit(req, book)
            except ValueError:
                return
            if record.filled_shares > 0:
                from poly_alpha_sniper.core.contracts import FillRecord
                fill = FillRecord(order_id=record.order_id, token_id=req.token_id,
                                  market_id=m.market_id, side=req.side,
                                  price=record.avg_fill_price,
                                  size_shares=record.filled_shares, ts_ms=now)
                portfolio.apply_fill(fill, m)
                pos = portfolio.get(req.token_id)
                if pos is not None:
                    pos.tier = signal.tier
                freq.record_entry(m.asset, m.market_id)
                slip_bps = abs(record.avg_fill_price - signal.edge.market_price) \
                    / max(signal.edge.market_price, 1e-9) * 10_000
                entry_meta[req.token_id] = {
                    "expected_edge": signal.edge.edge_after_slippage,
                    "size_usd": req.size_usd, "entry_ts_ms": now,
                    "entry_slippage_bps": slip_bps, "tier": signal.tier.value,
                    "direction": signal.direction.value,
                    "outcome": signal.side.outcome.value}

        # ---------------- main loop ----------------
        for row in df.itertuples(index=False):
            now = int(row.ts_ms)
            clock.set_ms(now)
            tick = CexTick(asset=str(row.asset), exchange=str(row.exchange),
                           price=float(row.price), ts_ms=now, recv_ts_ms=now)
            cex_state.update(tick)
            odds.observe_price(tick.asset, now, tick.price)
            while pending_specs and pending_specs[0]["market"].expiry_ts_ms - WINDOW_S * 1000 <= now:
                active.append(pending_specs.pop(0))
            if now >= next_cycle:
                next_cycle = now + CYCLE_MS
                await refresh_books(now)
                await settle_expired(now)
                await manage_exits(now)
                await scan_entries(now)
                aggression.evaluate(portfolio.snapshot(now))
        # final settlement pass
        clock.advance_ms(WINDOW_S * 1000)
        await refresh_books(clock.now_ms())
        await settle_expired(clock.now_ms())

        final_snap = portfolio.snapshot(clock.now_ms())
        from poly_alpha_sniper.backtest.metrics import _base_stats
        return {"trades": trades, "predictions": predictions, "rejects": rejects,
                "equity": final_snap.equity_usd, "starting": starting,
                "fixed_size": fixed_size, "synthetic_odds": self.synthetic,
                **_base_stats(trades, starting)}

    def _in_window(self, market: MarketInfo, now_ms: int) -> bool:
        tte = market.seconds_to_expiry(now_ms)
        u = self.cfg.ultra_short_expiry
        return u.min_time_to_expiry_seconds <= tte <= u.max_time_to_expiry_seconds

    # ------------------------------------------------------------------
    def run_both(self) -> dict:
        """Fixed-size + compounded runs + combined metrics."""
        from poly_alpha_sniper.backtest.metrics import compute_metrics
        fixed = self.execute(fixed_size=True)
        compounded = self.execute(fixed_size=False)
        metrics = compute_metrics(
            compounded["trades"], compounded["predictions"], compounded["rejects"],
            compounded["starting"], fixed_result=fixed, compounded_result=compounded,
            synthetic_odds=self.synthetic)
        return {"fixed": fixed, "compounded": compounded, "metrics": metrics}


def _trade_row(now: int, market: MarketInfo, pos, pnl: float, meta: dict,
               aggression_mode: str, reason: str) -> dict:
    hold_s = (now - meta.get("entry_ts_ms", now)) / 1000.0
    return {"ts_ms": now, "market_id": market.market_id, "asset": market.asset,
            "pnl": round(pnl, 4), "size_usd": meta.get("size_usd", 0.0),
            "tier": meta.get("tier", pos.tier.value if hasattr(pos.tier, "value") else "?"),
            "aggression_mode": aggression_mode,
            "direction": meta.get("direction", "?"),
            "outcome": meta.get("outcome", pos.outcome.value),
            "expected_edge": meta.get("expected_edge", 0.0),
            "entry_slippage_bps": meta.get("entry_slippage_bps", 0.0),
            "exit_slippage_bps": 0.0, "fill_quality": 90.0,
            "hold_s": hold_s, "exit_reason": reason}
