"""Application orchestrator.

Wires every subsystem together and runs the trade loop. The SAME pipeline
objects (signal engine, gate, risk, sizer, validator, exit engine) are used in
all modes — only the execution client differs:

    simulation  -> SimulatedClobClient (synthetic fills, no network orders)
    shadow_live -> ShadowClobClient    (real data, hypothetical fills, no orders)
    live_micro  -> LiveExecutor client (real orders, all gates enforced)
    live_full   -> LiveExecutor client (real orders, config-bounded sizing)

ASSUMPTIONS:
- Entry evaluation is shock-driven: no CEX shock, no entry (lag-arb thesis).
- One in-flight entry per market; exits always outrank entries for API budget.
- Live mode refuses to start (raises SystemExit) if preflight/live-readiness
  fails — it never silently downgrades.
"""
from __future__ import annotations

import asyncio
import traceback
import uuid
from dataclasses import asdict
from typing import Optional

from poly_alpha_sniper.core.clock import WallClock
from poly_alpha_sniper.core.config_loader import Config, Secrets, load_config, load_secrets, PROJECT_ROOT
from poly_alpha_sniper.core.config_validator import validate_config, validate_live_env
from poly_alpha_sniper.core.contracts import (
    AggressionMode, Decision, Direction, ExitReason, GateResult, MarketInfo, OrderRequest,
    OrderSide, Outcome, PredictionRecord, RequestPriority, RejectReason, Signal, Tier,
    TradingMode,
)
from poly_alpha_sniper.core.event_bus import EventBus, Topics
from poly_alpha_sniper.core.logger import configure as configure_logging, get_logger
from poly_alpha_sniper.portfolio.position_reconciliation import (
    DEFAULT_STUCK_EXIT_CUTOFF_MS, reconcile_unexitable_position,
)
from poly_alpha_sniper.strategy.oracle_anchor import resolve_oracle_anchor, validate_oracle_anchor
from poly_alpha_sniper.strategy.oracle_ev import compute_oracle_ev
from poly_alpha_sniper.strategy.shock_near_miss import compute_shock_near_miss

log = get_logger("app")

# risk (position_sizer) and validation (order_validator) are two independent
# checks that can both reject for the same RejectReason -- when risk rejects
# BEFORE an OrderRequest is ever built, `validation` is a synthetic stand-in
# that copies risk.reject_reason but never risk.sizing_detail (see
# App._evaluate_market). Matching on reject_reason alone would silently pick
# the source with no detail dict; prefer whichever one actually carries data.
_SIZING_DETAIL_REASONS = (RejectReason.MIN_ORDER_SIZE_TOO_HIGH,
                          RejectReason.INSUFFICIENT_CASH_FOR_5_SHARES,
                          RejectReason.MAX_EXPOSURE)


def select_sizing_detail_source(risk, validation):
    if risk.reject_reason in _SIZING_DETAIL_REASONS and risk.sizing_detail:
        return risk
    if validation.reject_reason in _SIZING_DETAIL_REASONS and validation.sizing_detail:
        return validation
    return None


class App:
    def __init__(self, cfg: Optional[Config] = None, secrets: Optional[Secrets] = None):
        self.cfg = cfg or load_config()
        self.secrets = secrets or load_secrets()
        configure_logging(level=self.secrets.log_level,
                          rotation_mb=self.cfg.runtime.log_rotation_mb,
                          keep_days=self.cfg.runtime.keep_log_days)
        self.clock = WallClock()
        self.bus = EventBus()
        self.mode = TradingMode(self.cfg.mode.trading_mode)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self.paused = False
        self._pending_confirmed_sells: dict[str, int] = {}
        # positions whose exit path has been failing continuously (no book,
        # or every attempt raises) -- ts_ms of the FIRST failure seen, per
        # token; cleared once the position closes by any means. Used to
        # bound the exit_no_book retry loop (see _manage_exits).
        self._exit_stuck_since_ms: dict[str, int] = {}
        # tokens for which emergency_exit_failed panic has already been
        # activated -- avoids re-activating (and re-logging) every cycle
        # for a token already known to be failing.
        self._panic_triggered_tokens: set[str] = set()
        # runtime observability (shadow diagnostics + /status + dashboard)
        self.diag: dict = {
            "runtime_alive": True,
            "prediction_loop_iterations": 0,
            "last_prediction_loop_ts": 0,
            "last_prediction_ts": 0,
            "last_near_miss_ts": 0,
            "last_discovery_ts": 0,
            "last_block_reason": "",
            "prediction_rows_written": 0,
            "near_miss_rows_written": 0,
            "diagnostic_rows_written": 0,
            "cex_selected_source": {},       # asset -> exchange chosen as primary
            "cex_freshest_age_ms": {},       # asset -> staleness of that primary
            "cex_no_fresh_count_by_source": {},  # exchange -> times it was the
                                                  # freshest-available AND still stale
            "cex_freshness_degraded": {},    # asset -> bool, live_signal < age <= fail_closed
            "cex_source_debug": {},          # asset -> structured source/fallback debug (see
                                              # _cex_source_debug); read-only dashboard surface
            "latest_oracle_anchor": None,    # updated as soon as ANY candidate market is
                                              # discovered, independent of shock/freshness
                                              # gates -- see _scan_entries's early anchor pass
            "last_scan_snapshot": {},        # updates every scan iteration, not just signals
            "watchlist_no_shock_near_miss": [],  # bounded recent list, see shock_near_miss.py
            "candidate_book_status": {},     # most recent candidate's executable-book
                                              # freshness + direct-refresh outcome (Task A)
        }
        self._diag_throttle: dict[tuple[str, str], int] = {}
        self._feature_store_throttle: dict[str, int] = {}  # asset -> last row ts_ms

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def build(self) -> None:
        """Instantiate all subsystems (no network yet)."""
        from poly_alpha_sniper.storage.db import get_store
        from poly_alpha_sniper.storage.migrations import run_migrations
        from poly_alpha_sniper.storage.backup_manager import BackupManager
        from poly_alpha_sniper.core.rate_limit_governor import RateLimitGovernor
        from poly_alpha_sniper.core.runtime_state import RuntimeState
        from poly_alpha_sniper.data.cex_state import CexState
        from poly_alpha_sniper.data.orderbook_state import OrderbookStore
        from poly_alpha_sniper.data.latency_tracker import LatencyTracker
        from poly_alpha_sniper.data.market_cache import MarketCache
        from poly_alpha_sniper.discovery.market_discovery import MarketDiscovery
        from poly_alpha_sniper.discovery.expiry_tracker import ExpiryTracker
        from poly_alpha_sniper.strategy.shock_detector import ShockDetector
        from poly_alpha_sniper.strategy.probability_model import ProbabilityModel
        from poly_alpha_sniper.strategy.market_quality_score import MarketQualityScorer
        from poly_alpha_sniper.strategy.signal_engine import SignalEngine
        from poly_alpha_sniper.strategy.exit_engine import ExitEngine
        from poly_alpha_sniper.strategy.sell_signal_engine import SellSignalEngine
        from poly_alpha_sniper.strategy.opportunity_queue import OpportunityQueue
        from poly_alpha_sniper.deterministic_intelligence.balanced_alpha_gate import BalancedAlphaGate
        from poly_alpha_sniper.deterministic_intelligence.adaptive_aggression import AdaptiveAggression
        from poly_alpha_sniper.deterministic_intelligence.trade_frequency_controller import TradeFrequencyController
        from poly_alpha_sniper.deterministic_intelligence.final_decision_engine import FinalDecisionEngine
        from poly_alpha_sniper.deterministic_intelligence.signal_sanity_checker import check_signal
        from poly_alpha_sniper.risk.kill_switch import KillSwitch
        from poly_alpha_sniper.risk.panic_mode import PanicMode
        from poly_alpha_sniper.risk.risk_manager import RiskManager
        from poly_alpha_sniper.risk.market_filter_list import MarketFilterList
        from poly_alpha_sniper.portfolio.positions import Portfolio
        from poly_alpha_sniper.execution.order_lifecycle import OrderLifecycle
        from poly_alpha_sniper.execution.order_manager import OrderManager
        from poly_alpha_sniper.execution.sell_executor import SellExecutor
        from poly_alpha_sniper.execution.emergency_exit import EmergencyExit
        from poly_alpha_sniper.execution.cancel_manager import CancelManager
        from poly_alpha_sniper.analytics.fill_quality import FillQualityTracker
        from poly_alpha_sniper.analytics.edge_realization import EdgeRealization
        from poly_alpha_sniper.analytics.near_miss_logger import NearMissLogger
        from poly_alpha_sniper.reporting.telegram import TelegramClient

        errors = validate_config(self.cfg)
        if errors:
            raise SystemExit("Config invalid:\n- " + "\n- ".join(errors))

        self.store = get_store(self.secrets.database_url)
        run_migrations(self.store)
        self.backup_manager = BackupManager(self.cfg, self.clock, getattr(self.store, "path", ""))
        self.runtime_state = RuntimeState(clock=self.clock)
        self.governor = RateLimitGovernor(self.clock)
        self.latency = LatencyTracker()

        self.cex_state = CexState(self.cfg, self.clock)
        self.book_store = OrderbookStore(self.cfg, self.clock)
        self.market_cache = MarketCache()
        self.expiry_tracker = ExpiryTracker(self.cfg, self.clock)

        self.kill_switch = KillSwitch()
        self.panic = PanicMode()
        self.portfolio = Portfolio(self.cfg, self.clock)
        self.risk_manager = RiskManager(self.cfg, self.clock, self.kill_switch, self.panic)
        self.filter_list = MarketFilterList(self.clock)

        self.shock_detector = ShockDetector(self.cfg, self.clock)
        self.prob_model = ProbabilityModel(self.cfg)
        self.quality_scorer = MarketQualityScorer(self.cfg)
        self.signal_engine = SignalEngine(self.cfg, self.clock, self.prob_model, self.quality_scorer)
        self.gate = BalancedAlphaGate(self.cfg)
        self.aggression = AdaptiveAggression(self.cfg, self.clock)
        self.freq = TradeFrequencyController(self.cfg, self.clock)
        self.final_decision = FinalDecisionEngine(self.cfg)
        self.check_signal = check_signal
        self.opportunity_queue = OpportunityQueue(self.clock)

        self.exit_engine = ExitEngine(self.cfg, self.clock)
        self.sell_signal_engine = SellSignalEngine(self.exit_engine)

        self.fill_quality = FillQualityTracker()
        self.edge_realization = EdgeRealization()
        self.near_miss = NearMissLogger()
        self.telegram = TelegramClient(self.secrets, self.cfg, self.clock)

        # Execution client per mode
        self.client = self._build_client()
        self.lifecycle = OrderLifecycle()
        self.order_manager = OrderManager(self.cfg, self.clock, self.client, self.lifecycle,
                                          on_update=self._on_order_update)
        self.sell_executor = SellExecutor(self.cfg, self.clock, self.order_manager)
        self.cancel_manager = CancelManager(self.order_manager)
        self.emergency = EmergencyExit(self.cfg, self.order_manager, self.sell_executor, self.cancel_manager)

        # data feeds are attached in run() (network)
        self.discovery: Optional[MarketDiscovery] = None
        self.mirror = None
        self.controls = None

        # panic wiring: cancel + freeze on activation
        try:
            self.panic.on_activate(self._on_panic)
        except (AttributeError, TypeError):
            pass

    def _build_client(self):
        from poly_alpha_sniper.execution.simulator import SimulatedClobClient
        from poly_alpha_sniper.execution.live_executor import ShadowClobClient, LiveExecutor

        book_provider = lambda token_id: self.book_store.get(token_id)  # noqa: E731
        if self.mode == TradingMode.SIMULATION:
            client = SimulatedClobClient(self.clock, book_provider)
            if hasattr(client, "set_balance"):
                client.set_balance(self.cfg.risk.starting_bankroll_usd)
            return client
        if self.mode == TradingMode.SHADOW_LIVE:
            client = ShadowClobClient(self.clock, book_provider)
            if hasattr(client, "set_balance"):
                client.set_balance(self.cfg.risk.starting_bankroll_usd)
            return client
        # live modes: config/env gates first, client built during startup after
        # preflight + readiness. Placeholder raises if used early.
        env_errors = validate_live_env(self.cfg, self.secrets)
        if env_errors:
            raise SystemExit("LIVE BLOCKED — env/config gates failed:\n- " + "\n- ".join(env_errors))
        self._live_executor = LiveExecutor(
            self.cfg, self.secrets, self.clock, None,
            live_gates_checker=lambda: (True, []))
        return self._live_executor.build_client()

    # ------------------------------------------------------------------
    # Startup / shutdown
    # ------------------------------------------------------------------
    async def startup(self, offline_ok: bool = False) -> None:
        from poly_alpha_sniper.core.startup_preflight import run_preflight
        from poly_alpha_sniper.core.process_lock import ProcessLock
        from poly_alpha_sniper.core.live_readiness import check_live_readiness
        from poly_alpha_sniper.reporting.telegram import format_preflight

        self.process_lock = ProcessLock()
        self.process_lock.acquire(self.mode.value)

        try:
            await self._reconcile_stale_shadow_orders()
            pf = await run_preflight(self.cfg, self.secrets, offline_ok=offline_ok)
            try:
                await self.telegram.send(format_preflight(pf), critical=not pf.ok)
            except Exception:
                log.warning("preflight_telegram_failed")
            if pf.cex_health.get("status") not in ("HEALTHY", "SKIPPED", None):
                log.warning("cex_health_degraded", extra={"extra": pf.cex_health})
            if not pf.ok:
                failed = [c for c in pf.checks if not c[1]]
                raise SystemExit("Preflight failed:\n- "
                                 + "\n- ".join(f"{n}: {d}" for n, _, d in failed))

            if self.mode.is_live:
                reconciled = await self._reconcile_account()
                ok, gates = await check_live_readiness(
                    self.cfg, self.secrets, self.client, self.panic, self.kill_switch,
                    telegram_ok=await self._telegram_ok(), reconciled=reconciled)
                if not ok:
                    raise SystemExit("LIVE BLOCKED — readiness gates failed:\n- "
                                     + "\n- ".join(gates))
                log.info("live_readiness_passed", extra={"extra": {"mode": self.mode.value}})
        except BaseException:
            # failed startup must not leak aiohttp sessions or hold the lock
            await self._close_clients()
            try:
                self.process_lock.release()
            except Exception:  # noqa: BLE001
                pass
            raise

    async def _close_clients(self) -> None:
        """Close every network client that may hold an aiohttp session."""
        for attr in ("telegram", "gamma", "clob_public"):
            obj = getattr(self, attr, None)
            if obj is not None and hasattr(obj, "close"):
                try:
                    await obj.close()
                except Exception:  # noqa: BLE001
                    pass

    async def _telegram_ok(self) -> bool:
        if not self.cfg.telegram.enabled:
            return True
        try:
            return await self.telegram.send("Poly Alpha Sniper: live readiness Telegram check.", critical=True)
        except Exception:
            return False

    async def _reconcile_account(self) -> bool:
        from poly_alpha_sniper.execution.fill_reconciler import FillReconciler
        try:
            balance = await self.client.get_balance_usd()
            exch_positions = await self.client.get_positions()
            exch_orders = await self.client.get_open_orders()
        except Exception as exc:
            log.error("reconcile_fetch_failed", extra={"extra": {"error": repr(exc)}})
            return False
        # adopt exchange truth at startup
        if hasattr(self.portfolio, "set_cash"):
            self.portfolio.set_cash(balance)
        rec = FillReconciler().reconcile(
            local_orders=[], exchange_orders=exch_orders,
            local_positions=self.portfolio.open_positions(),
            exchange_positions=exch_positions,
            balance_local=balance, balance_exchange=balance)
        self._insert("reconciliation_events", {
            "ts_ms": self.clock.now_ms(), "ok": int(rec.ok),
            "mismatches": ";".join(rec.mismatches)})
        if not rec.ok and self.portfolio.open_positions():
            return False
        return True

    async def _on_order_update(self, record) -> None:
        """Persist post-insert order state transitions (e.g. TIF cancel, stale-sweep
        cancel). The initial `_insert("orders", ...)` in `_execute_entry`/`_execute_exit`
        captures the order at submit time only; without this, any later state change
        (OPEN -> CANCELLED, fills completing async) never reaches the DB and the row
        is left showing a stale, misleading state forever."""
        try:
            self.store.execute(
                "UPDATE orders SET state=?, filled_shares=?, avg_fill_price=?, "
                "updated_ts_ms=?, error=?, exchange_order_id=? WHERE order_id=?",
                (record.state.value if hasattr(record.state, "value") else str(record.state),
                 record.filled_shares, record.avg_fill_price, self.clock.now_ms(),
                 record.error or "", record.exchange_order_id or "", record.order_id))
        except Exception as exc:
            log.error("order_update_persist_failed", extra={"extra": {
                "order_id": record.order_id, "error": repr(exc)}})

    async def _reconcile_stale_shadow_orders(self) -> None:
        """Startup safety net -- see storage/order_reconciliation.py for why this
        exists. Backs up the DB first, never deletes rows, idempotent on re-run."""
        from poly_alpha_sniper.storage.order_reconciliation import reconcile_stale_orders
        try:
            result = reconcile_stale_orders(
                self.store, self.backup_manager, self.clock.now_ms(), insert_fn=self._insert)
        except Exception as exc:
            log.error("stale_order_reconciliation_failed", extra={"extra": {"error": repr(exc)}})
            return
        if result["touched"]:
            log.warning("stale_orders_reconciled", extra={"extra": {
                "count": len(result["touched"]), "order_ids": result["touched"],
                "backup": result["backup_path"]}})

    async def _order_hygiene_loop(self) -> None:
        """Defense-in-depth: periodically cancel any order still resting well past
        its intended time-in-force, in case the primary per-order TIF-cancel timer
        (OrderManager._schedule_tif_cancel) didn't fire. Runs against the LIVE
        in-memory OrderManager, so this also persists to the DB via on_update."""
        sweep_older_than_ms = max(5000, self.cfg.execution_pricing.cancel_if_not_filled_ms * 5)
        while not self._stop.is_set():
            await asyncio.sleep(5.0)
            try:
                n = await self.cancel_manager.cancel_stale(sweep_older_than_ms, self.clock.now_ms())
                if n:
                    log.warning("order_hygiene_cancelled_stale", extra={"extra": {"count": n}})
            except Exception as exc:
                log.error("order_hygiene_error", extra={"extra": {"error": repr(exc)}})

    async def shutdown(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        try:
            if self.mode.is_live:
                await self.cancel_manager.cancel_all_priority()
        except Exception:
            log.error("shutdown_cancel_failed")
        try:
            self.runtime_state.update(mode=self.mode.value, panic_active=self.panic.is_active)
            self.runtime_state.save()
        except Exception:
            pass
        try:
            self.process_lock.release()
        except Exception:
            pass
        for closer in ("mirror", "discovery"):
            obj = getattr(self, closer, None)
            if obj and hasattr(obj, "stop"):
                try:
                    await obj.stop()
                except Exception:
                    pass
        if getattr(self, "feed", None) is not None:
            try:
                await self.feed.stop()
            except Exception:
                pass
        await self._close_clients()
        log.info("shutdown_complete")

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    async def run(self, offline_ok: bool = False) -> None:
        from poly_alpha_sniper.connectors.multi_cex_feed import MultiCexFeed
        from poly_alpha_sniper.connectors.polymarket_gamma import PolymarketGamma
        from poly_alpha_sniper.connectors.polymarket_clob_public import PolymarketClobPublic
        from poly_alpha_sniper.connectors.polymarket_ws import PolymarketWS
        from poly_alpha_sniper.data.orderbook_mirror import OrderbookMirror
        from poly_alpha_sniper.discovery.market_discovery import MarketDiscovery
        from poly_alpha_sniper.reporting.telegram_controls import TelegramControls

        await self.startup(offline_ok=offline_ok)

        self.feed = MultiCexFeed(self.cfg, self.clock, self.cex_state)
        self.gamma = PolymarketGamma(self.cfg)
        self.clob_public = PolymarketClobPublic(self.cfg)
        self.poly_ws = PolymarketWS(self.cfg, self.clock, self._on_book)
        self.mirror = OrderbookMirror(self.cfg, self.clock, self.poly_ws, self.clob_public, self.book_store)
        self.discovery = MarketDiscovery(
            self.cfg, self.clock, self.gamma.get_markets,
            clob_fetcher=self.clob_public.get_sampling_markets,
            event_hydrator=self.gamma.get_event_for_market)

        await self.feed.start()
        if hasattr(self.mirror, "start"):
            await self.mirror.start()

        if self.cfg.telegram.enabled and self.cfg.telegram.controls_enabled:
            self.controls = TelegramControls(self.secrets, self.cfg, self.clock, self._control_actions())
            self._tasks.append(asyncio.create_task(self.controls.run()))

        self._tasks.append(asyncio.create_task(self._discovery_loop()))
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))
        self._tasks.append(asyncio.create_task(self._backup_loop()))
        self._tasks.append(asyncio.create_task(self._health_loop()))
        self._tasks.append(asyncio.create_task(self._order_hygiene_loop()))

        await self.telegram.send(
            f"🟢 poly_alpha_sniper started | mode={self.mode.value} | dry_run={self.cfg.mode.dry_run} "
            f"| aggression={self.aggression.current.value} | bankroll=${self.cfg.risk.starting_bankroll_usd}")
        log.info("app_started", extra={"extra": {"mode": self.mode.value}})

        try:
            await self._trade_loop()
        finally:
            await self.shutdown()

    async def _on_book(self, snap) -> None:
        self.book_store.update_snapshot(snap)

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _discovery_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if await self.governor.acquire("gamma", RequestPriority.DISCOVERY):
                    markets = await self.discovery.refresh()
                    self.market_cache.upsert(markets)
                    # per-asset current/next anchor autopsy (diagnostics only)
                    self.diag["oracle_anchor_autopsy"] = getattr(
                        self.discovery, "anchor_autopsy", {}) or {}
                    now = self.clock.now_ms()
                    for m in markets:
                        self.expiry_tracker.track(m) if hasattr(self.expiry_tracker, "track") else None
                        if hasattr(self.mirror, "track_market"):
                            await _maybe_await(self.mirror.track_market(m))
                    # entry-window tokens refresh first in the REST fallback
                    if hasattr(self.mirror, "set_priority_tokens"):
                        priority = [t for m in markets
                                    if m.seconds_to_expiry(now) <= 400
                                    for t in (m.yes_token_id, m.no_token_id)]
                        self.mirror.set_priority_tokens(priority)
                    # untrack expired markets or the tracked set grows forever
                    for dead in self.market_cache.prune(now):
                        if hasattr(self.mirror, "untrack_market"):
                            self.mirror.untrack_market(dead)
                    self.diag["last_discovery_ts"] = now
                    self._insert("market_snapshots", {
                        "ts_ms": now, "n_markets": len(markets),
                        "rejects": str(getattr(self.discovery, "reject_stats", {}))[:2000]})
            except Exception as exc:
                log.error("discovery_error", extra={"extra": {"error": repr(exc)}})
            await asyncio.sleep(self.cfg.polymarket.refresh_markets_seconds)

    def _diag_summary(self) -> dict:
        fresh, total = self._book_freshness()
        now = self.clock.now_ms()
        return {
            **self.diag,
            "runtime_alive": True,
            "prediction_loop_alive": (now - self.diag["last_prediction_loop_ts"]) < 5000,
            "discovered_markets": len(self.market_cache),
            "fresh_books": fresh, "total_books": total,
            "cex_ticks_by_asset": dict(self.cex_state.tick_counts),
            "price_windows_ready": self.cex_state.windows_ready_by_asset(),
            "latest_prices": self.cex_state.latest_prices(),
            "last_cex_tick_ts_by_asset": self.cex_state.last_recv_by_asset(),
            "cex_live_staleness_budget_ms": self.cfg.cex.max_cex_staleness_ms,
            "cex_shadow_diag_budget_ms": self.cfg.cex.shadow_diagnostic_staleness_ms,
            "snapshot_ts_ms": now,
        }

    async def _heartbeat_loop(self) -> None:
        beats = 0
        while not self._stop.is_set():
            try:
                summary = self._diag_summary()
                self.runtime_state.update(
                    mode=self.mode.value, paused=self.paused,
                    panic_active=self.panic.is_active, kill_active=self.kill_switch.is_active,
                    aggression_mode=self.aggression.current.value,
                    diagnostics=summary)
                self.runtime_state.heartbeat()
                beats += 1
                if beats % 2 == 0:  # ~every 30 s with 15 s heartbeats
                    log.info("runtime_diagnostics", extra={"extra": summary})
            except Exception as exc:
                log.error("heartbeat_error", extra={"extra": {"error": repr(exc)}})
            await asyncio.sleep(self.cfg.runtime.heartbeat_seconds)

    async def _backup_loop(self) -> None:
        if not self.cfg.runtime.backup_database_enabled:
            return
        while not self._stop.is_set():
            await asyncio.sleep(self.cfg.runtime.backup_interval_minutes * 60)
            try:
                dest = self.backup_manager.backup_now()
                self._insert("database_backups", {"ts_ms": self.clock.now_ms(), "path": str(dest)})
            except Exception as exc:
                log.error("backup_error", extra={"extra": {"error": repr(exc)}})

    async def _health_loop(self) -> None:
        from poly_alpha_sniper.reporting.health_report import build_health
        interval = max(1.0, self.cfg.telegram.send_health_every_minutes * 60)
        while not self._stop.is_set():
            await asyncio.sleep(interval)
            try:
                snap = self.portfolio.snapshot(self.clock.now_ms())
                health = build_health(
                    runtime_state=self.runtime_state.state if hasattr(self.runtime_state, "state") else {},
                    cex_health=self.feed.health() if hasattr(self, "feed") else {},
                    poly_health={"books": True},
                    portfolio=snap, governor_usage=self.governor.usage(),
                    panic=self.panic.is_active, kill=self.kill_switch.is_active,
                    mode=self.mode.value)
                self._insert("health_logs", {"ts_ms": self.clock.now_ms(),
                                             "report": str(health)[:4000]})
                if self.cfg.telegram.enabled:
                    text = health.get("text") if isinstance(health, dict) else str(health)
                    await self.telegram.send(text or "health: ok")
            except Exception as exc:
                log.error("health_error", extra={"extra": {"error": repr(exc)}})

    # ------------------------------------------------------------------
    # Trade loop — shared decision pipeline
    # ------------------------------------------------------------------
    async def _trade_loop(self) -> None:
        cycle_s = self.cfg.strategy.cycle_ms / 1000.0
        while not self._stop.is_set():
            t0 = self.clock.now_ms()
            try:
                await self._manage_exits()
                if not self.paused and not self.panic.is_active and not self.kill_switch.is_active:
                    await self._scan_entries()
            except Exception as exc:
                log.error("trade_loop_error", extra={"extra": {
                    "error": repr(exc), "trace": traceback.format_exc()[-1500:]}})
                self._insert("errors", {"ts_ms": self.clock.now_ms(), "where": "trade_loop",
                                        "error": repr(exc)})
            elapsed = (self.clock.now_ms() - t0) / 1000.0
            await asyncio.sleep(max(0.0, cycle_s - elapsed))

    async def _scan_entries(self) -> None:
        now = self.clock.now_ms()
        self.diag["prediction_loop_iterations"] += 1
        self.diag["last_prediction_loop_ts"] = now
        entry_markets = [m for m in self.market_cache.active_markets(now)
                         if self._in_entry_window(m, now)]
        if not entry_markets:
            self._record_candidate_book_status(
                asset="", status="NOT_EVALUATED", now_ms=now,
                earlier_gate_reason="NO_CANDIDATE",
                direct_refresh_attempted=False,
                direct_refresh_result="not_reached_book_stage")
        for asset in self.cfg.assets:
            view = self.cex_state.multi_view(asset)
            candidate_markets = [m for m in entry_markets if m.asset == asset]

            # Oracle anchor visibility must not depend on CEX freshness or a
            # shock ever firing -- price_to_beat comes from Polymarket market
            # metadata (see strategy/oracle_anchor.py), not the CEX feed.
            # Diagnostic-only: does not change the shock-gated entry decision
            # below, and does not write oracle_anchor_log (that history table
            # is reserved for rows tied to an actual gate outcome).
            anchor = None
            if self.cfg.oracle_ev.enabled and candidate_markets:
                anchor = self._resolve_and_record_oracle_anchor(candidate_markets[0], view, now)

            freshness = self._classify_cex_freshness(view)
            block_reason = None

            if freshness == "no_data":
                block_reason = "rejected_by_no_fresh_cex_price"
                self._shadow_diag(asset, "", block_reason, "no ticks received yet")
            else:
                # record which source is currently driving decisions for this
                # asset, and how stale it is, on EVERY scan (not just rejections)
                self.diag["cex_selected_source"][asset] = view.primary.exchange
                self.diag["cex_freshest_age_ms"][asset] = view.primary.staleness_ms
                self.diag["cex_freshness_degraded"][asset] = (freshness == "degraded")
                self.diag["cex_source_debug"][asset] = self._cex_source_debug(view, freshness, now)
                if freshness == "fail_closed":
                    # the freshest source we have is STILL beyond budget ->
                    # every usable exchange is stale right now, not a
                    # selection error
                    by_source = self.diag["cex_no_fresh_count_by_source"]
                    by_source[view.primary.exchange] = by_source.get(view.primary.exchange, 0) + 1
                    block_reason = "rejected_by_no_fresh_cex_price"
                    self._shadow_diag(asset, "", block_reason,
                                      self._cex_source_detail(view), view)

            self._update_scan_snapshot(asset, view, candidate_markets, anchor,
                                       freshness, block_reason, now)
            if block_reason is not None:
                if candidate_markets:
                    self._record_candidate_book_status(
                        asset=asset, market=candidate_markets[0], status="NOT_REACHED_BOOK_STAGE",
                        now_ms=now, earlier_gate_reason=block_reason,
                        direct_refresh_attempted=False,
                        direct_refresh_result="not_reached_book_stage")
                continue

            if not self.cex_state.window_ready(asset):
                self._shadow_diag(asset, "", "rejected_by_price_window_not_ready",
                                  f"ticks={self.cex_state.tick_counts.get(asset, 0)}", view)
                if candidate_markets:
                    self._record_candidate_book_status(
                        asset=asset, market=candidate_markets[0], status="NOT_REACHED_BOOK_STAGE",
                        now_ms=now, earlier_gate_reason="rejected_by_price_window_not_ready",
                        direct_refresh_attempted=False,
                        direct_refresh_result="not_reached_book_stage")
                continue
            # In shadow modes, shock detection may use the wider degraded band
            # (up to fail_closed_max_age_ms) once freshness is classified
            # "degraded" above; the staleness-scaled EV penalty downstream is
            # the defense. Live modes are untouched -- _classify_cex_freshness
            # never returns "degraded" there, so shock detection uses its own
            # cfg.cex.max_cex_staleness_ms default in live modes.
            shock_max_staleness = (self.cfg.cex_freshness.fail_closed_max_age_ms
                                   if freshness == "degraded" else None)
            shock = self.shock_detector.detect(view, max_staleness_ms=shock_max_staleness)
            if shock is None:
                near_miss = compute_shock_near_miss(view, self.cfg)
                detail = (f"ret2={view.primary.returns.get(2, 0.0):+.5f} "
                         f"z={view.primary.zscore:+.2f} "
                         f"px={view.primary.price:.2f}")
                if near_miss is not None:
                    detail += " " + near_miss.detail_suffix()
                    if near_miss.is_near_miss:
                        watchlist = self.diag["watchlist_no_shock_near_miss"]
                        watchlist.append({
                            "ts_ms": now, "asset": asset,
                            "shock_score": near_miss.shock_score,
                            "direction": near_miss.direction,
                            "time_remaining_s": (candidate_markets[0].seconds_to_expiry(now)
                                                 if candidate_markets else None),
                            "cex_age_ms": view.primary.staleness_ms,
                            "anchor_available": anchor.available if anchor else False,
                        })
                        if len(watchlist) > 50:
                            del watchlist[:-50]
                        self._shadow_diag(asset, "", "watchlist_no_shock_near_miss", detail, view)
                self._shadow_diag(asset, "", "rejected_by_no_shock", detail, view)
                if candidate_markets:
                    self._record_candidate_book_status(
                        asset=asset, market=candidate_markets[0], status="NOT_REACHED_BOOK_STAGE",
                        now_ms=now, earlier_gate_reason="rejected_by_no_shock",
                        direct_refresh_attempted=False,
                        direct_refresh_result="not_reached_book_stage")
                continue
            self._insert("signals", {"ts_ms": now, "asset": asset,
                                     "direction": shock.direction.value,
                                     "zscore": shock.zscore, "impulse": shock.impulse,
                                     "kind": "shock", "reason": shock.reason})
            if not candidate_markets:
                self._shadow_diag(asset, "", "rejected_by_time_to_expiry",
                                  "shock fired but no market in entry window", view)
                self._record_candidate_book_status(
                    asset=asset, status="NOT_EVALUATED", now_ms=now,
                    earlier_gate_reason="NO_CANDIDATE",
                    direct_refresh_attempted=False,
                    direct_refresh_result="not_reached_book_stage")
                continue
            for market in candidate_markets[:4]:
                await self._evaluate_market(shock, market, view, now)

    def _classify_cex_freshness(self, view) -> str:
        """"no_data" | "fresh" | "degraded" | "fail_closed".

        live_micro/live_full ALWAYS use the single strict
        cex.max_cex_staleness_ms gate (never "degraded") -- this method
        can never relax live execution behavior. Shadow modes use the
        tiered cex_freshness budgets when cex_freshness.enabled.

        Shadow band (freshest-available source across Bybit/OKX; Binance is
        geo-blocked here and simply never selected):
          <= live_signal_max_age_ms      -> "fresh"    (no penalty)
          <= fail_closed_max_age_ms       -> "degraded" (shadow evaluates with a
             staleness-scaled EV penalty; see _oracle_ev_reject). This is the
             band that keeps low-volume assets (SOL/ETH between sparse trade
             prints) evaluable instead of going dark for hours.
          >  fail_closed_max_age_ms       -> "fail_closed" (always rejected; the
             config's explicit outer ceiling -- stale data past it is dead)."""
        if view is None or view.primary is None:
            return "no_data"
        staleness = view.primary.staleness_ms
        if self.mode.is_live or not self.cfg.cex_freshness.enabled:
            return "fresh" if staleness <= self.cfg.cex.max_cex_staleness_ms else "fail_closed"
        cf = self.cfg.cex_freshness
        if staleness <= cf.live_signal_max_age_ms:
            return "fresh"
        if staleness <= cf.fail_closed_max_age_ms:
            return "degraded"
        return "fail_closed"

    def _update_scan_snapshot(self, asset: str, view, candidate_markets: list,
                              anchor, freshness: str, block_reason: Optional[str],
                              now_ms: int) -> None:
        """Updated on EVERY scan iteration (not throttled, unlike
        _shadow_diag) -- distinct from last_prediction/last_signal
        snapshots, which only update when the pipeline actually produces
        a prediction/signal row. See Dashboard V3's "Last Scan Snapshot"
        panel; must never be mislabeled as a live-feed liveness signal."""
        self.diag["last_scan_snapshot"] = {
            "ts_ms": now_ms,
            "asset": asset,
            "candidate_market_id": candidate_markets[0].market_id if candidate_markets else None,
            "candidate_market_title": candidate_markets[0].title if candidate_markets else None,
            "block_reason": block_reason,
            "cex_source": view.primary.exchange if (view and view.primary) else None,
            "cex_source_age_ms": view.primary.staleness_ms if (view and view.primary) else None,
            "cex_freshness": freshness,
            "anchor_status": ("available" if (anchor and anchor.available)
                              else ("missing" if anchor is not None else "not_evaluated")),
        }
        self._record_feature_snapshot(asset, view, candidate_markets, anchor,
                                      freshness, block_reason, now_ms)

    def _record_feature_snapshot(self, asset: str, view, candidate_markets: list,
                                 anchor, freshness: str, block_reason: Optional[str],
                                 now_ms: int) -> None:
        """Research feature store (append-only, throttled, shadow-only): one
        wide point-in-time row per scanned candidate so replay/challenger/
        drift research works from what the pipeline actually saw. Never
        raises into the scan loop; never influences any trading decision."""
        try:
            from poly_alpha_sniper.research.feature_store import (
                build_scan_feature_row, should_record)
            from poly_alpha_sniper.strategy.shock_near_miss import (
                SCORING_VERSION, compute_shock_near_miss)
            last = self._feature_store_throttle.get(asset)
            if not should_record(last, now_ms, block_reason):
                return
            self._feature_store_throttle[asset] = now_ms
            market = candidate_markets[0] if candidate_markets else None
            book = self.book_store.get(market.yes_token_id) if market is not None else None
            row = build_scan_feature_row(
                asset=asset, view=view, market=market, anchor=anchor,
                freshness=freshness, block_reason=block_reason,
                near_miss=compute_shock_near_miss(view, self.cfg), book=book,
                scoring_version=SCORING_VERSION, now_ms=now_ms, lane="baseline")
            self._insert("feature_store", row)
            self._record_experimental_decisions(row, now_ms)
        except Exception:  # noqa: BLE001 -- diagnostics must never break scanning
            pass

    def _record_experimental_decisions(self, baseline_row: dict, now_ms: int) -> None:
        """EXPERIMENTAL_SHADOW challenger lane: evaluate the named research
        challengers against the SAME point-in-time features the baseline just
        recorded, and write one lane="experimental" row carrying every
        challenger's would-enter decision. Diagnostics only -- never an order,
        never baseline stats, never live readiness. Crash-proof by contract."""
        try:
            if not getattr(self.cfg, "research_challengers", None) \
                    or not self.cfg.research_challengers.enabled:
                return
            from poly_alpha_sniper.research.challenger_engine import (
                ChallengerEngine, build_experimental_row)
            if getattr(self, "_challenger_engine", None) is None:
                self._challenger_engine = ChallengerEngine()
            cash = None
            try:
                snap = self.portfolio.snapshot(now_ms)
                cash = getattr(snap, "cash_usd", None)
            except Exception:  # noqa: BLE001 -- cash gate degrades to not-evaluated
                pass
            decisions = self._challenger_engine.evaluate(baseline_row, now_ms,
                                                         available_cash_usd=cash)
            self._insert("feature_store", build_experimental_row(baseline_row, decisions))
        except Exception:  # noqa: BLE001 -- research must never break scanning
            pass
        try:
            # EXPERIMENTAL_PROBE_TRADING: simulated probe positions with a
            # separate simulated bankroll. Shadow-only; crash-proof.
            if getattr(self.cfg, "research_probe_trading", None) \
                    and self.cfg.research_probe_trading.enabled:
                from poly_alpha_sniper.research.probe_trader import ProbeTrader
                if getattr(self, "_probe_trader", None) is None:
                    self._probe_trader = ProbeTrader(self.cfg.research_probe_trading)
                self._probe_trader.on_scan(baseline_row, now_ms, self._insert)
        except Exception:  # noqa: BLE001 -- research must never break scanning
            pass

    def _cex_source_detail(self, view) -> str:
        """Human-readable per-source freshness breakdown for a stale-primary
        diagnostic row. Labels each source BORDERLINE (within the wider
        shadow_diagnostic_staleness_ms window) or FAR_STALE — informational
        only; never affects the actual fresh/trade decision."""
        budget = self.cfg.cex.max_cex_staleness_ms
        diag_budget = self.cfg.cex.shadow_diagnostic_staleness_ms
        parts = []
        for ex, st in sorted(view.per_exchange.items(), key=lambda kv: kv[1].staleness_ms):
            sev = "OK" if st.fresh else ("BORDERLINE" if st.staleness_ms <= diag_budget
                                         else "FAR_STALE")
            marker = "*" if ex == view.primary.exchange else ""
            parts.append(f"{ex}{marker}={st.staleness_ms}ms[{sev}]")
        return (f"selected={view.primary.exchange} age={view.primary.staleness_ms}ms "
                f"budget={budget}ms | " + " ".join(parts))

    def _cex_source_debug(self, view, freshness: str, now_ms: int) -> dict:
        """Structured per-asset source/fallback debug for the read-only
        dashboard + export. Proves the freshest valid source is always chosen
        (selected == best), that a stale Binance/one source never blocks a
        fresher one, and buckets the freshest source as FRESH/DEGRADED/
        FAIL_CLOSED/NO_SOURCE. Diagnostic only -- never a gate."""
        cf = self.cfg.cex_freshness
        per_ex = {ex: st.staleness_ms for ex, st in view.per_exchange.items()}
        # best == freshest available source (min staleness) -- the same rule
        # CexState._select_best uses, so selected should equal best every time.
        best_source, best_age = None, None
        if per_ex:
            best_source = min(per_ex, key=lambda e: per_ex[e])
            best_age = per_ex[best_source]
        selected = view.primary.exchange
        selected_age = view.primary.staleness_ms
        bucket = {"fresh": "FRESH", "degraded": "DEGRADED",
                  "fail_closed": "FAIL_CLOSED", "no_data": "NO_SOURCE"}.get(freshness, "NO_SOURCE")
        # "a fresher source we failed to use" -- should always be False, since
        # selection already minimises staleness. Surfaced so a regression shows.
        fresher_unused = any(age < selected_age for ex, age in per_ex.items() if ex != selected)
        return {
            "ts_ms": now_ms,
            "asset": view.asset,
            "selected_source": selected,
            "selected_age_ms": selected_age,
            "selected_status": ("FRESH" if selected_age <= cf.live_signal_max_age_ms
                                else ("DEGRADED" if selected_age <= cf.fail_closed_max_age_ms
                                      else "FAIL_CLOSED")),
            "best_source": best_source,
            "best_source_age_ms": best_age,
            "bybit_age_ms": per_ex.get("bybit"),
            "okx_age_ms": per_ex.get("okx"),
            "binance_age_ms": per_ex.get("binance"),
            "live_threshold_ms": cf.live_signal_max_age_ms,
            "shadow_eval_threshold_ms": cf.shadow_eval_max_age_ms,
            "fail_closed_threshold_ms": cf.fail_closed_max_age_ms,
            "selected_is_freshest": (selected == best_source),
            "better_fallback_existed": fresher_unused,
            "freshness_bucket": bucket,
        }

    def _shadow_diag(self, asset: str, market_id: str, reason: str,
                     detail: str, view=None) -> None:
        """Persist a monitoring row explaining why no signal was produced.
        Throttled per (asset, reason) to one row / 30 s. NEVER places orders."""
        now = self.clock.now_ms()
        key = (asset, reason)
        if now - self._diag_throttle.get(key, 0) < 30_000:
            self.diag["last_block_reason"] = f"{asset}:{reason}"
            return
        self._diag_throttle[key] = now
        fresh, total = self._book_freshness()
        stats = view.primary if (view is not None and view.primary is not None) else None
        self._insert("shadow_diagnostics", {
            "ts_ms": now, "asset": asset, "market_id": market_id,
            "reason": reason, "detail": detail[:300],
            "ret_2s": stats.returns.get(2, 0.0) if stats else 0.0,
            "zscore": stats.zscore if stats else 0.0,
            "volatility": stats.volatility if stats else 0.0,
            "fresh_books": fresh, "total_books": total})
        self.diag["diagnostic_rows_written"] += 1
        self.diag["last_block_reason"] = f"{asset}:{reason}"

    def _book_freshness(self) -> tuple[int, int]:
        mirror = getattr(self, "mirror", None)
        if mirror is not None and hasattr(mirror, "freshness"):
            try:
                return mirror.freshness()
            except Exception:  # noqa: BLE001
                pass
        tokens = self.book_store.tracked_tokens()
        return sum(1 for t in tokens if self.book_store.is_fresh(t)), len(tokens)

    def _in_entry_window(self, market: MarketInfo, now_ms: int) -> bool:
        tte = market.seconds_to_expiry(now_ms)
        u = self.cfg.ultra_short_expiry
        blocked, _ = self.filter_list.is_blocked(market.market_id)
        return (not blocked) and (u.min_time_to_expiry_seconds <= tte <= u.max_time_to_expiry_seconds)

    def _record_candidate_book_status(self, *, asset: str, status: str,
                                      market: Optional[MarketInfo] = None, shock=None,
                                      book=None, now_ms: Optional[int] = None,
                                      earlier_gate_reason: Optional[str] = None,
                                      direct_refresh_attempted: bool = False,
                                      direct_refresh_result: str = "not_attempted",
                                      final_reject_reason: Optional[str] = None) -> dict:
        """Normalize the dashboard/export contract for the latest candidate's
        executable-side book state. This is diagnostic bookkeeping only."""
        now = now_ms if now_ms is not None else self.clock.now_ms()
        exec_side = market.side_for_direction(shock.direction) if (market is not None and shock is not None) else None
        exec_token = market.token_for(exec_side.outcome) if (market is not None and exec_side is not None) else None
        threshold = self.cfg.polymarket.max_orderbook_staleness_ms
        depth_near_best = None
        if book is not None:
            depth_near_best = round(book.depth_usd_at_ask(1) + book.depth_usd_at_bid(1), 2)
        detail = {
            "ts_ms": now,
            "market_id": market.market_id if market is not None else None,
            "token_id": exec_token,
            "asset": market.asset if market is not None else asset,
            "side": exec_side.value if exec_side is not None else None,
            "status": status,
            "earlier_gate_reason": earlier_gate_reason,
            "book_age_ms": max(0, now - book.ts_ms) if book is not None else None,
            "freshness_threshold_ms": threshold,
            "best_bid": book.best_bid if book is not None else None,
            "best_ask": book.best_ask if book is not None else None,
            "spread": book.spread if book is not None else None,
            "depth_near_best_usd": depth_near_best,
            "direct_refresh_attempted": direct_refresh_attempted,
            "direct_refresh_result": direct_refresh_result,
            "final_reject_reason": final_reject_reason,
        }
        self.diag["candidate_book_status"] = detail
        return detail

    def _candidate_book_is_executable(self, book, now_ms: int) -> tuple[bool, str]:
        if book is None:
            return False, "missing_book"
        threshold = self.cfg.polymarket.max_orderbook_staleness_ms
        if book.is_stale(now_ms, threshold):
            return False, "stale"
        if book.best_bid is None or book.best_ask is None:
            return False, "no_executable_bid_ask"
        if book.crossed or book.spread is None or book.spread < 0:
            return False, "invalid_spread"
        if book.depth_usd_at_bid(1) <= 0 or book.depth_usd_at_ask(1) <= 0:
            return False, "no_depth_near_best"
        return True, "ok"

    async def _ensure_candidate_book_fresh(self, market: MarketInfo, shock,
                                           now_ms: int) -> dict:
        """When the candidate's executable-side book is stale/missing at
        evaluation time, attempt ONE direct CLOB /book refresh for that single
        token before the pipeline hard-rejects on book_fresh (the shared
        WS/REST mirror routinely can't keep every tracked token under the 1 s
        budget -- see data/orderbook_mirror.py). A fresh direct fetch
        legitimately SATISFIES the freshness gate; it never bypasses it, and
        the order validator's staleness/spread/bid-ask checks all still run.

        Read-only: only calls the public /book endpoint. Never places or
        cancels an order. Returns a diagnostic dict (also cached to
        self.diag['candidate_book_status']) with status in
        FRESH | STALE | FETCH_FAILED."""
        exec_side = market.side_for_direction(shock.direction)
        exec_token = market.token_for(exec_side.outcome)

        book = self.book_store.get(exec_token)
        executable, reason = self._candidate_book_is_executable(book, now_ms)
        if executable:
            return self._record_candidate_book_status(
                asset=market.asset, market=market, shock=shock, book=book, now_ms=now_ms,
                status="FRESH", direct_refresh_attempted=False,
                direct_refresh_result="not_needed", final_reject_reason=None)

        refresher = getattr(self, "clob_public", None)
        if not (self.cfg.polymarket.direct_book_refresh_on_stale_eval
                and refresher is not None and hasattr(refresher, "get_book")):
            return self._record_candidate_book_status(
                asset=market.asset, market=market, shock=shock, book=book, now_ms=now_ms,
                status="STALE", direct_refresh_attempted=False,
                direct_refresh_result="no_refresher", final_reject_reason=RejectReason.STALE_BOOK)

        try:
            refreshed = await refresher.get_book(exec_token)
        except Exception as exc:  # noqa: BLE001 -- network failure must not crash the loop
            log.warning("candidate_book_direct_refresh_error", extra={"extra": {
                "token": exec_token, "error": repr(exc)[:120]}})
            refreshed = None
        if refreshed is None:
            return self._record_candidate_book_status(
                asset=market.asset, market=market, shock=shock, book=book, now_ms=now_ms,
                status="FETCH_FAILED", direct_refresh_attempted=True,
                direct_refresh_result="fetch_failed", final_reject_reason=RejectReason.BOOK_FETCH_FAILED)

        self.book_store.update_snapshot(refreshed)
        executable, reason = self._candidate_book_is_executable(refreshed, now_ms)
        if executable:
            return self._record_candidate_book_status(
                asset=market.asset, market=market, shock=shock, book=refreshed, now_ms=now_ms,
                status="FRESH", direct_refresh_attempted=True,
                direct_refresh_result="success", final_reject_reason=None)
        # got a book but it has no executable bid/ask -- never trade on that
        return self._record_candidate_book_status(
            asset=market.asset, market=market, shock=shock, book=refreshed, now_ms=now_ms,
            status="STALE", direct_refresh_attempted=True,
            direct_refresh_result=reason, final_reject_reason=RejectReason.STALE_BOOK)

    async def _evaluate_market(self, shock, market: MarketInfo, view, now_ms: int,
                               is_retry: bool = False) -> None:
        yes_book = self.book_store.get(market.yes_token_id)
        no_book = self.book_store.get(market.no_token_id)

        if self.cfg.oracle_ev.enabled:
            anchor = self._resolve_and_record_oracle_anchor(market, view, now_ms)
            ok, anchor_reject = validate_oracle_anchor(
                anchor, market, now_ms,
                self.cfg.oracle_ev.max_anchor_age_ms, self.cfg.oracle_ev.max_basis_abs_pct)
            if not ok:
                self._log_oracle_anchor(anchor, anchor_reject)
                self._record_candidate_book_status(
                    asset=shock.asset, market=market, shock=shock, status="NOT_REACHED_BOOK_STAGE",
                    now_ms=now_ms, earlier_gate_reason=anchor_reject,
                    direct_refresh_attempted=False,
                    direct_refresh_result="not_reached_book_stage",
                    final_reject_reason=anchor_reject)
                self._shadow_diag(shock.asset, market.market_id,
                                  "rejected_by_" + anchor_reject.replace("REJECTED_", "").lower(),
                                  f"oracle anchor: {anchor_reject} for {market.title[:50]}", view)
                return

        # Anchor/scan data is already recorded above; a stale book never erases
        # it. Try one direct refresh of the executable-side book before the
        # book_fresh hard reject; failed fetches and still-unusable books stop
        # here before signal construction.
        book_status = await self._ensure_candidate_book_fresh(market, shock, now_ms)
        if book_status["status"] == "FETCH_FAILED":
            self._shadow_diag(shock.asset, market.market_id, "rejected_by_book_fetch_failed",
                              f"direct CLOB /book refresh failed for {book_status['token_id'][:16]} "
                              f"({market.title[:40]})", view)
            return
        if book_status["status"] == "STALE":
            self._shadow_diag(shock.asset, market.market_id, "rejected_by_stale_book",
                              f"{RejectReason.STALE_BOOK}: executable book invalid/stale "
                              f"for {market.title[:50]} "
                              f"(refresh={book_status['direct_refresh_result']})", view)
            return
        yes_book = self.book_store.get(market.yes_token_id)
        no_book = self.book_store.get(market.no_token_id)

        signal = self.signal_engine.build_signal(shock, market, view, yes_book, no_book, now_ms)
        if signal is None:
            side_book = yes_book if market.side_for_direction(shock.direction).outcome.value == "YES" \
                else no_book
            max_stale = self.cfg.polymarket.max_orderbook_staleness_ms
            if side_book is None or side_book.is_stale(now_ms, max_stale):
                self._shadow_diag(shock.asset, market.market_id,
                                  "rejected_by_stale_book",
                                  f"book {'missing' if side_book is None else 'stale'} "
                                  f"for {market.title[:50]} "
                                  f"(refresh={book_status['direct_refresh_result']})", view)
            else:
                self._shadow_diag(shock.asset, market.market_id, "rejected_by_edge",
                                  f"no positive edge on {market.title[:50]}", view)
            return
        ok, issues = self.check_signal(signal)
        if not ok:
            self._record_prediction(signal, Decision.REJECT.value, "SANITY:" + ",".join(issues))
            return

        if self.cfg.oracle_ev.enabled:
            ev_reject = self._oracle_ev_reject(signal, market, yes_book, no_book, now_ms, anchor)
            if ev_reject:
                self._record_prediction(signal, Decision.REJECT.value, ev_reject)
                return

        can, freq_reason = self.freq.can_trade(market.asset, market.market_id, self.mode)
        snap = self.portfolio.snapshot(now_ms)
        hard_checks = self._hard_checks(signal, market, yes_book, no_book, now_ms, can, freq_reason)
        gate = self.gate.evaluate(signal, self.aggression.current, self.mode, hard_checks, snap)
        signal.gate = gate
        signal.tier = gate.tier
        self._insert("balanced_alpha_gate_results", {
            "ts_ms": now_ms, "market_id": market.market_id, "signal_id": signal.signal_id,
            **{k: (";".join(v) if isinstance(v, list) else v) for k, v in gate.as_dict().items()}})

        if gate.decision == Decision.WAIT and not is_retry:
            await asyncio.sleep(0.15)
            return await self._evaluate_market(shock, market, view, self.clock.now_ms(), is_retry=True)

        risk = self.risk_manager.check_entry(signal, snap, self.mode)
        req = self._build_order_request(signal, risk) if risk.approved else None
        if req is not None:
            from poly_alpha_sniper.execution.order_validator import validate_order
            book = yes_book if signal.side.outcome == Outcome.YES else no_book
            validation = validate_order(req, book, market, snap, self.cfg, self.clock.now_ms())
        else:
            from poly_alpha_sniper.core.contracts import RiskDecision
            validation = RiskDecision(approved=False,
                                      reject_reason=risk.reject_reason or RejectReason.INCOMPLETE_TRADE_PACKET)

        decision = self.final_decision.decide(signal, gate, risk, validation, self.mode)
        reject_reason = ("" if decision in (Decision.APPROVE, Decision.SHADOW_ONLY)
                         else (gate.reason if gate.hard_reject else
                               risk.reject_reason or validation.reject_reason or gate.reason))
        self._record_prediction(signal, decision.value, reject_reason)
        near_row = self.near_miss.consider(signal, gate)
        if near_row is not None:
            self._insert("near_misses", near_row)
            self.diag["near_miss_rows_written"] += 1
            self.diag["last_near_miss_ts"] = self.clock.now_ms()

        if decision == Decision.REJECT:
            self.freq.record_reject(market.market_id)
            if self.cfg.telegram.send_rejected_close_opportunities and gate.score >= 60:
                from poly_alpha_sniper.reporting.telegram import format_rejected
                # risk/validation (not gate) carry the sizing_detail when the true
                # blocker is a sizing/exposure reject -- gate.failed_checks is
                # empty in that case (the signal cleanly passed the alpha gate),
                # which used to make the message wrongly say "edge/confidence".
                sizing_source = select_sizing_detail_source(risk, validation)
                await self.telegram.send(format_rejected(signal, gate, reject_reason,
                                                          sizing_detail=sizing_source.sizing_detail if sizing_source else None))
            return
        if decision == Decision.WAIT:
            return

        executes_here = (
            decision == Decision.APPROVE
            or (decision == Decision.SHADOW_ONLY and not self.mode.is_live
                and gate.decision in (Decision.APPROVE, Decision.SHADOW_ONLY) and risk.approved
                and validation.approved))
        if not executes_here:
            return

        await self._execute_entry(signal, req, gate)

    def _resolve_and_record_oracle_anchor(self, market: MarketInfo, view, now_ms: int):
        """Builds the OracleAnchor for `market` and remembers the most recent
        one for export/dashboard (mirrors how latest_market_state tracks the
        most recent prediction) -- read-only bookkeeping, no order impact."""
        cex_price = view.primary.price if (view is not None and view.primary is not None) else None
        cex_ts_ms = (now_ms - view.primary.staleness_ms) \
            if (view is not None and view.primary is not None) else None
        anchor = resolve_oracle_anchor(market, cex_price, cex_ts_ms, now_ms)
        self.diag["latest_oracle_anchor"] = asdict(anchor)
        return anchor

    def _log_oracle_anchor(self, anchor, gate_result: str, ev_result=None) -> None:
        """Persists one row per market evaluation to oracle_anchor_log -- the
        history strategy.oracle_lag_profiler / backtest.point_in_time_replay /
        strategy.loss_attribution read from. Never places/cancels an order;
        pure DB bookkeeping, same pattern as _shadow_diag/_record_prediction."""
        self._insert("oracle_anchor_log", {
            "ts_ms": self.clock.now_ms(),
            "market_id": anchor.market_id, "asset": anchor.asset,
            "window_start_ts_ms": anchor.window_start_ts_ms,
            "window_end_ts_ms": anchor.window_end_ts_ms,
            "oracle_source": anchor.oracle_source,
            "price_to_beat": anchor.oracle_open_price,
            "oracle_open_ts_ms": anchor.oracle_open_ts_ms,
            "cex_price": anchor.cex_price, "cex_ts_ms": anchor.cex_ts_ms,
            "basis_pct": anchor.oracle_vs_cex_basis,
            "anchor_quality": anchor.oracle_anchor_quality,
            "time_remaining_seconds": anchor.time_remaining_seconds,
            "ev": ev_result.ev if ev_result else None,
            "probability_of_payout": ev_result.probability_of_payout if ev_result else None,
            "executable_price": ev_result.executable_price if ev_result else None,
            "gate_result": gate_result,
        })

    def _oracle_ev_reject(self, signal: Signal, market: MarketInfo, yes_book, no_book,
                          now_ms: int, anchor) -> str:
        """Post-signal oracle-EV checks (needs signal.edge, so runs after
        build_signal). Returns a REJECTED_* reason string, or "" if the
        signal clears every check. Never places/cancels an order. Logs
        exactly one oracle_anchor_log row per call, whatever the outcome."""
        cfg = self.cfg.oracle_ev
        tte_s = market.seconds_to_expiry(now_ms)
        if tte_s < cfg.min_time_to_close_seconds:
            self._log_oracle_anchor(anchor, RejectReason.TIME_TO_CLOSE_RISK)
            return RejectReason.TIME_TO_CLOSE_RISK
        book = yes_book if signal.side.outcome == Outcome.YES else no_book
        if book is None:
            self._log_oracle_anchor(anchor, RejectReason.DATA_QUALITY)
            return RejectReason.DATA_QUALITY
        depth = (book.depth_usd_at_bid(1) or 0.0) + (book.depth_usd_at_ask(1) or 0.0)
        if depth < self.cfg.microstructure.min_depth_at_ask_usd:
            self._log_oracle_anchor(anchor, RejectReason.BOOK_TOO_THIN)
            return RejectReason.BOOK_TOO_THIN
        if signal.fair.confidence <= 0:
            self._log_oracle_anchor(anchor, RejectReason.MODEL_UNCALIBRATED)
            return RejectReason.MODEL_UNCALIBRATED
        # CEX_FRESHNESS_DEGRADED (shadow modes, live_signal_max_age_ms < age
        # <= fail_closed_max_age_ms) is allowed to reach EV evaluation, but
        # never for free -- widen the adverse-selection buffer so it needs a
        # bigger edge to clear. This is the actual defense; the freshness
        # gate itself was already relaxed to let it get this far. The penalty
        # SCALES with staleness: 1x at/under shadow_eval_max_age_ms, growing
        # linearly toward the fail_closed ceiling, so a ~6-7s price must clear
        # a materially bigger edge than a ~2s price (never below the base add).
        degraded = self.diag["cex_freshness_degraded"].get(signal.asset, False)
        adverse_selection_buffer = cfg.adverse_selection_buffer
        if degraded:
            cf = self.cfg.cex_freshness
            age_ms = self.diag["cex_freshest_age_ms"].get(signal.asset, 0) or 0
            scale = 1.0
            if cf.shadow_eval_max_age_ms > 0:
                scale = max(1.0, age_ms / cf.shadow_eval_max_age_ms)
            adverse_selection_buffer += cf.degraded_adverse_selection_buffer_add * scale
        result = compute_oracle_ev(
            probability_of_payout=signal.edge.fair_probability,
            executable_price=signal.edge.market_price,
            fee_rate=cfg.fee_rate, slippage_buffer=cfg.slippage_buffer,
            adverse_selection_buffer=adverse_selection_buffer)
        self.diag["latest_oracle_ev"] = {
            "market_id": market.market_id, "ev": result.ev,
            "probability_of_payout": result.probability_of_payout,
            "executable_price": result.executable_price,
            "cex_freshness_degraded": degraded,
            "adverse_selection_buffer_used": adverse_selection_buffer}
        if result.ev < cfg.min_ev_threshold:
            self._log_oracle_anchor(anchor, RejectReason.EV_TOO_LOW, result)
            return RejectReason.EV_TOO_LOW
        self._log_oracle_anchor(anchor, "PASSED", result)
        return ""

    def _cex_fresh_for_hard_check(self, asset: str) -> bool:
        """live_micro/live_full: unchanged, exactly cex.max_cex_staleness_ms
        via CexState.is_fresh(). Shadow modes: widened to
        cex_freshness.fail_closed_max_age_ms so a signal built anywhere in the
        "degraded" band isn't hard-rejected here after _scan_entries already
        chose to let it through -- the actual defense against a degraded
        signal is the staleness-scaled EV penalty in _oracle_ev_reject, not a
        blanket block. Past fail_closed_max_age_ms this still returns False."""
        if self.mode.is_live or not self.cfg.cex_freshness.enabled:
            return self.cex_state.is_fresh(asset)
        st = self.cex_state.stats(asset)
        if st is None:
            return False
        return st.staleness_ms <= self.cfg.cex_freshness.fail_closed_max_age_ms

    def _hard_checks(self, signal: Signal, market: MarketInfo, yes_book, no_book,
                     now_ms: int, freq_ok: bool, freq_reason: str) -> dict[str, bool]:
        book = yes_book if signal.side.outcome == Outcome.YES else no_book
        max_stale = self.cfg.polymarket.max_orderbook_staleness_ms
        checks = {
            "mapping_clear": market.mapping_confidence >= 95,
            "cex_fresh": self._cex_fresh_for_hard_check(market.asset),
            "book_fresh": book is not None and not book.is_stale(now_ms, max_stale),
            "best_bid_ask": book is not None and book.best_bid is not None and book.best_ask is not None,
            "spread_ok": book is not None and book.spread is not None
                         and book.spread <= self.cfg.microstructure.max_spread,
            "no_panic": not self.panic.is_active,
            "no_kill_switch": not self.kill_switch.is_active,
            "frequency_ok": freq_ok,
            "expiry_window_ok": self._in_entry_window(market, now_ms),
        }
        if self.mode.is_live and self.cfg.cex.require_multi_exchange_confirmation_live:
            view = self.cex_state.multi_view(market.asset)
            checks["multi_exchange_confirm"] = bool(view and view.confirming_exchanges >= 2
                                                    and view.direction_agreement)
        if not freq_ok:
            log.info("frequency_block", extra={"extra": {"reason": freq_reason}})
        return checks

    def _build_order_request(self, signal: Signal, risk) -> Optional[OrderRequest]:
        from poly_alpha_sniper.execution.smart_limit_pricer import price_entry
        from poly_alpha_sniper.execution.order_side import shares_for_usd
        book = self.book_store.get(signal.market.token_for(signal.side.outcome))
        if book is None:
            return None
        price, _note = price_entry(book, signal.side,
                                   self.cfg.execution_pricing.default_mode, self.cfg)
        if price is None or price <= 0:
            return None
        shares = shares_for_usd(risk.size_usd, price)
        return OrderRequest(
            order_id=str(uuid.uuid4()), token_id=signal.market.token_for(signal.side.outcome),
            market_id=signal.market.market_id, side=signal.side, price=price,
            size_shares=shares, size_usd=risk.size_usd,
            tif_ms=self.cfg.execution_pricing.cancel_if_not_filled_ms,
            priority=RequestPriority.NEW_ENTRY, reason=signal.exit_plan,
            tier=signal.tier, signal_id=signal.signal_id)

    async def _execute_entry(self, signal: Signal, req: OrderRequest, gate: GateResult) -> None:
        from poly_alpha_sniper.reporting.telegram import format_auto_entry
        if self.mode.is_live:
            if not await self.governor.acquire("clob_order", RequestPriority.NEW_ENTRY):
                log.warning("entry_rate_limited", extra={"extra": {"market": req.market_id}})
                return
        book = self.book_store.get(req.token_id)
        try:
            record = await self.order_manager.submit(req, book)
        except Exception as exc:
            log.error("entry_submit_failed", extra={"extra": {"error": repr(exc)}})
            self._insert("errors", {"ts_ms": self.clock.now_ms(), "where": "entry_submit",
                                    "error": repr(exc)})
            return
        self._insert("orders", _order_row(record, self.mode.value))
        self.freq.record_entry(signal.asset, req.market_id)
        if record.filled_shares > 0:
            from poly_alpha_sniper.core.contracts import FillRecord
            fill = FillRecord(order_id=record.order_id, token_id=req.token_id,
                              market_id=req.market_id, side=req.side,
                              price=record.avg_fill_price or req.price,
                              size_shares=record.filled_shares, ts_ms=self.clock.now_ms())
            self.portfolio.apply_fill(fill, signal.market)
            pos = self.portfolio.get(req.token_id)
            if pos is not None:
                pos.tier = signal.tier
                pos.entry_signal_id = signal.signal_id
                pos.exit_plan = signal.exit_plan
            self._insert("fills", asdict_safe(fill))
            fq = self.fill_quality.score_and_track(req, record) if hasattr(self.fill_quality, "score_and_track") else None
            if fq is not None:
                self._insert("fill_quality", {"ts_ms": self.clock.now_ms(),
                                              "order_id": record.order_id, **fq})
        if self.cfg.telegram.send_trades:
            await self.telegram.send(format_auto_entry(signal, gate, req, self.aggression.current.value))

    # ------------------------------------------------------------------
    # Exits
    # ------------------------------------------------------------------
    async def _manage_exits(self) -> None:
        positions = self.portfolio.open_positions()
        open_tokens = {p.token_id for p in positions}
        # position closed since we last checked (normal exit or a prior
        # reconciliation) -> stop tracking how long it's been stuck
        for token in list(self._exit_stuck_since_ms):
            if token not in open_tokens:
                self._exit_stuck_since_ms.pop(token, None)
                self._panic_triggered_tokens.discard(token)
        if not positions:
            return
        now = self.clock.now_ms()
        for pos in positions:
            book = self.book_store.get(pos.token_id)
            if book is not None:
                self.portfolio.mark(pos.token_id, book.best_bid, book.best_ask)
        snap = self.portfolio.snapshot(now)
        pairs = self.sell_signal_engine.evaluate_all(
            positions,
            market_lookup=lambda mid: self.market_cache.get(mid) if hasattr(self.market_cache, "get") else None,
            book_lookup=lambda tid: self.book_store.get(tid),
            fair_lookup=self._fair_for_position,
            view_lookup=lambda asset: self.cex_state.multi_view(asset),
            portfolio=snap, panic=self.panic.is_active, kill=self.kill_switch.is_active)
        for pos, decision in pairs:
            since = self._exit_stuck_since_ms.setdefault(pos.token_id, now)
            if now - since >= DEFAULT_STUCK_EXIT_CUTOFF_MS:
                await self._reconcile_stuck_exit(pos, now - since)
                continue
            await self._execute_exit(pos, decision)

    async def _reconcile_stuck_exit(self, pos, elapsed_ms: int) -> None:
        """A position has needed exiting for >= DEFAULT_STUCK_EXIT_CUTOFF_MS
        without success (no book, or every attempt raising). Conservatively
        reconcile it (shadow-only bookkeeping, never places/cancels a real
        order) and stop retrying its book. Never clears panic."""
        now = self.clock.now_ms()
        record = reconcile_unexitable_position(
            self.portfolio, pos,
            f"exit attempts have not succeeded for {elapsed_ms}ms "
            f"(>= {DEFAULT_STUCK_EXIT_CUTOFF_MS}ms cutoff)",
            now, self._insert)
        self._exit_stuck_since_ms.pop(pos.token_id, None)
        self._panic_triggered_tokens.discard(pos.token_id)
        if record is None:
            return  # already gone (race with a normal fill) -- not an error
        if hasattr(self.mirror, "untrack_token"):
            self.mirror.untrack_token(pos.token_id)
        log.error("position_reconciled_no_book", extra={"extra": {
            "token": pos.token_id, "market": pos.market_id,
            "pnl_usd": record["pnl_usd"], "elapsed_ms": elapsed_ms}})
        try:
            await self.telegram.send(
                f"RECONCILED (shadow-only, conservative): a position in market "
                f"{pos.market_id} could not be exited for {elapsed_ms / 1000:.0f}s "
                f"(book unavailable/exit failing) and was marked as a total loss of "
                f"cost basis (${abs(record['pnl_usd']):.4f}). This is NOT a confirmed "
                f"market resolution. Panic remains active -- review manually before "
                f"/clear_panic.", critical=True)
        except Exception:
            log.error("reconcile_telegram_send_failed")

    def _fair_for_position(self, pos):
        market = self.market_cache.get(pos.market_id) if hasattr(self.market_cache, "get") else None
        if market is None:
            return None
        stats = self.cex_state.stats(market.asset)
        if stats is None:
            return None
        yes_book = self.book_store.get(market.yes_token_id)
        no_book = self.book_store.get(market.no_token_id)
        try:
            return self.prob_model.fair(stats, market, yes_book, no_book, self.clock.now_ms())
        except Exception:
            return None

    async def _execute_exit(self, pos, decision) -> None:
        from poly_alpha_sniper.reporting.telegram import format_auto_exit
        book = self.book_store.get(pos.token_id)
        if book is None:
            log.warning("exit_no_book", extra={"extra": {"token": pos.token_id}})
            return
        if self.mode.is_live:
            prio = RequestPriority.EMERGENCY_EXIT if decision.priority <= 2 else RequestPriority.CANCEL
            await self.governor.acquire("clob_order", prio)
        try:
            record = await self.sell_executor.execute_exit(pos, decision, book)
        except Exception as exc:
            log.error("exit_failed", extra={"extra": {"error": repr(exc), "token": pos.token_id}})
            if decision.priority <= 2 and pos.token_id not in self._panic_triggered_tokens:
                self._panic_triggered_tokens.add(pos.token_id)
                self.panic.activate(f"emergency_exit_failed:{pos.token_id}")
            return
        self._insert("orders", _order_row(record, self.mode.value))
        if record.filled_shares > 0:
            # real progress was made -- this position is not "stuck"
            self._exit_stuck_since_ms.pop(pos.token_id, None)
            self._panic_triggered_tokens.discard(pos.token_id)
            from poly_alpha_sniper.core.contracts import FillRecord
            price = record.avg_fill_price or (book.best_bid or 0.0)
            fill = FillRecord(order_id=record.order_id, token_id=pos.token_id,
                              market_id=pos.market_id,
                              side=OrderSide.SELL_YES if pos.outcome == Outcome.YES else OrderSide.SELL_NO,
                              price=price, size_shares=record.filled_shares,
                              ts_ms=self.clock.now_ms())
            pnl = record.filled_shares * (price - pos.avg_entry_price)
            hold_s = (self.clock.now_ms() - pos.entry_ts_ms) / 1000.0
            self.portfolio.apply_fill(fill)
            self.portfolio.record_trade_result(pnl > 0)
            self.freq.record_result(pos.market_id.split(":")[0], pos.market_id, pnl > 0)
            self.aggression.record_trade(pnl, 0.0, 0.0, 100.0)
            await self._maybe_mode_change()
            self._insert("fills", asdict_safe(fill))
            self._insert("exits", {
                "ts_ms": self.clock.now_ms(), "token_id": pos.token_id,
                "market_id": pos.market_id, "reason": decision.reason.value if decision.reason else "",
                "price": price, "shares": record.filled_shares, "pnl_usd": pnl,
                "hold_seconds": hold_s, "detail": decision.detail})
            self._insert("pnl", {"ts_ms": self.clock.now_ms(), "realized_pnl_usd": pnl,
                                 "equity_usd": self.portfolio.snapshot(self.clock.now_ms()).equity_usd})
            if self.cfg.telegram.send_exits:
                await self.telegram.send(format_auto_exit(pos, decision, price, pnl, hold_s))

    async def _maybe_mode_change(self) -> None:
        from poly_alpha_sniper.reporting.telegram import format_mode_change
        snap = self.portfolio.snapshot(self.clock.now_ms())
        old = self.aggression.current
        new = self.aggression.evaluate(snap)
        if new != old:
            log.info("aggression_mode_change", extra={"extra": {
                "old": old.value, "new": new.value, "reason": self.aggression.reason}})
            await self.telegram.send(format_mode_change(old.value, new.value,
                                                        self.aggression.reason, snap))

    # ------------------------------------------------------------------
    # Panic / controls / persistence helpers
    # ------------------------------------------------------------------
    async def _on_panic(self, trigger: str) -> None:
        from poly_alpha_sniper.reporting.incident_report import build_incident, save_incident
        from poly_alpha_sniper.reporting.telegram import format_panic
        log.error("panic_activated", extra={"extra": {"trigger": trigger}})
        self._insert("panic_events", {"ts_ms": self.clock.now_ms(), "trigger": trigger})
        try:
            await self.cancel_manager.cancel_all_priority()
        except Exception:
            log.error("panic_cancel_failed")
        try:
            books = {p.token_id: self.book_store.get(p.token_id) for p in self.portfolio.open_positions()}
            await self.emergency.close_everything(self.portfolio.open_positions(),
                                                  books.get, ExitReason.PANIC)
        except Exception:
            log.error("panic_close_failed")
        incident = build_incident("panic", {"trigger": trigger, "mode": self.mode.value},
                                  self.clock.now_ms())
        try:
            save_incident(self.store, incident)
        except Exception:
            pass
        await self.telegram.send(format_panic(trigger), critical=True)

    def _control_actions(self) -> dict:
        """Telegram command handlers. Every action routes through the normal
        risk/panic APIs — none bypasses gates."""
        def _cex_summary() -> str:
            feed = getattr(self, "feed", None)
            if feed is None:
                return "not started"
            parts = []
            for name, h in feed.health().items():
                state = "ok" if h.get("connected") else "DEGRADED"
                parts.append(f"{name}:{state}")
            return " ".join(parts) or "none"

        def _count(sql: str) -> int:
            try:
                rows = self.store.query(sql)
                return int(rows[0]["n"]) if rows else 0
            except Exception:  # noqa: BLE001
                return 0

        async def status():
            snap = self.portfolio.snapshot(self.clock.now_ms())
            hour_ago = self.clock.now_ms() - 3_600_000
            return (
                f"mode={self.mode.value} dry_run={self.cfg.mode.dry_run} "
                f"live_enabled={self.mode.is_live}\n"
                f"paused={self.paused} panic={self.panic.is_active} "
                f"kill={self.kill_switch.is_active} "
                f"aggression={self.aggression.current.value}\n"
                f"bankroll: equity=${snap.equity_usd:.2f} cash=${snap.available_cash_usd:.2f} "
                f"today=${snap.realized_pnl_today_usd:+.2f} open={snap.open_positions}\n"
                f"cex: {_cex_summary()}\n"
                f"markets discovered: {len(self.market_cache)}\n"
                f"predictions: {_count('SELECT COUNT(*) AS n FROM predictions')} "
                f"signals: {_count('SELECT COUNT(*) AS n FROM signals')} "
                f"errors(1h): {_count(f'SELECT COUNT(*) AS n FROM errors WHERE ts_ms > {hour_ago}')}\n"
                + _pred_diag_lines())

        def _pred_diag_lines() -> str:
            d = self._diag_summary()
            ticks = " ".join(f"{a}={n}" for a, n in d["cex_ticks_by_asset"].items()) or "none"
            ready = " ".join(f"{a}={'y' if ok else 'n'}"
                             for a, ok in d["price_windows_ready"].items())
            loop_age = (self.clock.now_ms() - d["last_prediction_loop_ts"]) / 1000 \
                if d["last_prediction_loop_ts"] else -1
            pred_age = (self.clock.now_ms() - d["last_prediction_ts"]) / 1000 \
                if d["last_prediction_ts"] else -1
            sources = " ".join(f"{a}={ex}({d['cex_freshest_age_ms'].get(a, '?')}ms)"
                              for a, ex in d["cex_selected_source"].items()) or "none yet"
            no_fresh_by_src = " ".join(f"{ex}={n}"
                                       for ex, n in d["cex_no_fresh_count_by_source"].items()) or "none"
            return (f"pred_loop: alive={d['prediction_loop_alive']} "
                    f"iter={d['prediction_loop_iterations']} last_run={loop_age:.0f}s ago\n"
                    f"last_prediction: {'never' if pred_age < 0 else f'{pred_age:.0f}s ago'} "
                    f"(rows={d['prediction_rows_written']}, diag_rows={d['diagnostic_rows_written']})\n"
                    f"last_block: {d['last_block_reason'] or '-'}\n"
                    f"books fresh: {d['fresh_books']}/{d['total_books']}\n"
                    f"windows ready: {ready or 'none'}\n"
                    f"ticks: {ticks}\n"
                    f"cex selected source (age): {sources}\n"
                    f"budgets: live={d['cex_live_staleness_budget_ms']}ms "
                    f"shadow_diag={d['cex_shadow_diag_budget_ms']}ms\n"
                    f"no_fresh_cex_price by source: {no_fresh_by_src}")

        async def pause():
            self.paused = True
            return "paused: no new entries (exits still active)"

        async def resume():
            if self.panic.is_active:
                return "cannot resume: panic active (use /clear_panic first)"
            self.paused = False
            return "resumed"

        async def panic_cmd():
            self.panic.activate("telegram_manual")
            return "panic activated"

        async def clear_panic():
            self.panic.clear(manual=True)
            return "panic cleared (entries stay paused until /resume)"

        async def close_all():
            books = {p.token_id: self.book_store.get(p.token_id) for p in self.portfolio.open_positions()}
            res = await self.emergency.close_everything(self.portfolio.open_positions(),
                                                        books.get, ExitReason.MANUAL)
            return f"close_all done: {res}"

        async def cancel_orders():
            n = await self.cancel_manager.cancel_all_priority()
            return f"cancelled {n} orders"

        async def positions():
            rows = [f"{p.market_id} {p.outcome.value} {p.shares:.2f}@{p.avg_entry_price:.3f}"
                    for p in self.portfolio.open_positions()]
            return "\n".join(rows) or "no open positions"

        async def pnl():
            snap = self.portfolio.snapshot(self.clock.now_ms())
            return (f"equity=${snap.equity_usd:.2f} realized=${snap.realized_pnl_usd:.2f} "
                    f"today=${snap.realized_pnl_today_usd:.2f} unrealized=${snap.unrealized_pnl_usd:.2f}")

        async def open_orders():
            try:
                orders = await self.client.get_open_orders()
                return "\n".join(f"{o.order_id[:8]} {o.side.value} {o.price}" for o in orders) or "none"
            except Exception as exc:
                return f"error: {exc}"

        async def mode():
            return f"trading_mode={self.mode.value} aggression={self.aggression.current.value} reason={self.aggression.reason}"

        async def mode_shadow():
            self.paused = True
            self.kill_switch.activate("telegram_mode_shadow_requested")
            return "live entries disabled (kill switch). Restart with --mode shadow_live to fully switch."

        async def disable_live():
            self.kill_switch.activate("telegram_disable_live")
            return "kill switch active: live entries disabled until restart + manual clear"

        async def mode_live_micro():
            return ("Refusing runtime upgrade to live. Restart with --mode live_micro; "
                    "all preflight + live gates will be enforced at startup.")

        async def blacklist_market(arg: str = ""):
            if not arg:
                return "usage: /blacklist_market <market_id>"
            self.filter_list.blacklist(arg, "telegram", ttl_s=None)
            return f"blacklisted {arg}"

        async def whitelist_market(arg: str = ""):
            if not arg:
                return "usage: /whitelist_market <market_id>"
            self.filter_list.whitelist(arg)
            return f"whitelisted {arg}"

        async def disable_asset(arg: str = ""):
            if arg in self.cfg.assets:
                self.cfg.assets.remove(arg)
                return f"disabled {arg}"
            return f"unknown asset {arg}"

        async def enable_asset(arg: str = ""):
            if arg and arg not in self.cfg.assets:
                self.cfg.assets.append(arg)
                return f"enabled {arg}"
            return f"{arg} already enabled or empty"

        async def backup_now():
            dest = self.backup_manager.backup_now()
            return f"backup written: {dest}"

        async def latency():
            return str(self.latency.snapshot())

        async def budget():
            return str(self.governor.usage())

        async def health():
            now = self.clock.now_ms()
            feed = getattr(self, "feed", None)
            lines = ["HEALTH"]
            if feed is not None:
                for name, h in feed.health().items():
                    state = "ok" if h.get("connected") else "DEGRADED"
                    note = " (geo-blocked?)" if name == "binance" and state == "DEGRADED" else ""
                    lines.append(f"cex {name}: {state} "
                                 f"staleness={h.get('staleness_ms', -1)}ms{note}")
            else:
                lines.append("cex: feeds not started")
            fresh_books = sum(1 for t in self.book_store.tracked_tokens()
                              if self.book_store.is_fresh(t))
            lines.append(f"polymarket public: {len(self.market_cache)} markets cached, "
                         f"{fresh_books}/{len(self.book_store.tracked_tokens())} books fresh")
            lines.append(f"polymarket auth: {'live client active' if self.mode.is_live else 'not used (shadow read-only)'}")
            try:
                self.store.query("SELECT 1 AS n")
                lines.append("db: ok")
            except Exception as exc:  # noqa: BLE001
                lines.append(f"db: ERROR {type(exc).__name__}")
            lines.append(f"telegram: {'enabled' if self.telegram.enabled else 'disabled'}")
            hb = self.runtime_state.state.get("heartbeat_ts_ms", 0)
            lines.append(f"heartbeat age: {(now - hb) / 1000:.0f}s" if hb else "heartbeat: none yet")
            lines.append(f"rate limiter: {'healthy' if self.governor.healthy else 'PENALIZED'}")
            hour_ago = now - 3_600_000
            try:
                errs = self.store.query(
                    f"SELECT COUNT(*) AS n FROM errors WHERE ts_ms > {hour_ago}")[0]["n"]
            except Exception:  # noqa: BLE001
                errs = "?"
            lines.append(f"errors last hour: {errs}")
            lines.append(f"panic={self.panic.is_active} kill={self.kill_switch.is_active} "
                         f"mode={self.mode.value} dry_run={self.cfg.mode.dry_run}")
            return "\n".join(lines)

        async def daily():
            from poly_alpha_sniper.reporting.daily_report import build_daily
            try:
                rep = build_daily(self.store, "")
                return rep.get("text", str(rep)) if isinstance(rep, dict) else str(rep)
            except Exception as exc:
                return f"daily report error: {exc}"

        return {
            "status": status, "health": health, "daily": daily, "positions": positions,
            "open_orders": open_orders, "pnl": pnl, "mode": mode, "pause": pause,
            "resume": resume, "panic": panic_cmd, "clear_panic": clear_panic,
            "close_all": close_all, "cancel_orders": cancel_orders,
            "mode_shadow": mode_shadow, "mode_live_micro": mode_live_micro,
            "disable_live": disable_live, "blacklist_market": blacklist_market,
            "whitelist_market": whitelist_market, "disable_asset": disable_asset,
            "enable_asset": enable_asset, "backup_now": backup_now,
            "latency": latency, "budget": budget,
        }

    def _record_prediction(self, signal: Signal, decision: str, reject_reason: str) -> None:
        r = signal.shock.returns if signal.shock else {}
        rec = PredictionRecord(
            ts_ms=self.clock.now_ms(), asset=signal.asset,
            market_id=signal.market.market_id, market_title=signal.market.title[:200],
            direction=signal.direction.value, cex_price=signal.shock.returns.get(0, 0.0) if False else 0.0,
            return_1s=r.get(1, 0.0), return_2s=r.get(2, 0.0), return_3s=r.get(3, 0.0),
            return_5s=r.get(5, 0.0), return_10s=r.get(10, 0.0), return_15s=r.get(15, 0.0),
            return_30s=r.get(30, 0.0),
            volatility=signal.shock.volatility if signal.shock else 0.0,
            zscore=signal.shock.zscore if signal.shock else 0.0,
            polymarket_price=signal.edge.market_price, fair_probability=signal.edge.fair_probability,
            edge=signal.edge.raw_edge, edge_after_spread=signal.edge.edge_after_spread,
            edge_after_slippage=signal.edge.edge_after_slippage,
            confidence=signal.fair.confidence, market_quality=signal.market_quality.score,
            trade_quality=signal.trade_quality, alpha_score=signal.alpha_score,
            tier=signal.tier.value, aggression_mode=self.aggression.current.value,
            decision=decision, reject_reason=reject_reason, mode=self.mode.value)
        self._insert("predictions", asdict_safe(rec))
        self.diag["prediction_rows_written"] += 1
        self.diag["last_prediction_ts"] = self.clock.now_ms()

    def _insert(self, table: str, row: dict) -> None:
        try:
            self.store.insert(table, row)
        except Exception as exc:
            log.error("db_insert_failed", extra={"extra": {"table": table, "error": repr(exc)}})


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def asdict_safe(obj) -> dict:
    d = asdict(obj)
    out = {}
    for k, v in d.items():
        if hasattr(v, "value"):
            out[k] = v.value
        elif isinstance(v, (dict, list)):
            out[k] = str(v)[:1000]
        else:
            out[k] = v
    return out


def _order_row(record, mode: str) -> dict:
    row = asdict_safe(record)
    row["mode"] = mode
    return row


async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x
