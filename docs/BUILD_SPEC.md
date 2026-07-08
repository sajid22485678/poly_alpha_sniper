# poly_alpha_sniper — BUILD SPEC (read this before writing any file)

Project root: `D:\claude\poly_alpha_sniper`. The repo directory IS the Python
package `poly_alpha_sniper` (parent dir `D:\claude` goes on sys.path; tests use
root `conftest.py` for that).

## MUST READ FIRST (already written — do not modify)
- `core/contracts.py`  — ALL shared enums/dataclasses/protocols. Import from here.
- `core/config_loader.py` — pydantic `Config` tree + `Secrets`. Access like `cfg.risk.max_trade_usd`.
- `core/clock.py` — `Clock`/`WallClock`/`SimClock`. NEVER call time.time() in logic; take a `Clock` or `now_ms` param.
- `core/event_bus.py` — `EventBus`, `Topics`.
- `core/logger.py` — `get_logger(name)`, JSON + secret redaction. Log via `log.info("event_name", extra={"extra": {...}})`.
- `config.yaml` — canonical defaults.

## Coding standards (all agents)
- Python 3.11+, full type hints, dataclasses. Async (`async def`) for anything doing I/O.
- Deps allowed: stdlib, aiohttp, websockets, numpy, pydantic, yaml. pandas/plotly ONLY in backtest/analytics/dashboard. NO other deps. NO AI/LLM libraries anywhere.
- Determinism: no `random` without an explicit seed parameter; no wall-clock reads outside `WallClock`.
- Every module starts with a docstring; include an `ASSUMPTIONS:` section where you made a judgement call.
- NEVER log/print secrets. Use `core.logger.get_logger`. Never store secrets in DB rows.
- Persistence rule: strategy/risk/execution modules DO NOT write to the DB directly. They return records / publish on the EventBus; `core/app.py` (written later) wires persistence via the `Store` protocol.
- Prices: Polymarket outcome prices in [0,1]; tick via `contracts.round_to_tick` / `is_valid_tick`. Timestamps: epoch ms ints.
- Reject reasons: use `contracts.RejectReason` constants verbatim.
- Files must be complete and import-clean. Small glue modules may be concise (30–80 lines); core engines should be substantial and correct.

## Tests (agents write tests for their own modules)
- Location `tests/`, plain pytest functions, NO network, NO sleeps > 0.05 s, use `SimClock`.
- Import as `from poly_alpha_sniper.strategy.edge_engine import ...`.
- Construct configs with `Config()` defaults from `core.config_loader` and mutate fields as needed.
- Async tests: `pytest-asyncio` auto mode — just write `async def test_...`.

## Cross-package interface contracts (BINDING — other agents compile against these)

### data package
- `data/price_windows.py`: `class PriceWindow(max_age_s: float = 120)`: `.add(price: float, ts_ms: int)`, `.return_over(seconds: float, now_ms: int) -> float | None` (pct return), `.volatility_per_s(now_ms: int, lookback_s: float = 60) -> float`, `.zscore(window_s: float, now_ms: int) -> float`, `.last_price() -> float | None`.
- `data/cex_state.py`: `class CexState(cfg, clock)`: `.update(tick: CexTick) -> None`, `.stats(asset: str, exchange: str | None = None) -> CexWindowStats | None` (None if no data), `.multi_view(asset: str) -> MultiCexView`, `.is_fresh(asset: str) -> bool`.
- `data/orderbook_state.py`: `class OrderbookStore(cfg, clock)`: `.update_snapshot(snap: OrderbookSnapshot)`, `.get(token_id: str) -> OrderbookSnapshot | None`, `.is_fresh(token_id: str) -> bool`.

### discovery package
- `discovery/threshold_parser.py`: `parse_market_text(title: str, description: str = "") -> ParsedMarket`.
- `deterministic_intelligence/token_mapping_validator.py`: `validate_token_mapping(raw_market: dict, parsed: ParsedMarket) -> TokenMappingResult`.
- `discovery/market_mapper.py`: `map_raw_market(raw: dict, now_ms: int) -> MarketInfo` (fills parse + mapping fields; sets `parse_reject_reason` when rejected).

### strategy package
- `strategy/shock_detector.py`: `class ShockDetector(cfg, clock)`: `.detect(view: MultiCexView) -> Shock | None`; `.on_shock_consumed(asset)` for cooldown.
- `strategy/distance_to_strike_model.py`: `prob_above_threshold(price: float, threshold: float, tte_s: float, vol_per_s: float, momentum: float = 0.0, zscore: float = 0.0, mean_reversion_penalty: float = 0.12) -> tuple[float, float, float]` returning `(p_above, p_below, confidence_0_100)`.
- `strategy/probability_model.py`: `class ProbabilityModel(cfg)`: `.fair(stats: CexWindowStats, market: MarketInfo, yes_book: OrderbookSnapshot | None, no_book: OrderbookSnapshot | None, now_ms: int, shock: Shock | None = None) -> FairProbability`.
- `strategy/edge_engine.py`: `compute_edge(side: OrderSide, fair_p: float, book: OrderbookSnapshot, cfg, size_usd: float, confidence: float, slippage_bps: float | None = None) -> EdgeResult | None` (None if no executable price).
- `strategy/market_quality_score.py`: `class MarketQualityScorer(cfg)`: `.score(market: MarketInfo, yes_book, no_book, now_ms: int) -> MarketQualityResult`.
- `strategy/signal_engine.py`: `class SignalEngine(...)`: `.build_signal(shock, market, view, yes_book, no_book, now_ms) -> Signal | None` — composes probability, edge, quality; assigns preliminary tier via `deterministic_intelligence/opportunity_tier.py`.
- `strategy/exit_engine.py`: `class ExitEngine(cfg, clock)`: `.evaluate(position: Position, market: MarketInfo, book: OrderbookSnapshot | None, fair: FairProbability | None, view: MultiCexView | None, portfolio: PortfolioSnapshot, panic: bool = False, kill: bool = False) -> ExitDecision` — implements ALL triggers + tier_exit_rules + expiry force-exit + priority via `contracts.EXIT_PRIORITY`.

### deterministic_intelligence package
- `deterministic_intelligence/opportunity_tier.py`: `classify_tier(edge: EdgeResult, confidence: float, market_quality: float, cfg, mode: TradingMode) -> Tier`.
- `deterministic_intelligence/balanced_alpha_gate.py`: `class BalancedAlphaGate(cfg)`: `.evaluate(signal: Signal, aggression: AggressionMode, mode: TradingMode, hard_checks: dict[str, bool], portfolio: PortfolioSnapshot) -> GateResult`. `hard_checks` maps check-name -> ok (e.g. {"cex_fresh": True, "book_fresh": False, ...}); any False = hard reject. Implement decision rules + soft penalties exactly as configured.
- `deterministic_intelligence/adaptive_aggression.py`: `class AdaptiveAggression(cfg, clock)`: `.record_trade(pnl_usd: float, edge_expected: float, edge_realized: float, fill_quality: float)`, `.evaluate(portfolio: PortfolioSnapshot) -> AggressionMode`, `.current -> AggressionMode`, `.reason -> str`.
- `deterministic_intelligence/trade_frequency_controller.py`: `class TradeFrequencyController(cfg, clock)`: `.can_trade(asset: str, market_id: str, mode: TradingMode) -> tuple[bool, str]`, `.record_entry(asset, market_id)`, `.record_result(asset, market_id, win: bool, bad_fill: bool = False)`, `.record_reject(market_id)`.
- `deterministic_intelligence/final_decision_engine.py`: `class FinalDecisionEngine(...)`: `.decide(signal, gate: GateResult, risk: RiskDecision, validation: RiskDecision, mode: TradingMode) -> Decision` — APPROVE only if everything passed; SHADOW_ONLY when gate says so or mode is shadow.

### risk package
- `risk/position_sizer.py`: `compute_position_size(cfg, portfolio: PortfolioSnapshot, market: MarketInfo, mode: TradingMode, edge_after_slippage: float) -> RiskDecision` — exact 10-step formula from the master spec (clamp equity*pct into [min,max], reject on cash/min-order/exposure/daily-loss/loss-streak).
- `risk/risk_manager.py`: `class RiskManager(cfg, clock, kill_switch, panic_mode)`: `.check_entry(signal: Signal, portfolio: PortfolioSnapshot, mode: TradingMode) -> RiskDecision` (delegates to sizer + exposure + caps), `.check_sell(position, sell_shares: float, book) -> RiskDecision`.
- `risk/kill_switch.py`: `class KillSwitch`: `.activate(reason: str)`, `.clear(manual: bool = True)`, `.is_active -> bool`, `.reason -> str`.
- `risk/panic_mode.py`: `class PanicMode(...)`: same shape + `.triggers` list + async `on_activate` callbacks registration.

### execution package
- `execution/order_validator.py`: `validate_order(req: OrderRequest, book: OrderbookSnapshot | None, market: MarketInfo, portfolio: PortfolioSnapshot, cfg, now_ms: int, owned_shares: float = 0.0) -> RiskDecision` — full checklist, RejectReason constants.
- `execution/simulator.py`: `class SimulatedClobClient(clock, book_provider: Callable[[str], OrderbookSnapshot | None], fill_latency_ms: int = 150, seed: int = 7)` implementing `ClobTradingClient` — walk the book, depth-limited partial fills, price-time realistic; no fills better than book.
- `execution/order_lifecycle.py`: `class OrderLifecycle`: `.transition(order: OrderRecord, new_state: OrderState, now_ms: int, note: str = "") -> OrderRecord`; validates legal transitions (dict `LEGAL_TRANSITIONS`); illegal transition raises `IllegalTransition`.
- `execution/order_manager.py`: `class OrderManager(cfg, clock, client: ClobTradingClient, lifecycle)` — submit/cancel/track, timeout cancels after `cancel_if_not_filled_ms`, retry policy.
- `execution/fill_reconciler.py`: `class FillReconciler(...)`: `.reconcile(local_orders: list[OrderRecord], exchange_orders: list[OrderRecord], local_positions: list[Position], exchange_positions: list[Position], balance_local: float, balance_exchange: float) -> ReconcileResult` (dataclass defined in that file with `.ok`, `.mismatches: list[str]`).

### portfolio package
- `portfolio/positions.py`: `class Portfolio(cfg, clock)`: `.apply_fill(fill: FillRecord, market: MarketInfo | None = None)`, `.get(token_id) -> Position | None`, `.open_positions() -> list[Position]`, `.snapshot(now_ms: int) -> PortfolioSnapshot`, `.mark(token_id, bid, ask)`. SELL fills reduce shares and realize PnL vs avg entry; BUY fills increase shares/avg. Never negative shares (raise ValueError).
- `portfolio/bankroll.py`: realized-only equity math + daily loss tracking (day boundary via clock, tz-naive UTC ok).

### storage package
- `storage/sqlite_store.py`: `class SqliteStore(path: str)` implementing `Store` protocol (contracts). Thread-safe (lock), WAL mode. `.insert(table, row_dict)` auto-ignores unknown keys.
- `storage/migrations.py`: `run_migrations(store) -> None`; ALL tables from master spec (cex_ticks, market_snapshots, orderbook_snapshots, predictions, signals, orders, order_lifecycle, fills, positions, exits, pnl, health_logs, errors, tuning_changes, fill_quality, settlement_events, reconciliation_events, panic_events, latency_metrics, shadow_live_discrepancy, incident_reports, rate_limit_usage, near_misses, market_memory, experiment_registry, deterministic_reviews, balanced_alpha_gate_results, market_text_parse_results, token_mapping_validation, signal_sanity_checks, trade_packet_validations, telegram_commands, watchdog_events, database_backups, calibration_results) with a `schema_migrations` version table.

### core runtime (agent-built parts)
- `core/rate_limit_governor.py`: `class RateLimitGovernor(clock)`: `async .acquire(endpoint: str, priority: RequestPriority) -> bool`, `.report_429(endpoint)`, `.healthy -> bool`, `.usage() -> dict`. Token-bucket per endpoint class; priority queue; emergency never starved.
- `core/runtime_state.py`: JSON state file save/load (mode, positions summary, panic flag, restart counts) under `runtime/state.json`; atomic writes.
- `core/startup_preflight.py`: `async run_preflight(cfg, secrets, *, offline_ok: bool = False) -> PreflightResult` (dataclass: ok, checks: list[tuple[name, ok, detail]]). Network checks are try/except with clear failure detail; in tests use offline_ok.
- `core/live_readiness.py`: `async check_live_readiness(cfg, secrets, client: ClobTradingClient | None, panic, kill, telegram_ok: bool, reconciled: bool) -> tuple[bool, list[str]]`.
- `core/process_lock.py`: lock-file based single-instance guard (`runtime/live.lock`, pid-checked via psutil).
- `core/watchdog.py`: subprocess supervisor: spawn `python main.py --mode X`, watch heartbeat file `runtime/heartbeat.json`, restart on crash/stale up to max_restarts_per_hour, `--once` flag for tests to run a single check cycle without spawning.

## Mode-sharing rule (enforced)
Entry path (all modes): SignalEngine -> BalancedAlphaGate -> RiskManager -> validate_order -> client.place_order where client is SimulatedClobClient (simulation/backtest), ShadowClient (records, no orders — implement in execution/live_executor.py as `ShadowClobClient`), or LiveClobClient (gated). Exit path: ExitEngine -> sell validation -> same client. Backtest replays through the SAME engines with SimClock.
