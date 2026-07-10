"""Typed configuration loading.

- config.yaml -> pydantic Config tree (every section validated, safe defaults)
- .env -> Secrets object (never repr'd, never serialized, never stored)
- Profiles apply named overrides on top of base config (see profile_loader).

The SAME Config object drives backtest, simulation, shadow_live, live_micro
and live_full — this is a core anti-drift guarantee.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field

from poly_alpha_sniper.core.contracts import AggressionMode, TradingMode

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Config sections (mirror config.yaml)
# ---------------------------------------------------------------------------

class ModeConfig(BaseModel):
    trading_mode: str = "shadow_live"
    dry_run: bool = True
    profile: str = "safe_shadow"


class ProfileConfig(BaseModel):
    trading_mode: Optional[str] = None
    dry_run: Optional[bool] = None
    aggression_default: Optional[str] = None
    ultra_short_expiry_enabled: Optional[bool] = None


class CexConfig(BaseModel):
    preferred_exchange: str = "binance"
    fallback_exchange: str = "bybit"
    optional_exchange: str = "okx"
    symbols: dict[str, str] = Field(default_factory=lambda: {
        "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT"})
    websocket_reconnect_seconds: float = 2
    # The strict freshness budget. Always used, unchanged, for live_micro/
    # live_full (see CexFreshnessConfig below -- live execution behavior is
    # never relaxed by that config). In shadow_live/simulation, this is only
    # the "fully fresh, no EV penalty" tier; CexFreshnessConfig widens what
    # the pipeline will still evaluate (with a penalty) beyond this budget.
    max_cex_staleness_ms: int = 500
    # Diagnostic-only severity threshold: labels a shadow_diagnostics row as
    # "BORDERLINE" (within this wider window) vs "FAR_STALE" so an operator
    # can tell "missed freshness by a hair" from "this source is actually
    # down". Superseded for actual gating purposes by CexFreshnessConfig in
    # shadow modes; kept for this purely-cosmetic label.
    shadow_diagnostic_staleness_ms: int = 1500
    require_multi_exchange_confirmation_live: bool = True
    # Bybit main domain can be geo-blocked; bytick is Bybit's official mirror.
    bybit_ws_url: str = "wss://stream.bybit.com/v5/public/spot"
    bybit_ws_fallback_url: str = "wss://stream.bytick.com/v5/public/spot"


class CexFreshnessConfig(BaseModel):
    """Tiered CEX freshness handling for shadow_live/simulation ONLY -- see
    core/app.py._classify_cex_freshness. live_micro/live_full always use the
    strict cex.max_cex_staleness_ms single gate, completely unchanged; this
    config can never relax live execution behavior.

    Root cause this exists for: at cex.max_cex_staleness_ms=500ms, Bybit/OKX
    public trade-print feeds for lower-volume pairs (ETH/SOL) routinely sit
    600-2500ms between prints even while genuinely alive -- verified against
    901 real rejected_by_no_fresh_cex_price rows, none ever showing Binance
    selected (it never ticks in this deployment; the freshest-wins source
    selection in data/cex_state.py was already correct and unaffected by
    this config). The fix is not "trust stale data": it's "let the pipeline
    still evaluate market state / oracle anchor / EV so the dashboard isn't
    empty for hours, and only relax actual signal ACCEPTANCE with an EV
    penalty, never for free."

    live_signal_max_age_ms: fully fresh -- no penalty, normal decisioning.
    shadow_eval_max_age_ms: the reference point where the CEX_FRESHNESS_DEGRADED
      EV penalty is at its 1x base; deeper into the degraded band the penalty
      scales up linearly with staleness (see _oracle_ev_reject). Not itself a
      hard reject boundary any more -- that is fail_closed_max_age_ms.
    fail_closed_max_age_ms: the actual shadow reject boundary and explicit outer
      ceiling (must be >= the other two). In shadow modes, shock detection /
      oracle-anchor / EV evaluation may still proceed (flagged
      CEX_FRESHNESS_DEGRADED, with a staleness-scaled EV penalty) up to this
      age; staleness beyond it is unambiguously dead and always rejected as
      rejected_by_no_fresh_cex_price. Live modes never enter the degraded band.
    dashboard_live_feed_warn_ms: dashboard-display threshold only, never a
      pipeline gate (see dashboard_v3's Live Feed State panel).
    degraded_adverse_selection_buffer_add: added on top of
      oracle_ev.adverse_selection_buffer whenever CEX_FRESHNESS_DEGRADED --
      the actual defense against accepting a degraded-freshness signal,
      since the hard freshness gate itself is relaxed in this zone.
    """
    enabled: bool = True
    live_signal_max_age_ms: int = 1500
    shadow_eval_max_age_ms: int = 3000
    dashboard_live_feed_warn_ms: int = 5000
    fail_closed_max_age_ms: int = 8000
    degraded_adverse_selection_buffer_add: float = 0.015


class ResearchChallengersConfig(BaseModel):
    """EXPERIMENTAL_SHADOW challenger lane (research/challenger_engine.py).

    Writes lane="experimental" feature-store rows with per-challenger
    would-enter decisions. STRICTLY diagnostic: never places orders, never
    changes baseline gates, never counts toward baseline shadow stats or
    live readiness. Disabling it only stops the experimental rows -- the
    baseline pipeline is untouched either way."""
    enabled: bool = True


class PolymarketConfig(BaseModel):
    refresh_markets_seconds: float = 20
    refresh_orderbooks_ms: int = 500
    max_orderbook_staleness_ms: int = 1000
    max_spread: float = 0.035
    min_liquidity_usd: float = 200
    min_time_to_expiry_seconds: float = 45
    max_time_to_expiry_seconds: float = 330
    use_websocket_orderbook_first: bool = True
    rest_snapshot_fallback: bool = True
    # When a candidate market's executable-side book is stale at evaluation
    # time (the WS/REST mirror couldn't keep it under max_orderbook_staleness_ms
    # among many tracked tokens), attempt ONE direct CLOB /book fetch for that
    # single token before hard-rejecting -- see core/app.py._ensure_candidate_book_fresh.
    # Read-only (public /book endpoint); never places or cancels an order.
    # A fresh direct fetch legitimately satisfies the book_fresh gate; it does
    # not bypass it. Disable to fall back to mirror-only freshness.
    direct_book_refresh_on_stale_eval: bool = True
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class UltraShortExpiryConfig(BaseModel):
    enabled: bool = True
    target_expiry_seconds: float = 300
    min_time_to_expiry_seconds: float = 60
    max_time_to_expiry_seconds: float = 330
    preferred_time_to_expiry_seconds: list[float] = Field(
        default_factory=lambda: [180, 240, 300])
    reject_if_less_than_seconds: float = 45
    force_exit_before_expiry_seconds: float = 20
    max_hold_seconds: float = 180
    prefer_exit_before_resolution: bool = True
    only_trade_a_plus_setups: bool = False
    allow_a_tier_live: bool = True
    allow_b_tier_live_when_aggressive: bool = True
    require_clean_threshold_mapping: bool = True
    reject_ambiguous_markets: bool = True


class StrategyConfig(BaseModel):
    cycle_ms: int = 100
    prediction_windows_seconds: list[int] = Field(
        default_factory=lambda: [1, 2, 3, 5, 10, 15, 30])
    hard_min_edge_live_micro: float = 0.055
    preferred_edge_live_micro: float = 0.08
    a_plus_edge_live_micro: float = 0.10
    min_edge_shadow: float = 0.035
    confidence_min_live_hard_floor: float = 65
    confidence_preferred_live: float = 75
    max_entry_delay_ms: int = 1000
    cooldown_market_seconds: float = 20
    max_signals_per_hour: int = 200
    max_trades_per_hour: int = 40
    shock_min_abs_return: float = 0.0012   # 0.12% short-window move
    shock_min_zscore: float = 2.0


class ProbabilityModelConfig(BaseModel):
    type: str = "ensemble_logistic"
    clamp_min: float = 0.02
    clamp_max: float = 0.98
    use_distance_to_strike_if_available: bool = True
    momentum_weight: float = 0.35
    volatility_weight: float = 0.20
    orderbook_imbalance_weight: float = 0.15
    lag_weight: float = 0.20
    mean_reversion_penalty: float = 0.12
    time_decay_weight: float = 0.10


class MarketQualityConfig(BaseModel):
    enabled: bool = True
    min_score_shadow: float = 50
    min_score_live_micro: float = 60
    min_score_live_full: float = 75


class DynamicEdgeConfig(BaseModel):
    enabled: bool = True
    hard_min_edge: float = 0.03
    live_micro_min_edge_floor: float = 0.055
    live_full_min_edge_floor: float = 0.08


class MicrostructureConfig(BaseModel):
    max_spread: float = 0.035
    min_depth_at_ask_usd: float = 100
    max_slippage_bps: float = 60
    adverse_selection_block: bool = True
    require_orderbook_freshness: bool = True
    require_cex_freshness: bool = True


class CapitalScalingTier(BaseModel):
    equity_min: float
    equity_max: float
    max_trade_usd: Optional[float] = None
    max_trade_pct_equity: Optional[float] = None


class CapitalScalingConfig(BaseModel):
    enabled: bool = True
    auto_increase_size: bool = False
    require_min_live_trades_before_scale: int = 100
    require_rolling_pf_above: float = 1.25
    require_good_fill_quality: bool = True
    tiers: list[CapitalScalingTier] = Field(default_factory=lambda: [
        CapitalScalingTier(equity_min=10, equity_max=25, max_trade_usd=1),
        CapitalScalingTier(equity_min=25, equity_max=50, max_trade_usd=2),
        CapitalScalingTier(equity_min=50, equity_max=100, max_trade_usd=5),
        CapitalScalingTier(equity_min=100, equity_max=999999, max_trade_pct_equity=0.05),
    ])


class ProfitLockConfig(BaseModel):
    enabled: bool = True
    lock_profit_when_equity_up_pct: float = 50
    lock_profit_pct: float = 20
    if_equity_doubles_reduce_risk_one_day: bool = True
    if_drawdown_from_ath_pct: float = 20
    switch_to_defensive: bool = True


class MicroBankrollConfig(BaseModel):
    enabled: bool = True
    require_higher_edge_live: bool = True
    reject_wide_spread: bool = True
    reject_min_order_too_high: bool = True
    stop_after_two_losses: bool = True
    max_positions: int = 2


class DynamicExposureByTierConfig(BaseModel):
    """Tier-aware market exposure cap, fixed_min_shares mode only (see
    risk/exposure_cap.py). max_trade_usd mode always uses the flat
    risk.max_market_exposure_pct_equity regardless of this config -- this
    only replaces that ONE check, for ONE sizing mode; total exposure cap,
    daily loss cap, loss streak cap, kill switch, and panic are untouched."""
    enabled: bool = True
    default_pct: float = 0.10
    tiers: dict[str, float] = Field(default_factory=lambda: {
        "A_PLUS": 0.50, "A": 0.50, "B": 0.10})


class RiskConfig(BaseModel):
    starting_bankroll_usd: float = 10
    compound_enabled: bool = True
    use_realized_equity_only: bool = True
    position_size_pct_equity: float = 0.10
    min_trade_usd: float = 1
    live_micro_trade_usd: float = 1
    max_trade_usd: float = 1
    max_total_exposure_pct_equity: float = 0.30
    max_market_exposure_pct_equity: float = 0.10
    max_daily_loss_pct_equity: float = 0.20
    max_daily_loss_usd: float = 2
    max_open_positions: int = 2
    max_consecutive_losses: int = 2
    one_position_per_market: bool = True
    no_martingale: bool = True
    no_averaging_down: bool = True
    no_unrealized_compounding: bool = True
    # WS4: "fixed_min_shares" sizes every entry to exactly fixed_order_shares
    # shares (at the executable price) instead of max_trade_usd, so a valid
    # candidate is never rejected just because max_trade_usd is below what
    # Polymarket's minimum order requires at the current price. See
    # risk/position_sizer.py. "max_trade_usd" (default) preserves prior
    # behavior exactly -- this is opt-in, not a silent behavior change.
    sizing_mode: str = "max_trade_usd"   # "max_trade_usd" | "fixed_min_shares"
    fixed_order_shares: float = 5.0
    use_max_trade_usd: bool = True
    dynamic_exposure_by_tier: DynamicExposureByTierConfig = Field(
        default_factory=DynamicExposureByTierConfig)


class TierExitRule(BaseModel):
    max_hold_seconds: float = 180
    take_profit_pct: float = 0.12
    stop_loss_pct: float = 0.10
    close_when_edge_below: float = 0.005


class ExitConfig(BaseModel):
    max_hold_seconds: float = 180
    close_before_expiry_seconds: float = 20
    take_profit_pct: float = 0.12
    stop_loss_pct: float = 0.10
    close_when_edge_below: float = 0.005
    exit_on_repricing_complete: bool = True
    exit_on_momentum_decay: bool = True
    exit_on_expiry_risk: bool = True
    exit_on_opposite_shock: bool = True
    exit_on_orderbook_flip: bool = True
    profit_lock_enabled: bool = True


class SellExecutionConfig(BaseModel):
    enabled: bool = True
    default_mode: str = "smart"
    emergency_mode: str = "aggressive_limit"
    profit_take_mode: str = "passive_limit"
    cancel_if_not_filled_ms: int = 800
    max_chase_ticks: int = 1
    min_acceptable_exit_price: float = 0.01
    allow_partial_exit: bool = True


class PositionRulesConfig(BaseModel):
    allow_both_sides_same_market: bool = False
    close_before_flip_side: bool = True
    one_direction_per_market: bool = True


class RebalanceConfig(BaseModel):
    enabled: bool = True
    min_alpha_improvement: float = 20
    cooldown_seconds: float = 60
    require_sell_confirmed_before_new_buy: bool = True


class ExecutionPricingConfig(BaseModel):
    enabled: bool = True
    default_mode: str = "aggressive_limit"
    max_chase_ticks: int = 1
    max_slippage_bps: float = 60
    cancel_if_not_filled_ms: int = 800
    use_limit_orders_only: bool = True
    use_market_orders: bool = False


class AdaptiveAggressionConfig(BaseModel):
    enabled: bool = True
    default_mode: str = "NORMAL"
    allow_b_tier_live_only_in_aggressive: bool = True
    min_rolling_trades_for_aggressive: int = 20
    aggressive_min_profit_factor: float = 1.25
    aggressive_min_winrate: float = 0.52
    aggressive_min_edge_realization: float = 0.60
    defensive_loss_streak: int = 2
    defensive_max_drawdown_pct: float = 0.15
    defensive_min_profit_factor: float = 0.90
    defensive_cooldown_minutes: float = 30


class TradeFrequencyConfig(BaseModel):
    enabled: bool = True
    target_trades_per_hour_shadow: int = 20
    target_trades_per_hour_live_micro: int = 5
    max_trades_per_hour_live_micro: int = 12
    max_trades_per_asset_per_hour: int = 5
    cooldown_after_win_seconds: float = 10
    cooldown_after_loss_seconds: float = 60
    cooldown_after_bad_fill_seconds: float = 120
    cooldown_after_reject_seconds: float = 5
    allow_reentry_after_profit: bool = True
    max_reentries_same_market: int = 2


class AutoTuningConfig(BaseModel):
    enabled: bool = True
    tune_every_minutes: float = 60
    min_sample_size: int = 100
    tune_thresholds_only: bool = True
    never_increase_risk_automatically: bool = True
    optimize_for: list[str] = Field(default_factory=lambda: [
        "profit_factor", "expectancy", "max_drawdown", "realized_edge"])


class DashboardConfig(BaseModel):
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8501
    refresh_seconds: float = 3
    auth_enabled: bool = True
    mobile_friendly: bool = True
    read_only: bool = True


class TelegramConfig(BaseModel):
    enabled: bool = True
    send_signals: bool = True
    send_rejected_close_opportunities: bool = True
    send_trades: bool = True
    send_exits: bool = True
    send_health_every_minutes: float = 30
    send_daily_report_time: str = "23:00"
    controls_enabled: bool = True
    require_control_confirmation: bool = True


class RuntimeConfig(BaseModel):
    watchdog_enabled: bool = True
    auto_restart_on_crash: bool = True
    max_restarts_per_hour: int = 5
    graceful_shutdown_seconds: float = 10
    resume_state_after_restart: bool = True
    heartbeat_seconds: float = 15
    stale_heartbeat_seconds: float = 60
    log_rotation_mb: int = 50
    keep_log_days: int = 14
    backup_database_enabled: bool = True
    backup_interval_minutes: float = 30
    keep_backups: int = 20


class AgentExportConfig(BaseModel):
    """Read-only sanitized export for Hermes Agent / Obsidian consumption.
    Never reads .env, never writes to bot config, never touches orders."""
    enabled: bool = True
    output_dir: str = "D:/claude/agent_readonly/poly_alpha_sniper"


class ObsidianConfig(BaseModel):
    """Optional, disabled-by-default copy of the daily note into an Obsidian
    vault. Never requires Obsidian to be installed -- this just copies a
    plain markdown file to a folder."""
    enabled: bool = False
    vault_notes_dir: str = "D:/TradingVault/06_Poly_Hermes_Reports"
    overwrite_existing: bool = False


class OracleEvConfig(BaseModel):
    """Oracle-aware EV engine (see strategy/oracle_anchor.py, oracle_ev.py).
    Entries fail closed (REJECTED_MISSING_ORACLE_ANCHOR etc.) when the
    resolution-relevant price_to_beat anchor is missing/stale/mismatched --
    this exists because Polymarket resolves these markets against a
    Chainlink price stream, not CEX spot, and the bot's signal was
    previously CEX-only with no anchor-awareness at all (see audit in the
    poly_oracle_ev_5share_auto_export commit message)."""
    enabled: bool = True
    max_anchor_age_ms: float = 300_000       # anchor is only valid within its own 5-min window
    max_basis_abs_pct: float = 0.02          # |cex - price_to_beat| / price_to_beat beyond this = unstable
    fee_rate: float = 0.0                    # Polymarket crypto markets currently show takerBaseFee separately;
    slippage_buffer: float = 0.01            # kept conservative/explicit rather than assumed zero
    adverse_selection_buffer: float = 0.01
    min_ev_threshold: float = 0.0            # EV must be non-negative at minimum; configurable, not asserted-correct
    min_time_to_close_seconds: float = 5.0   # too close to expiry = oracle uncertainty too high


class EvThesisExitConfig(BaseModel):
    """Challenger-only EV/thesis-based exit engine (see strategy/ev_winner_hold.py).
    NOT wired into the live/shadow trade loop -- consumed only by the
    replay/research tooling until a replay comparison shows it improves
    risk-adjusted outcomes over the champion fixed-TP exit logic. Flipping
    `enabled` here does not change live trading behavior by itself; the
    live exit path (ExitConfig / tier_exit_rules) is untouched."""
    enabled: bool = False
    disable_fixed_take_profit: bool = True
    allow_hold_to_resolution: bool = True
    min_ev_to_hold: float = 0.0
    exit_on_thesis_flip: bool = True
    exit_on_oracle_basis_flip: bool = True


class ShadowAggressiveOpportunityConfig(BaseModel):
    """Shadow-only opportunity DISCOVERY amplifier (see strategy/opportunity_engine.py).

    This mode ONLY increases diagnostics, near-miss surfacing, and candidate
    coverage visibility. It is structurally incapable of:
      - enabling live trading (apply_to_live is hard-pinned False and never
        consulted in live_micro/live_full),
      - placing or cancelling any order (it never touches the execution path),
      - accepting a trade the STANDARD pipeline would reject.

    Any opportunity it surfaces beyond what the standard gates already accept
    is classified WATCHLIST_ONLY or RESEARCH_ONLY -- never a (even shadow)
    trade. The require_* flags are asserted-True invariants for the
    STANDARD_QUALIFIED classification: a candidate can only be called
    'qualified' when oracle anchor, executable book, spread, depth, risk, and
    positive EV all hold. They exist so this config can never be edited into
    something that calls a bad candidate qualified."""
    enabled: bool = True
    target_qualified_opportunities_per_hour: int = 12
    force_trade_count: bool = False          # MUST stay False -- never force trades
    require_positive_ev: bool = True         # MUST stay True
    require_oracle_anchor: bool = True        # MUST stay True
    require_executable_book: bool = True      # MUST stay True
    require_spread_ok: bool = True            # MUST stay True
    require_depth_ok: bool = True             # MUST stay True
    require_risk_ok: bool = True              # MUST stay True
    allow_near_miss_watchlist: bool = True
    allow_shadow_diagnostics_after_early_gate: bool = True
    allow_threshold_research: bool = True
    apply_to_live: bool = False              # MUST stay False -- hard safety lock


class Config(BaseModel):
    mode: ModeConfig = Field(default_factory=ModeConfig)
    profiles: dict[str, ProfileConfig] = Field(default_factory=dict)
    assets: list[str] = Field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    cex: CexConfig = Field(default_factory=CexConfig)
    cex_freshness: CexFreshnessConfig = Field(default_factory=CexFreshnessConfig)
    research_challengers: ResearchChallengersConfig = Field(default_factory=ResearchChallengersConfig)
    polymarket: PolymarketConfig = Field(default_factory=PolymarketConfig)
    ultra_short_expiry: UltraShortExpiryConfig = Field(default_factory=UltraShortExpiryConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    probability_model: ProbabilityModelConfig = Field(default_factory=ProbabilityModelConfig)
    market_quality: MarketQualityConfig = Field(default_factory=MarketQualityConfig)
    dynamic_edge: DynamicEdgeConfig = Field(default_factory=DynamicEdgeConfig)
    microstructure: MicrostructureConfig = Field(default_factory=MicrostructureConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    capital_scaling: CapitalScalingConfig = Field(default_factory=CapitalScalingConfig)
    profit_lock: ProfitLockConfig = Field(default_factory=ProfitLockConfig)
    micro_bankroll_mode: MicroBankrollConfig = Field(default_factory=MicroBankrollConfig)
    exit: ExitConfig = Field(default_factory=ExitConfig)
    tier_exit_rules: dict[str, TierExitRule] = Field(default_factory=lambda: {
        "A_PLUS": TierExitRule(max_hold_seconds=180, take_profit_pct=0.14,
                               stop_loss_pct=0.10, close_when_edge_below=0.005),
        "A": TierExitRule(max_hold_seconds=150, take_profit_pct=0.12,
                          stop_loss_pct=0.09, close_when_edge_below=0.007),
        "B": TierExitRule(max_hold_seconds=90, take_profit_pct=0.08,
                          stop_loss_pct=0.06, close_when_edge_below=0.010),
    })
    sell_execution: SellExecutionConfig = Field(default_factory=SellExecutionConfig)
    position_rules: PositionRulesConfig = Field(default_factory=PositionRulesConfig)
    rebalance: RebalanceConfig = Field(default_factory=RebalanceConfig)
    execution_pricing: ExecutionPricingConfig = Field(default_factory=ExecutionPricingConfig)
    adaptive_aggression: AdaptiveAggressionConfig = Field(default_factory=AdaptiveAggressionConfig)
    trade_frequency: TradeFrequencyConfig = Field(default_factory=TradeFrequencyConfig)
    auto_tuning: AutoTuningConfig = Field(default_factory=AutoTuningConfig)
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    agent_export: AgentExportConfig = Field(default_factory=AgentExportConfig)
    obsidian: ObsidianConfig = Field(default_factory=ObsidianConfig)
    oracle_ev: OracleEvConfig = Field(default_factory=OracleEvConfig)
    ev_thesis_exit: EvThesisExitConfig = Field(default_factory=EvThesisExitConfig)
    shadow_aggressive_opportunity_mode: ShadowAggressiveOpportunityConfig = Field(
        default_factory=ShadowAggressiveOpportunityConfig)

    @property
    def trading_mode(self) -> TradingMode:
        return TradingMode(self.mode.trading_mode)

    @property
    def default_aggression(self) -> AggressionMode:
        prof = self.profiles.get(self.mode.profile)
        if prof and prof.aggression_default:
            return AggressionMode(prof.aggression_default)
        return AggressionMode(self.adaptive_aggression.default_mode)


# ---------------------------------------------------------------------------
# Secrets (from .env / environment only — never persisted, never repr'd)
# ---------------------------------------------------------------------------

class Secrets:
    """Holds secret values. repr/str never reveal contents."""

    FIELDS = (
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
        "POLYMARKET_PRIVATE_KEY", "POLYMARKET_API_KEY", "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE", "POLYMARKET_FUNDER_ADDRESS",
        "POLYMARKET_SIGNATURE_TYPE",
        "DASHBOARD_USERNAME", "DASHBOARD_PASSWORD",
    )

    def __init__(self, env: Optional[dict[str, str]] = None):
        src = env if env is not None else os.environ
        self._values: dict[str, str] = {k: src.get(k, "") for k in self.FIELDS}
        # Non-secret runtime toggles read from env
        self.live_trading_enabled = str(src.get("LIVE_TRADING_ENABLED", "false")).lower() == "true"
        self.i_understand_risk = str(src.get("I_UNDERSTAND_REAL_MONEY_RISK", "false")).lower() == "true"
        try:
            self.max_real_trade_usd = float(src.get("MAX_REAL_TRADE_USD", "1") or 0)
        except ValueError:
            self.max_real_trade_usd = 0.0
        self.dashboard_auth_enabled = str(src.get("DASHBOARD_AUTH_ENABLED", "true")).lower() == "true"
        self.database_url = src.get("DATABASE_URL", "sqlite:///storage/poly_alpha_sniper.db")
        self.bot_timezone = src.get("BOT_TIMEZONE", "Asia/Jakarta")
        self.log_level = src.get("LOG_LEVEL", "INFO")

    def get(self, key: str) -> str:
        return self._values.get(key, "")

    def has(self, key: str) -> bool:
        return bool(self._values.get(key, ""))

    def __repr__(self) -> str:  # pragma: no cover - safety net
        present = [k for k, v in self._values.items() if v]
        return f"Secrets(present={present})"

    __str__ = __repr__


def parse_env_file(path: Path) -> dict[str, str]:
    """Robust .env parser.

    Handles: UTF-8 with/without BOM (utf-16 fallback for PowerShell-written
    files), `export KEY=...` prefixes, quoted values, and inline comments
    after whitespace on unquoted values (`KEY=val  # note`).
    """
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = path.read_text(encoding="utf-16")
        except (UnicodeError, OSError):
            return {}
    except OSError:
        return {}
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if value[:1] in ('"', "'") and value[-1:] == value[:1] and len(value) >= 2:
            value = value[1:-1]  # quoted: keep contents verbatim
        else:
            # unquoted: strip inline comments introduced by whitespace + '#'
            for sep in (" #", "\t#"):
                idx = value.find(sep)
                if idx != -1:
                    value = value[:idx].rstrip()
        if key:
            out[key] = value
    return out


def load_dotenv_file(path: Optional[Path] = None, override: bool = True) -> dict[str, str]:
    """Load the project .env into os.environ.

    The project .env is the CANONICAL secrets source: by default it OVERRIDES
    any same-named variable already in the process environment, so a stale
    shell variable can never silently supersede the file. (Pass
    override=False for classic fill-only-missing behavior.)
    """
    env_path = path or (PROJECT_ROOT / ".env")
    values = parse_env_file(env_path)
    for key, value in values.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return values


def load_config(path: Optional[str | Path] = None,
                mode_override: Optional[str] = None,
                profile_override: Optional[str] = None) -> Config:
    """Load config.yaml, apply profile, apply CLI mode override."""
    from poly_alpha_sniper.core.profile_loader import apply_profile

    cfg_path = Path(path) if path else (PROJECT_ROOT / "config.yaml")
    raw: dict[str, Any] = {}
    if cfg_path.exists():
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    cfg = Config.model_validate(raw)
    if profile_override:
        cfg.mode.profile = profile_override
    cfg = apply_profile(cfg, cfg.mode.profile)
    if mode_override:
        cfg.mode.trading_mode = mode_override
        if mode_override in (TradingMode.SHADOW_LIVE.value, TradingMode.SIMULATION.value):
            cfg.mode.dry_run = True
    return cfg


def load_secrets() -> Secrets:
    """Build Secrets with .env-canonical precedence.

    Merge order: process environment first, then the project .env file ON TOP
    — the file wins for every key it defines. This guarantees e.g.
    TELEGRAM_CHAT_ID is passed exactly as written in
    <project>/.env regardless of stale shell variables.
    """
    file_values = load_dotenv_file()  # also syncs os.environ (redaction filter)
    merged: dict[str, str] = dict(os.environ)
    merged.update(file_values)
    return Secrets(env=merged)
