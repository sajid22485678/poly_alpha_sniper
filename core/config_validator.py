"""Config + environment validation.

validate_config(cfg) -> list of error strings (empty = valid).
validate_live_env(cfg, secrets) -> live-gate errors (empty = live permitted
at the *config/env* level; runtime live_readiness adds the operational gates).

These functions are pure and shared by preflight, tests, and the dashboard.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import AggressionMode, TradingMode


def validate_config(cfg) -> list[str]:
    errors: list[str] = []

    # mode
    try:
        mode = TradingMode(cfg.mode.trading_mode)
    except ValueError:
        errors.append(f"mode.trading_mode invalid: {cfg.mode.trading_mode!r}")
        mode = None

    if mode and mode.is_live and cfg.mode.dry_run:
        errors.append("live mode with dry_run=true is contradictory; pick one")

    try:
        AggressionMode(cfg.adaptive_aggression.default_mode)
    except ValueError:
        errors.append(
            f"adaptive_aggression.default_mode invalid: {cfg.adaptive_aggression.default_mode!r}")

    # assets
    if not cfg.assets:
        errors.append("assets list is empty")
    for a in cfg.assets:
        if a not in cfg.cex.symbols:
            errors.append(f"asset {a} has no CEX symbol mapping")

    # risk sanity
    r = cfg.risk
    if r.starting_bankroll_usd <= 0:
        errors.append("risk.starting_bankroll_usd must be > 0")
    if not (0 < r.position_size_pct_equity <= 1):
        errors.append("risk.position_size_pct_equity must be in (0, 1]")
    if r.min_trade_usd <= 0 or r.max_trade_usd <= 0:
        errors.append("risk trade sizes must be > 0")
    if r.min_trade_usd > r.max_trade_usd:
        errors.append("risk.min_trade_usd > risk.max_trade_usd")
    if r.max_daily_loss_usd <= 0:
        errors.append("risk.max_daily_loss_usd must be > 0")
    if r.max_open_positions < 1:
        errors.append("risk.max_open_positions must be >= 1")
    if not (0 < r.max_total_exposure_pct_equity <= 1):
        errors.append("risk.max_total_exposure_pct_equity must be in (0, 1]")
    if r.max_market_exposure_pct_equity > r.max_total_exposure_pct_equity:
        errors.append("per-market exposure cap exceeds total exposure cap")

    # expiry window sanity
    u = cfg.ultra_short_expiry
    if u.min_time_to_expiry_seconds >= u.max_time_to_expiry_seconds:
        errors.append("ultra_short_expiry: min_time_to_expiry >= max_time_to_expiry")
    if u.force_exit_before_expiry_seconds >= u.min_time_to_expiry_seconds:
        errors.append("ultra_short_expiry: force_exit window >= min time-to-expiry")

    # cex freshness tiers (shadow-only, but ordering must always be sane)
    cf = cfg.cex_freshness
    if not (0 < cf.live_signal_max_age_ms <= cf.shadow_eval_max_age_ms <= cf.fail_closed_max_age_ms):
        errors.append("cex_freshness thresholds must satisfy "
                      "0 < live_signal_max_age_ms <= shadow_eval_max_age_ms <= fail_closed_max_age_ms")

    # edges
    d = cfg.dynamic_edge
    if d.hard_min_edge <= 0:
        errors.append("dynamic_edge.hard_min_edge must be > 0")
    if d.live_micro_min_edge_floor < d.hard_min_edge:
        errors.append("live_micro edge floor below hard_min_edge")

    # probability model
    p = cfg.probability_model
    if not (0 < p.clamp_min < p.clamp_max < 1):
        errors.append("probability_model clamps invalid")

    # microstructure
    if cfg.microstructure.max_spread <= 0:
        errors.append("microstructure.max_spread must be > 0")
    if cfg.microstructure.max_slippage_bps <= 0:
        errors.append("microstructure.max_slippage_bps must be > 0")

    # tier exit rules must exist for tradable tiers
    for tier in ("A_PLUS", "A", "B"):
        if tier not in cfg.tier_exit_rules:
            errors.append(f"tier_exit_rules missing {tier}")

    # execution
    if cfg.execution_pricing.use_market_orders:
        errors.append("execution_pricing.use_market_orders must be false (limit-only policy)")

    # dashboard
    if not (1 <= cfg.dashboard.port <= 65535):
        errors.append("dashboard.port out of range")

    # frequency
    tf = cfg.trade_frequency
    if tf.max_trades_per_hour_live_micro < tf.target_trades_per_hour_live_micro:
        errors.append("trade_frequency: live max < live target")

    return errors


def validate_live_env(cfg, secrets) -> list[str]:
    """Config/env-level live gates. ALL must pass before live order code paths
    are even constructed. Runtime gates live in core/live_readiness.py."""
    errors: list[str] = []
    mode = TradingMode(cfg.mode.trading_mode)
    if not mode.is_live:
        errors.append(f"trading_mode {mode.value} is not a live mode")
    if cfg.mode.dry_run:
        errors.append("dry_run must be false for live trading")
    if not secrets.live_trading_enabled:
        errors.append("LIVE_TRADING_ENABLED must be true")
    if not secrets.i_understand_risk:
        errors.append("I_UNDERSTAND_REAL_MONEY_RISK must be true")
    if secrets.max_real_trade_usd <= 0:
        errors.append("MAX_REAL_TRADE_USD must be > 0")
    if cfg.risk.max_trade_usd > secrets.max_real_trade_usd:
        errors.append(
            f"risk.max_trade_usd ({cfg.risk.max_trade_usd}) exceeds "
            f"MAX_REAL_TRADE_USD ({secrets.max_real_trade_usd})")
    if not secrets.has("POLYMARKET_PRIVATE_KEY"):
        errors.append("POLYMARKET_PRIVATE_KEY missing from .env")
    if not secrets.has("POLYMARKET_FUNDER_ADDRESS"):
        errors.append("POLYMARKET_FUNDER_ADDRESS missing from .env")
    if cfg.telegram.enabled and not secrets.has("TELEGRAM_BOT_TOKEN"):
        errors.append("Telegram enabled but TELEGRAM_BOT_TOKEN missing")
    return errors
