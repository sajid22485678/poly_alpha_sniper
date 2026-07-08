"""Profile application: named override sets on top of base config.

Profiles may only touch a whitelisted set of fields. A profile can never relax
hard risk caps (max_trade_usd, daily loss caps, exposure caps) — those change
only by editing config.yaml directly.
"""
from __future__ import annotations

from poly_alpha_sniper.core.contracts import TradingMode
from poly_alpha_sniper.core.logger import get_logger

log = get_logger("profile_loader")

KNOWN_PROFILES = (
    "safe_shadow", "aggressive_shadow", "live_micro_safe",
    "live_micro_aggressive", "backtest_5min",
)


class ProfileError(ValueError):
    pass


def apply_profile(cfg, profile_name: str):
    """Apply a named profile's overrides to a Config instance (mutates + returns)."""
    if not profile_name:
        return cfg
    prof = cfg.profiles.get(profile_name)
    if prof is None:
        raise ProfileError(
            f"Unknown profile '{profile_name}'. Known: {sorted(cfg.profiles) or list(KNOWN_PROFILES)}")

    if prof.trading_mode is not None:
        TradingMode(prof.trading_mode)  # raises on invalid value
        cfg.mode.trading_mode = prof.trading_mode
    if prof.dry_run is not None:
        cfg.mode.dry_run = prof.dry_run
    if prof.aggression_default is not None:
        cfg.adaptive_aggression.default_mode = prof.aggression_default
    if prof.ultra_short_expiry_enabled is not None:
        cfg.ultra_short_expiry.enabled = prof.ultra_short_expiry_enabled

    cfg.mode.profile = profile_name
    log.info("profile_applied", extra={"extra": {
        "profile": profile_name,
        "trading_mode": cfg.mode.trading_mode,
        "dry_run": cfg.mode.dry_run,
        "aggression_default": cfg.adaptive_aggression.default_mode}})
    return cfg
