import pytest

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.core.profile_loader import ProfileError, apply_profile


def test_safe_shadow_applies():
    cfg = load_config(profile_override="safe_shadow")
    assert cfg.mode.trading_mode == "shadow_live"
    assert cfg.mode.dry_run is True
    assert cfg.adaptive_aggression.default_mode == "NORMAL"


def test_aggressive_shadow():
    cfg = load_config(profile_override="aggressive_shadow")
    assert cfg.mode.dry_run is True
    assert cfg.adaptive_aggression.default_mode == "AGGRESSIVE"


def test_live_micro_safe_flips_mode():
    cfg = load_config(profile_override="live_micro_safe")
    assert cfg.mode.trading_mode == "live_micro"
    assert cfg.mode.dry_run is False


def test_backtest_profile_sets_simulation():
    cfg = load_config(profile_override="backtest_5min")
    assert cfg.mode.trading_mode == "simulation"
    assert cfg.ultra_short_expiry.enabled is True


def test_unknown_profile_raises():
    cfg = load_config()
    with pytest.raises(ProfileError):
        apply_profile(cfg, "does_not_exist")
