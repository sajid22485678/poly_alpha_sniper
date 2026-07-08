"""Config validation + live-blocked-by-default proofs."""
from poly_alpha_sniper.core.config_loader import Config, Secrets, load_config
from poly_alpha_sniper.core.config_validator import validate_config, validate_live_env


def _cfg() -> Config:
    return load_config()


def test_default_config_valid():
    assert validate_config(_cfg()) == []


def test_bad_mode_caught():
    cfg = _cfg()
    cfg.mode.trading_mode = "yolo"
    assert any("trading_mode" in e for e in validate_config(cfg))


def test_live_with_dry_run_contradiction():
    cfg = _cfg()
    cfg.mode.trading_mode = "live_micro"
    cfg.mode.dry_run = True
    assert any("dry_run" in e for e in validate_config(cfg))


def test_min_over_max_trade():
    cfg = _cfg()
    cfg.risk.min_trade_usd = 5
    cfg.risk.max_trade_usd = 1
    assert any("min_trade_usd" in e for e in validate_config(cfg))


def test_market_orders_forbidden():
    cfg = _cfg()
    cfg.execution_pricing.use_market_orders = True
    assert any("market_orders" in e for e in validate_config(cfg))


def test_exposure_cap_relation():
    cfg = _cfg()
    cfg.risk.max_market_exposure_pct_equity = 0.9
    cfg.risk.max_total_exposure_pct_equity = 0.3
    assert any("exposure" in e for e in validate_config(cfg))


def test_live_blocked_by_default():
    """THE core safety proof: default config + empty env can never go live."""
    cfg = _cfg()
    cfg.mode.trading_mode = "live_micro"
    cfg.mode.dry_run = False
    secrets = Secrets(env={})  # empty environment
    errors = validate_live_env(cfg, secrets)
    joined = " ".join(errors)
    assert "LIVE_TRADING_ENABLED" in joined
    assert "I_UNDERSTAND_REAL_MONEY_RISK" in joined
    assert "POLYMARKET_PRIVATE_KEY" in joined


def test_shadow_mode_is_not_live():
    cfg = _cfg()  # defaults: shadow_live, dry_run True
    secrets = Secrets(env={})
    errors = validate_live_env(cfg, secrets)
    assert any("not a live mode" in e for e in errors)


def test_max_trade_exceeding_env_cap_caught():
    cfg = _cfg()
    cfg.mode.trading_mode = "live_micro"
    cfg.mode.dry_run = False
    cfg.risk.max_trade_usd = 5
    secrets = Secrets(env={
        "LIVE_TRADING_ENABLED": "true", "I_UNDERSTAND_REAL_MONEY_RISK": "true",
        "MAX_REAL_TRADE_USD": "1", "POLYMARKET_PRIVATE_KEY": "x" * 64,
        "POLYMARKET_FUNDER_ADDRESS": "0x" + "a" * 40, "TELEGRAM_BOT_TOKEN": "t"})
    errors = validate_live_env(cfg, secrets)
    assert any("MAX_REAL_TRADE_USD" in e for e in errors)
