"""LITE SHADOW ONLY: config for Poly Alpha Lite V1.

Deliberately NOT part of the advanced pydantic Config: Lite loads its own
config_lite.yaml (optional -- code defaults below are authoritative) so the
advanced loader and its gates are never touched. ``max_trade_usd`` exists only
as an explicit disabled compatibility flag; it is never consulted for sizing.
Sizing is a fixed five shares, full stop."""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from pathlib import Path

LITE_ROOT = Path(__file__).resolve().parents[1]
FIXED_SHARES = 5.0  # the only size Lite ever records
LITE_DB_PATH = str(LITE_ROOT / "data" / "poly_alpha_lite.db")
LITE_RUNTIME_DIR = str(LITE_ROOT / "runtime" / "lite_shadow")
LITE_EXPORT_DIR = "D:/claude/agent_readonly/poly_alpha_lite"
LITE_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
LITE_CLOB_BASE_URL = "https://clob.polymarket.com"


@dataclass
class LiteConfig:
    enabled: bool = True
    mode: str = "lite_shadow"          # never a live mode
    dry_run: bool = True               # hard-locked; load() re-asserts
    live_enabled: bool = False         # hard-locked; load() re-asserts
    assets: list[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    fixed_order_shares: float = FIXED_SHARES
    use_max_trade_usd: bool = False
    max_trade_usd: None = None         # sentinel only; never a sizing input
    max_open_positions: int = 6
    max_open_per_asset: int = 1
    max_one_per_asset_window: bool = True
    cex_max_age_ms: int = 8000
    book_max_age_ms: int = 8000
    momentum_windows_s: list[int] = field(default_factory=lambda: [5, 10, 30, 60])
    momentum_min_pct: float = 0.0002   # 0.02%
    fair_value_max_adjustment: float = 0.12
    fair_value_signal_scale: float = 2.0
    fair_value_uncertainty_buffer: float = 0.015
    min_net_edge: float = 0.005
    min_cross_edge: float = 0.010
    execution_buffer_base: float = 0.002
    execution_buffer_spread_fraction: float = 0.05
    max_book_pair_skew_ms: int = 2000
    maker_wait_s: float = 4.0
    lead_lag_min_cex_move: float = 0.0002
    lead_lag_max_ms: int = 8000
    lead_lag_max_adjustment: float = 0.015
    exit_hold_uncertainty: float = 0.02
    exit_value_margin_usd: float = 0.02
    thesis_invalidation_score: float = 0.60
    max_chase_worsening: float = 0.02
    max_spread: float = 0.20
    time_to_close_min_s: float = 20.0
    time_to_close_max_s: float = 280.0
    anchor_optional: bool = True
    allow_no_anchor_trades: bool = True
    require_fired: bool = False
    require_imbalance: bool = False
    require_oracle: bool = False
    allow_book_exit: bool = True
    allow_official_resolution: bool = True
    resolver_retry_seconds: float = 30.0
    resolver_retry_cap_seconds: float = 300.0
    resolver_final_recheck_seconds: float = 3600.0
    resolver_max_retries: int = 30
    resolver_batch_size: int = 12
    crypto_taker_fee_rate: float = 0.07
    live_small_equity_usd: float = 13.0
    equity_exposure_cap_pct: float = 0.75
    fee_buffer_usd: float = 0.02
    shadow_live_small_preview: bool = True
    live_kill_switch_engaged: bool = True
    max_daily_realized_loss_usd: float = 2.0
    max_consecutive_losses: int = 3
    scan_interval_s: float = 3.0
    reject_bucket_s: int = 30          # reject rows throttled per bucket
    db_path: str = LITE_DB_PATH
    export_dir: str = LITE_EXPORT_DIR
    runtime_dir: str = LITE_RUNTIME_DIR
    gamma_base_url: str = LITE_GAMMA_BASE_URL
    clob_base_url: str = LITE_CLOB_BASE_URL


def _validate_lite_config(cfg: LiteConfig) -> None:
    def fail(name: str) -> None:
        raise RuntimeError(f"invalid Lite config field: {name}")

    bool_fields = (
        "enabled", "max_one_per_asset_window", "anchor_optional",
        "allow_no_anchor_trades", "require_fired", "require_imbalance",
        "require_oracle", "allow_book_exit", "allow_official_resolution",
        "shadow_live_small_preview", "live_kill_switch_engaged",
    )
    for name in bool_fields:
        if type(getattr(cfg, name)) is not bool:
            fail(name)

    def number(name: str, *, minimum: float | None = None,
               maximum: float | None = None, strict_min: bool = False) -> float:
        value = getattr(cfg, name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            fail(name)
        parsed = float(value)
        if not math.isfinite(parsed):
            fail(name)
        if minimum is not None and (parsed <= minimum if strict_min else parsed < minimum):
            fail(name)
        if maximum is not None and parsed > maximum:
            fail(name)
        return parsed

    def integer(name: str, *, minimum: int = 1, maximum: int | None = None) -> int:
        value = getattr(cfg, name)
        if isinstance(value, bool) or not isinstance(value, int):
            fail(name)
        if value < minimum or (maximum is not None and value > maximum):
            fail(name)
        return value

    integer("max_open_positions", maximum=6)
    integer("max_open_per_asset", maximum=3)
    integer("cex_max_age_ms", maximum=60_000)
    book_age = integer("book_max_age_ms", maximum=60_000)
    pair_skew = integer("max_book_pair_skew_ms", minimum=0, maximum=60_000)
    if pair_skew > book_age:
        fail("max_book_pair_skew_ms")
    lead_lag_max = integer("lead_lag_max_ms", minimum=1, maximum=60_000)
    if lead_lag_max > int(cfg.cex_max_age_ms):
        fail("lead_lag_max_ms")
    integer("resolver_max_retries", maximum=10_000)
    integer("resolver_batch_size", maximum=100)
    integer("max_consecutive_losses", maximum=100)
    integer("reject_bucket_s", maximum=3_600)
    number("momentum_min_pct", minimum=0.0, maximum=0.1, strict_min=True)
    fair_adjustment = number(
        "fair_value_max_adjustment", minimum=0.0, maximum=0.2,
        strict_min=True)
    number("fair_value_signal_scale", minimum=0.0, maximum=10.0,
           strict_min=True)
    uncertainty = number(
        "fair_value_uncertainty_buffer", minimum=0.0, maximum=0.1)
    if uncertainty >= fair_adjustment:
        fail("fair_value_uncertainty_buffer")
    min_net_edge = number("min_net_edge", minimum=0.0, maximum=0.2)
    min_cross_edge = number("min_cross_edge", minimum=0.0, maximum=0.2)
    if min_cross_edge < min_net_edge:
        fail("min_cross_edge")
    number("execution_buffer_base", minimum=0.0, maximum=0.1)
    number("execution_buffer_spread_fraction", minimum=0.0, maximum=1.0)
    number("maker_wait_s", minimum=0.0, maximum=60.0, strict_min=True)
    number("lead_lag_min_cex_move", minimum=0.0, maximum=0.1,
           strict_min=True)
    lead_adjustment = number(
        "lead_lag_max_adjustment", minimum=0.0, maximum=0.05)
    if lead_adjustment > fair_adjustment:
        fail("lead_lag_max_adjustment")
    number("exit_hold_uncertainty", minimum=0.0, maximum=1.0)
    number("exit_value_margin_usd", minimum=0.0, maximum=100.0)
    number("thesis_invalidation_score", minimum=0.0, maximum=1.0)
    number("max_chase_worsening", minimum=0.0, maximum=0.5)
    number("max_spread", minimum=0.0, maximum=1.0)
    close_min = number("time_to_close_min_s", minimum=0.0, maximum=299.0)
    close_max = number("time_to_close_max_s", minimum=0.0, maximum=300.0,
                       strict_min=True)
    if close_max <= close_min:
        fail("time_to_close_max_s")
    retry_base = number("resolver_retry_seconds", minimum=0.0,
                        maximum=3_600.0, strict_min=True)
    retry_cap = number("resolver_retry_cap_seconds", minimum=0.0,
                       maximum=86_400.0, strict_min=True)
    if retry_cap < retry_base:
        fail("resolver_retry_cap_seconds")
    number("resolver_final_recheck_seconds", minimum=0.0,
           maximum=604_800.0, strict_min=True)
    number("crypto_taker_fee_rate", minimum=0.0, maximum=1.0)
    number("live_small_equity_usd", minimum=0.0, maximum=1_000_000.0,
           strict_min=True)
    number("equity_exposure_cap_pct", minimum=0.0, maximum=1.0,
           strict_min=True)
    number("fee_buffer_usd", minimum=0.0, maximum=100.0)
    number("max_daily_realized_loss_usd", minimum=0.0, maximum=1_000_000.0,
           strict_min=True)
    number("scan_interval_s", minimum=0.0, maximum=60.0, strict_min=True)


def load_lite_config(path: str | None = None) -> LiteConfig:
    """Defaults + optional overrides from config_lite.yaml's lite_shadow block.
    Safety fields (dry_run / live_enabled / mode) are re-asserted after the
    merge -- a config file can never make Lite live."""
    cfg = LiteConfig()
    yaml_path = Path(path) if path else (LITE_ROOT / "config_lite.yaml")
    if yaml_path.exists():
        try:
            import yaml
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
            if not isinstance(raw, dict):
                raise TypeError("root must be a mapping")
            block = raw.get("lite_shadow", {})
            if not isinstance(block, dict):
                raise TypeError("lite_shadow must be a mapping")
            unknown = set(block) - set(LiteConfig.__dataclass_fields__)
            if unknown:
                raise KeyError("unknown Lite fields")
            for key, value in (block or {}).items():
                if hasattr(cfg, key) and key not in (
                    "mode", "dry_run", "live_enabled", "fixed_order_shares",
                    "use_max_trade_usd", "max_trade_usd", "db_path",
                    "runtime_dir", "export_dir", "live_kill_switch_engaged",
                    "assets", "momentum_windows_s", "gamma_base_url",
                    "clob_base_url", "max_one_per_asset_window",
                    "crypto_taker_fee_rate", "equity_exposure_cap_pct",
                    "live_small_equity_usd", "max_daily_realized_loss_usd",
                    "max_consecutive_losses", "enabled", "anchor_optional",
                    "allow_no_anchor_trades", "shadow_live_small_preview",
                ):
                    setattr(cfg, key, value)
        except Exception as exc:
            raise RuntimeError(f"invalid Lite config: {type(exc).__name__}") from exc
    # hard locks, regardless of any file content
    cfg.mode = "lite_shadow"
    cfg.dry_run = True
    cfg.live_enabled = False
    cfg.fixed_order_shares = FIXED_SHARES
    cfg.use_max_trade_usd = False
    cfg.max_trade_usd = None
    cfg.db_path = LITE_DB_PATH
    cfg.runtime_dir = LITE_RUNTIME_DIR
    cfg.export_dir = LITE_EXPORT_DIR
    cfg.live_kill_switch_engaged = True
    cfg.enabled = True
    cfg.assets = ["BTC", "ETH", "SOL"]
    cfg.momentum_windows_s = [5, 10, 30, 60]
    cfg.gamma_base_url = LITE_GAMMA_BASE_URL
    cfg.clob_base_url = LITE_CLOB_BASE_URL
    cfg.max_one_per_asset_window = True
    cfg.anchor_optional = True
    cfg.allow_no_anchor_trades = True
    cfg.shadow_live_small_preview = True
    cfg.crypto_taker_fee_rate = 0.07
    cfg.live_small_equity_usd = 13.0
    cfg.equity_exposure_cap_pct = 0.75
    cfg.max_daily_realized_loss_usd = 2.0
    cfg.max_consecutive_losses = 3
    cfg.require_fired = False
    cfg.require_imbalance = False
    cfg.require_oracle = False
    _validate_lite_config(cfg)
    return cfg
