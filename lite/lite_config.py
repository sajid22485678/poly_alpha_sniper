"""LITE SHADOW ONLY: config for Poly Alpha Lite V1.

Deliberately NOT part of the advanced pydantic Config: Lite loads its own
config_lite.yaml (optional -- code defaults below are authoritative) so the
advanced loader and its gates are never touched. ``max_trade_usd`` exists only
as an explicit disabled compatibility flag; it is never consulted for sizing.
Sizing is a fixed five shares, full stop."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

LITE_ROOT = Path(__file__).resolve().parents[1]
FIXED_SHARES = 5.0  # the only size Lite ever records


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
    max_open_per_asset: int = 2
    max_one_per_asset_window_side: bool = True
    cex_max_age_ms: int = 8000
    book_max_age_ms: int = 8000
    momentum_windows_s: list[int] = field(default_factory=lambda: [10, 30, 60])
    momentum_min_pct: float = 0.0002   # 0.02%
    max_spread: float = 0.20
    min_depth_usd: float = 1.0
    time_to_close_min_s: float = 20.0
    time_to_close_max_s: float = 280.0
    exit_before_close_s: float = 15.0
    anchor_optional: bool = True
    allow_no_anchor_trades: bool = True
    require_fired: bool = False
    require_imbalance: bool = False
    require_oracle: bool = False
    allow_book_exit: bool = True
    allow_official_resolution: bool = True
    resolver_retry_seconds: float = 30.0
    resolver_max_retries: int = 30
    scan_interval_s: float = 3.0
    reject_bucket_s: int = 30          # reject rows throttled per bucket
    db_path: str = str(LITE_ROOT / "data" / "poly_alpha_lite.db")
    export_dir: str = "D:/claude/agent_readonly/poly_alpha_lite"
    runtime_dir: str = str(LITE_ROOT / "runtime" / "lite_shadow")
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"


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
            block = raw.get("lite_shadow", {}) if isinstance(raw, dict) else {}
            for key, value in (block or {}).items():
                if hasattr(cfg, key) and key not in (
                    "mode", "dry_run", "live_enabled", "fixed_order_shares",
                    "use_max_trade_usd", "max_trade_usd",
                ):
                    setattr(cfg, key, value)
        except Exception:  # noqa: BLE001 -- bad yaml falls back to safe defaults
            pass
    # hard locks, regardless of any file content
    cfg.mode = "lite_shadow"
    cfg.dry_run = True
    cfg.live_enabled = False
    cfg.fixed_order_shares = FIXED_SHARES
    cfg.use_max_trade_usd = False
    cfg.max_trade_usd = None
    cfg.require_fired = False
    cfg.require_imbalance = False
    cfg.require_oracle = False
    return cfg
