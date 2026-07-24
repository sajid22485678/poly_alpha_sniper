"""Fail-closed configuration for the isolated Frequency V4 shadow lane."""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import Any


FREQUENCY_V4_ROOT = Path(__file__).resolve().parents[1]
STRATEGY_ID = "lite_frequency_v4"
MODE = "lite_frequency_v4_shadow"
FIXED_SHARES = 5.0

# Phase 1 forward-cohort identity.  The active cohort owns the authoritative
# $13 shadow ledger; every earlier row remains under the legacy cohort and is
# reported as non-authoritative.
ACTIVE_COHORT = "dynamic_universe_phase1_post_activation"
LEGACY_COHORT = "legacy_mixed_universe"
RUNTIME_LABEL = (
    "DYNAMIC_MULTI_ASSET_ULTRA_AGGRESSIVE_13_USD_100_PERCENT_EXPOSURE_"
    "SHADOW_PHASE1"
)

FREQUENCY_V4_DB_PATH = str(
    FREQUENCY_V4_ROOT / "data" / "poly_alpha_frequency_v4.db")
FREQUENCY_V4_RUNTIME_DIR = str(
    FREQUENCY_V4_ROOT / "runtime" / MODE)
# Phase 2B C1 isolation: the dashboard export directory is derived from the
# source root, not hardcoded.  When the source root is the live deployment
# (FREQUENCY_V4_ROOT = .../poly_alpha_sniper) this resolves to the existing
# live path D:/claude/agent_readonly/poly_alpha_frequency_v4, preserving the
# deployed path exactly.  When the source root is an isolated staging
# checkout, the export is isolated under that staging parent so tests and
# staging runs never write to the production export directory.
FREQUENCY_V4_EXPORT_DIR = str(
    FREQUENCY_V4_ROOT.parent / "agent_readonly" / "poly_alpha_frequency_v4"
)

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

# Short aliases kept explicit for orchestrators and operational scripts.
V4_DB_PATH = FREQUENCY_V4_DB_PATH
V4_RUNTIME_DIR = FREQUENCY_V4_RUNTIME_DIR
V4_EXPORT_DIR = FREQUENCY_V4_EXPORT_DIR


@dataclass(slots=True)
class FrequencyV4Config:
    # Permanent identity and live-safety locks.
    strategy_id: str = STRATEGY_ID
    mode: str = MODE
    enabled: bool = True
    dry_run: bool = True
    live_enabled: bool = False
    real_orders_possible: bool = False
    live_adapter_present: bool = False
    kill_switch_engaged: bool = True
    fixed_shares: float = FIXED_SHARES

    db_path: str = FREQUENCY_V4_DB_PATH
    runtime_dir: str = FREQUENCY_V4_RUNTIME_DIR
    export_dir: str = FREQUENCY_V4_EXPORT_DIR
    gamma_base_url: str = GAMMA_BASE_URL
    clob_base_url: str = CLOB_BASE_URL
    clob_ws_url: str = CLOB_WS_URL
    okx_ws_url: str = OKX_WS_URL
    primary_cex_provider: str = "okx"

    required_assets: list[str] = field(
        default_factory=lambda: ["BTC", "ETH", "SOL"])
    discover_additional_assets: bool = True
    exact_window_seconds: int = 300
    discovery_interval_s: float = 20.0
    discovery_lookahead_s: int = 1_800
    broad_discovery_limit: int = 500
    minimum_remaining_s: float = 10.0

    cex_max_age_ms: int = 2_000
    book_max_age_ms: int = 2_000
    max_book_pair_skew_ms: int = 1_500
    max_spread: float = 0.20
    reconnect_base_ms: int = 250
    reconnect_cap_ms: int = 15_000
    heartbeat_timeout_ms: int = 15_000

    # Transparent initial research tiers.  A quota never alters these gates.
    strong_cross_edge: float = 0.020
    medium_maker_edge: float = 0.010
    weak_observe_edge: float = 0.005
    maker_observation_min_ms: int = 500
    maker_observation_default_ms: int = 1_000
    maker_observation_max_ms: int = 1_500
    max_chase_worsening: float = 0.020

    execution_buffer_base: float = 0.002
    latency_buffer_base: float = 0.001
    uncertainty_buffer_base: float = 0.010
    fair_probability_floor: float = 0.001
    fair_probability_ceiling: float = 0.999
    max_model_adjustment: float = 0.15

    rest_timeout_s: float = 5.0
    rest_recovery_min_ms: int = 250
    rest_recovery_max_ms: int = 750
    rest_recovery_attempts: int = 3

    max_open_positions: int = 6
    max_open_per_asset: int = 1
    research_equity_usd: float = 13.0
    exposure_cap_pct: float = 1.0
    fee_buffer_usd: float = 0.02
    crypto_taker_fee_rate: float = 0.07

    raw_event_retention_hours: int = 24
    raw_event_max_rows: int = 250_000
    sqlite_busy_timeout_ms: int = 10_000
    # Persistence lanes.  These bounds are operational safety controls; they
    # do not alter strategy signals, execution tiers, or fixed-share sizing.
    critical_queue_capacity: int = 2_048
    telemetry_queue_capacity: int = 20_000
    critical_command_timeout_s: float = 15.0
    telemetry_batch_size: int = 512
    telemetry_flush_interval_ms: int = 250
    telemetry_coalescing_interval_ms: int = 1_000
    writer_heartbeat_interval_ms: int = 1_000
    writer_failure_timeout_ms: int = 5_000
    checkpoint_wal_size_trigger_bytes: int = 32 * 1024 * 1024
    checkpoint_min_interval_s: int = 60
    retention_chunk_size: int = 250
    retention_time_budget_ms: int = 1_000
    reporting_worker_timeout_s: float = 120.0
    maintenance_worker_timeout_s: float = 30.0
    reporting_queue_capacity: int = 16
    maintenance_queue_capacity: int = 8
    writer_queue_max: int = 10_000
    cex_writer_queue_max: int = 10_000
    loop_lag_threshold_ms: int = 250
    loop_lag_safety_ms: int = 2_000
    shutdown_drain_timeout_s: float = 5.0
    maintenance_chunk_rows: int = 250
    maintenance_max_rows_per_pass: int = 4_000
    maintenance_max_seconds_per_pass: float = 1.0

    @property
    def exposure_cap_usd(self) -> float:
        return round(self.research_equity_usd * self.exposure_cap_pct, 10)

    @property
    def safety_state(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "mode": self.mode,
            "dry_run": self.dry_run,
            "live_enabled": self.live_enabled,
            "real_orders_possible": self.real_orders_possible,
            "live_adapter_present": self.live_adapter_present,
            "kill_switch_engaged": self.kill_switch_engaged,
            "fixed_shares": self.fixed_shares,
        }


V4Config = FrequencyV4Config


_HARD_LOCKED_FIELDS = {
    "strategy_id", "mode", "enabled", "dry_run", "live_enabled",
    "real_orders_possible", "live_adapter_present", "kill_switch_engaged",
    "fixed_shares", "db_path", "runtime_dir", "export_dir",
    "gamma_base_url", "clob_base_url", "clob_ws_url", "okx_ws_url",
    "primary_cex_provider", "required_assets", "discover_additional_assets",
    "exact_window_seconds", "crypto_taker_fee_rate", "research_equity_usd",
    "exposure_cap_pct", "max_open_per_asset",
}


def _strict_number(value: Any, name: str, *, minimum: float | None = None,
                   maximum: float | None = None, strict_minimum: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(f"invalid Frequency V4 config field: {name}")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise RuntimeError(f"invalid Frequency V4 config field: {name}")
    if minimum is not None:
        invalid = parsed <= minimum if strict_minimum else parsed < minimum
        if invalid:
            raise RuntimeError(f"invalid Frequency V4 config field: {name}")
    if maximum is not None and parsed > maximum:
        raise RuntimeError(f"invalid Frequency V4 config field: {name}")
    return parsed


def _strict_integer(value: Any, name: str, *, minimum: int = 0,
                    maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"invalid Frequency V4 config field: {name}")
    if value < minimum or (maximum is not None and value > maximum):
        raise RuntimeError(f"invalid Frequency V4 config field: {name}")
    return value


def validate_frequency_v4_config(cfg: FrequencyV4Config) -> None:
    expected = {
        "strategy_id": STRATEGY_ID,
        "mode": MODE,
        "enabled": True,
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "kill_switch_engaged": True,
        "fixed_shares": FIXED_SHARES,
        "db_path": FREQUENCY_V4_DB_PATH,
        "runtime_dir": FREQUENCY_V4_RUNTIME_DIR,
        "export_dir": FREQUENCY_V4_EXPORT_DIR,
        "gamma_base_url": GAMMA_BASE_URL,
        "clob_base_url": CLOB_BASE_URL,
        "clob_ws_url": CLOB_WS_URL,
        "okx_ws_url": OKX_WS_URL,
        "primary_cex_provider": "okx",
        "exact_window_seconds": 300,
        "crypto_taker_fee_rate": 0.07,
        "research_equity_usd": 13.0,
        "exposure_cap_pct": 1.0,
        "max_open_per_asset": 1,
    }
    for name, value in expected.items():
        actual = getattr(cfg, name)
        if name.endswith(("_path", "_dir")):
            if Path(str(actual)).resolve() != Path(str(value)).resolve():
                raise RuntimeError(f"Frequency V4 safety lock failed: {name}")
        elif actual != value or type(actual) is not type(value):
            # int and float equality must not allow boolean safety overrides.
            raise RuntimeError(f"Frequency V4 safety lock failed: {name}")
    if cfg.required_assets != ["BTC", "ETH", "SOL"]:
        raise RuntimeError("Frequency V4 safety lock failed: required_assets")
    if cfg.discover_additional_assets is not True:
        raise RuntimeError("Frequency V4 safety lock failed: broad discovery")

    strict_bools = ("enabled", "dry_run", "live_enabled", "real_orders_possible",
                    "live_adapter_present", "kill_switch_engaged",
                    "discover_additional_assets")
    for name in strict_bools:
        if type(getattr(cfg, name)) is not bool:
            raise RuntimeError(f"invalid Frequency V4 config field: {name}")

    integers = {
        "discovery_lookahead_s": (300, 86_400),
        "broad_discovery_limit": (10, 2_000),
        "cex_max_age_ms": (1, 60_000),
        "book_max_age_ms": (1, 60_000),
        "max_book_pair_skew_ms": (0, 60_000),
        "reconnect_base_ms": (50, 60_000),
        "reconnect_cap_ms": (100, 300_000),
        "heartbeat_timeout_ms": (1_000, 300_000),
        "maker_observation_min_ms": (500, 1_500),
        "maker_observation_default_ms": (500, 1_500),
        "maker_observation_max_ms": (500, 1_500),
        "rest_recovery_min_ms": (250, 750),
        "rest_recovery_max_ms": (250, 750),
        "rest_recovery_attempts": (1, 5),
        "max_open_positions": (1, 100),
        "raw_event_retention_hours": (1, 720),
        "raw_event_max_rows": (1_000, 10_000_000),
        "sqlite_busy_timeout_ms": (100, 120_000),
        "critical_queue_capacity": (16, 1_000_000),
        "telemetry_queue_capacity": (100, 10_000_000),
        "telemetry_batch_size": (1, 100_000),
        "telemetry_flush_interval_ms": (10, 60_000),
        "telemetry_coalescing_interval_ms": (10, 300_000),
        "writer_heartbeat_interval_ms": (100, 60_000),
        "writer_failure_timeout_ms": (500, 300_000),
        "checkpoint_wal_size_trigger_bytes": (1_048_576, 10_737_418_240),
        "checkpoint_min_interval_s": (1, 86_400),
        "retention_chunk_size": (1, 100_000),
        "retention_time_budget_ms": (10, 60_000),
        "reporting_queue_capacity": (1, 10_000),
        "maintenance_queue_capacity": (1, 10_000),
        "writer_queue_max": (100, 1_000_000),
        "cex_writer_queue_max": (100, 1_000_000),
        "loop_lag_threshold_ms": (10, 60_000),
        "loop_lag_safety_ms": (50, 120_000),
        "maintenance_chunk_rows": (1, 100_000),
        "maintenance_max_rows_per_pass": (1, 5_000_000),
    }
    for name, (minimum, maximum) in integers.items():
        _strict_integer(getattr(cfg, name), name, minimum=minimum, maximum=maximum)
    if cfg.loop_lag_safety_ms < cfg.loop_lag_threshold_ms:
        raise RuntimeError("invalid Frequency V4 config field: loop_lag_safety_ms")
    if cfg.maintenance_max_rows_per_pass < cfg.maintenance_chunk_rows:
        raise RuntimeError("invalid Frequency V4 config field: maintenance_max_rows_per_pass")
    if cfg.maintenance_max_rows_per_pass < cfg.retention_chunk_size:
        raise RuntimeError("invalid Frequency V4 config field: retention_chunk_size")
    if cfg.telemetry_queue_capacity < cfg.telemetry_batch_size:
        raise RuntimeError("invalid Frequency V4 config field: telemetry queue/batch")
    if cfg.critical_command_timeout_s * 1_000 <= cfg.sqlite_busy_timeout_ms:
        raise RuntimeError("invalid Frequency V4 config field: critical command timeout")
    if cfg.max_book_pair_skew_ms > cfg.book_max_age_ms:
        raise RuntimeError("invalid Frequency V4 config field: max_book_pair_skew_ms")
    if cfg.reconnect_cap_ms < cfg.reconnect_base_ms:
        raise RuntimeError("invalid Frequency V4 config field: reconnect_cap_ms")
    if not (cfg.maker_observation_min_ms <= cfg.maker_observation_default_ms
            <= cfg.maker_observation_max_ms):
        raise RuntimeError("invalid Frequency V4 config field: maker observation")
    if cfg.rest_recovery_min_ms > cfg.rest_recovery_max_ms:
        raise RuntimeError("invalid Frequency V4 config field: REST recovery")

    numbers = {
        "discovery_interval_s": (0.25, 3_600.0, True),
        "minimum_remaining_s": (0.0, 299.0, False),
        "max_spread": (0.0, 1.0, False),
        "strong_cross_edge": (0.0, 0.5, True),
        "medium_maker_edge": (0.0, 0.5, True),
        "weak_observe_edge": (0.0, 0.5, True),
        "max_chase_worsening": (0.0, 0.5, False),
        "execution_buffer_base": (0.0, 0.25, False),
        "latency_buffer_base": (0.0, 0.25, False),
        "uncertainty_buffer_base": (0.0, 0.25, False),
        "fair_probability_floor": (0.0, 0.49, True),
        "fair_probability_ceiling": (0.51, 1.0, False),
        "max_model_adjustment": (0.0, 0.49, True),
        "rest_timeout_s": (0.0, 60.0, True),
        "fee_buffer_usd": (0.0, 100.0, False),
        "shutdown_drain_timeout_s": (0.0, 60.0, True),
        "critical_command_timeout_s": (0.0, 120.0, True),
        "reporting_worker_timeout_s": (0.0, 600.0, True),
        "maintenance_worker_timeout_s": (0.0, 600.0, True),
        "maintenance_max_seconds_per_pass": (0.0, 30.0, True),
    }
    for name, (minimum, maximum, strict_minimum) in numbers.items():
        _strict_number(getattr(cfg, name), name, minimum=minimum,
                       maximum=maximum, strict_minimum=strict_minimum)
    if not (0.0 < cfg.weak_observe_edge <= cfg.medium_maker_edge
            <= cfg.strong_cross_edge):
        raise RuntimeError("invalid Frequency V4 config field: execution tiers")
    if (cfg.fair_probability_floor >= cfg.fair_probability_ceiling
            or cfg.fair_probability_ceiling >= 1.0):
        raise RuntimeError("invalid Frequency V4 config field: probability bounds")


def _reassert_safety(cfg: FrequencyV4Config) -> None:
    cfg.strategy_id = STRATEGY_ID
    cfg.mode = MODE
    cfg.enabled = True
    cfg.dry_run = True
    cfg.live_enabled = False
    cfg.real_orders_possible = False
    cfg.live_adapter_present = False
    cfg.kill_switch_engaged = True
    cfg.fixed_shares = FIXED_SHARES
    cfg.db_path = FREQUENCY_V4_DB_PATH
    cfg.runtime_dir = FREQUENCY_V4_RUNTIME_DIR
    cfg.export_dir = FREQUENCY_V4_EXPORT_DIR
    cfg.gamma_base_url = GAMMA_BASE_URL
    cfg.clob_base_url = CLOB_BASE_URL
    cfg.clob_ws_url = CLOB_WS_URL
    cfg.okx_ws_url = OKX_WS_URL
    cfg.primary_cex_provider = "okx"
    cfg.required_assets = ["BTC", "ETH", "SOL"]
    cfg.discover_additional_assets = True
    cfg.exact_window_seconds = 300
    cfg.crypto_taker_fee_rate = 0.07
    cfg.research_equity_usd = 13.0
    cfg.exposure_cap_pct = 1.0
    cfg.max_open_per_asset = 1


def load_frequency_v4_config(path: str | None = None) -> FrequencyV4Config:
    """Load tunable research settings while reasserting every safety lock."""
    cfg = FrequencyV4Config()
    yaml_path = (Path(path) if path is not None
                 else FREQUENCY_V4_ROOT / "config_frequency_v4.yaml")
    if yaml_path.exists():
        try:
            import yaml

            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
            if not isinstance(raw, dict):
                raise TypeError("root must be a mapping")
            block = raw.get(MODE, {})
            if not isinstance(block, dict):
                raise TypeError(f"{MODE} must be a mapping")
            unknown = set(block) - set(FrequencyV4Config.__dataclass_fields__)
            if unknown:
                raise KeyError("unknown Frequency V4 fields")
            for name, value in block.items():
                if name not in _HARD_LOCKED_FIELDS:
                    setattr(cfg, name, value)
        except Exception as exc:
            raise RuntimeError(
                f"invalid Frequency V4 config: {type(exc).__name__}") from exc
    _reassert_safety(cfg)
    validate_frequency_v4_config(cfg)
    return cfg


load_v4_config = load_frequency_v4_config
