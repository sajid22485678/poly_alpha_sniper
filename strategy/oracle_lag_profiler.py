"""WS5A: oracle lag profiler (research/read-only).

Honest data-availability note: this bot has no continuous Chainlink price
feed -- only ONE point-in-time price_to_beat snapshot per market (captured
via Polymarket's event metadata, see strategy/oracle_anchor.py). A true
cross-correlation lag measurement needs two continuous time series (CEX
ticks and oracle ticks); we only ever have one oracle point per market. So:

- basis_stability and oracle_print_risk ARE measurable (real, from
  oracle_anchor_log, which gets one row per market evaluation -- i.e. a real
  time series of CEX-vs-anchor basis within each market's window).
- cex_lead_seconds and oracle_lag_seconds_estimate are NOT measurable from
  available data and are always returned as None with an explicit reason --
  never fabricated, never a guessed number.
- Per-exchange lead score is NOT available: oracle_anchor_log's cex_price is
  the already-selected primary CEX source (see core.app.App's
  cex_selected_source diagnostic), not broken out per exchange.

Reads the DB read-only via the caller-supplied `store` (e.g. SqliteStore in
read-only mode via DashboardData, or a direct read-only connection). Places
no orders, touches no secrets.
"""
from __future__ import annotations

import statistics
from typing import Optional

MIN_SAMPLES = 10
ASSETS = ("BTC", "ETH", "SOL")


def _rows_for_asset(rows: list[dict], asset: str) -> list[dict]:
    return [r for r in rows if r.get("asset") == asset and r.get("basis_pct") is not None]


def profile_asset_lag(rows: list[dict], asset: str, min_samples: int = MIN_SAMPLES) -> dict:
    """Pure. `rows` = oracle_anchor_log rows (as dicts) for ANY asset; this
    filters to `asset` itself. Returns the profile dict for one asset."""
    asset_rows = _rows_for_asset(rows, asset)
    n = len(asset_rows)
    if n < min_samples:
        return {
            "asset": asset, "insufficient_data": True, "n_samples": n,
            "min_samples_required": min_samples,
            "cex_lead_seconds": None, "oracle_lag_seconds_estimate": None,
            "cex_to_oracle_correlation": None, "basis_stability": None,
            "oracle_print_risk": None,
        }

    basis_values = [float(r["basis_pct"]) for r in asset_rows]
    basis_stability = round(statistics.pstdev(basis_values), 6)  # lower = more stable

    quality_values = [r.get("anchor_quality") for r in asset_rows]
    not_good = sum(1 for q in quality_values if q != "good")
    oracle_print_risk = round(not_good / n, 4)

    return {
        "asset": asset,
        "insufficient_data": False,
        "n_samples": n,
        "estimated": True,
        # Not measurable from available data -- see module docstring. Never
        # fabricated: explicitly None with a reason, not a guessed number.
        "cex_lead_seconds": None,
        "oracle_lag_seconds_estimate": None,
        "cex_to_oracle_correlation": None,
        "not_measurable_reason": (
            "no continuous oracle price feed available -- only one price_to_beat "
            "snapshot exists per market, so lead/lag/correlation against a second "
            "continuous series cannot be computed"),
        # Real, computed from oracle_anchor_log's basis_pct time series.
        "basis_stability": basis_stability,
        "basis_mean_pct": round(statistics.fmean(basis_values), 6),
        "oracle_print_risk": oracle_print_risk,
    }


def profile_lag(rows: list[dict], min_samples: int = MIN_SAMPLES) -> dict:
    """Profile all three tracked assets from one oracle_anchor_log read.
    exchange_lead_score is always None -- see module docstring."""
    return {
        "min_samples_required": min_samples,
        "exchange_lead_score": None,
        "exchange_lead_score_reason": (
            "oracle_anchor_log stores the already-selected primary CEX price, "
            "not a per-exchange breakdown"),
        "by_asset": {asset: profile_asset_lag(rows, asset, min_samples) for asset in ASSETS},
    }
