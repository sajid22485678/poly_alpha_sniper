"""max_scope_shadow_research_optimization: feature store, anchor
missing-reason taxonomy, and current-blocker honesty. All research surfaces
are shadow-only and never affect the trading decision."""
from __future__ import annotations

import json

import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets, TradingMode, load_config
from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.discovery.market_mapper import extract_oracle_anchor_metadata
from poly_alpha_sniper.reporting.agent_export import build_latest_status
from poly_alpha_sniper.research.feature_store import (
    THROTTLE_MS, build_scan_feature_row, should_record)
from poly_alpha_sniper.strategy.shock_near_miss import SCORING_VERSION
from poly_alpha_sniper.tests.helpers import NOW_MS, book, market, view


# ---------------------------------------------------------------------------
# Anchor missing-reason taxonomy (honest, never guesses)
# ---------------------------------------------------------------------------

def _reason(raw: dict) -> str:
    _p, _s, _u, diag = extract_oracle_anchor_metadata(raw)
    return diag["missing_reason"]


def test_missing_reason_upstream_not_published_when_slot_null():
    raw = {"id": "m1", "events": [{"id": "e1", "eventMetadata": None}]}
    assert _reason(raw) == "UPSTREAM_NOT_PUBLISHED"


def test_missing_reason_schema_unknown_when_metadata_has_no_price_key():
    raw = {"id": "m1", "events": [{"id": "e1", "eventMetadata": {"somethingElse": 1.0}}]}
    assert _reason(raw) == "SCHEMA_UNKNOWN"


def test_missing_reason_hydration_failed():
    raw = {"id": "m1", "events": [{"id": "e1", "eventMetadata": None}],
           "_anchor_hydration_attempted": True, "_anchor_hydration_success": False}
    assert _reason(raw) == "HYDRATION_FAILED"


def test_missing_reason_event_not_found_for_shallow_row():
    raw = {"id": "m1"}  # no events, no metadata slot anywhere
    assert _reason(raw) == "EVENT_NOT_FOUND"


def test_missing_reason_empty_when_anchor_available():
    raw = {"id": "m1", "events": [{"id": "e1", "eventMetadata": {"priceToBeat": 100.5}}]}
    assert _reason(raw) == ""


# ---------------------------------------------------------------------------
# Feature store: throttled, append-only, point-in-time honest
# ---------------------------------------------------------------------------

def test_should_record_throttles_always_except_fired_shock():
    """Flood post-mortem (2026-07-10): block_reason=None ("passed early
    gates") is the NORMAL state of a healthy scan, not a rare event -- the old
    exemption bypassed the throttle every iteration and wrote ~110k rows/lane/
    hour (2.17 GB DB). Only a genuinely rare FIRED shock skips the throttle."""
    assert should_record(None, NOW_MS, "rejected_by_no_shock") is True
    assert should_record(NOW_MS - 1000, NOW_MS, "rejected_by_no_shock") is False
    assert should_record(NOW_MS - THROTTLE_MS, NOW_MS, "rejected_by_no_shock") is True
    assert should_record(NOW_MS - 1, NOW_MS, None) is False          # flood fix
    assert should_record(NOW_MS - THROTTLE_MS, NOW_MS, None) is True
    assert should_record(NOW_MS - 1, NOW_MS, None, near_miss_tier="FIRED") is True


def test_build_scan_feature_row_captures_pipeline_view():
    m = market()
    m.price_to_beat = 100_000.0
    b = book(token_id=m.yes_token_id, ts_ms=NOW_MS - 200)
    from poly_alpha_sniper.strategy.shock_near_miss import compute_shock_near_miss
    from poly_alpha_sniper.tests.helpers import cfg
    nm = compute_shock_near_miss(view(asset="BTC"), cfg())
    row = build_scan_feature_row(
        asset="BTC", view=view(asset="BTC"), market=m, anchor=None,
        freshness="fresh", block_reason="rejected_by_no_shock", near_miss=nm,
        book=b, scoring_version=SCORING_VERSION, now_ms=NOW_MS)
    assert row["asset"] == "BTC"
    assert row["lane"] == "baseline"
    assert row["price_to_beat"] == 100_000.0
    assert row["scoring_version"] == "shock_and_gate_v2"
    assert row["blocker"] == "rejected_by_no_shock"
    assert row["book_bid"] == b.best_bid
    assert row["book_age_ms"] == 200
    assert json.loads(row["extra"])  # valid JSON bag


def test_build_scan_feature_row_all_optional_none_is_safe():
    row = build_scan_feature_row(
        asset="SOL", view=None, market=None, anchor=None, freshness="no_data",
        block_reason="rejected_by_no_fresh_cex_price", near_miss=None, book=None,
        scoring_version=SCORING_VERSION, now_ms=NOW_MS)
    assert row["cex_price"] is None
    assert row["price_to_beat"] is None
    assert row["near_miss_tier"] == ""


@pytest.fixture()
def app(tmp_path):
    from poly_alpha_sniper.core.app import App
    c = load_config()
    c.telegram.enabled = False
    c.dashboard.auth_enabled = False
    db = (tmp_path / "feature_store.db").as_posix()
    a = App(c, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                            "DATABASE_URL": f"sqlite:///{db}"}))
    a.clock = SimClock(NOW_MS)
    a.build()
    yield a
    a.store.close()


async def test_scan_writes_feature_store_rows(app):
    ts = app.clock.now_ms() - 100
    app.cex_state.update(CexTick(asset="SOL", exchange="bybit", price=150.0,
                                 ts_ms=ts, recv_ts_ms=ts))
    await app._scan_entries()
    rows = app.store.query("SELECT * FROM feature_store WHERE asset='SOL'")
    assert len(rows) >= 1
    assert rows[0]["lane"] == "baseline"
    assert rows[0]["scoring_version"] == "shock_and_gate_v2"


# ---------------------------------------------------------------------------
# Current blocker: from the live scan, never the historical signal row
# ---------------------------------------------------------------------------

def test_current_blocker_comes_from_last_scan_snapshot(app):
    from poly_alpha_sniper.dashboard.db_reader import DashboardData
    app.diag["last_scan_snapshot"] = {"block_reason": "rejected_by_no_shock"}
    app.diag["last_block_reason"] = "BTC:rejected_by_stale_book"  # older, must lose
    status = build_latest_status(DashboardData(app.store),
                                 {"diagnostics": app.diag, "mode": "shadow_live"},
                                 app.cfg, NOW_MS)
    assert status["current_blocker"] == "rejected_by_no_shock"


def test_current_blocker_none_when_latest_scan_unblocked(app):
    from poly_alpha_sniper.dashboard.db_reader import DashboardData
    app.diag["last_scan_snapshot"] = {"block_reason": None}
    app.diag["last_block_reason"] = ""
    status = build_latest_status(DashboardData(app.store),
                                 {"diagnostics": app.diag, "mode": "shadow_live"},
                                 app.cfg, NOW_MS)
    assert status["current_blocker"] is None


# ---------------------------------------------------------------------------
# Research challenger report: lane separation + never-automatic promotion
# ---------------------------------------------------------------------------

def test_research_challenger_empty_rows_is_safe():
    from poly_alpha_sniper.research.challenger_report import build_research_challenger
    out = build_research_challenger([], baseline_trades=8,
                                    assets=["BTC", "ETH", "SOL"], now_ms=NOW_MS)
    assert out["research_only"] is True
    assert out["lane_separation"] == {"baseline_rows": 0, "experimental_rows": 0,
                                      "mixed": False}
    assert "INSUFFICIENT_SAMPLE" in out["promotion_status"]
    assert out["per_asset"]["BTC"]["markov"] is None


def test_research_challenger_separates_lanes_and_never_promotes_thin_samples():
    from poly_alpha_sniper.research.challenger_report import build_research_challenger
    rows = []
    for i in range(10):
        rows.append({"ts_ms": NOW_MS + i * 10_000, "asset": "BTC", "lane": "baseline",
                     "ret_2s": 0.0002, "zscore": 0.5, "shock_score": 0.2,
                     "volatility": 0.0001, "time_to_close_s": 200.0,
                     "blocker": "rejected_by_no_shock", "anchor_status": "missing",
                     "cex_freshness": "fresh"})
    rows.append({"ts_ms": NOW_MS, "asset": "BTC", "lane": "experimental",
                 "ret_2s": 0.0, "zscore": 0.0, "shock_score": 0.0})
    out = build_research_challenger(rows, baseline_trades=8,
                                    assets=["BTC"], now_ms=NOW_MS)
    assert out["lane_separation"]["baseline_rows"] == 10
    assert out["lane_separation"]["experimental_rows"] == 1  # never mixed in
    btc = out["per_asset"]["BTC"]
    assert btc["markov"]["state_now"] in ("QUIET", "CHOPPY", "BUILDUP_UP", "BUILDUP_DOWN")
    assert btc["regime"]["regime"]
    assert out["drift"]["available"] is True
    assert "NOT_PROMOTED" in out["promotion_status"]


# ---------------------------------------------------------------------------
# Standing safety invariants
# ---------------------------------------------------------------------------

def test_live_remains_disabled_and_research_lane_is_shadow_only():
    c = load_config()
    assert c.mode.trading_mode == "shadow_live"
    assert c.mode.dry_run is True
    assert TradingMode(c.mode.trading_mode).is_live is False


def test_feature_store_module_places_no_orders_and_reads_no_secrets():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "research" / "feature_store.py").read_text(
        encoding="utf-8")
    for forbidden in ("place_order", "cancel_order", "load_secrets", "dotenv"):
        assert forbidden not in src
