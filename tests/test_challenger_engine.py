"""EXPERIMENTAL_SHADOW challenger lane: research-only would-enter decisions,
hard gates that no challenger can loosen, lane separation, and the app-level
writer. Never places orders, never touches baseline stats or live readiness."""
from __future__ import annotations

import json

import pytest

from poly_alpha_sniper.core.clock import SimClock
from poly_alpha_sniper.core.config_loader import Secrets, TradingMode, load_config
from poly_alpha_sniper.core.contracts import CexTick
from poly_alpha_sniper.research.challenger_engine import (
    CHALLENGER_NAMES, ChallengerEngine, build_experimental_row, hard_safety_gates)
from poly_alpha_sniper.tests.helpers import NOW_MS


def _good_row(**overrides) -> dict:
    """A feature row where EVERY hard gate passes and the shock legs are hot
    (both legs at 95% of threshold, HOT_NEAR_MISS)."""
    row = {
        "ts_ms": NOW_MS, "asset": "BTC", "lane": "baseline",
        "market_slug": "btc-updown-5m-1", "market_id": "m1",
        "yes_token_id": "tok_yes", "no_token_id": "tok_no",
        "time_to_close_s": 200.0, "price_to_beat": 99_990.0,
        "anchor_status": "available",
        "cex_source": "bybit", "cex_age_ms": 500, "cex_freshness": "fresh",
        "cex_price": 100_000.0,
        "ret_1s": 0.0010, "ret_2s": 0.0012, "ret_5s": 0.0011,
        "volatility": 0.0002, "zscore": 1.9,
        "shock_score": 0.95, "ret_score": 0.95, "zscore_score": 0.95,
        "near_miss_tier": "HOT_NEAR_MISS",
        "book_bid": 0.38, "book_ask": 0.40, "spread": 0.02,
        "book_age_ms": 100, "depth_usd": 500.0,
        "blocker": "rejected_by_no_shock",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Hard gates: no challenger can loosen these
# ---------------------------------------------------------------------------

def test_hard_gates_pass_on_good_row():
    ok, failure, gates = hard_safety_gates(_good_row(), available_cash_usd=100.0)
    assert ok and failure == ""
    assert all(gates[g] for g in ("anchor_available", "cex_not_fail_closed",
                                  "market_valid", "token_mapping",
                                  "executable_book", "spread_ok", "depth_ok", "cash_ok"))


@pytest.mark.parametrize("override,expected_gate", [
    ({"price_to_beat": None}, "anchor_available"),
    ({"cex_age_ms": 9000}, "cex_not_fail_closed"),
    ({"cex_freshness": "fail_closed"}, "cex_not_fail_closed"),
    ({"time_to_close_s": 10.0}, "market_valid"),          # expired/too-late market
    ({"market_id": ""}, "market_valid"),
    ({"yes_token_id": ""}, "token_mapping"),               # ambiguous token mapping
    ({"book_bid": None, "book_ask": None}, "executable_book"),
    ({"spread": 0.5}, "spread_ok"),                        # catastrophic spread
    ({"depth_usd": 1.0}, "depth_ok"),                      # catastrophic depth
])
def test_each_hard_gate_blocks(override, expected_gate):
    ok, failure, _g = hard_safety_gates(_good_row(**override), available_cash_usd=100.0)
    assert not ok
    assert failure == expected_gate


def test_insufficient_cash_blocks():
    ok, failure, _g = hard_safety_gates(_good_row(), available_cash_usd=0.5)
    assert not ok and failure == "cash_ok"  # 5 shares * 0.40 = $2 > $0.50


# ---------------------------------------------------------------------------
# Challenger decisions
# ---------------------------------------------------------------------------

def test_hot_candidate_with_all_gates_can_would_enter():
    engine = ChallengerEngine()
    decisions = engine.evaluate(_good_row(), NOW_MS, available_cash_usd=100.0)
    assert set(decisions) == set(CHALLENGER_NAMES)
    for name in ("loose_shock_10", "loose_shock_15", "hot_near_miss_entry"):
        assert decisions[name].would_enter is True, (name, decisions[name].reason)
        assert decisions[name].blocker == ""
        assert decisions[name].ev is not None and decisions[name].ev > 0
        assert decisions[name].promotion_status == "NOT_PROMOTED"


def test_missing_anchor_blocks_every_challenger_with_precise_reason():
    """A missing anchor must NEVER read as merely no_challenger_trigger --
    the data problem outranks the trigger question."""
    engine = ChallengerEngine()
    decisions = engine.evaluate(_good_row(price_to_beat=None), NOW_MS, 100.0)
    for d in decisions.values():
        assert d.would_enter is False
        assert d.blocker == "missing_anchor"
    # and when the upstream reason is known, it is named exactly
    engine2 = ChallengerEngine()
    decisions2 = engine2.evaluate(
        _good_row(price_to_beat=None, anchor_missing_reason="UPSTREAM_NOT_PUBLISHED"),
        NOW_MS, 100.0)
    assert all(d.blocker == "anchor_upstream_not_published" for d in decisions2.values())


def test_stale_cex_blocks_every_challenger():
    engine = ChallengerEngine()
    decisions = engine.evaluate(_good_row(cex_age_ms=9000, cex_freshness="fail_closed"),
                                NOW_MS, 100.0)
    assert not any(d.would_enter for d in decisions.values())


def test_no_executable_book_blocks():
    engine = ChallengerEngine()
    decisions = engine.evaluate(_good_row(book_bid=None, book_ask=None, spread=None),
                                NOW_MS, 100.0)
    assert not any(d.would_enter for d in decisions.values())


def test_negative_ev_blocks_entry():
    """ask=0.99 makes any bounded posterior EV negative -- must never enter."""
    engine = ChallengerEngine()
    decisions = engine.evaluate(_good_row(book_ask=0.99, book_bid=0.97, spread=0.02),
                                NOW_MS, 100.0)
    for d in decisions.values():
        assert d.would_enter is False


def test_routine_no_shock_does_not_enter_by_default():
    """Cold row (legs at 20% of threshold, ROUTINE tier): every challenger's
    trigger must refuse -- ROUTINE_NO_SHOCK is not an entry."""
    engine = ChallengerEngine()
    cold = _good_row(ret_score=0.2, zscore_score=0.2, shock_score=0.2,
                     near_miss_tier="ROUTINE_NO_SHOCK", ret_2s=0.0002, zscore=0.4)
    decisions = engine.evaluate(cold, NOW_MS, 100.0)
    assert not any(d.would_enter for d in decisions.values())
    # precise trigger rejects: shock proximity for the shock-family
    # challengers, tier for the hot-near-miss challenger
    assert decisions["hot_near_miss_entry"].blocker == "no_hot_near_miss"
    for name, d in decisions.items():
        if name != "hot_near_miss_entry":
            assert d.blocker == "shock_too_low", (name, d.blocker)


def test_duplicate_experimental_position_guard():
    engine = ChallengerEngine()
    first = engine.evaluate(_good_row(), NOW_MS, 100.0)
    assert first["loose_shock_10"].would_enter is True
    second = engine.evaluate(_good_row(), NOW_MS + 1000, 100.0)
    assert second["loose_shock_10"].would_enter is False
    assert second["loose_shock_10"].blocker == "duplicate_experimental_position"
    # a different market is a fresh opportunity
    third = engine.evaluate(_good_row(market_id="m2"), NOW_MS + 2000, 100.0)
    assert third["loose_shock_10"].would_enter is True


def test_loose_shock_15_wider_than_10():
    engine = ChallengerEngine()
    row = _good_row(ret_score=0.87, zscore_score=0.87, near_miss_tier="NEAR_MISS")
    decisions = engine.evaluate(row, NOW_MS, 100.0)
    assert decisions["loose_shock_10"].would_enter is False   # needs >= 0.90
    assert decisions["loose_shock_15"].would_enter is True    # needs >= 0.85


def test_engine_is_deterministic():
    a = ChallengerEngine().evaluate(_good_row(), NOW_MS, 100.0)
    b = ChallengerEngine().evaluate(_good_row(), NOW_MS, 100.0)
    for name in CHALLENGER_NAMES:
        assert a[name].as_dict() == b[name].as_dict()


# ---------------------------------------------------------------------------
# Experimental row payload
# ---------------------------------------------------------------------------

def test_build_experimental_row_carries_all_decisions_and_separate_lane():
    engine = ChallengerEngine()
    baseline = _good_row()
    decisions = engine.evaluate(baseline, NOW_MS, 100.0)
    row = build_experimental_row(baseline, decisions)
    assert row["lane"] == "experimental"
    assert baseline["lane"] == "baseline"      # original not mutated
    extra = json.loads(row["extra"])
    assert extra["challenger_version"] == "challengers_v1"
    assert set(extra["challengers"]) == set(CHALLENGER_NAMES)
    assert extra["simulated_entry_price"] == baseline["book_ask"]


# ---------------------------------------------------------------------------
# App-level: experimental rows written next to baseline, baseline unchanged
# ---------------------------------------------------------------------------

@pytest.fixture()
def app(tmp_path):
    from poly_alpha_sniper.core.app import App
    c = load_config()
    c.telegram.enabled = False
    c.dashboard.auth_enabled = False
    assert c.research_challengers.enabled is True  # active by default
    db = (tmp_path / "challenger.db").as_posix()
    a = App(c, Secrets(env={"DASHBOARD_AUTH_ENABLED": "false",
                            "DATABASE_URL": f"sqlite:///{db}"}))
    a.clock = SimClock(NOW_MS)
    a.build()
    a.test_db_path = db  # DashboardData takes a PATH, not the store object
    yield a
    a.store.close()


async def test_scan_writes_baseline_and_experimental_rows(app):
    ts = app.clock.now_ms() - 100
    app.cex_state.update(CexTick(asset="SOL", exchange="bybit", price=150.0,
                                 ts_ms=ts, recv_ts_ms=ts))
    await app._scan_entries()
    base = app.store.query("SELECT * FROM feature_store WHERE lane='baseline' AND asset='SOL'")
    exp = app.store.query("SELECT * FROM feature_store WHERE lane='experimental' AND asset='SOL'")
    assert len(base) >= 1                      # baseline unchanged, still written
    assert len(exp) >= 1                       # the activation this ticket is about
    extra = json.loads(exp[0]["extra"])
    assert set(extra["challengers"]) == set(CHALLENGER_NAMES)
    # fixture has no candidate market -> hard gates reject; still recorded honestly
    assert exp[0]["decision"] == "REJECT"


async def test_disabled_flag_stops_experimental_but_not_baseline(app):
    app.cfg.research_challengers.enabled = False
    ts = app.clock.now_ms() - 100
    app.cex_state.update(CexTick(asset="SOL", exchange="bybit", price=150.0,
                                 ts_ms=ts, recv_ts_ms=ts))
    await app._scan_entries()
    assert app.store.query("SELECT * FROM feature_store WHERE lane='baseline'")
    assert app.store.query("SELECT * FROM feature_store WHERE lane='experimental'") == []


async def test_live_readiness_ignores_experimental_rows(app):
    """Live readiness reads baseline trades (exits), never feature rows --
    inject experimental rows and confirm the scorecard is identical."""
    from poly_alpha_sniper.dashboard.db_reader import DashboardData
    from poly_alpha_sniper.reporting.agent_export import build_live_readiness
    state = {"diagnostics": app.diag, "mode": "shadow_live"}
    before = build_live_readiness(DashboardData(app.test_db_path), state, app.cfg, NOW_MS)
    engine = ChallengerEngine()
    row = build_experimental_row(_good_row(), engine.evaluate(_good_row(), NOW_MS, 100.0))
    app._insert("feature_store", row)
    after = build_live_readiness(DashboardData(app.test_db_path), state, app.cfg, NOW_MS)
    assert before == after


# ---------------------------------------------------------------------------
# Report: per-challenger stats, mixed=false
# ---------------------------------------------------------------------------

def test_report_counts_per_challenger_and_stays_unmixed():
    from poly_alpha_sniper.research.challenger_report import build_research_challenger
    engine = ChallengerEngine()
    rows = [_good_row(ts_ms=NOW_MS + i * 10_000) for i in range(3)]
    exp_rows = [build_experimental_row(r, engine.evaluate(r, r["ts_ms"], 100.0))
                for r in rows]
    out = build_research_challenger(rows + exp_rows, baseline_trades=8,
                                    assets=["BTC"], now_ms=NOW_MS,
                                    challengers_enabled=True)
    assert out["lane_separation"] == {"baseline_rows": 3, "experimental_rows": 3,
                                      "mixed": False}
    assert out["experimental_zero_reason"] is None
    ch = out["challengers"]
    assert set(ch) == set(CHALLENGER_NAMES)
    assert ch["loose_shock_10"]["rows"] == 3
    assert ch["loose_shock_10"]["would_enter"] == 1        # duplicate guard after 1st
    assert ch["loose_shock_10"]["status"] == "INSUFFICIENT_SAMPLE"  # 8 < 30 trades
    assert "duplicate_experimental_position" in ch["loose_shock_10"]["top_reject_reasons"]
    assert out["baseline_blocker_distribution"]
    assert out["experimental_blocker_distribution"]


def test_report_explains_zero_experimental_rows():
    from poly_alpha_sniper.research.challenger_report import build_research_challenger
    out = build_research_challenger([_good_row()], baseline_trades=8,
                                    assets=["BTC"], now_ms=NOW_MS,
                                    challengers_enabled=True)
    assert "restart" in out["experimental_zero_reason"]
    disabled = build_research_challenger([_good_row()], baseline_trades=8,
                                         assets=["BTC"], now_ms=NOW_MS,
                                         challengers_enabled=False)
    assert disabled["experimental_zero_reason"] == "research_challengers.enabled=false"


# ---------------------------------------------------------------------------
# Standing safety invariants
# ---------------------------------------------------------------------------

def test_live_remains_disabled():
    c = load_config()
    assert c.mode.trading_mode == "shadow_live"
    assert c.mode.dry_run is True
    assert TradingMode(c.mode.trading_mode).is_live is False


def test_challenger_engine_touches_no_orders_and_no_secrets():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "research" / "challenger_engine.py"
           ).read_text(encoding="utf-8")
    for forbidden in ("place_order", "cancel_order", "submit", "load_secrets", "dotenv"):
        assert forbidden not in src
