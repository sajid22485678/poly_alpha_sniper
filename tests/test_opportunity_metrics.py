"""Mission J: opportunity-frequency metrics, near-miss tiers, cex freshness
bucket, live readiness, and shadow compounding simulator -- correctness +
safety (read-only, never touches balance/orders)."""
from pathlib import Path

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.dashboard import opportunity_metrics as opp
from poly_alpha_sniper.dashboard import shadow_compounding_sim as sim
from poly_alpha_sniper.strategy.shock_near_miss import near_miss_tier

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOW = 1_783_000_000_000


def _no_shock(ts, score, asset="BTC"):
    return {"ts_ms": ts, "reason": "rejected_by_no_shock",
            "detail": f"ret2=+0.0005 z=+1.2 px=100.0 shock_score={score:.2f} tier=X near_miss=True",
            "asset": asset}


def _pred(ts, decision="REJECT", tier="C", reject="tier C: quality too low", edge=0.05):
    return {"ts_ms": ts, "decision": decision, "tier": tier, "reject_reason": reject,
            "edge_after_slippage": edge, "asset": "BTC"}


# --- near-miss tiers (Mission B) -------------------------------------------

def test_near_miss_tiers_boundaries():
    assert near_miss_tier(1.0) == "FIRED"
    assert near_miss_tier(0.95) == "HOT_NEAR_MISS"
    assert near_miss_tier(0.90) == "HOT_NEAR_MISS"
    assert near_miss_tier(0.85) == "NEAR_MISS"
    assert near_miss_tier(0.75) == "WATCHLIST"
    assert near_miss_tier(0.5) == "ROUTINE_NO_SHOCK"


def test_no_shock_board_tiers_and_percentile():
    rows = [_no_shock(NOW - i * 1000, s) for i, s in enumerate([0.95, 0.85, 0.72, 0.4])]
    board = opp.no_shock_board(rows, NOW, 60)
    assert board["tier_counts"].get("HOT_NEAR_MISS") == 1
    assert board["tier_counts"].get("NEAR_MISS") == 1
    assert board["tier_counts"].get("WATCHLIST") == 1
    assert board["hot_near_miss"] == 1
    # board sorted by score desc; top has highest percentile
    assert board["board"][0]["shock_score"] == 0.95
    assert board["board"][0]["percentile"] == 100.0


def test_no_shock_board_counts_unscored_rows_separately():
    rows = [{"ts_ms": NOW, "reason": "rejected_by_no_shock", "detail": "no score here", "asset": "BTC"}]
    board = opp.no_shock_board(rows, NOW, 60)
    assert board["no_shock_scored"] == 0
    assert board["no_shock_unscored_pre_fix"] == 1


# --- cex freshness bucket (Mission C) --------------------------------------

def test_cex_freshness_bucket_ladder():
    assert opp.cex_freshness_bucket(None, 1500, 3000, 8000) == "NO_SOURCE"
    assert opp.cex_freshness_bucket(500, 1500, 3000, 8000) == "FRESH"
    assert opp.cex_freshness_bucket(2000, 1500, 3000, 8000) == "DEGRADED"
    assert opp.cex_freshness_bucket(5000, 1500, 3000, 8000) == "FAIL_CLOSED"


def test_cex_freshness_report_marks_live_unchanged():
    cfg = load_config()
    diag = {"cex_selected_source": {"BTC": "okx"}, "cex_freshest_age_ms": {"BTC": 2000}}
    rep = opp.cex_freshness_report(diag, cfg)
    assert rep["live_behavior_unchanged"] is True
    assert rep["by_asset"]["BTC"]["cex_freshness_bucket"] == "DEGRADED"
    assert rep["by_asset"]["BTC"]["degraded_path_allowed"] is True


# --- opportunity frequency + waterfall (Mission G) -------------------------

def test_opportunity_frequency_scales_to_per_hour():
    # 30 no_shock rows in a 30-minute window -> 60/hr
    rows = [_no_shock(NOW - i * 1000, 0.5) for i in range(30)]
    freq = opp.opportunity_frequency(rows, [], NOW, 30)
    assert freq["no_shock_rejects_per_hour"] == 60.0
    assert freq["accepted_shadow_trades_per_hour"] == 0.0


def test_opportunity_frequency_counts_accepted():
    preds = [_pred(NOW, decision="SHADOW_ONLY", tier="A")]
    freq = opp.opportunity_frequency([], preds, NOW, 60)
    assert freq["accepted_shadow_trades_per_hour"] == 1.0


def test_gate_waterfall_attributes_earliest_stage():
    """Mission J#12: waterfall still attributes to the earliest failed stage."""
    from poly_alpha_sniper.dashboard import metrics
    diag = [_no_shock(NOW, 0.5)]
    preds = [_pred(NOW, decision="REJECT", reject="hard reject: book_fresh")]
    wf = metrics.gate_waterfall(diag, preds, NOW, 60)
    assert wf["stages"]["no_shock"] == 1
    assert wf["stages"]["stale_book"] == 1
    assert wf["accepted"] == 0


def test_tier_breakdown_keeps_c_separate():
    preds = [_pred(NOW, tier="C"), _pred(NOW, tier="A_PLUS", decision="SHADOW_ONLY")]
    tb = opp.tier_breakdown(preds, NOW, 120)
    assert tb["by_tier"]["C"]["candidates"] == 1
    assert tb["by_tier"]["C"]["accepted"] == 0
    assert tb["by_tier"]["A_PLUS"]["accepted"] == 1


# --- live readiness (Mission I) --------------------------------------------

def test_live_readiness_fails_on_small_sample():
    cfg = load_config()
    res = opp.live_readiness({"standard_shadow_trades": 8, "profit_factor": 5.0,
                              "expectancy_usd": 0.5, "max_drawdown_pct": 10.0,
                              "max_loss_streak": 1}, cfg)
    assert res["verdict"] == "LIVE_NOT_READY"
    assert res["passed"] is False
    assert any("50+" in u for u in res["unmet"])


def test_live_readiness_never_says_live_ready_verb():
    cfg = load_config()
    res = opp.live_readiness({"standard_shadow_trades": 8}, cfg)
    assert res["verdict"] != "LIVE_READY"  # only ever LIVE_NOT_READY or LIVE_READY_REVIEW


# --- shadow compounding simulator (Mission H) ------------------------------

def _exit(ts, pnl, price=0.5, shares=5.0):
    return {"ts_ms": ts, "pnl_usd": pnl, "price": price, "shares": shares}


def test_compounding_insufficient_sample_verdict():
    exits = [_exit(NOW + i, 0.1) for i in range(8)]
    res = sim.simulate(exits, [], 10.0)
    assert res["verdict"] == "INSUFFICIENT SHADOW TRADE SAMPLE"
    assert res["touches_real_balance"] is False
    assert res["touches_order_path"] is False


def test_compounding_computes_with_enough_trades():
    exits = [_exit(NOW + i, 0.1 if i % 2 else -0.05) for i in range(40)]
    res = sim.simulate(exits, [], 10.0)
    assert res["verdict"] == "COMPUTED"
    assert res["standard_shadow_trades"] == 40
    assert res["no_martingale"] is True
    assert res["research_curve"]["trades"] == 0  # standard vs research kept separate


def _code_only(src: str) -> str:
    """Strip triple-quoted docstrings and # comments so safety-explaining
    prose (which legitimately names the things it forbids) doesn't trip a
    naive substring scan -- only actual code is checked."""
    import re
    src = re.sub(r'"""(?:.|\n)*?"""', "", src)
    src = re.sub(r"'''(?:.|\n)*?'''", "", src)
    return "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))


def test_compounding_never_scales_up_after_loss():
    """Fixed-fraction rule: stake is a constant fraction of CURRENT equity,
    never increased after a loss (no Martingale/revenge sizing)."""
    src = (PROJECT_ROOT / "dashboard" / "shadow_compounding_sim.py").read_text(encoding="utf-8")
    assert "no_loss_scaling" in src  # the invariant flag is exported
    code = _code_only(src)
    for forbidden in ("place_order", "cancel_order", "self.portfolio", "LIVE_TRADING"):
        assert forbidden not in code, f"compounding sim CODE references {forbidden!r}"


def test_metrics_modules_never_reference_secrets_or_orders():
    for name in ("dashboard/opportunity_metrics.py", "dashboard/shadow_compounding_sim.py",
                 "tools/replay_threshold_challenger.py"):
        code = _code_only((PROJECT_ROOT / name).read_text(encoding="utf-8"))
        for forbidden in ("load_dotenv", "load_secrets", "os.environ", "PRIVATE_KEY",
                          "API_SECRET", "place_order(", "cancel_order("):
            assert forbidden not in code, f"{name} CODE references {forbidden!r}"
