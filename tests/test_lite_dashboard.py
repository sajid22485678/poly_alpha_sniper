import json
from pathlib import Path

from poly_alpha_sniper.lite.lite_config import LiteConfig, load_lite_config
from poly_alpha_sniper.lite.lite_export import LITE_WARNING, build_lite_dashboard, write_lite_dashboard


class _Store:
    def dashboard_metrics(self, now_ms):
        assert now_ms == 123_000
        return {
            "open_positions": 1,
            "exit_pending": 1,
            "completed_trades": 2,
            "verified_completed_trades": 1,
            "pending_resolution": 1,
            "unresolved_retrying": 1,
            "unresolved_final": 1,
            "total_lite_pnl": 1.25,
            "verified_realized_pnl": 0.75,
            "today_lite_pnl": 0.5,
            "winrate": 0.5,
            "profit_factor": 1.4,
            "expectancy": 0.625,
            "max_drawdown": 0.5,
            "average_win": 1.5,
            "average_loss": -0.25,
            "entries_by_asset": {"BTC": 2},
            "entries_by_side": {"BUY_YES": 1, "BUY_NO": 1},
            "anchor_breakdown": {"anchor": 1, "no_anchor": 1},
            "resolution_source_breakdown": {
                "book_exit": 1, "official_outcome": 1, "unresolved": 1,
            },
            "top_reject_reasons": {"no_momentum": 7},
            "historical_both_side_conflicts": 147,
            "asset_window_locks": {"active": 1, "by_status": {"ENTERED": 1}},
            "anti_dead_bot_last_hour": {"candidate": 10, "ENTER_NOW": 2},
            "committed_exposure_usd": 2.5,
            "last_error": None,
            "last_20_trades": [
                {"id": 3, "status": "UNRESOLVED_FINAL", "pnl": None},
                {"id": 2, "status": "CLOSED_WIN", "pnl": 1.25},
            ],
            "db_diagnostics": {"size_bytes": 4096, "writes_per_min": 3},
        }

    def risk_snapshot(self, now_ms):
        return {"today_realized_pnl": -0.5, "consecutive_losses": 1,
                "committed_exposure_usd": 2.5}


def test_lite_dashboard_contains_required_safety_and_metrics():
    payload = build_lite_dashboard(
        _Store(), load_lite_config(path="Z:/missing/lite.yaml"), 123_000,
        runtime_state={"heartbeat_ts_ms": 122_000, "current_commit": "a"*40},
        cex_feed_state={"BTC": {"status": "ok"}},
        current_market_by_asset={"BTC": {"slug": "btc-updown-5m-0"}},
    )
    assert payload["mode"] == "lite_shadow"
    assert payload["dry_run"] is True
    assert payload["live_enabled"] is False
    assert payload["warning"] == LITE_WARNING
    assert payload["total_lite_pnl"] == 1.25
    assert payload["verified_realized_pnl"] == 0.75
    assert payload["live_small_preview"]["exposure_cap_usd"] == 9.75
    assert payload["live_readiness_verdict"] == "READY_FOR_MORE_SHADOW"
    assert payload["real_orders_possible"] is False
    assert payload["last_20_trades"][0]["pnl"] is None
    assert payload["cex_feed_state"]["BTC"]["status"] == "ok"


def test_export_writes_only_the_separate_lite_file(tmp_path):
    cfg = load_lite_config(path="Z:/missing/lite.yaml")
    result = write_lite_dashboard(_Store(), cfg, 123_000, output_dir=str(tmp_path))
    path = Path(result["path"])
    assert path.name == "lite_dashboard.json"
    assert json.loads(path.read_text(encoding="utf-8"))["mode"] == "lite_shadow"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["lite_dashboard.json"]


def test_lite_dashboard_is_not_wired_into_baseline_pnl_or_readiness():
    root = Path(__file__).resolve().parent.parent
    client = (root / "dashboard_v3/src/components/DashboardClient.tsx").read_text(encoding="utf-8")
    route = (root / "dashboard_v3/src/app/api/snapshot/route.ts").read_text(encoding="utf-8")
    assert "lite_dashboard" in route
    assert "lite_dashboard_missing" in route
    assert "<LiteShadowPanel" in client
    # Lite is a separate panel prop, never fed to advanced metric components.
    assert "<PnlBanner summary={summary}" in client
    assert "<LiveReadinessPanel" in client
    assert "PnlBanner summary={data?.lite_dashboard" not in client
    assert "LiveReadinessPanel readiness={data?.lite_dashboard" not in client


def test_lite_panel_renders_safety_operational_fields_and_null_pnl_honestly():
    root = Path(__file__).resolve().parent.parent
    panel = (root / "dashboard_v3/src/components/LiteShadowPanel.tsx").read_text(encoding="utf-8")
    assert "LITE SHADOW ONLY" in panel
    assert "Real Orders Possible" in panel
    assert "Verified PnL" in panel
    assert "Window Locks" in panel
    assert "Entry State" in panel
    assert "pnl" in panel.lower()
    assert "—" in panel or "\\u2014" in panel


def test_export_live_readiness_remains_lite_only_and_never_claims_live():
    payload = build_lite_dashboard(
        _Store(), load_lite_config(path="Z:/missing/lite.yaml"), 123_000)
    assert "trade_summary" not in payload
    assert "baseline" not in payload
    assert payload["live_readiness_verdict"] == "READY_FOR_MORE_SHADOW"
    assert payload["live_enabled"] is False
