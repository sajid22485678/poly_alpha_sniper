"""Safety + correctness tests for the read-only agent exporter
(reporting/agent_export.py). Covers: no .env access, no secrets in output,
sanitized JSON/Markdown, directory creation, and that nothing here can place
or cancel an order."""
import json
import re
from pathlib import Path

import pytest

from poly_alpha_sniper.core.config_loader import load_config
from poly_alpha_sniper.reporting.agent_export import (
    MIN_TRADES_FOR_LIVE_REVIEW, build_daily_report_md, build_dashboard_snapshot,
    build_hermes_brief, build_latest_status, build_obsidian_note_md,
    build_reject_breakdown_export, build_trade_summary, write_exports)
from poly_alpha_sniper.dashboard.db_reader import DashboardData
from poly_alpha_sniper.storage.migrations import run_migrations
from poly_alpha_sniper.storage.sqlite_store import SqliteStore

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOW_MS = 1_752_000_000_000
# matches core.logger._PATTERNS' api_key/api_secret key-value regex -- a
# realistic shape for what could leak through a free-text DB field, not an
# arbitrary string the redaction filter was never designed to catch.
FAKE_SECRET = "api_secret=SuperSecretValue123456"


def _live_db(tmp_path, with_secret_like_text=False):
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    store.insert("predictions", {
        "ts_ms": NOW_MS - 1000, "asset": "ETH",
        "market_title": ("compromised " + FAKE_SECRET) if with_secret_like_text else "ETH Up or Down",
        "direction": "UP", "polymarket_price": 0.38, "fair_probability": 0.55,
        "edge": 0.13, "edge_after_slippage": 0.12, "confidence": 90, "tier": "A",
        "decision": "REJECT", "reject_reason": "REJECTED_MIN_ORDER_SIZE_TOO_HIGH", "mode": "shadow_live"})
    store.insert("exits", {"ts_ms": NOW_MS - 500, "market_id": "m1", "reason": "TAKE_PROFIT",
                           "pnl_usd": 0.12, "hold_seconds": 3.0, "price": 0.6, "shares": 1.6})
    store.insert("pnl", {"ts_ms": NOW_MS - 500, "realized_pnl_usd": 0.12, "equity_usd": 10.12})
    store.close()
    return path


def _cfg(tmp_path):
    cfg = load_config()
    cfg.agent_export.output_dir = str(tmp_path / "agent_readonly")
    return cfg


# ---------------------------------------------------------------------------
# Static safety checks (source inspection -- same pattern as test_no_secrets.py)
# ---------------------------------------------------------------------------

EXPORT_SOURCE_FILES = [
    "reporting/agent_export.py", "reporting/obsidian_export.py",
    "tools/export_agent_readonly.py", "tools/export_to_obsidian.py",
]


def _code_lines(src: str) -> str:
    """Strip comments/docstring prose so safety-explaining comments (which
    legitimately name the very things they promise NOT to do, e.g. "never
    reads .env's LIVE_TRADING_ENABLED flag") don't trigger their own
    forbidden-pattern check. Only checks actual code-shaped lines. Handles
    both multi-line and single-line \"\"\"...\"\"\" docstrings."""
    out = []
    in_docstring = False
    for line in src.splitlines():
        stripped = line.strip()
        quote_marks = stripped.count('"""') + stripped.count("'''")
        if stripped.startswith("#"):
            continue
        if quote_marks % 2 == 1:
            in_docstring = not in_docstring  # opens or closes a multi-line docstring
            continue
        if in_docstring or quote_marks >= 2:  # inside one, or a self-contained one-liner
            continue
        out.append(line)
    return "\n".join(out)


def test_exporter_never_reads_env_or_secrets():
    """Checks actual call patterns, not prose -- the module docstrings
    legitimately explain what they DON'T do, which would false-positive on a
    naive whole-file substring check."""
    forbidden = ("load_secrets(", "load_dotenv_file(", "Secrets(", "os.environ")
    for rel in EXPORT_SOURCE_FILES:
        code = _code_lines((PROJECT_ROOT / rel).read_text(encoding="utf-8"))
        for term in forbidden:
            assert term not in code, f"{rel} calls {term!r} — must never touch .env/secrets"


def test_exporter_never_places_or_cancels_orders():
    forbidden = (".place_order(", ".cancel(", ".cancel_order(", ".cancel_all(",
                "post_order(", "create_order(", "LiveExecutor(")
    for rel in EXPORT_SOURCE_FILES:
        code = _code_lines((PROJECT_ROOT / rel).read_text(encoding="utf-8"))
        for term in forbidden:
            assert term not in code, f"{rel} calls {term!r} — exporter must be strictly read-only"


def test_exporter_never_writes_bot_config_or_db():
    """Only sqlite_store.query()/DashboardData reads are allowed -- no INSERT/
    UPDATE/DELETE against the bot DB, no writes to config.yaml."""
    forbidden = ("INSERT INTO", "UPDATE orders", "UPDATE predictions", "DELETE FROM",
                "store.execute(", "store.insert(", 'open("config.yaml"', "yaml.dump(")
    for rel in ("reporting/agent_export.py",):
        code = _code_lines((PROJECT_ROOT / rel).read_text(encoding="utf-8"))
        for term in forbidden:
            assert term not in code, f"{rel} calls {term!r} — exporter must never write to bot state"


# ---------------------------------------------------------------------------
# Functional: sanitized output, no secrets leak through free-text DB fields
# ---------------------------------------------------------------------------

def test_write_exports_creates_directory_and_all_six_files(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    assert not Path(cfg.agent_export.output_dir).exists()

    result = write_exports(cfg, db_path=db_path, now_ms=NOW_MS)

    out_dir = Path(result["output_dir"])
    assert out_dir.exists()
    expected = {"latest_status.json", "trade_summary.json", "reject_breakdown.json",
               "dashboard_snapshot.json", "daily_report.md", "obsidian_daily_note.md"}
    assert {Path(p).name for p in result["files_written"]} == expected
    for p in result["files_written"]:
        assert Path(p).exists() and Path(p).stat().st_size > 0


def test_json_exports_are_valid_and_sanitized(tmp_path):
    db_path = _live_db(tmp_path, with_secret_like_text=True)
    cfg = _cfg(tmp_path)
    result = write_exports(cfg, db_path=db_path, now_ms=NOW_MS)

    for p in result["files_written"]:
        if p.endswith(".json"):
            payload = json.loads(Path(p).read_text(encoding="utf-8"))  # must parse cleanly
            assert isinstance(payload, dict)
        text = Path(p).read_text(encoding="utf-8")
        assert FAKE_SECRET not in text, f"{p} leaked a secret-like value from a free-text DB field"


def test_latest_status_schema_matches_spec(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    status = build_latest_status(data, {"mode": "shadow_live", "heartbeat_ts_ms": NOW_MS - 5000,
                                        "panic_active": False, "kill_active": False}, cfg, NOW_MS)
    for key in ("mode", "dry_run", "live_enabled", "heartbeat_age_ms", "equity_usd", "trades",
               "winrate", "profit_factor", "expectancy_usd", "predictions", "signals",
               "diagnostics_rows", "fresh_books", "last_block_reason", "errors"):
        assert key in status, f"latest_status.json missing required key: {key}"
    assert status["live_enabled"] is False
    assert status["dry_run"] is True
    assert status["heartbeat_age_ms"] == 5000


def test_daily_report_recommends_not_live_ready_when_sample_small(tmp_path):
    db_path = _live_db(tmp_path)  # only 1 exit row
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    state = {"mode": "shadow_live", "heartbeat_ts_ms": NOW_MS, "panic_active": False, "kill_active": False}
    report = build_daily_report_md(data, state, cfg, NOW_MS)
    assert "NOT LIVE READY" in report
    assert f"1/{MIN_TRADES_FOR_LIVE_REVIEW}" in report
    assert "## Readiness checklist" in report
    assert "## Reject breakdown" in report
    assert "## Min-order blockers" in report


def test_daily_report_never_recommends_going_live_regardless_of_sample(tmp_path):
    """Even with a large sample, the export must never say the bot is ready
    for live trading -- that call requires manual review."""
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    for i in range(40):
        store.insert("exits", {"ts_ms": NOW_MS - i * 1000, "market_id": f"m{i}",
                               "reason": "TAKE_PROFIT", "pnl_usd": 0.1, "hold_seconds": 2.0,
                               "price": 0.6, "shares": 1.0})
    store.close()
    cfg = _cfg(tmp_path)
    data = DashboardData(path)
    state = {"mode": "shadow_live", "heartbeat_ts_ms": NOW_MS, "panic_active": False, "kill_active": False}
    report = build_daily_report_md(data, state, cfg, NOW_MS)
    assert "CONTINUE SHADOW" in report
    assert not re.search(r"ready\s+for\s+live|go\s+live|start\s+live", report, re.IGNORECASE)


def test_obsidian_note_has_frontmatter_and_no_title_duplication(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    state = {"mode": "shadow_live", "heartbeat_ts_ms": NOW_MS, "panic_active": False, "kill_active": False}
    note = build_obsidian_note_md(data, state, cfg, NOW_MS)
    assert note.startswith("---\n")
    assert "tags: [poly_alpha_sniper, hermes, shadow_live]" in note
    assert "live_enabled: false" in note
    assert note.count("# Poly Alpha Sniper Daily") == 1  # no duplicated title


def test_dashboard_snapshot_marks_classification_not_implemented(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    state = {"mode": "shadow_live", "heartbeat_ts_ms": NOW_MS, "panic_active": False, "kill_active": False}
    snap = build_dashboard_snapshot(data, state, cfg, NOW_MS)
    assert snap["classification_framework"] == "not_implemented"


def test_write_exports_is_safe_to_call_repeatedly(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    r1 = write_exports(cfg, db_path=db_path, now_ms=NOW_MS)
    r2 = write_exports(cfg, db_path=db_path, now_ms=NOW_MS + 1000)
    assert r1["files_written"] == r2["files_written"]  # same paths, overwritten in place


def test_write_exports_handles_missing_db_gracefully(tmp_path):
    cfg = _cfg(tmp_path)
    result = write_exports(cfg, db_path=str(tmp_path / "nope.db"), now_ms=NOW_MS)
    assert result["has_data"] is False
    for p in result["files_written"]:
        assert Path(p).exists()  # still writes files, just reflecting empty state


# ---------------------------------------------------------------------------
# build_hermes_brief -- fixed verdict rules (Part B/E spec)
# ---------------------------------------------------------------------------

def _state(**overrides):
    base = {"mode": "shadow_live", "heartbeat_ts_ms": NOW_MS, "panic_active": False, "kill_active": False}
    base.update(overrides)
    return base


def test_hermes_brief_unavailable_with_no_data(tmp_path):
    cfg = _cfg(tmp_path)
    data = DashboardData(str(tmp_path / "nope.db"))
    brief = build_hermes_brief(data, _state(), cfg, NOW_MS)
    assert brief["available"] is False


def test_hermes_brief_sample_too_small(tmp_path):
    db_path = _live_db(tmp_path)  # 1 exit row
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    brief = build_hermes_brief(data, _state(), cfg, NOW_MS)
    assert brief["available"] is True
    assert "CONTINUE SHADOW — SAMPLE TOO SMALL" in brief["verdict"]
    assert "1/30" in brief["verdict"]
    assert brief["live_readiness_status"] == "NOT READY"


def test_hermes_brief_not_live_ready_on_errors(tmp_path):
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    for i in range(35):
        store.insert("exits", {"ts_ms": NOW_MS - i * 1000, "market_id": f"m{i}",
                               "reason": "TAKE_PROFIT", "pnl_usd": 0.1, "hold_seconds": 2.0,
                               "price": 0.6, "shares": 1.0})
    store.insert("errors", {"ts_ms": NOW_MS, "where_": "test", "error": "boom"})
    store.close()
    cfg = _cfg(tmp_path)
    data = DashboardData(path)
    brief = build_hermes_brief(data, _state(), cfg, NOW_MS)
    assert "NOT LIVE READY" in brief["verdict"]
    assert "1 error" in brief["verdict"]


def test_hermes_brief_investigate_before_live_on_extra_signals(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    for signal in ("duplicate_process", "stuck_order", "stale_db", "stale_heartbeat"):
        brief = build_hermes_brief(data, _state(), cfg, NOW_MS, extra_signals={signal: True})
        assert "INVESTIGATE BEFORE LIVE" in brief["verdict"], f"signal={signal}"


def test_hermes_brief_missing_extra_signals_defaults_to_not_detected(tmp_path):
    """Unset/unknown signals must never be silently assumed true."""
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    brief = build_hermes_brief(data, _state(), cfg, NOW_MS, extra_signals=None)
    assert "INVESTIGATE BEFORE LIVE" not in brief["verdict"]
    brief2 = build_hermes_brief(data, _state(), cfg, NOW_MS, extra_signals={})
    assert "INVESTIGATE BEFORE LIVE" not in brief2["verdict"]


def test_hermes_brief_continue_shadow_when_sample_sufficient_and_healthy(tmp_path):
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    for i in range(35):
        store.insert("exits", {"ts_ms": NOW_MS - i * 1000, "market_id": f"m{i}",
                               "reason": "TAKE_PROFIT", "pnl_usd": 0.1, "hold_seconds": 2.0,
                               "price": 0.6, "shares": 1.0})
    store.close()
    cfg = _cfg(tmp_path)
    data = DashboardData(path)
    brief = build_hermes_brief(data, _state(), cfg, NOW_MS)
    assert brief["verdict"] == "CONTINUE SHADOW"
    assert brief["live_readiness_status"] == "SAMPLE SUFFICIENT — MANUAL REVIEW REQUIRED"


def test_hermes_brief_never_says_ready_for_live(tmp_path):
    """No matter how healthy, the brief must never say the bot is ready to
    go live -- that's always a human, manual-review decision."""
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    for i in range(100):
        store.insert("exits", {"ts_ms": NOW_MS - i * 1000, "market_id": f"m{i}",
                               "reason": "TAKE_PROFIT", "pnl_usd": 0.5, "hold_seconds": 2.0,
                               "price": 0.6, "shares": 1.0})
    store.close()
    cfg = _cfg(tmp_path)
    data = DashboardData(path)
    brief = build_hermes_brief(data, _state(), cfg, NOW_MS)
    full_text = " ".join(str(v) for v in brief.values())
    assert not re.search(r"ready\s+for\s+live|go\s+live|start\s+live|safe\s+to\s+trade\s+live",
                         full_text, re.IGNORECASE)


def test_hermes_brief_top_blocker_from_reject_breakdown(tmp_path):
    path = str(tmp_path / "live.db")
    store = SqliteStore(path)
    run_migrations(store)
    for _ in range(5):
        store.insert("predictions", {"ts_ms": NOW_MS, "asset": "ETH", "decision": "REJECT",
                                     "reject_reason": "REJECTED_MIN_ORDER_SIZE_TOO_HIGH"})
    store.close()
    cfg = _cfg(tmp_path)
    data = DashboardData(path)
    brief = build_hermes_brief(data, _state(), cfg, NOW_MS)
    assert "min_order" in brief["top_blocker"]


def test_dashboard_snapshot_includes_hermes_brief(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    snap = build_dashboard_snapshot(data, _state(), cfg, NOW_MS)
    assert "hermes_brief" in snap
    assert snap["hermes_brief"]["available"] is True


# ---------------------------------------------------------------------------
# CEX-freshness-pipeline fix: oracle status distinguishes "no candidate
# market" from "candidate found, price_to_beat missing"; new live_feed_state/
# last_scan_snapshot/no_shock_watchlist exports.
# ---------------------------------------------------------------------------

def test_oracle_status_no_candidate_market_reason(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_oracle_status
    cfg = _cfg(tmp_path)
    status = build_oracle_status(_state(), cfg, NOW_MS)
    assert status["available"] is False
    assert status["reason"] == "no candidate market discovered yet"


def test_oracle_status_missing_price_to_beat_distinct_reason(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_oracle_status
    cfg = _cfg(tmp_path)
    state = _state(diagnostics={"latest_oracle_anchor": {
        "market_id": "m1", "asset": "BTC", "oracle_open_price": None,
        "oracle_source": "", "oracle_anchor_quality": "missing"}})
    status = build_oracle_status(state, cfg, NOW_MS)
    assert status["available"] is False
    assert status["reason"] == "candidate market found, price_to_beat missing"
    assert status["market_id"] == "m1"  # still surfaces what WAS found


def test_oracle_status_available_when_price_to_beat_present(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_oracle_status
    cfg = _cfg(tmp_path)
    state = _state(diagnostics={"latest_oracle_anchor": {
        "market_id": "m1", "asset": "BTC", "oracle_open_price": 100_000.0,
        "oracle_source": "polymarket_event_metadata", "oracle_anchor_quality": "good"}})
    status = build_oracle_status(state, cfg, NOW_MS)
    assert status["available"] is True
    assert status["reason"] is None


def test_live_feed_state_covers_configured_assets(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_live_feed_state
    cfg = _cfg(tmp_path)
    state = _state(diagnostics={
        "cex_selected_source": {"BTC": "okx"}, "cex_freshest_age_ms": {"BTC": 200},
        "cex_freshness_degraded": {"BTC": False}})
    feed = build_live_feed_state(state, cfg, NOW_MS)
    assert set(feed.keys()) == set(cfg.assets)
    assert feed["BTC"]["status"] == "ok"
    assert feed["ETH"]["status"] == "no_data"  # never seen a tick


def test_live_feed_state_warn_status_beyond_dashboard_threshold(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_live_feed_state
    cfg = _cfg(tmp_path)
    state = _state(diagnostics={
        "cex_selected_source": {"BTC": "bybit"},
        "cex_freshest_age_ms": {"BTC": cfg.cex_freshness.dashboard_live_feed_warn_ms + 1},
        "cex_freshness_degraded": {"BTC": False}})
    feed = build_live_feed_state(state, cfg, NOW_MS)
    assert feed["BTC"]["status"] == "warn"


def test_last_scan_snapshot_passthrough(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_last_scan_snapshot
    snap = {"ts_ms": NOW_MS, "asset": "BTC", "block_reason": "rejected_by_no_shock"}
    state = _state(diagnostics={"last_scan_snapshot": snap})
    assert build_last_scan_snapshot(state) == snap


def test_last_scan_snapshot_defaults_to_empty_dict_not_none(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_last_scan_snapshot
    assert build_last_scan_snapshot(_state()) == {}


def test_no_shock_watchlist_passthrough(tmp_path):
    from poly_alpha_sniper.reporting.agent_export import build_no_shock_watchlist
    entries = [{"ts_ms": NOW_MS, "asset": "SOL", "shock_score": 0.8}]
    state = _state(diagnostics={"watchlist_no_shock_near_miss": entries})
    assert build_no_shock_watchlist(state) == entries


def test_dashboard_snapshot_includes_new_freshness_pipeline_fields(tmp_path):
    db_path = _live_db(tmp_path)
    cfg = _cfg(tmp_path)
    data = DashboardData(db_path)
    snap = build_dashboard_snapshot(data, _state(), cfg, NOW_MS)
    assert "live_feed_state" in snap
    assert "last_scan_snapshot" in snap
    assert "no_shock_watchlist" in snap
    assert set(snap["live_feed_state"].keys()) == set(cfg.assets)
