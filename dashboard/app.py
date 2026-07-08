"""Poly Alpha Sniper dashboard (Streamlit).

Run:
  streamlit run poly_alpha_sniper/dashboard/app.py --server.address 0.0.0.0 --server.port 8501

Read-only. Auth-gated. Mobile-friendly. Auto-refreshes every
cfg.dashboard.refresh_seconds. Reads the SAME SQLite file as the runtime
(project-root anchored resolution — launch directory irrelevant). Demo data
appears ONLY when no database file exists, always labeled. No trading
controls exist. No secrets displayed.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# streamlit runs this file as a script: bootstrap the package path
_PKG_PARENT = str(Path(__file__).resolve().parent.parent.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

import streamlit as st  # noqa: E402

from poly_alpha_sniper.core.config_loader import load_config, load_dotenv_file  # noqa: E402
from poly_alpha_sniper.dashboard import charts, metrics  # noqa: E402
from poly_alpha_sniper.dashboard.auth import check_auth  # noqa: E402
from poly_alpha_sniper.dashboard.components import (  # noqa: E402
    metric_card_row, scrollable_table, status_badges, warning_banner)
from poly_alpha_sniper.dashboard.db_reader import (  # noqa: E402
    DEMO_LABEL, DashboardData, demo_data, read_runtime_state, resolve_db_path)
from poly_alpha_sniper.dashboard.mobile_layout import inject_mobile_css  # noqa: E402


def _fmt_ts(ts_ms) -> str:
    if not ts_ms:
        return "—"
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    age_s = time.time() - ts_ms / 1000
    return f"{dt.strftime('%H:%M:%S')}Z ({age_s:.0f}s ago)"


def main() -> None:
    st.set_page_config(page_title="Poly Alpha Sniper", page_icon="🎯",
                       layout="wide", initial_sidebar_state="collapsed")
    inject_mobile_css()

    if not check_auth():
        st.stop()

    load_dotenv_file()  # ensure DATABASE_URL from project .env (canonical)
    cfg = load_config()
    db_path = resolve_db_path()
    data = DashboardData(db_path)
    state = read_runtime_state()
    diag = state.get("diagnostics", {}) if isinstance(state.get("diagnostics"), dict) else {}
    demo = not data.has_data

    starting = cfg.risk.starting_bankroll_usd
    is_mobile = st.sidebar.checkbox("Phone layout", value=False)
    auto = st.sidebar.checkbox("Auto-refresh", value=True)
    interval = max(2, int(cfg.dashboard.refresh_seconds))
    st.sidebar.caption(f"refresh every {interval}s — read-only, no trading controls")
    with st.sidebar.expander("📱 Access from your phone"):
        st.markdown(
            "1. On this PC run `ipconfig` and note the IPv4 address.\n"
            "2. Allow the port through Windows Firewall (admin PowerShell):\n"
            "```\nnetsh advfirewall firewall add rule name=\"PAS Dashboard\" "
            "dir=in action=allow protocol=TCP localport=8501\n```\n"
            "3. On the phone (same Wi-Fi): `http://LOCAL_PC_IP:8501`\n\n"
            "For remote access use **Tailscale** and `http://<tailscale-ip>:8501`.\n"
            "⚠️ Never expose this dashboard publicly without auth.")
    if st.sidebar.button("🔄 Refresh now"):
        st.rerun()

    st.title("🎯 Poly Alpha Sniper")

    if demo:
        warning_banner(DEMO_LABEL + " — no database file found at " + db_path)

    # ------------------------------------------------------------------
    # data connection diagnostic panel (always first: is anything stale?)
    # ------------------------------------------------------------------
    info = data.db_info()
    hb = state.get("heartbeat_ts_ms", 0)
    with st.expander("🔌 Data connection & runtime liveness", expanded=True):
        c1, c2 = (st, st) if is_mobile else st.columns(2)
        with (c1 if not is_mobile else st.container()):
            st.markdown(
                f"**database:** `{info['path']}`\n\n"
                f"exists: **{info['exists']}** · readable: **{info['readable']}** · "
                f"size: {info['size_bytes'] / 1024:.0f} KB\n\n"
                f"db last write: **{_fmt_ts(info['last_write_ms'])}**\n\n"
                f"bot heartbeat: **{_fmt_ts(hb)}**\n\n"
                f"page refreshed: **{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z**"
                + (" (auto)" if auto else " (manual)"))
        with (c2 if not is_mobile else st.container()):
            lt = info["last_ts"]
            st.markdown(
                "**latest rows:**\n\n"
                f"prediction: {_fmt_ts(lt.get('predictions'))}\n\n"
                f"signal: {_fmt_ts(lt.get('signals'))}\n\n"
                f"diagnostic: {_fmt_ts(lt.get('shadow_diagnostics'))}\n\n"
                f"gate result: {_fmt_ts(lt.get('balanced_alpha_gate_results'))}\n\n"
                f"error: {_fmt_ts(lt.get('errors'))}")
        st.caption("row counts: " + " · ".join(
            f"{t}={n}" for t, n in info["row_counts"].items()))

    if demo:
        d = demo_data()
        preds, exit_rows, pnl_rows = d["predictions"], d["exits"], d["pnl"]
        gate_rows, positions_rows, fq_rows, lat_rows = [], [], [], []
        diag_rows, incidents, near_misses = [], [], []
        panic_rows, wd_rows, backups, recon_rows, rl_rows = [], [], [], [], []
        orders_rows, failed_orders, snapshots = [], [], []
    else:
        preds = data.predictions()
        exit_rows = data.exits()
        pnl_rows = data.pnl_series()
        gate_rows = data.gate_results()
        diag_rows = data.diagnostics(200)
        positions_rows = data.recent("positions", 50)
        fq_rows = data.recent("fill_quality", 200)
        lat_rows = data.recent("latency_metrics", 20)
        incidents = data.recent("incident_reports", 30)
        near_misses = data.recent("near_misses", 50)
        panic_rows = data.recent("panic_events", 20)
        wd_rows = data.recent("watchdog_events", 20)
        backups = data.recent("database_backups", 10)
        recon_rows = data.recent("reconciliation_events", 20)
        rl_rows = data.recent("rate_limit_usage", 20)
        orders_rows = data.orders()
        failed_orders = data.failed_orders()
        snapshots = data.recent("market_snapshots", 1)

    # ------------------------------------------------------------------
    # live pipeline strip: WHY are there (no) predictions right now?
    # ------------------------------------------------------------------
    discovered = diag.get("discovered_markets",
                          (snapshots[0].get("n_markets") if snapshots else 0) or 0)
    fresh_books = diag.get("fresh_books", "n/a*")
    total_books = diag.get("total_books", "n/a*")
    last_block = diag.get("last_block_reason", "")
    if not last_block and diag_rows:
        last_block = f"{diag_rows[0].get('reason')} ({str(diag_rows[0].get('detail'))[:60]})"
    reject_counts = metrics.reject_breakdown(preds)
    min_order_blocked = sum(v for k, v in reject_counts.items()
                            if "MIN_ORDER" in str(k))

    status_badges({
        "mode": state.get("mode") or cfg.mode.trading_mode,
        "dry_run": bool(cfg.mode.dry_run),
        "panic clear": not state.get("panic_active", False),
        "kill clear": not state.get("kill_active", False),
        "aggression": state.get("aggression_mode", "NORMAL"),
        "heartbeat": "fresh" if hb and (time.time() * 1000 - hb) < 60_000 else "STALE",
    })
    metric_card_row([
        ("Markets discovered", discovered, None),
        ("Predictions", len(preds) if not demo else "demo", None),
        ("Signals", info["row_counts"].get("signals", 0), None),
        ("Diagnostics rows", info["row_counts"].get("shadow_diagnostics", 0), None),
        ("Fresh books", f"{fresh_books}/{total_books}", None),
        ("Loop iterations", diag.get("prediction_loop_iterations", "n/a*"), None),
        ("Min-order blocked", min_order_blocked, None),
        ("Errors", info["row_counts"].get("errors", 0), None),
    ], is_mobile)
    if isinstance(fresh_books, str):
        st.caption("*extended runtime diagnostics appear after the next bot restart")

    if not demo and not preds:
        st.info("**No predictions yet — reason:** " + data.why_no_predictions())
    elif not demo and last_block:
        st.caption(f"last pipeline block: {last_block}")

    # ------------------------------------------------------------------
    curve = metrics.equity_curve(pnl_rows, starting)
    equity_now = curve[-1]["equity"] if curve else starting
    dd = metrics.max_drawdown(pnl_rows, starting)
    metric_card_row([
        ("Equity", f"${equity_now:.2f}", f"{metrics.compounded_roi(pnl_rows, starting):+.1f}%"),
        ("Trades", len(exit_rows), None),
        ("Winrate", f"{metrics.winrate(exit_rows):.0%}", None),
        ("Profit factor", metrics.profit_factor(exit_rows), None),
        ("Expectancy", f"${metrics.expectancy(exit_rows):.3f}", None),
        ("Max DD", f"${dd['usd']:.2f}", f"-{dd['pct']:.1f}%"),
        ("Avg edge", metrics.avg_edge(preds), None),
        ("Fill quality", metrics.fill_quality_avg(fq_rows), None),
    ], is_mobile)

    if is_mobile:
        st.plotly_chart(charts.equity_curve_fig(curve), width="stretch")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(charts.equity_curve_fig(curve), width="stretch")
        with col2:
            st.plotly_chart(charts.drawdown_fig(curve), width="stretch")

    with st.expander("🩺 Pipeline diagnostics (why signals pass or block)",
                     expanded=not demo and not preds):
        if diag:
            st.write({k: diag.get(k) for k in (
                "prediction_loop_iterations", "last_prediction_loop_ts",
                "last_prediction_ts", "last_block_reason", "discovered_markets",
                "fresh_books", "total_books", "cex_ticks_by_asset",
                "price_windows_ready", "latest_prices", "last_discovery_ts",
                "cex_selected_source", "cex_freshest_age_ms",
                "cex_no_fresh_count_by_source", "cex_live_staleness_budget_ms",
                "cex_shadow_diag_budget_ms")
                if k in diag})
        else:
            st.caption("runtime diagnostics not in state.json yet — restart the "
                       "bot to enable (dashboard-side data below still live).")
        scrollable_table(diag_rows[:30], "shadow_diagnostics (latest blocks)")
        st.write("reject breakdown (predictions):", reject_counts)
        if min_order_blocked:
            st.caption("Min-order rejections mean the signal already had edge/confidence — "
                      "the blocker is Polymarket's share minimum vs. this bankroll's "
                      "max_trade_usd, not signal quality.")
            scrollable_table(
                metrics.min_order_sizing_rows(preds, cfg.risk.max_trade_usd),
                "Min-order sizing detail (why each was infeasible)")

    with st.expander("🎚️ Tiers & gate results", expanded=False):
        tier_dist = metrics.tier_distribution(preds)
        if tier_dist:
            st.plotly_chart(charts.tier_distribution_fig(tier_dist), width="stretch")
        st.write("B allowed vs skipped:", metrics.b_allowed_vs_skipped(gate_rows))
        scrollable_table(gate_rows[:20], "Recent gate decisions")

    with st.expander("📈 Positions, trades & orders", expanded=False):
        yes_pos, no_pos = metrics.open_positions_split(positions_rows)
        scrollable_table(yes_pos, "Open YES positions")
        scrollable_table(no_pos, "Open NO positions")
        scrollable_table(exit_rows[:25], "Recent exits")
        scrollable_table(orders_rows[:25], "Recent orders")
        scrollable_table(failed_orders[:15], "Failed orders")

    with st.expander("🔍 Predictions & near misses", expanded=False):
        scrollable_table(preds[:30], "Recent predictions")
        scrollable_table(near_misses[:20], "Near misses")

    with st.expander("🧪 Calibration", expanded=False):
        try:
            from poly_alpha_sniper.strategy.calibration_report import build_report
            report = build_report(preds)
            st.plotly_chart(charts.calibration_fig(report["buckets"]), width="stretch")
            st.write({k: report[k] for k in ("n", "brier", "calibration_error",
                                             "overconfident", "underconfident")})
        except Exception as exc:  # noqa: BLE001
            st.caption(f"calibration unavailable: {exc}")

    with st.expander("🛡️ Ops: incidents, watchdog, backups, limits", expanded=False):
        scrollable_table(panic_rows, "Panic events")
        scrollable_table(incidents, "Incident reports")
        scrollable_table(recon_rows, "Reconciliation events")
        scrollable_table(wd_rows, "Watchdog events")
        scrollable_table(backups, "Database backups")
        scrollable_table(rl_rows, "Rate limit usage")
        st.plotly_chart(charts.latency_fig(metrics.latency_percentiles(lat_rows)),
                        width="stretch")

    st.caption("poly_alpha_sniper — read-only dashboard · "
               + (DEMO_LABEL if demo else f"live db: {Path(db_path).name}")
               + f" · refreshed {datetime.now(timezone.utc).strftime('%H:%M:%S')}Z")

    if auto:
        time.sleep(interval)
        st.rerun()


main()
