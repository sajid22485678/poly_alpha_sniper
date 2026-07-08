"""Poly Alpha Sniper — Premium Dashboard (Claude x Hermes).

Run:
  streamlit run poly_alpha_sniper/dashboard/app.py --server.address 0.0.0.0 --server.port 8501

Read-only. Auth-gated. Mobile-friendly. Auto-refreshes every
cfg.dashboard.refresh_seconds. Reads the SAME SQLite file as the runtime
(project-root anchored resolution — launch directory irrelevant). Demo data
appears ONLY when no database file exists, always labeled. No trading
controls exist anywhere in this file. No secrets displayed. live_enabled is
derived structurally from cfg.mode.trading_mode (never from .env).
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

from poly_alpha_sniper.core.config_loader import TradingMode, load_config, load_dotenv_file  # noqa: E402
from poly_alpha_sniper.dashboard import charts, metrics  # noqa: E402
from poly_alpha_sniper.dashboard.auth import check_auth  # noqa: E402
from poly_alpha_sniper.dashboard.components import (  # noqa: E402
    disclaimer, hermes_brief_card, hero_header, live_status_badge, metric_card_row,
    not_available, not_implemented, scrollable_table, section_title, status_badges,
    warning_banner)
from poly_alpha_sniper.dashboard.db_reader import (  # noqa: E402
    DEMO_LABEL, DashboardData, demo_data, read_runtime_state, resolve_db_path)
from poly_alpha_sniper.dashboard.mobile_layout import inject_mobile_css  # noqa: E402
from poly_alpha_sniper.dashboard.process_health import check_single_instance, wal_status  # noqa: E402
from poly_alpha_sniper.dashboard.theme import inject_premium_theme  # noqa: E402

BOT_NAME = "Claude x Hermes / Poly Alpha Sniper"
HERMES_WORKSPACE_DIR = "D:/claude/agent_readonly/hermes_poly_workspace"


def _fmt_ts(ts_ms) -> str:
    if not ts_ms:
        return "—"
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    age_s = time.time() - ts_ms / 1000
    return f"{dt.strftime('%H:%M:%S')}Z ({age_s:.0f}s ago)"


def _fmt_ms(ms) -> str:
    return "not available" if ms is None else f"{ms}ms"


def main() -> None:
    st.set_page_config(page_title="Poly Alpha Sniper", page_icon="🎯",
                       layout="wide", initial_sidebar_state="collapsed")
    inject_mobile_css()
    inject_premium_theme()

    if not check_auth():
        st.stop()

    load_dotenv_file()  # ensure DATABASE_URL from project .env (canonical)
    cfg = load_config()
    db_path = resolve_db_path()
    data = DashboardData(db_path)
    state = read_runtime_state()
    diag = state.get("diagnostics", {}) if isinstance(state.get("diagnostics"), dict) else {}
    demo = not data.has_data

    # Structural signal only -- never reads .env's LIVE_TRADING_ENABLED flag.
    # shadow_live/simulation can never reach the live order path regardless
    # of that flag's value, so this is both accurate and secret-free.
    live_enabled = TradingMode(cfg.mode.trading_mode).is_live

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
            "⚠️ Never expose this dashboard publicly without auth — "
            "this bot must never be reachable from the open internet.")
    if st.sidebar.button("🔄 Refresh now"):
        st.rerun()

    if demo:
        warning_banner(DEMO_LABEL + " — no database file found at " + db_path)

    # ------------------------------------------------------------------
    # data loading (single read per rerun, no cross-rerun caching)
    # ------------------------------------------------------------------
    if demo:
        d = demo_data()
        preds, exit_rows, pnl_rows = d["predictions"], d["exits"], d["pnl"]
        gate_rows, positions_rows, fq_rows, lat_rows = [], [], [], []
        diag_rows, incidents, near_misses, signals_rows = [], [], [], []
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
        signals_rows = data.recent("signals", 5)
        panic_rows = data.recent("panic_events", 20)
        wd_rows = data.recent("watchdog_events", 20)
        backups = data.recent("database_backups", 10)
        recon_rows = data.recent("reconciliation_events", 20)
        rl_rows = data.recent("rate_limit_usage", 20)
        orders_rows = data.orders()
        failed_orders = data.failed_orders()
        snapshots = data.recent("market_snapshots", 1)

    info = data.db_info()
    hb = state.get("heartbeat_ts_ms", 0)
    hb_fresh = bool(hb and (time.time() * 1000 - hb) < 60_000)

    # ==================================================================
    # 1) HERO HEADER
    # ==================================================================
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    hero_header(
        f"🎯 {BOT_NAME}",
        f"session: {now_str} UTC" + (" · " + DEMO_LABEL if demo else ""))
    live_status_badge(live_enabled)
    status_badges({
        "mode": state.get("mode") or cfg.mode.trading_mode,
        "dry_run": bool(cfg.mode.dry_run),
        "heartbeat": "fresh" if hb_fresh else "STALE",
        "panic clear": not state.get("panic_active", False),
        "kill clear": not state.get("kill_active", False),
        "aggression": state.get("aggression_mode", "NORMAL"),
    })

    # ==================================================================
    # 2) BIG PnL PANEL
    # ==================================================================
    section_title("💰 PnL")
    curve = metrics.equity_curve(pnl_rows, starting)
    equity_now = curve[-1]["equity"] if curve else starting
    dd = metrics.max_drawdown(pnl_rows, starting)
    metric_card_row([
        ("Equity", f"${equity_now:.2f}", f"{metrics.compounded_roi(pnl_rows, starting):+.1f}%"),
        ("Today PnL", f"${metrics.today_pnl(pnl_rows):+.4f}", None),
        ("All-time PnL", f"${metrics.all_time_pnl(pnl_rows):+.4f}", None),
        ("Trades", len(exit_rows), None),
        ("Winrate", f"{metrics.winrate(exit_rows):.0%}", None),
        ("Profit factor", metrics.profit_factor(exit_rows), None),
        ("Expectancy", f"${metrics.expectancy(exit_rows):.3f}", None),
        ("Avg edge", metrics.avg_edge(preds), None),
        ("Fill quality", metrics.fill_quality_avg(fq_rows), None),
        ("Max DD", f"${dd['usd']:.2f}", f"-{dd['pct']:.1f}%"),
    ], is_mobile)
    if len(exit_rows) < 30:
        disclaimer(f"Sample size is {len(exit_rows)} completed trades — below the 30-trade "
                  f"floor for any statistical confidence in the numbers above.")

    if is_mobile:
        st.plotly_chart(charts.equity_curve_fig(curve), width="stretch")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(charts.equity_curve_fig(curve), width="stretch")
        with col2:
            st.plotly_chart(charts.drawdown_fig(curve), width="stretch")

    # ==================================================================
    # 3) LIVE MARKET STATE
    # ==================================================================
    section_title("📡 Live market state")
    discovered = diag.get("discovered_markets",
                          (snapshots[0].get("n_markets") if snapshots else 0) or 0)
    fresh_books_n = diag.get("fresh_books")
    total_books_n = diag.get("total_books")
    stale_books_n = (total_books_n - fresh_books_n) if (fresh_books_n is not None
                                                        and total_books_n is not None) else None
    no_fresh_cex_by_source = diag.get("cex_no_fresh_count_by_source") or {}
    no_fresh_cex_total = sum(no_fresh_cex_by_source.values()) if no_fresh_cex_by_source else None
    metric_card_row([
        ("Discovered markets", discovered, None),
        ("Fresh order books", f"{fresh_books_n}/{total_books_n}" if fresh_books_n is not None else "not available", None),
        ("Stale book count", stale_books_n if stale_books_n is not None else "not available", None),
        ("No-fresh-CEX count", no_fresh_cex_total if no_fresh_cex_total is not None else "not available", None),
    ], is_mobile)
    not_available("Active markets (distinct from discovered)",
                  "this runtime does not track a separate active-vs-discovered count")
    if no_fresh_cex_by_source:
        st.caption("no-fresh-CEX by source: " + ", ".join(
            f"{k}={v}" for k, v in no_fresh_cex_by_source.items()))
    mstate = metrics.latest_market_state(preds, diag)
    if not mstate["available"]:
        not_available("latest signal/market detail", "no predictions recorded yet")
    else:
        metric_card_row([
            ("Asset", mstate["asset"] or "—", None),
            ("Decision", mstate["decision_label"], None),
            ("Direction", mstate["direction"] or "—", None),
            ("Market price (signal side)",
             f"{mstate['signal_side_price']:.3f}" if mstate["signal_side_price"] is not None else "n/a", None),
            ("CEX source", mstate["cex_selected_source"] or "not available", None),
            ("CEX source age", _fmt_ms(mstate["cex_freshest_age_ms"]), None),
            ("CEX price", f"{mstate['cex_price']:.2f}" if mstate["cex_price"] is not None else "not available", None),
            ("Books fresh/total",
             f"{mstate['fresh_books']}/{mstate['total_books']}"
             if mstate["fresh_books"] is not None else "not available", None),
        ], is_mobile)
        st.caption(f"market: {mstate['market_title'] or '—'}")
        st.caption(f"last block reason: {mstate['last_block_reason'] or '—'}")
        if mstate["decision_raw"] == "REJECT" and mstate["reject_reason"]:
            st.caption(f"latest rejection: {mstate['reject_reason']}")
        st.caption("Note: only the signal-side price is recorded per prediction row — "
                  "the opposite side's price is not separately captured in this schema, "
                  "so it is not shown here rather than estimated.")

    # ==================================================================
    # 4) SIGNAL INTELLIGENCE
    # ==================================================================
    section_title("🧠 Signal intelligence")
    latest_pred = max(preds, key=lambda r: r.get("ts_ms") or 0) if preds else None
    latest_signal = signals_rows[0] if signals_rows else None
    latest_accepted = next((p for p in sorted(preds, key=lambda r: r.get("ts_ms") or 0, reverse=True)
                            if p.get("decision") in ("APPROVE", "SHADOW_ONLY")), None)
    latest_reject = next((p for p in sorted(preds, key=lambda r: r.get("ts_ms") or 0, reverse=True)
                          if p.get("decision") == "REJECT"), None)
    metric_card_row([
        ("Predictions count", info["row_counts"].get("predictions", len(preds)), None),
        ("Signals count", info["row_counts"].get("signals", 0), None),
    ], is_mobile)
    if latest_pred is None:
        not_available("signal intelligence", "no predictions recorded yet")
    else:
        metric_card_row([
            ("Edge", f"{(latest_pred.get('edge_after_slippage') or latest_pred.get('edge') or 0):.3f}", None),
            ("Fair probability",
             f"{latest_pred.get('fair_probability'):.3f}" if latest_pred.get("fair_probability") is not None else "n/a", None),
            ("Market ask", f"{latest_pred.get('polymarket_price'):.3f}" if latest_pred.get("polymarket_price") is not None else "n/a", None),
            ("Tier", latest_pred.get("tier") or "—", None),
        ], is_mobile)
        mo_summary = metrics.min_order_summary(preds, cfg.risk.max_trade_usd)
        feasible = "infeasible (min-order blocked)" if (latest_pred.get("reject_reason") or "").find("MIN_ORDER") >= 0 \
            else ("feasible" if mo_summary["blocked_count"] == 0 else "see min-order panel")
        st.caption(f"min-order feasibility (latest signal): {feasible}")
        st.caption(f"latest signal: {latest_signal or 'not available'}" if latest_signal is None
                  else f"latest signal: {latest_signal.get('asset')} {latest_signal.get('direction')} "
                       f"kind={latest_signal.get('kind')} z={latest_signal.get('zscore')}")
        st.caption(f"latest accepted candidate: "
                  f"{latest_accepted.get('asset')} {latest_accepted.get('direction')} "
                  f"tier={latest_accepted.get('tier')} edge={latest_accepted.get('edge_after_slippage')}"
                  if latest_accepted else "latest accepted candidate: none recorded")
        st.caption(f"latest rejected candidate: "
                  f"{latest_reject.get('asset')} {latest_reject.get('direction')} "
                  f"reason={latest_reject.get('reject_reason')}"
                  if latest_reject else "latest rejected candidate: none recorded")
        not_implemented("Classification (CONTINUATION / FADE / NO_TRADE)")

    # ==================================================================
    # 6) REJECT BREAKDOWN
    # ==================================================================
    section_title("🚫 Reject breakdown")
    unified = metrics.unified_reject_breakdown(diag_rows, preds)
    metric_card_row([(b.replace("_", " "), unified["buckets"].get(b, 0), None)
                     for b in metrics.CANONICAL_REJECT_BUCKETS], is_mobile)
    if unified["buckets"].get("other"):
        st.caption(f"other (unmapped reasons, not dropped): {unified['buckets']['other']} "
                  f"— {unified['raw_by_bucket'].get('other', {})}")

    # ==================================================================
    # 7) MIN-ORDER SIZING PANEL
    # ==================================================================
    section_title("📏 Min-order sizing")
    mo = metrics.min_order_summary(preds, cfg.risk.max_trade_usd)
    if mo["blocked_count"] == 0:
        st.caption("no min-order rejections recorded")
    else:
        latest = mo["latest"]
        metric_card_row([
            ("Blocked count", mo["blocked_count"], None),
            ("Latest ask", f"${latest['ask_price']:.4f}", None),
            ("Min shares", latest["min_shares"], None),
            ("Required USD", f"${latest['min_required_usd']:.2f}", None),
            ("Configured max_trade_usd", f"${latest['configured_max_trade_usd']:.2f}", None),
            ("Shortfall USD", f"${latest['shortfall_usd']:.2f}", None),
        ], is_mobile)
        st.caption("These signals already had edge/confidence — the blocker is Polymarket's "
                  "share minimum vs. this bankroll's max_trade_usd, not signal quality.")
        scrollable_table(metrics.min_order_sizing_rows(preds, cfg.risk.max_trade_usd),
                         "Min-order sizing detail")

    # ==================================================================
    # 5) TRADE TAPE
    # ==================================================================
    section_title("📼 Trade tape")
    disclaimer(f"mode={state.get('mode') or cfg.mode.trading_mode} — every row below is a "
              f"SHADOW (simulated) order. No real orders have been placed or cancelled.")
    yes_pos, no_pos = metrics.open_positions_split(positions_rows)
    with st.expander("Open shadow positions / recent orders / recent exits", expanded=not demo):
        scrollable_table(yes_pos, "Open YES positions")
        scrollable_table(no_pos, "Open NO positions")
        scrollable_table(orders_rows[:25], "Recent shadow orders")
        scrollable_table(exit_rows[:25], "Recent exits")
        scrollable_table(failed_orders[:15], "Failed orders")

    # ==================================================================
    # 8) RUNTIME HEALTH
    # ==================================================================
    section_title("🩺 Runtime health")
    with st.expander("Runtime & data-connection diagnostics", expanded=not demo and not preds):
        c1, c2 = (st, st) if is_mobile else st.columns(2)
        with (c1 if not is_mobile else st.container()):
            st.markdown(
                f"**dashboard DB path:** `{info['path']}`\n\n"
                f"exists: **{info['exists']}** · readable: **{info['readable']}** · "
                f"size: {info['size_bytes'] / 1024:.0f} KB\n\n"
                f"DB last write: **{_fmt_ts(info['last_write_ms'])}**\n\n"
                f"heartbeat age: **{_fmt_ms(int(time.time() * 1000 - hb) if hb else None)}**\n\n"
                f"page refreshed: **{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z**"
                + (" (auto)" if auto else " (manual)"))
        with (c2 if not is_mobile else st.container()):
            st.markdown(
                "**counts:**\n\n"
                f"prediction loop iterations: {diag.get('prediction_loop_iterations', 'not available')}\n\n"
                f"diagnostics rows: {info['row_counts'].get('shadow_diagnostics', 0)}\n\n"
                f"errors: {info['row_counts'].get('errors', 0)}\n\n"
                f"panic active: {state.get('panic_active', False)} · "
                f"kill active: {state.get('kill_active', False)}")
        if not demo and not preds:
            st.info("**No predictions yet — reason:** " + data.why_no_predictions())

        proc = check_single_instance()
        wal = wal_status(db_path)
        if not proc["available"]:
            not_available("bot process health", proc["reason"])
        elif proc["likely_duplicate"]:
            disclaimer(f"⚠️ SINGLE-INSTANCE WARNING: {proc['count']} shadow_live processes detected "
                      f"(expected 2 -- launcher + interpreter). Investigate before any live promotion. "
                      f"PIDs: {[m['pid'] for m in proc['matches']]}")
        else:
            st.caption(f"bot process health: OK — {proc['count']} process(es) matched "
                      f"(launcher + interpreter, normal). PIDs: {[m['pid'] for m in proc['matches']]}")
        st.caption(f"WAL file present: {wal['wal_present']} "
                  f"({wal['wal_size_bytes'] / 1024:.0f} KB)" if wal["wal_present"] else
                  "WAL file present: False")
        if incidents:
            st.caption(f"⚠️ {len(incidents)} incident report(s) recorded — see Ops expander below.")
        scrollable_table(diag_rows[:20], "shadow_diagnostics (latest blocks)")

    # ==================================================================
    # 10) HERMES AGENT PANEL (display-only -- Hermes is not installed/running;
    #     this only reports on the read-only workspace/export files that
    #     exist on disk for a future Hermes agent to consume)
    # ==================================================================
    section_title("🪐 Hermes Agent panel")
    export_dir = Path(cfg.agent_export.output_dir)
    workspace_dir = Path(HERMES_WORKSPACE_DIR)
    nightly = export_dir / "daily_report.md"
    obsidian_note = export_dir / "obsidian_daily_note.md"

    if not workspace_dir.exists():
        hermes_status = "NOT INSTALLED"
    elif not nightly.exists():
        hermes_status = "NOT CONFIGURED"
    else:
        hermes_status = "READY READ-ONLY"
    st.markdown(f"**Hermes status:** `{hermes_status}`")
    st.caption("Note: Hermes is not installed or running in this project -- this status only "
              "reflects whether the read-only workspace and exported reports exist on disk for "
              "a future Hermes agent to consume. There is no read-receipt mechanism, so a "
              "\"LAST REPORT READ\" state cannot be honestly reported and is not shown.")

    with st.expander("Paths, permissions, and last export timestamps", expanded=False):
        st.markdown(
            f"**read-only source path:** `{cfg.agent_export.output_dir}`\n\n"
            f"**Obsidian target:** `{cfg.obsidian.vault_notes_dir}`\n\n"
            f"**Hermes workspace:** `{HERMES_WORKSPACE_DIR}` — "
            f"{'present' if workspace_dir.exists() else 'not created yet'}")
        if nightly.exists():
            st.markdown(f"last exported report: {_fmt_ts(int(nightly.stat().st_mtime * 1000))}")
        else:
            not_available("last exported report", "run scripts\\export_agent_readonly.bat")
        if obsidian_note.exists():
            st.markdown(f"last Obsidian-ready note generated: "
                       f"{_fmt_ts(int(obsidian_note.stat().st_mtime * 1000))}")
        else:
            not_available("last Obsidian-ready note", "run scripts\\export_agent_readonly.bat")
        st.markdown(
            "**safety permissions (by design, once a Hermes agent is connected):**\n\n"
            "- can read reports: **yes** (`D:\\claude\\agent_readonly\\poly_alpha_sniper\\`)\n"
            "- can read Obsidian notes: **yes** (`D:\\TradingVault\\`)\n"
            "- can access .env: **no**\n"
            "- can place orders: **no**\n"
            "- can cancel orders: **no**\n"
            "- can modify strategy: **no**")

    section_title("📋 Hermes Brief")
    stale_db = bool(info["last_write_ms"] and (time.time() * 1000 - info["last_write_ms"]) > 120_000)
    if demo:
        not_available("Hermes Brief", "no live database")
    else:
        from poly_alpha_sniper.reporting.agent_export import build_hermes_brief
        brief = build_hermes_brief(data, state, cfg, int(time.time() * 1000), extra_signals={
            "duplicate_process": bool(check_single_instance().get("likely_duplicate")),
            "stale_db": stale_db,
            "stale_heartbeat": not hb_fresh,
        })
        hermes_brief_card(brief)
    st.caption("Hermes Brief is computed live from the current DB/runtime state (same logic the "
              "exporter uses) -- it does not require the exporter to have run first.")

    # ------------------------------------------------------------------
    # supplementary sections (kept from the original dashboard)
    # ------------------------------------------------------------------
    with st.expander("🎚️ Tiers & gate results", expanded=False):
        tier_dist = metrics.tier_distribution(preds)
        if tier_dist:
            st.plotly_chart(charts.tier_distribution_fig(tier_dist), width="stretch")
        st.write("B allowed vs skipped:", metrics.b_allowed_vs_skipped(gate_rows))
        scrollable_table(gate_rows[:20], "Recent gate decisions")

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

    st.caption(f"{BOT_NAME} — read-only dashboard · "
              + (DEMO_LABEL if demo else f"live db: {Path(db_path).name}")
              + f" · refreshed {datetime.now(timezone.utc).strftime('%H:%M:%S')}Z")

    if auto:
        time.sleep(interval)
        st.rerun()


main()
