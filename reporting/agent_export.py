"""Read-only sanitized export layer for a future Hermes Agent / Obsidian
integration.

SAFETY CONTRACT (enforced by tests/test_agent_export.py):
- Never imports core.config_loader.load_secrets / load_dotenv_file / Secrets.
- Never reads .env, never touches any POLYMARKET_*/TELEGRAM_*/DASHBOARD_*
  credential value.
- Never writes to config.yaml or any bot state file -- output only.
- Never calls anything that places, cancels, or modifies an order.
- Reuses the exact same read-only data path the dashboard uses
  (dashboard.db_reader.DashboardData, read_runtime_state) -- both are
  already secret-free by construction.

Everything here is pure/testable except write_exports(), which is the only
function that touches the filesystem (creates the output directory and
writes files).
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

from poly_alpha_sniper.core.config_loader import TradingMode, load_config
from poly_alpha_sniper.dashboard import metrics
from poly_alpha_sniper.dashboard.db_reader import DashboardData, read_runtime_state, resolve_db_path

EXPORT_FILENAMES = (
    "latest_status.json", "trade_summary.json", "reject_breakdown.json",
    "dashboard_snapshot.json", "daily_report.md", "obsidian_daily_note.md",
)

MIN_TRADES_FOR_LIVE_REVIEW = 30


def _live_enabled(cfg) -> bool:
    """Structural signal only -- derived from config.yaml's trading_mode, not
    from .env's LIVE_TRADING_ENABLED flag. Keeps this whole export path free
    of any .env dependency while still being accurate: shadow_live/simulation
    can never reach the live order path regardless of that flag's value."""
    return TradingMode(cfg.mode.trading_mode).is_live


def _heartbeat_age_ms(state: dict, now_ms: int) -> Optional[int]:
    hb = state.get("heartbeat_ts_ms")
    if not hb:
        return None
    return max(0, now_ms - int(hb))


# ---------------------------------------------------------------------------
# Individual export payloads (pure functions: data in, dict/str out)
# ---------------------------------------------------------------------------

def build_latest_status(data: DashboardData, state: dict, cfg, now_ms: int) -> dict:
    diag = state.get("diagnostics", {}) if isinstance(state.get("diagnostics"), dict) else {}
    info = data.db_info()
    preds = data.predictions()
    exit_rows = data.exits()
    pnl_rows = data.pnl_series()
    curve = metrics.equity_curve(pnl_rows, cfg.risk.starting_bankroll_usd)
    return {
        "generated_ts_ms": now_ms,
        "mode": state.get("mode") or cfg.mode.trading_mode,
        "dry_run": bool(cfg.mode.dry_run),
        "live_enabled": _live_enabled(cfg),
        "heartbeat_age_ms": _heartbeat_age_ms(state, now_ms),
        "equity_usd": curve[-1]["equity"] if curve else cfg.risk.starting_bankroll_usd,
        "trades": len(exit_rows),
        "winrate": metrics.winrate(exit_rows),
        "profit_factor": metrics.profit_factor(exit_rows),
        "expectancy_usd": metrics.expectancy(exit_rows),
        "predictions": len(preds),
        "signals": info["row_counts"].get("signals", 0),
        "diagnostics_rows": info["row_counts"].get("shadow_diagnostics", 0),
        "fresh_books": diag.get("fresh_books"),
        "total_books": diag.get("total_books"),
        "last_block_reason": diag.get("last_block_reason"),
        "errors": info["row_counts"].get("errors", 0),
        "panic_active": bool(state.get("panic_active", False)),
        "kill_active": bool(state.get("kill_active", False)),
    }


def build_trade_summary(data: DashboardData, cfg, now_ms: int) -> dict:
    exit_rows = data.exits()
    pnl_rows = data.pnl_series()
    preds = data.predictions()
    fq_rows = data.recent("fill_quality", 200)
    dd = metrics.max_drawdown(pnl_rows, cfg.risk.starting_bankroll_usd)
    return {
        "generated_ts_ms": now_ms,
        "trades": len(exit_rows),
        "winrate": metrics.winrate(exit_rows),
        "profit_factor": metrics.profit_factor(exit_rows),
        "expectancy_usd": metrics.expectancy(exit_rows),
        "today_pnl_usd": metrics.today_pnl(pnl_rows, now_ms),
        "all_time_pnl_usd": metrics.all_time_pnl(pnl_rows),
        "avg_edge": metrics.avg_edge(preds),
        "fill_quality_avg": metrics.fill_quality_avg(fq_rows),
        "max_drawdown_usd": dd["usd"],
        "max_drawdown_pct": dd["pct"],
        "recent_exits": exit_rows[:25],
        "sample_size_note": (
            f"{len(exit_rows)}/{MIN_TRADES_FOR_LIVE_REVIEW} minimum trades for any "
            f"statistical read" if len(exit_rows) < MIN_TRADES_FOR_LIVE_REVIEW
            else f"{len(exit_rows)} trades — meets the {MIN_TRADES_FOR_LIVE_REVIEW}-trade floor"),
    }


def build_reject_breakdown_export(data: DashboardData, cfg, now_ms: int) -> dict:
    preds = data.predictions()
    diag_rows = data.diagnostics(500)
    unified = metrics.unified_reject_breakdown(diag_rows, preds)
    min_order = metrics.min_order_summary(preds, cfg.risk.max_trade_usd)
    return {"generated_ts_ms": now_ms, **unified, "min_order": min_order}


def build_oracle_status(state: dict, cfg, now_ms: int) -> dict:
    """WS3: surfaces the most recent OracleAnchor/EV computation the live
    process recorded (core.app.App._resolve_and_record_oracle_anchor /
    _oracle_ev_reject), via the same runtime_state.diagnostics path
    latest_market_state already uses. "available": False (not a fabricated
    anchor) when the process hasn't evaluated a market yet."""
    diag = state.get("diagnostics", {}) if isinstance(state.get("diagnostics"), dict) else {}
    anchor = diag.get("latest_oracle_anchor")
    ev = diag.get("latest_oracle_ev")
    if not isinstance(anchor, dict):
        return {"available": False, "enabled": cfg.oracle_ev.enabled,
               "reason": "no market evaluated yet"}
    return {
        "available": True,
        "enabled": cfg.oracle_ev.enabled,
        "generated_ts_ms": now_ms,
        "market_id": anchor.get("market_id"),
        "asset": anchor.get("asset"),
        "price_to_beat": anchor.get("oracle_open_price"),
        "oracle_source": anchor.get("oracle_source"),
        "resolution_source_url": anchor.get("resolution_source_url"),
        "oracle_open_ts_ms": anchor.get("oracle_open_ts_ms"),
        "cex_price": anchor.get("cex_price"),
        "cex_ts_ms": anchor.get("cex_ts_ms"),
        "oracle_vs_cex_basis_pct": anchor.get("oracle_vs_cex_basis"),
        "oracle_anchor_quality": anchor.get("oracle_anchor_quality"),
        "time_remaining_seconds": anchor.get("time_remaining_seconds"),
        "latest_ev": ev if isinstance(ev, dict) else None,
    }


def build_dashboard_snapshot(data: DashboardData, state: dict, cfg, now_ms: int) -> dict:
    """Everything the dashboard shows, in one payload -- lets a future agent
    reconstruct dashboard state without touching the DB directly."""
    diag = state.get("diagnostics", {}) if isinstance(state.get("diagnostics"), dict) else {}
    preds = data.predictions()
    return {
        "generated_ts_ms": now_ms,
        "latest_status": build_latest_status(data, state, cfg, now_ms),
        "trade_summary": build_trade_summary(data, cfg, now_ms),
        "reject_breakdown": build_reject_breakdown_export(data, cfg, now_ms),
        "hermes_brief": build_hermes_brief(data, state, cfg, now_ms),
        "latest_market_state": metrics.latest_market_state(preds, diag),
        "oracle_status": build_oracle_status(state, cfg, now_ms),
        "open_positions": data.recent("positions", 50),
        "recent_orders": data.orders(25),
        "classification_framework": "not_implemented",  # honest: no continuation/fade label exists yet
    }


def _readiness_checklist(cfg, status: dict) -> list[tuple[str, bool, str]]:
    trades = status["trades"]
    return [
        ("mode is shadow_live or simulation (never live)", not status["live_enabled"],
         f"mode={status['mode']}"),
        ("dry_run is True", status["dry_run"], f"dry_run={status['dry_run']}"),
        ("no panic/kill-switch active", not status["panic_active"] and not status["kill_active"],
         f"panic={status['panic_active']} kill={status['kill_active']}"),
        ("heartbeat fresh (<60s)", (status["heartbeat_age_ms"] or 10**9) < 60_000,
         f"age={status['heartbeat_age_ms']}ms"),
        (f"sample size >= {MIN_TRADES_FOR_LIVE_REVIEW} trades", trades >= MIN_TRADES_FOR_LIVE_REVIEW,
         f"trades={trades}"),
    ]


def _recommendation(trades: int) -> str:
    """Never recommends going live -- that decision requires manual review of
    the live-canary checklist. Only ever says continue shadow, with an
    explicit insufficient-sample flag when the trade count is too low to
    mean anything."""
    if trades < MIN_TRADES_FOR_LIVE_REVIEW:
        return (f"NOT LIVE READY — sample too small ({trades}/{MIN_TRADES_FOR_LIVE_REVIEW} "
                f"minimum completed trades). Continue shadow_live.")
    return ("CONTINUE SHADOW — sample size is sufficient for a first statistical read, "
           "but no automated go-live recommendation is made. Live promotion requires "
           "manual review of the live-canary checklist (see prior optimization report).")


def build_hermes_brief(data: DashboardData, state: dict, cfg, now_ms: int,
                       extra_signals: Optional[dict] = None) -> dict:
    """Structured verdict/blocker/anomaly/next-action summary -- the same
    content as the daily report's Recommendation + anomalies sections, but as
    data the dashboard's Hermes panel and any future Hermes agent can render
    directly instead of re-parsing markdown.

    extra_signals (all optional, dashboard-only checks the exporter itself
    can't perform without psutil): duplicate_process, stuck_order, stale_db,
    stale_heartbeat -- each bool. Missing/unknown signals are treated as
    "not detected", never assumed true or false silently mislabeled; the
    dashboard is responsible for passing what it actually checked.

    Verdict rules (fixed, never overridden by config or sample size):
    - any of duplicate_process/stuck_order/stale_db/stale_heartbeat -> the
      verdict includes "INVESTIGATE BEFORE LIVE"
    - errors > 0 -> the verdict includes "NOT LIVE READY"
    - trades < MIN_TRADES_FOR_LIVE_REVIEW -> the verdict includes
      "CONTINUE SHADOW — SAMPLE TOO SMALL"
    - none of the above -> "CONTINUE SHADOW" (manual review still required
      for any live promotion; this function never recommends going live)."""
    extra_signals = extra_signals or {}
    status = build_latest_status(data, state, cfg, now_ms)
    if status["predictions"] == 0 and status["trades"] == 0 and not data.has_data:
        return {"available": False, "reason": "no bot database found/readable"}

    rejects = build_reject_breakdown_export(data, cfg, now_ms)
    trades = status["trades"]

    investigate_reasons = []
    if extra_signals.get("duplicate_process"):
        investigate_reasons.append("duplicate process detected")
    if extra_signals.get("stuck_order"):
        investigate_reasons.append("stuck/non-terminal order detected")
    if extra_signals.get("stale_db"):
        investigate_reasons.append("DB has not been written to recently")
    if extra_signals.get("stale_heartbeat"):
        investigate_reasons.append("heartbeat is stale")

    verdict_parts = []
    if investigate_reasons:
        verdict_parts.append(f"INVESTIGATE BEFORE LIVE — {'; '.join(investigate_reasons)}")
    if status["errors"] > 0:
        verdict_parts.append(f"NOT LIVE READY — {status['errors']} error(s) present")
    if trades < MIN_TRADES_FOR_LIVE_REVIEW:
        verdict_parts.append(f"CONTINUE SHADOW — SAMPLE TOO SMALL ({trades}/{MIN_TRADES_FOR_LIVE_REVIEW})")
    verdict = " | ".join(verdict_parts) if verdict_parts else "CONTINUE SHADOW"

    buckets = rejects["buckets"]
    actionable = {k: v for k, v in buckets.items() if k not in ("no_shock", "no_fresh_cex_price") and v}
    if actionable:
        top_blocker_key = max(actionable, key=actionable.get)
        top_blocker = f"{top_blocker_key} ({actionable[top_blocker_key]})"
    elif any(buckets.values()):
        top_blocker_key = max(buckets, key=buckets.get)
        top_blocker = f"{top_blocker_key} ({buckets[top_blocker_key]}) — likely genuine edge scarcity, not a bug"
    else:
        top_blocker = "none recorded"

    anomalies = list(investigate_reasons)
    if status["panic_active"] or status["kill_active"]:
        anomalies.append("panic or kill-switch is currently active")
    if status["heartbeat_age_ms"] is not None and status["heartbeat_age_ms"] > 60_000:
        anomalies.append(f"heartbeat stale ({status['heartbeat_age_ms']}ms)")
    anomaly = "; ".join(anomalies) if anomalies else "none observed"

    if investigate_reasons:
        next_action = "Investigate flagged runtime issues before taking any further readiness steps."
    elif status["errors"] > 0:
        next_action = "Review the errors table before continuing to accumulate shadow samples."
    elif trades < MIN_TRADES_FOR_LIVE_REVIEW:
        next_action = f"Keep running shadow_live until at least {MIN_TRADES_FOR_LIVE_REVIEW} completed trades exist."
    else:
        next_action = "Sample is sufficient for a first statistical read; manual canary checklist review still required."

    live_readiness_status = "NOT READY" if (investigate_reasons or status["errors"] > 0
                                            or trades < MIN_TRADES_FOR_LIVE_REVIEW) else \
        "SAMPLE SUFFICIENT — MANUAL REVIEW REQUIRED"

    return {
        "available": True, "generated_ts_ms": now_ms, "verdict": verdict,
        "top_blocker": top_blocker, "anomaly": anomaly, "next_action": next_action,
        "live_readiness_status": live_readiness_status,
    }


def build_daily_report_md(data: DashboardData, state: dict, cfg, now_ms: int) -> str:
    status = build_latest_status(data, state, cfg, now_ms)
    trades_summary = build_trade_summary(data, cfg, now_ms)
    rejects = build_reject_breakdown_export(data, cfg, now_ms)
    checklist = _readiness_checklist(cfg, status)
    date_str = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(now_ms / 1000))

    hb_age_text = (f"{status['heartbeat_age_ms']}ms" if status["heartbeat_age_ms"] is not None
                  else "not available")
    lines = [
        f"# Poly Alpha Sniper — Daily Report ({date_str})", "",
        "## Summary",
        f"- mode: **{status['mode']}** · dry_run: **{status['dry_run']}** · "
        f"live_enabled: **{status['live_enabled']}**",
        f"- equity: **${status['equity_usd']:.2f}** · heartbeat age: {hb_age_text}",
        "",
        "## PnL",
        f"- today: ${trades_summary['today_pnl_usd']:+.4f}",
        f"- all-time (realized): ${trades_summary['all_time_pnl_usd']:+.4f}",
        f"- max drawdown: ${trades_summary['max_drawdown_usd']:.4f} "
        f"({trades_summary['max_drawdown_pct']:.1f}%)",
        "",
        "## Trades",
        f"- count: {trades_summary['trades']} ({trades_summary['sample_size_note']})",
        f"- winrate: {trades_summary['winrate']:.0%}",
        f"- profit factor: {trades_summary['profit_factor']}",
        f"- expectancy: ${trades_summary['expectancy_usd']:.4f}/trade",
        f"- avg edge: {trades_summary['avg_edge']}",
        f"- avg fill quality: {trades_summary['fill_quality_avg']}",
        "",
        "## Reject breakdown",
    ]
    for bucket, count in rejects["buckets"].items():
        lines.append(f"- {bucket}: {count}")
    lines += ["", "## Min-order blockers"]
    mo = rejects["min_order"]
    if mo["blocked_count"]:
        latest = mo["latest"]
        lines += [
            f"- blocked count: {mo['blocked_count']}",
            f"- latest: {latest['asset']} ask=${latest['ask_price']:.4f} "
            f"min_shares={latest['min_shares']} min_required=${latest['min_required_usd']:.2f} "
            f"configured_max_trade_usd=${latest['configured_max_trade_usd']:.2f} "
            f"shortfall=${latest['shortfall_usd']:.2f}",
        ]
    else:
        lines.append("- none")
    lines += ["", "## Notable anomalies"]
    anomalies = []
    if status["errors"]:
        anomalies.append(f"{status['errors']} error row(s) in the errors table")
    if status["panic_active"] or status["kill_active"]:
        anomalies.append("panic or kill-switch is currently active")
    if status["heartbeat_age_ms"] is not None and status["heartbeat_age_ms"] > 60_000:
        anomalies.append(f"heartbeat stale ({status['heartbeat_age_ms']}ms)")
    lines += ([f"- {a}" for a in anomalies] if anomalies else ["- none observed"])
    lines += ["", "## Readiness checklist"]
    for label, ok, detail in checklist:
        lines.append(f"- [{'x' if ok else ' '}] {label} — {detail}")
    lines += ["", "## Recommendation", _recommendation(status["trades"]), ""]
    return "\n".join(lines)


def build_obsidian_note_md(data: DashboardData, state: dict, cfg, now_ms: int) -> str:
    """Same content as the daily report, formatted as an Obsidian note with
    YAML frontmatter properties."""
    status = build_latest_status(data, state, cfg, now_ms)
    date_str = time.strftime("%Y-%m-%d", time.gmtime(now_ms / 1000))
    body = build_daily_report_md(data, state, cfg, now_ms)
    # drop the H1 title line -- Obsidian notes use the filename as title
    body_no_title = "\n".join(body.splitlines()[2:])
    frontmatter = (
        "---\n"
        f"title: Poly Alpha Sniper Daily — {date_str}\n"
        f"date: {date_str}\n"
        "tags: [poly_alpha_sniper, hermes, shadow_live]\n"
        f"mode: {status['mode']}\n"
        f"live_enabled: {str(status['live_enabled']).lower()}\n"
        "source: agent_export (read-only, no secrets)\n"
        "---\n\n"
    )
    return frontmatter + f"# Poly Alpha Sniper Daily — {date_str}\n" + body_no_title


# ---------------------------------------------------------------------------
# Orchestrator: the only function that touches the filesystem
# ---------------------------------------------------------------------------

def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)  # atomic on the same filesystem -- no partial reads


def write_exports(cfg=None, output_dir: Optional[str] = None,
                  db_path: Optional[str] = None, now_ms: Optional[int] = None) -> dict:
    """Read-only export: creates output_dir if missing, writes the 6 files,
    returns a manifest of what was written. Never touches the bot DB/config,
    never reads .env, never places or cancels orders.

    Defense in depth: every payload passes through the same secret-redaction
    filter the structured logger uses (core.logger.redact_obj/redact_text)
    before being written, in case a free-text DB field (e.g. a market title)
    ever contained something secret-shaped."""
    from poly_alpha_sniper.core.logger import redact_obj, redact_text

    cfg = cfg or load_config()
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    out_dir = Path(output_dir or cfg.agent_export.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = DashboardData(db_path or resolve_db_path())
    state = read_runtime_state()

    payloads = {
        "latest_status.json": json.dumps(redact_obj(build_latest_status(data, state, cfg, now_ms)), indent=2, default=str),
        "trade_summary.json": json.dumps(redact_obj(build_trade_summary(data, cfg, now_ms)), indent=2, default=str),
        "reject_breakdown.json": json.dumps(redact_obj(build_reject_breakdown_export(data, cfg, now_ms)), indent=2, default=str),
        "dashboard_snapshot.json": json.dumps(redact_obj(build_dashboard_snapshot(data, state, cfg, now_ms)), indent=2, default=str),
        "daily_report.md": redact_text(build_daily_report_md(data, state, cfg, now_ms)),
        "obsidian_daily_note.md": redact_text(build_obsidian_note_md(data, state, cfg, now_ms)),
    }
    written = []
    obsidian_note_path = None
    for filename, content in payloads.items():
        path = out_dir / filename
        _atomic_write(path, content)
        written.append(str(path))
        if filename == "obsidian_daily_note.md":
            obsidian_note_path = path

    obsidian_result = {"copied": False, "dest": None, "reason": "obsidian.enabled is False"}
    if cfg.obsidian.enabled and obsidian_note_path is not None:
        from poly_alpha_sniper.reporting.obsidian_export import copy_note_to_vault
        obsidian_result = copy_note_to_vault(str(obsidian_note_path), cfg, now_ms)

    return {"output_dir": str(out_dir), "generated_ts_ms": now_ms,
           "files_written": written, "has_data": data.has_data,
           "obsidian": obsidian_result}
