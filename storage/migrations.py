"""Schema migrations. Idempotent; versioned via schema_migrations table.

Every table the system records into is defined here. Column sets mirror the
contract dataclasses (PredictionRecord, OrderRecord, FillRecord, Position...).
No secrets are ever stored — enforced again at the store layer.
"""
from __future__ import annotations

from poly_alpha_sniper.core.logger import get_logger

log = get_logger("migrations")

_COMMON = "id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER"

TABLES: dict[str, str] = {
    "cex_ticks": f"{_COMMON}, asset TEXT, exchange TEXT, price REAL, bid REAL, ask REAL, recv_ts_ms INTEGER",
    "market_snapshots": f"{_COMMON}, n_markets INTEGER, rejects TEXT",
    "orderbook_snapshots": f"{_COMMON}, token_id TEXT, best_bid REAL, best_ask REAL, spread REAL, "
                           "bid_depth_usd REAL, ask_depth_usd REAL, source TEXT",
    "predictions": f"{_COMMON}, asset TEXT, market_id TEXT, market_title TEXT, direction TEXT, "
                   "cex_price REAL, return_1s REAL, return_2s REAL, return_3s REAL, return_5s REAL, "
                   "return_10s REAL, return_15s REAL, return_30s REAL, volatility REAL, zscore REAL, "
                   "polymarket_price REAL, fair_probability REAL, edge REAL, edge_after_spread REAL, "
                   "edge_after_slippage REAL, confidence REAL, market_quality REAL, trade_quality REAL, "
                   "alpha_score REAL, tier TEXT, aggression_mode TEXT, decision TEXT, reject_reason TEXT, "
                   "mode TEXT, resolved_outcome TEXT",
    "signals": f"{_COMMON}, asset TEXT, market_id TEXT, signal_id TEXT, direction TEXT, kind TEXT, "
               "zscore REAL, impulse REAL, edge REAL, confidence REAL, tier TEXT, reason TEXT",
    "orders": f"{_COMMON}, order_id TEXT, exchange_order_id TEXT, token_id TEXT, market_id TEXT, "
              "side TEXT, price REAL, size_shares REAL, size_usd REAL, state TEXT, filled_shares REAL, "
              "avg_fill_price REAL, created_ts_ms INTEGER, updated_ts_ms INTEGER, error TEXT, mode TEXT, "
              "tier TEXT, signal_id TEXT, exit_reason TEXT",
    "order_lifecycle": f"{_COMMON}, order_id TEXT, from_state TEXT, to_state TEXT, note TEXT",
    "fills": f"{_COMMON}, order_id TEXT, token_id TEXT, market_id TEXT, side TEXT, price REAL, "
             "size_shares REAL, fee_usd REAL, liquidity TEXT",
    "positions": f"{_COMMON}, token_id TEXT, market_id TEXT, outcome TEXT, shares REAL, "
                 "avg_entry_price REAL, realized_pnl REAL, entry_ts_ms INTEGER, tier TEXT, "
                 "entry_signal_id TEXT, exit_plan TEXT, event TEXT",
    "exits": f"{_COMMON}, token_id TEXT, market_id TEXT, reason TEXT, price REAL, shares REAL, "
             "pnl_usd REAL, hold_seconds REAL, detail TEXT",
    "pnl": f"{_COMMON}, realized_pnl_usd REAL, equity_usd REAL, unrealized_pnl_usd REAL",
    "health_logs": f"{_COMMON}, report TEXT",
    "errors": f"{_COMMON}, where_ TEXT, error TEXT",
    "tuning_changes": f"{_COMMON}, param TEXT, old REAL, new REAL, reason TEXT",
    "fill_quality": f"{_COMMON}, order_id TEXT, market_id TEXT, score REAL, slippage_bps REAL, "
                    "delay_ms REAL, partial_ratio REAL",
    "settlement_events": f"{_COMMON}, market_id TEXT, token_id TEXT, outcome TEXT, shares REAL, "
                         "payout_usd REAL, status TEXT",
    "reconciliation_events": f"{_COMMON}, ok INTEGER, mismatches TEXT",
    "panic_events": f"{_COMMON}, trigger TEXT, cleared INTEGER DEFAULT 0",
    "latency_metrics": f"{_COMMON}, stage TEXT, p50 REAL, p95 REAL, p99 REAL, n INTEGER",
    "shadow_live_discrepancy": f"{_COMMON}, signal_id TEXT, shadow_price REAL, live_price REAL, "
                               "size_usd REAL, diff_bps REAL",
    "incident_reports": f"{_COMMON}, kind TEXT, severity TEXT, detail TEXT",
    "rate_limit_usage": f"{_COMMON}, endpoint TEXT, used INTEGER, throttled INTEGER, healthy INTEGER",
    "near_misses": f"{_COMMON}, signal_id TEXT, market_id TEXT, asset TEXT, tier TEXT, score REAL, "
                   "edge REAL, decision TEXT, gap TEXT",
    "market_memory": f"{_COMMON}, market_id TEXT, fills INTEGER, realized_edge REAL, "
                     "fill_quality_avg REAL, blacklist_until_ms INTEGER, notes TEXT",
    "experiment_registry": f"{_COMMON}, name TEXT, config_hash TEXT, params TEXT, metrics TEXT",
    "deterministic_reviews": f"{_COMMON}, subject TEXT, passed TEXT, failed TEXT, fatal INTEGER",
    "balanced_alpha_gate_results": f"{_COMMON}, market_id TEXT, signal_id TEXT, allow_trade INTEGER, "
                                   "tier TEXT, aggression_mode TEXT, decision TEXT, score REAL, "
                                   "hard_reject INTEGER, soft_penalties TEXT, failed_checks TEXT, reason TEXT",
    "market_text_parse_results": f"{_COMMON}, market_id TEXT, ok INTEGER, asset TEXT, market_type TEXT, "
                                 "threshold REAL, confidence REAL, reject_reason TEXT",
    "token_mapping_validation": f"{_COMMON}, market_id TEXT, ok INTEGER, confidence REAL, "
                                "direction_up_means_yes INTEGER, reject_reason TEXT, checks TEXT",
    "signal_sanity_checks": f"{_COMMON}, signal_id TEXT, ok INTEGER, issues TEXT",
    "trade_packet_validations": f"{_COMMON}, signal_id TEXT, ok INTEGER, missing TEXT",
    "telegram_commands": f"{_COMMON}, chat_id TEXT, command TEXT, arg TEXT, result TEXT, confirmed INTEGER",
    "watchdog_events": f"{_COMMON}, event TEXT, detail TEXT, restart_count INTEGER",
    "database_backups": f"{_COMMON}, path TEXT, size_bytes INTEGER",
    "calibration_results": f"{_COMMON}, bucket_lo REAL, bucket_hi REAL, n INTEGER, mean_pred REAL, "
                           "winrate REAL, gap REAL, brier REAL, scope TEXT",
    # shadow-mode observability: WHY the pipeline produced no signal this
    # moment. Monitoring rows only — never used to place orders.
    "shadow_diagnostics": f"{_COMMON}, asset TEXT, market_id TEXT, reason TEXT, detail TEXT, "
                          "ret_2s REAL, zscore REAL, volatility REAL, "
                          "fresh_books INTEGER, total_books INTEGER",
    # WS3 oracle-aware EV engine: one row per market evaluation, so the
    # research modules (lag profiler, replay engine, loss attribution) have
    # real history to work from instead of only the single latest snapshot
    # kept in-memory (core.app.App.diag["latest_oracle_anchor"]).
    "oracle_anchor_log": f"{_COMMON}, market_id TEXT, asset TEXT, window_start_ts_ms INTEGER, "
                         "window_end_ts_ms INTEGER, oracle_source TEXT, price_to_beat REAL, "
                         "oracle_open_ts_ms INTEGER, cex_price REAL, cex_ts_ms INTEGER, "
                         "basis_pct REAL, anchor_quality TEXT, time_remaining_seconds REAL, "
                         "ev REAL, probability_of_payout REAL, executable_price REAL, "
                         "gate_result TEXT",
}

SCHEMA_VERSION = 2


def run_migrations(store) -> None:
    store.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, ts TEXT)")
    for name, cols in TABLES.items():
        store.execute(f"CREATE TABLE IF NOT EXISTS {name} ({cols})")
    rows = store.query("SELECT MAX(version) AS v FROM schema_migrations")
    current = rows[0]["v"] if rows and rows[0]["v"] is not None else 0
    if current < SCHEMA_VERSION:
        store.execute("INSERT INTO schema_migrations (version, ts) VALUES (?, datetime('now'))"
                      if store.__class__.__name__ == "SqliteStore"
                      else "INSERT INTO schema_migrations (version, ts) VALUES (?, now())",
                      (SCHEMA_VERSION,))
    log.info("migrations_complete", extra={"extra": {"tables": len(TABLES), "version": SCHEMA_VERSION}})
