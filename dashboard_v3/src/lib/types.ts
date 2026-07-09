// Mirrors the real payload shapes written by
// D:\claude\poly_alpha_sniper\reporting\agent_export.py — see that file for
// the authoritative field list. Every field here is real, exported data;
// nothing in this app invents a field that isn't in the exporter output.

export interface LatestStatus {
  generated_ts_ms: number;
  mode: string;
  dry_run: boolean;
  live_enabled: boolean;
  heartbeat_age_ms: number | null;
  equity_usd: number;
  trades: number;
  winrate: number;
  profit_factor: number;
  expectancy_usd: number;
  predictions: number;
  signals: number;
  diagnostics_rows: number;
  fresh_books: number | null;
  total_books: number | null;
  last_block_reason: string | null;
  errors: number;
  panic_active: boolean;
  kill_active: boolean;
  cex_freshness_thresholds?: {
    live_signal_max_age_ms: number;
    shadow_eval_max_age_ms: number;
    fail_closed_max_age_ms: number;
  };
}

export interface ExitRow {
  id: number;
  ts_ms: number;
  token_id: string;
  market_id: string;
  reason: string;
  price: number;
  shares: number;
  pnl_usd: number;
  hold_seconds: number;
  detail: string;
}

export interface TradeSummary {
  generated_ts_ms: number;
  trades: number;
  winrate: number;
  profit_factor: number;
  expectancy_usd: number;
  today_pnl_usd: number;
  all_time_pnl_usd: number;
  avg_edge: number;
  fill_quality_avg: number;
  max_drawdown_usd: number;
  max_drawdown_pct: number;
  recent_exits: ExitRow[];
  sample_size_note: string;
}

export const CANONICAL_REJECT_BUCKETS = [
  "no_shock",
  "no_fresh_cex_price",
  "stale_book",
  "spread",
  "edge",
  "max_exposure",
  "min_order",
  "other",
] as const;

export type RejectBucket = (typeof CANONICAL_REJECT_BUCKETS)[number];

export interface MinOrderLatest {
  ts_ms: number;
  asset: string;
  market_title: string;
  tier: string;
  edge: number;
  confidence: number;
  ask_price: number;
  min_shares: number;
  min_required_usd: number;
  configured_max_trade_usd: number;
  shortfall_usd: number;
}

export interface RejectBreakdown {
  generated_ts_ms: number;
  buckets: Record<string, number>;
  raw_by_bucket: Record<string, Record<string, number>>;
  total: number;
  min_order: {
    blocked_count: number;
    latest: MinOrderLatest | null;
    // The currently-configured risk.sizing_mode -- "latest" above reflects
    // whichever mode was active when that historical row was recorded, not
    // necessarily this one. See SignalEnginePanel's MinOrderFormulaBlock.
    sizing_mode?: string;
  };
}

export type HermesBrief =
  | {
      available: true;
      generated_ts_ms: number;
      verdict: string;
      top_blocker: string;
      anomaly: string;
      next_action: string;
      live_readiness_status: string;
    }
  | { available: false; reason: string };

export type LatestMarketState =
  | {
      available: true;
      ts_ms: number;
      asset: string;
      market_title: string;
      direction: string;
      signal_side_price: number;
      fair_probability: number;
      edge: number;
      confidence: number;
      tier: string;
      decision_raw: string;
      decision_label: string;
      reject_reason: string | null;
      cex_selected_source: string | null;
      cex_freshest_age_ms: number | null;
      cex_price: number | null;
      fresh_books: number | null;
      total_books: number | null;
      last_block_reason: string | null;
    }
  | { available: false };

export interface OrderRow {
  id: number;
  ts_ms: number | null;
  order_id: string;
  exchange_order_id: string;
  token_id: string;
  market_id: string;
  side: string;
  price: number;
  size_shares: number;
  size_usd: number;
  state: string;
  filled_shares: number;
  avg_fill_price: number;
  created_ts_ms: number;
  updated_ts_ms: number;
  error: string;
  mode: string;
  tier: string;
  signal_id: string;
  exit_reason: string;
}

export type OracleStatus =
  | { available: false; enabled: boolean; reason: string }
  | {
      available: true;
      enabled: boolean;
      generated_ts_ms: number;
      market_id: string | null;
      asset: string | null;
      price_to_beat: number | null;
      // The bot's settlement anchor is always price_to_beat. anchor_source is
      // where that value came from (polymarket_event_metadata). cex_lead_source
      // is a LEAD indicator only. metadata_url is Polymarket's own declared
      // resolution reference (Chainlink usually, historically Binance for some
      // SOL variants) -- metadata only, NEVER the bot's settlement truth.
      anchor_source: string | null;
      settlement_anchor: string; // literal "price_to_beat"
      cex_lead_source: string | null;
      metadata_url: string | null;
      oracle_source: string | null;
      resolution_source_url: string | null; // back-compat alias of metadata_url
      oracle_open_ts_ms: number | null;
      cex_price: number | null;
      cex_ts_ms: number | null;
      oracle_vs_cex_basis_pct: number | null;
      oracle_anchor_quality: string | null;
      time_remaining_seconds: number | null;
      latest_ev: {
        ev: number;
        probability_of_payout: number;
        executable_price: number;
      } | null;
    };

export interface CandidateBookStatus {
  ts_ms?: number;
  market_id?: string | null;
  asset?: string | null;
  token_id?: string | null;
  side?: string | null;
  status?: "FRESH" | "STALE" | "FETCH_FAILED" | "NOT_REACHED_BOOK_STAGE" | "NOT_EVALUATED";
  earlier_gate_reason?: string | null;
  book_age_ms?: number | null;
  freshness_threshold_ms?: number | null;
  best_bid?: number | null;
  best_ask?: number | null;
  spread?: number | null;
  depth_near_best_usd?: number | null;
  direct_refresh_attempted?: boolean;
  direct_refresh_result?: string | null;
  final_reject_reason?: string | null;
}

export interface GateWaterfall {
  window_minutes: number;
  generated_ts_ms: number;
  stages: Record<string, number>;
  stage_order: string[];
  total_candidates: number;
  accepted: number;
  acceptance_pct: number;
}

export interface LiveFeedAssetState {
  selected_source: string | null;
  source_age_ms: number | null;
  status: "no_data" | "ok" | "degraded" | "warn";
}

export type LiveFeedState = Record<string, LiveFeedAssetState>;

export interface LastScanSnapshot {
  ts_ms?: number;
  asset?: string;
  candidate_market_id?: string | null;
  candidate_market_title?: string | null;
  block_reason?: string | null;
  cex_source?: string | null;
  cex_source_age_ms?: number | null;
  cex_freshness?: "no_data" | "fresh" | "degraded" | "fail_closed";
  anchor_status?: "available" | "missing" | "not_evaluated";
}

export interface NoShockWatchlistEntry {
  ts_ms: number;
  asset: string;
  shock_score: number;
  direction: string;
  time_remaining_s: number | null;
  cex_age_ms: number | null;
  anchor_available: boolean;
}

export interface DashboardSnapshot {
  generated_ts_ms: number;
  latest_status: LatestStatus;
  trade_summary: TradeSummary;
  reject_breakdown: RejectBreakdown;
  hermes_brief: HermesBrief;
  latest_market_state: LatestMarketState;
  oracle_status: OracleStatus;
  live_feed_state: LiveFeedState;
  last_scan_snapshot: LastScanSnapshot;
  no_shock_watchlist: NoShockWatchlistEntry[];
  candidate_book_status: CandidateBookStatus;
  gate_waterfall: GateWaterfall;
  open_positions: unknown[];
  recent_orders: OrderRow[];
  classification_framework: string;
}

/** What GET /api/snapshot returns. `missing: true` on any field means that
 * exporter file was not found on disk — the UI must show "No data yet" for
 * that section, never fabricate a value. */
export interface AutoExportStatus {
  running: boolean;
  pid: number;
  last_started_at: string | null;
  last_finished_at: string | null;
  last_success_at: string | null;
  last_error_at: string | null;
  last_error_message: string;
  interval_seconds: number;
  exports_completed: number;
  consecutive_failures: number;
  lock_active: boolean;
  log_path: string;
  status_path: string;
}

export interface SnapshotResponse {
  fetched_ts_ms: number;
  snapshot: DashboardSnapshot | null;
  latest_status: LatestStatus | null;
  trade_summary: TradeSummary | null;
  reject_breakdown: RejectBreakdown | null;
  auto_export_status: AutoExportStatus | null;
  auto_export_status_missing: boolean;
  missing_files: string[];
  file_ages_ms: Record<string, number | null>;
}
