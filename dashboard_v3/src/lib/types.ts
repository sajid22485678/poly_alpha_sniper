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
  // blocker of the LATEST scan iteration (live), never a historical signal
  current_blocker?: string | null;
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
      // HISTORICAL last-signal snapshot (a predictions row only exists when a
      // shock once fired) -- can be hours old while the pipeline is healthy.
      // Never the CURRENT blocker; that is LatestStatus.current_blocker.
      snapshot_kind?: string;
      age_ms?: number | null;
      is_stale?: boolean;
      staleness_label?: string | null;
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

export interface OracleAnchorDiagnostics {
  generated_ts_ms?: number;
  market_id?: string | null;
  event_id?: string | null;
  slug?: string | null;
  asset?: string | null;
  hydration_attempted?: boolean;
  hydration_success?: boolean;
  fields_checked?: string[];
  price_to_beat?: number | null;
  metadata_url?: string | null;
  resolution_source_url?: string | null;
  final_anchor_status?: string | null;
}

export type OracleStatus =
  | ({ available: false; enabled: boolean; reason: string } & OracleAnchorDiagnostics)
  | {
      available: true;
      enabled: boolean;
      generated_ts_ms: number;
      market_id: string | null;
      event_id: string | null;
      slug: string | null;
      asset: string | null;
      hydration_attempted: boolean;
      hydration_success: boolean;
      fields_checked: string[];
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
      final_anchor_status: string | null;
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

export interface CexSourceDebugAsset {
  asset?: string;
  selected_source?: string | null;
  selected_age_ms?: number | null;
  selected_status?: string | null;
  best_source?: string | null;
  best_source_age_ms?: number | null;
  bybit_age_ms?: number | null;
  okx_age_ms?: number | null;
  binance_age_ms?: number | null;
  live_threshold_ms?: number;
  shadow_eval_threshold_ms?: number;
  fail_closed_threshold_ms?: number;
  selected_is_freshest?: boolean;
  better_fallback_existed?: boolean;
  freshness_bucket?: "FRESH" | "DEGRADED" | "FAIL_CLOSED" | "NO_SOURCE" | string;
}

export type CexSourceDebug = Record<string, CexSourceDebugAsset>;

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

export interface OpportunityFrequency {
  window_minutes: number;
  qualified_opportunities_per_hour: number;
  accepted_shadow_trades_per_hour: number;
  near_miss_per_hour: number;
  hot_near_miss_per_hour: number;
  watchlist_per_hour: number;
  no_shock_rejects_per_hour: number;
  no_fresh_cex_rejects_per_hour: number;
}

export interface NoShockBoardEntry {
  ts_ms: number;
  asset: string;
  shock_score: number;
  tier: string;
  distance_to_threshold: number;
  percentile: number;
}

export interface NoShockBoard {
  window_minutes: number;
  no_shock_scored: number;
  no_shock_unscored_pre_fix: number;
  avg_shock_score: number | null;
  max_shock_score: number | null;
  tier_counts: Record<string, number>;
  hot_near_miss: number;
  near_miss: number;
  watchlist: number;
  board: NoShockBoardEntry[];
}

export interface TierStat {
  candidates: number;
  accepted: number;
  rejected: number;
  avg_edge: number | null;
  top_blocker: string | null;
}

export interface OpportunityWindow {
  total_candidates: number;
  accepted: number;
  acceptance_pct: number;
  stages: Record<string, number>;
  stages_pct: Record<string, number>;
  dominant_blocker: string;
}

export interface OpportunityDiagnostics {
  generated_ts_ms: number;
  mode_enabled: boolean;
  mode_active: boolean;
  mode_config_safe: boolean;
  apply_to_live: boolean;
  target_qualified_opportunities_per_hour: number;
  summary: { generated_ts_ms: number; windows: Record<string, OpportunityWindow>; diagnosis: string };
  no_shock_board: NoShockBoard;
  opportunity_frequency: OpportunityFrequency;
  tier_breakdown: { window_minutes: number; by_tier: Record<string, TierStat> };
  cex_freshness: { by_asset: Record<string, Record<string, unknown>>; live_behavior_unchanged: boolean };
}

export interface LiveReadiness {
  verdict: string;
  passed: boolean;
  requirements: { name: string; ok: boolean; detail: string }[];
  unmet: string[];
  note: string;
}

export interface ShadowCompounding {
  verdict: string;
  detail?: string;
  standard_shadow_trades: number;
  min_sample: number;
  simulated_equity?: number;
  profit_factor?: number;
  max_drawdown_pct?: number;
  max_loss_streak?: number;
  compounding_safe?: boolean;
  no_martingale: boolean;
  touches_real_balance: boolean;
  touches_order_path: boolean;
}

/** Per-asset current/next window anchor evidence (diagnostics, never a gate). */
export interface AnchorWindowReport {
  asset?: string;
  current_or_next?: string;
  slug?: string | null;
  event_id?: string;
  market_id?: string;
  title?: string;
  seconds_since_open?: number | null;
  seconds_until_close?: number | null;
  hydration_attempted?: boolean;
  hydration_success?: boolean;
  fields_checked?: string[];
  field_presence?: Record<string, boolean | number>;
  final_anchor_available?: boolean;
  final_price_to_beat?: number | null;
  final_anchor_source_path?: string | null;
  final_missing_reason?: string;
  retry_count?: number;
  last_successful_anchor_for_asset?: { price_to_beat: number; ts_ms: number; slug: string } | null;
  last_successful_anchor_age_s?: number | null;
  exact_window_match?: boolean;
  note?: string;
  /** "scan_candidate" when reconciled from the live candidate the Oracle panel shows. */
  source?: string;
}

export interface OracleAnchorAutopsy {
  generated_ts_ms?: number;
  error?: string;
  assets?: Record<string, { current: AnchorWindowReport; next: AnchorWindowReport }>;
  /** ORACLE_EXPORT_MISMATCH entries when panel and autopsy disagreed. */
  warnings?: string[];
}

export interface DbDiagnostics {
  db_size_mb?: number;
  feature_store_rows?: number;
  feature_rows_per_min_10m?: number;
  throttle_ms?: number;
  warning?: string | null;
  error?: string;
}

/** RESEARCH lane only — never mixed into baseline stats or live readiness. */
export interface ResearchChallenger {
  research_only: boolean;
  available?: boolean;
  error?: string;
  generated_ts_ms?: number;
  note?: string;
  scoring_version?: string;
  lane_separation?: { baseline_rows: number; experimental_rows: number; mixed: boolean };
  experimental_zero_reason?: string | null;
  challengers?: Record<string, {
    rows: number;
    would_enter: number;
    top_reject_reasons: Record<string, number>;
    avg_ev: number | null;
    status: string;
  }>;
  baseline_blocker_distribution?: Record<string, number>;
  experimental_blocker_distribution?: Record<string, number>;
  anchor_stats?: {
    available: boolean;
    note?: string;
    rows?: number;
    anchor_available_pct?: number;
    anchor_missing_pct?: number;
    missing_reason_counts?: Record<string, number>;
  };
  per_asset?: Record<string, {
    markov?: { state_now: string; n_observations: number;
               continuation_probability: number; reversal_probability: number } | null;
    regime?: { regime: string; confidence: number; reasons: string[] } | null;
    n_feature_rows?: number;
  }>;
  drift?: {
    available: boolean;
    note?: string;
    n_rows?: number;
    older_half?: { top_blockers: Record<string, number>; anchor_available_rate: number; cex_fresh_rate: number };
    newer_half?: { top_blockers: Record<string, number>; anchor_available_rate: number; cex_fresh_rate: number };
  };
  promotion_status?: string;
}

/** Simulated research probes — never baseline, never live readiness, never real funds. */
export interface ExperimentalProbeTrading {
  enabled: boolean;
  error?: string;
  warning: string;
  probe_rows?: number;
  open_positions?: number;
  completed_trades?: number;
  pending_resolution?: number;
  unresolved_trades?: number;
  resolution_source_breakdown?: { book_exit: number; official_outcome: number; unresolved: number };
  pnl_usd?: number;
  winrate?: number | null;
  profit_factor?: number | null;
  avg_hold_s?: number | null;
  by_strategy?: Record<string, { entries: number; closed: number; pnl_usd: number }>;
  zero_reason?: string | null;
}

/** One simulated Lite trade. A null PnL means the exact market has not been
 * resolved and must never be treated as zero in performance statistics. */
export interface LiteTradeRow {
  id: number | string;
  asset: string;
  market_id?: string;
  event_id?: string;
  slug?: string;
  side: string;
  shares: number;
  entry_price: number;
  entry_cost: number;
  entry_ts: number;
  status: string;
  exit_price?: number | null;
  exit_ts?: number | null;
  pnl: number | null;
  resolution_source?: string | null;
  resolution_reason?: string | null;
  anchor_available?: boolean;
  no_anchor_trade?: boolean;
  cex_source?: string | null;
  cex_entry_price?: number | null;
  momentum_pct?: number | null;
  strategy_name?: string;
}

export interface LiteCexFeedAssetState {
  source?: string | null;
  selected_source?: string | null;
  price?: number | null;
  latest_price?: number | null;
  age_ms?: number | null;
  source_age_ms?: number | null;
  stale?: boolean;
  last_update_ts_ms?: number | null;
  [key: string]: unknown;
}

export interface LiteCurrentMarketState {
  slug?: string | null;
  market_id?: string | null;
  event_id?: string | null;
  condition_id?: string | null;
  window_close_ts?: number | null;
  seconds_to_close?: number | null;
  time_to_close_s?: number | null;
  [key: string]: unknown;
}

export interface LiteDbDiagnostics {
  size_bytes: number;
  writes_per_min: number;
}

/** Fully isolated Lite shadow export. It is never merged into DashboardSnapshot,
 * baseline PnL/KPIs, or live-readiness calculations. */
export interface LiteDashboardSnapshot {
  generated_ts_ms: number;
  mode: string;
  dry_run: boolean;
  live_enabled: boolean;
  warning: string;
  heartbeat_ts_ms: number | null;
  open_positions: number;
  completed_trades: number;
  pending_resolution: number;
  unresolved_final: number;
  total_lite_pnl: number;
  today_lite_pnl: number;
  winrate: number | null;
  profit_factor: number | null;
  expectancy: number | null;
  entries_by_asset: Record<string, number>;
  entries_by_side: Record<string, number>;
  anchor_breakdown: { anchor: number; no_anchor: number };
  resolution_source_breakdown: {
    book_exit: number;
    official_outcome: number;
    unresolved: number;
  };
  top_reject_reasons: Record<string, number>;
  cex_feed_state: Record<string, LiteCexFeedAssetState | null>;
  current_market_by_asset: Record<string, LiteCurrentMarketState | null>;
  last_20_trades: LiteTradeRow[];
  db_diagnostics: LiteDbDiagnostics;
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
  cex_source_debug?: CexSourceDebug;
  last_scan_snapshot: LastScanSnapshot;
  no_shock_watchlist: NoShockWatchlistEntry[];
  candidate_book_status: CandidateBookStatus;
  gate_waterfall: GateWaterfall;
  opportunity_diagnostics?: OpportunityDiagnostics;
  live_readiness?: LiveReadiness;
  research_challenger?: ResearchChallenger;
  oracle_anchor_autopsy?: OracleAnchorAutopsy;
  experimental_probe_trading?: ExperimentalProbeTrading;
  db_diagnostics?: DbDiagnostics;
  shadow_compounding?: ShadowCompounding;
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
  lite_dashboard: LiteDashboardSnapshot | null;
  lite_dashboard_missing: boolean;
  missing_files: string[];
  file_ages_ms: Record<string, number | null>;
}
