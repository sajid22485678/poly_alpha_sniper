export type Scalar = string | number | boolean | null;

export interface SourceHealth {
  source?: string;
  channel?: string;
  connected?: boolean;
  hydrated?: boolean;
  status?: string;
  state?: string;
  age_ms?: number | null;
  latency_ms?: number | null;
  heartbeat_age_ms?: number | null;
  freshness_ms?: number | null;
  last_provider_ts_ms?: number | null;
  last_receipt_ts_ms?: number | null;
  reconnect_count?: number;
  sequence_gap_count?: number;
  duplicate_count?: number;
  future_count?: number;
  regressed_count?: number;
  rest_recovery_status?: string | null;
  last_error?: string | null;
  [key: string]: unknown;
}

export interface RollingFrequency {
  horizon_hours?: number;
  complete_interval?: boolean;
  observed_hours?: number;
  available_asset_windows?: number;
  eligible_windows?: number;
  positive_edge_windows?: number;
  actual_entries?: number;
  entries_per_hour?: number | null;
  coverage_pct?: number | null;
  missed_opportunities?: number;
  theoretical_max_trades_per_hour?: number;
  bottlenecks?: Record<string, number>;
  [key: string]: unknown;
}

export interface CapitalLedger {
  cohort?: string;
  activation_ts_ms?: number | null;
  activation_commit?: string | null;
  starting_equity_usd?: number;
  realized_net_pnl_usd?: number;
  current_equity_usd?: number;
  open_position_count?: number;
  open_position_cost_usd?: number;
  unresolved_position_count?: number;
  unresolved_capital_usd?: number;
  reserved_order_usd?: number;
  exit_fee_buffer_per_position_usd?: number;
  exit_fee_buffers_usd?: number;
  committed_total_usd?: number;
  available_cash_usd?: number;
  max_exposure_pct?: number;
  max_committed_usd?: number;
  exposure_pct?: number;
  peak_committed_usd?: number;
  peak_exposure_pct?: number;
  invariant_committed_within_equity?: boolean;
  [key: string]: unknown;
}

export interface FrequencyV4Snapshot {
  schema_version?: number;
  generated_ts_ms?: number;
  current_commit?: string;
  strategy_id?: string;
  mode?: string;
  runtime_label?: string;
  cohort?: Record<string, unknown>;
  authoritative_capital?: Record<string, unknown> & { ledger?: CapitalLedger | null };
  legacy_non_authoritative?: Record<string, unknown> | null;
  dry_run?: boolean;
  live_enabled?: boolean;
  real_orders_possible?: boolean;
  live_adapter_present?: boolean;
  kill_switch_engaged?: boolean;
  fixed_shares?: number;
  heartbeat_ts_ms?: number;
  export_age_ms?: number;
  runtime?: Record<string, unknown>;
  effective_config?: Record<string, unknown>;
  persistence?: Record<string, unknown>;
  database?: Record<string, unknown>;
  sources?: SourceHealth[] | Record<string, SourceHealth>;
  market_universe?: Record<string, unknown>;
  frequency?: Record<string, unknown> & { rolling?: Record<string, RollingFrequency> };
  execution?: Record<string, unknown>;
  no_book?: Record<string, number>;
  data_invalid?: Record<string, number>;
  pnl?: Record<string, unknown>;
  compounding_preview?: Record<string, unknown>;
  exposure?: Record<string, unknown>;
  model_contributions?: unknown[] | Record<string, unknown>;
  breakdowns?: Record<string, unknown>;
  integrity?: Record<string, unknown>;
  acceptance_gate?: Record<string, unknown>;
  recent_entries?: unknown[];
  open_positions?: unknown[] | number;
  terminal_trades?: unknown[] | number;
  reject_reasons?: Record<string, number>;
  [key: string]: unknown;
}

export interface SnapshotResponse {
  ok: boolean;
  missing: boolean;
  age_ms: number | null;
  snapshot: FrequencyV4Snapshot | null;
  error?: string;
}
