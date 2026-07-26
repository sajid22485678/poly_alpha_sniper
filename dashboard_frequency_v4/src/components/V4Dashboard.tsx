"use client";

import { useEffect, useState } from "react";

import type { FrequencyV4Snapshot, SnapshotResponse } from "@/lib/types";

const EMPTY: SnapshotResponse = { ok: false, missing: true, age_ms: null, snapshot: null };

function fmt(value: unknown, digits = 2): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "number") return Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: digits }) : "INVALID";
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

function entries(value: unknown): Array<[string, unknown]> {
  return value && typeof value === "object" && !Array.isArray(value) ? Object.entries(value as Record<string, unknown>) : [];
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function sourceLatency(source: Record<string, unknown>): number | null {
  if (typeof source.latency_ms === "number") return source.latency_ms;
  if (typeof source.last_provider_ts_ms === "number" && typeof source.last_receipt_ts_ms === "number") {
    return Math.max(0, source.last_receipt_ts_ms - source.last_provider_ts_ms);
  }
  return null;
}

function Card({ label, value, tone = "neutral" }: { label: string; value: unknown; tone?: "neutral" | "good" | "bad" | "warn" }) {
  return <div className={`card ${tone}`}><span>{label}</span><strong>{fmt(value)}</strong></div>;
}

function Table({ title, value }: { title: string; value: unknown }) {
  const rows = entries(value);
  return (
    <section className="panel">
      <h2>{title}</h2>
      {rows.length ? <dl>{rows.map(([key, item]) => <div key={key}><dt>{key.replaceAll("_", " ")}</dt><dd>{typeof item === "object" && item !== null ? <code>{JSON.stringify(item)}</code> : fmt(item)}</dd></div>)}</dl> : <p className="muted">No observations yet.</p>}
    </section>
  );
}

function JsonPanel({ title, value }: { title: string; value: unknown }) {
  const empty = value === null || value === undefined || (Array.isArray(value) && value.length === 0);
  return <section className="panel"><h2>{title}</h2>{empty ? <p className="muted">No observations yet.</p> : <pre>{JSON.stringify(value, null, 2)}</pre>}</section>;
}

function RollingFrequencyTable({ value }: { value: unknown }) {
  const rows = entries(value);
  return (
    <section className="panel span-all">
      <h2>Rolling frequency — observed, never extrapolated</h2>
      <div className="table-wrap"><table><thead><tr><th>Interval</th><th>Observed h</th><th>Entries</th><th>Entries/h</th><th>Capacity/h</th><th>Coverage</th><th>Positive edge</th><th>Status</th></tr></thead><tbody>
        {rows.map(([key, raw]) => {
          const row = record(raw);
          return <tr key={key}><td>{key}</td><td>{fmt(row.observed_hours)}</td><td>{fmt(row.actual_entries, 0)}</td><td>{fmt(row.entries_per_hour)}</td><td>{fmt(row.theoretical_max_trades_per_hour, 0)}</td><td>{fmt(row.coverage_pct)}%</td><td>{fmt(row.positive_edge_windows, 0)}</td><td>{fmt(row.rate_status)}</td></tr>;
        })}
      </tbody></table></div>
    </section>
  );
}

function sourceRows(snapshot: FrequencyV4Snapshot): Array<Record<string, unknown>> {
  if (Array.isArray(snapshot.sources)) return snapshot.sources as Array<Record<string, unknown>>;
  return entries(snapshot.sources).map(([source, value]) => ({ source, ...(value as Record<string, unknown>) }));
}

export default function V4Dashboard() {
  const [payload, setPayload] = useState<SnapshotResponse>(EMPTY);
  const [requestError, setRequestError] = useState("");
  const [sampledAtMs, setSampledAtMs] = useState<number | null>(null);

  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const response = await fetch("/api/snapshot", { cache: "no-store" });
        const body = (await response.json()) as SnapshotResponse;
        if (active) {
          setPayload(body);
          setSampledAtMs(Date.now());
          setRequestError("");
        }
      } catch {
        if (active) setRequestError("snapshot_request_failed");
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 1000);
    return () => { active = false; window.clearInterval(timer); };
  }, []);

  const data = payload.snapshot;
  // Runtime heartbeat age is read from the export payload (computed at export
  // generation from the freshest writer/runtime-health signal).  This is
  // intentionally independent of export freshness: a stale export must turn the
  // export-age card red, not make a live runtime heartbeat look stale.  The
  // total observed heartbeat age adds the export's own staleness since the file
  // was written, so a very stale file still surfaces honestly.
  const heartbeatAge = typeof data?.runtime_heartbeat_age_ms === "number"
    ? data.runtime_heartbeat_age_ms + (payload.age_ms ?? 0)
    : (data?.heartbeat_ts_ms && sampledAtMs !== null
      ? Math.max(0, sampledAtMs - data.heartbeat_ts_ms)
      : null);
  if (!data) {
    return <main><header><p className="eyebrow">ISOLATED RESEARCH LANE</p><h1>Poly Alpha Frequency V4</h1></header><section className="panel danger"><h2>Telemetry unavailable</h2><p>{requestError || payload.error || "Waiting for the first V4 export."}</p></section></main>;
  }

  const safe = data.mode === "lite_frequency_v4_shadow" && data.dry_run === true && data.live_enabled === false && data.real_orders_possible === false && data.live_adapter_present === false && data.kill_switch_engaged === true && data.fixed_shares === 5;
  const frequency = data.frequency ?? {};
  const execution = data.execution ?? {};
  const pnl = data.pnl ?? {};
  const integrity = data.integrity ?? {};
  const sessionFrequency = record(frequency.session);
  const executionSummary = record(execution);
  const pnlSummary = record(pnl);
  const integritySummary = record(integrity);
  const exposureSummary = record(data.exposure);
  const cohortInfo = record(data.cohort);
  const capital = record(data.authoritative_capital);
  const ledger = record(capital.ledger);
  const universe = record(data.market_universe);
  const ledgerAvailable = capital.ledger_available === true;
  const invariantOk = ledger.invariant_committed_within_equity === true;
  const persistenceSummary = record(data.persistence);
  const criticalWriter = record(persistenceSummary.critical);
  const telemetryWriter = record(persistenceSummary.telemetry);
  const checkpoint = record(persistenceSummary.latest_checkpoint);
  const persistenceReady = persistenceSummary.operational_ready === true;
  const criticalReady = persistenceSummary.critical_execution_ready === true;

  return (
    <main>
      <header>
        <div><p className="eyebrow">ISOLATED • DETERMINISTIC • SHADOW ONLY</p><h1>Frequency V4</h1><p className="subtitle">{fmt(data.runtime_label)}</p><p className="subtitle">Fee-net economics outrank the 19–36 entries/hour research objective.</p></div>
        <div><div className={`safety ${safe ? "safe" : "unsafe"}`}>{safe ? "SAFETY LOCKS VERIFIED" : "UNSAFE STATE — FAIL CLOSED"}</div><div className={`safety ${criticalReady ? "safe" : "unsafe"}`}>{criticalReady ? "CRITICAL EXECUTION READY" : "CRITICAL EXECUTION BLOCKED"}</div><div className={`safety ${persistenceReady ? "safe" : "unsafe"}`}>{persistenceReady ? "PERSISTENCE HEALTHY" : "PERSISTENCE DEGRADED — REVIEW REQUIRED"}</div><div className={`safety ${ledgerAvailable && invariantOk ? "safe" : "unsafe"}`}>{ledgerAvailable && invariantOk ? "LEDGER INVARIANT HELD" : "LEDGER UNAVAILABLE OR VIOLATED"}</div></div>
      </header>

      <section className="grid hero-grid">
        <Card label="cohort" value={cohortInfo.authoritative} tone="good" />
        <Card label="cohort activation" value={cohortInfo.activation_ts_ms ? new Date(Number(cohortInfo.activation_ts_ms)).toISOString() : null} />
        <Card label="starting equity USD" value={ledger.starting_equity_usd} tone={typeof ledger.starting_equity_usd === "number" && ledger.starting_equity_usd > 0 ? "good" : "bad"} />
        <Card label="current equity USD" value={ledger.current_equity_usd} />
        <Card label="available cash USD" value={ledger.available_cash_usd} tone={Number(ledger.available_cash_usd ?? 0) >= 0 ? "good" : "bad"} />
        <Card label="committed USD" value={ledger.committed_total_usd} />
        <Card label="exposure %" value={ledger.exposure_pct !== undefined ? Number(ledger.exposure_pct) * 100 : null} />
        <Card label="max exposure %" value={ledger.max_exposure_pct !== undefined ? Number(ledger.max_exposure_pct) * 100 : null} tone={ledger.max_exposure_pct === 1 ? "good" : "warn"} />
        <Card label="peak exposure %" value={ledger.peak_exposure_pct !== undefined ? Number(ledger.peak_exposure_pct) * 100 : null} />
        <Card label="open position cost USD" value={ledger.open_position_cost_usd} />
        <Card label="reserved USD" value={ledger.reserved_order_usd} />
        <Card label="unresolved USD" value={ledger.unresolved_capital_usd} />
        <Card label="exit fee buffers USD" value={ledger.exit_fee_buffers_usd} />
        <Card label="realized net PnL USD" value={ledger.realized_net_pnl_usd} tone={Number(ledger.realized_net_pnl_usd ?? 0) >= 0 ? "good" : "warn"} />
        <Card label="insufficient-capital rejects" value={capital.insufficient_capital_rejects} />
        <Card label="eligible markets" value={universe.eligible_market_count_active} />
        <Card label="observed-only markets" value={universe.observed_only_market_count_active} />
        <Card label="dynamic universe" value={universe.dynamic_universe_enabled} tone={universe.dynamic_universe_enabled === true ? "good" : "bad"} />
        <Card label="fail-closed execution" value={universe.execution_fail_closed} tone={universe.execution_fail_closed === true ? "good" : "bad"} />
        <Card label="hardcoded BTC/ETH/SOL only" value={universe.hardcoded_to_required_assets_only} tone={universe.hardcoded_to_required_assets_only === false ? "good" : "bad"} />
      </section>

      <section className="grid hero-grid">
        <Card label="mode" value={data.mode} tone={safe ? "good" : "bad"} />
        <Card label="dry run" value={data.dry_run} tone={data.dry_run ? "good" : "bad"} />
        <Card label="live enabled" value={data.live_enabled} tone={data.live_enabled ? "bad" : "good"} />
        <Card label="real orders possible" value={data.real_orders_possible} tone={data.real_orders_possible ? "bad" : "good"} />
        <Card label="kill switch" value={data.kill_switch_engaged} tone={data.kill_switch_engaged ? "good" : "bad"} />
        <Card label="fixed shares" value={data.fixed_shares} tone={data.fixed_shares === 5 ? "good" : "bad"} />
        <Card label="runtime heartbeat age ms" value={heartbeatAge} tone={heartbeatAge !== null && heartbeatAge < 15000 ? "good" : "warn"} />
        <Card label="export age ms" value={payload.age_ms} tone={payload.age_ms !== null && payload.age_ms < 10000 ? "good" : "warn"} />
        <Card label="capacity / hour" value={sessionFrequency.theoretical_max_trades_per_hour} />
        <Card label="actual entries" value={sessionFrequency.actual_entries} />
        <Card label="observed entries / hour" value={sessionFrequency.entries_per_hour} />
        <Card label="window coverage %" value={sessionFrequency.coverage_pct} />
        <Card label="positive-edge windows" value={sessionFrequency.positive_edge_windows} />
        <Card label="maker → cross" value={executionSummary.maker_to_cross_count} />
        <Card label="open positions" value={exposureSummary.open_count} />
        <Card label="verified terminal" value={pnlSummary.count} />
        <Card label="conflicts / duplicates" value={`${fmt(integritySummary.conflicts, 0)} / ${fmt(integritySummary.duplicates, 0)}`} tone={integritySummary.conflicts === 0 && integritySummary.duplicates === 0 ? "good" : "bad"} />
        <Card label="unresolved final" value={integritySummary.unresolved_final} tone={integritySummary.unresolved_final === 0 ? "good" : "bad"} />
        <Card label="assumed maker fills" value={integritySummary.maker_fill_assumed_count} tone={integritySummary.maker_fill_assumed_count === 0 ? "good" : "bad"} />
        <Card label="critical writer" value={criticalWriter.state} tone={criticalWriter.state === "HEALTHY" ? "good" : "bad"} />
        <Card label="critical queue" value={`${fmt(criticalWriter.queue_depth, 0)} / ${fmt(criticalWriter.queue_capacity, 0)}`} tone={Number(criticalWriter.queue_depth ?? 0) === 0 ? "good" : "warn"} />
        <Card label="critical p95 ms" value={criticalWriter.commit_latency_p95_ms} />
        <Card label="telemetry queue" value={`${fmt(telemetryWriter.queue_depth, 0)} / ${fmt(telemetryWriter.queue_capacity, 0)}`} tone={Number(telemetryWriter.rows_dropped ?? 0) === 0 ? "good" : "warn"} />
        <Card label="telemetry coalesced" value={telemetryWriter.rows_coalesced} />
        <Card label="telemetry dropped" value={telemetryWriter.rows_dropped} tone={Number(telemetryWriter.rows_dropped ?? 0) === 0 ? "good" : "warn"} />
        <Card label="telemetry batch failures" value={telemetryWriter.failed_batches} tone={Number(telemetryWriter.failed_batches ?? 0) === 0 ? "good" : "warn"} />
        <Card label="critical evidence incomplete" value={telemetryWriter.critical_evidence_incomplete_count} tone={Number(telemetryWriter.critical_evidence_incomplete_count ?? 0) === 0 ? "good" : "bad"} />
        <Card label="WAL bytes" value={record(data.database).wal_bytes} />
        <Card label="checkpoint status" value={checkpoint.status ?? checkpoint.mode} tone={checkpoint.successful === false ? "warn" : "neutral"} />
        <Card label="checkpoint busy" value={checkpoint.busy_result} tone={Number(checkpoint.busy_result ?? 0) === 0 ? "good" : "warn"} />
      </section>

      <section className="panel">
        <h2>Source health</h2>
        <div className="source-grid">{sourceRows(data).map((source, index) => <div className={`source ${source.connected ? "" : "source-bad"}`} key={`${fmt(source.source)}-${fmt(source.channel)}-${index}`}><strong>{fmt(source.source)} / {fmt(source.channel)}</strong><span>{fmt(source.status ?? source.state ?? (source.connected ? "CONNECTED" : "DISCONNECTED"))}</span><small>hydrated {fmt(source.hydrated)} · fresh {fmt(source.freshness_ms ?? source.age_ms ?? source.heartbeat_age_ms)} ms · latency {fmt(sourceLatency(source))} ms</small><small>reconnects {fmt(source.reconnect_count, 0)} · gaps {fmt(source.sequence_gap_count, 0)} · duplicates {fmt(source.duplicate_count, 0)}</small><small>future {fmt(source.future_count, 0)} · regressed {fmt(source.regressed_count, 0)} · REST {fmt(source.rest_recovery_status)}</small>{source.last_error ? <small className="error-text">{fmt(source.last_error)}</small> : null}</div>)}</div>
      </section>

      <section className="grid metrics-grid">
        <Table title={`Authoritative capital ledger (${fmt(cohortInfo.authoritative)} · $${fmt(ledger.starting_equity_usd, 0)} cohort)`} value={capital} />
        <Table title="Dynamic universe & capacity" value={data.market_universe} />
        <RollingFrequencyTable value={frequency} />
        <Table title="Execution router" value={execution} />
        <Table title="Exposure" value={data.exposure} />
        <Table title="Fee-net PnL (authoritative cohort only)" value={pnl} />
        <Table title="Compound preview (READ_ONLY_THEORETICAL_PREVIEW_DOES_NOT_INFLUENCE_EXECUTION)" value={data.compounding_preview} />
        <Table title="No-book taxonomy" value={data.no_book} />
        <Table title="Data-invalid taxonomy" value={data.data_invalid} />
        <Table title="Reject constraints" value={data.reject_reasons} />
        <Table title="Integrity & conflicts" value={integrity} />
        <Table title="Persistence architecture" value={data.persistence} />
        <Table title="Effective persistence config" value={data.effective_config} />
        <Table title="Database / WAL" value={data.database} />
        <Table title="300-trade forward gate (authoritative cohort only)" value={data.acceptance_gate} />
        <Table title="Performance breakdowns (authoritative cohort only)" value={data.breakdowns} />
        <Table title="Legacy performance (NON-AUTHORITATIVE, mixed universe)" value={data.legacy_non_authoritative} />
      </section>

      <section className="panel"><h2>Model contribution breakdown</h2><pre>{JSON.stringify(data.model_contributions ?? {}, null, 2)}</pre></section>
      <section className="grid evidence-grid">
        <JsonPanel title="Open shadow positions" value={data.open_positions} />
        <JsonPanel title="Recent five-share entries" value={data.recent_entries} />
        <JsonPanel title="Recent terminal trades" value={data.terminal_trades} />
      </section>
      <footer><span>commit {fmt(data.current_commit)}</span><span>generated {data.generated_ts_ms ? new Date(data.generated_ts_ms).toISOString() : "—"}</span><span>v4 statistics never include v3 or Advanced</span></footer>
    </main>
  );
}
