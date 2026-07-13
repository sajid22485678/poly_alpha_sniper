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
  const heartbeatAge = data?.heartbeat_ts_ms && sampledAtMs !== null
    ? Math.max(0, sampledAtMs - data.heartbeat_ts_ms)
    : null;
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

  return (
    <main>
      <header>
        <div><p className="eyebrow">ISOLATED • DETERMINISTIC • SHADOW ONLY</p><h1>Frequency V4</h1><p className="subtitle">Fee-net economics outrank the 19–36 entries/hour research objective.</p></div>
        <div className={`safety ${safe ? "safe" : "unsafe"}`}>{safe ? "SAFETY LOCKS VERIFIED" : "UNSAFE STATE — FAIL CLOSED"}</div>
      </header>

      <section className="grid hero-grid">
        <Card label="mode" value={data.mode} tone={safe ? "good" : "bad"} />
        <Card label="dry run" value={data.dry_run} tone={data.dry_run ? "good" : "bad"} />
        <Card label="live enabled" value={data.live_enabled} tone={data.live_enabled ? "bad" : "good"} />
        <Card label="real orders possible" value={data.real_orders_possible} tone={data.real_orders_possible ? "bad" : "good"} />
        <Card label="kill switch" value={data.kill_switch_engaged} tone={data.kill_switch_engaged ? "good" : "bad"} />
        <Card label="fixed shares" value={data.fixed_shares} tone={data.fixed_shares === 5 ? "good" : "bad"} />
        <Card label="heartbeat age ms" value={heartbeatAge} tone={heartbeatAge !== null && heartbeatAge < 10000 ? "good" : "warn"} />
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
      </section>

      <section className="panel">
        <h2>Source health</h2>
        <div className="source-grid">{sourceRows(data).map((source, index) => <div className={`source ${source.connected ? "" : "source-bad"}`} key={`${fmt(source.source)}-${fmt(source.channel)}-${index}`}><strong>{fmt(source.source)} / {fmt(source.channel)}</strong><span>{fmt(source.status ?? source.state ?? (source.connected ? "CONNECTED" : "DISCONNECTED"))}</span><small>hydrated {fmt(source.hydrated)} · fresh {fmt(source.freshness_ms ?? source.age_ms ?? source.heartbeat_age_ms)} ms · latency {fmt(sourceLatency(source))} ms</small><small>reconnects {fmt(source.reconnect_count, 0)} · gaps {fmt(source.sequence_gap_count, 0)} · duplicates {fmt(source.duplicate_count, 0)}</small><small>future {fmt(source.future_count, 0)} · regressed {fmt(source.regressed_count, 0)} · REST {fmt(source.rest_recovery_status)}</small>{source.last_error ? <small className="error-text">{fmt(source.last_error)}</small> : null}</div>)}</div>
      </section>

      <section className="grid metrics-grid">
        <Table title="Market universe & capacity" value={data.market_universe} />
        <RollingFrequencyTable value={frequency} />
        <Table title="Execution router" value={execution} />
        <Table title="Exposure" value={data.exposure} />
        <Table title="Fee-net PnL" value={pnl} />
        <Table title="Compound preview (read only)" value={data.compounding_preview} />
        <Table title="No-book taxonomy" value={data.no_book} />
        <Table title="Data-invalid taxonomy" value={data.data_invalid} />
        <Table title="Reject constraints" value={data.reject_reasons} />
        <Table title="Integrity & conflicts" value={integrity} />
        <Table title="300-trade forward gate" value={data.acceptance_gate} />
        <Table title="Performance breakdowns" value={data.breakdowns} />
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
