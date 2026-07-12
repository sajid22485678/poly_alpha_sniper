import { Card, CardHeader, Label, NotAvailable, Pill } from "@/components/ui";
import { ageSince, formatAgeMs, formatDateTime, formatNumber, formatPct, formatUsd } from "@/lib/format";
import type {
  LiteCexFeedAssetState,
  LiteCurrentMarketState,
  LiteDashboardSnapshot,
  LiteTradeRow,
} from "@/lib/types";

const LITE_WARNING = "LITE SHADOW ONLY — SEPARATE FROM ADVANCED READINESS — NO REAL ORDERS";
const HEARTBEAT_STALE_MS = 30_000;

export function LiteShadowPanel({
  lite,
  missing,
  nowMs,
  fileAgeMs,
}: {
  lite: LiteDashboardSnapshot | null;
  missing: boolean;
  nowMs: number;
  fileAgeMs: number | null;
}) {
  const heartbeatAgeMs = lite?.heartbeat_ts_ms == null ? null : ageSince(lite.heartbeat_ts_ms, nowMs);
  const heartbeatStale = heartbeatAgeMs !== null && heartbeatAgeMs > HEARTBEAT_STALE_MS;
  const safetyLocked = !!lite && lite.mode === "lite_shadow" && lite.dry_run && !lite.live_enabled;
  const assets = lite
    ? Array.from(new Set([
        ...Object.keys(lite.cex_feed_state ?? {}),
        ...Object.keys(lite.current_market_by_asset ?? {}),
      ])).sort()
    : [];

  return (
    <Card>
      <CardHeader
        title="Poly Alpha Lite V1"
        right={
          <Pill tone={!lite ? "neutral" : safetyLocked ? "green" : "red"}>
            {!lite ? "NO LITE DATA" : safetyLocked ? "ISOLATED SHADOW" : "SAFETY VIOLATION"}
          </Pill>
        }
      />

      <div
        className="v3-card-inset mb-4 text-center text-xs font-semibold"
        style={{ color: lite?.live_enabled ? "var(--v3-red)" : "var(--v3-gold)" }}
      >
        {lite?.warning || LITE_WARNING}
      </div>

      {missing || !lite ? (
        <div className="py-6 text-center">
          <NotAvailable>
            no Lite export yet — the advanced dashboard remains available and unchanged
          </NotAvailable>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
            <Stat label="Mode" value={lite.mode} valueColor={safetyLocked ? "var(--v3-green)" : "var(--v3-red)"} />
            <Stat label="Dry Run" value={String(lite.dry_run)} valueColor={lite.dry_run ? "var(--v3-green)" : "var(--v3-red)"} />
            <Stat label="Live Enabled" value={String(lite.live_enabled)} valueColor={!lite.live_enabled ? "var(--v3-green)" : "var(--v3-red)"} />
            <Stat
              label="Heartbeat"
              value={heartbeatAgeMs === null ? "not available" : `${formatAgeMs(heartbeatAgeMs)} ago`}
              valueColor={heartbeatStale ? "var(--v3-red)" : undefined}
            />
            <Stat label="Export Age" value={fileAgeMs === null ? "not available" : `${formatAgeMs(fileAgeMs)} ago`} />
            <Stat label="Commit" value={lite.current_commit?.slice(0, 8) ?? "unknown"} />
          </div>

          <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mt-3">
            <Stat label="Open Positions" value={String(lite.open_positions)} />
            <Stat label="Completed Trades" value={String(lite.completed_trades)} />
            <Stat label="Verified Completed" value={String(lite.verified_completed_trades ?? 0)} />
            <Stat label="Pending Resolution" value={String(lite.pending_resolution)} />
            <Stat label="Retrying" value={String(lite.unresolved_retrying ?? 0)} />
            <Stat label="Unresolved Final" value={String(lite.unresolved_final)} />
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-7 gap-3 mt-3">
            <Stat
              label="Total Lite PnL"
              value={formatUsd(lite.total_lite_pnl, { signed: true })}
              valueColor={pnlColor(lite.total_lite_pnl)}
            />
            <Stat
              label="Today Lite PnL"
              value={formatUsd(lite.today_lite_pnl, { signed: true })}
              valueColor={pnlColor(lite.today_lite_pnl)}
            />
            <Stat label="Winrate" value={formatPct(lite.winrate, 1)} />
            <Stat label="Profit Factor" value={formatNumber(lite.profit_factor, 2)} />
            <Stat label="Expectancy" value={formatUsd(lite.expectancy, { signed: true })} />
            <Stat
              label="Verified PnL"
              value={formatUsd(lite.verified_realized_pnl ?? 0, { signed: true })}
              valueColor={pnlColor(lite.verified_realized_pnl)}
            />
            <Stat label="Max Drawdown" value={formatUsd(lite.max_drawdown ?? null)} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-6 gap-3 mt-4">
            <Breakdown title="Entries by Asset" values={lite.entries_by_asset} />
            <Breakdown title="Entries by Side" values={lite.entries_by_side} />
            <Breakdown title="Anchor Usage" values={lite.anchor_breakdown} />
            <Breakdown title="Resolution Sources" values={lite.resolution_source_breakdown} />
            <Breakdown title="Entry Decisions (1h)" values={lite.anti_dead_bot_last_hour ?? {}} />
            <Breakdown title="Window Locks" values={lite.asset_window_locks?.by_status ?? {}} />
          </div>

          <div className="v3-divider mt-4 pt-4">
            <Label>Current Market & CEX Feed</Label>
            {assets.length === 0 ? (
              <div className="py-3 text-center"><NotAvailable>no feed or market state yet</NotAvailable></div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-2">
                {assets.map((asset) => (
                  <AssetState
                    key={asset}
                    asset={asset}
                    feed={lite.cex_feed_state?.[asset] ?? null}
                    market={lite.current_market_by_asset?.[asset] ?? null}
                  />
                ))}
              </div>
            )}
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-3 gap-3 mt-4">
            <div className="v3-card-inset xl:col-span-2 overflow-x-auto">
              <div className="flex items-center justify-between mb-3">
                <Label>Last 20 Lite Trades</Label>
                <span className="v3-label">{Math.min(20, lite.last_20_trades.length)} shown</span>
              </div>
              <TradeTable trades={lite.last_20_trades.slice(0, 20)} />
            </div>

            <div className="space-y-3">
              <div className="v3-card-inset">
                <Label>Top Reject Reasons</Label>
                <RejectReasons reasons={lite.top_reject_reasons} />
              </div>
              <div className="v3-card-inset">
                <Label>Lite DB Diagnostics</Label>
                <div className="grid grid-cols-2 gap-3 mt-2">
                  <Stat label="DB Size" value={formatBytes(lite.db_diagnostics.size_bytes)} inset={false} />
                  <Stat label="Writes / Min" value={formatNumber(lite.db_diagnostics.writes_per_min, 1)} inset={false} />
                  <Stat label="Evaluations / Hour" value={formatNumber(lite.candidate_evaluations_last_hour ?? 0, 0)} inset={false} />
                  <Stat label="Events / Min" value={formatNumber(lite.db_diagnostics.bucket_events_per_min ?? 0, 0)} inset={false} />
                </div>
              </div>
              <div className="v3-card-inset">
                <Label>Live-Small Readiness Preview</Label>
                <div className="mt-2">
                  <Detail label="Verdict" value={lite.live_readiness_verdict ?? "not available"} />
                  <Detail label="Real Orders Possible" value={String(lite.real_orders_possible ?? false)} />
                  <Detail label="Both-Side History" value={String(lite.historical_both_side_conflicts ?? 0)} />
                  <Detail label="Active Window Locks" value={String(lite.asset_window_locks?.active ?? 0)} />
                  <Detail label="Exposure / Cap" value={`${formatUsd(lite.live_small_preview?.committed_exposure_usd ?? 0)} / ${formatUsd(lite.live_small_preview?.exposure_cap_usd ?? null)}`} />
                  <Detail label="Available Preview" value={formatUsd(lite.live_small_preview?.available_balance_usd ?? null)} />
                  <Detail label="Last Error" value={lite.last_error ?? "none"} />
                </div>
              </div>
            </div>
          </div>
        </>
      )}
    </Card>
  );
}

function AssetState({
  asset,
  feed,
  market,
}: {
  asset: string;
  feed: LiteCexFeedAssetState | null;
  market: LiteCurrentMarketState | null;
}) {
  const source = feed?.source ?? feed?.selected_source ?? "none";
  const price = feed?.price ?? feed?.latest_price ?? null;
  const feedAgeMs = feed?.age_ms ?? feed?.source_age_ms ?? null;
  const closeS = market?.seconds_to_close ?? market?.time_to_close_s ?? null;

  return (
    <div className="v3-card-inset">
      <div className="flex items-center justify-between mb-2">
        <span className="font-semibold">{asset}</span>
        <Pill tone={feed?.stale ? "red" : feed ? "green" : "neutral"}>
          {feed?.stale ? "STALE" : feed ? "FEED OK" : "NO FEED"}
        </Pill>
      </div>
      <Detail label="CEX" value={source} />
      <Detail label="CEX Price" value={formatNumber(price, 2)} />
      <Detail label="Feed Age" value={formatAgeMs(feedAgeMs)} />
      <Detail label="Market" value={market?.slug ?? "none"} />
      <Detail label="Time to Close" value={closeS == null ? "not available" : `${formatNumber(closeS, 0)}s`} />
      <Detail label="Direction" value={market?.direction?.output ?? "not decided"} />
      <Detail label="Fair YES / NO" value={market?.direction?.fair_probability_yes == null ? "not available" : `${formatPct(market.direction.fair_probability_yes, 1)} / ${formatPct(market.direction.fair_probability_no ?? null, 1)}`} />
      <Detail label="Net Edge YES / NO" value={market?.direction?.net_edge_yes == null ? "not available" : `${formatPct(market.direction.net_edge_yes, 2)} / ${formatPct(market.direction.net_edge_no ?? null, 2)}`} />
      <Detail label="Lead / Lag" value={market?.direction?.lead_lag_status ?? "not observed"} />
      <Detail label="Entry State" value={market?.entry_decision?.action ?? "not decided"} />
      <Detail label="Maker Fill Assumed" value={String(market?.entry_decision?.maker_fill_assumed ?? false)} />
    </div>
  );
}

function TradeTable({ trades }: { trades: LiteTradeRow[] }) {
  if (trades.length === 0) {
    return <div className="py-5 text-center"><NotAvailable>no Lite trades yet</NotAvailable></div>;
  }

  return (
    <table className="w-full text-xs min-w-[760px]">
      <thead>
        <tr className="text-left v3-label">
          <th className="pb-2 pr-3">Time</th>
          <th className="pb-2 pr-3">Asset</th>
          <th className="pb-2 pr-3">Side</th>
          <th className="pb-2 pr-3 text-right">Shares</th>
          <th className="pb-2 pr-3 text-right">Entry</th>
          <th className="pb-2 pr-3 text-right">Cost</th>
          <th className="pb-2 pr-3">Status / Resolution</th>
          <th className="pb-2 text-right">PnL</th>
        </tr>
      </thead>
      <tbody>
        {trades.map((trade, index) => (
          <tr key={`${trade.id}-${index}`} className="border-t" style={{ borderColor: "var(--v3-card-border)" }}>
            <td className="py-2 pr-3 whitespace-nowrap">{formatDateTime(epochMs(trade.entry_ts))}</td>
            <td className="py-2 pr-3 font-semibold">{trade.asset}</td>
            <td className="py-2 pr-3 v3-mono">{trade.side}</td>
            <td className="py-2 pr-3 text-right v3-mono">{formatNumber(trade.shares, 0)}</td>
            <td className="py-2 pr-3 text-right v3-mono">{formatNumber(trade.entry_price, 4)}</td>
            <td className="py-2 pr-3 text-right v3-mono">{formatUsd(trade.entry_cost)}</td>
            <td className="py-2 pr-3">
              <div>{trade.status}</div>
              <div style={{ color: "var(--v3-muted-2)" }}>
                {trade.entry_mode ?? "LEGACY MODE UNKNOWN"} · {trade.execution_verified ? "ENTRY VERIFIED" : "ENTRY UNVERIFIED"}
              </div>
              <div style={{ color: "var(--v3-muted-2)" }}>
                {trade.resolution_source ?? "pending"} · {trade.resolution_verified ? "RESOLUTION VERIFIED" : "RESOLUTION UNVERIFIED"}
              </div>
              <div style={{ color: "var(--v3-muted-2)" }}>
                {trade.model_version ?? "LEGACY MODEL"} · {trade.execution_state ?? "NO EDGE STATE"} · maker fill assumed {String(Boolean(trade.maker_fill_assumed))}
              </div>
            </td>
            <td className="py-2 text-right font-semibold v3-mono" style={{ color: pnlColor(trade.pnl) }}>
              {trade.pnl === null ? "—" : formatUsd(trade.pnl, { signed: true })}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RejectReasons({ reasons }: { reasons: Record<string, number> }) {
  const rows = Object.entries(reasons ?? {}).sort((a, b) => b[1] - a[1]).slice(0, 10);
  if (rows.length === 0) {
    return <div className="py-3 text-center"><NotAvailable>no rejects recorded</NotAvailable></div>;
  }
  const max = Math.max(1, ...rows.map(([, count]) => count));
  return (
    <div className="space-y-2 mt-2">
      {rows.map(([reason, count]) => (
        <div key={reason}>
          <div className="flex justify-between gap-2 text-xs">
            <span>{humanize(reason)}</span>
            <span className="v3-mono">{count}</span>
          </div>
          <div className="h-1 rounded-full mt-1" style={{ background: "var(--v3-card-border)" }}>
            <div className="h-1 rounded-full" style={{ width: `${(count / max) * 100}%`, background: "var(--v3-gold)" }} />
          </div>
        </div>
      ))}
    </div>
  );
}

function Breakdown({ title, values }: { title: string; values: Record<string, number> }) {
  const rows = Object.entries(values ?? {});
  return (
    <div className="v3-card-inset">
      <Label>{title}</Label>
      {rows.length === 0 ? (
        <div className="mt-2"><NotAvailable>no data</NotAvailable></div>
      ) : (
        <div className="space-y-1.5 mt-2">
          {rows.map(([name, count]) => (
            <div key={name} className="flex items-center justify-between gap-2 text-xs">
              <span style={{ color: "var(--v3-muted)" }}>{humanize(name)}</span>
              <span className="v3-mono font-semibold">{count}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-2 text-xs mt-1">
      <span style={{ color: "var(--v3-muted-2)" }}>{label}</span>
      <span className="v3-mono text-right break-all">{value}</span>
    </div>
  );
}

function Stat({
  label,
  value,
  valueColor,
  inset = true,
}: {
  label: string;
  value: string;
  valueColor?: string;
  inset?: boolean;
}) {
  return (
    <div className={inset ? "v3-card-inset text-center" : "text-center"}>
      <div className="font-semibold tabular-nums" style={{ color: valueColor }}>{value}</div>
      <div className="v3-label mt-0.5">{label}</div>
    </div>
  );
}

function pnlColor(value: number | null | undefined): string | undefined {
  if (value === null || value === undefined || value === 0) return undefined;
  return value > 0 ? "var(--v3-green)" : "var(--v3-red)";
}

function humanize(value: string): string {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function epochMs(value: number): number {
  return value < 1_000_000_000_000 ? value * 1000 : value;
}

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) return "not available";
  if (value < 1024) return `${value} B`;
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 ** 2).toFixed(2)} MB`;
}
