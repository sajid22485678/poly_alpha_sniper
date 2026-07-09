import { ageSince, formatAgeMs, formatNumber } from "@/lib/format";
import { Card, CardHeader, Label, NotAvailable, Pill } from "@/components/ui";
import type { LatestMarketState, LatestStatus, RejectBreakdown } from "@/lib/types";

const STALE_HEARTBEAT_MS = 60_000; // matches Hero.tsx's hbFresh threshold
const OLD_SNAPSHOT_MS = 30 * 60_000; // informational-only threshold, not an alarm

export function MarketIntelPanel({
  status,
  marketState,
  rejects,
  nowMs,
}: {
  status: LatestStatus | null;
  marketState: LatestMarketState | null;
  rejects: RejectBreakdown | null;
  nowMs: number;
}) {
  const state = marketState?.available ? marketState : null;
  const heartbeatAgeMs = status?.heartbeat_age_ms ?? null;
  const heartbeatStale = heartbeatAgeMs !== null && heartbeatAgeMs > STALE_HEARTBEAT_MS;
  const snapshotAgeMs = state ? ageSince(state.ts_ms, nowMs) : null;
  const snapshotOld = snapshotAgeMs !== null && snapshotAgeMs > OLD_SNAPSHOT_MS;

  return (
    <Card>
      <CardHeader title="Market Intelligence" />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        <Stat label="Fresh Books" value={status ? `${status.fresh_books ?? "—"}/${status.total_books ?? "—"}` : "—"} />
        <Stat label="Stale Book Rejects" value={formatNumber(rejects?.buckets?.stale_book ?? null)} />
        <Stat label="No Fresh CEX Rejects" value={formatNumber(rejects?.buckets?.no_fresh_cex_price ?? null)} />
        <Stat label="Last Block Reason" value={status?.last_block_reason ?? "none"} small />
      </div>

      <div className="v3-divider pt-4">
        <div className="flex items-center justify-between mb-2 flex-wrap gap-2">
          <Label>Last Market-State Snapshot</Label>
          {heartbeatStale ? (
            <Pill tone="red">STALE — heartbeat itself is old, investigate</Pill>
          ) : snapshotOld ? (
            <Pill tone="gold">OLD SNAPSHOT — likely just a quiet market, not a bug</Pill>
          ) : null}
        </div>
        {!state ? (
          <div className="py-4 text-center">
            <NotAvailable>No prediction recorded yet</NotAvailable>
          </div>
        ) : (
          <div className="v3-card-inset grid grid-cols-2 md:grid-cols-5 gap-3">
            <MiniStat label="Asset" value={state.asset} />
            <MiniStat label="Direction" value={state.direction} />
            <MiniStat label="CEX Source" value={state.cex_selected_source ?? "not available"} />
            <MiniStat label="CEX Price" value={state.cex_price != null ? state.cex_price.toString() : "not available"} />
            <MiniStat
              label="Snapshot Age"
              value={formatAgeMs(snapshotAgeMs)}
              valueColor={snapshotOld ? "var(--v3-gold)" : undefined}
            />
          </div>
        )}
        <div className="grid grid-cols-2 gap-3 mt-3">
          <MiniStat
            label="Heartbeat Age (process alive)"
            value={formatAgeMs(heartbeatAgeMs)}
            valueColor={heartbeatStale ? "var(--v3-red)" : "var(--v3-green)"}
          />
          <MiniStat
            label="CEX Source Age (live feed, this asset)"
            value={state ? formatAgeMs(state.cex_freshest_age_ms) : "not available"}
          />
        </div>
        <div className="text-xs mt-3 space-y-1.5" style={{ color: "var(--v3-muted-2)" }}>
          <p>
            <b>Heartbeat age</b>{" "}and <b>snapshot age</b>{" "}measure different things and can
            legitimately diverge by hours. Heartbeat proves the bot&apos;s process loop is
            alive (a separate always-on timer). The snapshot only updates when the bot&apos;s
            pipeline reaches a full prediction — which requires a fresh CEX price <i>and</i>{" "}
            a detected shock. Since <span className="v3-mono">no_shock</span>{" "}and{" "}
            <span className="v3-mono">no_fresh_cex_price</span>{" "}are typically the largest
            reject buckets (see Top Reject Reasons in the Hermes panel below), it is normal
            for the snapshot to be much older than the heartbeat during quiet markets —
            that is a timestamp semantics difference, not a broken feed. The CEX Source Age
            field above is pulled from live runtime diagnostics for the snapshot&apos;s
            asset, so it can be fresh even when the snapshot itself is old.
          </p>
          <p>
            Per-asset BTC/ETH/SOL live rows are not available from the current read-only
            export contract — only the single latest evaluated market is exported. See
            SCREENSHOT_COMPARISON_NOTES.md.
          </p>
        </div>
      </div>
    </Card>
  );
}

function Stat({ label, value, small }: { label: string; value: string; small?: boolean }) {
  return (
    <div>
      <Label>{label}</Label>
      <div className={small ? "text-xs mt-1 v3-mono" : "font-semibold mt-1"} style={small ? { color: "var(--v3-muted)" } : undefined}>
        {value}
      </div>
    </div>
  );
}

function MiniStat({ label, value, valueColor }: { label: string; value: string; valueColor?: string }) {
  return (
    <div>
      <Label>{label}</Label>
      <div className="font-semibold text-sm mt-1" style={valueColor ? { color: valueColor } : undefined}>
        {value}
      </div>
    </div>
  );
}
