import { formatAgeMs, formatNumber } from "@/lib/format";
import { Card, CardHeader, Label, NotAvailable } from "@/components/ui";
import type { LatestMarketState, LatestStatus, RejectBreakdown } from "@/lib/types";

export function MarketIntelPanel({
  status,
  marketState,
  rejects,
}: {
  status: LatestStatus | null;
  marketState: LatestMarketState | null;
  rejects: RejectBreakdown | null;
}) {
  const state = marketState?.available ? marketState : null;

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
        <Label className="mb-2">Latest Evaluated Market</Label>
        {!state ? (
          <div className="py-4 text-center">
            <NotAvailable>No evaluated market yet</NotAvailable>
          </div>
        ) : (
          <div className="v3-card-inset grid grid-cols-2 md:grid-cols-5 gap-3">
            <MiniStat label="Asset" value={state.asset} />
            <MiniStat label="Direction" value={state.direction} />
            <MiniStat label="CEX Source" value={state.cex_selected_source ?? "not available"} />
            <MiniStat label="Source Age" value={formatAgeMs(state.cex_freshest_age_ms)} />
            <MiniStat label="CEX Price" value={state.cex_price != null ? state.cex_price.toString() : "not available"} />
          </div>
        )}
        <div className="text-xs mt-3" style={{ color: "var(--v3-muted-2)" }}>
          Per-asset BTC/ETH/SOL live rows are not available from the current read-only
          export contract — only the single latest evaluated market is exported. See
          SCREENSHOT_COMPARISON_NOTES.md.
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

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <Label>{label}</Label>
      <div className="font-semibold text-sm mt-1">{value}</div>
    </div>
  );
}
