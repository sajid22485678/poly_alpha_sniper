import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { GateWaterfall } from "@/lib/types";

const STAGE_LABELS: Record<string, string> = {
  no_fresh_cex_price: "No Fresh CEX",
  price_window_not_ready: "Price Window",
  no_shock: "No Shock",
  no_candidate_market: "No Candidate",
  missing_oracle_anchor: "Oracle Anchor",
  book_fetch_failed: "Book Fetch Failed",
  stale_book: "Stale Book",
  edge_too_small: "Edge",
  ev_too_low: "EV",
  spread: "Spread",
  max_exposure: "Max Exposure",
  insufficient_cash: "Cash / Min Order",
  frequency: "Frequency",
  other: "Other",
  accepted: "Accepted",
};

export function GateWaterfallPanel({ waterfall }: { waterfall: GateWaterfall | null }) {
  const w = waterfall && waterfall.total_candidates > 0 ? waterfall : null;
  const max = w ? Math.max(...w.stage_order.map((s) => w.stages[s] ?? 0), 1) : 1;

  return (
    <Card>
      <CardHeader
        title="Entry Gate Waterfall"
        right={
          w ? (
            <Pill tone={w.accepted > 0 ? "green" : "gold"}>
              {w.accepted}/{w.total_candidates} accepted ({w.acceptance_pct}%)
            </Pill>
          ) : undefined
        }
      />
      {!w ? (
        <div className="py-4 text-center">
          <NotAvailable>no candidates in the recent window</NotAvailable>
        </div>
      ) : (
        <div className="space-y-1.5">
          {w.stage_order.map((stage) => {
            const count = w.stages[stage] ?? 0;
            const pct = (count / max) * 100;
            const isAccepted = stage === "accepted";
            return (
              <div key={stage} className="flex items-center gap-3">
                <div className="w-40 text-xs shrink-0" style={{ color: "var(--v3-muted)" }}>
                  {STAGE_LABELS[stage] ?? stage}
                </div>
                <div className="flex-1 h-4 rounded overflow-hidden" style={{ background: "var(--v3-card-inset, #1a1a1a)" }}>
                  <div
                    className="h-full rounded"
                    style={{
                      width: `${pct}%`,
                      background: isAccepted ? "var(--v3-green)" : "var(--v3-gold)",
                      opacity: count === 0 ? 0.15 : 0.85,
                    }}
                  />
                </div>
                <div className="w-12 text-right text-xs v3-mono shrink-0">{count}</div>
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
