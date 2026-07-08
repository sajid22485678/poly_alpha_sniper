import { CANONICAL_REJECT_BUCKETS } from "@/lib/types";
import { Card, CardHeader, Label, NotAvailable } from "@/components/ui";
import type { RejectBreakdown } from "@/lib/types";

const BUCKET_LABELS: Record<string, string> = {
  no_shock: "No Shock",
  no_fresh_cex_price: "No Fresh CEX Price",
  stale_book: "Stale Book",
  spread: "Spread Too Wide",
  edge: "Edge Too Low",
  max_exposure: "Max Exposure",
  min_order: "Min Order Size",
  other: "Other",
};

export function RejectTaxonomyPanel({ rejects }: { rejects: RejectBreakdown | null }) {
  if (!rejects) {
    return (
      <Card>
        <CardHeader title="Reject Taxonomy" />
        <div className="py-6 text-center">
          <NotAvailable>No data yet</NotAvailable>
        </div>
      </Card>
    );
  }

  const max = Math.max(1, ...CANONICAL_REJECT_BUCKETS.map((b) => rejects.buckets[b] ?? 0));

  return (
    <Card>
      <CardHeader title="Reject Taxonomy" right={<Label>{rejects.total} total</Label>} />
      <div className="space-y-2.5">
        {CANONICAL_REJECT_BUCKETS.map((bucket) => {
          const count = rejects.buckets[bucket] ?? 0;
          const pct = (count / max) * 100;
          return (
            <div key={bucket}>
              <div className="flex justify-between text-xs mb-1">
                <span style={{ color: "var(--v3-muted)" }}>{BUCKET_LABELS[bucket]}</span>
                <span className="v3-mono">{count}</span>
              </div>
              <div className="h-1.5 rounded-full" style={{ background: "var(--v3-card-border)" }}>
                <div
                  className="h-1.5 rounded-full"
                  style={{ width: `${pct}%`, background: bucket === "other" ? "var(--v3-muted-2)" : "var(--v3-gold)" }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
}
