import { formatAgeMs } from "@/lib/format";
import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { LiveFeedState } from "@/lib/types";

const STATUS_TONE: Record<string, "green" | "gold" | "red" | "neutral"> = {
  ok: "green",
  degraded: "gold",
  warn: "red",
  no_data: "neutral",
};

const STATUS_LABEL: Record<string, string> = {
  ok: "OK",
  degraded: "DEGRADED",
  warn: "WARN",
  no_data: "NO DATA",
};

/** Per-asset live CEX feed state -- selected source, price age, and a
 * dashboard-only severity label (never a pipeline gate). Closes the gap the
 * dashboard used to flag: "only the single latest evaluated market is
 * exported" -- this is BTC/ETH/SOL, every scan, regardless of whether a
 * shock ever fires for any of them. */
export function LiveFeedStatePanel({ state }: { state: LiveFeedState | null }) {
  const assets = state ? Object.keys(state) : [];
  return (
    <Card>
      <CardHeader title="Live Feed State" />
      {assets.length === 0 ? (
        <div className="py-4 text-center">
          <NotAvailable>no live feed data yet</NotAvailable>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {assets.map((asset) => {
            const s = state![asset];
            return (
              <div key={asset} className="v3-card-inset">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-semibold">{asset}</span>
                  <Pill tone={STATUS_TONE[s.status] ?? "neutral"}>
                    {STATUS_LABEL[s.status] ?? s.status.toUpperCase()}
                  </Pill>
                </div>
                <div className="text-xs v3-mono" style={{ color: "var(--v3-muted-2)" }}>
                  source: {s.selected_source ?? "none"}
                </div>
                <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
                  age: {formatAgeMs(s.source_age_ms)}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
