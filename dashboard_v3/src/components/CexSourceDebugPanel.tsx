import { formatAgeMs } from "@/lib/format";
import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { CexSourceDebug } from "@/lib/types";

const BUCKET_TONE: Record<string, "green" | "gold" | "red" | "neutral"> = {
  FRESH: "green",
  DEGRADED: "gold",
  FAIL_CLOSED: "red",
  NO_SOURCE: "neutral",
};

/** Read-only CEX source / fallback debug. For each asset it shows the selected
 * source vs the freshest ("best") source, every per-source age, the tiered
 * thresholds, and two regression guards: `selected == freshest` (must be true)
 * and `better fallback existed` (must be false). If either flips, source
 * selection has a bug -- this panel makes that visible. Never a gate. */
export function CexSourceDebugPanel({ debug }: { debug: CexSourceDebug | null | undefined }) {
  const assets = debug ? Object.keys(debug) : [];
  return (
    <Card>
      <CardHeader title="CEX Source / Fallback Debug" />
      <div className="text-xs mb-3" style={{ color: "var(--v3-muted-2)" }}>
        freshest valid source is always selected; DEGRADED evaluates with an EV penalty up to the fail-closed ceiling
      </div>
      {assets.length === 0 ? (
        <div className="py-4 text-center">
          <NotAvailable>no CEX source debug yet</NotAvailable>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {assets.map((asset) => {
            const s = debug![asset];
            const bucket = s.freshness_bucket ?? "NO_SOURCE";
            const selectionOk = s.selected_is_freshest !== false && s.better_fallback_existed !== true;
            return (
              <div key={asset} className="v3-card-inset">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-semibold">{asset}</span>
                  <Pill tone={BUCKET_TONE[bucket] ?? "neutral"}>{bucket}</Pill>
                </div>
                <div className="text-xs v3-mono" style={{ color: "var(--v3-muted-2)" }}>
                  selected: {s.selected_source ?? "none"} ({formatAgeMs(s.selected_age_ms ?? null)})
                </div>
                <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
                  freshest: {s.best_source ?? "none"} ({formatAgeMs(s.best_source_age_ms ?? null)})
                </div>
                <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
                  bybit {formatAgeMs(s.bybit_age_ms ?? null)} · okx {formatAgeMs(s.okx_age_ms ?? null)} · binance {formatAgeMs(s.binance_age_ms ?? null)}
                </div>
                <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
                  thresholds: {s.live_threshold_ms ?? "?"}/{s.shadow_eval_threshold_ms ?? "?"}/{s.fail_closed_threshold_ms ?? "?"}ms
                </div>
                <div className="mt-2">
                  <Pill tone={selectionOk ? "green" : "red"}>
                    {selectionOk ? "selection OK (freshest chosen)" : "SELECTION BUG"}
                  </Pill>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
