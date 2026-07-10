import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { ResearchChallenger } from "@/lib/types";

/** RESEARCH lane panel (read-only). Everything here is diagnostics from the
 * feature store run through the shadow-only research modules (Markov states,
 * regime tagger, drift). It is NEVER an entry signal, never part of baseline
 * shadow stats, and never an input to live readiness — the promotion status
 * line makes that explicit. */
export function AdvancedQuantPanel({ research }: { research: ResearchChallenger | null | undefined }) {
  const assets = research?.per_asset ? Object.keys(research.per_asset) : [];
  return (
    <Card>
      <CardHeader
        title="Advanced Quant (Research lane)"
        right={<Pill tone="gold">RESEARCH ONLY — not baseline, not live readiness</Pill>}
      />
      {!research || research.available === false || assets.length === 0 ? (
        <div className="py-4 text-center">
          <NotAvailable>
            {research?.error ? `research layer unavailable: ${research.error}` : "no feature-store rows yet (restart bot to start recording)"}
          </NotAvailable>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {assets.map((asset) => {
              const a = research.per_asset![asset];
              return (
                <div key={asset} className="v3-card-inset">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-semibold">{asset}</span>
                    <Pill tone="neutral">{a.markov?.state_now ?? "no state"}</Pill>
                  </div>
                  <div className="text-xs v3-mono" style={{ color: "var(--v3-muted-2)" }}>
                    regime: {a.regime?.regime ?? "n/a"} ({((a.regime?.confidence ?? 0) * 100).toFixed(0)}%)
                  </div>
                  <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
                    continuation {((a.markov?.continuation_probability ?? 0) * 100).toFixed(0)}% · reversal {((a.markov?.reversal_probability ?? 0) * 100).toFixed(0)}%
                  </div>
                  <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
                    {a.n_feature_rows ?? 0} feature rows · {a.markov?.n_observations ?? 0} transitions
                  </div>
                </div>
              );
            })}
          </div>
          <div className="text-xs mt-3 v3-mono" style={{ color: "var(--v3-muted-2)" }}>
            lanes: baseline {research.lane_separation?.baseline_rows ?? 0} rows · experimental {research.lane_separation?.experimental_rows ?? 0} rows · mixed: {String(research.lane_separation?.mixed ?? false)}
          </div>
          {research.experimental_zero_reason ? (
            <div className="text-xs mt-1" style={{ color: "var(--v3-gold)" }}>
              experimental rows = 0: {research.experimental_zero_reason}
            </div>
          ) : null}
          {research.challengers && Object.keys(research.challengers).length > 0 && (
            <div className="mt-3 overflow-x-auto">
              <table className="w-full text-xs v3-mono">
                <thead>
                  <tr style={{ color: "var(--v3-muted-2)" }}>
                    <th className="text-left py-1">challenger</th>
                    <th className="text-right">rows</th>
                    <th className="text-right">would enter</th>
                    <th className="text-right">avg EV</th>
                    <th className="text-left pl-3">top reject</th>
                    <th className="text-left pl-3">status</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(research.challengers).map(([name, c]) => (
                    <tr key={name} className="border-t" style={{ borderColor: "var(--v3-border)" }}>
                      <td className="py-1">{name}</td>
                      <td className="text-right">{c.rows}</td>
                      <td className="text-right" style={{ color: c.would_enter > 0 ? "var(--v3-green)" : "var(--v3-muted-2)" }}>
                        {c.would_enter}
                      </td>
                      <td className="text-right">{c.avg_ev ?? "—"}</td>
                      <td className="pl-3">{Object.keys(c.top_reject_reasons)[0] ?? "—"}</td>
                      <td className="pl-3">{c.status}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {research.drift?.available && (
            <div className="text-xs mt-1 v3-mono" style={{ color: "var(--v3-muted-2)" }}>
              drift (old→new half): anchor {((research.drift.older_half?.anchor_available_rate ?? 0) * 100).toFixed(0)}%→{((research.drift.newer_half?.anchor_available_rate ?? 0) * 100).toFixed(0)}% · cex fresh {((research.drift.older_half?.cex_fresh_rate ?? 0) * 100).toFixed(0)}%→{((research.drift.newer_half?.cex_fresh_rate ?? 0) * 100).toFixed(0)}%
            </div>
          )}
          <div className="mt-3">
            <Pill tone="neutral">{research.promotion_status ?? "NOT_PROMOTED"}</Pill>
          </div>
        </>
      )}
    </Card>
  );
}
