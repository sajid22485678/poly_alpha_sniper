import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { OpportunityDiagnostics } from "@/lib/types";

/** Missions A/B/C/G: opportunity frequency, top blockers (30/60/120m),
 * near-miss board, and A/A+/B/C tier breakdown. Read-only diagnostics --
 * this panel never triggers a trade. */
export function OpportunityEnginePanel({ diag }: { diag: OpportunityDiagnostics | null }) {
  if (!diag) {
    return (
      <Card>
        <CardHeader title="Opportunity Engine" />
        <div className="py-4 text-center"><NotAvailable>no opportunity data yet</NotAvailable></div>
      </Card>
    );
  }
  const f = diag.opportunity_frequency;
  const board = diag.no_shock_board;
  const windows = diag.summary?.windows ?? {};
  const tiers = diag.tier_breakdown?.by_tier ?? {};

  return (
    <Card>
      <CardHeader
        title="Opportunity Engine"
        right={
          <div className="flex items-center gap-2">
            <Pill tone={diag.apply_to_live ? "red" : "green"}>
              {diag.apply_to_live ? "APPLY_TO_LIVE!" : "SHADOW-ONLY"}
            </Pill>
            <Pill tone={diag.mode_config_safe ? "green" : "red"}>
              {diag.mode_config_safe ? "CONFIG SAFE" : "CONFIG UNSAFE"}
            </Pill>
          </div>
        }
      />
      <div className="text-xs mb-3" style={{ color: "var(--v3-muted-2)" }}>
        Target {diag.target_qualified_opportunities_per_hour} qualified/hr · diagnosis:{" "}
        <span className="v3-mono">{diag.summary?.diagnosis ?? "—"}</span>. Discovery-only — never
        forces or accepts a trade.
      </div>

      {/* Opportunity frequency */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        <Stat label="Accepted / hr" value={f.accepted_shadow_trades_per_hour} tone={f.accepted_shadow_trades_per_hour > 0 ? "green" : "gold"} />
        <Stat label="HOT Near-Miss / hr" value={f.hot_near_miss_per_hour} tone="gold" />
        <Stat label="Near-Miss / hr" value={f.near_miss_per_hour} />
        <Stat label="No-Shock / hr" value={f.no_shock_rejects_per_hour} />
        <Stat label="No-Fresh-CEX / hr" value={f.no_fresh_cex_rejects_per_hour} />
        <Stat label="Avg Shock Score" value={board.avg_shock_score ?? "—"} />
        <Stat label="Max Shock Score" value={board.max_shock_score ?? "—"} />
        <Stat label="Watchlist / hr" value={f.watchlist_per_hour} />
      </div>

      {/* Top blockers 30/60/120 */}
      <div className="v3-divider pt-3 pb-3">
        <div className="v3-label mb-2">Top Blockers (30m / 60m / 120m)</div>
        <div className="grid grid-cols-3 gap-3">
          {["30", "60", "120"].map((w) => {
            const win = windows[w];
            return (
              <div key={w} className="v3-card-inset">
                <div className="text-xs font-semibold mb-1">{w}m · {win?.total_candidates ?? 0} cand · {win?.accepted ?? 0} acc</div>
                {win && Object.entries(win.stages_pct || {})
                  .filter(([k]) => k !== "accepted")
                  .sort((a, b) => b[1] - a[1])
                  .slice(0, 4)
                  .map(([k, pct]) => (
                    <div key={k} className="flex justify-between text-xs v3-mono" style={{ color: "var(--v3-muted)" }}>
                      <span>{k}</span><span>{pct}%</span>
                    </div>
                  ))}
              </div>
            );
          })}
        </div>
      </div>

      {/* Tier breakdown */}
      <div className="v3-divider pt-3 pb-3">
        <div className="v3-label mb-2">Tier Breakdown ({diag.tier_breakdown?.window_minutes ?? 120}m)</div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs v3-mono">
            <thead>
              <tr style={{ color: "var(--v3-muted-2)" }}>
                <th className="text-left">Tier</th><th className="text-right">Cand</th>
                <th className="text-right">Acc</th><th className="text-right">Rej</th>
                <th className="text-right">Avg Edge</th><th className="text-left pl-3">Top Blocker</th>
              </tr>
            </thead>
            <tbody>
              {["A_PLUS", "A", "B", "C"].map((t) => {
                const s = tiers[t];
                return (
                  <tr key={t}>
                    <td>{t}</td>
                    <td className="text-right">{s?.candidates ?? 0}</td>
                    <td className="text-right" style={{ color: (s?.accepted ?? 0) > 0 ? "var(--v3-green)" : undefined }}>{s?.accepted ?? 0}</td>
                    <td className="text-right">{s?.rejected ?? 0}</td>
                    <td className="text-right">{s?.avg_edge ?? "—"}</td>
                    <td className="text-left pl-3 truncate" style={{ color: "var(--v3-muted)" }}>{s?.top_blocker ?? "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Near-miss board */}
      <div className="v3-divider pt-3">
        <div className="v3-label mb-2">Near-Miss Board — tiers {JSON.stringify(board.tier_counts)}</div>
        {board.board.length === 0 ? (
          <NotAvailable>no scored near-misses in window</NotAvailable>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs v3-mono">
              <thead>
                <tr style={{ color: "var(--v3-muted-2)" }}>
                  <th className="text-left">Asset</th><th className="text-left pl-3">Tier</th>
                  <th className="text-right">Score</th><th className="text-right">Dist</th>
                  <th className="text-right">Pctl</th>
                </tr>
              </thead>
              <tbody>
                {board.board.slice(0, 10).map((e, i) => (
                  <tr key={i}>
                    <td>{e.asset}</td>
                    <td className="pl-3" style={{ color: e.tier === "HOT_NEAR_MISS" ? "var(--v3-gold)" : undefined }}>{e.tier}</td>
                    <td className="text-right">{e.shock_score}</td>
                    <td className="text-right">{e.distance_to_threshold}</td>
                    <td className="text-right">{e.percentile}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </Card>
  );
}

function Stat({ label, value, tone }: { label: string; value: number | string; tone?: "green" | "gold" }) {
  const color = tone === "green" ? "var(--v3-green)" : tone === "gold" ? "var(--v3-gold)" : undefined;
  return (
    <div>
      <div className="v3-label">{label}</div>
      <div className="font-semibold mt-1" style={color ? { color } : undefined}>{value}</div>
    </div>
  );
}
