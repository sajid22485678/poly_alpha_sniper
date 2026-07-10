import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { ExperimentalProbeTrading } from "@/lib/types";

/** EXPERIMENTAL_PROBE_TRADING panel (read-only). Simulated research probes
 * from CEX-vs-anchor direction with a separate simulated bankroll. Never
 * baseline stats, never live readiness, never real funds — the warning pill
 * is part of the contract. UNRESOLVED probes have no PnL (never fabricated). */
export function ExperimentalProbePanel({ probe }: { probe: ExperimentalProbeTrading | null | undefined }) {
  return (
    <Card>
      <CardHeader
        title="Experimental Probe Trading"
        right={<Pill tone="gold">{probe?.warning ?? "EXPERIMENTAL PROBE — NOT BASELINE, NOT LIVE READINESS, NOT REAL FUNDS"}</Pill>}
      />
      {!probe || probe.error ? (
        <div className="py-4 text-center">
          <NotAvailable>{probe?.error ? `probe export error: ${probe.error}` : "no probe data"}</NotAvailable>
        </div>
      ) : probe.zero_reason ? (
        <div className="py-4 text-center">
          <NotAvailable>probe rows = 0: {probe.zero_reason}</NotAvailable>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <Stat label="Open" value={String(probe.open_positions ?? 0)} />
            <Stat label="Completed" value={String(probe.completed_trades ?? 0)} />
            <Stat label="Pending Resolution" value={String(probe.pending_resolution ?? 0)} />
            <Stat label="Unresolved (no PnL)" value={String(probe.unresolved_trades ?? 0)} />
            <Stat label="Probe PnL (simulated)" value={`$${(probe.pnl_usd ?? 0).toFixed(2)}`} />
          </div>
          {probe.resolution_source_breakdown && (
            <div className="text-xs mt-2 v3-mono" style={{ color: "var(--v3-muted-2)" }}>
              resolution sources: official outcome {probe.resolution_source_breakdown.official_outcome} · book exit {probe.resolution_source_breakdown.book_exit} · unresolved {probe.resolution_source_breakdown.unresolved} (unresolved never counts in winrate/PF)
            </div>
          )}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3">
            <Stat label="Winrate" value={probe.winrate !== null && probe.winrate !== undefined ? `${(probe.winrate * 100).toFixed(0)}%` : "—"} />
            <Stat label="Profit Factor" value={probe.profit_factor !== null && probe.profit_factor !== undefined ? String(probe.profit_factor) : "—"} />
            <Stat label="Avg Hold" value={probe.avg_hold_s !== null && probe.avg_hold_s !== undefined ? `${probe.avg_hold_s}s` : "—"} />
            <Stat label="Rows" value={String(probe.probe_rows ?? 0)} />
          </div>
          {probe.by_strategy && Object.keys(probe.by_strategy).length > 0 && (
            <div className="text-xs mt-3 v3-mono" style={{ color: "var(--v3-muted-2)" }}>
              {Object.entries(probe.by_strategy).map(([name, s]) =>
                `${name}: ${s.entries} entries · ${s.closed} closed · $${s.pnl_usd.toFixed(2)}`
              ).join("  |  ")}
            </div>
          )}
        </>
      )}
    </Card>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="v3-card-inset text-center">
      <div className="font-semibold">{value}</div>
      <div className="v3-label mt-0.5">{label}</div>
    </div>
  );
}
