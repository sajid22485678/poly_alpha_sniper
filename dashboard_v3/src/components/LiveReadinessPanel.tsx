import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { LiveReadiness, ShadowCompounding } from "@/lib/types";

/** Mission I: strict live-readiness verdict (report only) + Mission H shadow
 * compounding summary. This panel never enables live -- it only reports. */
export function LiveReadinessPanel({
  readiness,
  compounding,
}: {
  readiness: LiveReadiness | null;
  compounding: ShadowCompounding | null;
}) {
  return (
    <Card>
      <CardHeader
        title="Live Readiness / Shadow Compounding"
        right={
          readiness ? (
            <Pill tone={readiness.passed ? "green" : "red"}>{readiness.verdict}</Pill>
          ) : undefined
        }
      />
      {!readiness ? (
        <div className="py-4 text-center"><NotAvailable>no readiness data yet</NotAvailable></div>
      ) : (
        <>
          <div className="text-xs mb-3" style={{ color: "var(--v3-muted-2)" }}>
            {readiness.note} This panel reports only — it never enables live trading.
          </div>
          <div className="space-y-1">
            {readiness.requirements.map((r, i) => (
              <div key={i} className="flex items-center justify-between text-xs">
                <span style={{ color: r.ok ? "var(--v3-green)" : "var(--v3-red)" }}>
                  {r.ok ? "✓" : "✗"} {r.name}
                </span>
                <span className="v3-mono" style={{ color: "var(--v3-muted)" }}>{r.detail}</span>
              </div>
            ))}
          </div>
        </>
      )}

      {compounding && (
        <div className="v3-divider pt-3 mt-3">
          <div className="flex items-center justify-between mb-2">
            <div className="v3-label">Shadow Compounding Simulator</div>
            <Pill tone={compounding.verdict === "COMPUTED" ? "green" : "gold"}>{compounding.verdict}</Pill>
          </div>
          <div className="text-xs mb-2" style={{ color: "var(--v3-muted-2)" }}>
            Simulated only — never touches a real balance or the order path. No Martingale, no
            averaging down, no loss-scaling.
          </div>
          {compounding.verdict === "COMPUTED" ? (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <MiniStat label="Sim Equity" value={`$${compounding.simulated_equity?.toFixed(2) ?? "—"}`} />
              <MiniStat label="Profit Factor" value={compounding.profit_factor ?? "—"} />
              <MiniStat label="Max DD %" value={compounding.max_drawdown_pct ?? "—"} />
              <MiniStat label="Max Loss Streak" value={compounding.max_loss_streak ?? "—"} />
            </div>
          ) : (
            <div className="text-xs v3-mono" style={{ color: "var(--v3-gold)" }}>{compounding.detail}</div>
          )}
        </div>
      )}
    </Card>
  );
}

function MiniStat({ label, value }: { label: string; value: number | string }) {
  return (
    <div>
      <div className="v3-label">{label}</div>
      <div className="font-semibold text-sm mt-1">{value}</div>
    </div>
  );
}
