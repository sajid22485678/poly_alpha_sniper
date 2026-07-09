import { formatDateTime } from "@/lib/format";
import { topRejectReasons } from "@/lib/derive";
import { Card, CardHeader, Label, NotAvailable, Pill } from "@/components/ui";
import type { HermesBrief, RejectBreakdown } from "@/lib/types";

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

const STEPS = [
  {
    n: "01",
    title: "Trade Executes",
    detail: "Bot enters/exits in shadow_live (dry_run). Every order, fill and exit is logged to SQLite.",
  },
  {
    n: "02",
    title: "Nightly Export",
    detail: "reporting/agent_export.py reads the DB read-only, redacts anything secret-shaped, writes sanitized JSON/MD.",
  },
  {
    n: "03",
    title: "Hermes Advisory Review",
    detail: "Hermes reads the export and writes a verdict/blocker/next-action brief. Advisory only — it never auto-applies strategy or threshold changes.",
    active: true,
  },
];

const PERMISSIONS: Array<[string, boolean]> = [
  ["Can read exported reports", true],
  ["Can read Obsidian vault notes", true],
  ["Can access .env / secrets", false],
  ["Can place orders", false],
  ["Can cancel orders", false],
  ["Can modify strategy or thresholds", false],
];

function readinessTone(status: string): "green" | "gold" | "red" {
  if (status.startsWith("SAMPLE SUFFICIENT")) return "gold";
  if (status === "NOT READY") return "red";
  return "gold";
}

export function HermesPanel({ brief, rejects }: { brief: HermesBrief | null; rejects: RejectBreakdown | null }) {
  const ranked = topRejectReasons(rejects?.buckets, 3);
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
      <Card>
        <CardHeader title="Read-Only Advisory Loop" right={<Pill tone="gold">Advisory Only — No Execution</Pill>} />
        <div className="space-y-3">
          {STEPS.map((s) => (
            <div key={s.n} className={s.active ? "v3-card-inset flex gap-3" : "flex gap-3 px-1"}>
              <div className="v3-mono font-bold" style={{ color: "var(--v3-gold)" }}>{s.n}</div>
              <div>
                <div className="font-semibold text-sm uppercase tracking-wide">{s.title}</div>
                <div className="text-xs mt-0.5" style={{ color: "var(--v3-muted)" }}>{s.detail}</div>
              </div>
            </div>
          ))}
        </div>
      </Card>

      <Card>
        <CardHeader
          title="Hermes Agent"
          right={
            <Pill tone={brief?.available ? "green" : "neutral"}>
              {brief?.available ? "MANUAL BRIEF READY · READ-ONLY" : "NOT AVAILABLE"}
            </Pill>
          }
        />

        {!brief?.available ? (
          <div className="py-4 text-center">
            <NotAvailable>{brief && !brief.available ? brief.reason : "no brief generated yet"}</NotAvailable>
          </div>
        ) : (
          <div className="mb-4">
            <Label>Verdict</Label>
            <div className="text-sm font-semibold mt-1">{brief.verdict}</div>
            <div className="grid grid-cols-2 gap-3 mt-3">
              <div>
                <Label>Top Reject Reasons (raw volume)</Label>
                {ranked.length === 0 ? (
                  <div className="text-sm mt-1">
                    <NotAvailable>no rejects recorded</NotAvailable>
                  </div>
                ) : (
                  <ol className="text-sm mt-1 space-y-0.5">
                    {ranked.map((r, i) => (
                      <li key={r.bucket} className="v3-mono">
                        {i + 1}. {BUCKET_LABELS[r.bucket] ?? r.bucket} — {r.count}
                      </li>
                    ))}
                  </ol>
                )}
                <div className="text-xs mt-1" style={{ color: "var(--v3-muted-2)" }}>
                  Hermes brief blocker (actionable-only, excludes no_shock/no_fresh_cex_price):{" "}
                  {brief.top_blocker}
                </div>
              </div>
              <div>
                <Label>Anomaly</Label>
                <div className="text-sm mt-1">{brief.anomaly}</div>
              </div>
            </div>
            <div className="mt-3">
              <Label>Next Action</Label>
              <div className="text-sm mt-1">{brief.next_action}</div>
            </div>
            <div className="flex items-center justify-between mt-4 v3-divider pt-3">
              <Pill tone={readinessTone(brief.live_readiness_status)}>{brief.live_readiness_status}</Pill>
              <span className="text-xs v3-mono" style={{ color: "var(--v3-muted-2)" }}>
                brief generated {formatDateTime(brief.generated_ts_ms)}
              </span>
            </div>
          </div>
        )}

        <div className="v3-divider pt-3">
          <Label className="mb-2">Permissions</Label>
          <table className="w-full text-sm">
            <tbody>
              {PERMISSIONS.map(([label, allowed]) => (
                <tr key={label}>
                  <td className="py-1 text-xs" style={{ color: "var(--v3-muted)" }}>{label}</td>
                  <td className="py-1 text-right">
                    <Pill tone={allowed ? "green" : "red"}>{allowed ? "YES" : "NO"}</Pill>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
