import type { HermesBrief, LatestStatus } from "@/lib/types";

function Item({ label, value, tone }: { label: string; value: string; tone?: "gold" | "green" | "red" }) {
  const color = tone === "gold" ? "var(--v3-gold)" : tone === "green" ? "var(--v3-green)" : tone === "red" ? "var(--v3-red)" : "var(--v3-cream)";
  return (
    <span className="whitespace-nowrap">
      <span className="v3-label">{label} </span>
      <span className="v3-mono" style={{ color }}>{value}</span>
    </span>
  );
}

export function SafetyFooter({ status, brief }: { status: LatestStatus | null; brief: HermesBrief | null }) {
  const trades = status?.trades ?? 0;
  const sampleOk = trades >= 30;
  const liveReady = !!brief && brief.available && brief.live_readiness_status.startsWith("SAMPLE SUFFICIENT");

  return (
    <footer className="border-t px-6 py-3" style={{ borderColor: "var(--v3-card-border)" }}>
      <div className="flex items-center gap-6 overflow-x-auto v3-scrollbar">
        <Item label="MODE" value="SHADOW ONLY" tone="gold" />
        <Item label="REAL ORDERS" value="NEVER PLACED" tone="green" />
        <Item label="LIVE TRADING" value={status?.live_enabled ? "ENABLED — ALERT" : "DISABLED"} tone={status?.live_enabled ? "red" : "green"} />
        <Item label="SAMPLE SIZE" value={`${trades} / 30 min`} tone={sampleOk ? "green" : "gold"} />
        <Item label="LIVE READINESS" value={liveReady ? "CHECKLIST NOT YET COMPLETE — MANUAL REVIEW REQUIRED" : "NOT LIVE READY"} tone="red" />
        <Item label="DASHBOARD" value="READ-ONLY" tone="gold" />
      </div>
    </footer>
  );
}
