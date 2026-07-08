import { formatAgeMs } from "@/lib/format";
import { Pill } from "@/components/ui";

const STALE_THRESHOLD_MS = 5 * 60 * 1000;

export function StatusBanner({
  generatedTsMs,
  nowMs,
  missingFiles,
  connectionError,
}: {
  generatedTsMs: number | null;
  nowMs: number;
  missingFiles: string[];
  connectionError: boolean;
}) {
  if (connectionError) {
    return (
      <div className="v3-card !border-red-900 flex items-center justify-between mb-3">
        <span style={{ color: "var(--v3-red)" }}>
          Could not reach the local read-only API — is the dashboard_v3 dev server running?
        </span>
      </div>
    );
  }

  if (missingFiles.length > 0) {
    return (
      <div className="v3-card flex items-center justify-between mb-3 flex-wrap gap-2">
        <span style={{ color: "var(--v3-gold)" }}>
          No data yet — waiting for the read-only exporter to write:{" "}
          <span className="v3-mono">{missingFiles.join(", ")}</span>. Run{" "}
          <span className="v3-mono">scripts\export_agent_readonly.bat</span> from the bot project.
        </span>
      </div>
    );
  }

  if (generatedTsMs === null) return null;
  const ageMs = Math.max(0, nowMs - generatedTsMs);
  const stale = ageMs > STALE_THRESHOLD_MS;

  return (
    <div className="flex items-center justify-between mb-3 px-1">
      <Pill tone={stale ? "red" : "green"}>
        {stale ? "STALE EXPORT" : "EXPORT FRESH"} · last generated {formatAgeMs(ageMs)} ago
      </Pill>
      <span className="v3-label">auto-refreshing every 3s</span>
    </div>
  );
}
