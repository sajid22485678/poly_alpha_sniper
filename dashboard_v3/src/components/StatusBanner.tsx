import { formatAgeMs } from "@/lib/format";
import { Pill } from "@/components/ui";
import type { AutoExportStatus } from "@/lib/types";

const STALE_THRESHOLD_MS = 5 * 60 * 1000;
// Grace window past the exporter's own interval before we call its
// last_success_at "not fresh" -- avoids false positives from ordinary poll
// timing jitter between the loop and the dashboard's own 3s refresh.
const EXPORTER_SUCCESS_GRACE_MS = 30 * 1000;

export function StatusBanner({
  generatedTsMs,
  nowMs,
  missingFiles,
  connectionError,
  autoExportStatus,
  autoExportStatusMissing,
}: {
  generatedTsMs: number | null;
  nowMs: number;
  missingFiles: string[];
  connectionError: boolean;
  autoExportStatus?: AutoExportStatus | null;
  autoExportStatusMissing?: boolean;
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
          <span className="v3-mono">Update Poly Obsidian Report.bat</span> or start the
          auto-exporter to refresh exported data.
        </span>
      </div>
    );
  }

  if (generatedTsMs === null) return null;
  const ageMs = Math.max(0, nowMs - generatedTsMs);
  const stale = ageMs > STALE_THRESHOLD_MS;

  // Exporter says it's running and succeeded recently, yet the dashboard's
  // own data files are stale -- that combination means the loop is alive
  // but writing somewhere the dashboard isn't reading from, not that the
  // loop is simply stopped.
  const exporterLastSuccessAgeMs = autoExportStatus?.last_success_at
    ? nowMs - new Date(autoExportStatus.last_success_at).getTime()
    : null;
  const exporterRunningButDataStale =
    stale && !autoExportStatusMissing && !!autoExportStatus?.running &&
    exporterLastSuccessAgeMs !== null && exporterLastSuccessAgeMs < EXPORTER_SUCCESS_GRACE_MS;

  return (
    <div className="mb-3">
      <div className="flex items-center justify-between px-1 flex-wrap gap-2">
        <Pill tone={stale ? "red" : "green"}>
          {stale ? "STALE EXPORT" : "EXPORT FRESH"} · last generated {formatAgeMs(ageMs)} ago
        </Pill>
        <span className="v3-label">auto-refreshing every 3s</span>
      </div>
      {exporterRunningButDataStale ? (
        <div className="text-xs mt-1.5 px-1" style={{ color: "var(--v3-red)" }}>
          Exporter running but dashboard data files are not updating — check exporter output path.
        </div>
      ) : (
        stale && (
          <div className="text-xs mt-1.5 px-1" style={{ color: "var(--v3-gold)" }}>
            Run <span className="v3-mono">Update Poly Obsidian Report.bat</span> or start the
            auto-exporter to refresh exported data.
          </div>
        )
      )}
      <div className="text-xs mt-1 px-1" style={{ color: "var(--v3-muted-2)" }}>
        Data source: read-only exported JSON · no DB writes · no order endpoints
      </div>
    </div>
  );
}
