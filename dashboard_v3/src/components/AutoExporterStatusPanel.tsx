import { formatDateTime } from "@/lib/format";
import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { AutoExportStatus } from "@/lib/types";

/** Read-only status display for scripts/auto_export_loop.ps1. Deliberately
 * has no buttons that execute anything -- start/stop/status are shown as
 * copy-paste commands only, matching the read-only-dashboard contract. */
export function AutoExporterStatusPanel({
  status,
  missing,
}: {
  status: AutoExportStatus | null;
  missing: boolean;
}) {
  const state = missing || !status ? "UNKNOWN" : status.running ? "RUNNING" : "STOPPED";
  const tone = state === "RUNNING" ? "green" : state === "STOPPED" ? "gold" : "neutral";

  return (
    <Card>
      <CardHeader title="Auto Exporter" right={<Pill tone={tone}>AUTO EXPORTER: {state}</Pill>} />

      {missing || !status ? (
        <div className="py-4 text-center">
          <NotAvailable>
            no status file yet — the auto-export loop has never been started on this machine
          </NotAvailable>
        </div>
      ) : (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
          <Stat label="Interval" value={`${status.interval_seconds}s`} />
          <Stat
            label="Last Success"
            value={status.last_success_at ? formatDateTime(new Date(status.last_success_at).getTime()) : "never"}
          />
          <Stat
            label="Consecutive Failures"
            value={String(status.consecutive_failures)}
            valueColor={status.consecutive_failures > 0 ? "var(--v3-red)" : undefined}
          />
          <Stat label="Exports Completed" value={String(status.exports_completed)} />
        </div>
      )}

      {status?.last_error_message && (
        <div className="v3-card-inset mb-3">
          <div className="v3-label mb-1">Last Error</div>
          <div className="text-xs v3-mono" style={{ color: "var(--v3-red)" }}>
            {status.last_error_message}
          </div>
          {status.last_error_at && (
            <div className="text-xs mt-1" style={{ color: "var(--v3-muted-2)" }}>
              at {formatDateTime(new Date(status.last_error_at).getTime())}
            </div>
          )}
        </div>
      )}

      {status && (
        <div className="text-xs v3-mono mb-3" style={{ color: "var(--v3-muted-2)" }}>
          <div>log: {status.log_path}</div>
          <div>status: {status.status_path}</div>
        </div>
      )}

      <div className="v3-divider pt-3 text-xs v3-mono space-y-1" style={{ color: "var(--v3-muted)" }}>
        <div>start: scripts\start_auto_export_loop.bat</div>
        <div>stop: powershell -File scripts\stop_auto_export_loop.ps1</div>
        <div>status: powershell -File scripts\status_auto_export_loop.ps1</div>
      </div>
    </Card>
  );
}

function Stat({ label, value, valueColor }: { label: string; value: string; valueColor?: string }) {
  return (
    <div>
      <div className="v3-label">{label}</div>
      <div className="font-semibold mt-1" style={{ color: valueColor }}>{value}</div>
    </div>
  );
}
