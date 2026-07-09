import { formatAgeMs } from "@/lib/format";
import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { CandidateBookStatus } from "@/lib/types";

const TONE: Record<string, "green" | "gold" | "red" | "neutral"> = {
  FRESH: "green",
  STALE: "gold",
  FETCH_FAILED: "red",
  NOT_REACHED_BOOK_STAGE: "gold",
  NOT_EVALUATED: "neutral",
  NO_CANDIDATE: "neutral",
};

function displayStatus(status: CandidateBookStatus | null): string {
  if (!status?.status) return "NO_CANDIDATE";
  if (status.status === "NOT_EVALUATED" && status.earlier_gate_reason === "NO_CANDIDATE") {
    return "NO_CANDIDATE";
  }
  return status.status;
}

export function CandidateBookStatusPanel({ status }: { status: CandidateBookStatus | null }) {
  const label = displayStatus(status);
  const reachedBookStage = label === "FRESH" || label === "STALE" || label === "FETCH_FAILED";

  return (
    <Card>
      <CardHeader title="Candidate Book Status" right={<Pill tone={TONE[label] ?? "neutral"}>{label}</Pill>} />
      {label === "NO_CANDIDATE" ? (
        <div className="py-4 text-center">
          <NotAvailable>NO_CANDIDATE</NotAvailable>
        </div>
      ) : label === "NOT_REACHED_BOOK_STAGE" ? (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <Stat label="Candidate" value={`${status?.asset ?? "unknown"} / ${status?.market_id ?? "unknown"}`} />
          <Stat label="Earlier Gate" value={status?.earlier_gate_reason ?? "not available"} />
        </div>
      ) : reachedBookStage ? (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Stat label="Asset / Side" value={`${status?.asset ?? "-"} / ${status?.side ?? "-"}`} />
            <Stat
              label="Book Age"
              value={formatAgeMs(status?.book_age_ms ?? null)}
              valueColor={
                status?.book_age_ms != null &&
                status?.freshness_threshold_ms != null &&
                status.book_age_ms > status.freshness_threshold_ms
                  ? "var(--v3-gold)"
                  : undefined
              }
            />
            <Stat
              label="Threshold"
              value={status?.freshness_threshold_ms != null ? `${status.freshness_threshold_ms}ms` : "-"}
            />
            <Stat
              label="Depth Near Best"
              value={status?.depth_near_best_usd != null ? `$${status.depth_near_best_usd}` : "-"}
            />
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3">
            <Stat label="Best Bid" value={status?.best_bid != null ? status.best_bid.toFixed(3) : "-"} />
            <Stat label="Best Ask" value={status?.best_ask != null ? status.best_ask.toFixed(3) : "-"} />
            <Stat label="Spread" value={status?.spread != null ? status.spread.toFixed(3) : "-"} />
            <Stat
              label="Direct Refresh"
              value={status?.direct_refresh_attempted ? `yes / ${status.direct_refresh_result ?? ""}` : "no"}
              valueColor={status?.direct_refresh_result === "fetch_failed" ? "var(--v3-red)" : undefined}
            />
          </div>
          {status?.final_reject_reason ? (
            <div className="mt-3">
              <Stat label="Final Reject" value={status.final_reject_reason} valueColor="var(--v3-red)" />
            </div>
          ) : null}
        </>
      ) : (
        <div className="py-4 text-center">
          <NotAvailable>{label}</NotAvailable>
        </div>
      )}
    </Card>
  );
}

function Stat({ label, value, valueColor }: { label: string; value: string; valueColor?: string }) {
  return (
    <div>
      <div className="v3-label">{label}</div>
      <div className="font-semibold text-sm mt-1 break-words" style={valueColor ? { color: valueColor } : undefined}>
        {value}
      </div>
    </div>
  );
}
