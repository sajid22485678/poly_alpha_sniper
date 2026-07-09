import { ageSince, formatAgeMs, formatPct } from "@/lib/format";
import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { OracleStatus } from "@/lib/types";

function qualityTone(quality: string | null): "green" | "gold" | "red" {
  if (quality === "good") return "green";
  if (quality === "fallback") return "gold";
  return "red";
}

export function OracleStatusPanel({ status, nowMs }: { status: OracleStatus | null; nowMs: number }) {
  const metadataUrl = status?.available ? status.metadata_url || status.resolution_source_url || "" : "";

  return (
    <Card>
      <CardHeader
        title="Oracle Anchor / Resolution Price"
        right={
          <Pill tone={status?.enabled ? "green" : "neutral"}>
            {status?.enabled ? "ORACLE-AWARE: ON" : "ORACLE-AWARE: OFF"}
          </Pill>
        }
      />
      <div className="text-xs mb-3" style={{ color: "var(--v3-muted-2)" }}>
        The bot settlement anchor is <span className="v3-mono">price_to_beat</span> from Polymarket event
        metadata. Binance, OKX, and Bybit are CEX lead indicators only.
      </div>

      {!status || !status.available ? (
        <div className="py-4 text-center">
          <Pill tone="gold">NO ANCHOR - {status?.reason ?? "not available"}</Pill>
        </div>
      ) : (
        <>
          <div className="flex items-center justify-between mb-3 gap-3">
            <Pill tone={qualityTone(status.oracle_anchor_quality)}>
              ANCHOR: {(status.oracle_anchor_quality ?? "unknown").toUpperCase()}
            </Pill>
            <span className="text-xs v3-mono text-right break-all" style={{ color: "var(--v3-muted-2)" }}>
              {status.market_id} / {status.asset}
            </span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Stat label="Price To Beat" value={status.price_to_beat != null ? status.price_to_beat.toFixed(2) : "not available"} />
            <Stat label="Anchor Source" value={status.anchor_source || status.oracle_source || "not available"} small />
            <Stat label="Settlement Anchor" value={status.settlement_anchor || "price_to_beat"} small />
            <Stat label="CEX Lead Source" value={status.cex_lead_source || "not available"} small />
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3">
            <Stat
              label="Anchor Age"
              value={status.oracle_open_ts_ms != null ? formatAgeMs(ageSince(status.oracle_open_ts_ms, nowMs)) : "not available"}
            />
            <Stat label="Latest CEX Price" value={status.cex_price != null ? status.cex_price.toFixed(2) : "not available"} />
            <Stat
              label="CEX vs Anchor Basis"
              value={status.oracle_vs_cex_basis_pct != null ? formatPct(status.oracle_vs_cex_basis_pct, 2) : "not available"}
              valueColor={
                status.oracle_vs_cex_basis_pct != null && Math.abs(status.oracle_vs_cex_basis_pct) > 0.02
                  ? "var(--v3-red)"
                  : undefined
              }
            />
            <Stat
              label="Latest EV"
              value={status.latest_ev ? status.latest_ev.ev.toFixed(4) : "not available"}
              valueColor={status.latest_ev ? (status.latest_ev.ev >= 0 ? "var(--v3-green)" : "var(--v3-red)") : undefined}
            />
          </div>
          {metadataUrl ? (
            <div className="text-xs mt-3 v3-mono truncate" style={{ color: "var(--v3-muted-2)" }}>
              Metadata URL (Polymarket-declared reference URL): {metadataUrl}
            </div>
          ) : null}
        </>
      )}
    </Card>
  );
}

function Stat({ label, value, small, valueColor }: { label: string; value: string; small?: boolean; valueColor?: string }) {
  return (
    <div>
      <div className="v3-label">{label}</div>
      <div
        className={small ? "text-xs mt-1 v3-mono break-words" : "font-semibold mt-1 break-words"}
        style={{ color: valueColor ?? (small ? "var(--v3-muted)" : undefined) }}
      >
        {value || <NotAvailable />}
      </div>
    </div>
  );
}
