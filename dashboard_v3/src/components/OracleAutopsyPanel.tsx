import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { AnchorWindowReport, OracleAnchorAutopsy } from "@/lib/types";

/** Per-asset current/next anchor autopsy (read-only diagnostics). Answers
 * "is this upstream delay or our bug?" with the exact field-presence map,
 * source path, missing reason, retry count, and window match — never a gate,
 * never fabricates an anchor. */

function classify(r: AnchorWindowReport | undefined): { label: string; tone: "green" | "gold" | "red" | "neutral" } {
  if (!r || r.final_missing_reason === "NOT_RECORDED") return { label: "NOT RECORDED", tone: "neutral" };
  if (r.final_anchor_available) return { label: "ANCHOR GOOD", tone: "green" };
  switch (r.final_missing_reason) {
    case "UPSTREAM_NOT_PUBLISHED": return { label: "MISSING — UPSTREAM NOT PUBLISHED", tone: "gold" };
    case "HYDRATION_FAILED": return { label: "MISSING — HYDRATION FAILED", tone: "gold" };
    case "SCHEMA_UNKNOWN": return { label: "MISSING — SCHEMA UNKNOWN", tone: "red" };
    case "EXPIRED_OR_WRONG_WINDOW": return { label: "MISMATCH — WRONG WINDOW", tone: "red" };
    case "EVENT_NOT_FOUND": return { label: "MISSING — EVENT NOT FOUND", tone: "gold" };
    default: return { label: "MISSING — NEEDS INVESTIGATION", tone: "red" };
  }
}

function verdict(r: AnchorWindowReport | undefined): string {
  if (!r || r.final_anchor_available) return "";
  if (r.final_missing_reason === "UPSTREAM_NOT_PUBLISHED")
    return "upstream delay (Polymarket publishes priceToBeat after window open) — fail-closed is correct";
  if (r.final_missing_reason === "SCHEMA_UNKNOWN")
    return "schema change upstream — extractor needs a new variant";
  if (r.final_missing_reason === "EXPIRED_OR_WRONG_WINDOW")
    return "window mismatch — investigate rollover";
  if (r.final_missing_reason === "HYDRATION_FAILED")
    return "hydration endpoint did not recover it — will retry every refresh";
  return "";
}

function WindowCell({ r, which }: { r: AnchorWindowReport | undefined; which: string }) {
  const c = classify(r);
  return (
    <div className="v3-card-inset">
      <div className="flex items-center justify-between mb-1 gap-2 flex-wrap">
        <span className="v3-label">{which}</span>
        <Pill tone={c.tone}>{c.label}</Pill>
      </div>
      {r?.slug ? (
        <>
          <div className="text-xs v3-mono truncate" style={{ color: "var(--v3-muted-2)" }}>
            {r.slug} · close in {r.seconds_until_close ?? "?"}s · window match: {String(r.exact_window_match ?? false)}
          </div>
          <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
            price_to_beat: {r.final_price_to_beat ?? "—"}
            {r.final_anchor_source_path ? ` (via ${r.final_anchor_source_path})` : ""}
          </div>
          <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
            hydration: {r.hydration_attempted ? (r.hydration_success ? "recovered" : "attempted, not recovered") : "not needed"} · retries: {r.retry_count ?? 0} · fields checked: {r.fields_checked?.length ?? 0}
          </div>
          {r.last_successful_anchor_for_asset ? (
            <div className="text-xs v3-mono mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
              last good anchor: {r.last_successful_anchor_for_asset.price_to_beat} ({r.last_successful_anchor_age_s ?? "?"}s ago) — never reused across windows
            </div>
          ) : null}
          {verdict(r) ? (
            <div className="text-xs mt-1" style={{ color: "var(--v3-gold)" }}>{verdict(r)}</div>
          ) : null}
        </>
      ) : (
        <NotAvailable>{r?.note ?? "no market row this refresh"}</NotAvailable>
      )}
    </div>
  );
}

export function OracleAutopsyPanel({ autopsy }: { autopsy: OracleAnchorAutopsy | null | undefined }) {
  const assets = autopsy?.assets ? Object.keys(autopsy.assets) : [];
  return (
    <Card>
      <CardHeader title="Oracle Anchor Autopsy (current + next window)" />
      {assets.length === 0 ? (
        <div className="py-4 text-center">
          <NotAvailable>
            {autopsy?.error ? `autopsy error: ${autopsy.error}` : "no autopsy yet (bot restart required)"}
          </NotAvailable>
        </div>
      ) : (
        <div className="space-y-3">
          {assets.map((asset) => (
            <div key={asset}>
              <div className="font-semibold mb-1">{asset}</div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <WindowCell r={autopsy!.assets![asset].current} which="CURRENT" />
                <WindowCell r={autopsy!.assets![asset].next} which="NEXT (prehydrated)" />
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
