import { formatUsd } from "@/lib/format";
import { NotAvailable } from "@/components/ui";
import type { TradeSummary } from "@/lib/types";

/**
 * Visually mirrors the reference's hero PnL banner, but the content is
 * deliberately NOT a 1:1 copy: the reference shows a real-money "Anonymous
 * Whale / Verified on-chain / $881,418" claim for a different, live product.
 * Ours is shadow-mode simulated PnL and must never be styled or worded to
 * look like a verified real-money result. See SCREENSHOT_COMPARISON_NOTES.md.
 */
export function PnlBanner({ summary }: { summary: TradeSummary | null }) {
  const allTime = summary?.all_time_pnl_usd ?? null;
  const positive = allTime !== null && allTime >= 0;

  return (
    <div className="v3-card flex items-center justify-between gap-6 flex-wrap py-6">
      <div className="min-w-[140px]">
        <div className="v3-label">All-Time · PnL</div>
        <div className="text-xs mt-1" style={{ color: "var(--v3-gold)" }}>
          Shadow — Simulated, Not Live
        </div>
      </div>

      <div className="flex-1 text-center min-w-[220px]">
        <div className="text-sm italic" style={{ color: "var(--v3-muted)" }}>
          Poly Alpha Sniper
        </div>
        <div className="v3-mono text-xs mt-0.5" style={{ color: "var(--v3-muted-2)" }}>
          shadow_live · dry_run=true · not real funds
        </div>
        <div
          className="v3-hero-number mt-1"
          style={{ fontSize: "clamp(2.2rem, 5vw, 3.2rem)", color: positive ? "var(--v3-cream)" : "var(--v3-red)" }}
        >
          {allTime !== null ? (
            <>
              <span style={{ color: "var(--v3-gold)" }}>{allTime < 0 ? "-$" : "+$"}</span>
              {Math.abs(allTime).toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 4 })}
            </>
          ) : (
            <NotAvailable>No data yet</NotAvailable>
          )}
        </div>
      </div>

      <div className="min-w-[140px] text-right">
        <div className="v3-label">Trades · Sample</div>
        <div className="text-xs mt-1" style={{ color: "var(--v3-gold)" }}>
          {summary ? summary.sample_size_note : <NotAvailable />}
        </div>
        <div className="text-xs mt-1" style={{ color: "var(--v3-muted-2)" }}>
          today: {formatUsd(summary?.today_pnl_usd ?? null, { signed: true })}
        </div>
      </div>
    </div>
  );
}
