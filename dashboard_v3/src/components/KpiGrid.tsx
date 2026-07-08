import { formatNumber, formatPct, formatUsd } from "@/lib/format";
import { KpiCard } from "@/components/ui";
import type { LatestStatus, TradeSummary } from "@/lib/types";

export function KpiGrid({ status, summary }: { status: LatestStatus | null; summary: TradeSummary | null }) {
  const green = "var(--v3-green)";
  const red = "var(--v3-red)";
  const cream = "var(--v3-cream)";

  const cards = [
    { label: "Equity", value: formatUsd(status?.equity_usd ?? null), sub: "current shadow balance" },
    {
      label: "Realized PnL",
      value: formatUsd(summary?.all_time_pnl_usd ?? null, { signed: true }),
      sub: "all-time",
      color: (summary?.all_time_pnl_usd ?? 0) >= 0 ? green : red,
    },
    {
      label: "Today PnL",
      value: formatUsd(summary?.today_pnl_usd ?? null, { signed: true }),
      sub: "since 00:00 local",
      color: (summary?.today_pnl_usd ?? 0) >= 0 ? green : red,
    },
    { label: "Win Rate", value: formatPct(summary?.winrate ?? null), sub: `${status?.trades ?? 0} completed trades` },
    { label: "Profit Factor", value: formatNumber(summary?.profit_factor ?? null, 2), sub: "gross win / gross loss", color: "var(--v3-gold-bright)" },
    { label: "Expectancy", value: formatUsd(summary?.expectancy_usd ?? null, { signed: true, decimals: 4 }), sub: "per trade" },
    { label: "Trades Completed", value: formatNumber(status?.trades ?? null), sub: summary?.sample_size_note ?? "" },
    { label: "Average Edge", value: formatPct(summary?.avg_edge ?? null, 1), sub: "mean signal edge" },
    {
      label: "Drawdown",
      value: summary ? `${formatUsd(summary.max_drawdown_usd, { decimals: 4 })} (${summary.max_drawdown_pct.toFixed(1)}%)` : "—",
      sub: "max, shadow equity curve",
      color: red,
    },
  ];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
      {cards.map((c) => (
        <KpiCard key={c.label} label={c.label} value={c.value} sub={c.sub} valueColor={c.color ?? cream} />
      ))}
    </div>
  );
}
