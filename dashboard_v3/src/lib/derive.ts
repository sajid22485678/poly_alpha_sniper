import type { ExitRow, OrderRow } from "@/lib/types";

// Pure client-side derivations from real exported data only. Nothing here
// invents a data point — every value is a running computation (cumulative
// sum, running peak) over the real recent_exits array from trade_summary.json.
// Because recent_exits is capped (25 rows) this reflects a recent sample,
// not full trading history — components must label it as such.

export interface CumulativePoint {
  ts_ms: number;
  label: string;
  cumulative_pnl: number;
  pnl_usd: number;
  drawdown: number;
}

export function cumulativePnlSeries(exits: ExitRow[]): CumulativePoint[] {
  const sorted = [...exits].sort((a, b) => a.ts_ms - b.ts_ms);
  let running = 0;
  let peak = 0;
  return sorted.map((row) => {
    running += row.pnl_usd;
    peak = Math.max(peak, running);
    return {
      ts_ms: row.ts_ms,
      label: new Date(row.ts_ms).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" }),
      cumulative_pnl: Number(running.toFixed(4)),
      pnl_usd: row.pnl_usd,
      drawdown: Number((peak - running).toFixed(4)),
    };
  });
}

export interface TradeTapeRow extends ExitRow {
  side: string | null;
  entry_price: number | null;
}

/** Best-effort join of an exit row to its entry order (same market_id, the
 * order with an empty exit_reason, created before the exit) so the trade
 * tape can show side + entry price. Falls back to null (rendered as "—")
 * when no confident match exists — never guesses. */
export function joinTradeTape(exits: ExitRow[], orders: OrderRow[]): TradeTapeRow[] {
  const entriesByMarket = new Map<string, OrderRow[]>();
  for (const o of orders) {
    if (o.exit_reason === "") {
      const list = entriesByMarket.get(o.market_id) ?? [];
      list.push(o);
      entriesByMarket.set(o.market_id, list);
    }
  }
  return exits.map((exit) => {
    const candidates = (entriesByMarket.get(exit.market_id) ?? []).filter(
      (o) => o.created_ts_ms <= exit.ts_ms
    );
    candidates.sort((a, b) => b.created_ts_ms - a.created_ts_ms);
    const entry = candidates[0] ?? null;
    return { ...exit, side: entry?.side ?? null, entry_price: entry?.price ?? null };
  });
}
