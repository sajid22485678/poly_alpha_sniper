import { formatDateTime, formatUsd } from "@/lib/format";
import { joinTradeTape } from "@/lib/derive";
import { Card, CardHeader, NotAvailable, Pill } from "@/components/ui";
import type { ExitRow, OrderRow } from "@/lib/types";

const STATUS_TONE: Record<string, "green" | "red" | "gold"> = {
  TAKE_PROFIT: "green",
  PARTIAL_TAKE_PROFIT: "green",
  STOP_LOSS: "red",
};

export function TradeTape({ exits, orders }: { exits: ExitRow[] | null; orders: OrderRow[] | null }) {
  const rows = exits && exits.length > 0 ? joinTradeTape(exits, orders ?? []) : [];

  return (
    <Card>
      <CardHeader title="Trade Tape · Recent Exits" />
      {rows.length === 0 ? (
        <div className="py-6 text-center">
          <NotAvailable>No completed trades yet</NotAvailable>
        </div>
      ) : (
        <div className="overflow-x-auto v3-scrollbar">
          <table className="w-full text-sm">
            <thead>
              <tr className="v3-divider text-left">
                {["Time", "Market", "Side", "Entry", "Exit", "PnL", "Hold", "Status"].map((h) => (
                  <th key={h} className="v3-label pb-2 pr-4 whitespace-nowrap font-normal text-left">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id} className="v3-divider">
                  <td className="py-2 pr-4 v3-mono text-xs whitespace-nowrap" style={{ color: "var(--v3-muted)" }}>
                    {formatDateTime(r.ts_ms)}
                  </td>
                  <td className="py-2 pr-4 v3-mono text-xs">#{r.market_id}</td>
                  <td className="py-2 pr-4">{r.side ?? <NotAvailable>—</NotAvailable>}</td>
                  <td className="py-2 pr-4 v3-mono text-xs">{r.entry_price != null ? r.entry_price.toFixed(3) : "—"}</td>
                  <td className="py-2 pr-4 v3-mono text-xs">{r.price.toFixed(3)}</td>
                  <td className="py-2 pr-4 v3-mono font-semibold" style={{ color: r.pnl_usd >= 0 ? "var(--v3-green)" : "var(--v3-red)" }}>
                    {formatUsd(r.pnl_usd, { signed: true })}
                  </td>
                  <td className="py-2 pr-4 v3-mono text-xs">{r.hold_seconds.toFixed(1)}s</td>
                  <td className="py-2 pr-4">
                    <Pill tone={STATUS_TONE[r.reason] ?? "neutral"}>{r.reason.replace(/_/g, " ")}</Pill>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}
