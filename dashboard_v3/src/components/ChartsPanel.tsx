"use client";

import { Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Card, CardHeader, Label, NotAvailable } from "@/components/ui";
import { cumulativePnlSeries } from "@/lib/derive";
import { formatUsd } from "@/lib/format";
import type { ExitRow } from "@/lib/types";

const GOLD = "#c9a35a";
const RED = "#e5615a";
const GREEN = "#5fd98a";
const GRID = "#2a262040";

function TooltipBox({ active, payload, label }: { active?: boolean; payload?: { value: number; name: string }[]; label?: string }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="v3-card-inset v3-mono text-xs">
      <div style={{ color: "var(--v3-muted)" }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i}>{p.name}: {formatUsd(p.value)}</div>
      ))}
    </div>
  );
}

export function ChartsPanel({ exits }: { exits: ExitRow[] | null }) {
  const series = exits && exits.length > 0 ? cumulativePnlSeries(exits) : [];
  const last = series.at(-1);
  const peak = series.length ? Math.max(...series.map((p) => p.cumulative_pnl)) : 0;
  const trough = series.length ? Math.min(...series.map((p) => p.cumulative_pnl)) : 0;

  return (
    <div className="grid grid-cols-1 gap-3">
      <Card>
        <CardHeader
          title="Cumulative Shadow PnL · Recent Exits"
          right={
            series.length > 0 ? (
              <div className="flex items-center gap-4 v3-mono text-xs" style={{ color: "var(--v3-muted)" }}>
                <span>PEAK {formatUsd(peak)}</span>
                <span>TROUGH {formatUsd(trough)}</span>
                <span className={last && last.cumulative_pnl >= 0 ? "v3-pill v3-pill-green" : "v3-pill v3-pill-red"}>
                  {last ? formatUsd(last.cumulative_pnl, { signed: true }) : "—"}
                </span>
              </div>
            ) : null
          }
        />
        {series.length === 0 ? (
          <div className="py-10 text-center">
            <NotAvailable>No completed exits yet</NotAvailable>
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
              <defs>
                <linearGradient id="pnlFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={GOLD} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={GOLD} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke={GRID} vertical={false} />
              <XAxis dataKey="label" tick={{ fill: "#8b8578", fontSize: 10 }} axisLine={{ stroke: GRID }} tickLine={false} />
              <YAxis tick={{ fill: "#8b8578", fontSize: 10 }} axisLine={false} tickLine={false} orientation="right" width={56} />
              <Tooltip content={<TooltipBox />} />
              <Area type="monotone" dataKey="cumulative_pnl" name="Cumulative PnL" stroke={GOLD} strokeWidth={2} fill="url(#pnlFill)" />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <Card>
          <CardHeader title="Drawdown · Recent Exits Sample" />
          {series.length === 0 ? (
            <div className="py-6 text-center">
              <NotAvailable>No data yet</NotAvailable>
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={140}>
              <AreaChart data={series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                <defs>
                  <linearGradient id="ddFill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={RED} stopOpacity={0.4} />
                    <stop offset="100%" stopColor={RED} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid stroke={GRID} vertical={false} />
                <XAxis dataKey="label" tick={{ fill: "#8b8578", fontSize: 10 }} axisLine={{ stroke: GRID }} tickLine={false} />
                <YAxis tick={{ fill: "#8b8578", fontSize: 10 }} axisLine={false} tickLine={false} orientation="right" width={48} />
                <Tooltip content={<TooltipBox />} />
                <Area type="monotone" dataKey="drawdown" name="Drawdown" stroke={RED} strokeWidth={1.5} fill="url(#ddFill)" />
              </AreaChart>
            </ResponsiveContainer>
          )}
          <Label className="mt-2">Full-history max drawdown is on the KPI row above (this chart is the recent-sample only).</Label>
        </Card>

        <Card>
          <CardHeader title="Trade Activity · Exit PnL" />
          {series.length === 0 ? (
            <div className="py-6 text-center">
              <NotAvailable>No data yet</NotAvailable>
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={140}>
              <BarChart data={series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                <CartesianGrid stroke={GRID} vertical={false} />
                <XAxis dataKey="label" tick={{ fill: "#8b8578", fontSize: 10 }} axisLine={{ stroke: GRID }} tickLine={false} />
                <YAxis tick={{ fill: "#8b8578", fontSize: 10 }} axisLine={false} tickLine={false} orientation="right" width={48} />
                <Tooltip content={<TooltipBox />} />
                <Bar dataKey="pnl_usd" name="Exit PnL" radius={[2, 2, 0, 0]}>
                  {series.map((p, i) => (
                    <Cell key={i} fill={p.pnl_usd >= 0 ? GREEN : RED} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </Card>
      </div>
    </div>
  );
}
