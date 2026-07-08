"use client";

import { useEffect, useState } from "react";
import { formatAgeMs, formatClockTime } from "@/lib/format";
import { Pill, StatusDot } from "@/components/ui";
import type { LatestStatus } from "@/lib/types";

export function Hero({ status }: { status: LatestStatus | null }) {
  const [now, setNow] = useState<number | null>(null);
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const hbFresh = status?.heartbeat_age_ms != null && status.heartbeat_age_ms < 60_000;

  return (
    <header className="v3-card !rounded-none !border-x-0 !border-t-0 px-6 py-4">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <div
            className="w-9 h-9 rounded-lg flex items-center justify-center v3-mono font-bold"
            style={{ border: "1px solid var(--v3-card-border-strong)", color: "var(--v3-gold)" }}
          >
            P
          </div>
          <div>
            <div className="font-bold text-sm leading-tight">Claude × Hermes</div>
            <div className="v3-label-gold leading-tight">Poly Alpha Sniper · 5-MIN POLYMARKET AGENT</div>
          </div>
        </div>

        <div className="hidden md:flex items-center gap-3 v3-label">
          <span>Read-Only</span>
          <span style={{ color: "var(--v3-muted-2)" }}>·</span>
          <span>No Execution</span>
          <span style={{ color: "var(--v3-muted-2)" }}>·</span>
          <span>Shadow Mode</span>
        </div>

        <Pill tone={hbFresh ? "green" : "red"}>
          <StatusDot color={hbFresh ? "var(--v3-green)" : "var(--v3-red)"} pulse={hbFresh} />
          {now !== null ? formatClockTime(now) : "--:--:--"}
        </Pill>
      </div>

      <div className="flex items-center flex-wrap gap-2 mt-4">
        <Pill tone="gold">MODE: {status?.mode ?? "unknown"}</Pill>
        <Pill tone={status?.dry_run ? "green" : "red"}>DRY_RUN: {status ? String(status.dry_run).toUpperCase() : "—"}</Pill>
        <Pill tone={status?.live_enabled ? "red" : "green"}>LIVE: {status?.live_enabled ? "ON — ALERT" : "OFF"}</Pill>
        <Pill tone="green">REAL ORDERS: DISABLED</Pill>
        <Pill tone={hbFresh ? "green" : "red"}>HEARTBEAT: {formatAgeMs(status?.heartbeat_age_ms ?? null)} old</Pill>
        {(status?.panic_active || status?.kill_active) && (
          <Pill tone="red">
            {status?.panic_active ? "PANIC ACTIVE" : ""} {status?.kill_active ? "KILL ACTIVE" : ""}
          </Pill>
        )}
      </div>
    </header>
  );
}
