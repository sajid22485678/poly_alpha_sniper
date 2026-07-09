"use client";

import { useEffect, useRef, useState } from "react";
import { Hero } from "@/components/Hero";
import { PnlBanner } from "@/components/PnlBanner";
import { KpiGrid } from "@/components/KpiGrid";
import { ChartsPanel } from "@/components/ChartsPanel";
import { SignalEnginePanel } from "@/components/SignalEnginePanel";
import { MarketIntelPanel } from "@/components/MarketIntelPanel";
import { TradeTape } from "@/components/TradeTape";
import { RejectTaxonomyPanel } from "@/components/RejectTaxonomyPanel";
import { AutoExporterStatusPanel } from "@/components/AutoExporterStatusPanel";
import { CandidateBookStatusPanel } from "@/components/CandidateBookStatusPanel";
import { GateWaterfallPanel } from "@/components/GateWaterfallPanel";
import { HermesPanel } from "@/components/HermesPanel";
import { LiveFeedStatePanel } from "@/components/LiveFeedStatePanel";
import { LiveReadinessPanel } from "@/components/LiveReadinessPanel";
import { OpportunityEnginePanel } from "@/components/OpportunityEnginePanel";
import { OracleStatusPanel } from "@/components/OracleStatusPanel";
import { SafetyFooter } from "@/components/SafetyFooter";
import { StatusBanner } from "@/components/StatusBanner";
import { SectionShell } from "@/components/ui";
import type { SnapshotResponse } from "@/lib/types";

const POLL_MS = 3000;

export function DashboardClient() {
  const [data, setData] = useState<SnapshotResponse | null>(null);
  const [connectionError, setConnectionError] = useState(false);
  const [now, setNow] = useState<number>(() => Date.now());
  const inFlight = useRef(false);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      if (inFlight.current) return;
      inFlight.current = true;
      try {
        const res = await fetch("/api/snapshot", { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = (await res.json()) as SnapshotResponse;
        if (!cancelled) {
          setData(json);
          setConnectionError(false);
        }
      } catch {
        if (!cancelled) setConnectionError(true);
      } finally {
        inFlight.current = false;
      }
    }

    poll();
    const dataId = setInterval(poll, POLL_MS);
    const clockId = setInterval(() => setNow(Date.now()), 1000);
    return () => {
      cancelled = true;
      clearInterval(dataId);
      clearInterval(clockId);
    };
  }, []);

  const status = data?.latest_status ?? null;
  const summary = data?.trade_summary ?? null;
  const rejects = data?.reject_breakdown ?? null;
  const snapshot = data?.snapshot ?? null;

  return (
    <div className="min-h-screen flex flex-col">
      <Hero status={status} />

      <main className="flex-1 px-6 py-4 max-w-[1440px] w-full mx-auto space-y-4">
        <StatusBanner
          generatedTsMs={data?.snapshot?.generated_ts_ms ?? data?.latest_status?.generated_ts_ms ?? null}
          nowMs={now}
          missingFiles={data?.missing_files ?? []}
          connectionError={connectionError}
          autoExportStatus={data?.auto_export_status ?? null}
          autoExportStatusMissing={data?.auto_export_status_missing ?? true}
        />

        <PnlBanner summary={summary} />

        <SectionShell title="Key Metrics">
          <KpiGrid status={status} summary={summary} />
        </SectionShell>

        <SectionShell title="Performance">
          <ChartsPanel exits={summary?.recent_exits ?? null} />
        </SectionShell>

        <SectionShell title="Signal Engine">
          <SignalEnginePanel
            marketState={snapshot?.latest_market_state ?? null}
            status={status}
            rejects={rejects}
            classificationFramework={snapshot?.classification_framework ?? null}
          />
        </SectionShell>

        <SectionShell title="Oracle-Aware EV Engine">
          <OracleStatusPanel status={snapshot?.oracle_status ?? null} nowMs={now} />
        </SectionShell>

        <SectionShell title="Live Feed State">
          <LiveFeedStatePanel state={snapshot?.live_feed_state ?? null} />
        </SectionShell>

        <SectionShell title="Candidate Book & Entry Gates">
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
            <CandidateBookStatusPanel status={snapshot?.candidate_book_status ?? null} />
            <GateWaterfallPanel waterfall={snapshot?.gate_waterfall ?? null} />
          </div>
        </SectionShell>

        <SectionShell title="Aggressive Shadow Opportunity Engine">
          <OpportunityEnginePanel diag={snapshot?.opportunity_diagnostics ?? null} />
        </SectionShell>

        <SectionShell title="Live Readiness & Shadow Compounding">
          <LiveReadinessPanel
            readiness={snapshot?.live_readiness ?? null}
            compounding={snapshot?.shadow_compounding ?? null}
          />
        </SectionShell>

        <SectionShell title="Market Intelligence">
          <MarketIntelPanel
            status={status}
            marketState={snapshot?.latest_market_state ?? null}
            rejects={rejects}
            lastScanSnapshot={snapshot?.last_scan_snapshot ?? null}
            noShockWatchlist={snapshot?.no_shock_watchlist ?? null}
            nowMs={now}
          />
        </SectionShell>

        <SectionShell title="Trade Tape & Reject Taxonomy">
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-3">
            <div className="xl:col-span-2">
              <TradeTape exits={summary?.recent_exits ?? null} orders={snapshot?.recent_orders ?? null} />
            </div>
            <RejectTaxonomyPanel rejects={rejects} />
          </div>
        </SectionShell>

        <SectionShell title="Hermes Agent">
          <HermesPanel brief={snapshot?.hermes_brief ?? null} rejects={rejects} />
        </SectionShell>

        <SectionShell title="Data Pipeline">
          <AutoExporterStatusPanel
            status={data?.auto_export_status ?? null}
            missing={data?.auto_export_status_missing ?? true}
          />
        </SectionShell>
      </main>

      <SafetyFooter status={status} brief={snapshot?.hermes_brief ?? null} />
    </div>
  );
}
