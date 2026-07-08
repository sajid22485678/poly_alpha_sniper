"use client";

import { useEffect, useState } from "react";
import { formatPct } from "@/lib/format";
import { Card, CardHeader, Label, NotAvailable, Pill } from "@/components/ui";
import type { LatestMarketState, LatestStatus, MinOrderLatest, RejectBreakdown } from "@/lib/types";

const FIVE_MIN_MS = 5 * 60 * 1000;

function useNextWindowCountdown() {
  const [msLeft, setMsLeft] = useState<number | null>(null);
  useEffect(() => {
    const id = setInterval(() => {
      const now = Date.now();
      setMsLeft(FIVE_MIN_MS - (now % FIVE_MIN_MS));
    }, 1000);
    return () => clearInterval(id);
  }, []);
  if (msLeft === null) return "—";
  const m = Math.floor(msLeft / 60_000);
  const s = Math.floor((msLeft % 60_000) / 1000);
  return `${m}:${s.toString().padStart(2, "0")}`;
}

function decisionColor(label: string | undefined) {
  if (label === "ENTER" || label === "ENTER (shadow)") return "var(--v3-green)";
  if (label === "WAIT") return "var(--v3-gold)";
  return "var(--v3-muted)";
}

export function SignalEnginePanel({
  marketState,
  status,
  rejects,
  classificationFramework,
}: {
  marketState: LatestMarketState | null;
  status: LatestStatus | null;
  rejects: RejectBreakdown | null;
  classificationFramework: string | null;
}) {
  const countdown = useNextWindowCountdown();
  const state = marketState?.available ? marketState : null;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
      <Card>
        <CardHeader
          dot={state ? decisionColor(state.decision_label) : "var(--v3-muted-2)"}
          title={`Signal Engine${state ? ` · ${state.asset}` : ""}`}
          right={
            <Pill tone={classificationFramework === "not_implemented" ? "neutral" : "gold"}>
              Classification: {classificationFramework === "not_implemented" ? "NOT IMPLEMENTED" : classificationFramework ?? "—"}
            </Pill>
          }
        />

        {!state ? (
          <div className="py-6 text-center">
            <NotAvailable>No evaluated market yet</NotAvailable>
          </div>
        ) : (
          <>
            <div className="flex items-center gap-3 mb-4">
              <div className="v3-card-inset flex-1 text-center">
                <div className="font-bold text-lg">{state.direction}</div>
                <Label>Direction</Label>
              </div>
              <div className="v3-pill v3-pill-gold shrink-0">p̂={state.fair_probability.toFixed(2)}</div>
              <div className="v3-card-inset flex-1 text-center">
                <div className="font-bold text-lg" style={{ color: decisionColor(state.decision_label) }}>
                  {state.decision_label}
                </div>
                <Label>Decision</Label>
              </div>
            </div>

            <div className="grid grid-cols-3 gap-2 mb-4">
              <div>
                <Label>Edge</Label>
                <div className="font-semibold">{formatPct(state.edge, 1)}</div>
              </div>
              <div>
                <Label>Confidence</Label>
                <div className="font-semibold">{state.confidence.toFixed(0)}%</div>
              </div>
              <div>
                <Label>Tier</Label>
                <div className="font-semibold">{state.tier}</div>
              </div>
            </div>

            <div className="v3-pill w-full justify-center !py-2 mb-4">
              SIGNAL · {state.decision_label}
              {state.reject_reason ? ` — ${state.reject_reason}` : ""}
            </div>

            <div className="v3-divider pt-3 grid grid-cols-3 gap-2 text-center">
              <div>
                <Label>Predictions</Label>
                <div className="font-semibold">{status?.predictions ?? "—"}</div>
              </div>
              <div>
                <Label>Signals</Label>
                <div className="font-semibold">{status?.signals ?? "—"}</div>
              </div>
              <div>
                <Label>Rejects (recent)</Label>
                <div className="font-semibold">{rejects?.total ?? "—"}</div>
              </div>
            </div>
          </>
        )}
      </Card>

      <Card>
        <CardHeader title="Signal & Sizing Logic (real, read-only)" />
        <div className="text-xs mb-3" style={{ color: "var(--v3-muted-2)" }}>
          This bot does not implement Markov-chain state transitions or Kelly-criterion
          sizing — those are <b>NOT IMPLEMENTED</b>. The formulas below are the bot&apos;s
          actual edge gate and min-order sizing checks, from{" "}
          <code className="v3-mono">strategy/edge_engine.py</code> and{" "}
          <code className="v3-mono">execution/order_validator.py</code>.
        </div>

        <FormulaBlock
          formula="edge = fair_probability − executable_price"
          detail={
            state
              ? `edge = ${state.fair_probability.toFixed(3)} − ${state.signal_side_price.toFixed(3)} = ${state.edge >= 0 ? "+" : ""}${state.edge.toFixed(3)} · ${state.decision_label}`
              : "no evaluated market yet"
          }
        />
        <MinOrderFormulaBlock minOrder={rejects?.min_order?.latest ?? null} />
        <FormulaBlock
          formula="5-MIN windows · 288 windows / day"
          detail={`next window in ${countdown}`}
        />
      </Card>
    </div>
  );
}

function FormulaBlock({ formula, detail }: { formula: string; detail: string }) {
  return (
    <div className="v3-divider py-3 first:border-t-0 first:pt-0">
      <div className="v3-mono text-sm font-semibold">{formula}</div>
      <div className="v3-mono text-xs mt-1" style={{ color: "var(--v3-muted)" }}>
        {detail}
      </div>
    </div>
  );
}

function MinOrderFormulaBlock({ minOrder }: { minOrder: MinOrderLatest | null }) {
  return (
    <FormulaBlock
      formula="min_required_usd = min_shares × ask_price"
      detail={
        minOrder
          ? `min_required = ${minOrder.min_shares} × ${minOrder.ask_price.toFixed(2)} = $${minOrder.min_required_usd.toFixed(2)} vs max_trade_usd=$${minOrder.configured_max_trade_usd.toFixed(2)} · shortfall=$${minOrder.shortfall_usd.toFixed(2)}`
          : "no recent min-order block"
      }
    />
  );
}
