import type { ReactNode } from "react";
import { cx } from "@/lib/format";

export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cx("v3-card", className)}>{children}</div>;
}

export function CardHeader({ dot, title, right }: { dot?: string; title: string; right?: ReactNode }) {
  return (
    <div className="flex items-center justify-between mb-4">
      <div className="flex items-center gap-2">
        {dot && <span className="v3-dot" style={{ background: dot }} />}
        <span className="v3-label">{title}</span>
      </div>
      {right}
    </div>
  );
}

export function Label({ children, gold, className }: { children: ReactNode; gold?: boolean; className?: string }) {
  return <div className={cx(gold ? "v3-label-gold" : "v3-label", className)}>{children}</div>;
}

export function Pill({
  children,
  tone = "neutral",
  className,
}: {
  children: ReactNode;
  tone?: "neutral" | "gold" | "green" | "red";
  className?: string;
}) {
  const toneClass = {
    neutral: "v3-pill",
    gold: "v3-pill v3-pill-gold",
    green: "v3-pill v3-pill-green",
    red: "v3-pill v3-pill-red",
  }[tone];
  return <span className={cx(toneClass, className)}>{children}</span>;
}

export function StatusDot({ color, pulse }: { color: string; pulse?: boolean }) {
  return <span className={cx("v3-dot", pulse && "v3-pulse")} style={{ background: color }} />;
}

export function KpiCard({
  label,
  value,
  sub,
  valueColor,
}: {
  label: string;
  value: string;
  sub?: string;
  valueColor?: string;
}) {
  return (
    <div className="v3-card !p-4">
      <Label>{label}</Label>
      <div
        className="text-2xl font-bold mt-2 tabular-nums"
        style={{ color: valueColor ?? "var(--v3-cream)" }}
      >
        {value}
      </div>
      {sub && <div className="text-xs mt-1" style={{ color: "var(--v3-muted-2)" }}>{sub}</div>}
    </div>
  );
}

export function NotAvailable({ children = "not available" }: { children?: ReactNode }) {
  return <span style={{ color: "var(--v3-muted-2)" }}>{children}</span>;
}

export function SectionShell({
  title,
  subtitle,
  right,
  children,
}: {
  title: string;
  subtitle?: string;
  right?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="mb-4">
      <div className="flex items-end justify-between mb-3 px-1">
        <div>
          <div className="v3-label">{title}</div>
          {subtitle && <div className="text-xs mt-0.5" style={{ color: "var(--v3-muted-2)" }}>{subtitle}</div>}
        </div>
        {right}
      </div>
      {children}
    </section>
  );
}
