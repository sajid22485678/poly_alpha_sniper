import { NextResponse } from "next/server";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";

import type { DashboardSnapshot, LatestStatus, RejectBreakdown, SnapshotResponse, TradeSummary } from "@/lib/types";

export const dynamic = "force-dynamic";

/**
 * Read-only, allow-listed export reader.
 *
 * SAFETY CONTRACT (mirrored by tests/test_dashboard_v3_safety.py):
 * - GET only. No POST/PUT/DELETE/PATCH handler exists in this file or
 *   anywhere else under src/app/api/ — there is no write path.
 * - The 4 file paths below are hardcoded constants, not built from request
 *   input (query params, headers, body) — no path-traversal surface.
 * - Never reads process.env, .env, or any *_KEY/*_TOKEN/*_SECRET value.
 * - Never imports/calls anything from the bot's execution or order-placement
 *   code — this route only calls node:fs readFile/stat on plain JSON files
 *   that poly_alpha_sniper/reporting/agent_export.py already redacted
 *   before writing to disk.
 */

const EXPORT_DIR = "D:/claude/agent_readonly/poly_alpha_sniper";

const FILES = {
  dashboard_snapshot: path.join(EXPORT_DIR, "dashboard_snapshot.json"),
  latest_status: path.join(EXPORT_DIR, "latest_status.json"),
  trade_summary: path.join(EXPORT_DIR, "trade_summary.json"),
  reject_breakdown: path.join(EXPORT_DIR, "reject_breakdown.json"),
} as const;

async function readJsonIfExists<T>(filePath: string): Promise<{ data: T | null; ageMs: number | null; missing: boolean }> {
  try {
    const [raw, st] = await Promise.all([readFile(filePath, "utf-8"), stat(filePath)]);
    return { data: JSON.parse(raw) as T, ageMs: Date.now() - st.mtimeMs, missing: false };
  } catch {
    return { data: null, ageMs: null, missing: true };
  }
}

export async function GET() {
  const [snapshot, latestStatus, tradeSummary, rejectBreakdown] = await Promise.all([
    readJsonIfExists<DashboardSnapshot>(FILES.dashboard_snapshot),
    readJsonIfExists<LatestStatus>(FILES.latest_status),
    readJsonIfExists<TradeSummary>(FILES.trade_summary),
    readJsonIfExists<RejectBreakdown>(FILES.reject_breakdown),
  ]);

  const missing_files: string[] = [];
  const file_ages_ms: Record<string, number | null> = {};
  for (const [key, result] of Object.entries({
    dashboard_snapshot: snapshot,
    latest_status: latestStatus,
    trade_summary: tradeSummary,
    reject_breakdown: rejectBreakdown,
  })) {
    file_ages_ms[key] = result.ageMs;
    if (result.missing) missing_files.push(key);
  }

  const body: SnapshotResponse = {
    fetched_ts_ms: Date.now(),
    snapshot: snapshot.data,
    latest_status: latestStatus.data,
    trade_summary: tradeSummary.data,
    reject_breakdown: rejectBreakdown.data,
    missing_files,
    file_ages_ms,
  };

  return NextResponse.json(body, { headers: { "Cache-Control": "no-store" } });
}
