import { readFile, stat } from "node:fs/promises";

import type { FrequencyV4Snapshot, SnapshotResponse } from "@/lib/types";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const EXPORT_FILE = "D:/claude/agent_readonly/poly_alpha_frequency_v4/frequency_v4_dashboard.json";
const MAX_EXPORT_BYTES = 10 * 1024 * 1024;
const MAX_FUTURE_SKEW_MS = 5_000;

function response(body: SnapshotResponse, status = 200): Response {
  return Response.json(body, {
    status,
    headers: {
      "Cache-Control": "no-store, max-age=0, must-revalidate",
      "X-Content-Type-Options": "nosniff",
    },
  });
}

export async function GET(): Promise<Response> {
  try {
    const info = await stat(EXPORT_FILE);
    if (!info.isFile() || info.size > MAX_EXPORT_BYTES) {
      return response({ ok: false, missing: false, age_ms: null, snapshot: null, error: "invalid_export_file" }, 503);
    }
    const raw = await readFile(EXPORT_FILE, "utf8");
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return response({ ok: false, missing: false, age_ms: null, snapshot: null, error: "invalid_export_payload" }, 503);
    }
    const snapshot = parsed as FrequencyV4Snapshot;
    if (snapshot.schema_version !== 3) {
      return response({ ok: false, missing: false, age_ms: null, snapshot: null, error: "unsupported_export_schema" }, 503);
    }
    if (snapshot.strategy_id !== "lite_frequency_v4" || snapshot.mode !== "lite_frequency_v4_shadow") {
      return response({ ok: false, missing: false, age_ms: null, snapshot: null, error: "wrong_export_namespace" }, 503);
    }
    if (snapshot.dry_run !== true || snapshot.live_enabled !== false
      || snapshot.real_orders_possible !== false || snapshot.live_adapter_present !== false
      || snapshot.kill_switch_engaged !== true || snapshot.fixed_shares !== 5) {
      return response({ ok: false, missing: false, age_ms: null, snapshot: null, error: "unsafe_export_contract" }, 503);
    }
    const generatedTsMs = snapshot.generated_ts_ms;
    if (typeof generatedTsMs !== "number" || !Number.isSafeInteger(generatedTsMs) || generatedTsMs <= 0) {
      return response({ ok: false, missing: false, age_ms: null, snapshot: null, error: "invalid_export_timestamp" }, 503);
    }
    const currentTsMs = Date.now();
    if (generatedTsMs > currentTsMs + MAX_FUTURE_SKEW_MS) {
      return response({ ok: false, missing: false, age_ms: null, snapshot: null, error: "future_export_timestamp" }, 503);
    }
    // A small positive clock skew is tolerated, but never converted into an
    // apparently fresh payload when it exceeds the explicit bound above.
    const ageMs = Math.max(0, currentTsMs - generatedTsMs);
    return response({ ok: true, missing: false, age_ms: ageMs, snapshot });
  } catch (error: unknown) {
    const code = typeof error === "object" && error && "code" in error ? String(error.code) : "read_failed";
    const missing = code === "ENOENT";
    return response({ ok: false, missing, age_ms: null, snapshot: null, error: missing ? "export_missing" : "export_unreadable" }, 503);
  }
}
