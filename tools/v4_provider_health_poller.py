"""Poll published provider health into an evidence stream.

Reads the dashboard export the runtime already writes.  Deliberately an
observer: it opens no socket to a provider, touches no database, and writes only
its own output file.

The frozen contract judges Polymarket hydration from this stream, so each entry
carries not only the status but the fields that let a HYDRATING episode be
*attributed*: reconnect count, sequence gaps, freshness and the last error.  A
bound with no attribution would say an episode was short without saying whether
it was a market-window rotation or a transport failure.

Usage: v4_provider_health_poller.py --duration-s N --interval-s N --out PATH
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_EXPORT = Path(
    r"D:\claude\agent_readonly\poly_alpha_frequency_v4\frequency_v4_dashboard.json")

FIELDS = (
    "source", "channel", "status", "connected", "hydrated",
    "future_count", "freshness_ms", "heartbeat_age_ms", "reconnect_count",
    "sequence_gap_count", "regressed_count", "duplicate_count",
    "rest_recovery_status", "last_error",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-s", type=float, required=True)
    parser.add_argument("--interval-s", type=float, default=30.0)
    parser.add_argument("--out", required=True)
    parser.add_argument("--export", default=str(DEFAULT_EXPORT))
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        print(f"refusing to reuse output path {out}")
        return 2
    export = Path(args.export)
    deadline = time.monotonic() + args.duration_s
    index = 0

    with out.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({
            "record": "manifest",
            "started_utc": datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"),
            "export_path": str(export),
            "interval_s": args.interval_s,
            "duration_s": args.duration_s,
        }) + "\n")
        handle.flush()
        while time.monotonic() < deadline:
            index += 1
            row: dict = {
                "record": "sample",
                "index": index,
                "wall_utc": datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"),
                "wall_ms": int(time.time() * 1000),
            }
            try:
                payload = json.loads(export.read_text(encoding="utf-8"))
                row["export_age_ms"] = payload.get("export_age_ms")
                row["generated_ts_ms"] = payload.get("generated_ts_ms")
                row["current_commit"] = payload.get("current_commit")
                row["sources"] = [
                    {key: entry.get(key) for key in FIELDS}
                    for entry in (payload.get("sources") or [])
                ]
            except Exception as exc:  # noqa: BLE001 - observation never fatal
                row["error"] = f"{type(exc).__name__}:{exc}"[:200]
                row["sources"] = []
            handle.write(json.dumps(row) + "\n")
            handle.flush()
            statuses = ",".join(
                f"{e.get('source')}:{e.get('status')}" for e in row["sources"])
            print(f"[{index}] {statuses}", flush=True)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(args.interval_s, remaining))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
