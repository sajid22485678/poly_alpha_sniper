"""Measure host clock offset and jitter against several independent references.

W32Time reporting "synchronized" is the service's own opinion.  This asks the
references directly, over raw SNTP, from more than one operator, so a single
misbehaving peer cannot make the host look healthy.

Usage: v4_clock_probe.py --duration-s N --interval-s N --out PATH [--summary PATH]
"""
from __future__ import annotations

import argparse
import json
import shutil
import socket
import statistics
import struct
import time
from datetime import datetime, timezone
from pathlib import Path

#: Deliberately different operators: Google, Cloudflare, NIST, Microsoft.
REFERENCES = (
    "time.google.com",
    "time.cloudflare.com",
    "time.nist.gov",
    "time.windows.com",
)

#: Seconds between the NTP epoch (1900-01-01) and the Unix epoch (1970-01-01).
NTP_UNIX_DELTA = 2_208_988_800


def _free_gb(drive: str) -> float | None:
    try:
        return round(shutil.disk_usage(drive).free / 2**30, 3)
    except OSError:
        return None


def query(host: str, *, timeout_s: float = 3.0) -> dict:
    """One SNTP exchange; returns offset in milliseconds, or an error."""

    packet = b"\x1b" + 47 * b"\0"
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout_s)
    try:
        t1 = time.time()
        sock.sendto(packet, (host, 123))
        data, _ = sock.recvfrom(48)
        t4 = time.time()
    except OSError as exc:
        return {"host": host, "error": f"{type(exc).__name__}:{exc}"[:120]}
    finally:
        sock.close()
    if len(data) < 48:
        return {"host": host, "error": f"short reply ({len(data)} bytes)"}
    fields = struct.unpack("!12I", data[:48])
    # Receive (t2) and transmit (t3) timestamps from the server.
    t2 = fields[8] + fields[9] / 2**32 - NTP_UNIX_DELTA
    t3 = fields[10] + fields[11] / 2**32 - NTP_UNIX_DELTA
    stratum = (fields[0] >> 16) & 0xFF
    leap = (fields[0] >> 30) & 0x3
    return {
        "host": host,
        "stratum": stratum,
        "leap": leap,
        # Standard NTP offset and round-trip delay.
        "offset_ms": ((t2 - t1) + (t3 - t4)) / 2.0 * 1000.0,
        "delay_ms": ((t4 - t1) - (t3 - t2)) * 1000.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-s", type=float, default=360.0)
    parser.add_argument("--interval-s", type=float, default=20.0)
    parser.add_argument("--out", required=True)
    parser.add_argument("--summary")
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration_s
    index = 0
    medians: list[float] = []

    with out.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({
            "record": "manifest",
            "started_utc": datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"),
            "references": list(REFERENCES),
            "interval_s": args.interval_s,
            "duration_s": args.duration_s,
        }) + "\n")
        handle.flush()
        while time.monotonic() < deadline:
            index += 1
            samples = [query(host) for host in REFERENCES]
            offsets = [s["offset_ms"] for s in samples if "offset_ms" in s]
            row = {
                "record": "sample",
                "index": index,
                "wall_utc": datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"),
                "samples": samples,
                "responded": len(offsets),
                "offset_median_ms": (
                    round(statistics.median(offsets), 4) if offsets else None),
                "offset_spread_ms": (
                    round(max(offsets) - min(offsets), 4)
                    if len(offsets) > 1 else None),
                # Measured, not left null: the contract checks free space from
                # this stream, and a null would let it pass by default.
                "free_gb_C": _free_gb("C:\\"),
                "free_gb_D": _free_gb("D:\\"),
            }
            if row["offset_median_ms"] is not None:
                medians.append(row["offset_median_ms"])
            handle.write(json.dumps(row) + "\n")
            handle.flush()
            print(f"[{index}] responded={row['responded']}/{len(REFERENCES)} "
                  f"median={row['offset_median_ms']}ms "
                  f"spread={row['offset_spread_ms']}ms", flush=True)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(args.interval_s, remaining))

    jumps = [abs(b - a) for a, b in zip(medians, medians[1:])]
    summary = {
        "samples": index,
        "references": list(REFERENCES),
        "median_offsets_ms": medians,
        "max_abs_median_ms": round(max((abs(v) for v in medians), default=0.0), 4),
        "max_jump_ms": round(max(jumps, default=0.0), 4),
        "stdev_ms": round(statistics.pstdev(medians), 4) if len(medians) > 1 else 0.0,
        "finished_utc": datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"),
    }
    text = json.dumps(summary, indent=2)
    if args.summary:
        Path(args.summary).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
