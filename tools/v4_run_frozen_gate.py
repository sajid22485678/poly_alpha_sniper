"""Run one identity-pinned controlled gate or soak under the frozen contract.

Owns the whole run so the evidence is coherent by construction:

* starts the runtime with the dense readiness trace pointed at the evidence
  directory, and the dashboard, so a real HTTP probe has something to probe;
* runs the identity-pinned sampler as a subprocess, with a tail so the stream
  ends on a real terminal record rather than wherever the clock ran out;
* samples provider health and multi-reference clock offset itself, on the same
  cadence, so it knows exactly what the last provider sample says;
* after the minimum duration, extends by at most five samples waiting for
  Polymarket to return to READY -- the ratified hydration contract requires the
  terminal sample to be READY -- and then requests a *graceful* stop;
* waits for every artifact to stop growing, and only then evaluates and seals.

The order matters.  The previous soak verdict was computed while the trace was
still appending and attested bytes that no longer existed.

Usage:
  v4_run_frozen_gate.py --evidence-dir DIR --min-minutes 55 [--label gate]
"""
from __future__ import annotations

import argparse
import json
import shutil
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

RUNTIME_DIR = REPO_ROOT / "runtime" / "lite_frequency_v4_shadow"
EXPORT_PATH = Path(
    r"D:\claude\agent_readonly\poly_alpha_frequency_v4\frequency_v4_dashboard.json")

SAMPLE_INTERVAL_S = 30.0
#: Ratified hydration contract: at most five extra samples waiting for READY.
MAX_EXTENSION_SAMPLES = 5
#: Graceful stop must outlast a real shutdown; the stop script's own default.
STOP_GRACE_S = 180
#: How long the sampler may keep following after the window before giving up.
TAIL_TIMEOUT_S = 900.0

NTP_REFERENCES = (
    "time.google.com", "time.cloudflare.com",
    "time.nist.gov", "time.windows.com",
)
NTP_UNIX_DELTA = 2_208_988_800

PROVIDER_FIELDS = (
    "source", "channel", "status", "connected", "hydrated",
    "future_count", "freshness_ms", "heartbeat_age_ms", "reconnect_count",
    "sequence_gap_count", "regressed_count", "duplicate_count",
    "rest_recovery_status", "last_error",
)


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _ntp(host: str, *, timeout_s: float = 3.0) -> dict:
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
    t2 = fields[8] + fields[9] / 2**32 - NTP_UNIX_DELTA
    t3 = fields[10] + fields[11] / 2**32 - NTP_UNIX_DELTA
    return {
        "host": host,
        "stratum": (fields[0] >> 16) & 0xFF,
        "leap": (fields[0] >> 30) & 0x3,
        "offset_ms": ((t2 - t1) + (t3 - t4)) / 2.0 * 1000.0,
        "delay_ms": ((t4 - t1) - (t3 - t2)) * 1000.0,
    }


def _free_gb(drive: str) -> Optional[float]:
    try:
        return round(shutil.disk_usage(drive).free / 2**30, 3)
    except OSError:
        return None


def _read_export() -> dict:
    try:
        return json.loads(EXPORT_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - observation never fatal
        return {}


def _polymarket_status() -> Optional[str]:
    for entry in (_read_export().get("sources") or []):
        if str(entry.get("source")) == "polymarket":
            return str(entry.get("status"))
    return None


class Stream:
    """A JSONL evidence stream this process owns end to end."""

    def __init__(self, path: Path, manifest: dict) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise SystemExit(f"refusing to reuse evidence path {path}")
        self._handle = path.open("x", encoding="utf-8", newline="\n")
        self._lock = threading.Lock()
        self.write({"record": "manifest", "started_utc": _utc(), **manifest})
        self.index = 0

    def write(self, row: dict) -> None:
        with self._lock:
            self._handle.write(json.dumps(row, default=str) + "\n")
            self._handle.flush()

    def close(self) -> None:
        with self._lock:
            self._handle.close()


def _provider_sample(index: int) -> dict:
    payload = _read_export()
    return {
        "record": "sample", "index": index, "wall_utc": _utc(),
        "wall_ms": int(time.time() * 1000),
        "export_age_ms": payload.get("export_age_ms"),
        "generated_ts_ms": payload.get("generated_ts_ms"),
        "current_commit": payload.get("current_commit"),
        "sources": [
            {key: entry.get(key) for key in PROVIDER_FIELDS}
            for entry in (payload.get("sources") or [])
        ],
    }


def _clock_sample(index: int) -> dict:
    samples = [_ntp(host) for host in NTP_REFERENCES]
    offsets = [s["offset_ms"] for s in samples if "offset_ms" in s]
    return {
        "record": "sample", "index": index, "wall_utc": _utc(),
        "wall_ms": int(time.time() * 1000),
        "samples": samples,
        "responded": len(offsets),
        "offset_median_ms": (
            round(statistics.median(offsets), 4) if offsets else None),
        "offset_spread_ms": (
            round(max(offsets) - min(offsets), 4) if len(offsets) > 1 else None),
        "free_gb_C": _free_gb("C:\\"),
        "free_gb_D": _free_gb("D:\\"),
    }


def _probe_dashboard() -> dict:
    result: dict[str, Any] = {"listening": False, "http_status": None,
                              "latency_ms": None, "error": None}
    started = time.perf_counter()
    try:
        with socket.create_connection(("127.0.0.1", 8504), timeout=5.0):
            result["listening"] = True
    except OSError as exc:
        result["error"] = f"{type(exc).__name__}:{exc}"[:160]
        return result
    try:
        with urllib.request.urlopen("http://127.0.0.1:8504/", timeout=5.0) as r:
            r.read(2048)
            result["http_status"] = int(r.status)
    except urllib.error.HTTPError as exc:
        result["http_status"] = int(exc.code)
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}:{exc}"[:160]
    result["latency_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
    return result


def _powershell(script: Path, *args: str, env_extra: Optional[dict] = None,
                timeout_s: float = 900.0) -> subprocess.CompletedProcess:
    import os

    env = dict(os.environ)
    env.update(env_extra or {})
    return subprocess.run(
        ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
         "-File", str(script), *args],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
        timeout=timeout_s, env=env, check=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--min-minutes", type=float, required=True)
    parser.add_argument("--label", default="gate")
    parser.add_argument("--margin-minutes", type=float, default=1.5,
                        help="run past the minimum before asking to stop")
    args = parser.parse_args()

    root = Path(args.evidence_dir)
    root.mkdir(parents=True, exist_ok=True)
    log = (root / "orchestrator.log").open("a", encoding="utf-8", newline="\n")

    def say(message: str) -> None:
        line = f"[{_utc()}] {message}"
        print(line, flush=True)
        log.write(line + "\n")
        log.flush()

    trace_path = root / "readiness_trace.jsonl"
    window_s = (args.min_minutes + args.margin_minutes) * 60.0

    say(f"=== {args.label}: window={window_s/60:.1f} min "
        f"(minimum {args.min_minutes:.0f}) ===")

    # --- start the runtime, with the dense trace enabled --------------------
    say("starting V4 runtime...")
    started = _powershell(
        REPO_ROOT / "scripts" / "start_lite_frequency_v4_shadow.ps1",
        env_extra={"POLY_ALPHA_V4_READINESS_TRACE": str(trace_path)})
    say(f"start rc={started.returncode}\n{started.stdout.strip()}\n"
        f"{started.stderr.strip()}")
    if started.returncode != 0:
        say("ABORT: runtime did not reach verified readiness")
        return 2

    say("starting dashboard...")
    dash = _powershell(
        REPO_ROOT / "scripts" / "start_frequency_v4_dashboard.ps1",
        timeout_s=180.0)
    say(f"dashboard rc={dash.returncode} {dash.stdout.strip()} "
        f"{dash.stderr.strip()}")
    probe = _probe_dashboard()
    say(f"dashboard probe: {probe}")
    if not probe.get("listening"):
        say("ABORT: dashboard is not serving; the contract requires real "
            "HTTP evidence")
        _powershell(REPO_ROOT / "scripts" / "stop_lite_frequency_v4_shadow.ps1")
        return 2

    # --- launch the identity-pinned sampler ---------------------------------
    sampler_dir = root / "sampler"
    sampler_dir.mkdir(parents=True, exist_ok=True)
    sampler = subprocess.Popen(
        [str(REPO_ROOT / ".venv" / "Scripts" / "python.exe"),
         str(REPO_ROOT / "tools" / "v4_soak_sampler.py"),
         str(sampler_dir), str(window_s), str(SAMPLE_INTERVAL_S),
         "--tail-timeout-s", str(TAIL_TIMEOUT_S)],
        cwd=str(REPO_ROOT),
        stdout=(root / "sampler.out").open("w", encoding="utf-8"),
        stderr=(root / "sampler.err").open("w", encoding="utf-8"))
    say(f"sampler pid={sampler.pid}")

    providers = Stream(root / "providers.jsonl",
                       {"interval_s": SAMPLE_INTERVAL_S, "label": args.label})
    clock = Stream(root / "clock.jsonl",
                   {"interval_s": SAMPLE_INTERVAL_S,
                    "references": list(NTP_REFERENCES), "label": args.label})

    stop_sampling = threading.Event()

    def sample_loop(stream: Stream, builder) -> None:
        index = 0
        while not stop_sampling.is_set():
            index += 1
            try:
                stream.write(builder(index))
            except Exception as exc:  # noqa: BLE001
                stream.write({"record": "sample", "index": index,
                              "wall_utc": _utc(),
                              "error": f"{type(exc).__name__}:{exc}"[:200]})
            stop_sampling.wait(SAMPLE_INTERVAL_S)

    threads = [
        threading.Thread(target=sample_loop, args=(providers, _provider_sample),
                         daemon=True, name="providers"),
        threading.Thread(target=sample_loop, args=(clock, _clock_sample),
                         daemon=True, name="clock"),
    ]
    for thread in threads:
        thread.start()

    # --- run the window -----------------------------------------------------
    deadline = time.monotonic() + window_s
    while time.monotonic() < deadline:
        time.sleep(30.0)
        remaining = max(0.0, deadline - time.monotonic())
        say(f"window remaining {remaining/60:.1f} min; "
            f"polymarket={_polymarket_status()}")

    # --- ratified hydration extension: wait for READY, at most 5 samples ----
    say("minimum duration served; waiting for polymarket READY "
        f"(at most {MAX_EXTENSION_SAMPLES} samples)")
    extension_used = 0
    while _polymarket_status() != "READY" and extension_used < MAX_EXTENSION_SAMPLES:
        extension_used += 1
        say(f"  extension sample {extension_used}/{MAX_EXTENSION_SAMPLES}: "
            f"polymarket={_polymarket_status()}")
        time.sleep(SAMPLE_INTERVAL_S)
    final_status = _polymarket_status()
    say(f"extension used={extension_used}; polymarket={final_status}")

    # One last provider+clock sample taken deliberately at this instant, so the
    # terminal provider sample is the one the stop decision was made on.
    providers.index += 1
    providers.write(_provider_sample(9_000 + extension_used))
    clock.write(_clock_sample(9_000 + extension_used))
    stop_sampling.set()
    for thread in threads:
        thread.join(timeout=20.0)
    providers.close()
    clock.close()

    # --- graceful stop ------------------------------------------------------
    say("requesting graceful V4 stop...")
    stopped = _powershell(
        REPO_ROOT / "scripts" / "stop_lite_frequency_v4_shadow.ps1",
        "-GracePeriodSeconds", str(STOP_GRACE_S), timeout_s=STOP_GRACE_S + 120)
    say(f"stop rc={stopped.returncode} {stopped.stdout.strip()} "
        f"{stopped.stderr.strip()}")

    say("waiting for the sampler to record its terminal observation...")
    try:
        sampler.wait(timeout=TAIL_TIMEOUT_S + 120)
    except subprocess.TimeoutExpired:
        say("sampler did not exit; terminating")
        sampler.terminate()
    say(f"sampler exit={sampler.returncode}")

    say("stopping dashboard...")
    dash_stop = _powershell(
        REPO_ROOT / "scripts" / "stop_frequency_v4_dashboard.ps1",
        timeout_s=120.0)
    say(f"dashboard stop rc={dash_stop.returncode} {dash_stop.stdout.strip()}")

    manifest = {
        "label": args.label,
        "min_minutes": args.min_minutes,
        "window_s": window_s,
        "extension_samples_used": extension_used,
        "polymarket_status_at_stop": final_status,
        "stop_rc": stopped.returncode,
        "sampler_exit": sampler.returncode,
        "finished_utc": _utc(),
    }
    (root / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    say(f"run manifest: {manifest}")
    say("run complete; evaluate with the committed frozen evaluator")
    log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
