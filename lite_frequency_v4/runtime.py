"""Process ownership and immutable safety state for Frequency V4.

This namespace is deliberately independent from both ``core.runtime_state``
and the existing ``lite.lite_bot`` runtime files.  It owns only the caller
supplied V4 runtime directory and never exposes an execution adapter.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional


MODE = "lite_frequency_v4_shadow"
STRATEGY_ID = "lite_frequency_v4"
MODULE = "lite_frequency_v4.bot"
LAUNCH_NONCE_ENV = "POLY_ALPHA_FREQUENCY_V4_LAUNCH_NONCE"
FIXED_SHARES = 5.0


def now_ms() -> int:
    return int(time.time() * 1000)


def current_commit(root: Optional[Path] = None) -> str:
    checkout = root or Path(__file__).resolve().parents[1]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=checkout, capture_output=True,
            text=True, timeout=3, check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN"
    value = result.stdout.strip().lower()
    return value if len(value) == 40 and all(c in "0123456789abcdef" for c in value) else "UNKNOWN"


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil

        return bool(psutil.pid_exists(int(pid)))
    except Exception:  # pragma: no cover - psutil is a declared dependency
        try:
            os.kill(int(pid), 0)
            return True
        except OSError:
            return False


def process_create_time(pid: int) -> Optional[float]:
    try:
        import psutil

        return float(psutil.Process(int(pid)).create_time())
    except Exception:
        return None


def _valid_nonce(value: str) -> bool:
    return len(value) == 32 and all(char in "0123456789abcdefABCDEF" for char in value)


def immutable_safety_state() -> dict[str, Any]:
    return {
        "strategy_id": STRATEGY_ID,
        "mode": MODE,
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "kill_switch_engaged": True,
        "fixed_shares": FIXED_SHARES,
        "real_wallet_signing": False,
        "authenticated_trading_client": False,
        "real_order_placement": False,
        "real_order_cancellation": False,
    }


class V4RuntimeFiles:
    """Nonce-correlated V4 process lock, state, heartbeat, and stop request."""

    def __init__(self, runtime_dir: str | Path, *, repo_root: Optional[Path] = None):
        self.directory = Path(runtime_dir).resolve()
        self.repo_root = (repo_root or Path(__file__).resolve().parents[1]).resolve()
        self.guard_path = self.directory / "process.guard"
        self.lock_path = self.directory / "process.lock"
        self.state_path = self.directory / "state.json"
        self.heartbeat_path = self.directory / "heartbeat.json"
        self.stop_path = self.directory / "stop.request"
        self.pid = os.getpid()
        self.started_ts_ms = now_ms()
        supplied = str(os.environ.get(LAUNCH_NONCE_ENV, ""))
        self.launch_nonce = supplied.lower() if _valid_nonce(supplied) else uuid.uuid4().hex
        self.process_create_time = process_create_time(self.pid)
        self.commit = current_commit(self.repo_root)
        self._guard_fd: Optional[int] = None
        self._held = False

    def _acquire_guard(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.guard_path, os.O_CREAT | os.O_RDWR)
        try:
            if os.path.getsize(self.guard_path) == 0:
                os.write(fd, b"\0")
            os.lseek(fd, 0, os.SEEK_SET)
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - Windows is the operational target
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as exc:
            os.close(fd)
            raise RuntimeError("Frequency V4 OS process guard is already held") from exc
        self._guard_fd = fd

    def _release_guard(self) -> None:
        fd, self._guard_fd = self._guard_fd, None
        if fd is None:
            return
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:  # pragma: no cover
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def acquire(self) -> dict[str, Any]:
        try:
            self._acquire_guard()
            if self.lock_path.exists():
                existing = self._read_json(self.lock_path)
                existing_pid = int(existing.get("pid") or 0)
                recorded_create = existing.get("process_create_time")
                actual_create = process_create_time(existing_pid) if pid_alive(existing_pid) else None
                same_process = bool(
                    actual_create is not None
                    and (recorded_create is None or abs(float(recorded_create) - actual_create) < 0.01)
                )
                if same_process:
                    raise RuntimeError(
                        f"Frequency V4 already has an active process lock (pid={existing_pid})"
                    )
                self.lock_path.unlink(missing_ok=True)
            self.stop_path.unlink(missing_ok=True)
            payload = {
                **immutable_safety_state(),
                "pid": self.pid,
                "module": MODULE,
                "launch_nonce": self.launch_nonce,
                "started_ts_ms": self.started_ts_ms,
                "process_create_time": self.process_create_time,
                "current_commit": self.commit,
            }
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, encoded)
            finally:
                os.close(fd)
            self._held = True
            return payload
        except Exception:
            self._release_guard()
            raise

    def publish(self, state: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        timestamp = now_ms()
        payload = {
            **(state or {}),
            **immutable_safety_state(),
            "schema_version": 1,
            "running": True,
            "pid": self.pid,
            "module": MODULE,
            "launch_nonce": self.launch_nonce,
            "process_create_time": self.process_create_time,
            "started_ts_ms": self.started_ts_ms,
            "heartbeat_ts_ms": timestamp,
            "current_commit": self.commit,
        }
        atomic_json(self.state_path, payload)
        atomic_json(
            self.heartbeat_path,
            {
                **immutable_safety_state(),
                "ts_ms": timestamp,
                "pid": self.pid,
                "module": MODULE,
                "launch_nonce": self.launch_nonce,
                "current_commit": self.commit,
            },
        )
        return payload

    def stop_requested(self) -> bool:
        request = self._read_json(self.stop_path)
        return bool(
            request.get("mode") == MODE
            and request.get("module") == MODULE
            and int(request.get("target_pid") or 0) == self.pid
            and str(request.get("launch_nonce") or "").lower() == self.launch_nonce
        )

    def process_ownership(self) -> dict[str, Any]:
        """Inspect exact v4 module processes without touching other lanes."""

        exact: list[int] = []
        parents: dict[int, int] = {}
        try:
            import psutil

            for process in psutil.process_iter(["pid", "ppid", "cmdline"]):
                try:
                    command = [str(part) for part in (process.info.get("cmdline") or [])]
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    continue
                matches = [index for index, part in enumerate(command) if part == "-m"]
                if any(index + 1 < len(command)
                       and command[index + 1] == MODULE
                       and index + 2 == len(command) for index in matches):
                    pid = int(process.info["pid"])
                    exact.append(pid)
                    parents[pid] = int(process.info.get("ppid") or 0)
        except Exception:
            exact = [self.pid] if pid_alive(self.pid) else []
        lock = self._read_json(self.lock_path)
        owner_valid = bool(
            self._held
            and int(lock.get("pid") or 0) == self.pid
            and str(lock.get("launch_nonce") or "").lower() == self.launch_nonce
            and str(lock.get("mode") or "") == MODE
            and self.pid in exact
        )
        owned = {self.pid} if owner_valid else set()
        changed = True
        while changed:
            changed = False
            for pid in exact:
                if pid in owned or parents.get(pid) in owned:
                    if pid not in owned:
                        owned.add(pid)
                        changed = True
                elif pid in parents.values():
                    children = {child for child, parent in parents.items() if parent == pid}
                    if children & owned:
                        owned.add(pid)
                        changed = True
        return {
            "process_ownership_valid": owner_valid,
            "exact_v4_processes": len(exact),
            "owned_v4_processes": len(owned),
            "orphan_processes": len([pid for pid in exact if pid not in owned]),
            "exact_pids": sorted(exact),
        }

    def release(self, state: Optional[dict[str, Any]] = None) -> None:
        if state is not None:
            try:
                final = {
                    **state,
                    **immutable_safety_state(),
                    "schema_version": 1,
                    "running": False,
                    "pid": self.pid,
                    "module": MODULE,
                    "launch_nonce": self.launch_nonce,
                    "process_create_time": self.process_create_time,
                    "started_ts_ms": self.started_ts_ms,
                    "heartbeat_ts_ms": now_ms(),
                    "current_commit": self.commit,
                }
                atomic_json(self.state_path, final)
            except OSError:
                pass
        if self._held:
            existing = self._read_json(self.lock_path)
            if (
                int(existing.get("pid") or 0) == self.pid
                and str(existing.get("launch_nonce") or "").lower() == self.launch_nonce
                and existing.get("mode") == MODE
            ):
                self.lock_path.unlink(missing_ok=True)
        request = self._read_json(self.stop_path)
        if (
            int(request.get("target_pid") or 0) == self.pid
            and str(request.get("launch_nonce") or "").lower() == self.launch_nonce
        ):
            self.stop_path.unlink(missing_ok=True)
        self._held = False
        self._release_guard()

    def __enter__(self) -> "V4RuntimeFiles":
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()
