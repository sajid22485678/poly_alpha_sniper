from __future__ import annotations

import asyncio
import ast
import json
import os
from pathlib import Path

import pytest

from poly_alpha_sniper.lite_frequency_v4 import bot as bot_module
from poly_alpha_sniper.lite_frequency_v4.config import (
    FREQUENCY_V4_DB_PATH,
    FREQUENCY_V4_EXPORT_DIR,
    FREQUENCY_V4_RUNTIME_DIR,
)
from poly_alpha_sniper.lite_frequency_v4.runtime import (
    LAUNCH_NONCE_ENV,
    MODE,
    MODULE,
    V4RuntimeFiles,
    immutable_safety_state,
)


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_bot_preserves_failed_terminal_state_when_stop_raises():
    class FailingStopEngine:
        def __init__(self):
            self._stopping = asyncio.Event()
            self._critical_evidence_lost_rows = 7

        async def run_until_stopped(self):
            return

        async def stop(self, _reason):
            raise RuntimeError("terminal command failed")

        def _runtime_state(self, state):
            assert state == "FAILED"
            return {
                **immutable_safety_state(),
                "state": state,
                "persistence": {
                    "telemetry": {"true_lost_critical_rows": 7},
                },
            }

    exit_code, final = asyncio.run(
        bot_module._main(None, None, FailingStopEngine()))
    assert exit_code == 1
    assert final is not None
    assert final["state"] == "FAILED"
    assert final["persistence"]["telemetry"][
        "true_lost_critical_rows"] == 7


def test_bot_minimal_fallback_preserves_lifetime_and_new_critical_loss():
    class FailingStateEngine:
        def __init__(self):
            self._stopping = asyncio.Event()
            self.session_id = "failed-session"
            self._critical_evidence_lost_rows = 7
            self._telemetry_lifetime_baseline = {
                "raw_telemetry_loss_count": 68_361,
                "failed_batches": 35,
                "true_lost_critical_rows": 2,
            }
            self._db_path_resolved = "D:/v4/poly_alpha_frequency_v4.db"
            self._runtime_dir_resolved = "D:/v4/runtime"
            self._export_path_resolved = "D:/v4/export/frequency_v4_dashboard.json"
            self._lineage_fingerprint = "a" * 64

        async def run_until_stopped(self):
            return

        async def stop(self, _reason):
            raise RuntimeError("terminal command failed")

        def _runtime_state(self, _state):
            raise RuntimeError("state capture failed")

    exit_code, final = asyncio.run(
        bot_module._main(None, None, FailingStateEngine()))
    assert exit_code == 1
    assert final is not None
    telemetry = final["persistence"]["telemetry"]
    assert telemetry["true_lost_critical_rows"] == 9
    assert telemetry["raw_telemetry_loss_count"] == 68_361
    assert telemetry["failed_batches"] == 35


def test_immutable_runtime_safety_contract_has_no_live_execution_surface():
    assert immutable_safety_state() == {
        "strategy_id": "lite_frequency_v4",
        "mode": "lite_frequency_v4_shadow",
        "dry_run": True,
        "live_enabled": False,
        "real_orders_possible": False,
        "live_adapter_present": False,
        "kill_switch_engaged": True,
        "fixed_shares": 5.0,
        "real_wallet_signing": False,
        "authenticated_trading_client": False,
        "real_order_placement": False,
        "real_order_cancellation": False,
    }


def test_nonce_correlated_lock_heartbeat_stop_and_release(tmp_path, monkeypatch):
    nonce = "0123456789abcdef0123456789abcdef"
    monkeypatch.setenv(LAUNCH_NONCE_ENV, nonce)
    runtime = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
    acquired = runtime.acquire()
    try:
        assert acquired["launch_nonce"] == nonce
        assert acquired["mode"] == MODE
        assert acquired["module"] == MODULE
        assert acquired["dry_run"] is True
        assert acquired["live_enabled"] is False

        # Caller-supplied unsafe values are overwritten by immutable constants.
        published = runtime.publish({
            "dry_run": False,
            "live_enabled": True,
            "real_orders_possible": True,
            "kill_switch_engaged": False,
        })
        assert published["dry_run"] is True
        assert published["live_enabled"] is False
        assert published["real_orders_possible"] is False
        assert published["kill_switch_engaged"] is True
        heartbeat = _read(runtime.heartbeat_path)
        assert heartbeat["launch_nonce"] == nonce
        assert heartbeat["fixed_shares"] == 5.0

        runtime.stop_path.write_text(json.dumps({
            "mode": MODE,
            "module": MODULE,
            "target_pid": runtime.pid,
            "launch_nonce": "f" * 32,
        }), encoding="utf-8")
        assert runtime.stop_requested() is False
        runtime.stop_path.write_text(json.dumps({
            "mode": MODE,
            "module": MODULE,
            "target_pid": runtime.pid,
            "launch_nonce": nonce,
        }), encoding="utf-8")
        assert runtime.stop_requested() is True
    finally:
        runtime.release({"state": "STOPPED"})

    assert not runtime.lock_path.exists()
    assert not runtime.stop_path.exists()
    final = _read(runtime.state_path)
    assert final["running"] is False
    assert final["launch_nonce"] == nonce
    assert final["live_enabled"] is False


def test_clean_release_proves_prior_nonce_absent_for_next_launch(
        tmp_path, monkeypatch):
    nonce_one = "0123456789abcdef0123456789abcdef"
    monkeypatch.setenv(LAUNCH_NONCE_ENV, nonce_one)
    first = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
    first.acquire()
    # A brand-new runtime directory yields no proof at all.
    assert first.proven_absent_launch_nonces == ()
    first.release({"state": "STOPPED"})

    # The releasing process in this test is the test process itself and is
    # still alive; while it lives its nonce must NOT be proven absent.
    same_pid = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
    same_pid.acquire()
    assert nonce_one not in same_pid.proven_absent_launch_nonces
    same_pid.release(None)

    # Simulate the released process having exited: the graceful release
    # removed the lock but left the final running=false state; the next
    # launch must still prove the prior nonce absent so recovery can finish
    # maker observations that the stop left unfinished.
    state_path = tmp_path / "runtime" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.update({"launch_nonce": nonce_one, "running": False,
                  "pid": 999_999_999})
    state_path.write_text(json.dumps(state), encoding="utf-8")
    nonce_two = "fedcba9876543210fedcba9876543210"
    monkeypatch.setenv(LAUNCH_NONCE_ENV, nonce_two)
    second = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
    second.acquire()
    try:
        assert second.proven_absent_launch_nonces == (nonce_one,)
    finally:
        second.release(None)

    # A state that still claims running=true (no clean shutdown) proves
    # nothing, even with a dead PID.
    state = json.loads((tmp_path / "runtime" / "state.json").read_text(
        encoding="utf-8"))
    state.update({"running": True, "launch_nonce": nonce_one,
                  "pid": 999_999_999})
    (tmp_path / "runtime" / "state.json").write_text(
        json.dumps(state), encoding="utf-8")
    third = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
    third.acquire()
    try:
        assert nonce_one not in third.proven_absent_launch_nonces
    finally:
        third.release(None)


def test_verify_ownership_accepts_venv_launcher_pair_and_rejects_orphans(
        tmp_path, monkeypatch):
    monkeypatch.delenv(LAUNCH_NONCE_ENV, raising=False)
    runtime = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
    runtime.acquire()
    try:
        # A Windows venv launcher runs the real interpreter as its child with
        # an identical exact-module command line: two exact processes, both
        # inside the ownership tree, zero orphans. This must be accepted.
        monkeypatch.setattr(runtime, "process_ownership", lambda: {
            "process_ownership_valid": True,
            "exact_v4_processes": 2,
            "owned_v4_processes": 2,
            "orphan_processes": 0,
            "exact_pids": [runtime.pid, runtime.pid + 1],
        })
        verified = runtime.verify_process_ownership()
        assert verified["owned_v4_processes"] == 2

        # Any exact process outside the ownership tree stays fail-closed.
        monkeypatch.setattr(runtime, "process_ownership", lambda: {
            "process_ownership_valid": True,
            "exact_v4_processes": 3,
            "owned_v4_processes": 2,
            "orphan_processes": 1,
            "exact_pids": [runtime.pid, runtime.pid + 1, runtime.pid + 2],
        })
        with pytest.raises(RuntimeError, match="ownership preflight"):
            runtime.verify_process_ownership()
        assert runtime.verified_process_ownership is None

        # Zero exact processes can never validate ownership.
        monkeypatch.setattr(runtime, "process_ownership", lambda: {
            "process_ownership_valid": False,
            "exact_v4_processes": 0,
            "owned_v4_processes": 0,
            "orphan_processes": 0,
            "exact_pids": [],
        })
        with pytest.raises(RuntimeError, match="ownership preflight"):
            runtime.verify_process_ownership()
    finally:
        runtime.release(None)


def test_os_guard_and_process_lock_allow_only_one_owner(tmp_path, monkeypatch):
    monkeypatch.delenv(LAUNCH_NONCE_ENV, raising=False)
    first = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
    first.acquire()
    try:
        second = V4RuntimeFiles(tmp_path / "runtime", repo_root=tmp_path)
        with pytest.raises(RuntimeError, match="guard|active process lock"):
            second.acquire()
        lock = _read(first.lock_path)
        assert lock["pid"] == os.getpid()
        assert lock["launch_nonce"] == first.launch_nonce
        assert len(first.launch_nonce) == 32
    finally:
        first.release()


def test_stale_lock_is_replaced_without_accepting_its_nonce(tmp_path, monkeypatch):
    directory = tmp_path / "runtime"
    directory.mkdir()
    stale_nonce = "1" * 32
    (directory / "process.lock").write_text(json.dumps({
        "pid": 2_147_483_647,
        "process_create_time": 1.0,
        "launch_nonce": stale_nonce,
        "mode": MODE,
    }), encoding="utf-8")
    monkeypatch.delenv(LAUNCH_NONCE_ENV, raising=False)
    runtime = V4RuntimeFiles(directory, repo_root=tmp_path)
    runtime.acquire()
    try:
        lock = _read(runtime.lock_path)
        assert lock["launch_nonce"] == runtime.launch_nonce
        assert lock["launch_nonce"] != stale_nonce
        assert lock["pid"] == os.getpid()
        assert runtime.proven_absent_launch_nonces == (stale_nonce,)
    finally:
        runtime.release()


def test_runtime_and_storage_defaults_do_not_collide_with_v3_or_advanced():
    database = Path(FREQUENCY_V4_DB_PATH).as_posix().lower()
    runtime = Path(FREQUENCY_V4_RUNTIME_DIR).as_posix().lower()
    export = Path(FREQUENCY_V4_EXPORT_DIR).as_posix().lower()
    assert database.endswith("/data/poly_alpha_frequency_v4.db")
    assert runtime.endswith("/runtime/lite_frequency_v4_shadow")
    assert export.endswith("/poly_alpha_frequency_v4")
    for legacy in (
        "poly_alpha_lite.db", "/runtime/lite_shadow", "/runtime/live",
        "/poly_alpha_lite",
    ):
        assert legacy not in database
        assert legacy not in runtime
        assert legacy not in export


def test_engine_and_entrypoint_ast_expose_no_order_cancel_signing_calls():
    root = Path(__file__).resolve().parents[1] / "lite_frequency_v4"
    forbidden_import_fragments = {
        "py_clob_client", "web3", "eth_account", "private_key",
        "wallet", "authenticated",
    }
    forbidden_calls = {
        "place_order", "create_order", "post_order", "cancel_order",
        "cancel_all", "sign", "sign_order", "approve",
    }
    for name in ("engine.py", "bot.py"):
        path = root / name
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        imports = []
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name.lower() for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append(str(node.module or "").lower())
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    calls.append(node.func.id.lower())
                elif isinstance(node.func, ast.Attribute):
                    calls.append(node.func.attr.lower())
        assert not any(fragment in imported for fragment in forbidden_import_fragments
                       for imported in imports)
        assert forbidden_calls.isdisjoint(calls)
