from __future__ import annotations

import ast
import json
import os
from pathlib import Path

import pytest

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
