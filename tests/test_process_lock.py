import json
import os

import pytest

from poly_alpha_sniper.core.process_lock import AlreadyRunning, ProcessLock


def test_acquire_and_release(tmp_path):
    lock = ProcessLock(str(tmp_path / "live.lock"))
    lock.acquire("shadow_live")
    assert (tmp_path / "live.lock").exists()
    lock.release()
    assert not (tmp_path / "live.lock").exists()


def test_duplicate_live_instance_blocked(tmp_path):
    path = tmp_path / "live.lock"
    # simulate ANOTHER alive process holding a live lock (use our own pid as "alive")
    path.write_text(json.dumps({"pid": os.getpid() + 0, "mode": "live_micro"}))
    lock = ProcessLock(str(path))
    # our own pid is treated as self -> allowed; use a definitely-other alive pid: parent pid
    other = os.getppid()
    path.write_text(json.dumps({"pid": other, "mode": "live_micro"}))
    with pytest.raises(AlreadyRunning):
        lock.acquire("live_micro")
    with pytest.raises(AlreadyRunning):
        lock.acquire("shadow_live")  # live lock blocks everything


def test_stale_lock_cleared(tmp_path):
    path = tmp_path / "live.lock"
    path.write_text(json.dumps({"pid": 999999999, "mode": "live_micro"}))  # dead pid
    lock = ProcessLock(str(path))
    lock.acquire("shadow_live")  # should clear stale lock and acquire
    lock.release()


def test_reacquire_after_release(tmp_path):
    path = str(tmp_path / "live.lock")
    a = ProcessLock(path)
    a.acquire("shadow_live")
    a.release()
    b = ProcessLock(path)
    b.acquire("shadow_live")
    b.release()
