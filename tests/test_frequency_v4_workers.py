"""Dedicated-thread worker coverage for the isolated Frequency V4 lane."""
from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any

import pytest

from poly_alpha_sniper.lite_frequency_v4.store import V4ReadOnlyStore, V4Store
from poly_alpha_sniper.lite_frequency_v4.workers import (
    V4MaintenanceWorker,
    V4ReadWorker,
    V4RuntimeIOWorker,
    V4WorkerJobTimeout,
    V4WorkerNotRunning,
    V4WorkerQueueFull,
)


def _fresh_db(tmp_path: Path, name: str = "frequency-v4-workers.db") -> Path:
    path = tmp_path / name
    store = V4Store(path)
    store.close()
    return path


async def _pong_surrogate(
    *, duration_s: float = 0.18, interval_s: float = 0.005
) -> tuple[int, float]:
    ticks = 0
    largest_gap = 0.0
    prior = time.monotonic()
    deadline = prior + duration_s
    while time.monotonic() < deadline:
        await asyncio.sleep(interval_s)
        current = time.monotonic()
        largest_gap = max(largest_gap, current - prior)
        prior = current
        ticks += 1
    return ticks, largest_gap


@pytest.mark.asyncio
async def test_read_worker_query_apis_and_sync_result(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path).start()
    try:
        assert worker.query_one_sync("SELECT COUNT(*) AS n FROM markets") == {"n": 0}
        assert await worker.query_one("SELECT COUNT(*) AS n FROM entries") == {"n": 0}
        tables = await worker.query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        assert any(row["name"] == "runtime_sessions" for row in tables)
        assert await worker.open_positions() == []
        health = worker.health()
        assert health["completed"] == 4
        assert health["failed"] == 0
        assert health["thread_affine_connection"] is True
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_read_store_is_created_used_and_closed_on_one_thread(tmp_path):
    path = _fresh_db(tmp_path)
    observed: dict[str, int] = {}
    main_thread = threading.get_ident()

    class RecordingReadStore(V4ReadOnlyStore):
        def __init__(self) -> None:
            observed["created"] = threading.get_ident()
            super().__init__(path)

        def close(self) -> None:
            observed["closed"] = threading.get_ident()
            super().close()

    worker = V4ReadWorker(path, store_factory=RecordingReadStore)
    await worker.start_async()
    try:
        result = await worker.run_report(
            lambda store: {
                "used": threading.get_ident(),
                "query_only": int(
                    store.connection.execute("PRAGMA query_only").fetchone()[0]
                ),
            }
        )
        owner = worker.health()["owner_thread_id"]
        assert result == {"used": owner, "query_only": 1}
        assert observed["created"] == owner
        assert owner != main_thread
    finally:
        await worker.stop_async()
    assert observed["closed"] == observed["created"]


@pytest.mark.asyncio
async def test_read_worker_enforces_query_only_and_survives_failure(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path).start()
    try:
        with pytest.raises(sqlite3.OperationalError):
            await worker.run_report(
                lambda store: store.connection.execute(
                    "CREATE TABLE forbidden_write(value INTEGER)"
                ).fetchall()
            )
        assert await worker.query_one("SELECT COUNT(*) AS n FROM markets") == {"n": 0}
        assert worker.health()["failed"] == 1
        assert worker.health()["completed"] == 1
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_read_worker_closes_callable_snapshot_between_jobs(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path).start()

    def leave_read_snapshot(store: V4ReadOnlyStore) -> bool:
        store.connection.execute("BEGIN")
        store.connection.execute("SELECT COUNT(*) FROM markets").fetchone()
        return bool(store.connection.in_transaction)

    try:
        assert await worker.run_report(leave_read_snapshot) is True
        assert await worker.run_report(
            lambda store: bool(store.connection.in_transaction)
        ) is False
        assert worker.health()["snapshot_rollbacks"] == 1
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_slow_read_report_does_not_block_pong_surrogate(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4ReadWorker(path).start()

    def slow_report(store: V4ReadOnlyStore) -> int:
        time.sleep(0.15)
        return int(store.query_one("SELECT COUNT(*) AS n FROM markets")["n"])

    try:
        started = time.monotonic()
        report, pong = await asyncio.gather(
            worker.run_report(slow_report, timeout_s=1.0),
            _pong_surrogate(),
        )
        elapsed = time.monotonic() - started
        ticks, largest_gap = pong
        assert report == 0
        assert elapsed < 0.28
        assert ticks >= 10
        assert largest_gap < 0.06
        assert worker.health()["last_duration_ms"] >= 120
    finally:
        worker.stop()


def test_bounded_queue_rejects_without_blocking_submitter(tmp_path):
    _ = tmp_path
    worker = V4RuntimeIOWorker(queue_capacity=1, default_timeout_s=2.0).start()
    entered = threading.Event()
    release = threading.Event()

    def blocking_job() -> str:
        entered.set()
        assert release.wait(timeout=2.0)
        return "first"

    try:
        first = worker.submit_io(blocking_job)
        assert entered.wait(timeout=1.0)
        second = worker.submit_io(lambda: "second")
        started = time.monotonic()
        with pytest.raises(V4WorkerQueueFull):
            worker.submit_io(lambda: "rejected")
        assert time.monotonic() - started < 0.05
        assert worker.health()["queue_depth"] == 1
        assert worker.health()["rejected_queue_full"] == 1
        release.set()
        assert first.result(timeout=1.0) == "first"
        assert second.result(timeout=1.0) == "second"
    finally:
        release.set()
        worker.stop()


@pytest.mark.asyncio
async def test_runtime_io_timeout_and_failure_leave_loop_and_worker_healthy():
    worker = V4RuntimeIOWorker(default_timeout_s=1.0)
    await worker.start_async()
    try:
        pong_task = asyncio.create_task(_pong_surrogate(duration_s=0.08))
        with pytest.raises(V4WorkerJobTimeout):
            await worker.run_io(time.sleep, 0.15, timeout_s=0.03)
        ticks, largest_gap = await pong_task
        assert ticks >= 5
        assert largest_gap < 0.06
        await asyncio.sleep(0.15)

        def boom() -> None:
            raise RuntimeError("runtime I/O boom")

        with pytest.raises(RuntimeError, match="runtime I/O boom"):
            await worker.run_io(boom)
        owner = await worker.run_io(threading.get_ident)
        assert owner == worker.health()["owner_thread_id"]
        health = worker.health()
        assert health["client_timeouts"] >= 1
        assert health["expired_jobs"] >= 1
        assert health["failed"] >= 2
        assert health["completed"] >= 1
    finally:
        await worker.stop_async()


@pytest.mark.asyncio
async def test_runtime_io_worker_does_not_use_default_executor(monkeypatch):
    worker = V4RuntimeIOWorker()
    await worker.start_async()
    loop = asyncio.get_running_loop()

    def forbidden_executor(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("default executor must not be used")

    monkeypatch.setattr(loop, "run_in_executor", forbidden_executor)
    try:
        main_thread = threading.get_ident()
        owner = await worker.run_io(threading.get_ident)
        assert owner != main_thread
    finally:
        await worker.stop_async()


@pytest.mark.asyncio
async def test_slow_checkpoint_and_retention_do_not_block_pong(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4MaintenanceWorker(path).start()
    threads: list[int] = []

    def slow_checkpoint(store: V4Store) -> tuple[int, int, int]:
        threads.append(threading.get_ident())
        time.sleep(0.12)
        row = store.connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        return int(row[0]), int(row[1]), int(row[2])

    def slow_retention(store: V4Store) -> dict[str, Any]:
        threads.append(threading.get_ident())
        time.sleep(0.12)
        return store.compact_raw_evidence(
            int(time.time() * 1_000),
            retention_ms=60_000,
            batch_size=10,
            run_integrity=False,
        )

    try:
        checkpoint, checkpoint_pong = await asyncio.gather(
            worker.run_checkpoint(slow_checkpoint, timeout_s=1.0),
            _pong_surrogate(duration_s=0.15),
        )
        retention, retention_pong = await asyncio.gather(
            worker.run_retention(slow_retention, timeout_s=1.0),
            _pong_surrogate(duration_s=0.15),
        )
        assert len(checkpoint) == 3
        assert "retention_run_id" in retention
        assert checkpoint_pong[0] >= 8 and checkpoint_pong[1] < 0.06
        assert retention_pong[0] >= 8 and retention_pong[1] < 0.06
        assert set(threads) == {worker.health()["owner_thread_id"]}
        assert worker.health()["last_kind"] == "RETENTION"
        assert worker.health()["max_duration_ms"] >= 100
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_maintenance_store_lifecycle_is_thread_affine(tmp_path):
    path = _fresh_db(tmp_path)
    observed: dict[str, int] = {}
    main_thread = threading.get_ident()

    class RecordingStore(V4Store):
        def __init__(self) -> None:
            observed["created"] = threading.get_ident()
            super().__init__(path, enforce_thread_ownership=True)

        def close(self) -> None:
            observed["closed"] = threading.get_ident()
            super().close()

    worker = V4MaintenanceWorker(path, store_factory=RecordingStore)
    await worker.start_async()
    try:
        owner = await worker.run_maintenance(
            lambda _store: threading.get_ident(), name="owner_probe"
        )
        assert owner == worker.health()["owner_thread_id"]
        assert owner == observed["created"]
        assert owner != main_thread
        checkpoint = await worker.checkpoint()
        assert checkpoint["mode"] == "PASSIVE"
    finally:
        await worker.stop_async()
    assert observed["closed"] == observed["created"]


@pytest.mark.asyncio
async def test_maintenance_failure_propagates_and_worker_recovers(tmp_path):
    path = _fresh_db(tmp_path)
    worker = V4MaintenanceWorker(path).start()

    def boom(_store: V4Store) -> None:
        raise RuntimeError("retention boom")

    try:
        with pytest.raises(RuntimeError, match="retention boom"):
            await worker.run_retention(boom)
        result = await worker.checkpoint("PASSIVE")
        assert result["mode"] == "PASSIVE"
        assert worker.health()["failed"] == 1
        assert worker.health()["completed"] == 1
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_graceful_async_stop_drains_accepted_runtime_io_jobs():
    worker = V4RuntimeIOWorker(queue_capacity=2).start()
    entered = threading.Event()
    release = threading.Event()
    order: list[str] = []

    def first_job() -> str:
        entered.set()
        assert release.wait(timeout=2.0)
        order.append("first")
        return "first"

    def second_job() -> str:
        order.append("second")
        return "second"

    first = worker.submit_io(first_job)
    deadline = time.monotonic() + 1.0
    while not entered.is_set() and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    assert entered.is_set()
    second = worker.submit_io(second_job)
    stop_task = asyncio.create_task(worker.stop_async(timeout_s=2.0))
    await asyncio.sleep(0.02)
    with pytest.raises(V4WorkerNotRunning):
        worker.submit_io(lambda: "late")
    release.set()
    await stop_task
    assert first.result() == "first"
    assert second.result() == "second"
    assert order == ["first", "second"]
    assert worker.health()["state"] == "STOPPED"
