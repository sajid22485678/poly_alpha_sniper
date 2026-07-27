"""Thread-affine, bounded SQLite workers for the isolated Frequency V4 lane.

The asyncio event loop must never own or share a persistent SQLite connection.
These workers create, use, and close their connection on exactly one dedicated
thread.  Results are delivered with ``concurrent.futures.Future`` objects, which
can be consumed synchronously or wrapped by asyncio without using its default
executor.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
import inspect
from pathlib import Path
import queue
import sqlite3
import threading
import time
from typing import Any, Callable, Generic, Iterable, Optional, TypeVar, cast

from .reader_diag import READER_DIAGNOSTICS
from .store import V4ReadOnlyStore, V4Store


StoreT = TypeVar("StoreT")
ResultT = TypeVar("ResultT")


class V4WorkerError(RuntimeError):
    """Base error for the isolated V4 database workers."""


class V4WorkerNotRunning(V4WorkerError):
    """Raised when work is submitted outside the RUNNING lifecycle state."""


class V4WorkerQueueFull(V4WorkerError):
    """Raised immediately when the bounded worker queue has no free slot."""


class V4WorkerStartupError(V4WorkerError):
    """Raised when a worker cannot create its thread-owned store."""


class V4WorkerJobTimeout(TimeoutError, V4WorkerError):
    """Raised when a queued or running worker job exceeds its deadline."""


class V4WorkerShutdownTimeout(TimeoutError, V4WorkerError):
    """Raised when graceful worker shutdown does not finish in time."""


class V4WorkerInvariantError(V4WorkerError):
    """Raised when a job attempts to violate connection-ownership invariants."""


@dataclass(slots=True)
class _WorkItem:
    sequence: int
    kind: str
    name: str
    operation: Callable[[Any], Any]
    future: Future[Any]
    enqueued_monotonic: float
    deadline_monotonic: Optional[float]


_STOP = object()


def _wall_ms() -> int:
    return int(time.time() * 1_000)


def _callable_name(operation: Callable[..., Any]) -> str:
    return str(
        getattr(operation, "__qualname__", None)
        or getattr(operation, "__name__", None)
        or type(operation).__name__
    )


def _consume_async_result(future: asyncio.Future[Any]) -> None:
    """Retrieve a late result after a caller-side timeout to avoid noisy logs."""

    if future.cancelled():
        return
    try:
        future.exception()
    except (asyncio.CancelledError, Exception):
        pass


class _DedicatedStoreWorker(Generic[StoreT]):
    """One bounded queue, one persistent store, and one owning thread."""

    def __init__(
        self,
        *,
        worker_name: str,
        worker_kind: str,
        queue_capacity: int,
        heartbeat_interval_s: float,
        default_timeout_s: Optional[float],
        store_factory: Callable[[], StoreT],
    ) -> None:
        if isinstance(queue_capacity, bool) or not 1 <= int(queue_capacity) <= 1_000_000:
            raise ValueError("queue_capacity must be within [1, 1000000]")
        if heartbeat_interval_s <= 0:
            raise ValueError("heartbeat_interval_s must be positive")
        if default_timeout_s is not None and default_timeout_s <= 0:
            raise ValueError("default_timeout_s must be positive or None")

        self.worker_name = str(worker_name)
        self.worker_kind = str(worker_kind)
        self.queue_capacity = int(queue_capacity)
        self.heartbeat_interval_s = float(heartbeat_interval_s)
        self.default_timeout_s = (
            float(default_timeout_s) if default_timeout_s is not None else None
        )
        self._store_factory = store_factory
        self._queue: queue.Queue[_WorkItem | object] = queue.Queue(
            maxsize=self.queue_capacity
        )
        self._lock = threading.RLock()
        self._stop_lock = threading.Lock()
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._state = "NEW"
        self._accepting = False
        self._stop_enqueued = False
        self._startup_error: Optional[BaseException] = None
        self._owner_thread_id: Optional[int] = None
        self._sequence = 0

        self._submitted = 0
        self._started = 0
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        self._rejected = 0
        self._client_timeouts = 0
        self._expired_jobs = 0
        self._snapshot_rollbacks = 0
        self._queue_high_water = 0
        self._heartbeat_ts_ms = 0
        self._heartbeat_monotonic = 0.0
        self._current_job: Optional[str] = None
        self._current_kind: Optional[str] = None
        self._current_started_monotonic = 0.0
        self._last_job: Optional[str] = None
        self._last_kind: Optional[str] = None
        self._last_duration_ms = 0.0
        self._max_duration_ms = 0.0
        self._total_duration_ms = 0.0
        self._last_error = ""

    def _effective_timeout(self, timeout_s: Optional[float]) -> Optional[float]:
        value = self.default_timeout_s if timeout_s is None else timeout_s
        if value is not None and value <= 0:
            raise ValueError("timeout_s must be positive or None")
        return float(value) if value is not None else None

    def _touch_heartbeat_locked(self) -> None:
        self._heartbeat_ts_ms = _wall_ms()
        self._heartbeat_monotonic = time.monotonic()

    def _begin_start(self) -> None:
        with self._lock:
            if self._state == "RUNNING":
                return
            if self._state == "STARTING":
                return
            if self._state != "NEW":
                raise V4WorkerNotRunning(
                    f"{self.worker_name} cannot start from state {self._state}"
                )
            self._state = "STARTING"
            self._thread = threading.Thread(
                target=self._thread_main,
                name=self.worker_name,
                daemon=False,
            )
            self._thread.start()

    def _raise_start_error(self) -> None:
        with self._lock:
            error = self._startup_error
            state = self._state
        if error is not None:
            raise V4WorkerStartupError(
                f"{self.worker_name} failed to start: {type(error).__name__}: {error}"
            ) from error
        if state != "RUNNING":
            raise V4WorkerStartupError(
                f"{self.worker_name} did not reach RUNNING (state={state})"
            )

    def start(self, *, timeout_s: float = 10.0) -> _DedicatedStoreWorker[StoreT]:
        """Start and synchronously wait for the thread-owned store to open."""

        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        self._begin_start()
        if not self._ready.wait(timeout=float(timeout_s)):
            raise V4WorkerStartupError(f"{self.worker_name} startup timed out")
        self._raise_start_error()
        return self

    async def start_async(
        self, *, timeout_s: float = 10.0
    ) -> _DedicatedStoreWorker[StoreT]:
        """Start without blocking the asyncio loop on store initialization."""

        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        self._begin_start()
        deadline = time.monotonic() + float(timeout_s)
        while not self._ready.is_set():
            if time.monotonic() >= deadline:
                raise V4WorkerStartupError(f"{self.worker_name} startup timed out")
            await asyncio.sleep(0.005)
        self._raise_start_error()
        return self

    def submit(
        self,
        operation: Callable[[StoreT], ResultT],
        *,
        kind: str,
        name: Optional[str] = None,
        timeout_s: Optional[float] = None,
    ) -> Future[ResultT]:
        """Submit without blocking; raise immediately when the queue is full."""

        if not callable(operation):
            raise TypeError("operation must be callable")
        timeout = self._effective_timeout(timeout_s)
        enqueued = time.monotonic()
        future: Future[ResultT] = Future()
        with self._lock:
            if self._state != "RUNNING" or not self._accepting:
                raise V4WorkerNotRunning(
                    f"{self.worker_name} is not accepting work (state={self._state})"
                )
            self._sequence += 1
            item = _WorkItem(
                sequence=self._sequence,
                kind=str(kind),
                name=str(name or _callable_name(operation)),
                operation=cast(Callable[[Any], Any], operation),
                future=cast(Future[Any], future),
                enqueued_monotonic=enqueued,
                deadline_monotonic=(enqueued + timeout if timeout is not None else None),
            )
            try:
                self._queue.put_nowait(item)
            except queue.Full as exc:
                self._rejected += 1
                self._last_error = "queue_full"
                raise V4WorkerQueueFull(
                    f"{self.worker_name} queue is full ({self.queue_capacity})"
                ) from exc
            self._submitted += 1
            self._queue_high_water = max(
                self._queue_high_water, self._queue.qsize()
            )
        return future

    async def _await_result(
        self,
        future: Future[ResultT],
        *,
        timeout_s: Optional[float],
        name: str,
    ) -> ResultT:
        wrapped = asyncio.wrap_future(future)
        if timeout_s is None:
            return await wrapped
        try:
            return await asyncio.wait_for(
                asyncio.shield(wrapped), timeout=float(timeout_s)
            )
        except asyncio.TimeoutError as exc:
            if future.done():
                return future.result()
            with self._lock:
                self._client_timeouts += 1
                self._last_error = f"client_timeout:{name}"
            wrapped.add_done_callback(_consume_async_result)
            raise V4WorkerJobTimeout(
                f"{self.worker_name} job {name!r} exceeded {timeout_s:.3f}s"
            ) from exc

    def _sync_result(
        self,
        future: Future[ResultT],
        *,
        timeout_s: Optional[float],
        name: str,
    ) -> ResultT:
        try:
            return future.result(timeout=timeout_s)
        except FutureTimeoutError as exc:
            if future.done():
                return future.result()
            with self._lock:
                self._client_timeouts += 1
                self._last_error = f"client_timeout:{name}"
            raise V4WorkerJobTimeout(
                f"{self.worker_name} job {name!r} exceeded {timeout_s:.3f}s"
            ) from exc

    async def _run(
        self,
        operation: Callable[[StoreT], ResultT],
        *,
        kind: str,
        name: Optional[str],
        timeout_s: Optional[float],
    ) -> ResultT:
        timeout = self._effective_timeout(timeout_s)
        label = str(name or _callable_name(operation))
        future = self.submit(
            operation, kind=kind, name=label, timeout_s=timeout
        )
        return await self._await_result(future, timeout_s=timeout, name=label)

    def _run_sync(
        self,
        operation: Callable[[StoreT], ResultT],
        *,
        kind: str,
        name: Optional[str],
        timeout_s: Optional[float],
    ) -> ResultT:
        timeout = self._effective_timeout(timeout_s)
        label = str(name or _callable_name(operation))
        future = self.submit(
            operation, kind=kind, name=label, timeout_s=timeout
        )
        return self._sync_result(future, timeout_s=timeout, name=label)

    def _assert_owner_thread(self) -> None:
        if threading.get_ident() != self._owner_thread_id:
            raise V4WorkerInvariantError(
                f"{self.worker_name} store access escaped its owner thread"
            )

    def _prepare_deadline(self, store: StoreT, item: _WorkItem) -> None:
        deadline = item.deadline_monotonic
        if deadline is None:
            store.connection.set_progress_handler(None, 0)
            return

        def expired() -> int:
            return int(time.monotonic() >= deadline)

        store.connection.set_progress_handler(expired, 1_000)

    def _clear_deadline(self, store: StoreT) -> None:
        store.connection.set_progress_handler(None, 0)

    def _after_job(self, store: StoreT) -> None:
        """Subclass hook, always executed on the connection-owning thread."""

        if store.connection.in_transaction:
            store.connection.rollback()
            raise V4WorkerInvariantError("worker job leaked an open transaction")

    def _job_started(self, item: _WorkItem) -> None:
        """Diagnostics hook: one job began on the owner thread (no-op here)."""

    def _job_finished(self, item: _WorkItem, error: Optional[BaseException]) -> None:
        """Diagnostics hook: the job from ``_job_started`` ended (no-op here)."""

    def _validate_result(self, store: StoreT, result: Any) -> None:
        if result is store or result is store.connection or isinstance(
            result, (sqlite3.Connection, sqlite3.Cursor)
        ):
            raise V4WorkerInvariantError(
                "worker callables may not return a store, connection, or cursor"
            )
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()
            raise TypeError("worker operations must be synchronous callables")

    def _execute_item(self, store: StoreT, item: _WorkItem) -> None:
        self._assert_owner_thread()
        if not item.future.set_running_or_notify_cancel():
            with self._lock:
                self._cancelled += 1
            return
        started = time.monotonic()
        with self._lock:
            self._started += 1
            self._current_job = item.name
            self._current_kind = item.kind
            self._current_started_monotonic = started
            self._touch_heartbeat_locked()
        try:
            self._job_started(item)
        except Exception:  # diagnostics must never fail the job itself
            pass

        error: Optional[BaseException] = None
        result: Any = None
        if item.deadline_monotonic is not None and started >= item.deadline_monotonic:
            error = V4WorkerJobTimeout(
                f"{self.worker_name} job {item.name!r} expired in queue"
            )
        else:
            try:
                self._prepare_deadline(store, item)
                result = item.operation(store)
                self._validate_result(store, result)
                if (
                    item.deadline_monotonic is not None
                    and time.monotonic() >= item.deadline_monotonic
                ):
                    raise V4WorkerJobTimeout(
                        f"{self.worker_name} job {item.name!r} exceeded its deadline"
                    )
            except BaseException as exc:  # worker must survive individual job failures
                if (
                    item.deadline_monotonic is not None
                    and time.monotonic() >= item.deadline_monotonic
                    and isinstance(exc, sqlite3.OperationalError)
                ):
                    error = V4WorkerJobTimeout(
                        f"{self.worker_name} job {item.name!r} was interrupted at deadline"
                    )
                else:
                    error = exc
            finally:
                try:
                    self._clear_deadline(store)
                except BaseException as exc:
                    if error is None:
                        error = exc

        try:
            self._after_job(store)
        except BaseException as exc:
            if error is None:
                error = exc
        try:
            self._job_finished(item, error)
        except Exception:  # diagnostics must never fail the job itself
            pass

        finished = time.monotonic()
        duration_ms = max(0.0, (finished - started) * 1_000.0)
        with self._lock:
            if error is None:
                self._completed += 1
                self._last_error = ""
            else:
                self._failed += 1
                if isinstance(error, V4WorkerJobTimeout):
                    self._expired_jobs += 1
                self._last_error = f"{type(error).__name__}:{error}"[:240]
            self._last_job = item.name
            self._last_kind = item.kind
            self._last_duration_ms = duration_ms
            self._max_duration_ms = max(self._max_duration_ms, duration_ms)
            self._total_duration_ms += duration_ms
            self._current_job = None
            self._current_kind = None
            self._current_started_monotonic = 0.0
            self._touch_heartbeat_locked()
        if not item.future.done():
            if error is None:
                item.future.set_result(result)
            else:
                item.future.set_exception(error)

    def _fail_pending(self, error: BaseException) -> None:
        while True:
            try:
                pending = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                if isinstance(pending, _WorkItem) and not pending.future.done():
                    pending.future.set_exception(error)
            finally:
                self._queue.task_done()

    def _thread_main(self) -> None:
        store: Optional[StoreT] = None
        close_error: Optional[BaseException] = None
        with self._lock:
            self._owner_thread_id = threading.get_ident()
            self._touch_heartbeat_locked()
        try:
            store = self._store_factory()
            # V4ReadOnlyStore gained the base class's owner checks before its
            # constructor exposed the corresponding opt-in parameter.  Binding
            # these private invariants here keeps custom/older read-only stores
            # strict at the worker layer; newer constructors receive the public
            # ``enforce_thread_ownership=True`` argument in default_factory.
            if isinstance(store, V4ReadOnlyStore):
                store._enforce_thread_ownership = True
                store._owner_thread_id = threading.get_ident()
            self._assert_owner_thread()
            with self._lock:
                self._state = "RUNNING"
                self._accepting = True
                self._touch_heartbeat_locked()
            self._ready.set()
            while True:
                try:
                    queued = self._queue.get(timeout=self.heartbeat_interval_s)
                except queue.Empty:
                    with self._lock:
                        self._touch_heartbeat_locked()
                    continue
                try:
                    if queued is _STOP:
                        break
                    self._execute_item(store, cast(_WorkItem, queued))
                finally:
                    self._queue.task_done()
        except BaseException as exc:
            with self._lock:
                if self._state == "STARTING":
                    self._startup_error = exc
                self._last_error = f"{type(exc).__name__}:{exc}"[:240]
                self._state = "FAILED"
                self._accepting = False
            self._ready.set()
        finally:
            if store is not None:
                try:
                    self._assert_owner_thread()
                    store.close()
                except BaseException as exc:
                    close_error = exc
            self._fail_pending(
                V4WorkerNotRunning(f"{self.worker_name} stopped before executing job")
            )
            with self._lock:
                if close_error is not None:
                    self._last_error = (
                        f"close:{type(close_error).__name__}:{close_error}"
                    )[:240]
                    self._state = "FAILED"
                elif self._state != "FAILED":
                    self._state = "STOPPED"
                self._accepting = False
                self._current_job = None
                self._current_kind = None
                self._current_started_monotonic = 0.0
                self._touch_heartbeat_locked()
            self._ready.set()
            self._stopped.set()

    def _mark_stopping(self) -> bool:
        with self._lock:
            if self._state == "NEW":
                self._state = "STOPPED"
                self._accepting = False
                self._ready.set()
                self._stopped.set()
                return False
            if self._state in {"STOPPED", "FAILED"}:
                return False
            if threading.get_ident() == self._owner_thread_id:
                raise V4WorkerInvariantError("worker cannot stop itself from a job")
            self._state = "STOPPING"
            self._accepting = False
            return True

    def _enqueue_stop_sync(self, deadline: float) -> None:
        with self._stop_lock:
            if self._stop_enqueued or self._stopped.is_set():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise V4WorkerShutdownTimeout(
                    f"{self.worker_name} shutdown timed out before stop admission"
                )
            try:
                self._queue.put(_STOP, timeout=remaining)
            except queue.Full as exc:
                raise V4WorkerShutdownTimeout(
                    f"{self.worker_name} shutdown could not enter the full queue"
                ) from exc
            self._stop_enqueued = True

    async def _enqueue_stop_async(self, deadline: float) -> None:
        while True:
            with self._stop_lock:
                if self._stop_enqueued or self._stopped.is_set():
                    return
                try:
                    self._queue.put_nowait(_STOP)
                except queue.Full:
                    pass
                else:
                    self._stop_enqueued = True
                    return
            if time.monotonic() >= deadline:
                raise V4WorkerShutdownTimeout(
                    f"{self.worker_name} shutdown could not enter the full queue"
                )
            await asyncio.sleep(0.005)

    def stop(self, *, timeout_s: float = 10.0) -> None:
        """Reject new jobs, drain accepted jobs, and close on the owner thread."""

        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        if not self._mark_stopping():
            return
        deadline = time.monotonic() + float(timeout_s)
        self._enqueue_stop_sync(deadline)
        remaining = max(0.0, deadline - time.monotonic())
        if not self._stopped.wait(timeout=remaining):
            raise V4WorkerShutdownTimeout(
                f"{self.worker_name} did not stop within {timeout_s:.3f}s"
            )
        thread = self._thread
        if thread is not None:
            thread.join(timeout=0)

    async def stop_async(self, *, timeout_s: float = 10.0) -> None:
        """Async-loop-safe graceful stop; no executor or blocking join is used."""

        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        if not self._mark_stopping():
            return
        deadline = time.monotonic() + float(timeout_s)
        await self._enqueue_stop_async(deadline)
        while not self._stopped.is_set():
            if time.monotonic() >= deadline:
                raise V4WorkerShutdownTimeout(
                    f"{self.worker_name} did not stop within {timeout_s:.3f}s"
                )
            await asyncio.sleep(0.005)
        thread = self._thread
        if thread is not None:
            thread.join(timeout=0)

    def health(self) -> dict[str, Any]:
        """Return a lock-consistent, JSON-safe worker health snapshot."""

        current_mono = time.monotonic()
        with self._lock:
            thread = self._thread
            duration_count = self._completed + self._failed
            return {
                "worker_name": self.worker_name,
                "worker_kind": self.worker_kind,
                "state": self._state,
                "running": bool(self._state == "RUNNING" and thread and thread.is_alive()),
                "accepting": self._accepting,
                "thread_affine_connection": True,
                "owner_thread_id": self._owner_thread_id,
                "thread_alive": bool(thread and thread.is_alive()),
                "queue_depth": self._queue.qsize(),
                "queue_capacity": self.queue_capacity,
                "queue_high_water": self._queue_high_water,
                "submitted": self._submitted,
                "started": self._started,
                "completed": self._completed,
                "failed": self._failed,
                "cancelled": self._cancelled,
                "rejected_queue_full": self._rejected,
                "client_timeouts": self._client_timeouts,
                "expired_jobs": self._expired_jobs,
                "snapshot_rollbacks": self._snapshot_rollbacks,
                "heartbeat_ts_ms": self._heartbeat_ts_ms,
                "heartbeat_age_ms": round(
                    max(0.0, current_mono - self._heartbeat_monotonic) * 1_000.0,
                    3,
                ) if self._heartbeat_monotonic else None,
                "current_job": self._current_job,
                "current_kind": self._current_kind,
                "current_job_age_ms": round(
                    max(0.0, current_mono - self._current_started_monotonic) * 1_000.0,
                    3,
                ) if self._current_started_monotonic else 0.0,
                "last_job": self._last_job,
                "last_kind": self._last_kind,
                "last_duration_ms": round(self._last_duration_ms, 3),
                "max_duration_ms": round(self._max_duration_ms, 3),
                "average_duration_ms": round(
                    self._total_duration_ms / duration_count, 3
                ) if duration_count else 0.0,
                "last_error": self._last_error,
            }

    def __enter__(self) -> _DedicatedStoreWorker[StoreT]:
        return self.start()

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.stop()

    async def __aenter__(self) -> _DedicatedStoreWorker[StoreT]:
        return await self.start_async()

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        await self.stop_async()


class V4ReadWorker(_DedicatedStoreWorker[V4ReadOnlyStore]):
    """Bounded read/report worker with a private URI ``mode=ro`` connection."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        worker_name: str = "lite-frequency-v4-read-worker",
        worker_kind: str = "READ_REPORT",
        queue_capacity: int = 128,
        busy_timeout_ms: int = 5_000,
        heartbeat_interval_s: float = 0.25,
        default_timeout_s: Optional[float] = 30.0,
        store_factory: Optional[Callable[[], V4ReadOnlyStore]] = None,
    ) -> None:
        path = Path(db_path)

        def default_factory() -> V4ReadOnlyStore:
            parameters = inspect.signature(V4ReadOnlyStore).parameters
            ownership = (
                {"enforce_thread_ownership": True}
                if "enforce_thread_ownership" in parameters else {}
            )
            return V4ReadOnlyStore(
                path, busy_timeout_ms=busy_timeout_ms, **ownership
            )

        self.db_path = path
        super().__init__(
            worker_name=str(worker_name),
            worker_kind=str(worker_kind),
            queue_capacity=queue_capacity,
            heartbeat_interval_s=heartbeat_interval_s,
            default_timeout_s=default_timeout_s,
            store_factory=store_factory or default_factory,
        )

    def _after_job(self, store: V4ReadOnlyStore) -> None:
        connection = store.connection
        if connection.in_transaction:
            connection.rollback()
            with self._lock:
                self._snapshot_rollbacks += 1
        query_only = connection.execute("PRAGMA query_only").fetchone()
        if query_only is None or int(query_only[0]) != 1:
            connection.execute("PRAGMA query_only=ON")
            raise V4WorkerInvariantError("read worker query_only invariant changed")

    def _job_started(self, item: _WorkItem) -> None:
        # Job-scope reader occupancy: every logical read operation on this
        # worker registers so WAL reclamation can see which reader was live
        # while a checkpoint made (or failed to make) progress.  Statement
        # scope is registered separately by the read-only store itself.
        self._diag_job_token = READER_DIAGNOSTICS.begin_job(
            self.worker_name, item.name, item.kind)

    def _job_finished(self, item: _WorkItem, error: Optional[BaseException]) -> None:
        token = getattr(self, "_diag_job_token", 0)
        self._diag_job_token = 0
        READER_DIAGNOSTICS.end_job(
            token,
            error=(f"{type(error).__name__}:{error}"[:120]
                   if error is not None else None),
        )

    def submit_query(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        timeout_s: Optional[float] = None,
    ) -> Future[list[dict[str, Any]]]:
        statement, parameters = str(sql), tuple(params)
        return self.submit(
            lambda store: store.query(statement, parameters),
            kind="QUERY",
            name="query",
            timeout_s=timeout_s,
        )

    async def query(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        timeout_s: Optional[float] = None,
    ) -> list[dict[str, Any]]:
        statement, parameters = str(sql), tuple(params)
        return await self._run(
            lambda store: store.query(statement, parameters),
            kind="QUERY",
            name="query",
            timeout_s=timeout_s,
        )

    def query_sync(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        timeout_s: Optional[float] = None,
    ) -> list[dict[str, Any]]:
        statement, parameters = str(sql), tuple(params)
        return self._run_sync(
            lambda store: store.query(statement, parameters),
            kind="QUERY",
            name="query",
            timeout_s=timeout_s,
        )

    async def query_one(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        timeout_s: Optional[float] = None,
    ) -> Optional[dict[str, Any]]:
        statement, parameters = str(sql), tuple(params)
        return await self._run(
            lambda store: store.query_one(statement, parameters),
            kind="QUERY_ONE",
            name="query_one",
            timeout_s=timeout_s,
        )

    def query_one_sync(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        timeout_s: Optional[float] = None,
    ) -> Optional[dict[str, Any]]:
        statement, parameters = str(sql), tuple(params)
        return self._run_sync(
            lambda store: store.query_one(statement, parameters),
            kind="QUERY_ONE",
            name="query_one",
            timeout_s=timeout_s,
        )

    async def open_positions(
        self, *, timeout_s: Optional[float] = None
    ) -> list[dict[str, Any]]:
        return await self._run(
            lambda store: store.open_positions(),
            kind="OPEN_POSITIONS",
            name="open_positions",
            timeout_s=timeout_s,
        )

    def open_positions_sync(
        self, *, timeout_s: Optional[float] = None
    ) -> list[dict[str, Any]]:
        return self._run_sync(
            lambda store: store.open_positions(),
            kind="OPEN_POSITIONS",
            name="open_positions",
            timeout_s=timeout_s,
        )

    async def run_report(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> ResultT:
        """Run a synchronous reporting callable against the read-only store."""

        if not callable(operation):
            raise TypeError("operation must be callable")
        return await self._run(
            lambda store: operation(store, *args, **kwargs),
            kind="REPORT",
            name=name or _callable_name(operation),
            timeout_s=timeout_s,
        )

    def run_report_sync(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> ResultT:
        if not callable(operation):
            raise TypeError("operation must be callable")
        return self._run_sync(
            lambda store: operation(store, *args, **kwargs),
            kind="REPORT",
            name=name or _callable_name(operation),
            timeout_s=timeout_s,
        )


class V4MaintenanceWorker(_DedicatedStoreWorker[V4Store]):
    """Bounded checkpoint/retention worker with a private writer connection."""

    _CHECKPOINT_MODES = frozenset({"PASSIVE", "FULL", "RESTART", "TRUNCATE"})

    def __init__(
        self,
        db_path: str | Path,
        *,
        queue_capacity: int = 16,
        busy_timeout_ms: int = 10_000,
        heartbeat_interval_s: float = 0.25,
        default_timeout_s: Optional[float] = 30.0,
        background_write_admission: Optional[Callable[[], bool]] = None,
        background_write_release: Optional[Callable[[], None]] = None,
        store_factory: Optional[Callable[[], V4Store]] = None,
    ) -> None:
        path = Path(db_path)

        def default_factory() -> V4Store:
            return V4Store(
                path,
                busy_timeout_ms=busy_timeout_ms,
                enforce_thread_ownership=True,
                background_write_admission=background_write_admission,
                background_write_release=background_write_release,
            )

        self.db_path = path
        super().__init__(
            worker_name="lite-frequency-v4-maintenance-worker",
            worker_kind="MAINTENANCE",
            queue_capacity=queue_capacity,
            heartbeat_interval_s=heartbeat_interval_s,
            default_timeout_s=default_timeout_s,
            store_factory=store_factory or default_factory,
        )

    async def run_maintenance(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        kind: str = "MAINTENANCE",
        **kwargs: Any,
    ) -> ResultT:
        if not callable(operation):
            raise TypeError("operation must be callable")
        return await self._run(
            lambda store: operation(store, *args, **kwargs),
            kind=kind,
            name=name or _callable_name(operation),
            timeout_s=timeout_s,
        )

    def run_maintenance_sync(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        kind: str = "MAINTENANCE",
        **kwargs: Any,
    ) -> ResultT:
        if not callable(operation):
            raise TypeError("operation must be callable")
        return self._run_sync(
            lambda store: operation(store, *args, **kwargs),
            kind=kind,
            name=name or _callable_name(operation),
            timeout_s=timeout_s,
        )

    @classmethod
    def _checkpoint_job(cls, store: V4Store, mode: str) -> dict[str, int | str]:
        normalized = str(mode).strip().upper()
        if normalized not in cls._CHECKPOINT_MODES:
            raise ValueError(f"unsupported WAL checkpoint mode: {mode!r}")
        row = store.connection.execute(
            f"PRAGMA wal_checkpoint({normalized})"
        ).fetchone()
        if row is None or len(row) != 3:
            raise V4WorkerInvariantError("unexpected wal_checkpoint response")
        return {
            "mode": normalized,
            "busy": int(row[0]),
            "log_frames": int(row[1]),
            "checkpointed_frames": int(row[2]),
        }

    async def checkpoint(
        self, mode: str = "PASSIVE", *, timeout_s: Optional[float] = None
    ) -> dict[str, int | str]:
        return await self._run(
            lambda store: self._checkpoint_job(store, mode),
            kind="CHECKPOINT",
            name=f"wal_checkpoint_{str(mode).lower()}",
            timeout_s=timeout_s,
        )

    def checkpoint_sync(
        self, mode: str = "PASSIVE", *, timeout_s: Optional[float] = None
    ) -> dict[str, int | str]:
        return self._run_sync(
            lambda store: self._checkpoint_job(store, mode),
            kind="CHECKPOINT",
            name=f"wal_checkpoint_{str(mode).lower()}",
            timeout_s=timeout_s,
        )

    async def run_checkpoint(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> ResultT:
        return await self.run_maintenance(
            operation,
            *args,
            timeout_s=timeout_s,
            name=name,
            kind="CHECKPOINT",
            **kwargs,
        )

    async def run_retention(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> ResultT:
        return await self.run_maintenance(
            operation,
            *args,
            timeout_s=timeout_s,
            name=name,
            kind="RETENTION",
            **kwargs,
        )

    async def compact_raw_evidence(
        self,
        now_ms: int,
        *,
        retention_ms: int,
        batch_size: int,
        run_integrity: bool = False,
        timeout_s: Optional[float] = None,
    ) -> dict[str, Any]:
        return await self._run(
            lambda store: store.compact_raw_evidence(
                int(now_ms),
                retention_ms=int(retention_ms),
                batch_size=int(batch_size),
                run_integrity=bool(run_integrity),
            ),
            kind="RETENTION",
            name="compact_raw_evidence",
            timeout_s=timeout_s,
        )

    async def enforce_raw_row_cap(
        self, maximum_rows: int, *, timeout_s: Optional[float] = None
    ) -> dict[str, int]:
        return await self._run(
            lambda store: store.enforce_raw_row_cap(int(maximum_rows)),
            kind="RETENTION",
            name="enforce_raw_row_cap",
            timeout_s=timeout_s,
        )

    async def compact_event_buckets(
        self,
        now_ms: int,
        *,
        detail_retention_ms: int,
        timeout_s: Optional[float] = None,
    ) -> int:
        return await self._run(
            lambda store: store.compact_event_buckets(
                int(now_ms), detail_retention_ms=int(detail_retention_ms)
            ),
            kind="RETENTION",
            name="compact_event_buckets",
            timeout_s=timeout_s,
        )


class _RuntimeIOResource:
    """Opaque owner-thread token; unlike database workers it owns no connection."""

    def close(self) -> None:
        return


class V4RuntimeIOWorker(_DedicatedStoreWorker[_RuntimeIOResource]):
    """Bounded single-thread executor for V4 runtime and export filesystem I/O.

    Callers pass ordinary synchronous callables.  Typical jobs are
    ``runtime.publish``, ``runtime.process_ownership``, ``runtime.stop_requested``,
    and the final dashboard export.  They execute in FIFO order on this worker's
    dedicated thread and never consume asyncio's shared default executor.
    """

    def __init__(
        self,
        *,
        queue_capacity: int = 32,
        heartbeat_interval_s: float = 0.25,
        default_timeout_s: Optional[float] = 10.0,
    ) -> None:
        super().__init__(
            worker_name="lite-frequency-v4-runtime-io-worker",
            worker_kind="RUNTIME_IO",
            queue_capacity=queue_capacity,
            heartbeat_interval_s=heartbeat_interval_s,
            default_timeout_s=default_timeout_s,
            store_factory=_RuntimeIOResource,
        )

    def _prepare_deadline(
        self, store: _RuntimeIOResource, item: _WorkItem
    ) -> None:
        _ = store, item

    def _clear_deadline(self, store: _RuntimeIOResource) -> None:
        _ = store

    def _after_job(self, store: _RuntimeIOResource) -> None:
        _ = store

    def _validate_result(self, store: _RuntimeIOResource, result: Any) -> None:
        if result is store:
            raise V4WorkerInvariantError("runtime I/O resource may not escape its thread")
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()
            raise TypeError("runtime I/O operations must be synchronous callables")

    def submit_io(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> Future[ResultT]:
        if not callable(operation):
            raise TypeError("operation must be callable")
        return self.submit(
            lambda _resource: operation(*args, **kwargs),
            kind="RUNTIME_IO",
            name=name or _callable_name(operation),
            timeout_s=timeout_s,
        )

    async def run_io(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> ResultT:
        if not callable(operation):
            raise TypeError("operation must be callable")
        return await self._run(
            lambda _resource: operation(*args, **kwargs),
            kind="RUNTIME_IO",
            name=name or _callable_name(operation),
            timeout_s=timeout_s,
        )

    def run_io_sync(
        self,
        operation: Callable[..., ResultT],
        *args: Any,
        timeout_s: Optional[float] = None,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> ResultT:
        if not callable(operation):
            raise TypeError("operation must be callable")
        return self._run_sync(
            lambda _resource: operation(*args, **kwargs),
            kind="RUNTIME_IO",
            name=name or _callable_name(operation),
            timeout_s=timeout_s,
        )


__all__ = [
    "V4MaintenanceWorker",
    "V4ReadWorker",
    "V4RuntimeIOWorker",
    "V4WorkerError",
    "V4WorkerInvariantError",
    "V4WorkerJobTimeout",
    "V4WorkerNotRunning",
    "V4WorkerQueueFull",
    "V4WorkerShutdownTimeout",
    "V4WorkerStartupError",
]
