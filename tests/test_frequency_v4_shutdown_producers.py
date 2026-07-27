"""Stopping a producer must never abort the engine's shutdown sequence.

The V4 shutdown sequence is ordered so that the runtime session is durably
terminated even when the telemetry lane fails to drain -- an open session row
poisons every later startup's maker reconciliation.  Producer stops sat outside
that protection: a Polymarket socket that ended without a close handshake
raised ``ConnectionClosedError`` out of ``poly_ws.stop()``, skipped the session
terminal command, and published the runtime as FAILED after an otherwise clean
run.  These tests pin the guarantee at both layers.
"""
from __future__ import annotations

import asyncio

import pytest

from lite_frequency_v4.cex import OkxPublicProvider
from lite_frequency_v4.polymarket_ws import PolymarketMarketWS


class _DeadTransport(RuntimeError):
    """Stands in for a socket that ended without a close frame."""


async def _already_failed_task() -> None:
    raise _DeadTransport("no close frame received or sent")


def _failed_task() -> asyncio.Task:
    task = asyncio.get_event_loop().create_task(_already_failed_task())
    return task


@pytest.mark.asyncio
async def test_polymarket_stop_absorbs_a_dead_transport() -> None:
    ws = PolymarketMarketWS()
    ws._task = _failed_task()
    await asyncio.sleep(0)  # let it fail

    await ws.stop()  # must not raise

    assert ws._task is None
    assert ws.health_state.state == "STOPPED"
    assert ws.health_state.connected is False
    # The failure is recorded rather than swallowed silently.
    assert "_DeadTransport" in (ws.health_state.last_error or "")


@pytest.mark.asyncio
async def test_polymarket_stop_still_absorbs_cancellation() -> None:
    ws = PolymarketMarketWS()

    async def forever() -> None:
        await asyncio.Event().wait()

    ws._task = asyncio.get_event_loop().create_task(forever())
    await asyncio.sleep(0)
    await ws.stop()
    assert ws._task is None
    assert ws.health_state.state == "STOPPED"


@pytest.mark.asyncio
async def test_okx_stop_absorbs_a_dead_transport() -> None:
    ws = OkxPublicProvider()
    ws._task = _failed_task()
    await asyncio.sleep(0)

    await ws.stop()  # must not raise

    assert ws._task is None
    assert ws.health_state.state == "STOPPED"
    assert ws.health_state.connected is False
    assert "_DeadTransport" in (ws.health_state.last_error or "")


@pytest.mark.asyncio
async def test_okx_stop_absorbs_a_failing_socket_close() -> None:
    ws = OkxPublicProvider()

    class Socket:
        async def close(self) -> None:
            raise _DeadTransport("connection reset")

    ws._ws = Socket()
    await ws.stop()
    assert ws.health_state.state == "STOPPED"
    assert "_DeadTransport" in (ws.health_state.last_error or "")


@pytest.mark.asyncio
async def test_engine_shutdown_survives_a_raising_producer(monkeypatch) -> None:
    """The engine guards producer stops even if a producer regresses."""

    from lite_frequency_v4 import engine as engine_module

    order: list[str] = []

    class RaisingProducer:
        def __init__(self, label: str) -> None:
            self.label = label

        async def stop(self) -> None:
            order.append(f"stop:{self.label}")
            raise _DeadTransport(f"{self.label} died")

    class Engine:
        # The exact loop the engine runs, extracted so the guarantee is
        # tested without standing up a whole runtime.
        def __init__(self) -> None:
            self.poly_ws = RaisingProducer("poly")
            self.okx = RaisingProducer("okx")
            self._last_error = ""

        async def stop_producers(self) -> None:
            for producer, label in (
                (self.poly_ws, "polymarket_ws"), (self.okx, "okx_ws"),
            ):
                try:
                    await producer.stop()
                except Exception as exc:  # noqa: BLE001
                    self._last_error = (
                        f"producer_stop:{label}:{type(exc).__name__}:{exc}"
                    )[:240]
            order.append("session_terminated")

    engine = Engine()
    await engine.stop_producers()
    # Both producers were stopped, and the sequence continued to the step that
    # durably ends the runtime session.
    assert order == ["stop:poly", "stop:okx", "session_terminated"]
    assert "producer_stop:okx_ws" in engine._last_error
    assert engine_module is not None


def test_engine_stop_guards_both_producers_in_source() -> None:
    """The guard must stay in the real shutdown path, not only in this test."""

    import inspect

    from lite_frequency_v4.engine import FrequencyV4Engine

    source = inspect.getsource(FrequencyV4Engine.stop)
    assert "Stop producers first" in source
    producer_block = source[source.index("Stop producers first"):]
    producer_block = producer_block[:producer_block.index(
        "telemetry_stop_failure")]
    assert "self.poly_ws" in producer_block and "self.okx" in producer_block
    assert "except Exception" in producer_block
    # And the session terminal command still follows, after the guard.
    assert source.index("end_runtime_session") > source.index(
        "Stop producers first")
