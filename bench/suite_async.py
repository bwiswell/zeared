"""Async suite: the cost of each sync/async publish + delivery combination.

zeared's async surface is an ergonomic wrapper — Zenoh's Python bindings have
no native async entry points, so ``asend`` offloads to a thread and ``alisten``
bridges the Rust callback thread to asyncio via a queue. This suite prices
that wrapper: which combinations are free, and which pay for the bridge.

The headline numbers come from here.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

import zeared as z

from .harness import DEFAULT_DURATION_S, InnerPy, Run, drain, payload, settle, versions, wire_size

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    import zenoh

SUITE = 'async'

#: Publishes between cooperative yields, so a sync publish loop inside an
#: event loop still lets the consumer drain.
_YIELD_EVERY = 500

#: Async drain budget — the async paths settle far faster than the sync ones.
_ASYNC_DRAIN_S = 3.0


def _message_class(topic: str, encoding: str) -> type[z.Message]:
    """Build a fresh message class on ``topic`` so publisher caches don't persist."""

    @z.zeared(accel=False)
    class Outer(z.Message):
        TOPIC = topic
        ENCODING = encoding
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    return Outer


async def _alisten_consumer(
    msg_cls: type[z.Message],
    do_publish: Callable[[], Coroutine[Any, Any, tuple[int, float]]],
) -> tuple[int, int, float, float]:
    """Run ``do_publish`` against an ``alisten`` consumer task; return the counters.

    The consumer's async generator closes its subscriber in a ``finally``, so
    cancelling the task is the teardown.
    """
    received = 0

    async def consumer() -> None:
        nonlocal received
        async for _m in msg_cls.alisten():
            received += 1

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.15)

    sent, t_pub_end = await do_publish()

    deadline = t_pub_end + _ASYNC_DRAIN_S
    # Polling a counter incremented from Zenoh's callback thread toward a
    # target, not awaiting a single signal — an asyncio.Event doesn't fit.
    while received < sent and time.perf_counter() < deadline:  # noqa: ASYNC110
        await asyncio.sleep(0.05)
    t_end = time.perf_counter()

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    return sent, received, t_pub_end, t_end


def _sync_send_sync_sub(msg_cls: type[z.Message], data: dict[str, Any], duration_s: float, label: str) -> Run:
    """Baseline: sync ``send`` into a sync ``on_message`` callback."""
    instance = msg_cls.load(data)
    received = [0]
    sub = msg_cls.on_message(lambda _m: received.__setitem__(0, received[0] + 1))
    settle()

    t0 = time.perf_counter()
    deadline = t0 + duration_s
    sent = 0
    while time.perf_counter() < deadline:
        instance.send()
        sent += 1
    t_pub = time.perf_counter()
    t_end = drain(received, sent, t_pub)
    sub.close()
    return Run(label, versions(), sent, received[0], t_pub - t0, t_end - t0, wire_size(msg_cls, data))


def _asend_sync_sub(msg_cls: type[z.Message], data: dict[str, Any], duration_s: float, label: str) -> Run:
    """Async ``asend`` into a sync ``on_message`` callback."""
    instance = msg_cls.load(data)
    received = [0]
    sub = msg_cls.on_message(lambda _m: received.__setitem__(0, received[0] + 1))
    settle()

    async def pub() -> tuple[int, float, float]:
        sent = 0
        t0 = time.perf_counter()
        deadline = t0 + duration_s
        while time.perf_counter() < deadline:
            await instance.asend()
            sent += 1
        return sent, t0, time.perf_counter()

    sent, t0, t_pub = asyncio.run(pub())
    t_end = drain(received, sent, t_pub)
    sub.close()
    return Run(label, versions(), sent, received[0], t_pub - t0, t_end - t0, wire_size(msg_cls, data))


def _send_alisten(msg_cls: type[z.Message], data: dict[str, Any], duration_s: float, label: str) -> Run:
    """Sync ``send`` into an ``alisten`` async generator."""
    instance = msg_cls.load(data)
    wire = wire_size(msg_cls, data)

    async def main() -> Run:
        t0 = time.perf_counter()

        async def do_publish() -> tuple[int, float]:
            sent = 0
            deadline = t0 + duration_s
            while time.perf_counter() < deadline:
                instance.send()
                sent += 1
                if sent % _YIELD_EVERY == 0:
                    await asyncio.sleep(0)
            return sent, time.perf_counter()

        sent, received, t_pub, t_end = await _alisten_consumer(msg_cls, do_publish)
        return Run(label, versions(), sent, received, t_pub - t0, t_end - t0, wire)

    return asyncio.run(main())


def _asend_alisten(msg_cls: type[z.Message], data: dict[str, Any], duration_s: float, label: str) -> Run:
    """Fully async: ``asend`` into ``alisten``."""
    instance = msg_cls.load(data)
    wire = wire_size(msg_cls, data)

    async def main() -> Run:
        t0 = time.perf_counter()

        async def do_publish() -> tuple[int, float]:
            sent = 0
            deadline = t0 + duration_s
            while time.perf_counter() < deadline:
                await instance.asend()
                sent += 1
            return sent, time.perf_counter()

        sent, received, t_pub, t_end = await _alisten_consumer(msg_cls, do_publish)
        return Run(label, versions(), sent, received, t_pub - t0, t_end - t0, wire)

    return asyncio.run(main())


def _send_async_cb(msg_cls: type[z.Message], data: dict[str, Any], duration_s: float, label: str) -> Run:
    """Sync ``send``; the ``on_message`` handler is an ``async def``."""
    instance = msg_cls.load(data)
    wire = wire_size(msg_cls, data)

    async def main() -> Run:
        received = 0
        sent = 0
        t0 = time.perf_counter()

        async def handler(_m: z.Message) -> None:
            nonlocal received
            received += 1

        sub = msg_cls.on_message(handler)
        await asyncio.sleep(0.15)

        deadline = t0 + duration_s
        while time.perf_counter() < deadline:
            instance.send()
            sent += 1
            if sent % _YIELD_EVERY == 0:
                await asyncio.sleep(0)
        t_pub = time.perf_counter()

        drain_deadline = t_pub + _ASYNC_DRAIN_S
        while received < sent and time.perf_counter() < drain_deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.05)
        t_end = time.perf_counter()
        sub.close()
        return Run(label, versions(), sent, received, t_pub - t0, t_end - t0, wire)

    return asyncio.run(main())


def run(session: zenoh.Session, duration_s: float = DEFAULT_DURATION_S) -> list[Run]:
    """Time every sync/async delivery combination."""
    # See the note in suite_wire.zeared_strategy.
    z.session = session  # ty: ignore[invalid-assignment]
    data = payload()
    runs = []

    try:
        from . import baseline
    except ImportError:
        baseline = None
    if baseline is not None:
        from .harness import publish_window

        pub, sub, und = baseline.strategy(session, 'bench/async/marshmallow', data)
        runs.append(
            publish_window(baseline.STRATEGY, baseline.version(), duration_s, pub, sub, und, baseline.wire_bytes(data))
        )

    combos = [
        ('sync send + sync on_message (json)', 'bench/async/sync_json', 'json', _sync_send_sync_sub),
        ('sync send + sync on_message (msgpack)', 'bench/async/sync_msgp', 'msgpack', _sync_send_sync_sub),
        ('asend + sync on_message (msgpack)', 'bench/async/asend_sync', 'msgpack', _asend_sync_sub),
        ('sync send + alisten (msgpack)', 'bench/async/send_alisten', 'msgpack', _send_alisten),
        ('asend + alisten (msgpack)', 'bench/async/full_msgp', 'msgpack', _asend_alisten),
        ('asend + alisten (json)', 'bench/async/full_json', 'json', _asend_alisten),
        ('sync send + async-def on_message (msgpack)', 'bench/async/async_cb', 'msgpack', _send_async_cb),
    ]
    for label, topic, encoding, fn in combos:
        runs.append(fn(_message_class(topic, encoding), data, duration_s, label))
    return runs
