"""Async façade over the sync zeared surface.

Zenoh's Python bindings have no native async entry points, so the async path
here is an ergonomic wrapper: publish/open calls are offloaded to the thread
pool via ``asyncio.to_thread``, and subscriber delivery bridges the Rust
callback thread to asyncio via ``loop.call_soon_threadsafe`` feeding an
``asyncio.Queue``.

Sync and async calls share state transparently — same ``Message`` class,
same session, same ``z.batch()`` buffer (backed by ``contextvars``).
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator, Iterable, Optional

from .batch import batch as _batch_cm

if TYPE_CHECKING:
    import zenoh

    from .message import Message


class _AsyncSessionContextManager:
    """Async context manager for ``apeer`` / ``aclient`` / ``aopen``.

    Usage::

        async with z.apeer(connect=['tcp/x:7447']) as sess:
            ...

    Constructor stashes the open factory + kwargs; ``__aenter__`` runs
    the open via ``asyncio.to_thread`` (Zenoh's Python bindings are
    sync, so the thread pool worker keeps the event loop unblocked);
    ``__aexit__`` runs ``z.release(session=sess)`` via the same
    mechanism so the cleanup walks happen off-loop too.

    Holds the wrapper across the block (returns it from ``__aenter__``,
    not ``raw()``) — code inside should bind to the wrapper so it
    survives reconnects.

    Doesn't suppress exceptions; ``release()`` raises propagate.
    """
    __slots__ = ('_factory', '_kwargs', '_sess')

    def __init__(self, factory, kwargs):
        self._factory = factory
        self._kwargs = kwargs
        self._sess = None

    async def __aenter__(self):
        self._sess = await asyncio.to_thread(
            lambda: self._factory(**self._kwargs),
        )
        return self._sess

    async def __aexit__(self, exc_type, exc, tb):
        from . import release
        sess = self._sess
        self._sess = None
        if sess is not None:
            await asyncio.to_thread(release, session=sess)
        return None


def apeer(
    *,
    connect: Optional[list[str]] = None,
    listen: Optional[list[str]] = None,
    config=None,
    zenoh_config: Optional['zenoh.Config'] = None,
    retry: bool = False,
    initial_backoff: float = 0.1,
    max_backoff: float = 30.0,
    max_attempts: Optional[int] = None,
    auto_reconnect: bool = False,
    probe_interval: float = 10.0,
    timestamping: bool = True,
    gc_interval: float = 60.0,
    retention_ttl: Optional[float] = None,
) -> _AsyncSessionContextManager:
    """Async-context-managed peer session.

    Returns an async context manager — use as
    ``async with z.apeer(...) as sess: ...``. The ``await z.apeer()``
    form from ≤0.0.14 is removed (pre-0.1.0 break).
    """
    from . import peer
    kwargs: dict = {
        'timestamping': timestamping, 'gc_interval': gc_interval,
        'auto_reconnect': auto_reconnect, 'probe_interval': probe_interval,
        'retention_ttl': retention_ttl,
    }
    if config is not None:
        kwargs['config'] = config
    else:
        kwargs.update(
            connect=connect, listen=listen, zenoh_config=zenoh_config,
            retry=retry, initial_backoff=initial_backoff,
            max_backoff=max_backoff, max_attempts=max_attempts,
        )
    return _AsyncSessionContextManager(peer, kwargs)


def aclient(
    router=None,
    *,
    config=None,
    zenoh_config: Optional['zenoh.Config'] = None,
    retry: bool = False,
    initial_backoff: float = 0.1,
    max_backoff: float = 30.0,
    max_attempts: Optional[int] = None,
    auto_reconnect: bool = False,
    probe_interval: float = 10.0,
    timestamping: bool = True,
    gc_interval: float = 60.0,
    retention_ttl: Optional[float] = None,
) -> _AsyncSessionContextManager:
    """Async-context-managed client session. See :func:`apeer`."""
    from . import client
    kwargs: dict = {
        'timestamping': timestamping, 'gc_interval': gc_interval,
        'auto_reconnect': auto_reconnect, 'probe_interval': probe_interval,
        'retention_ttl': retention_ttl,
    }
    if router is not None:
        kwargs['router'] = router
    if config is not None:
        kwargs['config'] = config
    else:
        kwargs.update(
            zenoh_config=zenoh_config,
            retry=retry, initial_backoff=initial_backoff,
            max_backoff=max_backoff, max_attempts=max_attempts,
        )
    return _AsyncSessionContextManager(client, kwargs)


def aopen(cfg) -> _AsyncSessionContextManager:
    """Async-context-managed dispatch on :class:`SessionConfig`. See
    :func:`apeer`."""
    from . import open as _open
    return _AsyncSessionContextManager(_open, {'cfg': cfg})


async def asend(
    msg: 'Message',
    *,
    session: Optional['zenoh.Session'] = None,
    topic: Optional[str] = None,
    retain: Optional[bool] = None,
) -> None:
    """Async variant of ``msg.send(...)``. Runs the sync send on a thread."""
    await asyncio.to_thread(
        msg.send, session=session, topic=topic, retain=retain,
    )


async def asend_batch(
    cls,
    items: Iterable['Message'],
    *,
    session: Optional['zenoh.Session'] = None,
    topic: Optional[str] = None,
    retain: Optional[bool] = None,
) -> None:
    """Async variant of ``Cls.send_batch(...)``."""
    await asyncio.to_thread(
        cls.send_batch, list(items),
        session=session, topic=topic, retain=retain,
    )


async def aunretain(
    cls_or_msg,
    *,
    session: Optional['zenoh.Session'] = None,
    topic: Optional[str] = None,
    **key_fields,
) -> None:
    """Async variant of ``msg.unretain()`` / ``Cls.unretain(**)``.

    Pass either a ``Message`` instance (uses its template fields) or a
    ``Message`` subclass (key fields supplied as kwargs).
    """
    from .message import Message
    if isinstance(cls_or_msg, Message):
        await asyncio.to_thread(
            cls_or_msg.unretain, session=session, topic=topic,
        )
    else:
        await asyncio.to_thread(
            cls_or_msg.unretain,
            session=session, topic=topic, **key_fields,
        )


async def alisten(
    cls,
    *,
    session: Optional['zenoh.Session'] = None,
    maxsize: int = 0,
) -> AsyncIterator:
    """Async generator yielding decoded messages for ``cls``.

    Bridges the sync ``on_message`` callback to an ``asyncio.Queue`` fed via
    ``loop.call_soon_threadsafe``. Cancellation or a break out of the loop
    undeclares the underlying subscriber cleanly.

    ``maxsize=0`` (default) means an unbounded queue; set a positive value
    to apply backpressure (delivery blocks when the queue is full, which
    for Zenoh means dropping to zenoh's internal buffering).
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
    loop = asyncio.get_running_loop()

    def _cb(msg: 'Message') -> None:
        loop.call_soon_threadsafe(queue.put_nowait, msg)

    sub = cls.on_message(_cb, session=session)
    try:
        while True:
            yield await queue.get()
    finally:
        sub.close()


@asynccontextmanager
async def abatch():
    """Async version of :func:`zeared.batch`.

    Shares the same contextvar-backed buffer as ``z.batch()``. An
    exception escaping the block discards the buffer without flushing,
    matching sync semantics.
    """
    with _batch_cm() as handle:
        yield handle


__all__ = [
    'abatch',
    'aclient',
    'alisten',
    'aopen',
    'apeer',
    'asend',
    'asend_batch',
    'aunretain',
]
