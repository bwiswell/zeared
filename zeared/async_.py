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
from typing import TYPE_CHECKING, Any

from .batch import batch as _batch_cm

if TYPE_CHECKING:
    import types
    from collections.abc import AsyncIterator, Callable, Iterable

    import zenoh

    from .batch import _BatchHandle
    from .config import SessionConfig
    from .message import Message
    from .meta import ZenohMeta
    from .queryable import Queryable, QueryContext


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

    def __init__(self, factory: Callable[..., Any], kwargs: dict[str, Any]) -> None:
        self._factory = factory
        self._kwargs = kwargs
        self._sess = None

    async def __aenter__(self) -> Any:
        self._sess = await asyncio.to_thread(
            lambda: self._factory(**self._kwargs),
        )
        return self._sess

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        from . import release

        sess = self._sess
        self._sess = None
        if sess is not None:
            await asyncio.to_thread(release, session=sess)


def apeer(  # noqa: PLR0913
    *,
    connect: list[str] | None = None,
    listen: list[str] | None = None,
    config: SessionConfig | None = None,
    zenoh_config: zenoh.Config | None = None,
    retry: bool = False,
    initial_backoff: float = 0.1,
    max_backoff: float = 30.0,
    max_attempts: int | None = None,
    auto_reconnect: bool = False,
    probe_interval: float = 10.0,
    timestamping: bool = True,
    gc_interval: float = 60.0,
    retention_ttl: float | None = None,
) -> _AsyncSessionContextManager:
    """Async-context-managed peer session.

    Returns an async context manager — use as
    ``async with z.apeer(...) as sess: ...``. The ``await z.apeer()``
    form from ≤0.0.14 is removed (pre-0.1.0 break).
    """
    from . import peer

    kwargs: dict = {
        'timestamping': timestamping,
        'gc_interval': gc_interval,
        'auto_reconnect': auto_reconnect,
        'probe_interval': probe_interval,
        'retention_ttl': retention_ttl,
    }
    if config is not None:
        kwargs['config'] = config
    else:
        kwargs.update(
            connect=connect,
            listen=listen,
            zenoh_config=zenoh_config,
            retry=retry,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            max_attempts=max_attempts,
        )
    return _AsyncSessionContextManager(peer, kwargs)


def aclient(  # noqa: PLR0913
    router: str | list[str] | None = None,
    *,
    config: SessionConfig | None = None,
    zenoh_config: zenoh.Config | None = None,
    retry: bool = False,
    initial_backoff: float = 0.1,
    max_backoff: float = 30.0,
    max_attempts: int | None = None,
    auto_reconnect: bool = False,
    probe_interval: float = 10.0,
    timestamping: bool = True,
    gc_interval: float = 60.0,
    retention_ttl: float | None = None,
) -> _AsyncSessionContextManager:
    """Async-context-managed client session. See :func:`apeer`."""
    from . import client

    kwargs: dict = {
        'timestamping': timestamping,
        'gc_interval': gc_interval,
        'auto_reconnect': auto_reconnect,
        'probe_interval': probe_interval,
        'retention_ttl': retention_ttl,
    }
    if router is not None:
        kwargs['router'] = router
    if config is not None:
        kwargs['config'] = config
    else:
        kwargs.update(
            zenoh_config=zenoh_config,
            retry=retry,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            max_attempts=max_attempts,
        )
    return _AsyncSessionContextManager(client, kwargs)


def aopen(cfg: SessionConfig) -> _AsyncSessionContextManager:
    """Async-context-managed dispatch on :class:`SessionConfig`. See :func:`apeer`."""
    from . import open as _open

    return _AsyncSessionContextManager(_open, {'cfg': cfg})


async def asend(
    msg: Message,
    *,
    session: zenoh.Session | None = None,
    topic: str | None = None,
    retain: bool | None = None,
) -> None:
    """Async variant of ``msg.send(...)``. Runs the sync send on a thread."""
    await asyncio.to_thread(
        msg.send,
        session=session,
        topic=topic,
        retain=retain,
    )


async def asend_batch(
    cls: type[Message],
    items: Iterable[Message],
    *,
    session: zenoh.Session | None = None,
    topic: str | None = None,
    retain: bool | None = None,
) -> None:
    """Async variant of ``Cls.send_batch(...)``."""
    await asyncio.to_thread(
        cls.send_batch,
        list(items),
        session=session,
        topic=topic,
        retain=retain,
    )


async def aunretain(
    cls_or_msg: Message | type[Message],
    *,
    session: zenoh.Session | None = None,
    topic: str | None = None,
    **key_fields: Any,
) -> None:
    """Async variant of ``msg.unretain()`` / ``Cls.unretain(**)``.

    Pass either a ``Message`` instance (uses its template fields) or a
    ``Message`` subclass (key fields supplied as kwargs).
    """
    from .message import Message

    if isinstance(cls_or_msg, Message):
        await asyncio.to_thread(
            cls_or_msg.unretain,
            session=session,
            topic=topic,
        )
    else:
        await asyncio.to_thread(
            cls_or_msg.unretain,
            session=session,
            topic=topic,
            **key_fields,
        )


async def afetch_retained(
    cls: type[Message],
    *,
    session: zenoh.Session | None = None,
    on_error: Callable[[Exception, bytes], None] | None = None,
) -> list:
    """Async variant of ``Cls.fetch_retained(...)``. Runs the sync fetch on a thread (Zenoh's ``get`` is blocking)."""
    return await asyncio.to_thread(
        cls.fetch_retained,
        session=session,
        on_error=on_error,
    )


async def alisten(
    cls: type[Message],
    *,
    session: zenoh.Session | None = None,
    maxsize: int = 0,
    meta: bool = False,
) -> AsyncIterator:
    """Async generator yielding decoded messages for ``cls``.

    Bridges the sync ``on_message`` callback to an ``asyncio.Queue`` fed via
    ``loop.call_soon_threadsafe``. Cancellation or a break out of the loop
    undeclares the underlying subscriber cleanly.

    ``maxsize=0`` (default) means an unbounded queue; set a positive value
    to apply backpressure (delivery blocks when the queue is full, which
    for Zenoh means dropping to zenoh's internal buffering).

    ``meta=True`` yields ``(msg, meta)`` tuples instead of bare messages —
    ``meta`` is the same ``ZenohMeta`` a 2-arg ``on_message`` callback
    receives (captures, schema, ``origin``, ...).
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
    loop = asyncio.get_running_loop()

    if meta:

        def _cb(msg: Message, m: ZenohMeta) -> None:
            loop.call_soon_threadsafe(queue.put_nowait, (msg, m))
    else:

        def _cb(msg: Message) -> None:
            loop.call_soon_threadsafe(queue.put_nowait, msg)

    sub = cls.on_message(_cb, session=session)
    try:
        while True:
            yield await queue.get()
    finally:
        sub.close()


async def aquery(  # noqa: PLR0913
    cls: type[Message],
    *,
    session: zenoh.Session | None = None,
    params: dict | None = None,
    request: Any = None,
    # Mirrors the sync `Cls.query(timeout=...)` public API; swapping it for
    # asyncio.timeout() at the call site would change the documented surface.
    timeout: float | None = None,  # noqa: ASYNC109
    target: Any = None,
    consolidation: Any = None,
    on_error: Callable[[Exception, bytes], None] | None = None,
    **key_fields: Any,
) -> list:
    """Async variant of ``Cls.query(...)``. Runs the blocking get on a thread and returns the decoded reply list."""
    return await asyncio.to_thread(
        lambda: cls.query(
            session=session,
            params=params,
            request=request,
            timeout=timeout,
            target=target,
            consolidation=consolidation,
            on_error=on_error,
            **key_fields,
        ),
    )


async def aquery_one(cls: type[Message], **kwargs: Any) -> Message | None:
    """Async variant of ``Cls.query_one(...)``."""
    return await asyncio.to_thread(lambda: cls.query_one(**kwargs))


async def aon_query(
    cls: type[Message],
    handler: Callable[[QueryContext], Any],
    *,
    session: zenoh.Session | None = None,
    on_error: Callable[[Exception, bytes], None] | None = None,
    auto_reconnect: bool = True,
) -> Queryable:
    """Async entry point for ``Cls.on_query(...)`` — the natural way to register an ``async def`` handler.

    Declared inline on the calling event-loop thread (not offloaded) so an
    ``async def`` handler captures *this* loop for its replies. The declare
    itself is a fast Zenoh call. Returns the :class:`Queryable` handle.
    """
    return cls.on_query(
        handler,
        session=session,
        on_error=on_error,
        auto_reconnect=auto_reconnect,
    )


@asynccontextmanager
async def abatch() -> AsyncIterator[_BatchHandle]:
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
    'afetch_retained',
    'alisten',
    'aon_query',
    'aopen',
    'apeer',
    'aquery',
    'aquery_one',
    'asend',
    'asend_batch',
    'aunretain',
]
