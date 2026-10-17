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
import contextlib
import threading
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Literal, overload

from ._factories import _MISSING
from .batch import batch as _batch_cm
from .stream import Stream, _open_channel

if TYPE_CHECKING:
    import types
    from collections.abc import AsyncIterator, Callable, Iterable

    import zenoh

    from .batch import _BatchHandle
    from .config import SessionConfig
    from .message import Message
    from .meta import ZenohMeta
    from .queryable import Queryable, QueryContext

# The query wrappers and ``afetch_retained`` below are generic in the
# message class, using PEP 695 type parameters. Their bounds are lazily
# evaluated, so ``Message`` staying a TYPE_CHECKING-only import is fine.
#
# The point is to carry the element type through from the sync surface —
# each wraps a method that is already typed in terms of its own message
# class — rather than flattening it to a bare ``list`` / ``Message | None``
# and pushing a ``cast`` onto every caller.
#
# ``alisten`` gets overloads instead (0.4.0): its ``meta=`` flag decides the
# element type, ``Stream[M]`` or ``Stream[tuple[M, ZenohMeta]]``, which a
# single type parameter can't express.


def _given(**kwargs: Any) -> dict[str, Any]:
    """The keyword arguments the caller actually passed: the rest defer to ``config=``."""
    return {k: v for k, v in kwargs.items() if v is not _MISSING}


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
    auto_reconnect: object = _MISSING,
    probe_interval: object = _MISSING,
    timestamping: bool = True,
    retention_ttl: float | None = None,
    resync_jitter: object = _MISSING,
    resync_concurrency: object = _MISSING,
) -> _AsyncSessionContextManager:
    """Async-context-managed peer session.

    Returns an async context manager — use as
    ``async with z.apeer(...) as sess: ...``. The ``await z.apeer()``
    form from ≤0.0.14 is removed (pre-0.1.0 break).
    """
    from . import peer

    kwargs: dict = {
        'timestamping': timestamping,
        'retention_ttl': retention_ttl,
        **_given(
            auto_reconnect=auto_reconnect,
            probe_interval=probe_interval,
            resync_jitter=resync_jitter,
            resync_concurrency=resync_concurrency,
        ),
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
    auto_reconnect: object = _MISSING,
    probe_interval: object = _MISSING,
    timestamping: bool = True,
    retention_ttl: float | None = None,
    resync_jitter: object = _MISSING,
    resync_concurrency: object = _MISSING,
) -> _AsyncSessionContextManager:
    """Async-context-managed client session. See :func:`apeer`."""
    from . import client

    kwargs: dict = {
        'timestamping': timestamping,
        'retention_ttl': retention_ttl,
        **_given(
            auto_reconnect=auto_reconnect,
            probe_interval=probe_interval,
            resync_jitter=resync_jitter,
            resync_concurrency=resync_concurrency,
        ),
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


async def afetch_retained[M: Message](
    cls: type[M],
    *,
    session: zenoh.Session | None = None,
    on_error: Callable[[Exception, bytes], None] | None = None,
) -> list[M]:
    """Async variant of ``Cls.fetch_retained(...)``. Runs the sync fetch on a thread (Zenoh's ``get`` is blocking)."""
    # Called through a lambda rather than passed as a bound method: a
    # method reference handed to ``to_thread`` erases the link between
    # ``fetch_retained``'s own type parameter and this one. Same shape as
    # ``aquery`` below, for the same reason.
    return await asyncio.to_thread(
        lambda: cls.fetch_retained(session=session, on_error=on_error),
    )


@overload
def alisten[M: Message](
    cls: type[M],
    *,
    session: zenoh.Session | None = ...,
    maxsize: int = ...,
    meta: Literal[False] = ...,
) -> Stream[M]: ...


@overload
def alisten[M: Message](
    cls: type[M],
    *,
    session: zenoh.Session | None = ...,
    maxsize: int = ...,
    meta: Literal[True],
) -> Stream[tuple[M, ZenohMeta]]: ...


@overload
def alisten[M: Message](
    cls: type[M],
    *,
    session: zenoh.Session | None = ...,
    maxsize: int = ...,
    meta: bool = ...,
) -> Stream[M] | Stream[tuple[M, ZenohMeta]]: ...


def alisten(
    cls: type[Message],
    *,
    session: zenoh.Session | None = None,
    maxsize: int = 1024,
    meta: bool = False,
) -> Stream[Any]:
    """Subscribe to ``cls`` and return a :class:`~zeared.Stream` of its decoded messages.

    ``async for msg in z.alisten(Cls): ...``. The subscription is declared
    when ``alisten`` is called, so call it from inside a running event loop;
    what arrives before the first iteration waits in the stream.

    ``maxsize`` bounds the stream (default 1024; ``0`` is unbounded). A full
    stream drops its *oldest* message to make room, counts the drop on
    ``stream.dropped`` and logs the first one; nothing is raised (0.4.0).
    Delivery never applies backpressure: Zenoh's callback thread never waits.

    ``meta=True`` yields ``(msg, meta)`` tuples instead of bare messages —
    ``meta`` is the same ``ZenohMeta`` a 2-arg ``on_message`` callback
    receives (captures, schema, ``origin``, ...).

    ``stream.aclose()``, leaving ``async with stream``, or the stream being
    collected after a ``break`` undeclares the subscriber.
    """
    channel = _open_channel(maxsize, cls.__name__)
    feed = channel.feed  # the subscriber holds the channel, never the Stream

    if meta:

        def _cb(msg: Message, m: ZenohMeta) -> None:
            feed((msg, m))
    else:

        def _cb(msg: Message) -> None:
            feed(msg)

    sub = cls.on_message(_cb, session=session)
    return Stream(channel, sub.close)


async def aquery[M: Message](  # noqa: PLR0913
    cls: type[M],
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
) -> list[M]:
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


def aquery_iter[M: Message](  # noqa: PLR0913
    cls: type[M],
    *,
    session: zenoh.Session | None = None,
    params: dict | None = None,
    request: Any = None,
    # Mirrors the sync `Cls.iter_query(timeout=...)` public API.
    timeout: float | None = None,
    target: Any = None,
    consolidation: Any = None,
    on_error: Callable[[Exception, bytes], None] | None = None,
    **key_fields: Any,
) -> Stream[M]:
    """Issue a query and return a :class:`~zeared.Stream` of its decoded replies as they arrive.

    ``async for row in z.aquery_iter(Cls, id='x'): ...`` — the streaming
    sibling of :func:`aquery`, which collects the whole list before
    returning. The query starts when ``aquery_iter`` is called, so call it
    from inside a running event loop.

    Zenoh's bindings are sync, so the blocking channel loop runs in a
    worker thread and hands items to the loop via ``call_soon_threadsafe``
    — the same bridge :func:`alisten` uses for subscribers. Unlike a
    subscriber a query *ends*, and the stream ends with it; an exception
    raised in the worker is re-raised in the consumer.

    Closing the stream early (``aclose()``, ``async with``, or a ``break``)
    stops this side promptly — the worker is told to stop and exits at the
    next reply or the timeout, whichever comes first — but it does **not**
    stop the serving queryable, which runs to completion. Zenoh's
    cancellation is client-side only.

    The stream is unbounded, so ``stream.dropped`` stays ``0``: replies are
    already in flight from the channel, so bounding the queue would relocate
    buffering rather than apply backpressure. The bound that matters is
    ``timeout``.
    """
    channel = _open_channel(0, cls.__name__)
    stop = threading.Event()

    def _pump() -> None:
        # Runs on a worker thread; every hand-off goes through the loop.
        failure: BaseException | None = None
        try:
            for msg in cls.iter_query(
                session=session,
                params=params,
                request=request,
                timeout=timeout,
                target=target,
                consolidation=consolidation,
                on_error=on_error,
                **key_fields,
            ):
                if stop.is_set():
                    return
                channel.feed(msg)
        except Exception as exc:  # noqa: BLE001
            # Surface it to the consumer rather than losing it on a detached thread.
            failure = exc
        channel.end(failure)

    worker = asyncio.ensure_future(asyncio.to_thread(_pump))

    def _close() -> None:
        # ``asyncio.to_thread`` can't interrupt a blocked worker, so ask it to
        # stand down; it notices at the next reply or the timeout.
        stop.set()
        if not worker.done():
            with contextlib.suppress(RuntimeError):  # the loop has closed (interpreter shutdown)
                worker.cancel()

    return Stream(channel, _close)


async def aquery_one[M: Message](cls: type[M], **kwargs: Any) -> M | None:
    """Async variant of ``Cls.query_one(...)``.

    Inherits the short-circuit: returns at the first decoded reply rather
    than waiting out ``timeout`` (0.3.4).
    """
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

    ``async def`` **generator** handlers are supported too: each yielded
    instance is replied as it is produced, giving a streaming multi-reply
    handler that never materialises its result set.
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
    'aquery_iter',
    'aquery_one',
    'asend',
    'asend_batch',
    'aunretain',
]
