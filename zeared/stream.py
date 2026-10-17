"""``Stream`` — the async iterator ``alisten`` and ``aquery_iter`` return (0.4.0).

Zenoh delivers on its own threads. A ``Stream`` bridges them to one event loop
through an ``asyncio.Queue`` fed with ``loop.call_soon_threadsafe``, so the
queue is only ever touched on the loop thread.

Bounded (``maxsize > 0``), a full stream drops the *oldest* queued item to make
room for the new one: a consumer that has fallen behind on live state wants the
freshest data. Each drop is counted on :attr:`Stream.dropped`, and the first is
logged once; nothing is raised. ``maxsize=0`` is unbounded.

Closing releases the source (the subscriber, or the query worker): explicitly
with :meth:`Stream.aclose` or ``async with``, at the end of a query, or when a
stream abandoned by a ``break`` is collected. The source holds only the stream's
inner channel, never the ``Stream`` itself, so dropping the last reference to
the ``Stream`` is enough to close it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import weakref
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    import types
    from collections.abc import Callable

_log = logging.getLogger('zeared.stream')


class _End:
    """The end-of-stream marker a finite source (a query) feeds last."""

    __slots__ = ()


class _Raise:
    """Carries an exception from the source to the consumer, who re-raises it."""

    __slots__ = ('exc',)

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc


_END = _End()


class _Channel:
    """The loop-side queue and its drop accounting.

    Producers call :meth:`feed` from any thread; everything else runs on the
    loop thread. The source keeps a reference to this, not to the ``Stream``.
    """

    __slots__ = ('dropped', 'label', 'loop', 'maxsize', 'queue', 'warned')

    def __init__(self, loop: asyncio.AbstractEventLoop, maxsize: int, label: str) -> None:
        self.loop = loop
        self.maxsize = maxsize
        self.label = label
        # Unbounded underneath: the bound is enforced in ``_put`` so a full
        # stream can evict its oldest item instead of refusing the newest.
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.dropped = 0
        self.warned = False

    def feed(self, item: object) -> None:
        """Hand ``item`` to the loop. Safe from any thread."""
        with contextlib.suppress(RuntimeError):  # the loop has closed: nobody is left to consume it
            self.loop.call_soon_threadsafe(self._put, item)

    def end(self, exc: BaseException | None = None) -> None:
        """Feed the end marker, after ``exc`` if the source failed. Safe from any thread."""
        if exc is not None:
            self.feed(_Raise(exc))
        self.feed(_END)

    def _put(self, item: object) -> None:
        bounded = self.maxsize > 0 and not isinstance(item, (_End, _Raise))
        if bounded and self.queue.qsize() >= self.maxsize:
            self.queue.get_nowait()  # the oldest
            self.dropped += 1
            if not self.warned:
                self.warned = True
                _log.warning(
                    '%s: stream full (maxsize=%d); dropping the oldest item to make room. '
                    'Further drops are counted on Stream.dropped and not logged.',
                    self.label,
                    self.maxsize,
                )
        self.queue.put_nowait(item)


class Stream[T]:
    """An async iterator over messages delivered from Zenoh's threads.

    ``async for item in stream`` yields each item. :attr:`dropped` counts the
    items a full, bounded stream discarded, oldest first. :meth:`aclose` (or
    leaving ``async with``) releases the source; so does the stream being
    collected after a ``break``.
    """

    __slots__ = ('__weakref__', '_channel', '_closed', '_finalizer')

    def __init__(self, channel: _Channel, close: Callable[[], None]) -> None:
        """Wrap ``channel``; ``close`` releases the source, once."""
        self._channel = channel
        self._closed = False
        # ``close`` must not reference this ``Stream``, or the stream could
        # never be collected and an abandoned one would never close.
        self._finalizer = weakref.finalize(self, close)

    @property
    def dropped(self) -> int:
        """How many items a full stream has discarded so far (oldest first)."""
        return self._channel.dropped

    @property
    def maxsize(self) -> int:
        """The bound, or ``0`` for an unbounded stream."""
        return self._channel.maxsize

    def __aiter__(self) -> Self:
        """Return the stream itself: it is its own iterator."""
        return self

    async def __anext__(self) -> T:
        """Wait for the next item; end once the stream is closed or its source ends."""
        if self._closed:
            raise StopAsyncIteration
        item = await self._channel.queue.get()
        if isinstance(item, _End):
            await self.aclose()
            raise StopAsyncIteration
        if isinstance(item, _Raise):
            await self.aclose()
            raise item.exc
        return item

    async def aclose(self) -> None:
        """Release the source. Idempotent."""
        self._closed = True
        self._finalizer()

    async def __aenter__(self) -> Self:
        """Enter ``async with``: the stream itself."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        """Leave ``async with``: close the stream."""
        await self.aclose()


def _open_channel(maxsize: int, label: str) -> _Channel:
    """A channel on the running loop. Raises ``RuntimeError`` without one."""
    return _Channel(asyncio.get_running_loop(), maxsize, label)
