"""Async publish and subscribe siblings for :class:`Message`.

``_MessageAsyncMixin`` — async publish + subscribe siblings (``asend`` / ``asend_batch``
/ ``aunretain`` / ``alisten``).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

if TYPE_CHECKING:
    from collections.abc import Iterable

    import zenoh

    from ..meta import ZenohMeta
    from ..stream import Stream
    from .message import Message


class _MessageAsyncMixin:
    """Async siblings of the sync publish/subscribe surface on :class:`Message`."""

    __slots__ = ()

    async def asend(
        self: Message,
        *,
        session: zenoh.Session | None = None,
        topic: str | None = None,
        retain: bool | None = None,
    ) -> None:
        """Async counterpart of :meth:`send`.

        Dispatches the sync send on a thread pool worker so an asyncio event loop stays
        unblocked.
        """
        from ..async_ import asend

        await asend(self, session=session, topic=topic, retain=retain)

    @classmethod
    async def asend_batch(
        cls: type[Message],
        items: Iterable[Message],
        *,
        session: zenoh.Session | None = None,
        topic: str | None = None,
        retain: bool | None = None,
    ) -> None:
        """Async counterpart of :meth:`send_batch`."""
        from ..async_ import asend_batch

        await asend_batch(
            cls,
            items,
            session=session,
            topic=topic,
            retain=retain,
        )

    async def aunretain(
        self: Message,
        *,
        session: zenoh.Session | None = None,
        topic: str | None = None,
    ) -> None:
        """Async counterpart of :meth:`unretain` (instance form)."""
        import asyncio

        await asyncio.to_thread(self.unretain, session=session, topic=topic)

    @overload
    @classmethod
    def alisten[M: Message](
        cls: type[M],
        *,
        session: zenoh.Session | None = ...,
        maxsize: int = ...,
        meta: Literal[False] = ...,
    ) -> Stream[M]: ...

    @overload
    @classmethod
    def alisten[M: Message](
        cls: type[M],
        *,
        session: zenoh.Session | None = ...,
        maxsize: int = ...,
        meta: Literal[True],
    ) -> Stream[tuple[M, ZenohMeta]]: ...

    @overload
    @classmethod
    def alisten[M: Message](
        cls: type[M],
        *,
        session: zenoh.Session | None = ...,
        maxsize: int = ...,
        meta: bool = ...,
    ) -> Stream[M] | Stream[tuple[M, ZenohMeta]]: ...

    @classmethod
    def alisten(
        cls: type[Message],
        *,
        session: zenoh.Session | None = None,
        maxsize: int = 1024,
        meta: bool = False,
    ) -> Stream[Any]:
        """Async-iterator subscriber. ``async for msg in Cls.alisten(): ...``.

        Returns a :class:`~zeared.Stream`; see :func:`zeared.alisten` for the
        bound (default 1024, drop-oldest, counted on ``stream.dropped``),
        ``meta=True`` and how the stream closes.
        """
        from ..async_ import alisten

        return alisten(cls, session=session, maxsize=maxsize, meta=meta)
