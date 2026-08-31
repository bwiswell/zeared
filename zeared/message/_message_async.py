"""Async publish and subscribe siblings for :class:`Message`.

``_MessageAsyncMixin`` — async publish + subscribe siblings (``asend`` / ``asend_batch``
/ ``aunretain`` / ``alisten``).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    import zenoh

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

    @classmethod
    def alisten(
        cls: type[Message],
        *,
        session: zenoh.Session | None = None,
        maxsize: int = 0,
        meta: bool = False,
    ) -> AsyncIterator:
        """Async-iterator subscriber. ``async for msg in Cls.alisten(): ...``.

        Each incoming sample is decoded and delivered through an
        ``asyncio.Queue`` bridging from the Zenoh callback thread. Break
        out of the loop (or cancel the iterating task) to close cleanly.

        ``meta=True`` yields ``(msg, meta)`` tuples — ``meta`` is the same
        ``ZenohMeta`` a 2-arg ``on_message`` callback receives (captures,
        schema, ``origin``, ...).
        """
        from ..async_ import alisten

        return alisten(cls, session=session, maxsize=maxsize, meta=meta)
