"""``_MessageAsyncMixin`` — async publish + subscribe siblings
(``asend`` / ``asend_batch`` / ``aunretain`` / ``alisten``).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    import zenoh

    from .message import Message


class _MessageAsyncMixin:
    """Async siblings of the sync publish/subscribe surface on :class:`Message`."""
    __slots__ = ()

    async def asend(
        self,
        *,
        session: Optional['zenoh.Session'] = None,
        topic: Optional[str] = None,
        retain: Optional[bool] = None,
    ) -> None:
        """Async counterpart of :meth:`send`. Dispatches the sync send on
        a thread pool worker so an asyncio event loop stays unblocked.
        """
        from ..async_ import asend
        await asend(self, session=session, topic=topic, retain=retain)

    @classmethod
    async def asend_batch(
        cls,
        items: Iterable['Message'],
        *,
        session: Optional['zenoh.Session'] = None,
        topic: Optional[str] = None,
        retain: Optional[bool] = None,
    ) -> None:
        """Async counterpart of :meth:`send_batch`."""
        from ..async_ import asend_batch
        await asend_batch(
            cls, items, session=session, topic=topic, retain=retain,
        )

    async def aunretain(
        self,
        *,
        session: Optional['zenoh.Session'] = None,
        topic: Optional[str] = None,
    ) -> None:
        """Async counterpart of :meth:`unretain` (instance form)."""
        import asyncio
        await asyncio.to_thread(self.unretain, session=session, topic=topic)

    @classmethod
    def alisten(
        cls,
        *,
        session: Optional['zenoh.Session'] = None,
        maxsize: int = 0,
    ):
        """Async-iterator subscriber. ``async for msg in Cls.alisten(): ...``.

        Each incoming sample is decoded and delivered through an
        ``asyncio.Queue`` bridging from the Zenoh callback thread. Break
        out of the loop (or cancel the iterating task) to close cleanly.
        """
        from ..async_ import alisten
        return alisten(cls, session=session, maxsize=maxsize)
