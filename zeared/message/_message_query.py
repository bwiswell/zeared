"""``_MessageQueryMixin`` — the query/queryable surface on :class:`Message`
(``on_query`` serving side + ``query`` / ``query_one`` getting side).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
The serving side is the compute-analogue of ``RETAINED`` (which serves a
*cached* value); the getting side is the typed sibling of ``send`` +
retained-fetch.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, List, Optional, Type, TypeVar

if TYPE_CHECKING:
    import zenoh

    from ..queryable import QueryContext, Queryable
    from .message import Message


_M = TypeVar('_M', bound='Message')


class _MessageQueryMixin:
    """Query / queryable surface on :class:`Message`."""
    __slots__ = ()

    @classmethod
    def on_query(
        cls: 'Type[_M]',
        handler: 'Callable[[QueryContext], Any]',
        *,
        session: Optional['zenoh.Session'] = None,
        on_error: Optional[Callable[[Exception, bytes], None]] = None,
        auto_reconnect: bool = True,
    ) -> "'Queryable[_M]'":
        """Declare a queryable answering ``session.get()`` on this class's
        topic(s) — all declared templates.

        ``handler`` receives a :class:`QueryContext` (the request key,
        parsed selector params, template captures, and optional decoded
        request payload). It may either **return** a message instance, an
        iterable of instances, or ``None``; or reply explicitly via
        ``ctx.reply`` / ``ctx.reply_err`` / ``ctx.reply_del`` (for
        multi-reply, streaming, or error cases) and return ``None``.

        ``async def`` handlers are scheduled on the loop running at
        ``on_query`` time; the query stays live until the coroutine
        resolves, then its return value is replied.

        Raises ``TopicError`` on a ``RETAINED = True`` class — retention
        already serves a queryable over the same topic.

        Returns a :class:`Queryable`; close via ``.close()`` or as a
        context manager. Survives reconnect when ``auto_reconnect`` (the
        default) and the session is managed.
        """
        from ..queryable import Queryable

        import zeared as z

        sess = z.session.resolve(session)
        return Queryable._declare(
            cls, sess, handler, on_error, auto_reconnect=auto_reconnect,
        )

    @classmethod
    def query(
        cls: 'Type[_M]',
        *,
        session: Optional['zenoh.Session'] = None,
        params: Optional[dict] = None,
        request: Optional['Message'] = None,
        timeout: Optional[float] = None,
        target: Any = None,
        consolidation: Any = None,
        on_error: Optional[Callable[[Exception, bytes], None]] = None,
        **key_fields: Any,
    ) -> 'List[_M]':
        """Query peer queryables for this class and return decoded replies.

        ``key_fields`` fill the canonical template's slots; omitted slots
        widen to wildcards (``query()`` with none asks the whole template
        wildcard, ``query(id='42')`` narrows). Embed ``*`` in a value for
        a partial wildcard (``query(epc='E280*')``).

        ``params`` is appended to the selector as ``?k=v&…``. ``request``
        (a ``REQUEST``-class instance) is sent as the query payload.
        ``timeout`` / ``target`` / ``consolidation`` pass through to the
        underlying ``session.get``. Blocks up to ``timeout``.

        Error replies and per-reply decode failures route to ``on_error``
        (``on_error(exc, raw)``) when supplied, else log; the returned list
        contains only successfully decoded instances (possibly empty).
        """
        from ..queryable._query_get import _run_query

        import zeared as z

        sess = z.session.resolve(session)
        return _run_query(
            sess, cls, key_fields,
            params=params, request=request,
            timeout=timeout, target=target, consolidation=consolidation,
            on_error=on_error,
        )

    @classmethod
    def query_one(
        cls: 'Type[_M]',
        **kwargs: Any,
    ) -> 'Optional[_M]':
        """``query`` convenience returning the first reply (arrival order)
        or ``None`` when nobody answered.

        Non-deterministic when several queryables answer — the first
        decoded reply wins. Accepts every ``query`` keyword.
        """
        results = cls.query(**kwargs)
        return results[0] if results else None
