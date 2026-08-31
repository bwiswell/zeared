"""``_MessageSubscribeMixin`` — subscriber + introspection surface
(``on_message`` + ``published_topics``).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
``_decode`` stays on the primary :class:`Message` class because both
``Subscriber`` (via the dispatch closure) and the introspection layer
import it directly.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Type, TypeVar, Union

if TYPE_CHECKING:
    import zenoh

    from ..meta import ZenohMeta
    from ..subscriber import Subscriber
    from .message import Message


_M = TypeVar('_M', bound='Message')


class _MessageSubscribeMixin:
    """Subscribe + introspection surface on :class:`Message`."""
    __slots__ = ()

    @classmethod
    def published_topics(
        cls,
        *,
        session: Optional['zenoh.Session'] = None,
    ) -> frozenset:
        """Snapshot of concrete topics this class has published on the given
        session (or aggregated across all sessions when ``session=None``).

        Includes topics that have since been tombstoned via ``unretain()``
        and topics that bypassed the publisher cache (e.g. ``PUBLISHER =
        False`` classes). Intended for dashboards and diagnostic tooling.
        """
        from ..publisher import published_topics as _pt
        per_session = _pt(cls=cls, session=session)
        out: set[str] = set()
        for topics in per_session.values():
            out.update(topics)
        return frozenset(out)

    @classmethod
    def on_message(
        cls: 'Type[_M]',
        cb: 'Union[Callable[[_M], None], Callable[[_M, ZenohMeta], None]]',
        *,
        session: Optional['zenoh.Session'] = None,
        on_error: Optional[Callable[[Exception, bytes], None]] = None,
        expected_interval: Optional[float] = None,
        on_quiet: Optional[Callable] = None,
        on_active: Optional[Callable] = None,
        startup_grace: Optional[float] = None,
        auto_reconnect: bool = True,
        dedupe: Optional[bool] = None,
        on_remove: 'Optional[Callable[[ZenohMeta], None]]' = None,
        retained_fetch: bool = True,
    ) -> "'Subscriber[_M]'":
        """Subscribe to this message's topic(s) — all declared templates.

        ``cb`` may be ``cb(msg)`` or ``cb(msg, meta)``; arity is inspected once
        at subscribe time. ``meta`` is a ``ZenohMeta`` seared dataclass.
        ``meta.origin`` carries the sample's provenance — ``Origin.LIVE``
        for a real publish, ``Origin.REPLAY`` for a retained-fetch delivery
        (subscribe-time or post-reconnect), ``Origin.WILL`` for a
        presence-synthesised will.

        ``retained_fetch`` (default ``True``) controls the subscribe-time
        retained fetch on ``RETAINED`` classes. Pass ``False`` for a
        live-only subscription: no cached values are replayed at subscribe
        time or after a reconnect, and the blocking ``session.get`` those
        fetches issue is skipped. By the time ``on_message`` returns with
        the default, every subscribe-time replay has been delivered (or
        dedupe-suppressed) — everything after is live or marked.

        ``on_remove`` (optional) is the tombstone feed: it fires on DELETE
        samples — a peer's ``unretain()`` or any ``session.delete`` on a
        matching key — which ``cb`` never sees. It receives a ``ZenohMeta``
        whose ``captures`` hold the removed key's template slots (raw
        strings; coerce with :meth:`coerce_captures`). A tombstone carries
        no payload, so there is no typed instance to hand back — identity is
        all a removal conveys. Use it to drive removal-side reconcile;
        pair it with periodic :meth:`fetch_retained` for a durable set,
        since a subscriber offline during the DELETE never sees it.
        ``async def`` on_remove is supported (scheduled like an async ``cb``).

        ``expected_interval`` (seconds, optional) opts into a per-subscription
        watchdog. ``on_quiet`` fires the first time no message arrives within
        the interval after a previous message; ``on_active`` fires on the
        next message after a quiet period. Watchdog callbacks fire on a
        dedicated watchdog thread, **not** on the Zenoh delivery thread —
        code that mutates shared state must handle this.

        The watchdog is **optimistic by default**: it doesn't start until
        the first message arrives. A subscription that never receives
        anything never fires ``on_quiet``.

        For "tell me if I haven't heard anything within N seconds of
        subscribing" semantics, pass ``startup_grace=N``: the watchdog
        starts immediately, and ``on_quiet`` fires once if no message has
        arrived after ``startup_grace`` seconds. After the first message
        arrives (or the grace window expires), subsequent waits use
        ``expected_interval`` as usual.
        """
        from ..subscriber import Subscriber  # local import: forward reference

        import zeared as z

        sess = z.session.resolve(session)
        return Subscriber._declare(
            cls, sess, cb, on_error,
            expected_interval=expected_interval,
            on_quiet=on_quiet,
            on_active=on_active,
            startup_grace=startup_grace,
            auto_reconnect=auto_reconnect,
            dedupe=dedupe,
            on_remove=on_remove,
            retained_fetch=retained_fetch,
        )

    @classmethod
    def coerce_captures(cls: 'type[Message]', captures: dict) -> dict:
        """Coerce raw string template captures through their declared fields.

        ``on_remove`` (and ``meta.captures`` generally) hands back template
        slots as raw strings. This runs each slot that maps to a declared
        seared field through that field's ``deserialize`` — so a
        ``{reader_id}`` slot bound to ``z.Int`` comes back as ``int`` — and
        passes capture-only slots (no matching field) through unchanged.
        Handy for keying removal-reconcile on typed identity.
        """
        spec_by_attr = {attr: f for attr, _, f in cls.__seared_fields__}
        out: dict = {}
        for name, raw_val in captures.items():
            f = spec_by_attr.get(name)
            out[name] = (
                f.deserialize(raw_val, validate=True) if f is not None
                else raw_val
            )
        return out

    @classmethod
    def fetch_retained(
        cls: 'Type[_M]',
        *,
        session: Optional['zenoh.Session'] = None,
        on_error: Optional[Callable[[Exception, bytes], None]] = None,
    ) -> 'list[_M]':
        """One-shot typed snapshot of this class's current retained set.

        Issues ``session.get`` across every declared template wildcard,
        decodes each OK reply through the class's own decode path, and
        returns the typed instances. This is the reliable reconcile path:
        ``on_remove`` gives low-latency incremental removals but a
        subscriber offline during a DELETE misses it permanently (the
        retained value is already gone, no tombstone is replayed), so a
        periodic ``fetch_retained`` → reconcile-against-set closes that
        hole. Requires ``RETAINED = True`` (only retained classes serve a
        queryable to fetch from).

        Decode failures and error replies route to ``on_error`` when
        supplied, else log; the returned list holds only decoded results.
        Duplicate keys (multiple publishers retaining the same concrete
        topic) are returned as-is — dedupe by identity at the call site.
        """
        from ..errors import TopicError

        if not getattr(cls, 'RETAINED', False):
            raise TopicError(
                f'{cls.__name__}.fetch_retained requires RETAINED = True'
            )
        import zeared as z

        from ..subscriber._subscriber_retained_fetch import _collect_retained

        sess = z.session.resolve(session)
        return _collect_retained(sess, cls._templates().all, cls, on_error)
