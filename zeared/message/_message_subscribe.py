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
    ) -> "'Subscriber[_M]'":
        """Subscribe to this message's topic(s) — all declared templates.

        ``cb`` may be ``cb(msg)`` or ``cb(msg, meta)``; arity is inspected once
        at subscribe time. ``meta`` is a ``ZenohMeta`` seared dataclass.

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
        )
