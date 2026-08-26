"""Zeared subscription handle — the ``Subscriber`` class.

Primary file of the ``subscriber`` Pattern B subdir. Per-sample
dispatch plumbing lives in ``_subscriber_dispatch.py``; the
retained-fetch helper lives in ``_subscriber_retained_fetch.py``;
the module-level subscriber registry used by
``z.release(session=)`` lives in ``_subscriber_registry.py``.
"""
from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Callable, Generic, Optional, Type, TypeVar

from .._managed_session import resolve_raw
from ..errors import SubscriptionError
from ._subscriber_dispatch import (
    _adapt_async_callback,
    _build_dispatch,
    _make_presence_dispatcher,
    _wants_meta,
)
from ._subscriber_registry import (
    _SCHEMA_MISMATCH_CACHE_MAX,
    _deregister_subscriber,
    _register_subscriber,
)
from ._subscriber_retained_fetch import _fetch_retained

if TYPE_CHECKING:
    import zenoh

    from ..message import Message


# Type parameter for generic-parameterised ``Subscriber[Cls]``. Bound to
# ``Message`` so callers can't accidentally parameterise with a non-message
# type. Runtime is identical to a non-parameterised ``Subscriber``.
M = TypeVar('M', bound='Message')


class Subscriber(Generic[M]):
    """Zeared subscription handle.

    Wraps N underlying ``zenoh.Subscriber`` instances — one per declared
    topic template on the message class. For ``LIVELINESS = True`` classes,
    also holds a presence-dispatcher registration that fires synthesised
    samples when a peer's liveliness token disappears.

    Close via ``.close()`` or as a context manager. Close is idempotent.

    Generic in the message class for IDE ergonomics —
    ``Cls.on_message(cb)`` returns ``Subscriber[Cls]``. The type parameter
    is type-only; runtime behavior is identical with or without it
    (bare ``Subscriber()`` and ``Subscriber[Cls]()`` are equivalent at
    runtime).
    """

    __slots__ = (
        '_zenoh_subs', '_session', '_presence_dispatcher',
        '_watchdog', '_closed',
        # Redeclaration state — populated by `_declare`, used on reconnect.
        '_msg_cls', '_dispatch', '_on_error', '_auto_reconnect',
        '_seen_mismatches',
    )

    def __init__(
        self,
        zenoh_subs: tuple,
        session=None,
        presence_dispatcher=None,
        watchdog=None,
    ):
        self._zenoh_subs = zenoh_subs
        self._session = session
        self._presence_dispatcher = presence_dispatcher
        self._watchdog = watchdog
        self._closed = False
        # Filled in by `_declare` — kept on the instance so the reconnect
        # machinery can rebuild the underlying zenoh subs against a new raw.
        self._msg_cls = None
        self._dispatch = None
        self._on_error = None
        self._auto_reconnect = True
        # Pointer to the dispatch closure's schema-mismatch cache; held on
        # the instance so reconnect can clear it (peer zids may have
        # changed; stale entries would silently drop legit mismatches).
        self._seen_mismatches: 'Optional[OrderedDict[tuple, None]]' = None

    @classmethod
    def _declare(
        cls,
        msg_cls: Type['Message'],
        session: 'zenoh.Session',
        cb: Callable[..., None],
        on_error: Optional[Callable[[Exception, bytes], None]],
        expected_interval: Optional[float] = None,
        on_quiet: Optional[Callable] = None,
        on_active: Optional[Callable] = None,
        startup_grace: Optional[float] = None,
        auto_reconnect: bool = True,
        dedupe: Optional[bool] = None,
    ) -> 'Subscriber':
        tpls = msg_cls._templates()
        cb = _adapt_async_callback(cb)
        wants_meta = _wants_meta(cb)

        # Optional per-subscription watchdog.
        watchdog = None
        if expected_interval is not None:
            from ..watchdog import _SubscriberWatchdog
            watchdog = _SubscriberWatchdog(
                expected_interval, on_quiet, on_active,
                startup_grace=startup_grace,
            )

        # Retention dedupe state — only active when both RETAINED and the
        # effective DEDUPE flag are True. Per-subscriber ``dedupe=`` kwarg
        # overrides class-level ``DEDUPE`` when explicit (``dedupe=None``
        # falls through to the class default). Maps key_expr → last seen
        # timestamp string. Zenoh timestamps are HLC-formatted, so
        # lexicographic ordering matches temporal ordering.
        class_dedupe = getattr(msg_cls, 'DEDUPE', True)
        effective_dedupe = dedupe if dedupe is not None else class_dedupe
        dedupe_active = (
            getattr(msg_cls, 'RETAINED', False) and effective_dedupe
        )
        seen_ts: 'dict[str, str]' = {}

        # Schema-mismatch warn-once cache — bounded ``OrderedDict``
        # keyed on (sender_zid, observed_schema). Cleared on close and
        # on `_redeclare` (post-reconnect; peer zids change).
        expected_schema = getattr(msg_cls, 'SCHEMA', None)
        seen_mismatches: 'OrderedDict[tuple, None]' = OrderedDict()

        dispatch = _build_dispatch(
            msg_cls, on_error, cb,
            wants_meta=wants_meta,
            dedupe_active=dedupe_active,
            expected_schema=expected_schema,
            seen_mismatches=seen_mismatches,
            seen_ts=seen_ts,
            watchdog=watchdog,
            schema_mismatch_cache_max=_SCHEMA_MISMATCH_CACHE_MAX,
        )

        # Internal declaration — route through the underlying raw to
        # avoid the user-facing declare_* RuntimeWarning. zeared rebuilds
        # these handles itself across reconnects via Subscriber._redeclare.
        raw = resolve_raw(session)

        zenoh_subs: list = []
        try:
            for tpl in tpls.all:
                zenoh_subs.append(raw.declare_subscriber(tpl.wildcard, dispatch))
        except Exception as e:  # noqa: BLE001
            for sub in zenoh_subs:
                try:
                    sub.undeclare()
                except Exception:  # noqa: BLE001
                    pass
            raise SubscriptionError(
                f'{msg_cls.__name__}: failed to declare subscriber: {e}'
            ) from e

        # Retained-fetch: for RETAINED classes, pull any cached values from
        # peer queryables via session.get() on each declared wildcard. Reply
        # samples flow through the same dispatch as live samples.
        if getattr(msg_cls, 'RETAINED', False):
            _fetch_retained(session, tpls.all, dispatch, msg_cls, on_error)

        # Presence-aware subscribers: for LIVELINESS classes, register an
        # interested-party dispatcher with the per-session presence observer.
        # The observer fires synthesised samples through ``dispatch`` when a
        # peer's liveliness token disappears.
        presence_dispatcher = None
        if getattr(msg_cls, 'LIVELINESS', False):
            presence_dispatcher = _make_presence_dispatcher(
                msg_cls, tpls, dispatch,
            )
            from ..presence import get_observer
            observer = get_observer(session)
            observer.start()
            observer.register(presence_dispatcher)

        sub_handle = cls(
            tuple(zenoh_subs),
            session=session,
            presence_dispatcher=presence_dispatcher,
            watchdog=watchdog,
        )
        sub_handle._msg_cls = msg_cls
        sub_handle._dispatch = dispatch
        sub_handle._on_error = on_error
        sub_handle._auto_reconnect = auto_reconnect
        sub_handle._seen_mismatches = seen_mismatches
        _register_subscriber(session, sub_handle)
        return sub_handle

    def _redeclare(self, new_raw_session, managed_session) -> None:
        """Rebuild the underlying ``zenoh.Subscriber`` set against
        ``new_raw_session``. Called by the reconnect machinery.

        Re-fires the retained fetch (RETAINED classes) so cached state
        replays — dedupe (0.0.9) suppresses already-seen samples.
        Re-registers the presence dispatcher with the post-reconnect
        observer. Clears the schema-mismatch warn-once cache because
        peer zids may have changed.
        """
        if self._closed or self._msg_cls is None:
            return
        if self._seen_mismatches is not None:
            self._seen_mismatches.clear()

        msg_cls = self._msg_cls
        dispatch = self._dispatch
        tpls = msg_cls._templates()

        new_subs: list = []
        try:
            for tpl in tpls.all:
                new_subs.append(
                    new_raw_session.declare_subscriber(tpl.wildcard, dispatch),
                )
        except Exception as e:  # noqa: BLE001
            for s in new_subs:
                try:
                    s.undeclare()
                except Exception:  # noqa: BLE001
                    pass
            raise SubscriptionError(
                f'{msg_cls.__name__}: redeclare after reconnect failed: {e}'
            ) from e
        self._zenoh_subs = tuple(new_subs)

        if getattr(msg_cls, 'RETAINED', False):
            _fetch_retained(
                new_raw_session, tpls.all, dispatch, msg_cls, self._on_error,
            )

        # Re-register with the per-session presence observer if applicable.
        # Observer registry is keyed on id(session); after reconnect we
        # bind to the managed session as the durable identity.
        if self._presence_dispatcher is not None:
            from ..presence import get_observer
            observer = get_observer(managed_session)
            observer.start()
            observer.register(self._presence_dispatcher)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Cancel the watchdog FIRST so a pending on_quiet can't fire after
        # the user thinks the subscriber is gone.
        if self._watchdog is not None:
            try:
                self._watchdog.cancel()
            except Exception:  # noqa: BLE001
                pass
        for sub in self._zenoh_subs:
            try:
                sub.undeclare()
            except Exception:  # noqa: BLE001
                pass
        if self._presence_dispatcher is not None and self._session is not None:
            try:
                from ..presence import get_observer
                observer = get_observer(self._session)
                observer.unregister(self._presence_dispatcher)
            except Exception:  # noqa: BLE001
                pass
        _deregister_subscriber(self._session, self)

    def __enter__(self) -> 'Subscriber':
        return self

    def __exit__(self, *exc) -> None:
        self.close()
