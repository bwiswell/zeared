"""Zeared subscription handle — the ``Subscriber`` class.

Primary file of the ``subscriber`` Pattern B subdir. Per-sample
dispatch plumbing lives in ``_subscriber_dispatch.py``; the
retained-fetch helper lives in ``_subscriber_retained_fetch.py``;
the module-level subscriber registry used by
``z.release(session=)`` lives in ``_subscriber_registry.py``.
"""

from __future__ import annotations

import contextlib
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Self, TypeVar

from .._managed_session import resolve_raw
from ..errors import SubscriptionError, TopicError
from ._subscriber_dispatch import (
    _adapt_async_callback,
    _build_dispatch,
    _build_presence_handler,
    _wants_meta,
)
from ._subscriber_registry import (
    _SCHEMA_MISMATCH_CACHE_MAX,
    _deregister_subscriber,
    _register_subscriber,
)
from ._subscriber_retained_fetch import _fetch_retained

if TYPE_CHECKING:
    from collections.abc import Callable

    import zenoh

    from .._managed_session import ManagedSession, SessionLike
    from ..message import Message
    from ..watchdog import _SubscriberWatchdog

_log = logging.getLogger('zeared.subscriber')


def _watch_presence(raw: zenoh.Session, tpls: Any, handler: Callable[..., None], msg_cls: type) -> tuple:
    """Declare a liveliness subscriber per template wildcard, then replay the keys already online.

    Subscribe first, then ``get``: a token that appears in between is reported
    twice (``LIVE``, then ``REPLAY``), which is harmless since online is
    idempotent, where the other order could miss it. ``history=True`` would
    replay too, but can't mark which PUTs are history.
    """
    from ..meta import Origin

    subs: list = []
    try:
        for tpl in tpls.all:
            subs.append(raw.liveliness().declare_subscriber(tpl.wildcard, handler))  # noqa: PERF401  (partial list drives rollback below)
    except Exception as e:
        for sub in subs:
            with contextlib.suppress(Exception):
                sub.undeclare()
        msg = f'{msg_cls.__name__}: failed to declare presence subscriber: {e}'
        raise SubscriptionError(msg) from e
    for tpl in tpls.all:
        try:
            replies = raw.liveliness().get(tpl.wildcard)
        except Exception as exc:  # noqa: BLE001
            _log.warning('%s: presence get() on %s failed: %s', msg_cls.__name__, tpl.wildcard, exc)
            continue
        for reply in replies:
            ok = reply.ok if hasattr(reply, 'ok') else None
            if ok is not None:
                handler(ok, origin=Origin.REPLAY)
    return tuple(subs)


def _start_presence(  # noqa: PLR0913, PLR0917
    raw: zenoh.Session,
    tpls: Any,
    msg_cls: type[Message],
    on_online: Callable | None,
    on_offline: Callable | None,
    on_error: Callable[[Exception, bytes], None] | None,
) -> tuple[Callable[..., None] | None, tuple]:
    """The presence handler and its liveliness subscribers, or ``(None, ())`` without callbacks."""
    if on_online is None and on_offline is None:
        return None, ()
    handler = _build_presence_handler(
        msg_cls,
        tpls,
        _adapt_async_callback(on_online) if on_online is not None else None,
        _adapt_async_callback(on_offline) if on_offline is not None else None,
        on_error,
    )
    return handler, _watch_presence(raw, tpls, handler, msg_cls)


# Type parameter for generic-parameterised ``Subscriber[Cls]``. Bound to
# ``Message`` so callers can't accidentally parameterise with a non-message
# type. Runtime is identical to a non-parameterised ``Subscriber``.
M = TypeVar('M', bound='Message')


class Subscriber[M: 'Message']:
    """Zeared subscription handle.

    Wraps N underlying ``zenoh.Subscriber`` instances — one per declared
    topic template on the message class. When ``on_online`` / ``on_offline``
    were given (``LIVELINESS = True`` classes only, 0.4.0), it also holds one
    liveliness subscriber per template.

    Close via ``.close()`` or as a context manager. Close is idempotent.

    Generic in the message class for IDE ergonomics —
    ``Cls.on_message(cb)`` returns ``Subscriber[Cls]``. The type parameter
    is type-only; runtime behavior is identical with or without it
    (bare ``Subscriber()`` and ``Subscriber[Cls]()`` are equivalent at
    runtime).
    """

    __slots__ = (
        '_auto_reconnect',
        '_closed',
        '_dispatch',
        # Redeclaration state — populated by `_declare`, used on reconnect.
        '_msg_cls',
        '_on_error',
        '_presence_handler',
        '_presence_subs',
        '_retained_fetch',
        '_seen_mismatches',
        '_session',
        '_watchdog',
        '_zenoh_subs',
    )

    def __init__(
        self,
        zenoh_subs: tuple,
        session: SessionLike | None = None,
        watchdog: _SubscriberWatchdog | None = None,
    ) -> None:
        """Wrap already-declared zenoh subscribers as a zeared handle."""
        self._zenoh_subs = zenoh_subs
        self._session = session
        self._watchdog = watchdog
        # Liveliness subscribers + their handler, for on_online / on_offline.
        self._presence_subs: tuple = ()
        self._presence_handler: Callable[..., None] | None = None
        self._closed = False
        # Filled in by `_declare` — kept on the instance so the reconnect
        # machinery can rebuild the underlying zenoh subs against a new raw.
        self._msg_cls = None
        self._dispatch = None
        self._on_error = None
        self._auto_reconnect = True
        self._retained_fetch = True
        # Pointer to the dispatch closure's schema-mismatch cache; held on
        # the instance so reconnect can clear it (peer zids may have
        # changed; stale entries would silently drop legit mismatches).
        self._seen_mismatches: OrderedDict[tuple, None] | None = None

    @classmethod
    def _declare(  # noqa: PLR0913, PLR0917
        cls,
        msg_cls: type[Message],
        session: SessionLike,
        # `Any` return: an `async def` callback hands back a coroutine, which
        # `_adapt_async_callback` schedules. See `MessageCallback`.
        cb: Callable[..., Any],
        on_error: Callable[[Exception, bytes], None] | None,
        expected_interval: float | None = None,
        on_quiet: Callable | None = None,
        on_active: Callable | None = None,
        startup_grace: float | None = None,
        auto_reconnect: bool = True,  # noqa: FBT001, FBT002
        dedupe: bool | None = None,  # noqa: FBT001
        on_remove: Callable | None = None,
        retained_fetch: bool = True,  # noqa: FBT001, FBT002
        on_online: Callable | None = None,
        on_offline: Callable | None = None,
    ) -> Subscriber:
        wants_presence = on_online is not None or on_offline is not None
        if wants_presence and not getattr(msg_cls, 'LIVELINESS', False):
            msg = f'{msg_cls.__name__}.on_message: on_online / on_offline need LIVELINESS = True'
            raise TopicError(msg)
        tpls = msg_cls._templates()  # noqa: SLF001
        cb = _adapt_async_callback(cb)
        wants_meta = _wants_meta(cb)
        # DELETE-sample (tombstone) callback — async-adapted like ``cb`` so
        # an ``async def`` on_remove is scheduled on the subscribe-time loop.
        if on_remove is not None:
            on_remove = _adapt_async_callback(on_remove)

        # Optional per-subscription watchdog.
        watchdog = None
        if expected_interval is not None:
            from ..watchdog import _SubscriberWatchdog

            watchdog = _SubscriberWatchdog(
                expected_interval,
                on_quiet,
                on_active,
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
        dedupe_active = getattr(msg_cls, 'RETAINED', False) and effective_dedupe
        seen_ts: dict[str, str] = {}

        # Schema-mismatch warn-once cache — bounded ``OrderedDict``
        # keyed on (sender_zid, observed_schema). Cleared on close and
        # on `_redeclare` (post-reconnect; peer zids change).
        expected_schema = getattr(msg_cls, 'SCHEMA', None)
        seen_mismatches: OrderedDict[tuple, None] = OrderedDict()

        dispatch = _build_dispatch(
            msg_cls,
            on_error,
            cb,
            wants_meta=wants_meta,
            dedupe_active=dedupe_active,
            expected_schema=expected_schema,
            seen_mismatches=seen_mismatches,
            seen_ts=seen_ts,
            watchdog=watchdog,
            schema_mismatch_cache_max=_SCHEMA_MISMATCH_CACHE_MAX,
            on_remove=on_remove,
        )

        # Internal declaration — route through the underlying raw to
        # avoid the user-facing declare_* RuntimeWarning. zeared rebuilds
        # these handles itself across reconnects via Subscriber._redeclare.
        raw = resolve_raw(session)

        zenoh_subs: list = []
        try:
            for tpl in tpls.all:
                zenoh_subs.append(raw.declare_subscriber(tpl.wildcard, dispatch))  # noqa: PERF401  (partial list drives rollback below)
        except Exception as e:
            for sub in zenoh_subs:
                with contextlib.suppress(Exception):
                    sub.undeclare()
            msg = f'{msg_cls.__name__}: failed to declare subscriber: {e}'
            raise SubscriptionError(msg) from e

        # Retained-fetch: for RETAINED classes, pull any cached values from
        # peer queryables via session.get() on each declared wildcard. Reply
        # samples flow through the same dispatch as live samples, marked
        # origin=REPLAY. ``retained_fetch=False`` skips the fetch entirely
        # (live-only subscriber) — here and on every reconnect.
        if getattr(msg_cls, 'RETAINED', False) and retained_fetch:
            _fetch_retained(session, tpls.all, dispatch, msg_cls, on_error)

        # Presence (0.4.0): a liveliness subscriber per template, mapping a
        # token PUT to on_online and a DELETE to on_offline, then a replay of
        # the keys already online (origin=REPLAY).
        try:
            presence_handler, presence_subs = _start_presence(raw, tpls, msg_cls, on_online, on_offline, on_error)
        except Exception:
            for zsub in zenoh_subs:
                with contextlib.suppress(Exception):
                    zsub.undeclare()
            raise

        sub_handle = cls(
            tuple(zenoh_subs),
            session=session,
            watchdog=watchdog,
        )
        sub_handle._presence_subs = presence_subs
        sub_handle._presence_handler = presence_handler
        sub_handle._msg_cls = msg_cls
        sub_handle._dispatch = dispatch
        sub_handle._on_error = on_error
        sub_handle._auto_reconnect = auto_reconnect
        sub_handle._seen_mismatches = seen_mismatches
        sub_handle._retained_fetch = retained_fetch
        _register_subscriber(session, sub_handle)
        return sub_handle

    def _redeclare(self, new_raw_session: zenoh.Session, managed_session: ManagedSession) -> None:  # noqa: ARG002  (the restore walk passes both)
        """Rebuild the underlying ``zenoh.Subscriber`` set against ``new_raw_session``.

        Called by the reconnect machinery.

        Re-fires the retained fetch (RETAINED classes, unless the
        subscription opted out via ``retained_fetch=False``) so cached
        state replays — dedupe (0.0.9) suppresses already-seen samples.
        Re-declares the presence watch, whose replay reports the keys
        online now as ``REPLAY``. Clears the schema-mismatch warn-once
        cache because peer zids may have changed.
        """
        if self._closed or self._msg_cls is None or self._dispatch is None:
            return
        if self._seen_mismatches is not None:
            self._seen_mismatches.clear()

        msg_cls = self._msg_cls
        dispatch = self._dispatch
        tpls = msg_cls._templates()  # noqa: SLF001

        new_subs: list = []
        try:
            for tpl in tpls.all:
                new_subs.append(  # noqa: PERF401  (partial list drives rollback below)
                    new_raw_session.declare_subscriber(tpl.wildcard, dispatch),
                )
        except Exception as e:
            for s in new_subs:
                with contextlib.suppress(Exception):
                    s.undeclare()
            msg = f'{msg_cls.__name__}: redeclare after reconnect failed: {e}'
            raise SubscriptionError(msg) from e
        self._zenoh_subs = tuple(new_subs)

        if getattr(msg_cls, 'RETAINED', False) and self._retained_fetch:
            _fetch_retained(
                new_raw_session,
                tpls.all,
                dispatch,
                msg_cls,
                self._on_error,
            )

        # The old liveliness subscribers died with the old raw session.
        if self._presence_handler is not None:
            self._presence_subs = _watch_presence(new_raw_session, tpls, self._presence_handler, msg_cls)

    def _wants_replay(self) -> bool:
        """Whether the re-attach walk should re-run this subscriber's retained fetch."""
        return (
            not self._closed
            and self._msg_cls is not None
            and self._dispatch is not None
            and bool(getattr(self._msg_cls, 'RETAINED', False))
            and self._retained_fetch
            and self._auto_reconnect
        )

    def _replay_retained(self, raw: zenoh.Session) -> None:
        """Re-run the retained fetch on ``raw`` (0.4.0, the re-attach walk).

        No redeclare: the Zenoh subscribers survived the link outage. Replies
        go through the same dispatch as ``REPLAY``, dedupe-safe.
        """
        if not self._wants_replay() or self._msg_cls is None or self._dispatch is None:
            return
        _fetch_retained(raw, self._msg_cls._templates().all, self._dispatch, self._msg_cls, self._on_error)  # noqa: SLF001

    def close(self) -> None:
        """Undeclare every underlying zenoh subscriber. Idempotent."""
        if self._closed:
            return
        self._closed = True
        # Cancel the watchdog FIRST so a pending on_quiet can't fire after
        # the user thinks the subscriber is gone.
        if self._watchdog is not None:
            with contextlib.suppress(Exception):
                self._watchdog.cancel()
        for sub in (*self._zenoh_subs, *self._presence_subs):
            with contextlib.suppress(Exception):
                sub.undeclare()
        if self._session is not None:
            _deregister_subscriber(self._session, self)

    def __enter__(self) -> Self:
        """Enter the context manager — returns the subscriber handle."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Exit the context manager — closes the handle."""
        self.close()
