from __future__ import annotations

import contextlib
import threading
import warnings
from typing import TYPE_CHECKING, Literal

from . import _codec as codec
from ._managed_session import resolve_raw
from .errors import SessionDeadError, ZearedError

if TYPE_CHECKING:
    import zenoh

    from ._managed_session import SessionLike
    from .message import Message


_DEFAULT_CAP = 256

Encoding = Literal['msgpack', 'json']


def effective_cap(cls: type[Message]) -> int:
    """Resolve the ``PUBLISHER`` class attribute to a concrete cap.

    - ``True`` → default cap (256)
    - ``False`` → 0 (cache disabled; always fall through to ``session.put``)
    - ``int``  → explicit cap
    """
    p = getattr(cls, 'PUBLISHER', True)
    if p is True:
        return _DEFAULT_CAP
    if p is False:
        return 0
    return int(p)


class _PublisherCache:
    """Cache of ``zenoh.Publisher`` keyed by concrete topic, scoped to a single ``(Message subclass, session)`` pair.

    When the cache is full, ``put()`` falls back to ``session.put`` and emits
    a one-time ``warnings.warn``. A send against a closed session drops the
    offending entry and raises ``ZearedError``.
    """

    __slots__ = ('_cap', '_cls', '_emitted', '_pubs', '_session', '_warned')

    def __init__(self, cls: type[Message], session: SessionLike, cap: int) -> None:
        self._cls = cls
        self._session = session
        self._cap = cap
        self._pubs: dict[str, zenoh.Publisher] = {}
        # Every concrete topic this (cls, session) has ever emitted — even
        # those tombstoned later, and those that went through the session.put
        # fallback (PUBLISHER=False or cap-exceeded). Used for introspection
        # via `Message.published_topics` and `z.published_topics`.
        self._emitted: set[str] = set()
        self._warned = False

    @property
    def size(self) -> int:
        return len(self._pubs)

    @property
    def emitted(self) -> frozenset[str]:
        """Snapshot of every concrete topic this cache has ever emitted."""
        return frozenset(self._emitted)

    def put(
        self,
        concrete_topic: str,
        raw: bytes,
        encoding: Encoding,
        *,
        attachment: bytes | None = None,
    ) -> None:
        # Record the emission before any branching — ensures introspection
        # reflects reality regardless of which path the send takes.
        self._emitted.add(concrete_topic)

        mime = codec.MIME[encoding]
        # PUBLISHER=False → cap=0 → always bypass the cache.
        if self._cap == 0:
            self._session_put(concrete_topic, raw, mime, attachment)
            return

        pub = self._pubs.get(concrete_topic)
        if pub is not None:
            self._pub_put(concrete_topic, pub, raw, attachment)
            return

        if len(self._pubs) >= self._cap:
            if not self._warned:
                warnings.warn(
                    f'{self._cls.__name__}: publisher cache cap '
                    f'({self._cap}) reached; subsequent sends with new '
                    f'concrete keys will fall back to session.put(). '
                    f'Consider setting PUBLISHER = False or raising the '
                    f'cap on this class.',
                    stacklevel=2,
                )
                self._warned = True
            self._session_put(concrete_topic, raw, mime, attachment)
            return

        try:
            pub = resolve_raw(self._session).declare_publisher(concrete_topic, encoding=mime)
        except Exception as e:
            self.drop()
            msg = f'{self._cls.__name__}: failed to declare publisher on {concrete_topic!r}: {e}'
            raise ZearedError(msg) from e
        self._pubs[concrete_topic] = pub
        self._pub_put(concrete_topic, pub, raw, attachment)

    def _session_put(
        self,
        topic: str,
        raw: bytes,
        mime: str,
        attachment: bytes | None = None,
    ) -> None:
        try:
            # Spelled out rather than spread from a dict: zenoh's ``put`` is
            # fully typed, and a ``dict[str, bytes | str]`` splat binds to
            # every optional keyword it declares.
            if attachment is not None:
                self._session.put(
                    topic,
                    raw,
                    encoding=mime,
                    attachment=attachment,
                )
            else:
                self._session.put(topic, raw, encoding=mime)
        except Exception as e:
            self.drop()
            msg = f'{self._cls.__name__}: session.put failed on {topic!r}: {e}'
            raise ZearedError(msg) from e

    def _pub_put(
        self,
        topic: str,
        pub: zenoh.Publisher,
        raw: bytes,
        attachment: bytes | None = None,
    ) -> None:
        try:
            if attachment is not None:
                pub.put(raw, attachment=attachment)
            else:
                pub.put(raw)
        except Exception as e:
            # Most likely the session closed out from under us.
            self._pubs.pop(topic, None)
            self.drop()
            self._note_session_failure(e)
            msg = f'{self._cls.__name__}: cached publisher put failed on {topic!r} (session likely closed): {e}'
            # ``_note_failure`` above CASes a dying ManagedSession into
            # RECONNECTING synchronously, so this read is current. Raising
            # the specific error lets the documented retry/queue handler
            # catch it; ``SessionDeadError`` subclasses ``ZearedError``, so
            # existing generic handlers are unaffected.
            if getattr(self._session, 'state', None) in ('RECONNECTING', 'DEAD'):
                raise SessionDeadError(msg) from e
            raise ZearedError(msg) from e

    def _note_session_failure(self, exc: Exception) -> None:
        """Report a send failure to the session so lazy reconnect detection fires.

        ``_session_put`` reaches ``ManagedSession._note_failure`` for free
        via ``_ZenohApiMixin.put``. A cached ``zenoh.Publisher`` bypasses
        the wrapper entirely, so the default ``PUBLISHER = True`` path has
        to report the failure itself — otherwise a publisher-only daemon
        running ``probe_interval=0`` never detects a dead session at all.

        No-op when ``_session`` is a raw ``zenoh.Session`` (no wrapper to
        tell) and best-effort otherwise: this runs on the way out of an
        already-failing send and must never mask the original error.
        """
        note = getattr(self._session, '_note_failure', None)
        if note is None:
            return
        with contextlib.suppress(Exception):
            note(exc)

    def invalidate(self) -> None:
        """Undeclare every cached publisher but keep the cache registered.

        The reconnect counterpart to ``drop()``. The handles are bound to
        the raw session that just died, but the cache object itself
        (``_emitted``, ``_warned``) is process-lifetime state and must
        survive the swap — dropping it would truncate
        ``published_topics()``, whose contract is literally "emitted during
        this process lifetime". The next ``put()`` re-declares lazily
        against the now-current raw.
        """
        # Swap first, then undeclare: a concurrent ``put()`` must not be
        # able to pull a handle out of ``_pubs`` that we are mid-undeclare on.
        pubs, self._pubs = self._pubs, {}
        for pub in pubs.values():
            with contextlib.suppress(Exception):
                pub.undeclare()

    def drop(self) -> None:
        """Undeclare all cached publishers and remove from the registry."""
        self.invalidate()
        _registry.pop((self._cls, id(self._session)), None)


# Module-level registry. Keyed on ``(cls, id(session))`` since Zenoh sessions
# don't support weakrefs. Stale entries are detected and dropped on failure.
_registry: dict[tuple[type, int], _PublisherCache] = {}
_registry_lock = threading.Lock()


def get_cache(cls: type[Message], session: SessionLike) -> _PublisherCache:
    """Return the publisher cache for ``(cls, session)``, creating it if needed."""
    key = (cls, id(session))
    cache = _registry.get(key)
    if cache is not None:
        return cache
    with _registry_lock:
        cache = _registry.get(key)
        if cache is not None:
            return cache
        cache = _PublisherCache(cls, session, effective_cap(cls))
        _registry[key] = cache
        return cache


def published_topics(
    *,
    cls: type | None = None,
    session: zenoh.Session | None = None,
) -> dict:
    """Snapshot of every concrete topic emitted this process lifetime.

    Snapshot introspection: every concrete topic emitted during this process lifetime,
    keyed on ``(Message subclass, session-id)``.

    Filter on ``cls=`` and/or ``session=`` to narrow the view. Tombstoned
    topics remain in the snapshot — "emitted during this process lifetime"
    is literal.
    """
    out: dict = {}
    sid = id(session) if session is not None else None
    with _registry_lock:
        items = list(_registry.items())
    for (cache_cls, cache_sid), cache in items:
        if cls is not None and cache_cls is not cls:
            continue
        if sid is not None and cache_sid != sid:
            continue
        out[(cache_cls, cache_sid)] = cache.emitted
    return out


def clear_publisher_cache(*, session: SessionLike | None = None) -> None:
    """Drop cached publishers.

    Without ``session=``, clears every entry in the registry. With
    ``session=``, drops only those targeting that session — useful just
    before closing a session if you don't want stale entries hanging
    around in the registry.
    """
    with _registry_lock:
        if session is None:
            caches = list(_registry.values())
            _registry.clear()
        else:
            sid = id(session)
            keys = [k for k in _registry if k[1] == sid]
            caches = [_registry.pop(k) for k in keys]
    for c in caches:
        # ``invalidate`` rather than ``drop`` — the registry entry is already
        # popped above, and re-entering that branch would be a no-op anyway.
        c.invalidate()
