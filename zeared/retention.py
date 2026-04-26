from __future__ import annotations

import logging
import threading
import time
from typing import Dict, Optional, Tuple

import zenoh

from . import _codec as codec
from ._managed_session import resolve_raw
from ._prefix_index import _PrefixIndex
from .errors import TopicError, ZearedError


_log = logging.getLogger('zeared.retention')


def _resolve_retention_ttl(cls, session) -> Optional[float]:
    """Resolve the effective retention TTL for ``cls`` on ``session``.

    Precedence:
      - class-level ``RETENTION_TTL`` if set (explicit, intentional);
      - else session-level ``session._retention_ttl`` (factory-kwarg
        fallback for unconfigured classes);
      - else ``None`` (no expiration).

    Read on every ``_handle_query`` invocation, so runtime mutations to
    either the class attribute or ``managed._retention_ttl`` propagate
    without restart.
    """
    cls_ttl = getattr(cls, 'RETENTION_TTL', None)
    if cls_ttl is not None:
        return cls_ttl
    return getattr(session, '_retention_ttl', None)


# Cache value: ``(raw_bytes, encoding, inserted_at_monotonic)``. The
# monotonic timestamp drives ``_RETENTION_TTL`` enforcement at query
# time and is immune to wall-clock jumps.
_CacheValue = Tuple[bytes, str, float]


class _RetentionCache:
    """Per-(cls, session) cache of last retained payload per concrete topic.

    Holds a single ``zenoh.Queryable`` spanning every declared template
    wildcard on the class; the queryable's handler enumerates cached
    concrete topics that intersect the incoming query's key-expr and
    replies to each.
    """

    __slots__ = (
        '_cls', '_session', '_cache', '_index', '_queryables', '_lock',
        '_redeclaring',
    )

    def __init__(self, cls: type, session: 'zenoh.Session'):
        self._cls = cls
        self._session = session
        # concrete_topic → (raw_bytes, encoding string, inserted_monotonic)
        self._cache: Dict[str, _CacheValue] = {}
        # Trie of cached concrete topics — replaces the O(N) iterate-and-
        # intersect on every incoming query.
        self._index = _PrefixIndex()
        self._queryables: list[zenoh.Queryable] = []
        self._lock = threading.Lock()
        # Set during ``_redeclare_queryables`` so concurrent ``store()``
        # calls skip ``_ensure_queryables`` instead of racing into a
        # parallel declaration. Cleared in a ``try/finally`` so an
        # exception during declare doesn't leave the flag stuck-on
        # (which would deadlock subsequent stores into never declaring).
        self._redeclaring = False

    @property
    def size(self) -> int:
        return len(self._cache)

    # -- publisher side -------------------------------------------------

    def store(self, concrete_topic: str, raw: bytes, encoding: str) -> None:
        """Record the last retained payload for ``concrete_topic`` and
        ensure the class's Queryable(s) are declared.

        Captures ``time.monotonic()`` at the call so ``_handle_query``
        can filter out entries older than ``cls.RETENTION_TTL`` (if set)
        on read.
        """
        self._ensure_queryables()
        now = time.monotonic()
        with self._lock:
            new_entry = concrete_topic not in self._cache
            self._cache[concrete_topic] = (raw, encoding, now)
            if new_entry:
                self._index.add(concrete_topic)

    def delete(self, concrete_topic: str) -> None:
        """Drop the cached entry for ``concrete_topic``. Wire-level DELETE
        is the caller's responsibility (``Message.unretain`` issues it)."""
        with self._lock:
            if self._cache.pop(concrete_topic, None) is not None:
                self._index.remove(concrete_topic)

    def drop(self) -> None:
        """Undeclare queryables, clear the cache, remove from registry."""
        with self._lock:
            for q in self._queryables:
                try:
                    q.undeclare()
                except Exception:  # noqa: BLE001
                    pass
            self._queryables.clear()
            self._cache.clear()
            self._index = _PrefixIndex()
        _registry.pop((self._cls, id(self._session)), None)

    # -- queryable --------------------------------------------------------

    def _redeclare_queryables(self) -> None:
        """Replace dead queryables with fresh ones bound to the (now-current)
        underlying raw session. Called by the reconnect machinery after
        the wrapper has swapped its raw.

        Cache state (``_cache``, ``_index``) is preserved — only the live
        Zenoh queryable handles are rebuilt. If the cache has never
        declared queryables (``_queryables`` empty), this is a no-op
        and the next ``store()`` call lazily declares as usual.
        """
        with self._lock:
            old = self._queryables
            if not old:
                return
            self._queryables = []
            self._redeclaring = True
        try:
            # Best-effort undeclare on the dead handles — they're tied
            # to the raw that just died, so this typically raises and
            # we swallow.
            for q in old:
                try:
                    q.undeclare()
                except Exception:  # noqa: BLE001
                    pass
            # Re-declare against the (now-current) raw via the wrapper.
            with self._lock:
                tpls = self._cls._templates()
                try:
                    for t in tpls.all:
                        q = resolve_raw(self._session).declare_queryable(
                            t.wildcard, self._handle_query,
                        )
                        self._queryables.append(q)
                except Exception as e:  # noqa: BLE001
                    for q in self._queryables:
                        try:
                            q.undeclare()
                        except Exception:  # noqa: BLE001
                            pass
                    self._queryables = []
                    raise ZearedError(
                        f'{self._cls.__name__}: redeclare retention '
                        f'queryable after reconnect failed: {e}'
                    ) from e
        finally:
            # Clear the flag unconditionally — leaving it stuck-on
            # would deadlock subsequent ``store()`` calls into never
            # declaring queryables (the next ``_ensure_queryables`` would
            # see it and skip).
            with self._lock:
                self._redeclaring = False

    def _ensure_queryables(self) -> None:
        if self._queryables:
            return
        with self._lock:
            if self._queryables:
                return  # re-check under lock
            if self._redeclaring:
                # Reconnect-driven redeclare in progress; let it install
                # the fresh queryables. The concurrent ``store()`` call
                # is independent — it updates ``_cache`` / ``_index``,
                # which the redeclared queryables will read on the next
                # ``_handle_query`` after they come up.
                return
            tpls = self._cls._templates()
            try:
                for t in tpls.all:
                    q = resolve_raw(self._session).declare_queryable(
                        t.wildcard, self._handle_query,
                    )
                    self._queryables.append(q)
            except Exception as e:  # noqa: BLE001
                # Roll back partial declarations.
                for q in self._queryables:
                    try:
                        q.undeclare()
                    except Exception:  # noqa: BLE001
                        pass
                self._queryables.clear()
                raise ZearedError(
                    f'{self._cls.__name__}: failed to declare retention '
                    f'queryable: {e}'
                ) from e

    def _handle_query(self, query: 'zenoh.Query') -> None:
        """Reply to a query with cached entries that intersect its key-expr.

        Uses the trie index — O(query depth × matches) rather than O(N)
        across the whole cache. When ``cls.RETENTION_TTL`` is set,
        entries older than the TTL are pruned inline (lazy expiration —
        no background thread).
        """
        query_key = str(query.key_expr)
        ttl = _resolve_retention_ttl(self._cls, self._session)
        now = time.monotonic()
        with self._lock:
            matches = list(self._index.matching(query_key))
            payloads = []
            expired = []
            for c in matches:
                entry = self._cache.get(c)
                if entry is None:
                    continue
                raw, encoding, inserted = entry
                if ttl is not None and (now - inserted) > ttl:
                    expired.append(c)
                    continue
                payloads.append((c, raw, encoding))
            # Prune expired entries inline — bounded I/O cost amortised
            # across queries.
            for c in expired:
                if self._cache.pop(c, None) is not None:
                    self._index.remove(c)
        for concrete, raw, encoding in payloads:
            try:
                query.reply(concrete, raw, encoding=codec.MIME[encoding])
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    '%s: retention reply failed on %s: %s',
                    self._cls.__name__, concrete, exc,
                )


# Module-level registry keyed on ``(cls, id(session))``.
_registry: Dict[Tuple[type, int], _RetentionCache] = {}
_registry_lock = threading.Lock()


def get_retention_cache(cls: type, session: 'zenoh.Session') -> _RetentionCache:
    key = (cls, id(session))
    cache = _registry.get(key)
    if cache is not None:
        return cache
    with _registry_lock:
        cache = _registry.get(key)
        if cache is not None:
            return cache
        cache = _RetentionCache(cls, session)
        _registry[key] = cache
        return cache


def clear_retention_cache(*, session: Optional['zenoh.Session'] = None) -> None:
    """Drop cached retained payloads and undeclare queryables.

    Without ``session=``, clears every entry. With ``session=``, drops
    only those targeting that session — useful just before closing a
    session in a long-running process.
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
        # drop() also tries to pop from _registry — already done above, no-op.
        with c._lock:
            for q in c._queryables:
                try:
                    q.undeclare()
                except Exception:  # noqa: BLE001
                    pass
            c._queryables.clear()
            c._cache.clear()
            c._index = _PrefixIndex()


def effective_retain(cls: type, arg: Optional[bool]) -> bool:
    """Resolve the per-send ``retain=`` value against the class's ``RETAINED``."""
    retained = getattr(cls, 'RETAINED', False)
    if arg is None:
        return retained
    if arg and not retained:
        raise TopicError(
            f'{cls.__name__}: retain=True requires RETAINED = True on the class'
        )
    return arg
