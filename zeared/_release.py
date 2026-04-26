"""Session-tearing-down helpers — ``release`` / ``release_all``.

Pulled out of ``__init__.py`` so the package init can stay a thin
re-export-and-glue module under the 300-line cap. Public names are
re-exported by ``__init__.py``.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union

from ._managed_session import ManagedSession

if TYPE_CHECKING:
    import zenoh


_log_connect = logging.getLogger('zeared.connect')


def release(*, session: 'Union[zenoh.Session, ManagedSession]') -> None:
    """Walk every zeared-owned resource for ``session`` in the right order
    and close the session itself.

    Order matters — step 5 (presence-state clear, which undeclares the
    liveliness token) MUST run before step 6 (``session.close()``) so the
    token DELETE propagates to peers BEFORE the local transport tears
    down. Flipping these silently breaks subscribers of peer wills.

    Idempotent: a second call is a no-op (each cleared registry is empty).

    The keyword-only ``session=`` is intentional — no implicit module-default
    fallback. Callers must explicitly name the session being released.
    """
    from .presence import clear_observer, clear_presence_state
    from .publisher import clear_publisher_cache
    from .retention import clear_retention_cache
    from .subscriber import _close_subscribers_for

    # If this is a ManagedSession, stop the probe BEFORE everything else:
    # we don't want a probe-detected death triggering reconnect mid-shutdown.
    if isinstance(session, ManagedSession):
        session._teardown(call_close=False)

    # 1. Close zeared subscribers on this session (cancels watchdogs,
    #    undeclares Zenoh subs, deregisters presence dispatchers).
    _close_subscribers_for(session)

    # 2. Cached publishers — undeclare each declared zenoh.Publisher.
    clear_publisher_cache(session=session)

    # 3. Retention cache + queryable.
    clear_retention_cache(session=session)

    # 4. Presence observer — undeclare the alive-sub + will-sub.
    clear_observer(session=session)

    # 5. Presence state — undeclare liveliness token + will queryable.
    #    *** Must run BEFORE session.close() below: this is the call that
    #    *** triggers peers' will-synthesis, and Zenoh needs the local
    #    *** transport alive to propagate the DELETE.
    clear_presence_state(session=session)

    # 6. Close the Zenoh session itself.
    try:
        session.close()
    except Exception:  # noqa: BLE001
        # Already closed (or otherwise unhealthy) — nothing useful we can do.
        pass


def release_all() -> None:
    """Release every zeared-managed session in this process.

    Walks the per-session registries (subscribers, publisher caches,
    retention caches, presence state, presence observers, ManagedSession
    wrappers) and calls :func:`release` on each unique session
    reference. Idempotent — running twice is a no-op (registries are
    emptied on first pass).

    Useful for process-shutdown hooks where the caller doesn't track
    every session it opened. Doesn't replace per-session
    ``release(session=...)`` for cases where one session needs to close
    without affecting others.
    """
    from ._managed_session import _managed_sessions
    from .presence import (
        _observer_registry,
        _registry as _presence_registry,
    )
    from .publisher import _registry as _publisher_registry
    from .retention import _registry as _retention_registry
    from .subscriber import _subscribers, _subscribers_lock

    sessions: 'dict[int, object]' = {}     # id(session) → session ref

    def _add(sess):
        if sess is None:
            return
        sessions.setdefault(id(sess), sess)

    # Walk the ManagedSession WeakSet FIRST — covers wrappers whose
    # only live state is the probe + reconnect threads (no subscribers,
    # publishers, retention, or presence yet). Per-resource walks
    # below would miss them entirely.
    for managed in list(_managed_sessions):
        _add(managed)

    # Subscriber registry: keys are id(session); values are sets of
    # Subscribers, each carrying a ``_session`` ref back to the wrapper
    # / raw they were declared against.
    with _subscribers_lock:
        for bucket in _subscribers.values():
            for sub in bucket:
                _add(getattr(sub, '_session', None))

    # Publisher / retention caches are keyed (cls, id(session)); values
    # carry ``_session``.
    for cache in list(_publisher_registry.values()):
        _add(getattr(cache, '_session', None))
    for cache in list(_retention_registry.values()):
        _add(getattr(cache, '_session', None))

    # Presence state + observer registries are keyed id(session); values
    # carry ``session`` (no underscore — the public attr).
    for state in list(_presence_registry.values()):
        _add(getattr(state, 'session', None))
    for observer in list(_observer_registry.values()):
        _add(getattr(observer, 'session', None))

    for sess in list(sessions.values()):
        try:
            release(session=sess)
        except Exception:  # noqa: BLE001
            _log_connect.warning(
                'release_all: release(%r) raised', sess, exc_info=True,
            )
