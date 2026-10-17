"""Router-link detection and the re-attach walk (0.4.0).

A hub restart drops a client session's router link without killing the
session, so the reconnect path never runs, and retained values published
while the link was down never reach the subscribers. This module notices the
link's loss and return from the probe thread, re-runs the retained fetch for
the session's subscribers, and fires ``on_link_lost`` / ``on_link_restored``.

Detection polls ``info.routers_zid()`` once per probe tick and compares the
set of router zids with the last tick's:

- non-empty → empty: the link is lost;
- empty → non-empty, while lost: it is back;
- a non-empty set replaced by a disjoint one: the router restarted between two
  ticks (a restarted router comes back with a new zid), so both at once.

A session that never had a router never transitions, so peers are
self-gated. A router that keeps a fixed zid and restarts within one probe
interval is invisible to polling; Zenoh's link events could catch it, and
are a 0.4.x candidate once upstream confirms they are a stable API.
"""

from __future__ import annotations

import logging
import random
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import zenoh

    from .._managed_session import ManagedSession

_log = logging.getLogger('zeared.reconnect')


def _router_zids(raw: zenoh.Session) -> frozenset[str] | None:
    """The router zids ``raw`` is attached to, or ``None`` if they can't be read."""
    try:
        return frozenset(str(zid) for zid in raw.info.routers_zid())
    except Exception:  # noqa: BLE001
        return None


def _note_routers(managed: ManagedSession) -> None:
    """Record the current router set as the baseline (probe start, after a reconnect)."""
    zids = _router_zids(managed.raw())
    if zids is not None:
        managed._routers = zids


def _check_link(managed: ManagedSession) -> None:
    """One probe tick of link detection: fire the signals and run the walk on a transition."""
    zids = _router_zids(managed._raw)
    if zids is None:
        return
    before = managed._routers
    managed._routers = zids
    if managed._link_state == 'up':
        if before and not zids:
            _link_lost(managed)
        elif before and zids and before.isdisjoint(zids):
            # Lost and back between two ticks: the router restarted.
            _link_lost(managed)
            _link_restored(managed)
    elif zids:
        _link_restored(managed)


def _link_lost(managed: ManagedSession) -> None:
    """Mark the view stale and fire ``on_link_lost``; a no-op if already lost.

    A loss while a restore is in flight (``'restoring'``) cancels that
    restore, which then fires nothing, and fires nothing itself: callbacks
    never saw the link come back, so to them it is still the same loss, and
    the signals stay paired.
    """
    with managed._lock:
        state, managed._link_state = managed._link_state, 'lost'
    if state == 'up':
        managed._fire('link_lost')


def _link_restored(managed: ManagedSession, *, walk: bool = True) -> None:
    """Republish, run the re-attach walk (unless ``walk=False``), mark the view whole, fire ``on_link_restored``.

    The republish pushes this session's retained values out (what others
    missed from us); the walk pulls others' values in (what we missed).

    The probe thread and the reconnect worker can both get here, so the
    first claims the transition, ``'lost'`` to ``'restoring'``, under the
    session lock, and the other returns. If the link is lost again before
    the walk finishes, the claim is gone and this restore fires nothing; the
    loss's own restore will.
    """
    with managed._lock:
        if managed._link_state != 'lost':
            return
        managed._link_state = 'restoring'
    _republish_retained(managed)
    if walk:
        _run_link_walk(managed)
    with managed._lock:
        if managed._link_state != 'restoring':
            return
        managed._link_state = 'up'
    managed._fire('link_restored')


def _republish_retained(managed: ManagedSession) -> None:
    """Re-put every retained value this session publishes, with its original timestamp (0.4.0).

    A value published while this session was detached reached nobody, and a
    subscriber whose own re-attach walk ran before this session came back
    missed it as well. Each cache re-puts its live entries marked as a
    republish: subscribers that already hold a value dedupe the copy (same
    timestamp), and the rest receive it as ``Origin.REPLAY``.
    """
    from ..retention import _registry as _retention_registry

    raw = managed.raw()
    for cache in list(_retention_registry.values()):
        if cache._session is managed or cache._session is raw:
            try:
                cache.republish()
            except Exception:  # noqa: BLE001
                _log.exception('republish failed for %s', getattr(cache, '_cls', None))


def _run_link_walk(managed: ManagedSession) -> None:
    """Re-run the retained fetch for every subscriber on ``managed`` that replays.

    Retention only: the Zenoh subscribers and queryables survive a link outage
    intact, so nothing is redeclared, and presence comes back through the
    tokens themselves. Skipped outright, with no jitter, when no subscriber
    would replay. Otherwise it waits ``random.uniform(0, resync_jitter)``
    seconds, so a fleet re-attaching to a restarted hub doesn't fetch all at
    once, then fetches on at most ``resync_concurrency`` workers: one task per
    subscriber, whose templates are fetched in order, so no subscriber's
    callback runs concurrently with its own replay. Returns when every task
    has finished; a failed one is logged.
    """
    from ..subscriber import _subscribers, _subscribers_lock

    with _subscribers_lock:
        subs = [sub for sub in _subscribers.get(id(managed), ()) if sub._wants_replay()]
    if not subs:
        return
    jitter = managed._resync_jitter
    if jitter > 0:
        # Interruptible: a teardown during the wait abandons the walk.
        managed._probe_cancel.wait(random.uniform(0, jitter))  # noqa: S311  (jitter, not security)
    if managed._probe_cancel.is_set():
        return
    raw = managed.raw()
    workers = max(1, managed._resync_concurrency)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f'zeared-resync-{id(managed):x}') as pool:
        futures = [pool.submit(sub._replay_retained, raw) for sub in subs]
        for sub, future in zip(subs, futures, strict=True):
            try:
                future.result()
            except Exception:  # noqa: BLE001
                _log.exception('re-attach replay failed for %s', getattr(sub, '_msg_cls', None))
