"""Reconnect restoration helpers — the post-reopen walks plus the
cancellable backoff loop.

Sibling helper file inside the ``_reconnect`` Pattern B subdir.
"""
from __future__ import annotations

import logging
import threading

from .._managed_session import ManagedSession


_log = logging.getLogger('zeared.reconnect')


# ---------------------------------------------------------------------------
# Backoff loop — close cousin of __init__.py::_open_with_retry, but with a
# cancel signal so z.release() can interrupt a long reconnect.
# ---------------------------------------------------------------------------


class _ReconnectAborted(Exception):
    pass


def _open_with_backoff(
    open_fn,
    *, initial: float, cap: float,
    max_attempts, label: str,
    cancel: threading.Event,
):
    backoff = initial
    attempts = 0
    while True:
        try:
            return open_fn()
        except Exception as e:  # noqa: BLE001
            attempts += 1
            if max_attempts is not None and attempts >= max_attempts:
                raise
            level = logging.INFO if attempts <= 3 else logging.WARNING
            _log.log(
                level,
                '%s reconnect failed (attempt %d): %s — retrying in %.1fs',
                label, attempts, e, backoff,
            )
            # Cancellable sleep — z.release sets this event during teardown.
            if cancel.wait(backoff):
                raise _ReconnectAborted()
            backoff = min(backoff * 2, cap)


# ---------------------------------------------------------------------------
# Restoration walks
# ---------------------------------------------------------------------------


def _restore_retention(managed: ManagedSession) -> None:
    """Walk the retention registry; redeclare queryables on every cache
    bound to this ManagedSession.

    Cache content (``_cache``, ``_index``) is preserved — only the live
    Zenoh queryable handles change. Without this step, queryables stay
    bound to the dead raw and late subscribers' ``session.get(wildcard)``
    silently misses retained values.
    """
    from ..retention import _registry as _retention_registry, _registry_lock

    with _registry_lock:
        candidates = [
            cache for cache in _retention_registry.values()
            if cache._session is managed
        ]
    for cache in candidates:
        try:
            cache._redeclare_queryables()
        except Exception:  # noqa: BLE001
            _log.exception(
                '%s: retention queryable redeclare failed during reconnect',
                cache._cls.__name__,
            )


def _restore_subscribers(managed: ManagedSession) -> None:
    """Walk the subscriber registry keyed on this ManagedSession and
    re-declare each Subscriber against the new raw session."""
    from ..subscriber import _subscribers, _subscribers_lock

    sid = id(managed)
    with _subscribers_lock:
        bucket = list(_subscribers.get(sid, ()))

    for sub in bucket:
        if not getattr(sub, '_auto_reconnect', True):
            continue
        try:
            sub._redeclare(managed.raw(), managed)
        except Exception:  # noqa: BLE001
            _log.exception(
                'subscriber redeclare failed for %s — closing it',
                getattr(sub, '_msg_cls', None),
            )
            try:
                sub.close()
            except Exception:  # noqa: BLE001
                pass


def _restore_wills(managed: ManagedSession) -> None:
    """Re-register every presence will against the new raw session.

    Wills are keyed on zid, which changes on reconnect. Peers see the
    OLD zid disappear (synthesise the will) and the NEW zid appear with
    fresh wills — legitimate offline → online from their perspective.
    """
    from ..presence import _registry as _presence_registry, _registry_lock

    raw = managed.raw()

    with _registry_lock:
        # The old presence-state was keyed on id(old_raw). Find any
        # state(s) registered under THIS managed session by walking and
        # matching the session ref.
        matches = [
            k for k, state in _presence_registry.items()
            if state.session is managed or state.session is raw
        ]

    for key in matches:
        with _registry_lock:
            old_state = _presence_registry.pop(key, None)
        if old_state is None:
            continue
        try:
            old_state.replay_to(managed)
        except Exception:  # noqa: BLE001
            _log.exception('presence replay raised during reconnect')
