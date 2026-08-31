"""Reconnect restoration helpers — the post-reopen walks plus the cancellable backoff loop.

Sibling helper file inside the ``_reconnect`` Pattern B subdir.
"""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    import zenoh

    from .._managed_session import ManagedSession

_log = logging.getLogger('zeared.reconnect')

# Attempts logged at INFO before escalating to WARNING.
_QUIET_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# Backoff loop — close cousin of __init__.py::_open_with_retry, but with a
# cancel signal so z.release() can interrupt a long reconnect.
# ---------------------------------------------------------------------------


class _ReconnectAbortedError(Exception):
    pass


def _open_with_backoff(  # noqa: PLR0913
    open_fn: Callable[[], zenoh.Session],
    *,
    initial: float,
    cap: float,
    max_attempts: int | None,
    label: str,
    cancel: threading.Event,
) -> zenoh.Session:
    backoff = initial
    attempts = 0
    while True:
        try:
            return open_fn()
        except Exception as e:
            attempts += 1
            if max_attempts is not None and attempts >= max_attempts:
                raise
            level = logging.INFO if attempts <= _QUIET_ATTEMPTS else logging.WARNING
            _log.log(
                level,
                '%s reconnect failed (attempt %d): %s — retrying in %.1fs',
                label,
                attempts,
                e,
                backoff,
            )
            # Cancellable sleep — z.release sets this event during teardown.
            if cancel.wait(backoff):
                raise _ReconnectAbortedError from e
            backoff = min(backoff * 2, cap)


# ---------------------------------------------------------------------------
# Restoration walks
# ---------------------------------------------------------------------------


def _restore_retention(managed: ManagedSession) -> None:
    """Walk the retention registry; redeclare queryables on every cache bound to this ManagedSession.

    Cache content (``_cache``, ``_index``) is preserved — only the live
    Zenoh queryable handles change. Without this step, queryables stay
    bound to the dead raw and late subscribers' ``session.get(wildcard)``
    silently misses retained values.
    """
    from ..retention import _registry as _retention_registry
    from ..retention import _registry_lock

    with _registry_lock:
        candidates = [cache for cache in _retention_registry.values() if cache._session is managed]
    for cache in candidates:
        try:
            cache._redeclare_queryables()
        except Exception:  # noqa: BLE001
            _log.exception(
                '%s: retention queryable redeclare failed during reconnect',
                cache._cls.__name__,
            )


def _restore_subscribers(managed: ManagedSession) -> None:
    """Re-declare every registered Subscriber against the new raw session.

    Walk the subscriber registry keyed on this ManagedSession and re-declare each
    Subscriber against the new raw session.
    """
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
            with contextlib.suppress(Exception):
                sub.close()


def _restore_queryables(managed: ManagedSession) -> None:
    """Re-declare every registered Queryable against the new raw session.

    Walk the queryable registry keyed on this ManagedSession and re-declare each
    ``Queryable`` against the new raw session.

    Queryables hold no replayed state — just the handler closure — so a
    failed redeclare closes the handle rather than retrying (mirrors the
    subscriber policy).
    """
    from ..queryable import _queryables, _queryables_lock

    sid = id(managed)
    with _queryables_lock:
        bucket = list(_queryables.get(sid, ()))

    for qbl in bucket:
        if not getattr(qbl, '_auto_reconnect', True):
            continue
        try:
            qbl._redeclare(managed.raw(), managed)
        except Exception:  # noqa: BLE001
            _log.exception(
                'queryable redeclare failed for %s — closing it',
                getattr(qbl, '_msg_cls', None),
            )
            with contextlib.suppress(Exception):
                qbl.close()


def _restore_wills(managed: ManagedSession) -> None:
    """Re-register every presence will against the new raw session.

    Wills are keyed on zid, which changes on reconnect. Peers see the
    OLD zid disappear (synthesise the will) and the NEW zid appear with
    fresh wills — legitimate offline → online from their perspective.
    """
    from ..presence import _registry as _presence_registry
    from ..presence import _registry_lock

    raw = managed.raw()

    with _registry_lock:
        # The old presence-state was keyed on id(old_raw). Find any
        # state(s) registered under THIS managed session by walking and
        # matching the session ref.
        matches = [k for k, state in _presence_registry.items() if state.session is managed or state.session is raw]

    for key in matches:
        with _registry_lock:
            old_state = _presence_registry.pop(key, None)
        if old_state is None:
            continue
        try:
            old_state.replay_to(managed)
        except Exception:  # noqa: BLE001
            _log.exception('presence replay raised during reconnect')
