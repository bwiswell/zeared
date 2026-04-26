"""Module-level helpers for ``_managed_session`` — the WeakSet registry,
declare-handle RuntimeWarning emitter, and the small liveness / raw-
resolution utilities.

Sibling helper inside the ``_managed_session`` Pattern B subdir.
"""
from __future__ import annotations

import warnings
import weakref
from typing import TYPE_CHECKING

import zenoh

if TYPE_CHECKING:
    from ._managed_session import ManagedSession


# Module-level registry of every live ``ManagedSession``. Walked by
# ``release_all`` so wrappers without registered zeared-level state
# (e.g. one opened with ``auto_reconnect=True`` that hasn't yet had a
# subscriber/publisher touch it) still get torn down on
# process-shutdown hooks.
#
# WeakSet — entries auto-disappear when the wrapper is GC'd. Probe +
# reconnect threads hold strong refs via ``args=(managed,)``, so the
# wrapper persists in the set as long as those threads run; once
# ``_teardown`` joins them and the user drops their ref, the entry
# vanishes naturally.
_managed_sessions: 'weakref.WeakSet[ManagedSession]' = weakref.WeakSet()


# Probe interval default; user-configurable via the factory kwarg.
_DEFAULT_PROBE_INTERVAL = 10.0


_DECLARE_HANDLE_WARNING = (
    'ManagedSession.{method} returns a handle bound to the current raw '
    'session; it does NOT survive reconnect. For subscriptions prefer '
    'Cls.on_message(...) (zeared rebuilds on reconnect transparently); '
    'for publishes prefer msg.send() (publisher cache rebuilds lazily); '
    'for queryables, treat the handle as raw-only state and re-declare '
    'inside an on_reconnect(cb) hook.'
)


def _warn_declare_handle(method: str) -> None:
    """Emit a once-per-call-site RuntimeWarning. ``stacklevel=3`` points at
    the user's call site (skipping this helper + the wrapper method)."""
    warnings.warn(
        _DECLARE_HANDLE_WARNING.format(method=method),
        RuntimeWarning,
        stacklevel=3,
    )


def _is_dead(raw: zenoh.Session) -> bool:
    """Cheap liveness check. Uses ``is_closed()`` if Zenoh exposes it,
    falls back to a ``zid()`` call (which raises on a closed session).
    """
    try:
        if hasattr(raw, 'is_closed'):
            return bool(raw.is_closed())
        raw.zid()
        return False
    except Exception:  # noqa: BLE001
        return True


def resolve_raw(session) -> zenoh.Session:
    """Return the underlying raw ``zenoh.Session`` for ``session``.

    Accepts either a raw ``zenoh.Session`` or a ``ManagedSession`` wrapper.
    Used by internals that need to reach into Zenoh-only APIs (e.g.
    ``session.zid()`` for presence wills, observer registries keyed on the
    raw session's identity).
    """
    # Late import — ``ManagedSession`` lives in a sibling module.
    from ._managed_session import ManagedSession

    if isinstance(session, ManagedSession):
        return session.raw()
    return session
