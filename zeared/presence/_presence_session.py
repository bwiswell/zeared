"""Per-session presence state — the liveliness tokens a session holds.

Holds ``_SessionPresence``, the module-level registry, ``get_presence``,
and ``clear_presence_state``. A session that never declares presence pays
nothing: the state is created on the first ``declare_presence``.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from typing import TYPE_CHECKING, Any

from .._managed_session import resolve_raw
from .presence import PresenceToken

if TYPE_CHECKING:
    from .._managed_session import SessionLike

_log = logging.getLogger('zeared.presence')


class _SessionPresence:
    """The liveliness tokens one session holds, one per concrete key.

    Each entry pairs the Zenoh token with the :class:`PresenceToken` handed
    to the caller. The handle stays the same across a reconnect, while
    :meth:`replay_to` swaps the Zenoh token underneath it.
    """

    __slots__ = ('_lock', '_tokens', 'session')

    def __init__(self, session: SessionLike) -> None:
        self.session = session
        self._lock = threading.Lock()
        # concrete key → (zenoh.LivelinessToken, PresenceToken)
        self._tokens: dict[str, tuple[Any, PresenceToken]] = {}

    def declare(self, key: str) -> PresenceToken:
        """Hold ``key``, returning its handle; the same handle if already held."""
        with self._lock:
            held = self._tokens.get(key)
            if held is not None:
                # One Zenoh token per key: a second one would outlive the
                # first ``undeclare()``, and the key would never go offline.
                return held[1]
            token = resolve_raw(self.session).liveliness().declare_token(key)
            handle = PresenceToken(self, key)
            self._tokens[key] = (token, handle)
            return handle

    def undeclare(self, key: str, handle: PresenceToken) -> None:
        """Withdraw ``key`` if ``handle`` still holds it."""
        with self._lock:
            held = self._tokens.get(key)
            if held is None or held[1] is not handle:
                return
            del self._tokens[key]
        with contextlib.suppress(Exception):
            held[0].undeclare()

    def holds(self, key: str, handle: PresenceToken) -> bool:
        """Whether ``handle`` is the live holder of ``key``."""
        with self._lock:
            held = self._tokens.get(key)
        return held is not None and held[1] is handle

    def keys(self) -> list[str]:
        """The concrete keys held, in declaration order."""
        with self._lock:
            return list(self._tokens)

    def replay_to(self, new_session: SessionLike) -> None:
        """Re-declare every held key on ``new_session``'s current raw session.

        Used by the reconnect machinery once the raw session is swapped. The
        old tokens died with the old raw session, so they are dropped, not
        undeclared. Every :class:`PresenceToken` stays valid. A key whose
        re-declare fails is logged and released.
        """
        with self._lock:
            self.session = new_session
            raw = resolve_raw(new_session)
            for key, (_dead, handle) in list(self._tokens.items()):
                try:
                    self._tokens[key] = (raw.liveliness().declare_token(key), handle)
                except Exception:  # noqa: BLE001
                    _log.exception('presence: re-declaring %s after reconnect failed; releasing it', key)
                    del self._tokens[key]

    def drop(self) -> None:
        """Withdraw every token and forget them. Idempotent."""
        with self._lock:
            tokens = [token for token, _handle in self._tokens.values()]
            self._tokens.clear()
        for token in tokens:
            with contextlib.suppress(Exception):
                token.undeclare()


# ---------------------------------------------------------------------------
# Module-level registry
# ---------------------------------------------------------------------------


_registry: dict[int, _SessionPresence] = {}
_registry_lock = threading.Lock()


def get_presence(session: SessionLike) -> _SessionPresence:
    """Return (creating if needed) the presence state for this session."""
    sid = id(session)
    state = _registry.get(sid)
    if state is not None:
        return state
    with _registry_lock:
        state = _registry.get(sid)
        if state is not None:
            return state
        state = _SessionPresence(session)
        _registry[sid] = state
        return state


def clear_presence_state(*, session: SessionLike | None = None) -> None:
    """Withdraw a session's presence tokens and drop its state. Without ``session=``, every session's."""
    with _registry_lock:
        if session is None:
            states = list(_registry.values())
            _registry.clear()
        else:
            state = _registry.pop(id(session), None)
            states = [state] if state is not None else []
    for state in states:
        state.drop()
