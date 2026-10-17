"""Presence — a Zenoh liveliness token at each contract key (0.4.0).

A publisher declares that one concrete key of a ``LIVELINESS = True`` class
is available with :meth:`Message.declare_presence`, which declares a Zenoh
liveliness token at that key and returns a :class:`PresenceToken`.
Subscribers hear about it through ``on_message(on_online=…, on_offline=…)``:
each event carries the key's captures, never a payload, because a token
can't carry one.

Zenoh aggregates tokens by key: a key reads online when its first token
appears and offline when its last one goes, from whichever sessions held
them. A session that closes, crashes or is released withdraws every token
it held, which is how a peer's death becomes an offline event.

Before 0.4.0 a staged *will* payload played this role, under
``__zeared/will/**`` and ``__zeared/alive/<zid>``. Both key spaces are gone.

Primary file of the ``presence`` Pattern B subdir: holds ``PresenceToken``.
Per-session state and its registry live in ``_presence_session.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from ._presence_session import _SessionPresence


class PresenceToken:
    """One concrete key's availability, held while its session lives.

    Returned by :meth:`Message.declare_presence`. :meth:`undeclare` withdraws
    it while the session lives on (subscribers see the key go offline, once
    no other session holds it); it is idempotent. Closing or releasing the
    session withdraws every token it holds.

    The handle names a key, not a Zenoh token, so it survives a reconnect:
    the restore walk re-declares the key on the new session, and a later
    :meth:`undeclare` withdraws that one. Declaring a key the session already
    holds returns the same handle.
    """

    __slots__ = ('_presence', 'key')

    def __init__(self, presence: _SessionPresence, key: str) -> None:
        """Bind the handle to ``key`` in ``presence``. Built by ``declare_presence``."""
        self._presence = presence
        self.key = key

    @property
    def active(self) -> bool:
        """Whether the session still holds this key through this handle."""
        return self._presence.holds(self.key, self)

    def undeclare(self) -> None:
        """Withdraw the token. Idempotent; a no-op once withdrawn."""
        self._presence.undeclare(self.key, self)

    def __enter__(self) -> Self:
        """Enter ``with``: the token itself."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Leave ``with``: withdraw the token."""
        self.undeclare()

    def __repr__(self) -> str:
        """Show the key and whether it is still held."""
        return f'PresenceToken({self.key!r}, active={self.active})'
