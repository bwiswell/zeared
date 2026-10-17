"""``presence`` — liveliness tokens at contract keys (0.4.0).

Pattern B subdir; ``presence.py`` is the primary file holding
``PresenceToken``. ``_presence_session.py`` holds the per-session state
(`_SessionPresence`) and its registry (`_registry`, `get_presence`,
`clear_presence_state`).

Subscribers hear about presence through ``on_message(on_online=…,
on_offline=…)``; their liveliness subscriptions live on the ``Subscriber``
itself (``subscriber/``), so there is no cross-session observer.
"""

from ._presence_session import (
    _registry,
    _registry_lock,
    _SessionPresence,
    clear_presence_state,
    get_presence,
)
from .presence import PresenceToken

__all__ = [
    'PresenceToken',
    '_SessionPresence',
    '_registry',
    '_registry_lock',
    'clear_presence_state',
    'get_presence',
]
