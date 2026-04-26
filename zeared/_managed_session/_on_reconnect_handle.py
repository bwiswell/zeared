"""``OnReconnectHandle`` — cancel handle returned by
``ManagedSession.on_reconnect(cb)``.

Sibling helper inside the ``_managed_session`` Pattern B subdir.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._managed_session import ManagedSession


class OnReconnectHandle:
    """Cancel handle returned by ``ManagedSession.on_reconnect(cb)``.

    Idiomatic usage:

    ```python
    handle = sess.on_reconnect(refresh_caches)
    ...
    handle.cancel()                  # deregister
    ```

    Cancel is idempotent. Holding the handle keeps no extra reference
    to the callback beyond what the registry already has.
    """
    __slots__ = ('_managed', '_entry', '_cancelled')

    def __init__(self, managed: 'ManagedSession', entry: tuple):
        self._managed = managed
        self._entry = entry
        self._cancelled = False

    def cancel(self) -> None:
        if self._cancelled:
            return
        self._cancelled = True
        with self._managed._lock:
            try:
                self._managed._on_reconnect_callbacks.remove(self._entry)
            except ValueError:
                pass
