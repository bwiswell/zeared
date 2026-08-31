"""``OnReconnectHandle`` — cancel handle returned by ``ManagedSession.on_reconnect(cb)``.

Sibling helper inside the ``_managed_session`` Pattern B subdir.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._managed_session import ManagedSession


class OnReconnectHandle:
    """Cancel handle returned by ``ManagedSession.on_reconnect(cb)``.

    Idiomatic usage:

    ```python
    handle = sess.on_reconnect(refresh_caches)
    ...
    handle.cancel()  # deregister
    ```

    Cancel is idempotent. Holding the handle keeps no extra reference
    to the callback beyond what the registry already has.
    """

    __slots__ = ('_cancelled', '_entry', '_managed')

    def __init__(self, managed: ManagedSession, entry: tuple) -> None:
        self._managed = managed
        self._entry = entry
        self._cancelled = False

    def cancel(self) -> None:
        if self._cancelled:
            return
        self._cancelled = True
        with self._managed._lock, contextlib.suppress(ValueError):  # noqa: SLF001
            self._managed._on_reconnect_callbacks.remove(self._entry)  # noqa: SLF001
