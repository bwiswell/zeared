"""``CallbackHandle`` — the cancel handle ``ManagedSession``'s callback registries return.

Sibling helper inside the ``_managed_session`` Pattern B subdir. One class for
``on_reconnect``, ``on_link_lost`` and ``on_link_restored`` (0.4.0);
``OnReconnectHandle``, its name before 0.4.0, is kept as an alias.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._managed_session import ManagedSession


class CallbackHandle:
    """Cancel handle returned by ``ManagedSession.on_reconnect(cb)`` and its siblings.

    Idiomatic usage:

    ```python
    handle = sess.on_link_restored(refresh_caches)
    ...
    handle.cancel()  # deregister
    ```

    Cancel is idempotent. Holding the handle keeps no extra reference
    to the callback beyond what the registry already has.
    """

    __slots__ = ('_cancelled', '_entry', '_managed', '_registry')

    def __init__(self, managed: ManagedSession, entry: tuple, registry: list | None = None) -> None:
        """Bind to ``entry`` in ``registry`` (the ``on_reconnect`` list when omitted)."""
        self._managed = managed
        self._entry = entry
        self._registry = registry if registry is not None else managed._on_reconnect_callbacks  # noqa: SLF001
        self._cancelled = False

    def cancel(self) -> None:
        """Deregister the callback. Idempotent."""
        if self._cancelled:
            return
        self._cancelled = True
        with self._managed._lock, contextlib.suppress(ValueError):  # noqa: SLF001
            self._registry.remove(self._entry)


#: The handle's name before 0.4.0, when only ``on_reconnect`` returned one.
OnReconnectHandle = CallbackHandle
