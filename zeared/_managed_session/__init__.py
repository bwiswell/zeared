"""``_managed_session`` — reconnect-aware wrapper around ``zenoh.Session``.

Pattern B subdir using the mixin-extract variant. ``_managed_session.py``
holds ``ManagedSession``. Method clusters extracted into mixins:

- ``_on_reconnect_mixin.py`` — ``_OnReconnectMixin`` (the
  ``on_reconnect`` / ``on_link_lost`` / ``on_link_restored`` callback
  registries + their driver).
- ``_zenoh_api_mixin.py`` — ``_ZenohApiMixin`` (pass-through delegators
  for ``zid`` / ``liveliness`` / ``info`` / ``put`` / ``get`` /
  ``delete`` / ``declare_*``).

Other helpers:

- ``_callback_handle.py`` — ``CallbackHandle`` cancel handle (``OnReconnectHandle`` is an alias).
- ``_helpers.py`` — module-level WeakSet registry, declare-handle
  warning emitter, ``_is_dead`` / ``resolve_raw``.

Public surface unchanged: callers continue to write
``from zeared._managed_session import ManagedSession``.
"""

from typing import TYPE_CHECKING

from ._callback_handle import CallbackHandle, OnReconnectHandle
from ._helpers import (
    _DECLARE_HANDLE_WARNING,
    _DEFAULT_PROBE_INTERVAL,
    _is_dead,
    _managed_sessions,
    _warn_declare_handle,
    resolve_raw,
)
from ._managed_session import ManagedSession

if TYPE_CHECKING:
    # Re-exported for annotations only — deliberately absent from __all__.
    from ._helpers import SessionLike as SessionLike


__all__ = [
    '_DECLARE_HANDLE_WARNING',
    '_DEFAULT_PROBE_INTERVAL',
    'CallbackHandle',
    'ManagedSession',
    'OnReconnectHandle',
    '_is_dead',
    '_managed_sessions',
    '_warn_declare_handle',
    'resolve_raw',
]
