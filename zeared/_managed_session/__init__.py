"""``_managed_session`` — reconnect-aware wrapper around ``zenoh.Session``.

Pattern B subdir using the mixin-extract variant. ``_managed_session.py``
holds ``ManagedSession``. Method clusters extracted into mixins:

- ``_on_reconnect_mixin.py`` — ``_OnReconnectMixin`` (the
  ``on_reconnect(cb)`` callback registry + ``_fire_reconnect_callbacks``
  driver).
- ``_zenoh_api_mixin.py`` — ``_ZenohApiMixin`` (pass-through delegators
  for ``zid`` / ``liveliness`` / ``info`` / ``put`` / ``get`` /
  ``delete`` / ``declare_*``).

Other helpers:

- ``_on_reconnect_handle.py`` — ``OnReconnectHandle`` cancel handle.
- ``_helpers.py`` — module-level WeakSet registry, declare-handle
  warning emitter, ``_is_dead`` / ``resolve_raw``.

Public surface unchanged: callers continue to write
``from zeared._managed_session import ManagedSession``.
"""
from ._helpers import (
    _DECLARE_HANDLE_WARNING,
    _DEFAULT_PROBE_INTERVAL,
    _is_dead,
    _managed_sessions,
    _warn_declare_handle,
    resolve_raw,
)
from ._managed_session import ManagedSession
from ._on_reconnect_handle import OnReconnectHandle


__all__ = [
    'ManagedSession',
    'OnReconnectHandle',
    '_DECLARE_HANDLE_WARNING',
    '_DEFAULT_PROBE_INTERVAL',
    '_is_dead',
    '_managed_sessions',
    '_warn_declare_handle',
    'resolve_raw',
]
