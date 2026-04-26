"""``_ZenohApiMixin`` — Zenoh-Session pass-through methods.

Methods that mirror ``zenoh.Session``'s surface (``zid``, ``liveliness``,
``info``, ``put``, ``get``, ``delete``, ``declare_*``). All read the
current raw via ``self.raw()`` so they always reflect the post-reconnect
session — never a stale handle.

Mixed into ``ManagedSession`` per the mixin-extract variant of
Pattern B (codified in ``CLAUDE.local.md``).
"""
from __future__ import annotations

from ._helpers import _warn_declare_handle


class _ZenohApiMixin:
    """Pass-through delegators for the ``zenoh.Session`` surface.

    Reads ``self._raw`` via ``self.raw()`` so a freshly-swapped raw is
    used on every call. Mutating methods (``put`` / ``get`` / ``delete``)
    additionally call ``self._note_failure`` on exception so a hung send
    drives lazy reconnect detection. ``declare_*`` methods emit a
    one-shot ``RuntimeWarning`` because the returned handle is bound
    to the current raw and won't survive reconnect.

    No instance state of its own — ``__slots__ = ()``.
    """
    __slots__ = ()

    # -- explicit wrappers (always current) -------------------------------
    #
    # Methods that callers might stash a result from get wrapped explicitly
    # so the result never points at an old raw session.

    def zid(self):
        self._guard_alive()
        return self.raw().zid()

    def liveliness(self):
        self._guard_alive()
        return self.raw().liveliness()

    @property
    def info(self):
        self._guard_alive()
        return self.raw().info

    def put(self, *args, **kwargs):
        self._guard_alive()
        try:
            return self.raw().put(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            self._note_failure(exc)
            raise

    def get(self, *args, **kwargs):
        self._guard_alive()
        try:
            return self.raw().get(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            self._note_failure(exc)
            raise

    def delete(self, *args, **kwargs):
        self._guard_alive()
        try:
            return self.raw().delete(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            self._note_failure(exc)
            raise

    def declare_publisher(self, *args, **kwargs):
        self._guard_alive()
        _warn_declare_handle('declare_publisher')
        return self.raw().declare_publisher(*args, **kwargs)

    def declare_subscriber(self, *args, **kwargs):
        self._guard_alive()
        _warn_declare_handle('declare_subscriber')
        return self.raw().declare_subscriber(*args, **kwargs)

    def declare_queryable(self, *args, **kwargs):
        self._guard_alive()
        _warn_declare_handle('declare_queryable')
        return self.raw().declare_queryable(*args, **kwargs)
