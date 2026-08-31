"""``_ZenohApiMixin`` — Zenoh-Session pass-through methods.

Methods that mirror ``zenoh.Session``'s surface (``zid``, ``liveliness``,
``info``, ``put``, ``get``, ``delete``, ``declare_*``). All read the
current raw via ``self.raw()`` so they always reflect the post-reconnect
session — never a stale handle.

Mixed into ``ManagedSession`` per the mixin-extract variant of
Pattern B (codified in ``CLAUDE.local.md``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ._helpers import _warn_declare_handle

if TYPE_CHECKING:
    import zenoh

    from ._helpers import _ManagedSessionProto


class _ZenohApiMixin:
    """Pass-through delegators for the ``zenoh.Session`` surface.

    Reads ``self._raw`` via ``self.raw()`` so a freshly-swapped raw is
    used on every call. Mutating methods (``put`` / ``get`` / ``delete``)
    additionally call ``self._note_failure`` on exception so a hung send
    drives lazy reconnect detection. ``declare_*`` methods emit a
    one-shot ``RuntimeWarning`` because the returned handle is bound
    to the current raw and won't survive reconnect.

    No instance state of its own — ``__slots__ = ()``. Every method
    annotates ``self`` with ``_ManagedSessionProto`` — the mixin is only
    ever reached through a concrete ``ManagedSession``, and spelling that
    out lets the delegators type-check in isolation.
    """

    __slots__ = ()

    # -- explicit wrappers (always current) -------------------------------
    #
    # Methods that callers might stash a result from get wrapped explicitly
    # so the result never points at an old raw session.

    def zid(self: _ManagedSessionProto) -> zenoh.ZenohId:
        self._guard_alive()
        return self.raw().zid()

    def liveliness(self: _ManagedSessionProto) -> zenoh.Liveliness:
        self._guard_alive()
        return self.raw().liveliness()

    @property
    def info(self: _ManagedSessionProto) -> zenoh.SessionInfo:
        self._guard_alive()
        return self.raw().info

    def put(self: _ManagedSessionProto, *args: Any, **kwargs: Any) -> None:
        self._guard_alive()
        try:
            return self.raw().put(*args, **kwargs)
        except Exception as exc:
            self._note_failure(exc)
            raise

    def get(self: _ManagedSessionProto, *args: Any, **kwargs: Any) -> Any:
        self._guard_alive()
        try:
            return self.raw().get(*args, **kwargs)
        except Exception as exc:
            self._note_failure(exc)
            raise

    def delete(self: _ManagedSessionProto, *args: Any, **kwargs: Any) -> None:
        self._guard_alive()
        try:
            return self.raw().delete(*args, **kwargs)
        except Exception as exc:
            self._note_failure(exc)
            raise

    def declare_publisher(self: _ManagedSessionProto, *args: Any, **kwargs: Any) -> zenoh.Publisher:
        self._guard_alive()
        _warn_declare_handle('declare_publisher')
        return self.raw().declare_publisher(*args, **kwargs)

    def declare_subscriber(self: _ManagedSessionProto, *args: Any, **kwargs: Any) -> zenoh.Subscriber:
        self._guard_alive()
        _warn_declare_handle('declare_subscriber')
        return self.raw().declare_subscriber(*args, **kwargs)

    def declare_queryable(self: _ManagedSessionProto, *args: Any, **kwargs: Any) -> zenoh.Queryable:
        self._guard_alive()
        _warn_declare_handle('declare_queryable')
        return self.raw().declare_queryable(*args, **kwargs)
