"""``_OnReconnectMixin`` — the ``on_reconnect`` / ``on_link_lost`` / ``on_link_restored`` registries + driver.

``ManagedSession`` composes this stateless mixin via the MRO so the
callback-registry methods stay as methods, rather than becoming helper
functions taking ``self``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import TYPE_CHECKING

from ._callback_handle import CallbackHandle

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._managed_session import ManagedSession


_log = logging.getLogger('zeared.session')


class _OnReconnectMixin:
    """The callback registries and their driver. Mixed into ``ManagedSession``.

    Three registries, one per signal (0.4.0):

    - ``on_reconnect`` — the raw session was swapped and every restore walk ran;
    - ``on_link_lost`` — this session's view of the mesh went stale: its router
      link dropped, or the session was found dead;
    - ``on_link_restored`` — the view is whole again, after the re-attach
      replay walk or a reconnect's walks.

    Reads / writes ``self._on_<kind>_callbacks`` and ``self._lock`` — slots on
    the concrete ``ManagedSession``. This mixin contributes no instance state
    of its own (``__slots__ = ()``).
    """

    __slots__ = ()

    def _register(self: ManagedSession, kind: str, cb: Callable[[ManagedSession], object]) -> CallbackHandle:
        """Add ``cb`` to the ``kind`` registry and return its cancel handle."""
        loop: asyncio.AbstractEventLoop | None = None
        if inspect.iscoroutinefunction(cb):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError as e:
                msg = (
                    f'on_{kind}: async callback requires a running event '
                    'loop at registration time; either register from inside '
                    'an async context or use a sync callback'
                )
                raise RuntimeError(msg) from e
        registry = getattr(self, f'_on_{kind}_callbacks')
        with self._lock:
            entry = (cb, loop)
            registry.append(entry)
        return CallbackHandle(self, entry, registry)

    def on_reconnect(self: ManagedSession, cb: Callable[[ManagedSession], object]) -> CallbackHandle:
        """Register ``cb`` to fire after every successful reconnect.

        Fires once the raw session was swapped and every restore walk ran.
        Sync callables run inline on the reconnect thread; ``async def``
        callables are scheduled via ``run_coroutine_threadsafe`` on the
        loop captured at registration time. Multiple callbacks fire in
        registration order. Exceptions from one callback log + continue —
        never break the reconnect itself, never short-circuit later
        callbacks.

        Returns a ``CallbackHandle``; call ``handle.cancel()`` to deregister.
        Long-lived daemons typically ignore it.

        ``async def`` registrations require a running event loop on the
        calling thread (mirrors ``Cls.on_message`` with an async callback).
        Without one, raises ``RuntimeError`` immediately — silently
        no-op'ing on the reconnect thread at 3am is the worst possible
        failure mode.

        A lost router link (a hub restart) doesn't swap the raw session, so
        it doesn't fire this; ``on_link_lost`` / ``on_link_restored`` cover
        both cases.
        """
        return self._register('reconnect', cb)

    def on_link_lost(self: ManagedSession, cb: Callable[[ManagedSession], object]) -> CallbackHandle:
        """Register ``cb`` to fire when this session's view of the mesh goes stale (0.4.0).

        Fires when the session's routers drop to none while the raw session
        lives (a hub outage), or when the raw session is found dead, before
        the reconnect starts. Never twice without an ``on_link_restored`` in
        between. The probe's link watch never fires it for a session that
        never had a router, but a reconnect fires the pair in any mode. Same
        threading, async and error rules as ``on_reconnect``.
        """
        return self._register('link_lost', cb)

    def on_link_restored(self: ManagedSession, cb: Callable[[ManagedSession], object]) -> CallbackHandle:
        """Register ``cb`` to fire when the view is whole again: re-sync now (0.4.0).

        Fires after the re-attach replay walk completes (retained values the
        session missed while detached are delivered first), or after a
        reconnect's walks and ``on_reconnect``'s callbacks. Only after an
        ``on_link_lost``; a session that goes ``DEAD`` never restores. Same
        threading, async and error rules as ``on_reconnect``.
        """
        return self._register('link_restored', cb)

    def _fire(self: ManagedSession, kind: str) -> None:
        """Invoke every callback registered for ``kind``, in registration order."""
        with self._lock:
            entries = list(getattr(self, f'_on_{kind}_callbacks'))
        for cb, loop in entries:
            try:
                if inspect.iscoroutinefunction(cb):
                    coro = cb(self)
                    if loop is not None and not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(coro, loop)
                    else:
                        # Loop went away after registration. Async
                        # callbacks need a loop; we can't recover.
                        coro.close()
                        _log.warning(
                            'on_%s: async callback %r registered against an event loop '
                            'that is no longer running; skipping this fire',
                            kind,
                            cb,
                        )
                else:
                    cb(self)
            except Exception:  # noqa: BLE001
                _log.exception('on_%s callback raised; continuing', kind)

    def _fire_reconnect_callbacks(self: ManagedSession) -> None:
        """Invoke every registered on_reconnect callback. Called by ``_reconnect`` after restoration."""
        self._fire('reconnect')
