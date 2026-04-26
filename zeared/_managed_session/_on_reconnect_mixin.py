"""``_OnReconnectMixin`` — the ``on_reconnect`` callback registry +
``_fire_reconnect_callbacks`` driver.

Mixin variant of Pattern B: the ``ManagedSession`` class composes this
mixin via MRO so the callback-registry methods stay as methods (not
helper functions taking ``self``). Per the convention codified in
``CLAUDE.local.md``.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Callable, Optional

from ._on_reconnect_handle import OnReconnectHandle


_log = logging.getLogger('zeared.session')


class _OnReconnectMixin:
    """Reconnect-callback registry and driver. Mixed into ``ManagedSession``.

    Reads / writes ``self._on_reconnect_callbacks`` and ``self._lock`` —
    both are slots on the concrete ``ManagedSession``. This mixin
    contributes no instance state of its own (``__slots__ = ()``).
    """
    __slots__ = ()

    def on_reconnect(
        self,
        cb: Callable[['ManagedSession'], object],   # noqa: F821
    ) -> OnReconnectHandle:
        """Register ``cb`` to fire after every successful reconnect.

        Sync callables run inline on the reconnect thread; ``async def``
        callables are scheduled via ``run_coroutine_threadsafe`` on the
        loop captured at registration time. Multiple callbacks fire in
        registration order. Exceptions from one callback log + continue —
        never break the reconnect itself, never short-circuit later
        callbacks.

        Returns an ``OnReconnectHandle``; call ``handle.cancel()`` to
        deregister. Mirrors ``Subscriber.close()`` / ``z.batch()`` handle
        patterns. The return value is also the registration receipt:
        long-lived daemons typically ignore it.

        ``async def`` registrations require a running event loop on the
        calling thread (mirrors ``Cls.on_message`` with an async callback).
        Without one, raises ``RuntimeError`` immediately — silently
        no-op'ing on the reconnect thread at 3am is the worst possible
        failure mode.
        """
        loop: Optional[asyncio.AbstractEventLoop] = None
        if inspect.iscoroutinefunction(cb):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError as e:
                raise RuntimeError(
                    'on_reconnect: async callback requires a running event '
                    'loop at registration time; either register from inside '
                    'an async context or use a sync callback'
                ) from e
        with self._lock:
            entry = (cb, loop)
            self._on_reconnect_callbacks.append(entry)
        return OnReconnectHandle(self, entry)

    def _fire_reconnect_callbacks(self) -> None:
        """Invoke every registered on_reconnect callback. Called by
        ``_reconnect`` after restoration."""
        with self._lock:
            entries = list(self._on_reconnect_callbacks)
        for cb, loop in entries:
            try:
                if inspect.iscoroutinefunction(cb):
                    coro = cb(self)
                    if loop is not None and not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(coro, loop)
                    else:
                        # Loop went away after registration. Async
                        # callbacks need a loop; we can't recover.
                        _log.warning(
                            'on_reconnect: async callback %r registered '
                            'against an event loop that is no longer '
                            'running; skipping this fire',
                            cb,
                        )
                else:
                    cb(self)
            except Exception:  # noqa: BLE001
                _log.exception('on_reconnect callback raised; continuing')
