"""Per-subscription freshness watchdog.

A subscriber that opts in via ``expected_interval=N`` gets one long-running
worker thread that wakes every ``N`` seconds (or sooner via
``threading.Event.set``). On each successful message dispatch, the
subscriber pings the watchdog. If a wait expires without a ping, the
watchdog fires ``on_quiet`` and goes into "quiet" state. The next ping
fires ``on_active`` and clears the state.

Optimistic semantics: the worker thread does not start until the first
real message arrives. A subscriber that never receives a message will
never fire ``on_quiet`` — useful when a producer is genuinely absent.

Callback-thread caveat: ``on_quiet`` and ``on_active`` fire on the
watchdog thread, NOT on the Zenoh delivery thread. Code that mutates
shared state from these callbacks must handle that.

Async callback caveat: when ``on_quiet`` / ``on_active`` is an ``async def``
function and no event loop is running on the thread that constructs the
``Subscriber``, the adapter falls back to spawning a fresh ``asyncio.run()``
per fire. Correct, but each loop-spinup costs a few ms — fine for
once-per-timeout firings, less so for sub-second intervals. Prefer sync
callbacks here unless you have a specific async dependency. For daemons
running an event loop, register the watchdog from inside the loop so the
adapter captures it once and routes via ``run_coroutine_threadsafe`` (the
cheap path). See ``docs/watchdog.md`` for the full caveat.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from typing import Callable, Optional


_log = logging.getLogger('zeared.watchdog')


def _adapt_maybe_async(cb: Optional[Callable]) -> Optional[Callable]:
    """Like ``subscriber._adapt_async_callback`` but without the running-loop
    requirement: if there's no loop at adaptation time, dispatch via a
    short-lived ``asyncio.run`` per call.

    Watchdog callbacks fire on a non-loop thread, so we typically don't
    have a running loop. ``asyncio.run`` per fire is acceptable for the
    rare on_quiet/on_active events; production hot paths shouldn't be
    routing through a watchdog at message rate.
    """
    if cb is None:
        return None
    if not inspect.iscoroutinefunction(cb):
        return cb

    inner = cb

    # If a running loop is available at adaptation time, schedule on it.
    try:
        loop = asyncio.get_running_loop()

        def _from_loop():
            asyncio.run_coroutine_threadsafe(inner(), loop)

        return _from_loop
    except RuntimeError:
        # No running loop — start one per fire.
        def _bare():
            try:
                asyncio.run(inner())
            except Exception:  # noqa: BLE001
                _log.exception('watchdog async callback raised')

        return _bare


class _SubscriberWatchdog:
    """One long-running thread doing ``event.wait(timeout=N)`` /
    ``event.clear()``. ``ping()`` sets the event from the Zenoh delivery
    thread; ``cancel()`` tears the thread down.

    Two startup modes:

    - **Optimistic** (default — ``startup_grace=None``): the loop thread
      doesn't spawn until the first ``ping()``. A subscriber that never
      receives a message never fires ``on_quiet``.
    - **Grace-window** (``startup_grace=<seconds>``): the loop thread
      spawns at construction time. If no message arrives within
      ``startup_grace`` seconds, ``on_quiet`` fires once. After the first
      message arrives (or grace expires), subsequent waits use
      ``interval`` as before.
    """

    __slots__ = (
        '_interval', '_startup_grace',
        '_on_quiet', '_on_active',
        '_ping', '_cancel', '_thread',
        '_quiet', '_msg_seen', '_lock',
    )

    def __init__(
        self,
        interval: float,
        on_quiet: Optional[Callable],
        on_active: Optional[Callable],
        startup_grace: Optional[float] = None,
    ):
        if interval <= 0:
            raise ValueError(f'expected_interval must be > 0, got {interval}')
        if startup_grace is not None and startup_grace <= 0:
            raise ValueError(
                f'startup_grace must be > 0 or None, got {startup_grace}'
            )
        self._interval = interval
        self._startup_grace = startup_grace
        self._on_quiet = _adapt_maybe_async(on_quiet)
        self._on_active = _adapt_maybe_async(on_active)
        self._ping = threading.Event()
        self._cancel = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._quiet = False
        self._msg_seen = False
        self._lock = threading.Lock()

        if startup_grace is not None:
            # Eager spawn — the loop's first wait covers the grace window.
            self._spawn_thread()

    def _spawn_thread(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name='zeared-watchdog', daemon=True,
        )
        self._thread.start()

    def ping(self) -> None:
        """Called from the Zenoh delivery thread on each successful dispatch."""
        # Lazy first-message thread spawn (optimistic mode).
        with self._lock:
            if self._thread is None:
                self._spawn_thread()
        self._msg_seen = True
        # Setting the event wakes the wait() so the loop reads as "active".
        self._ping.set()
        if self._quiet and self._on_active is not None:
            self._quiet = False
            try:
                self._on_active()
            except Exception:  # noqa: BLE001
                _log.exception('watchdog on_active raised')

    def cancel(self) -> None:
        """Stop the watchdog. Idempotent. Safe to call from any thread."""
        self._cancel.set()
        # Wake the wait so the thread can observe the cancel flag.
        self._ping.set()
        # Don't join — daemon thread; will exit shortly.

    def _loop(self) -> None:
        while not self._cancel.is_set():
            self._ping.clear()
            # Use startup_grace as the very first wait if set AND we
            # haven't seen any message yet. Subsequent waits use interval.
            if not self._msg_seen and self._startup_grace is not None:
                timeout = self._startup_grace
            else:
                timeout = self._interval
            fired = self._ping.wait(timeout=timeout)
            if self._cancel.is_set():
                return
            if not fired:
                # Wait timed out without a ping.
                if not self._quiet:
                    self._quiet = True
                    if self._on_quiet is not None:
                        try:
                            self._on_quiet()
                        except Exception:  # noqa: BLE001
                            _log.exception('watchdog on_quiet raised')
