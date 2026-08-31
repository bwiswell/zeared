"""Reconnect-aware wrapper around ``zenoh.Session``.

Users hold the wrapper forever; the underlying raw session is swapped
atomically on reconnect. ``__getattr__`` delegates unknown attributes to
the current raw session — for namespace handles (``.liveliness()``,
``.zid()``, etc.) we use explicit wrappers so handles always reflect the
current raw and never become stale.

Primary file of the ``_managed_session`` Pattern B subdir. Method
clusters extracted via mixin (per the variant codified in
``CLAUDE.local.md``):

- ``_OnReconnectMixin`` (in ``_on_reconnect_mixin.py``) — the
  ``on_reconnect(cb)`` callback registry + ``_fire_reconnect_callbacks``
  driver.
- ``_ZenohApiMixin`` (in ``_zenoh_api_mixin.py``) — pass-through
  delegators for ``zid`` / ``liveliness`` / ``info`` / ``put`` / ``get``
  / ``delete`` / ``declare_*``.

See ``_reconnect/`` for the detection + restoration logic.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from typing import TYPE_CHECKING, Any, Self

from ..errors import SessionDeadError
from ._helpers import _is_dead, _managed_sessions
from ._on_reconnect_mixin import _OnReconnectMixin
from ._zenoh_api_mixin import _ZenohApiMixin

if TYPE_CHECKING:
    import asyncio
    import types
    from collections.abc import Callable

    import zenoh

_log = logging.getLogger('zeared.session')


class ManagedSession(_OnReconnectMixin, _ZenohApiMixin):
    """Reconnect-aware facade over a raw ``zenoh.Session``.

    Surface mirrors ``zenoh.Session`` for the methods zeared cares about,
    plus an explicit ``raw()`` escape hatch. ``__getattr__`` delegates the
    rest. Internal state is mutated under a lock during reconnect; readers
    grab a local reference at the top of each method to avoid racing the
    swap.

    State machine:
      IDLE          — raw session is healthy.
      RECONNECTING  — probe / send-failure detected death; reconnect in
                      progress (or will be on next probe tick).
      DEAD          — reconnect terminally failed (max_attempts exhausted).
                      All ops raise ``SessionDeadError``.
    """

    __slots__ = (
        '__weakref__',  # required for WeakSet membership
        '_endpoint_label',
        '_gc_interval',  # presence-observer GC sweep period
        '_initial_backoff',
        '_lock',
        '_max_attempts',
        '_max_backoff',
        '_on_reconnect',  # legacy single-callback test hook
        '_on_reconnect_callbacks',  # public list — registered via on_reconnect()
        '_open_fn',
        '_probe_cancel',
        '_probe_interval',
        '_probe_thread',
        '_raw',
        '_reconnect_signal',
        '_reconnect_thread',
        '_retention_ttl',  # session-wide retention TTL fallback
        '_state',
    )

    def __init__(  # noqa: PLR0913
        self,
        raw: zenoh.Session,
        open_fn: Callable[[], zenoh.Session],
        *,
        endpoint_label: str,
        probe_interval: float,
        initial_backoff: float,
        max_backoff: float,
        max_attempts: int | None,
    ) -> None:
        self._raw = raw
        self._lock = threading.RLock()
        self._state = 'IDLE'
        self._open_fn = open_fn
        self._endpoint_label = endpoint_label
        self._probe_thread: threading.Thread | None = None
        self._probe_cancel = threading.Event()
        self._probe_interval = probe_interval
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._max_attempts = max_attempts
        self._on_reconnect: Callable[[ManagedSession], None] | None = None
        # (cb, loop) entries — loop is None for sync callbacks, the
        # captured running loop for coroutine callbacks.
        self._on_reconnect_callbacks: list[
            tuple[Callable[[ManagedSession], object], asyncio.AbstractEventLoop | None]
        ] = []
        # Single long-lived reconnect worker per ManagedSession.
        self._reconnect_thread: threading.Thread | None = None
        self._reconnect_signal = threading.Event()
        # GC sweep period for the per-session presence observer. Factory
        # callers override via ``peer(gc_interval=...)``; default mirrors
        # ``presence._GC_INTERVAL_SECONDS``.
        self._gc_interval = 60.0
        # Session-wide retention TTL fallback. ``None`` (default) means
        # no session-level fallback — class-level ``RETENTION_TTL`` is
        # the only knob. Factory ``peer(retention_ttl=N)`` overrides.
        self._retention_ttl: float | None = None
        # Register in the module-level WeakSet so ``release_all`` can
        # find this wrapper even when no zeared-level state has been
        # registered against it.
        _managed_sessions.add(self)

    # -- public escape hatch --------------------------------------------------

    def raw(self) -> zenoh.Session:
        """Return the current underlying ``zenoh.Session``.

        Valid until the next reconnect. Don't cache across reconnect
        windows — call ``raw()`` each time you need to hand a Zenoh
        session to non-zeared code.
        """
        with self._lock:
            return self._raw

    @property
    def state(self) -> str:
        """One of ``'IDLE'`` / ``'RECONNECTING'`` / ``'DEAD'``."""
        with self._lock:
            return self._state

    def close(self) -> None:
        """Stop the probe thread and close the current raw session.

        Idempotent. Distinct from ``z.release(session=)`` which also tears
        down zeared state — most callers should use ``z.release`` for full
        cleanup. ``close()`` here is the namesake of ``zenoh.Session.close``.
        """
        self._teardown(call_close=True)

    # -- context-manager protocol --------------------------------------------

    def __enter__(self) -> Self:
        """Enter the context manager — returns the wrapper itself.

        Holding the wrapper across the block is the whole point — code
        inside should bind to ``self`` (the ``ManagedSession``) rather
        than ``self.raw()`` (the underlying raw, which is invalidated
        on every reconnect).
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: types.TracebackType | None,
    ) -> None:
        """Exit the context manager, releasing the session.

        Exit — runs ``z.release(session=self)``: cancels the probe + reconnect worker
        threads, walks per-session registries (subscribers, retention, presence), then
        closes the raw session. Doesn't suppress exceptions; ``release()`` raises
        propagate.
        """
        from .. import release

        release(session=self)

    # -- reconnect plumbing (used by _reconnect/) -----------------------------

    def _swap_raw(self, new_raw: zenoh.Session) -> zenoh.Session:
        """Atomically swap the underlying raw session. Returns the old raw."""
        with self._lock:
            old, self._raw = self._raw, new_raw
            self._state = 'IDLE'
        return old

    def _set_state(self, state: str) -> None:
        with self._lock:
            self._state = state

    def _guard_alive(self) -> None:
        """Raise ``SessionDeadError`` if the session is mid-reconnect or dead."""
        with self._lock:
            state = self._state
        if state == 'RECONNECTING':
            msg = f'session {self._endpoint_label} is reconnecting; retry the operation after the reconnect window'
            raise SessionDeadError(msg)
        if state == 'DEAD':
            msg = f'session {self._endpoint_label} terminally failed reconnect and is no longer usable'
            raise SessionDeadError(msg)

    def _note_failure(self, exc: BaseException) -> None:  # noqa: ARG002  (kept for call-site symmetry)
        """Mark the session as needing a reconnect after a failed op.

        Lazy-detection hook: if a put/get/delete fails on the raw, mark the session as
        needing a reconnect (the probe loop will pick it up on the next tick, or the
        next call will trigger eagerly via ``_maybe_reconnect``).

        Conservative: we don't try to classify the exception. Any failure
        on the raw session is treated as "might be dead" — the probe will
        confirm via ``is_closed()`` / ``zid()`` and trigger the reconnect.
        """
        if _is_dead(self._raw):
            from .._reconnect import _trigger_reconnect

            _trigger_reconnect(self)

    def _teardown(self, *, call_close: bool) -> None:
        """Cancel the probe + reconnect worker threads, close the raw session. Idempotent."""
        self._probe_cancel.set()
        # Wake the reconnect worker so it sees the cancel and exits.
        self._reconnect_signal.set()

        probe = self._probe_thread
        worker = self._reconnect_thread
        self._probe_thread = None
        self._reconnect_thread = None
        if probe is not None and probe.is_alive():
            probe.join(timeout=1.0)
        if worker is not None and worker.is_alive():
            worker.join(timeout=1.0)
        if call_close:
            with contextlib.suppress(Exception):
                self._raw.close()

    # -- catch-all delegation -------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        # Called only if the attribute isn't found via normal lookup
        # (i.e. not in __slots__ and not a method defined here).
        return getattr(self._raw, name)

    def __repr__(self) -> str:
        return f'<ManagedSession state={self.state} endpoint={self._endpoint_label!r}>'
