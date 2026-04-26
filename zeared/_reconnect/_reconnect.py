"""Reconnect orchestration for ``ManagedSession``.

Two detection paths feed one reconnect implementation:

  - ``_probe_loop`` — daemon thread per ``ManagedSession``, polls
    ``is_closed()`` (or ``zid()`` fallback) every ``probe_interval``s.
    Required for subscriber-only daemons that never call ``put()``.
  - ``_trigger_reconnect`` — called from ``ManagedSession.put`` / ``.get``
    / ``.delete`` exception paths via ``_note_failure``. Catches the
    0–``probe_interval`` gap on publisher-heavy paths.

Both feed ``_reconnect`` which:
  1. CAS ``state`` IDLE → RECONNECTING.
  2. Open a new raw session via ``open_fn`` with backoff.
  3. Atomically swap the wrapper's raw reference.
  4. Walk the subscriber registry and re-declare each.
  5. Replay any registered presence wills under the new zid.
  6. Close the old raw session quietly.

If step 2 exhausts ``max_attempts``, set ``state = DEAD`` and stop the
probe.

Primary file of the ``_reconnect`` Pattern B subdir. The four
``_restore_*`` helpers + the cancellable ``_open_with_backoff`` loop
live in the sibling ``_restore.py``.
"""
from __future__ import annotations

import logging
import threading

from .._managed_session import ManagedSession, _is_dead
from ._restore import (
    _ReconnectAborted,
    _open_with_backoff,
    _restore_retention,
    _restore_subscribers,
    _restore_wills,
)


_log = logging.getLogger('zeared.reconnect')


def start_probe(managed: ManagedSession) -> None:
    """Spawn the probe daemon + the long-lived reconnect worker.

    Idempotent — call after the wrapper is fully constructed and the
    initial raw session is healthy. Two daemon threads per ManagedSession,
    fixed:
      - probe: polls liveness every ``probe_interval``s; signals the
        reconnect worker on detected death.
      - reconnect worker: blocks on the signal, runs ``_reconnect`` on
        each wake, exits cleanly when ``_probe_cancel`` is set or state
        becomes ``DEAD``.

    If ``probe_interval`` is ``0`` / ``None``, the probe thread is
    skipped (lazy-only detection); the worker still runs to consume
    send-failure-driven triggers.
    """
    managed._probe_cancel.clear()
    managed._reconnect_signal.clear()

    if managed._reconnect_thread is None:
        worker = threading.Thread(
            target=_reconnect_worker, args=(managed,),
            daemon=True, name=f'zeared-reconnect-{id(managed):x}',
        )
        managed._reconnect_thread = worker
        worker.start()

    if managed._probe_interval and managed._probe_interval > 0:
        if managed._probe_thread is None:
            t = threading.Thread(
                target=_probe_loop, args=(managed,),
                daemon=True, name=f'zeared-probe-{id(managed):x}',
            )
            managed._probe_thread = t
            t.start()


def _probe_loop(managed: ManagedSession) -> None:
    while not managed._probe_cancel.wait(managed._probe_interval):
        try:
            if managed.state == 'DEAD':
                return
            if managed.state == 'RECONNECTING':
                continue
            if _is_dead(managed._raw):
                _trigger_reconnect(managed)
        except Exception:  # noqa: BLE001
            _log.exception('zeared probe loop iteration raised — continuing')


def _reconnect_worker(managed: ManagedSession) -> None:
    """Long-lived reconnect worker — one per ManagedSession.

    Blocks on ``_reconnect_signal``; runs ``_reconnect`` on each wake.
    Exits when ``_probe_cancel`` is set (teardown) or state hits ``DEAD``.
    Coalesces concurrent triggers — the Event collapses N signals to one.
    """
    while not managed._probe_cancel.is_set():
        # ``wait()`` with no timeout blocks until set; clearing it before
        # the work means a concurrent trigger during the work cycle is
        # not lost — it just sets the Event again.
        managed._reconnect_signal.wait()
        managed._reconnect_signal.clear()
        if managed._probe_cancel.is_set():
            return
        if managed.state == 'DEAD':
            return
        try:
            _reconnect(managed)
        except Exception:  # noqa: BLE001
            _log.exception('reconnect worker iteration raised — continuing')


def _trigger_reconnect(managed: ManagedSession) -> None:
    """CAS into ``RECONNECTING`` and signal the reconnect worker.

    Concurrent triggers (probe + send-failure on the same tick) collapse
    to one reconnect attempt; the loser sees state already RECONNECTING
    and bails. The worker drains all pending triggers via Event
    semantics.

    Caller contract: ``start_probe(managed)`` MUST have run before any
    trigger fires — every factory-built session does this automatically;
    tests that drive ``_trigger_reconnect`` directly must call
    ``start_probe`` first.
    """
    with managed._lock:
        if managed._state in ('RECONNECTING', 'DEAD'):
            return
        managed._state = 'RECONNECTING'
    managed._reconnect_signal.set()


def _reconnect(managed: ManagedSession) -> None:
    """Open a fresh raw session with backoff; atomically swap; restore
    subscribers + wills. Sets state = DEAD on terminal failure.
    """
    label = managed._endpoint_label
    try:
        new_raw = _open_with_backoff(
            managed._open_fn,
            initial=managed._initial_backoff,
            cap=managed._max_backoff,
            max_attempts=managed._max_attempts,
            label=label,
            cancel=managed._probe_cancel,
        )
    except _ReconnectAborted:
        # The wrapper was torn down mid-reconnect (z.release).
        managed._set_state('DEAD')
        return
    except Exception as exc:  # noqa: BLE001
        _log.warning('reconnect for %s exhausted retries: %s', label, exc)
        managed._set_state('DEAD')
        managed._probe_cancel.set()
        return

    old_raw = managed._swap_raw(new_raw)
    _log.info('reconnect for %s succeeded; new zid=%s', label, new_raw.zid())

    # Restoration order (reconnect = startup, dependencies before
    # dependents):
    #   1. Retention queryables — publisher-side infrastructure that
    #      subscribers' retained-fetch will hit. MUST come before
    #      subscriber redeclare so a same-process publisher+subscriber
    #      pair finds a live queryable on the retained-fetch round.
    #   2. Subscribers — re-declare zenoh subs, re-fire retained fetch
    #      (dedupe-safe), re-register presence dispatcher.
    #   3. Wills — re-register every previously-registered envelope
    #      under the new zid; peers see legitimate offline → online.
    _restore_retention(managed)
    _restore_subscribers(managed)
    _restore_wills(managed)

    # Quietly close the old raw — best effort.
    try:
        old_raw.close()
    except Exception:  # noqa: BLE001
        pass

    # Legacy single-callback test hook (kept for tests).
    cb = managed._on_reconnect
    if cb is not None:
        try:
            cb(managed)
        except Exception:  # noqa: BLE001
            _log.exception('on_reconnect hook raised')

    # Public on_reconnect callbacks — fire in registration order.
    managed._fire_reconnect_callbacks()
