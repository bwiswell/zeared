"""Tests for ``zeared/_managed_session/_on_reconnect_handle.py`` —
the ``OnReconnectHandle`` cancel handle returned by
``ManagedSession.on_reconnect(cb)``."""
from __future__ import annotations

import threading

from zeared._managed_session._on_reconnect_handle import OnReconnectHandle


class TestPublicSurface:
    def test_class_importable(self):
        assert OnReconnectHandle is not None

    def test_uses_slots(self):
        assert OnReconnectHandle.__slots__ == ('_managed', '_entry', '_cancelled')


class _FakeManaged:
    """Minimal stand-in for ManagedSession."""
    def __init__(self):
        self._lock = threading.RLock()
        self._on_reconnect_callbacks = []


class TestCancel:
    def test_cancel_removes_entry(self):
        managed = _FakeManaged()
        entry = (lambda s: None, None)
        managed._on_reconnect_callbacks.append(entry)

        handle = OnReconnectHandle(managed, entry)
        assert handle._cancelled is False
        handle.cancel()
        assert handle._cancelled is True
        assert entry not in managed._on_reconnect_callbacks

    def test_cancel_idempotent(self):
        managed = _FakeManaged()
        entry = (lambda s: None, None)
        managed._on_reconnect_callbacks.append(entry)
        handle = OnReconnectHandle(managed, entry)
        handle.cancel()
        # Second cancel is a no-op (entry already gone).
        handle.cancel()
        assert handle._cancelled is True

    def test_cancel_when_entry_already_gone(self):
        managed = _FakeManaged()
        entry = (lambda s: None, None)
        # Don't add the entry — simulate a race where someone else removed it.
        handle = OnReconnectHandle(managed, entry)
        # cancel() catches the ValueError silently.
        handle.cancel()
        assert handle._cancelled is True
