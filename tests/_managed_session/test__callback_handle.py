"""Tests for ``zeared/_managed_session/_callback_handle.py`` — the
``CallbackHandle`` cancel handle ``ManagedSession``'s callback registries
return (``OnReconnectHandle`` is its pre-0.4.0 alias)."""

from __future__ import annotations

import threading

import zeared as z
from zeared._managed_session._callback_handle import CallbackHandle, OnReconnectHandle


class TestPublicSurface:
    def test_exported_with_its_alias(self):
        assert z.CallbackHandle is CallbackHandle
        assert z.OnReconnectHandle is CallbackHandle
        assert OnReconnectHandle is CallbackHandle

    def test_uses_slots(self):
        # Order is incidental (ruff's RUF023 keeps __slots__ sorted).
        assert set(CallbackHandle.__slots__) == {'_managed', '_entry', '_registry', '_cancelled'}


class _FakeManaged:
    """Minimal stand-in for ManagedSession."""

    def __init__(self):
        self._lock = threading.RLock()
        self._on_reconnect_callbacks = []
        self._on_link_restored_callbacks = []


class TestCancel:
    def test_cancel_removes_entry_from_its_registry(self):
        managed = _FakeManaged()
        entry = (lambda s: None, None)
        managed._on_link_restored_callbacks.append(entry)
        managed._on_reconnect_callbacks.append(entry)

        handle = CallbackHandle(managed, entry, managed._on_link_restored_callbacks)
        handle.cancel()
        assert handle._cancelled is True
        assert entry not in managed._on_link_restored_callbacks
        assert entry in managed._on_reconnect_callbacks  # only its own registry

    def test_registry_defaults_to_on_reconnect(self):
        managed = _FakeManaged()
        entry = (lambda s: None, None)
        managed._on_reconnect_callbacks.append(entry)
        OnReconnectHandle(managed, entry).cancel()
        assert managed._on_reconnect_callbacks == []

    def test_cancel_idempotent(self):
        managed = _FakeManaged()
        entry = (lambda s: None, None)
        managed._on_reconnect_callbacks.append(entry)
        handle = CallbackHandle(managed, entry)
        handle.cancel()
        handle.cancel()
        assert handle._cancelled is True

    def test_cancel_when_entry_already_gone(self):
        managed = _FakeManaged()
        handle = CallbackHandle(managed, (lambda s: None, None))
        handle.cancel()  # the ValueError is swallowed
        assert handle._cancelled is True
