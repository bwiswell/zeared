"""Smoke tests for ``zeared/presence/_presence_session.py`` — the
``_SessionPresence`` class and the per-session presence registry.

Comprehensive end-to-end coverage lives in ``test_presence.py``; this
file confirms the file's public surface is importable and that the
core constructor / registry helpers behave.
"""
from __future__ import annotations

import threading

from zeared.presence._presence_session import (
    _SessionPresence,
    _registry,
    _registry_lock,
    clear_presence_state,
    get_presence,
)


class TestSessionPresence:
    def test_class_importable(self):
        assert _SessionPresence is not None
        assert hasattr(_SessionPresence, '__slots__')

    def test_registry_is_dict(self):
        assert isinstance(_registry, dict)

    def test_registry_lock_is_lock(self):
        # threading.Lock is a factory; the result is a primitive — no
        # public class to isinstance-check. ``acquire`` + ``release``
        # presence is the durable contract.
        assert hasattr(_registry_lock, 'acquire')
        assert hasattr(_registry_lock, 'release')


class TestRegistryHelpers:
    def test_get_presence_callable(self):
        assert callable(get_presence)

    def test_clear_presence_state_callable(self):
        assert callable(clear_presence_state)

    def test_clear_with_no_session_clears_all(self):
        # Smoke: doesn't raise when the registry is empty.
        clear_presence_state()
        assert _registry == {}
