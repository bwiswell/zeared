"""Smoke tests for ``zeared/_managed_session/_on_reconnect_mixin.py``
— the ``_OnReconnectMixin`` (callback registry + fire driver).

End-to-end reconnect-callback coverage lives in ``test__reconnect.py``.
This file confirms the mixin's public surface and basic registration.
"""
from __future__ import annotations

import threading

from zeared._managed_session._on_reconnect_handle import OnReconnectHandle
from zeared._managed_session._on_reconnect_mixin import _OnReconnectMixin


class TestMixinSurface:
    def test_class_importable(self):
        assert _OnReconnectMixin is not None

    def test_no_instance_state(self):
        assert _OnReconnectMixin.__slots__ == ()

    def test_methods_present(self):
        assert hasattr(_OnReconnectMixin, 'on_reconnect')
        assert hasattr(_OnReconnectMixin, '_fire_reconnect_callbacks')


class _FakeManaged(_OnReconnectMixin):
    """Minimal stand-in to exercise the mixin without spinning Zenoh."""
    def __init__(self):
        self._lock = threading.RLock()
        self._on_reconnect_callbacks = []


class TestRegistrationAndFire:
    def test_register_returns_handle(self):
        m = _FakeManaged()
        h = m.on_reconnect(lambda s: None)
        assert isinstance(h, OnReconnectHandle)
        assert len(m._on_reconnect_callbacks) == 1

    def test_fire_calls_each_sync_callback(self):
        m = _FakeManaged()
        seen: list = []
        m.on_reconnect(lambda s: seen.append('a'))
        m.on_reconnect(lambda s: seen.append('b'))
        m._fire_reconnect_callbacks()
        assert seen == ['a', 'b']

    def test_callback_exception_doesnt_break_subsequent(self):
        m = _FakeManaged()
        seen: list = []

        def raises(s):
            raise RuntimeError('boom')

        m.on_reconnect(raises)
        m.on_reconnect(lambda s: seen.append('after-raise'))
        m._fire_reconnect_callbacks()
        # Second callback still fired despite the first raising.
        assert seen == ['after-raise']

    def test_cancel_via_handle_deregisters(self):
        m = _FakeManaged()
        seen: list = []
        h = m.on_reconnect(lambda s: seen.append('x'))
        h.cancel()
        m._fire_reconnect_callbacks()
        assert seen == []
