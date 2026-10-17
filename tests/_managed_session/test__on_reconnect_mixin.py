"""Smoke tests for ``zeared/_managed_session/_on_reconnect_mixin.py``
— the ``_OnReconnectMixin`` (callback registry + fire driver).

End-to-end reconnect-callback coverage lives in ``test__reconnect.py``.
This file confirms the mixin's public surface and basic registration.
"""

from __future__ import annotations

import threading

from zeared._managed_session._callback_handle import CallbackHandle
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
        self._on_link_lost_callbacks = []
        self._on_link_restored_callbacks = []


class TestRegistrationAndFire:
    def test_register_returns_handle(self):
        m = _FakeManaged()
        h = m.on_reconnect(lambda s: None)
        assert isinstance(h, CallbackHandle)
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
            msg = 'boom'
            raise RuntimeError(msg)

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


class TestLinkSignals:
    """0.4.0: ``on_link_lost`` / ``on_link_restored`` share the driver, each with its own registry."""

    def test_each_kind_fires_only_its_own(self):
        m = _FakeManaged()
        seen: list = []
        m.on_reconnect(lambda s: seen.append('reconnect'))
        m.on_link_lost(lambda s: seen.append('lost'))
        m.on_link_restored(lambda s: seen.append('restored'))
        m._fire('link_lost')
        m._fire('link_restored')
        m._fire_reconnect_callbacks()
        assert seen == ['lost', 'restored', 'reconnect']

    def test_handles_cancel_their_own_kind(self):
        m = _FakeManaged()
        seen: list = []
        handle = m.on_link_restored(lambda s: seen.append('restored'))
        m.on_link_lost(lambda s: seen.append('lost'))
        assert isinstance(handle, CallbackHandle)
        handle.cancel()
        m._fire('link_restored')
        m._fire('link_lost')
        assert seen == ['lost']

    def test_async_callback_needs_a_running_loop(self):
        import pytest

        m = _FakeManaged()

        async def cb(s):
            pass

        with pytest.raises(RuntimeError, match='on_link_lost: async callback requires a running event loop'):
            m.on_link_lost(cb)
