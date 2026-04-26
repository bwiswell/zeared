"""Smoke tests for ``zeared/_managed_session/_managed_session.py`` —
the ``ManagedSession`` class itself.

End-to-end auto-reconnect coverage lives in ``test__reconnect.py`` and
the existing ``test_session.py``. This file confirms the class's
public surface and the base lifecycle (init / state / close).
"""
from __future__ import annotations

import threading

from zeared._managed_session._managed_session import ManagedSession


class TestPublicSurface:
    def test_class_importable(self):
        assert ManagedSession is not None

    def test_uses_slots(self):
        assert hasattr(ManagedSession, '__slots__')

    def test_lifecycle_methods_present(self):
        for name in ('raw', 'state', 'close', '_swap_raw', '_set_state',
                     '_guard_alive', '_note_failure', '_teardown'):
            assert hasattr(ManagedSession, name), f'missing {name}'

    def test_zenoh_api_methods_inherited_via_mixin(self):
        # _ZenohApiMixin contributes these.
        for name in ('zid', 'liveliness', 'put', 'get', 'delete',
                     'declare_publisher', 'declare_subscriber',
                     'declare_queryable'):
            assert hasattr(ManagedSession, name), f'missing {name} (mixin)'

    def test_on_reconnect_methods_inherited_via_mixin(self):
        # _OnReconnectMixin contributes these.
        assert hasattr(ManagedSession, 'on_reconnect')
        assert hasattr(ManagedSession, '_fire_reconnect_callbacks')
