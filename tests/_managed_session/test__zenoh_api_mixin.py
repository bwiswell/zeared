"""Smoke tests for ``zeared/_managed_session/_zenoh_api_mixin.py`` —
the ``_ZenohApiMixin`` (pass-through delegators for the
``zenoh.Session`` surface).

End-to-end behaviour through ``ManagedSession`` lives in
``test_session.py`` / ``test_reconnect.py``. This file confirms the
mixin's public surface.
"""
from __future__ import annotations

from zeared._managed_session._zenoh_api_mixin import _ZenohApiMixin


class TestMixinSurface:
    def test_class_importable(self):
        assert _ZenohApiMixin is not None

    def test_no_instance_state(self):
        assert _ZenohApiMixin.__slots__ == ()

    def test_pass_through_methods_present(self):
        for name in ('zid', 'liveliness', 'info', 'put', 'get', 'delete'):
            assert hasattr(_ZenohApiMixin, name), f'missing {name}'

    def test_declare_methods_present(self):
        for name in ('declare_publisher', 'declare_subscriber',
                     'declare_queryable'):
            assert hasattr(_ZenohApiMixin, name), f'missing {name}'
