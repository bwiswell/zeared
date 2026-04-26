"""Smoke tests for ``zeared/message/_message_async.py`` — the
``_MessageAsyncMixin`` (``asend`` / ``asend_batch`` / ``aunretain`` /
``alisten``).

Detailed async coverage lives in ``test_async_.py``. This file confirms
the mixin surface.
"""
from __future__ import annotations

from zeared.message._message_async import _MessageAsyncMixin


class TestMixinSurface:
    def test_class_importable(self):
        assert _MessageAsyncMixin is not None

    def test_no_instance_state(self):
        assert _MessageAsyncMixin.__slots__ == ()

    def test_methods_present(self):
        assert hasattr(_MessageAsyncMixin, 'asend')
        assert hasattr(_MessageAsyncMixin, 'asend_batch')
        assert hasattr(_MessageAsyncMixin, 'aunretain')
        assert hasattr(_MessageAsyncMixin, 'alisten')
