"""Smoke tests for ``zeared/message/_message_will.py`` — the
``_MessageWillMixin`` (``register_will`` / ``aregister_will``).

End-to-end LWT coverage lives in ``test_presence.py``. This file
confirms the mixin surface and the LIVELINESS guard.
"""
from __future__ import annotations

import pytest

import zeared as z
from zeared.message._message_will import _MessageWillMixin


class TestMixinSurface:
    def test_class_importable(self):
        assert _MessageWillMixin is not None

    def test_no_instance_state(self):
        assert _MessageWillMixin.__slots__ == ()

    def test_methods_present(self):
        assert hasattr(_MessageWillMixin, 'register_will')
        assert hasattr(_MessageWillMixin, 'aregister_will')


class TestLivelinessGuard:
    """Pin: ``register_will`` rejects classes without ``LIVELINESS = True``."""

    def test_rejects_non_liveliness_class(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'will/guard/{n}'
            # LIVELINESS not set → defaults to False.
            n: int = z.Int(required=True)
            state: str = z.Str(required=True)

        z.session = session
        msg = M(n=1, state='offline')
        with pytest.raises(z.TopicError, match='LIVELINESS = True'):
            msg.register_will()
