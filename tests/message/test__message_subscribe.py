"""Smoke tests for ``zeared/message/_message_subscribe.py`` — the
``_MessageSubscribeMixin`` (``on_message`` + ``published_topics``).

End-to-end coverage lives in ``test_subscriber.py`` and
``test_publisher.py``. This file confirms the mixin surface.
"""
from __future__ import annotations

import zeared as z
from zeared.message._message_subscribe import _MessageSubscribeMixin


class TestMixinSurface:
    def test_class_importable(self):
        assert _MessageSubscribeMixin is not None

    def test_no_instance_state(self):
        assert _MessageSubscribeMixin.__slots__ == ()

    def test_methods_present(self):
        assert hasattr(_MessageSubscribeMixin, 'on_message')
        assert hasattr(_MessageSubscribeMixin, 'published_topics')


class TestPublishedTopics:
    def test_empty_for_unpublished_class(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'pub/topics/{id}'
            id: int = z.Int(required=True)

        # Never published anything on this class — empty set.
        assert M.published_topics(session=session) == frozenset()

    def test_returns_frozenset(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'pub/topics2/{id}'
            id: int = z.Int(required=True)

        z.session = session
        M(id=42).send()
        result = M.published_topics(session=session)
        assert isinstance(result, frozenset)
        assert 'pub/topics2/42' in result
