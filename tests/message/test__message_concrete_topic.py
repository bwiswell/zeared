"""Tests for ``zeared/message/_message_concrete_topic.py`` — the
``_ConcreteTopicDescriptor`` instance-vs-class dispatch.

Rendering itself (every slot kind, every path agreeing) is covered in
``test__message_render.py``. This file pins the public surface: what each
form accepts, and when it refuses.
"""

from __future__ import annotations

import pytest

import zeared as z
from zeared.message._message_concrete_topic import _ConcreteTopicDescriptor


@z.zeared
class Reading(z.Message):
    TOPIC = 'ct/read/{source}/{reader_id}'
    EXTRA_TOPICS = ('ct/legacy/{reader_id}', 'ct/any/**')
    source: str = z.Str(required=True)
    reader_id: int = z.Int(required=True)
    rssi: float = z.Float(default=0.0)
    note: str | None = z.Str(default=None)


class TestPublicSurface:
    def test_descriptor_on_message(self):
        assert isinstance(z.Message.__dict__['concrete_topic'], _ConcreteTopicDescriptor)

    def test_qualnames(self):
        assert Reading.concrete_topic.__qualname__ == 'Reading.concrete_topic'
        assert Reading(source='a', reader_id=1).concrete_topic.__qualname__ == 'Reading.concrete_topic'


class TestInstanceForm:
    def test_canonical(self):
        assert Reading(source='rtls', reader_id=7, rssi=-50.0).concrete_topic() == 'ct/read/rtls/7'

    def test_declared_topic_override(self):
        msg = Reading(source='rtls', reader_id=7)
        assert msg.concrete_topic(topic='ct/legacy/{reader_id}') == 'ct/legacy/7'

    def test_matches_send(self, session):
        msg = Reading(source='rtls', reader_id=7)
        msg.send(session=session)
        assert Reading.published_topics(session=session) == frozenset({msg.concrete_topic()})

    def test_undeclared_topic_raises(self):
        with pytest.raises(z.TopicError, match='not a declared topic'):
            Reading(source='rtls', reader_id=7).concrete_topic(topic='ct/other/{reader_id}')

    def test_subscribe_only_topic_raises(self):
        with pytest.raises(z.TopicError, match='subscribe-only'):
            Reading(source='rtls', reader_id=7).concrete_topic(topic='ct/any/**')

    def test_none_slot_raises_like_send(self):
        @z.zeared
        class Optional(z.Message):
            TOPIC = 'ct/opt/{tag}'
            tag: str | None = z.Str(default=None)

        with pytest.raises(z.TopicError, match="missing field 'tag'"):
            Optional().concrete_topic()


class TestClassForm:
    def test_from_slots(self):
        assert Reading.concrete_topic(source='rtls', reader_id=7) == 'ct/read/rtls/7'

    def test_extra_kwargs_ignored(self):
        # A full field dict can be passed as-is: identity comes from the slots.
        values = {'source': 'rtls', 'reader_id': 7, 'rssi': -50.0, 'note': 'x'}
        assert Reading.concrete_topic(**values) == 'ct/read/rtls/7'

    def test_declared_topic_override(self):
        assert Reading.concrete_topic(topic='ct/legacy/{reader_id}', reader_id=7) == 'ct/legacy/7'

    def test_missing_slot_raises(self):
        with pytest.raises(z.TopicError, match=r"Reading\.concrete_topic: missing slot 'reader_id'"):
            Reading.concrete_topic(source='rtls')

    def test_none_slot_raises(self):
        # Unlike a selector, which widens a missing slot to a wildcard.
        with pytest.raises(z.TopicError, match="missing slot 'source'"):
            Reading.concrete_topic(source=None, reader_id=7)

    def test_agrees_with_instance_form(self):
        msg = Reading(source='rtls', reader_id=7)
        assert Reading.concrete_topic(source='rtls', reader_id=7) == msg.concrete_topic()
