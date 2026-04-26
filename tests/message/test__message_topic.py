"""Tests for ``zeared/message/_message_topic.py`` — the
``_MessageTopicMixin`` (template parsing, schema attachment cache,
multi-segment field-binding validation).

End-to-end coverage lives in ``test_message.py``; this file targets
the mixin's behaviour directly.
"""
from __future__ import annotations

import pytest

import zeared as z
from zeared.message._message_topic import _MessageTopicMixin


class TestMixinSurface:
    def test_class_importable(self):
        assert _MessageTopicMixin is not None

    def test_no_instance_state(self):
        assert _MessageTopicMixin.__slots__ == ()


class TestTemplateCache:
    def test_templates_cached_per_class(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'cache/{x}'
            x: int = z.Int(required=True)

        t1 = M._templates()
        t2 = M._templates()
        assert t1 is t2   # cached

    def test_missing_topic_raises(self):
        with pytest.raises(z.TopicError, match='TOPIC is not defined'):
            @z.zeared
            class M(z.Message):
                # No TOPIC declared.
                x: int = z.Int(required=True)
            M._templates()


class TestSchemaAttachment:
    def test_no_schema_returns_none(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'sch/none'
            x: int = z.Int(required=True)

        assert M._schema_attachment_bytes() is None

    def test_with_schema_returns_bytes(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'sch/v1'
            SCHEMA = '1.0'
            x: int = z.Int(required=True)

        attachment = M._schema_attachment_bytes()
        assert isinstance(attachment, bytes)
        # Cache hit on second call returns identity.
        assert M._schema_attachment_bytes() is attachment


class TestMultiSegmentFieldValidation:
    def test_undeclared_multi_slot_passes(self):
        # Capture-only multi-slots are fine.
        @z.zeared
        class M(z.Message):
            TOPIC = 'log/{tail**}'
            x: int = z.Int(missing=0)

        # No raise — the {tail**} slot is capture-only.
        M._templates()

    def test_str_multi_slot_passes(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'log/{tail**}'
            tail: str = z.Str(required=True)

        M._templates()

    def test_non_str_multi_slot_rejected(self):
        with pytest.raises(z.TopicError, match='must bind to z.Str'):
            @z.zeared
            class M(z.Message):
                TOPIC = 'log/{tail**}'
                tail: int = z.Int(required=True)
            M._templates()
