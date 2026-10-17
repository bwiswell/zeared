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
        assert t1 is t2  # cached

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
            x: int = z.Int(default=0)

        # No raise — the {tail**} slot is capture-only.
        M._templates()

    def test_str_multi_slot_passes(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'log/{tail**}'
            tail: str = z.Str(required=True)

        M._templates()

    def test_non_str_multi_slot_rejected(self):
        with pytest.raises(z.TopicError, match=r'must bind to z\.Str'):

            @z.zeared
            class M(z.Message):
                TOPIC = 'log/{tail**}'
                tail: int = z.Int(required=True)

            M._templates()


class TestSingleSegmentFieldValidation:
    """0.4.0: a single-segment slot's field must render without ``/``."""

    def test_str_int_and_hex_bytes_pass(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'dev/{name}/{n}/{blob}'
            name: str = z.Str(required=True)
            n: int = z.Int(required=True)
            blob: bytes = z.Bytes(required=True, encoding='hex')

        M._templates()

    def test_capture_only_slot_passes(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'dev/{anything}'
            x: int = z.Int(default=0)

        M._templates()

    def test_base64_bytes_rejected(self):
        with pytest.raises(z.TopicError, match=r"single-segment slot 'blob' is bound to Bytes"):

            @z.zeared
            class M(z.Message):
                TOPIC = 'dev/{blob}'
                blob: bytes = z.Bytes(required=True)

            M._templates()

    def test_path_rejected(self):
        with pytest.raises(z.TopicError, match=r"single-segment slot 'where' is bound to Path"):

            @z.zeared
            class M(z.Message):
                TOPIC = 'dev/{where}'
                where: object = z.Path(required=True)

            M._templates()

    def test_extra_topic_single_slot_is_checked_too(self):
        with pytest.raises(z.TopicError, match='single-segment slot'):

            @z.zeared
            class M(z.Message):
                TOPIC = 'dev/{id}'
                EXTRA_TOPICS = ('alt/{id}/{blob}',)
                id: str = z.Str(required=True)
                blob: bytes = z.Bytes(required=True)

            M._templates()


class TestFieldSpecs:
    """Pin (0.3.8): ``_field_specs`` maps attribute name → ``FieldSpec``,
    built once per class (a subclass gets its own)."""

    def test_maps_attr_to_spec_with_wire_key(self):
        @z.zeared
        class Dev(z.Message):
            TOPIC = 'fs/dev/{dev}'
            dev: str = z.Str(required=True, data_key='device')

        spec = Dev._field_specs()['dev']
        assert (spec.attr, spec.wire) == ('dev', 'device')

    def test_cached_per_class(self):
        @z.zeared
        class Base(z.Message):
            TOPIC = 'fs/base/{n}'
            n: int = z.Int(required=True)

        @z.zeared
        class Child(Base):
            TOPIC = 'fs/child/{n}'
            extra: str = z.Str(default='')

        assert Base._field_specs() is Base._field_specs()
        assert set(Base._field_specs()) == {'n'}
        assert set(Child._field_specs()) == {'n', 'extra'}
