"""Tests for ``zeared/message/_message_render.py`` — the canonical topic render.

One renderer turns slot values into the key segments every path uses —
send, will registration, query replies, unretain, query selectors, and the
public ``concrete_topic`` — so a key a publisher renders is the key a
subscriber's capture decodes back.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import uuid
from typing import Any

import pytest
from conftest import wait

import zeared as z
from zeared import _codec as codec
from zeared.queryable._query_get import _build_selector


class _Str(enum.StrEnum):
    NORTH = 'n'


class _Int(enum.IntEnum):
    TWO = 2


class _Mixin(str, enum.Enum):  # noqa: UP042  (the mixin is the case under test)
    NORTH = 'n'


class _Plain(enum.Enum):
    NORTH = 'n'


# (id, field factory, value, expected key segment)
_KINDS: list[tuple[str, Any, Any, str]] = [
    ('Str', lambda: z.Str(required=True), 'a-b', 'a-b'),
    ('Int', lambda: z.Int(required=True), -7, '-7'),
    ('Float', lambda: z.Float(required=True), 1.5, '1.5'),
    ('Bool', lambda: z.Bool(required=True), True, 'True'),
    ('Decimal', lambda: z.Decimal(required=True), decimal.Decimal('1.25'), '1.25'),
    ('UUID', lambda: z.UUID(required=True), uuid.UUID(int=7), '00000000-0000-0000-0000-000000000007'),
    ('Date', lambda: z.Date(required=True), datetime.date(2026, 10, 15), '2026-10-15'),
    (
        'DateTime',
        lambda: z.DateTime(required=True),
        datetime.datetime(2026, 10, 15, 12, 30, tzinfo=datetime.UTC),
        '2026-10-15T12:30:00+00:00',
    ),
    ('Time', lambda: z.Time(required=True), datetime.time(12, 30), '12:30:00'),
    ('TimeDelta', lambda: z.TimeDelta(required=True), datetime.timedelta(seconds=5), '5.0'),
    ('StrEnum', lambda: z.Enum(enum=_Str, required=True), _Str.NORTH, 'n'),
    ('IntEnum', lambda: z.Enum(enum=_Int, required=True), _Int.TWO, '2'),
    ('MixinEnum', lambda: z.Enum(enum=_Mixin, required=True), _Mixin.NORTH, 'n'),
    ('PlainEnum', lambda: z.Enum(enum=_Plain, required=True), _Plain.NORTH, 'n'),
]


def _make(name: str, field: Any, value: Any, *, retained: bool = False) -> type[z.Message]:
    attrs = {
        '__annotations__': {'k': type(value), 'n': int},
        'TOPIC': f'render/kind/{name}/{{k}}',
        'RETAINED': retained,
        'k': field,
        'n': z.Int(default=0),
    }
    return z.zeared(type(f'K_{name}', (z.Message,), attrs))


class TestEveryPathAgrees:
    """Pin: for every scalar slot kind, every key-building path renders the
    same key, and that key decodes back to the value that was sent."""

    @pytest.mark.parametrize(('name', 'make_field', 'value', 'segment'), _KINDS, ids=[k[0] for k in _KINDS])
    def test_render_paths_agree_and_round_trip(self, session, name, make_field, value, segment):
        cls = _make(name, make_field(), value, retained=True)
        expected = f'render/kind/{name}/{segment}'
        msg = cls(k=value, n=1)

        # The public render, both forms, and the query selector.
        assert msg.concrete_topic() == expected
        assert cls.concrete_topic(k=value) == expected
        assert _build_selector(cls, {'k': value}, None) == expected

        received: list[tuple[Any, str]] = []
        removed: list[str] = []
        cls.on_message(
            lambda m, meta: received.append((m.k, meta.key_expr)),
            on_remove=lambda meta: removed.append(meta.key_expr),
            retained_fetch=False,
            session=session,
        )
        wait()

        # send publishes on the rendered key, and it decodes back.
        msg.send(session=session)
        wait(0.2)
        assert cls.published_topics(session=session) == frozenset({expected})
        assert received == [(value, expected)]

        # unretain, instance and class form, tombstone that same key.
        msg.unretain(session=session)
        cls.unretain(session=session, k=value)
        wait(0.2)
        assert removed == [expected, expected]


class TestValueRule:
    """Pin the rule for caller-supplied slot values: values of the field's
    exact type are serialized; everything else keeps ``str()``."""

    @pytest.fixture
    def counted(self):
        @z.zeared
        class Counted(z.Message):
            TOPIC = 'render/rule/{n}'
            n: int = z.Int(required=True)

        return Counted

    def test_selector_wildcard_string_passes_through(self, counted):
        assert _build_selector(counted, {'n': '4*'}, None) == 'render/rule/4*'

    def test_omitted_slot_widens_in_selector(self, counted):
        assert _build_selector(counted, {}, None) == 'render/rule/*'

    def test_pre_rendered_string_passes_through(self, counted):
        assert counted.concrete_topic(n='7') == 'render/rule/7'

    def test_bool_on_int_slot_keeps_str(self, counted):
        # ``bool`` is a subclass of ``int``; the exact type check keeps it
        # on today's ``str()`` rather than handing it to ``Int.serialize``.
        assert counted.concrete_topic(n=True) == 'render/rule/True'

    def test_unrecognised_value_keeps_str(self, counted):
        assert counted.concrete_topic(n=7.0) == 'render/rule/7.0'


class TestKeyIsTheStringForm:
    """Pin: a key is the field's string (JSON-carrier) form, whatever the
    class ``ENCODING`` — the only form a capture can decode."""

    @pytest.mark.parametrize('encoding', ['json', 'msgpack'])
    def test_bytes_key_is_base64_under_both_encodings(self, encoding):
        @z.zeared
        class Blob(z.Message):
            TOPIC = 'render/bytes/{k}'
            ENCODING = encoding
            k: bytes = z.Bytes(required=True)

        # Before 0.3.8, msgpack rendered the Python repr (``b'\\xfb\\xff\\x00'``).
        # base64 is still not a usable single segment (it contains ``/``);
        # see TestBytesSlotsAreBroken.
        assert Blob(k=b'\xfb\xff\x00').concrete_topic() == 'render/bytes/+/8A'


class TestBytesSlotsAreBroken:
    """Pin: a ``Bytes`` topic slot cannot round-trip, under either encoding.

    Under msgpack the key segment was the Python repr of the bytes; under
    JSON (and now under both) it's base64, whose alphabet includes ``/``, so
    the segment splits and the key matches no template. Rejecting ``Bytes``
    (and ``Path``) slots at class definition is planned for 0.4.0; until then
    this documents the breakage and flips loudly when it's resolved.
    """

    @pytest.mark.xfail(
        strict=True,
        reason='Bytes slots cannot round-trip: base64 contains "/" (rejection planned for 0.4.0)',
    )
    @pytest.mark.parametrize('encoding', ['json', 'msgpack'])
    def test_bytes_slot_round_trip(self, session, encoding):
        @z.zeared
        class Blob(z.Message):
            TOPIC = 'render/bytes/{k}'
            ENCODING = encoding
            k: bytes = z.Bytes(required=True)
            n: int = z.Int(default=0)

        got: list[bytes] = []
        Blob.on_message(lambda m: got.append(m.k), session=session)
        wait()
        Blob(k=b'\xfb\xff\x00', n=1).send(session=session)
        wait(0.3)
        assert got == [b'\xfb\xff\x00']


class TestDataKeySlots:
    """Pin: a slot field with a ``data_key`` renders from its attribute and
    decodes back under its wire key, on every path."""

    @pytest.fixture
    def device(self):
        @z.zeared
        class Device(z.Message):
            TOPIC = 'render/dk/{dev}'
            RETAINED = True
            dev: str = z.Str(required=True, data_key='device')
            level: int = z.Int(default=0)

        return Device

    def test_render(self, device):
        msg = device(dev='a', level=3)
        assert msg.concrete_topic() == 'render/dk/a'
        assert device.concrete_topic(dev='a') == 'render/dk/a'

    def test_send_decode_and_payload_strip(self, session, device):
        payloads: list[dict] = []
        raw_sub = session.declare_subscriber(
            'render/dk/*',
            lambda s: payloads.append(codec.unpack(bytes(s.payload), 'msgpack')),
        )
        received: list[tuple[str, int, dict]] = []
        device.on_message(
            lambda m, meta: received.append((m.dev, m.level, device.coerce_captures(meta.captures))),
            session=session,
        )
        wait()
        device(dev='a', level=3).send(session=session)
        wait(0.2)
        raw_sub.undeclare()
        assert received == [('a', 3, {'dev': 'a'})]
        # The slot lives in the key, so its wire key is stripped from the body.
        assert payloads == [{'level': 3}]

    def test_fetch_retained(self, session, device):
        device(dev='a', level=3).send(session=session)
        wait(0.2)
        got = device.fetch_retained(session=session)
        assert [(m.dev, m.level) for m in got] == [('a', 3)]

    def test_capture_wins_over_payload(self, device):
        # A foreign publisher that also puts the wire key in the body: the
        # key is authoritative for identity.
        raw = codec.pack({'device': 'payload-says', 'level': 1}, 'msgpack')
        msg, captures = device._decode(raw, 'render/dk/key-says', 'msgpack')
        assert msg.dev == 'key-says'
        assert captures == {'dev': 'key-says'}

    def test_register_will(self, session):
        @z.zeared
        class Alive(z.Message):
            TOPIC = 'render/dk/alive/{dev}'
            LIVELINESS = True
            dev: str = z.Str(required=True, data_key='device')
            state: str = z.Str(default='up')

        # Before 0.3.8 this raised ``TopicError: missing field 'dev'``.
        Alive(dev='a', state='down').register_will(session=session)


class TestQueryReplyAndSelector:
    """Pin: the selector a typed slot value builds and the key a queryable's
    reply renders on agree, so the reply reaches the getter."""

    def test_enum_slot_query_round_trip(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'render/q/{zone}'
            zone: _Mixin = z.Enum(enum=_Mixin, required=True)
            level: int = z.Int(default=0)

        Status.on_query(lambda ctx: ctx.reply(Status(zone=_Mixin.NORTH, level=4)), session=session)
        wait()
        got = Status.query(session=session, timeout=1.0, zone=_Mixin.NORTH)
        assert [(m.zone, m.level) for m in got] == [(_Mixin.NORTH, 4)]


class TestTemplateMethodsUnchanged:
    """Pin: ``Template.render`` stays a plain ``str.format`` renderer.

    Consumers call it directly (through ``_templates()``) to compute
    identity keys; the canonical render sits above it rather than changing
    it.
    """

    def test_template_render_formats_raw_values(self):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'render/tpl/{zone}'
            zone: _Mixin = z.Enum(enum=_Mixin, required=True)

        template = Status._templates().canonical
        assert template.render({'zone': 'n'}) == 'render/tpl/n'
        assert template.render({'zone': None}) == 'render/tpl/None'
