"""Tests for ``zeared/message/_message_render.py`` — the canonical topic render.

One renderer turns slot values into the key segments every path uses —
send, declare_presence, query replies, unretain, query selectors, and the
public ``concrete_topic`` — so a key a publisher renders is the key a
subscriber's capture decodes back.
"""

from __future__ import annotations

import datetime as dt
import decimal
import enum
import re
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
    ('Date', lambda: z.Date(required=True), dt.date(2026, 10, 15), '2026-10-15'),
    (
        'DateTime',
        lambda: z.DateTime(required=True),
        dt.datetime(2026, 10, 15, 12, 30, tzinfo=dt.UTC),
        '2026-10-15T12:30:00+00:00',
    ),
    ('Time', lambda: z.Time(required=True), dt.time(12, 30), '12:30:00'),
    ('TimeDelta', lambda: z.TimeDelta(required=True), dt.timedelta(seconds=5), '5.0'),
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
    def test_hex_bytes_key_is_its_hex_form_under_both_encodings(self, encoding):
        @z.zeared
        class Blob(z.Message):
            TOPIC = 'render/bytes/{k}'
            ENCODING = encoding
            k: bytes = z.Bytes(required=True, encoding='hex')

        # Before 0.3.8, msgpack rendered the Python repr (``b'\\xfb\\xff\\x00'``).
        assert Blob(k=b'\xfb\xff\x00').concrete_topic() == 'render/bytes/fbff00'


class TestSegmentUnsafeSlotsAreRejected:
    """0.4.0: a single-segment slot bound to a kind whose string form can hold
    ``/`` raises ``TopicError`` at first use. Flipped from 0.3.8's strict xfails
    (``TestBytesSlotsAreBroken``), which proved a base64 ``Bytes`` slot can't
    round-trip: ``b'\\xfb\\xff\\x00'`` renders ``+/8A`` and splits the key."""

    @pytest.mark.parametrize(
        ('make_field', 'hint'),
        [
            (lambda: z.Bytes(required=True), "z.Bytes(encoding='hex')"),
            (lambda: z.Path(required=True), 'z.Str'),
        ],
        ids=['bytes-base64', 'path'],
    )
    def test_rejected_at_first_use(self, make_field, hint):
        class Bad(z.Message):
            TOPIC = 'render/unsafe/{k}'

        Bad.__annotations__ = {'k': Any}
        Bad.k = make_field()
        Bad = z.zeared(Bad)

        with pytest.raises(z.TopicError, match=r"single-segment slot 'k' is bound to") as info:
            Bad._templates()
        assert hint in str(info.value)

    @pytest.mark.parametrize('encoding', ['json', 'msgpack'])
    def test_hex_bytes_slot_round_trips(self, session, encoding):
        @z.zeared
        class Blob(z.Message):
            TOPIC = 'render/hexbytes/{k}'
            ENCODING = encoding
            k: bytes = z.Bytes(required=True, encoding='hex')
            n: int = z.Int(default=0)

        got: list[bytes] = []
        Blob.on_message(lambda m: got.append(m.k), session=session)
        wait()
        Blob(k=b'\xfb\xff\x00', n=1).send(session=session)
        wait(0.3)
        assert got == [b'\xfb\xff\x00']


class TestSlashInASingleSegmentValue:
    """0.4.0: publish renders refuse a ``/`` in a single-segment slot value,
    which would land the message on a key no template matches. Selector
    renders don't check: they embed wildcards deliberately."""

    @pytest.fixture
    def named(self):
        @z.zeared
        class Named(z.Message):
            TOPIC = 'render/slash/{name}'
            RETAINED = True
            name: str = z.Str(required=True)
            v: int = z.Int(default=0)

        return Named

    def test_send_raises(self, session, named):
        with pytest.raises(z.TopicError, match='contains "/"'):
            named(name='a/b').send(session=session)

    def test_concrete_topic_raises_both_ways(self, named):
        with pytest.raises(z.TopicError, match='contains "/"'):
            named(name='a/b').concrete_topic()
        with pytest.raises(z.TopicError, match='contains "/"'):
            named.concrete_topic(name='a/b')

    def test_unretain_raises(self, session, named):
        with pytest.raises(z.TopicError, match='contains "/"'):
            named.unretain(name='a/b', session=session)

    def test_selector_does_not_check(self, named):
        assert _build_selector(named, {'name': 'a/b'}, None) == 'render/slash/a/b'

    def test_multi_segment_slot_accepts_slashes(self):
        @z.zeared
        class Log(z.Message):
            TOPIC = 'render/log/{tail**}'
            tail: str = z.Str(required=True)

        assert Log(tail='a/b/c').concrete_topic() == 'render/log/a/b/c'

    def test_query_reply_raises(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'render/slashq/{id}'
            id: str = z.Str(required=True)

        errors: list[Exception] = []

        def handler(ctx):
            try:
                ctx.reply(Row(id='a/b'))
            except z.TopicError as exc:
                errors.append(exc)

        z.session = session
        qbl = Row.on_query(handler)
        wait()
        assert Row.query(id='x', timeout=1.0) == []
        qbl.close()
        assert len(errors) == 1
        assert 'contains "/"' in str(errors[0])


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

    def test_declare_presence(self, session):
        @z.zeared
        class Alive(z.Message):
            TOPIC = 'render/dk/alive/{dev}'
            LIVELINESS = True
            dev: str = z.Str(required=True, data_key='device')
            state: str = z.Str(default='up')

        # The 0.3.8 render (attribute, not wire key) carries over to presence.
        assert Alive(dev='a').declare_presence(session=session).key == 'render/dk/alive/a'
        assert Alive.declare_presence(dev='b', session=session).key == 'render/dk/alive/b'


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


class TestSlotConstraints:
    """A slot field's constraints (seared 0.4.4): checked on receive and by ``dump``; the render never checks."""

    @pytest.fixture
    def robot(self):
        @z.zeared
        class Robot(z.Message):
            TOPIC = 'render/constrained/{robot}/{bay}'
            robot: str = z.Str(pattern=r'^r-[0-9]+$', required=True)
            bay: int = z.Int(ge=1, le=12, required=True)
            speed: float = z.Float(ge=0, default=0.0)

        return Robot

    def test_a_selector_wildcard_renders_on_a_pattern_slot(self, robot):
        # A string passes the render verbatim, so a wildcard isn't held to the pattern.
        assert _build_selector(robot, {'robot': 'r-$*'}, None) == 'render/constrained/r-$*/*'

    def test_the_render_never_checks(self, robot):
        # ``serialize`` refuses 13; the render falls back to ``str()``, as for any value it refuses.
        assert robot.concrete_topic(robot='r-1', bay=13) == 'render/constrained/r-1/13'

    @pytest.mark.parametrize(
        ('key', 'reason'),
        [
            ('render/constrained/R-1/3', "must match the pattern '^r-[0-9]+$'"),
            ('render/constrained/r-1/13', 'must be <= 12, got 13'),
        ],
    )
    def test_a_capture_that_fails_its_constraint_fails_the_decode(self, robot, key, reason):
        with pytest.raises(z.ValidationError, match=f'^{re.escape(reason)}$'):
            robot._decode(codec.pack({}, 'msgpack'), key, 'msgpack')

    def test_coerce_captures_checks_them_too(self, robot):
        assert robot.coerce_captures({'robot': 'r-1', 'bay': '12'}) == {'robot': 'r-1', 'bay': 12}
        with pytest.raises(z.ValidationError, match=r'^must be <= 12, got 13$'):
            robot.coerce_captures({'robot': 'r-1', 'bay': '13'})

    def test_send_refuses_an_out_of_range_slot_before_publishing(self, session, robot):
        seen: list[str] = []
        raw_sub = session.declare_subscriber('render/constrained/**', lambda s: seen.append(str(s.key_expr)))
        wait()
        with pytest.raises(z.ValidationError, match=r'^Robot\.bay: must be <= 12, got 13$'):
            robot(robot='r-1', bay=13).send(session=session)
        wait(0.2)
        raw_sub.undeclare()
        assert seen == []

    def test_a_subscriber_routes_a_failed_capture_to_on_error(self, session, robot):
        received: list[Any] = []
        errors: list[Exception] = []
        robot.on_message(received.append, session=session, on_error=lambda exc, raw: errors.append(exc))
        wait()
        # A foreign publisher's key that a zeared publisher's dump would have refused.
        session.put('render/constrained/R-1/3', codec.pack({}, 'msgpack'))
        wait(0.2)
        assert received == []
        assert len(errors) == 1
        assert isinstance(errors[0], z.DecodeError)
        assert isinstance(errors[0].__cause__, z.ValidationError)
        assert "must match the pattern '^r-[0-9]+$'" in str(errors[0])
