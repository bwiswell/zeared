from __future__ import annotations

import pytest

import zeared as z
from zeared import _codec as codec


class TestTopicValidation:
    def test_missing_topic_raises(self):
        @z.zeared
        class NoTopic(z.Message):
            x: int = z.Int(required=True)

        with pytest.raises(z.TopicError, match='TOPIC is not defined'):
            NoTopic._templates()

    def test_template_slot_not_required_to_be_declared_field(self):
        """Undeclared template slots are allowed; they become capture-only."""
        @z.zeared
        class CaptureOnly(z.Message):
            TOPIC = 'robot/{id}/telemetry'      # id is NOT a declared field
            x: int = z.Int(required=True)

        # Doesn't raise — id is valid as a capture-only slot.
        tpls = CaptureOnly._templates()
        assert 'id' in tpls.field_names

    def test_undeclared_slot_raises_at_publish_time(self, session):
        @z.zeared
        class CaptureOnly(z.Message):
            TOPIC = 'robot/{id}/telemetry'
            x: int = z.Int(required=True)

        z.session = session
        with pytest.raises(z.TopicError, match='missing field'):
            CaptureOnly(x=1).send()   # render needs id, isn't in data

    def test_valid_template_caches(self):
        @z.zeared
        class Good(z.Message):
            TOPIC = 'robot/{id}/telemetry'
            id: int = z.Int(required=True)
            x: float = z.Float(required=True)

        t1 = Good._templates()
        t2 = Good._templates()
        assert t1 is t2

    def test_static_topic_works(self):
        @z.zeared
        class Static(z.Message):
            TOPIC = 'events/alerts'
            msg: str = z.Str(required=True)

        tpls = Static._templates()
        assert tpls.field_names == frozenset()
        assert tpls.canonical.raw == 'events/alerts'
        assert tpls.extras == ()


class TestDecode:
    def test_decode_static_topic(self):
        @z.zeared
        class Static(z.Message):
            TOPIC = 'events/alerts'
            msg: str = z.Str(required=True)

        raw = codec.pack({'msg': 'fire'}, 'msgpack')
        obj, captures = Static._decode(raw, 'events/alerts', 'msgpack')
        assert obj.msg == 'fire'
        assert captures == {}

    def test_decode_templated_topic_populates_fields(self):
        @z.zeared
        class Telemetry(z.Message):
            TOPIC = 'robot/{id}/telemetry'
            id: int = z.Int(required=True)
            x: float = z.Float(required=True)

        raw = codec.pack({'x': 1.5}, 'msgpack')
        obj, captures = Telemetry._decode(raw, 'robot/42/telemetry', 'msgpack')
        assert obj.id == 42
        assert obj.x == 1.5
        assert captures == {'id': '42'}

    def test_decode_key_expr_mismatch_raises(self):
        @z.zeared
        class Telemetry(z.Message):
            TOPIC = 'robot/{id}/telemetry'
            id: int = z.Int(required=True)

        raw = codec.pack({}, 'msgpack')
        with pytest.raises(ValueError, match='does not match'):
            Telemetry._decode(raw, 'unrelated/topic', 'msgpack')

    def test_decode_json_encoding(self):
        @z.zeared
        class Telemetry(z.Message):
            TOPIC = 'robot/{id}/telemetry'
            ENCODING = 'json'
            id: int = z.Int(required=True)
            x: float = z.Float(required=True)

        raw = codec.pack({'x': 2.5}, 'json')
        obj, _ = Telemetry._decode(raw, 'robot/7/telemetry', 'json')
        assert obj.id == 7
        assert obj.x == 2.5


class TestExtraTopics:
    def test_templates_contain_canonical_plus_extras(self):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        tpls = Status._templates()
        assert tpls.canonical.raw == 'robot/{id}/status'
        assert len(tpls.extras) == 1
        assert tpls.extras[0].raw == 'vehicle/{id}/status'
        assert tpls.field_names == frozenset({'id'})

    def test_slots_need_not_match_across_templates(self):
        """0.0.5: each template has an independent slot set."""
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{vid}/status',)
            id: int = z.Int(required=True)
            vid: int = z.Int()
            status: str = z.Str(required=True)

        tpls = Status._templates()
        assert tpls.field_names == frozenset({'id', 'vid'})

    def test_resolve_publish_topic_canonical_default(self):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        assert Status._templates().resolve_publish_topic(None).raw == 'robot/{id}/status'

    def test_resolve_publish_topic_declared_extra(self):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        picked = Status._templates().resolve_publish_topic('vehicle/{id}/status')
        assert picked.raw == 'vehicle/{id}/status'

    def test_resolve_publish_topic_rejects_arbitrary(self):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        with pytest.raises(z.TopicError, match='not a declared topic'):
            Status._templates().resolve_publish_topic('arbitrary/{id}/topic')

    def test_templates_match_key_expr_by_first_matching(self):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        tpls = Status._templates()
        m1 = tpls.match('robot/7/status')
        assert m1 is not None
        assert m1[0].raw == 'robot/{id}/status'
        assert m1[1] == {'id': '7'}

        m2 = tpls.match('vehicle/42/status')
        assert m2 is not None
        assert m2[0].raw == 'vehicle/{id}/status'
        assert m2[1] == {'id': '42'}

        assert tpls.match('unrelated/1/topic') is None


class TestSendWithoutSession:
    def test_send_raises_when_no_session(self):
        @z.zeared
        class Telemetry(z.Message):
            TOPIC = 'robot/{id}/telemetry'
            id: int = z.Int(required=True)
            x: float = z.Float(required=True)

        z.session._set_default(None)
        with pytest.raises(z.NoSessionError):
            Telemetry(id=1, x=2.0).send()


class TestSearedReExports:
    """The single-import style: ``import zeared as z`` exposes every seared name."""

    def test_decorator_renamed_to_zeared(self):
        import seared
        # The decorator is re-exported under a zeared-flavoured name.
        assert z.zeared is seared.seared
        # And not leaked under its original name.
        assert not hasattr(z, 'seared')

    def test_base_class_renamed_to_zeared(self):
        import seared
        assert z.Zeared is seared.Seared
        assert not hasattr(z, 'Seared')

    def test_field_types_reachable(self):
        import seared
        for name in (
            'Bool', 'Bytes', 'Date', 'DateTime', 'Dict', 'Enum', 'Float',
            'Int', 'NDArray', 'Str', 'T', 'Time', 'TimeDelta', 'Tuple', 'UUID',
        ):
            assert getattr(z, name) is getattr(seared, name), name

    def test_error_types_reachable(self):
        import seared
        assert z.SearedError is seared.SearedError
        assert z.ValidationError is seared.ValidationError

    def test_two_import_style_still_works(self):
        """Confirm classical dual-import pattern is unaffected by re-export."""
        import seared as s

        @s.seared
        class M(z.Message):
            TOPIC = 't/{id}'
            id: int = s.Int(required=True)
            x: float = s.Float(required=True)

        obj = M.load({'id': 1, 'x': 2.5})
        assert obj.id == 1 and obj.x == 2.5


# ---------------------------------------------------------------------------
# Tagged-union end-to-end on the wire (folded from the previous
# tests/test_union_integration.py — z.Union dispatch through publish +
# subscribe).
# ---------------------------------------------------------------------------


from conftest import wait


@z.zeared
class _UnionStartAction(z.Zeared):
    speed: float = z.Float(required=True)


@z.zeared
class _UnionStopAction(z.Zeared):
    pass


@z.zeared
class _UnionConfigureAction(z.Zeared):
    mode: str = z.Str(required=True)


@z.zeared
class _UnionControl(z.Message):
    TOPIC = 'union/control'
    action: object = z.Union(
        variants={
            'start': _UnionStartAction,
            'stop': _UnionStopAction,
            'configure': _UnionConfigureAction,
        },
        tag_key='action',
        payload_key='args',
        required=True,
    )


class TestTaggedDispatch:
    def test_wire_shape_is_envelope(self, session):
        z.session = session
        ctrl = _UnionControl(action=_UnionStartAction(speed=10.0))
        assert _UnionControl.dump(ctrl) == {
            'action': 'start', 'args': {'speed': 10.0},
        }

    def test_round_trip_all_variants(self, session):
        received: list = []
        z.session = session
        sub = _UnionControl.on_message(lambda m: received.append(m.action))
        wait()

        _UnionControl(action=_UnionStartAction(speed=2.0)).send()
        _UnionControl(action=_UnionStopAction()).send()
        _UnionControl(action=_UnionConfigureAction(mode='auto')).send()
        wait()
        sub.close()

        assert len(received) == 3
        kinds = {type(a).__name__ for a in received}
        assert kinds == {
            '_UnionStartAction', '_UnionStopAction', '_UnionConfigureAction',
        }

    def test_match_dispatch(self, session):
        speeds: list[float] = []
        stops = 0

        def handler(msg: _UnionControl) -> None:
            nonlocal stops
            match msg.action:
                case _UnionStartAction(speed=s):
                    speeds.append(s)
                case _UnionStopAction():
                    stops += 1
                case _UnionConfigureAction():
                    pass

        z.session = session
        sub = _UnionControl.on_message(handler)
        wait()
        _UnionControl(action=_UnionStartAction(speed=1.5)).send()
        _UnionControl(action=_UnionStartAction(speed=3.0)).send()
        _UnionControl(action=_UnionStopAction()).send()
        wait()
        sub.close()

        assert speeds == [1.5, 3.0]
        assert stops == 1


class TestUnionWithFormatTopic:
    """Union field on a class with templated TOPIC."""

    def test_templated_control_with_union(self, session):
        @z.zeared
        class PeerControl(z.Message):
            TOPIC = 'union/peer/{name}/control'
            name: str = z.Str(required=True)
            action: object = z.Union(
                variants={
                    'start': _UnionStartAction,
                    'stop': _UnionStopAction,
                },
                tag_key='action', payload_key='args', required=True,
            )

        received: list[tuple[str, type]] = []
        z.session = session
        sub = PeerControl.on_message(
            lambda m: received.append((m.name, type(m.action)))
        )
        wait()
        PeerControl(name='alice', action=_UnionStartAction(speed=9.0)).send()
        PeerControl(name='bob', action=_UnionStopAction()).send()
        wait()
        sub.close()

        assert sorted(received) == [
            ('alice', _UnionStartAction),
            ('bob', _UnionStopAction),
        ]
