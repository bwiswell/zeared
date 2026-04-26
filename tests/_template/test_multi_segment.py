"""Tests for named multi-segment template captures (``{tail**}``) and the
named-single-segment-at-any-position regression introduced in 0.0.11.

Parser/match/render tests live here, plus one smoke test per affected
subsystem (retention, presence, batch, async) confirming multi-segment
captures compose correctly with the existing wire path.
"""
from __future__ import annotations

import asyncio

import pytest

import zeared as z
from zeared._template import Template, Templates
from zeared.errors import TopicError

from conftest import wait


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class TestNamedMultiSegmentParse:
    def test_trailing_named_multi(self):
        t = Template.parse('log/{service}/{tail**}')
        assert t.field_names == ('service', 'tail')
        assert t.wildcard == 'log/*/**'
        assert t.publishable is True
        assert t._named_multi == 'tail'

    def test_lone_named_multi(self):
        t = Template.parse('{tail**}')
        assert t.field_names == ('tail',)
        assert t.wildcard == '**'
        assert t.publishable is True
        assert t._named_multi == 'tail'

    def test_named_multi_no_other_fields(self):
        t = Template.parse('log/{tail**}')
        assert t.field_names == ('tail',)
        assert t.wildcard == 'log/**'
        assert t.publishable is True

    def test_named_multi_not_at_end_raises(self):
        with pytest.raises(TopicError, match='final path segment'):
            Template.parse('log/{tail**}/extra')

    def test_two_named_multi_raises(self):
        with pytest.raises(TopicError, match='final path segment'):
            Template.parse('log/{a**}/{b**}')

    def test_named_multi_plus_anonymous_multi_raises(self):
        with pytest.raises(TopicError, match='final path segment'):
            Template.parse('log/{tail**}/**')

    def test_format_spec_on_multi_slot_raises(self):
        with pytest.raises(TopicError):
            Template.parse('log/{tail**:fmt}')

    def test_conversion_on_multi_slot_raises(self):
        with pytest.raises(TopicError):
            Template.parse('log/{tail**!r}')

    def test_duplicate_name_with_single_raises(self):
        with pytest.raises(TopicError, match='duplicate'):
            Template.parse('log/{tail}/{tail**}')


# ---------------------------------------------------------------------------
# Match
# ---------------------------------------------------------------------------


class TestNamedMultiSegmentMatch:
    def test_match_single_trailing_segment(self):
        t = Template.parse('log/{service}/{tail**}')
        assert t.match('log/svc/x') == {'service': 'svc', 'tail': 'x'}

    def test_match_many_trailing_segments(self):
        t = Template.parse('log/{service}/{tail**}')
        assert t.match('log/svc/a/b/c') == {'service': 'svc', 'tail': 'a/b/c'}

    def test_match_requires_at_least_one_trailing(self):
        t = Template.parse('log/{service}/{tail**}')
        # Bare 'log/svc' has no trailing segment.
        assert t.match('log/svc') is None

    def test_match_lone_named_multi(self):
        t = Template.parse('{tail**}')
        assert t.match('a') == {'tail': 'a'}
        assert t.match('a/b/c') == {'tail': 'a/b/c'}

    def test_match_does_not_cross_single_field(self):
        t = Template.parse('log/{service}/{tail**}')
        # `service` slot is single-segment; no slash crossing.
        # 'log/svc/x' parses as service=svc, tail=x.
        # 'log/multi/svc/x' parses as service=multi, tail=svc/x.
        assert t.match('log/multi/svc/x') == {'service': 'multi', 'tail': 'svc/x'}


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


class TestNamedMultiSegmentRender:
    def test_render_single_segment_value(self):
        t = Template.parse('log/{service}/{tail**}')
        assert t.render({'service': 'svc', 'tail': 'x'}) == 'log/svc/x'

    def test_render_slash_containing_value(self):
        t = Template.parse('log/{service}/{tail**}')
        rendered = t.render({'service': 'svc', 'tail': 'a/b/c'})
        assert rendered == 'log/svc/a/b/c'

    def test_render_empty_tail_raises(self):
        t = Template.parse('log/{service}/{tail**}')
        with pytest.raises(TopicError, match='cannot be empty'):
            t.render({'service': 'svc', 'tail': ''})

    def test_render_missing_tail_raises(self):
        t = Template.parse('log/{service}/{tail**}')
        with pytest.raises(TopicError, match='missing field'):
            t.render({'service': 'svc'})

    def test_lone_named_multi_render(self):
        t = Template.parse('{tail**}')
        assert t.render({'tail': 'a/b'}) == 'a/b'


# ---------------------------------------------------------------------------
# Round-trip via Cls (declared field binding)
# ---------------------------------------------------------------------------


class TestDeclaredFieldRoundTrip:
    def test_str_field_round_trip(self):
        @z.zeared
        class Log(z.Message):
            TOPIC = 'log/{service}/{tail**}'
            service: str = z.Str(required=True)
            tail:    str = z.Str(required=True)

        # Just exercise template parse + render; live wire round-trip
        # is in the Phase-2 retention/batch/async smoke tests.
        tpls = Log._templates()
        assert tpls.canonical.publishable is True
        assert tpls.canonical.wildcard == 'log/*/**'
        rendered = tpls.canonical.render({'service': 'svc', 'tail': 'a/b/c'})
        assert rendered == 'log/svc/a/b/c'
        captures = tpls.match('log/svc/a/b/c')
        assert captures is not None
        _, caps = captures
        assert caps == {'service': 'svc', 'tail': 'a/b/c'}

    def test_capture_only_lands_on_meta(self, session):
        """Undeclared multi-segment slots flow through to meta.captures
        as a slash-containing string (matches single-segment behaviour)."""
        @z.zeared
        class Log(z.Message):
            TOPIC = 'log/{service}/{tail**}'
            # `tail` NOT declared as a field — should land on meta.captures
            service: str = z.Str(required=True)

        tpls = Log._templates()
        m = tpls.match('log/svc/a/b/c')
        assert m is not None
        _, caps = m
        # Both captures present in the regex match dict.
        assert caps['service'] == 'svc'
        assert caps['tail'] == 'a/b/c'


# ---------------------------------------------------------------------------
# Field-binding validation (rejected at first _templates() call)
# ---------------------------------------------------------------------------


class TestFieldBindingValidation:
    def test_many_str_field_rejected(self):
        @z.zeared
        class Bad(z.Message):
            TOPIC = 'log/{tail**}'
            tail: list = z.Str(many=True, missing=[])

        with pytest.raises(TopicError, match='multi-segment field'):
            Bad._templates()

    def test_keyed_str_field_rejected(self):
        @z.zeared
        class Bad(z.Message):
            TOPIC = 'log/{tail**}'
            tail: dict = z.Str(keyed=True, missing={})

        with pytest.raises(TopicError, match='multi-segment field'):
            Bad._templates()

    def test_int_field_rejected(self):
        @z.zeared
        class Bad(z.Message):
            TOPIC = 'log/{tail**}'
            tail: int = z.Int(required=True)

        with pytest.raises(TopicError, match='multi-segment field'):
            Bad._templates()

    def test_bool_field_rejected(self):
        @z.zeared
        class Bad(z.Message):
            TOPIC = 'log/{tail**}'
            tail: bool = z.Bool(required=True)

        with pytest.raises(TopicError, match='multi-segment field'):
            Bad._templates()

    def test_undeclared_slot_no_validation_error(self):
        """Capture-only slots are allowed — landing as meta.captures str."""
        @z.zeared
        class Ok(z.Message):
            TOPIC = 'log/{tail**}'
            # No declared `tail` field; capture-only is fine.
            something_else: str = z.Str(required=True, missing='x')

        # Should not raise.
        tpls = Ok._templates()
        assert tpls.canonical._named_multi == 'tail'


# ---------------------------------------------------------------------------
# Named single-segment at any position regression (already-works)
# ---------------------------------------------------------------------------


class TestNamedSingleAtAnyPosition:
    """Pin: ``peer/{cluster}/{host}/status`` — named single-segment captures
    at non-trailing positions work today via ``(?P<name>[^/]+)``. This is a
    regression test for that pre-existing behaviour."""
    def test_multi_named_singles_round_trip(self):
        t = Template.parse('peer/{cluster}/{host}/status')
        assert t.field_names == ('cluster', 'host')
        assert t.wildcard == 'peer/*/*/status'
        assert t.publishable is True

        rendered = t.render({'cluster': 'eu-west', 'host': 'h1'})
        assert rendered == 'peer/eu-west/h1/status'

        match = t.match('peer/eu-west/h1/status')
        assert match == {'cluster': 'eu-west', 'host': 'h1'}

    def test_named_singles_dont_cross_slash(self):
        t = Template.parse('peer/{cluster}/{host}/status')
        # cluster/host both single-segment; embedded slashes don't match.
        assert t.match('peer/eu/west/h1/status') is None


# ---------------------------------------------------------------------------
# EXTRA_TOPICS interaction (independent slot sets per template)
# ---------------------------------------------------------------------------


class TestExtraTopicsInteraction:
    def test_named_multi_in_topic_alongside_anonymous_extra(self):
        @z.zeared
        class Log(z.Message):
            TOPIC = 'log/{service}/{tail**}'
            EXTRA_TOPICS = ('audit/**',)
            service: str = z.Str(required=True)
            tail:    str = z.Str(required=True)

        tpls = Log._templates()
        assert tpls.canonical.publishable is True
        assert tpls.canonical.wildcard == 'log/*/**'

        # The extra is anonymous-trailing-** → subscribe-only.
        extras = tpls.extras
        assert len(extras) == 1
        assert extras[0].publishable is False
        assert extras[0].wildcard == 'audit/**'

        # Both templates contribute distinct match paths.
        m1 = tpls.match('log/svc/a/b')
        assert m1 is not None
        assert m1[0].raw == 'log/{service}/{tail**}'
        assert m1[1] == {'service': 'svc', 'tail': 'a/b'}

        m2 = tpls.match('audit/x/y')
        assert m2 is not None
        assert m2[0].raw == 'audit/**'
        assert m2[1] == {}


# ---------------------------------------------------------------------------
# Smoke tests — multi-segment x retention / presence / batch / async
# ---------------------------------------------------------------------------


class TestRetentionRoundTrip:
    def test_retained_publish_late_subscribe(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Log(z.Message):
            TOPIC = 'log/{service}/{tail**}'
            RETAINED = True
            service: str = z.Str(required=True)
            tail:    str = z.Str(required=True)
            line:    str = z.Str(required=True)

        # Publisher caches a deep-tail concrete topic.
        Log(service='svc', tail='2026/04/24/info', line='boot').send(session=session_a)
        wait(0.2)

        # Late subscriber on the other session — retained fetch via
        # session.get('log/*/**') should find the cached entry, the trie
        # walk should match it, and the dispatch should populate both
        # service and tail captures correctly.
        received: list[tuple[str, str, str]] = []
        sub = Log.on_message(
            lambda m: received.append((m.service, m.tail, m.line)),
            session=session_b,
        )
        wait(0.5)
        sub.close()

        assert ('svc', '2026/04/24/info', 'boot') in received


class TestPresenceWillRoundTrip:
    def test_will_with_multi_segment_topic(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Heartbeat(z.Message):
            TOPIC = 'service/{service}/{path**}'
            LIVELINESS = True
            service: str = z.Str(required=True)
            path:    str = z.Str(required=True)
            state:   str = z.Str(required=True)

        # Producer A registers a will at a deep concrete topic.
        Heartbeat(
            service='svc', path='region/eu/host1', state='offline',
        ).register_will(session=session_a)
        wait(0.3)

        received: list[tuple[str, str, str]] = []
        sub = Heartbeat.on_message(
            lambda m: received.append((m.service, m.path, m.state)),
            session=session_b,
        )
        wait(0.3)

        # Producer dies → subscriber synthesises will from the stashed envelope.
        session_a.close()
        wait(0.5)
        sub.close()

        assert ('svc', 'region/eu/host1', 'offline') in received


class TestBatchRoundTrip:
    def test_batch_buffer_renders_multi_segment(self, session):
        @z.zeared
        class Log(z.Message):
            TOPIC = 'log/{service}/{tail**}'
            service: str = z.Str(required=True)
            tail:    str = z.Str(required=True)
            line:    str = z.Str(required=True)

        z.session = session

        received: list[tuple[str, str, str]] = []
        sub = Log.on_message(
            lambda m: received.append((m.service, m.tail, m.line)),
        )
        wait(0.2)

        with z.batch():
            Log(service='svc', tail='a/b', line='one').send()
            Log(service='svc', tail='c/d/e', line='two').send()
        wait(0.3)
        sub.close()

        assert ('svc', 'a/b', 'one') in received
        assert ('svc', 'c/d/e', 'two') in received


class TestAsyncRoundTrip:
    def test_asend_alisten_round_trip(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Log(z.Message):
            TOPIC = 'log/{service}/{tail**}'
            service: str = z.Str(required=True)
            tail:    str = z.Str(required=True)
            line:    str = z.Str(required=True)

        async def main():
            received: list[tuple[str, str, str]] = []

            async def collect():
                async for m in Log.alisten(session=session_b):
                    received.append((m.service, m.tail, m.line))
                    if len(received) >= 1:
                        return

            consumer = asyncio.create_task(collect())
            await asyncio.sleep(0.2)
            await Log(service='svc', tail='deep/path', line='hi').asend(
                session=session_a,
            )
            await asyncio.wait_for(consumer, timeout=2.0)
            return received

        result = asyncio.run(main())
        assert ('svc', 'deep/path', 'hi') in result
