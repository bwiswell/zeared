from __future__ import annotations

import pytest

from zeared._template import Template, Templates
from zeared.errors import TopicError


class TestParse:
    def test_static_topic(self):
        t = Template.parse('events/alerts')
        assert t.field_names == ()
        assert t.wildcard == 'events/alerts'

    def test_one_field(self):
        t = Template.parse('robot/{id}/telemetry')
        assert t.field_names == ('id',)
        assert t.wildcard == 'robot/*/telemetry'

    def test_two_fields(self):
        t = Template.parse('site/{site}/robot/{id}/telemetry')
        assert t.field_names == ('site', 'id')
        assert t.wildcard == 'site/*/robot/*/telemetry'

    def test_leading_field(self):
        t = Template.parse('{tenant}/events')
        assert t.field_names == ('tenant',)
        assert t.wildcard == '*/events'

    def test_empty_raises(self):
        with pytest.raises(TopicError):
            Template.parse('')

    def test_duplicate_field_raises(self):
        with pytest.raises(TopicError, match='duplicate'):
            Template.parse('a/{x}/b/{x}')

    def test_format_spec_rejected(self):
        with pytest.raises(TopicError, match='format specs'):
            Template.parse('a/{x:03d}')

    def test_invalid_field_name(self):
        with pytest.raises(TopicError, match='invalid field name'):
            Template.parse('a/{1bad}')


class TestRender:
    def test_static_render(self):
        t = Template.parse('events/alerts')
        assert t.render({}) == 'events/alerts'

    def test_one_field_render(self):
        t = Template.parse('robot/{id}/telemetry')
        assert t.render({'id': 7}) == 'robot/7/telemetry'

    def test_missing_field_raises(self):
        t = Template.parse('robot/{id}/telemetry')
        with pytest.raises(TopicError, match='missing field'):
            t.render({})

    def test_extra_fields_ignored(self):
        t = Template.parse('robot/{id}/telemetry')
        assert t.render({'id': 1, 'extra': 'x'}) == 'robot/1/telemetry'


class TestMatch:
    def test_static_match(self):
        t = Template.parse('events/alerts')
        assert t.match('events/alerts') == {}
        assert t.match('events/other') is None

    def test_one_field_match(self):
        t = Template.parse('robot/{id}/telemetry')
        assert t.match('robot/7/telemetry') == {'id': '7'}
        assert t.match('robot/7/telemetry/extra') is None
        assert t.match('other/7/telemetry') is None

    def test_multi_field_match(self):
        t = Template.parse('site/{site}/robot/{id}/telemetry')
        assert t.match('site/a/robot/3/telemetry') == {'site': 'a', 'id': '3'}

    def test_field_does_not_cross_slash(self):
        t = Template.parse('robot/{id}/telemetry')
        # Slash in middle of the captured segment must not match.
        assert t.match('robot/a/b/telemetry') is None


class TestMultiWildcard:
    def test_parse_trailing_multi(self):
        t = Template.parse('robot/**')
        assert t.wildcard == 'robot/**'
        assert t.publishable is False
        assert t.field_names == ()

    def test_parse_plain_multi(self):
        t = Template.parse('**')
        assert t.wildcard == '**'
        assert t.publishable is False

    def test_parse_multi_with_capture(self):
        t = Template.parse('robot/{id}/**')
        assert t.wildcard == 'robot/*/**'
        assert t.publishable is False
        assert t.field_names == ('id',)

    def test_reject_non_trailing_multi(self):
        with pytest.raises(TopicError, match='final path segment'):
            Template.parse('robot/**/status')

    def test_match_trailing_multi(self):
        t = Template.parse('robot/**')
        # One trailing segment
        assert t.match('robot/x') == {}
        # Many segments
        assert t.match('robot/x/y/z') == {}
        # Missing / empty tail — '**' requires at least one trailing segment
        assert t.match('robot') is None
        # Different prefix
        assert t.match('other/x') is None

    def test_match_capture_plus_multi(self):
        t = Template.parse('robot/{id}/**')
        assert t.match('robot/7/status') == {'id': '7'}
        assert t.match('robot/7/a/b/c') == {'id': '7'}
        assert t.match('robot/7') is None   # no trailing segment for **

    def test_subscribe_only_template_rejects_render(self):
        t = Template.parse('robot/**')
        with pytest.raises(TopicError, match='subscribe-only'):
            t.render({})


class TestNamespaceReservation:
    """Pin: templates whose first literal segment is ``__zeared`` collide
    with internal zeared routing (liveliness tokens, will envelopes, the
    presence observer). Reject at parse time. Exemption: the unmodified
    anonymous catch-all forms ``**`` and ``__zeared/**`` — diagnostic
    tooling is allowed to subscribe to internal traffic explicitly.
    Named multi-segment under the prefix is NOT exempt (a structural
    claim on internal routing)."""

    def test_alive_namespace_with_capture_rejected(self):
        with pytest.raises(TopicError, match='reserved for internal'):
            Template.parse('__zeared/alive/{x}')

    def test_will_namespace_concrete_rejected(self):
        with pytest.raises(TopicError, match='reserved for internal'):
            Template.parse('__zeared/will/foo')

    def test_anonymous_trailing_catch_all_exempt(self):
        # Diagnostic tools that want internal traffic — explicit intent.
        t = Template.parse('__zeared/**')
        assert t.publishable is False
        assert t.wildcard == '__zeared/**'

    def test_universal_catch_all_exempt(self):
        t = Template.parse('**')
        assert t.publishable is False

    def test_named_multi_under_reserved_rejected(self):
        # Named multi-segment under the prefix is a structural claim on
        # internal routing — reject. The unmodified catch-all is the
        # only exempted form.
        with pytest.raises(TopicError, match='reserved for internal'):
            Template.parse('__zeared/{tail**}')

    def test_single_segment_under_reserved_rejected(self):
        with pytest.raises(TopicError, match='reserved for internal'):
            Template.parse('__zeared/alive/{x}')

    def test_lone_reserved_segment_rejected(self):
        # Defensive — a sole `__zeared` literal shouldn't be confusable
        # with the catch-all and could collide with a future
        # convention (e.g. internal heartbeat).
        with pytest.raises(TopicError, match='reserved for internal'):
            Template.parse('__zeared')

    def test_reserved_in_non_first_position_allowed(self):
        # Only the first segment matters — the user's own namespace is
        # free to use `__zeared` as a deeper component.
        t = Template.parse('mything/__zeared/x')
        assert t.match('mything/__zeared/x') == {}

    def test_first_segment_slot_skips_check(self):
        # `{tenant}` is a runtime value; can't statically validate.
        t = Template.parse('{tenant}/__zeared/x')
        # User's runtime value would never resolve to literal '__zeared'
        # under normal usage — but the static check has nothing to do.
        assert t.field_names == ('tenant',)


class TestTemplatesContainer:
    def test_build_collects_union_of_slots(self):
        tpls = Templates.build('a/{x}/b', ('c/{y}/d',))
        assert tpls.field_names == frozenset({'x', 'y'})

    def test_resolve_publish_rejects_subscribe_only_canonical(self):
        tpls = Templates.build('robot/**', ())
        with pytest.raises(TopicError, match='subscribe-only'):
            tpls.resolve_publish_topic(None)

    def test_resolve_publish_rejects_subscribe_only_extra_override(self):
        tpls = Templates.build('robot/{id}/status', ('robot/**',))
        with pytest.raises(TopicError, match='subscribe-only'):
            tpls.resolve_publish_topic('robot/**')

    def test_match_tries_templates_in_declaration_order(self):
        tpls = Templates.build('a/{x}', ('a/**',))
        # First template is more specific; a/7 matches it
        m = tpls.match('a/7')
        assert m is not None
        tpl, caps = m
        assert tpl.raw == 'a/{x}'
        assert caps == {'x': '7'}
        # Multi-segment only matches the second template
        m2 = tpls.match('a/7/8')
        assert m2 is not None
        tpl2, caps2 = m2
        assert tpl2.raw == 'a/**'
        assert caps2 == {}
