"""Tests for ``zeared/_template/_templates.py`` — the ``Templates``
aggregate (canonical + extras) that a message class declares.

Parser-level coverage of single-template behaviour lives in
``test__template.py``. This file targets the aggregate's build /
match / publish-resolution behaviour.
"""
from __future__ import annotations

import pytest

from zeared._template import Template, Templates
from zeared.errors import TopicError


class TestBuild:
    def test_canonical_only(self):
        ts = Templates.build('a/{x}', ())
        assert ts.canonical.raw == 'a/{x}'
        assert ts.extras == ()
        assert ts.field_names == frozenset({'x'})

    def test_canonical_plus_extras(self):
        ts = Templates.build('a/{x}', ('b/{y}',))
        assert ts.canonical.raw == 'a/{x}'
        assert len(ts.extras) == 1
        assert ts.extras[0].raw == 'b/{y}'
        assert ts.field_names == frozenset({'x', 'y'})

    def test_multi_field_names_only_named_multi(self):
        ts = Templates.build('log/{tail**}', ())
        assert ts.multi_field_names == frozenset({'tail'})
        # Single-segment fields don't appear in multi_field_names.
        ts2 = Templates.build('a/{x}', ())
        assert ts2.multi_field_names == frozenset()


class TestMatch:
    def test_match_canonical(self):
        ts = Templates.build('a/{x}', ('b/{y}',))
        result = ts.match('a/foo')
        assert result is not None
        tpl, caps = result
        assert tpl.raw == 'a/{x}'
        assert caps == {'x': 'foo'}

    def test_match_extra(self):
        ts = Templates.build('a/{x}', ('b/{y}',))
        result = ts.match('b/bar')
        assert result is not None
        tpl, caps = result
        assert tpl.raw == 'b/{y}'
        assert caps == {'y': 'bar'}

    def test_no_match(self):
        ts = Templates.build('a/{x}', ('b/{y}',))
        assert ts.match('c/baz') is None


class TestResolvePublishTopic:
    def test_default_returns_canonical(self):
        ts = Templates.build('a/{x}', ('b/{y}',))
        target = ts.resolve_publish_topic(None)
        assert target.raw == 'a/{x}'

    def test_explicit_override_picks_extra(self):
        ts = Templates.build('a/{x}', ('b/{y}',))
        target = ts.resolve_publish_topic('b/{y}')
        assert target.raw == 'b/{y}'

    def test_unknown_override_raises(self):
        ts = Templates.build('a/{x}', ('b/{y}',))
        with pytest.raises(TopicError, match='not a declared topic'):
            ts.resolve_publish_topic('c/{z}')

    def test_subscribe_only_template_rejected(self):
        ts = Templates.build('a/{x}', ('b/**',))
        with pytest.raises(TopicError, match='subscribe-only'):
            ts.resolve_publish_topic('b/**')
