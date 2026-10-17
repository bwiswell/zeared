"""Tests for ``zeared/keyexpr.py`` — key expressions, matched as Zenoh matches them.

The answers come from ``zenoh.KeyExpr``, so these pin the surface and the
cases the hand-rolled matchers got wrong or never had: ``$*`` within a chunk,
``**`` matching no chunk, non-canonical input, and a ``{name**}`` slot that
never renders empty.
"""

from __future__ import annotations

import pytest

import zeared as z
from zeared.keyexpr import includes, intersects, is_concrete, shape


class TestIncludes:
    @pytest.mark.parametrize(
        ('rule', 'key'),
        [
            # rio-gateway's own examples (auth/scope.py).
            ('rio/telemetry/**', 'rio/telemetry/read/r1'),
            ('rio/config/geometry/rtls/*', 'rio/config/geometry/rtls/r1'),
            ('rio/**/rtls/**', 'rio/config/geometry/rtls/r1'),
            ('fleet/r-$*/status', 'fleet/r-7/status'),
            ('fleet/r-$*/status', 'fleet/r-/status'),  # ``$*`` may match nothing within the chunk
            ('fleet/**', 'fleet'),  # ``**`` may match no chunk
            ('a/b', 'a/b'),
        ],
    )
    def test_a_rule_reaches_a_key(self, rule, key):
        assert includes(rule, key)

    @pytest.mark.parametrize(
        ('rule', 'key'),
        [
            ('rio/telemetry/*', 'rio/telemetry/read/r1'),  # ``*`` is one chunk
            ('fleet/r-$*/status', 'fleet/x-7/status'),
            ('fleet/**/status', 'fleet/a/other'),
            ('a/b', 'a/b/c'),
        ],
    )
    def test_a_rule_misses_a_key(self, rule, key):
        assert not includes(rule, key)

    def test_it_compares_wildcards_too(self):
        assert includes('fleet/**', 'fleet/*/status')
        assert not includes('fleet/*/status', 'fleet/**')

    def test_non_canonical_input_reads_as_its_canonical_form(self):
        assert includes('a/**/**', 'a/b/c')
        assert shape('a/**') == 'a/*/**'


class TestIntersects:
    def test_two_wildcards_meet_where_one_key_matches_both(self):
        assert intersects('fleet/*/status', 'fleet/r1/**')
        assert intersects('fleet/r-$*/status', 'fleet/$*-7/status')
        assert not intersects('fleet/*/status', 'depot/*/status')

    def test_it_is_symmetric(self):
        assert intersects('a/*', 'a/b') == intersects('a/b', 'a/*') is True


class TestShape:
    @pytest.mark.parametrize(
        ('topic', 'expected'),
        [
            ('fleet/status', 'fleet/status'),
            ('fleet/{robot}/status', 'fleet/*/status'),
            ('fleet/{robot}/status/{id}', 'fleet/*/status/*'),
            ('files/{path**}', 'files/*/**'),
            ('fleet/{robot}/log/{rest**}', 'fleet/*/log/*/**'),
            ('fleet/**', 'fleet/*/**'),
            ('{rest**}', '**'),
            ('**', '**'),
        ],
    )
    def test_slots_become_wildcards(self, topic, expected):
        assert shape(topic) == expected

    def test_a_tail_slot_is_one_chunk_or_more(self):
        # The case zemqtt's ``can_match`` writes out by hand: a tail slot
        # never renders empty, so a rule for the bare prefix can't reach it.
        assert not intersects('logs/app', shape('logs/app/{rest**}'))
        assert intersects('logs/app/today', shape('logs/app/{rest**}'))
        assert includes('logs/**', shape('logs/app/{rest**}'))

    def test_it_answers_route_questions(self):
        route = shape('rio/telemetry/read/{source}/{reader}')
        assert intersects('rio/**/rtls/**', route)  # could deliver something allowed
        assert not includes('rio/**/rtls/**', route)  # but not everything it carries
        assert includes('rio/telemetry/**', route)

    def test_a_template_that_isnt_one_raises_topic_error(self):
        with pytest.raises(z.TopicError):
            shape('a/**/b')


class TestIsConcrete:
    @pytest.mark.parametrize(
        ('key', 'concrete'),
        [('r-1', True), ('a/b/c', True), ('*', False), ('r-$*', False), ('a/**', False)],
    )
    def test_a_wildcard_chunk_makes_a_key_wide(self, key, concrete):
        assert is_concrete(key) is concrete


class TestErrors:
    def test_a_wildcard_within_a_chunk_gets_zenohs_spelling(self):
        with pytest.raises(z.KeyExprError, match=r'read-\*.*within a chunk, Zenoh writes the wildcard `\$\*`'):
            includes('rio/read-*', 'rio/read-1')

    @pytest.mark.parametrize('bad', ['', 'a//b', '/a', 'a/'])
    def test_an_empty_chunk_is_refused(self, bad):
        with pytest.raises(z.KeyExprError, match='empty chunks are forbidden') as info:
            intersects(bad, 'a')
        assert ' at /' not in str(info.value)  # Zenoh's source location is dropped
        assert 'within a chunk' not in str(info.value)

    def test_it_is_a_value_error(self):
        with pytest.raises(ValueError, match='Invalid Key Expr'):
            is_concrete('a//b')

    def test_only_strings(self):
        with pytest.raises(TypeError, match='a key expression is a str, got int'):
            includes('a', 1)  # ty: ignore[invalid-argument-type]


def test_the_module_is_reachable_from_the_package():
    assert z.keyexpr.includes is includes
