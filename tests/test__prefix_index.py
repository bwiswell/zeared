from __future__ import annotations

import random

import pytest
import zenoh

from zeared._prefix_index import _PrefixIndex
from zeared.keyexpr import intersects


def _linear_match(cached: list[str], query: str) -> set[str]:
    """Reference: every cached topic Zenoh says ``query`` intersects, by a linear scan.

    Until 0.4.2 this was a hand-written regex, which read a trailing ``**`` as
    one chunk or more and had no ``$*``; the trie agreed with it, and both
    differed from Zenoh. Now the reference is Zenoh's own answer.
    """
    return {t for t in cached if intersects(query, t)}


class TestBasic:
    def test_add_then_match_concrete(self):
        idx = _PrefixIndex()
        idx.add('a/b/c')
        assert set(idx.matching('a/b/c')) == {'a/b/c'}
        assert set(idx.matching('a/b/d')) == set()

    def test_remove(self):
        idx = _PrefixIndex()
        idx.add('a/b/c')
        idx.add('a/b/d')
        idx.remove('a/b/c')
        assert set(idx.matching('a/b/c')) == set()
        assert set(idx.matching('a/b/d')) == {'a/b/d'}

    def test_remove_idempotent(self):
        idx = _PrefixIndex()
        idx.add('a/b/c')
        idx.remove('a/b/c')
        idx.remove('a/b/c')  # no error
        assert len(idx) == 0

    def test_size_tracking(self):
        idx = _PrefixIndex()
        assert len(idx) == 0
        idx.add('x/y')
        idx.add('x/z')
        assert len(idx) == 2
        idx.add('x/y')  # idempotent
        assert len(idx) == 2
        idx.remove('x/y')
        assert len(idx) == 1


class TestSingleSegmentWildcard:
    def test_star_matches_one_segment(self):
        idx = _PrefixIndex()
        for t in ['a/1/c', 'a/2/c', 'a/3/c', 'a/1/d']:
            idx.add(t)
        assert set(idx.matching('a/*/c')) == {'a/1/c', 'a/2/c', 'a/3/c'}

    def test_star_does_not_cross_slash(self):
        idx = _PrefixIndex()
        idx.add('a/x/y/c')  # would only match a/*/*/c
        assert set(idx.matching('a/*/c')) == set()

    def test_star_at_root(self):
        idx = _PrefixIndex()
        idx.add('foo/x')
        idx.add('bar/x')
        assert set(idx.matching('*/x')) == {'foo/x', 'bar/x'}


class TestMultiSegmentWildcard:
    def test_trailing_starstar_matches_zero_or_more(self):
        # As Zenoh's `a/**` does, it takes `a` itself (0.4.2). Until then the
        # trie read it as one segment or more, so a getter on `a/**` missed a
        # value retained at `a`, which a subscriber on `a/**` does receive.
        idx = _PrefixIndex()
        for t in ['a', 'a/x', 'a/x/y', 'a/x/y/z', 'b']:
            idx.add(t)
        assert set(idx.matching('a/**')) == {'a', 'a/x', 'a/x/y', 'a/x/y/z'}

    def test_starstar_at_root(self):
        idx = _PrefixIndex()
        for t in ['x/y', 'a/b/c', 'one']:
            idx.add(t)
        # `**` matches everything with at least one segment.
        assert set(idx.matching('**')) == {'x/y', 'a/b/c', 'one'}

    def test_starstar_inside_pattern(self):
        idx = _PrefixIndex()
        for t in ['a/b/c', 'a/x/c', 'a/b/x/c', 'a/c']:
            idx.add(t)
        result = set(idx.matching('a/**/c'))
        # All cached topics start with 'a' and end with 'c' — should all match.
        _linear_match(list(idx._all_concretes()), 'a/**/c') if hasattr(idx, '_all_concretes') else None
        # Manual reference: a/b/c (a → ** matches 'b' → c) ✓; a/x/c ✓;
        # a/b/x/c (a → ** matches 'b/x' → c) ✓; a/c (a → ** matches zero → c) ✓.
        assert result == {'a/b/c', 'a/x/c', 'a/b/x/c', 'a/c'}


class TestWithinAChunk:
    def test_dollar_star_matches_within_one_chunk(self):
        # Until 0.4.2 the trie looked `r-$*` up as a literal chunk, so a
        # retained class answered `query(robot='r-$*')` with nothing.
        idx = _PrefixIndex()
        for t in ['fleet/r-1/status', 'fleet/r-22/status', 'fleet/x-1/status', 'fleet/r-1/log']:
            idx.add(t)
        assert set(idx.matching('fleet/r-$*/status')) == {'fleet/r-1/status', 'fleet/r-22/status'}
        assert set(idx.matching('fleet/$*-1/$*')) == {'fleet/r-1/status', 'fleet/x-1/status', 'fleet/r-1/log'}

    def test_each_topic_is_yielded_once(self):
        # A query with two `**` reaches `a/a/a` several ways, and until 0.4.2
        # the walk yielded it each time; retention replied once per yield.
        idx = _PrefixIndex()
        for t in ['a/a/a', 'a/b', 'b/a/b']:
            idx.add(t)
        assert sorted(idx.matching('**/a/**')) == ['a/a/a', 'a/b', 'b/a/b']


class TestParityAgainstLinearScan:
    """Randomised corpus parity test — pin that the trie matches a
    linear-scan reference for arbitrary topic/query combinations."""

    @pytest.mark.parametrize('seed', list(range(20)))
    def test_random_corpus(self, seed):
        rng = random.Random(seed)
        # Build a corpus of 50 random concrete topics.
        segments = ['robot', 'vehicle', 'peer', 'status', 'telemetry', 'registry', 'alice', 'bob', 'charlie', 'dana']
        topics = set()
        while len(topics) < 50:
            depth = rng.randint(2, 5)
            t = '/'.join(rng.choice(segments) for _ in range(depth))
            topics.add(t)
        topics = sorted(topics)

        idx = _PrefixIndex()
        for t in topics:
            idx.add(t)

        # Generate 30 queries with random wildcards.
        for _ in range(30):
            depth = rng.randint(1, 5)
            parts = []
            for _ in range(depth):
                r = rng.random()
                if r < 0.2:
                    parts.append('*')
                elif r < 0.3:
                    parts.append('**')
                elif r < 0.4:
                    parts.append(rng.choice(segments)[:2] + '$*')  # within a chunk
                else:
                    parts.append(rng.choice(segments))
            # Sometimes add trailing **
            if rng.random() < 0.3:
                parts.append('**')
            # As Zenoh would deliver it: canonical (`**/**` → `**`, `**/*` → `*/**`).
            query = str(zenoh.KeyExpr.autocanonize('/'.join(parts) if parts else '**'))

            trie_result = set(idx.matching(query))
            linear_result = _linear_match(topics, query)
            assert trie_result == linear_result, (
                f'mismatch for query {query!r}\ntrie: {sorted(trie_result)}\nlinear: {sorted(linear_result)}'
            )


class TestRemoveAndPrune:
    def test_branches_pruned_after_full_removal(self):
        idx = _PrefixIndex()
        idx.add('a/b/c')
        idx.add('a/b/d')
        idx.remove('a/b/c')
        # 'a/b' subtree still has 'd' — should still match.
        assert set(idx.matching('a/b/*')) == {'a/b/d'}
        idx.remove('a/b/d')
        # All gone; trie root should be empty.
        assert idx._root == {}
