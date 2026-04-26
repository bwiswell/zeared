from __future__ import annotations

import warnings

import pytest

import zeared as z
from zeared.errors import ZearedError
from zeared.publisher import _registry, effective_cap, get_cache

from conftest import wait


class TestEffectiveCap:
    def test_true_gives_default(self):
        class C:
            PUBLISHER = True
        assert effective_cap(C) == 256

    def test_false_gives_zero(self):
        class C:
            PUBLISHER = False
        assert effective_cap(C) == 0

    def test_int_gives_int(self):
        class C:
            PUBLISHER = 7
        assert effective_cap(C) == 7

    def test_missing_defaults_to_true(self):
        class C:
            pass
        assert effective_cap(C) == 256


class TestCacheDeclaresPublishersOnce:
    def test_static_topic_reuses_one_publisher(self, session):
        @z.zeared
        class Alert(z.Message):
            TOPIC = 'events/alerts'
            msg: str = z.Str(required=True)

        received = []
        z.session = session
        sub = Alert.on_message(received.append)
        wait()

        Alert(msg='a').send()
        Alert(msg='b').send()
        Alert(msg='c').send()
        wait()
        sub.close()

        cache = get_cache(Alert, session)
        assert cache.size == 1  # single concrete topic → one publisher reused
        assert [m.msg for m in received] == ['a', 'b', 'c']

    def test_templated_topic_caches_per_concrete_key(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'cache/tmpl/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        received = []
        z.session = session
        sub = Tele.on_message(received.append)
        wait()

        Tele(id=1, v=10).send()
        Tele(id=2, v=20).send()
        Tele(id=1, v=11).send()  # reuses publisher for id=1
        wait()
        sub.close()

        cache = get_cache(Tele, session)
        assert cache.size == 2  # two distinct concrete topics
        assert len(received) == 3


class TestCap:
    def test_disabled_cache_falls_through_to_session_put(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'nocache/{id}'
            PUBLISHER = False
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        received = []
        z.session = session
        sub = Tele.on_message(received.append)
        wait()

        for i in range(5):
            Tele(id=i, v=i).send()
        wait()
        sub.close()

        cache = get_cache(Tele, session)
        assert cache.size == 0  # nothing declared
        assert len(received) == 5

    def test_explicit_cap(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'capped/{id}'
            PUBLISHER = 2
            id: int = z.Int(required=True)

        z.session = session

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            Tele(id=1).send()
            Tele(id=2).send()
            assert get_cache(Tele, session).size == 2
            # Third distinct key → overflow → fallback + warn
            Tele(id=3).send()
            wait()
            assert get_cache(Tele, session).size == 2
            msgs = [str(w.message) for w in captured]
            assert any('publisher cache cap (2) reached' in m for m in msgs)

    def test_warning_fires_once_only(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'onewarn/{id}'
            PUBLISHER = 1
            id: int = z.Int(required=True)

        z.session = session

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            Tele(id=1).send()  # fills
            Tele(id=2).send()  # overflow → warn
            Tele(id=3).send()  # overflow → silent (already warned)
            Tele(id=4).send()  # overflow → silent
            cache_msgs = [w for w in captured if 'publisher cache cap' in str(w.message)]
            assert len(cache_msgs) == 1


class TestClearCache:
    def test_clear_all(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class M(z.Message):
            TOPIC = 'clearall/topic'
            v: int = z.Int(required=True)

        M(v=1).send(session=session_a)
        M(v=2).send(session=session_b)
        assert get_cache(M, session_a).size == 1
        assert get_cache(M, session_b).size == 1

        z.clear_publisher_cache()

        assert (M, id(session_a)) not in _registry
        assert (M, id(session_b)) not in _registry

    def test_clear_by_session(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class M(z.Message):
            TOPIC = 'clearbysess/topic'
            v: int = z.Int(required=True)

        M(v=1).send(session=session_a)
        M(v=2).send(session=session_b)

        z.clear_publisher_cache(session=session_a)

        assert (M, id(session_a)) not in _registry
        assert (M, id(session_b)) in _registry


class TestPublishedTopics:
    def test_class_method_snapshot(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'emit/cls/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        assert Tele.published_topics() == frozenset()

        Tele(id=1, v=10).send()
        Tele(id=2, v=20).send()
        Tele(id=1, v=11).send()   # overwrite existing concrete key

        assert Tele.published_topics() == frozenset({
            'emit/cls/1', 'emit/cls/2',
        })

    def test_tracks_publisher_false(self, session):
        """PUBLISHER=False classes still have their emissions recorded."""
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'emit/nocache/{id}'
            PUBLISHER = False
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Plain(id=1, v=1).send()
        Plain(id=2, v=2).send()

        cache = get_cache(Plain, session)
        assert cache.size == 0    # no publishers declared
        assert Plain.published_topics() == frozenset({
            'emit/nocache/1', 'emit/nocache/2',
        })

    def test_tombstone_does_not_remove(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'emit/tomb/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=1, v=1).send()
        Tele(id=2, v=2).send()
        Tele.unretain(id=1)

        # Topic 1 is no longer retained, but the "ever emitted" set keeps it.
        assert Tele.published_topics() == frozenset({
            'emit/tomb/1', 'emit/tomb/2',
        })

    def test_includes_cap_overflow_topics(self, session):
        @z.zeared
        class Small(z.Message):
            TOPIC = 'emit/cap/{id}'
            PUBLISHER = 2   # cap of 2 concrete keys
            id: int = z.Int(required=True)

        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            z.session = session
            for i in range(5):
                Small(id=i).send()

        assert Small.published_topics() == frozenset({
            f'emit/cap/{i}' for i in range(5)
        })

    def test_session_filter(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'emit/sess/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        Tele(id=1, v=1).send(session=session_a)
        Tele(id=2, v=2).send(session=session_b)

        assert Tele.published_topics(session=session_a) == frozenset({'emit/sess/1'})
        assert Tele.published_topics(session=session_b) == frozenset({'emit/sess/2'})
        # No-arg form aggregates across sessions
        assert Tele.published_topics() == frozenset({
            'emit/sess/1', 'emit/sess/2',
        })


class TestModulePublishedTopics:
    def test_returns_dict_of_sets(self, session):
        @z.zeared
        class A(z.Message):
            TOPIC = 'mod/a/{id}'
            id: int = z.Int(required=True)

        @z.zeared
        class B(z.Message):
            TOPIC = 'mod/b/{id}'
            id: int = z.Int(required=True)

        z.session = session
        A(id=1).send()
        A(id=2).send()
        B(id=1).send()

        all_topics = z.published_topics()
        # Keyed on (cls, id(session))
        assert (A, id(session)) in all_topics
        assert (B, id(session)) in all_topics
        assert all_topics[(A, id(session))] == frozenset({'mod/a/1', 'mod/a/2'})
        assert all_topics[(B, id(session))] == frozenset({'mod/b/1'})

    def test_filter_by_class(self, session):
        @z.zeared
        class A(z.Message):
            TOPIC = 'mod/fc/a/{id}'
            id: int = z.Int(required=True)

        @z.zeared
        class B(z.Message):
            TOPIC = 'mod/fc/b/{id}'
            id: int = z.Int(required=True)

        z.session = session
        A(id=1).send()
        B(id=1).send()

        only_a = z.published_topics(cls=A)
        assert (A, id(session)) in only_a
        assert (B, id(session)) not in only_a

    def test_filter_by_session(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'mod/fs/{id}'
            id: int = z.Int(required=True)

        Tele(id=1).send(session=session_a)
        Tele(id=2).send(session=session_b)

        only_a = z.published_topics(session=session_a)
        keys = list(only_a.keys())
        assert all(k[1] == id(session_a) for k in keys)
        assert (Tele, id(session_a)) in only_a
        assert (Tele, id(session_b)) not in only_a

    def test_empty_when_nothing_published(self):
        assert z.published_topics() == {}


class TestClosedSession:
    def test_send_on_closed_session_raises_zeared_error(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'closed/topic'
            v: int = z.Int(required=True)

        z.session = session
        M(v=1).send()  # populates cache
        assert get_cache(M, session).size == 1

        session.close()
        with pytest.raises(ZearedError):
            M(v=2).send()
        # Cache entry cleaned up on failure.
        assert (M, id(session)) not in _registry
