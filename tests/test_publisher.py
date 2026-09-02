from __future__ import annotations

import contextlib
import time
import warnings

import pytest
from conftest import _peer_session, wait

import zeared as z
from zeared._managed_session import ManagedSession
from zeared._reconnect import start_probe
from zeared.errors import SessionDeadError, ZearedError
from zeared.publisher import _registry, effective_cap, get_cache


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
        Tele(id=1, v=11).send()  # overwrite existing concrete key

        assert Tele.published_topics() == frozenset(
            {
                'emit/cls/1',
                'emit/cls/2',
            }
        )

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
        assert cache.size == 0  # no publishers declared
        assert Plain.published_topics() == frozenset(
            {
                'emit/nocache/1',
                'emit/nocache/2',
            }
        )

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
        assert Tele.published_topics() == frozenset(
            {
                'emit/tomb/1',
                'emit/tomb/2',
            }
        )

    def test_includes_cap_overflow_topics(self, session):
        @z.zeared
        class Small(z.Message):
            TOPIC = 'emit/cap/{id}'
            PUBLISHER = 2  # cap of 2 concrete keys
            id: int = z.Int(required=True)

        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            z.session = session
            for i in range(5):
                Small(id=i).send()

        assert Small.published_topics() == frozenset({f'emit/cap/{i}' for i in range(5)})

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
        assert Tele.published_topics() == frozenset(
            {
                'emit/sess/1',
                'emit/sess/2',
            }
        )


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


class TestInvalidate:
    """``invalidate()`` is the reconnect counterpart to ``drop()``.

    Both undeclare the cached ``zenoh.Publisher`` handles; only ``drop()``
    also deregisters the cache. The distinction matters because the cache
    object carries ``_emitted`` — the process-lifetime history behind
    ``published_topics()`` — which must survive a reconnect.
    """

    def test_clears_handles_but_keeps_registration(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'inval/{n}'
            n: int = z.Int(required=True)

        z.session = session
        M(n=1).send()
        M(n=2).send()
        cache = get_cache(M, session)
        assert cache.size == 2

        cache.invalidate()

        assert cache.size == 0
        assert (M, id(session)) in _registry
        assert get_cache(M, session) is cache

    def test_preserves_emitted_history(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'inval/hist/{n}'
            n: int = z.Int(required=True)

        z.session = session
        M(n=1).send()
        M(n=2).send()
        before = M.published_topics(session=session)
        assert len(before) == 2

        get_cache(M, session).invalidate()

        assert M.published_topics(session=session) == before

    def test_next_send_redeclares(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'inval/redeclare'
            v: int = z.Int(required=True)

        z.session = session
        M(v=1).send()
        cache = get_cache(M, session)
        old_pub = cache._pubs['inval/redeclare']

        cache.invalidate()
        M(v=2).send()

        assert cache.size == 1
        assert cache._pubs['inval/redeclare'] is not old_pub

    def test_drop_still_deregisters(self, session):
        """``drop()`` delegates its undeclare loop to ``invalidate()`` but
        keeps its own contract — the registry entry goes away."""

        @z.zeared
        class M(z.Message):
            TOPIC = 'inval/drop'
            v: int = z.Int(required=True)

        z.session = session
        M(v=1).send()
        cache = get_cache(M, session)

        cache.drop()

        assert cache.size == 0
        assert (M, id(session)) not in _registry


class TestCachedPublisherFailureDetection:
    """Pin: a cached-publisher send failure drives lazy reconnect detection.

    ``_session_put`` reaches ``ManagedSession._note_failure`` for free via
    ``_ZenohApiMixin.put``, so the ``PUBLISHER = False`` path always drove
    detection. A cached ``zenoh.Publisher`` bypasses the wrapper entirely,
    so the *default* ``PUBLISHER = True`` path did not — meaning a
    publisher-only daemon running ``probe_interval=0`` (documented as
    "only send-failure detection runs") never detected a dead session at
    all.
    """

    def _managed(self, old_raw, new_raw):
        return ManagedSession(
            old_raw,
            lambda: new_raw,
            endpoint_label='pub-detect',
            probe_interval=0,  # send-failure detection only
            initial_backoff=0.001,
            max_backoff=0.005,
            max_attempts=None,
        )

    def test_cached_publisher_failure_triggers_reconnect(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'detect/cached'
            v: int = z.Int(required=True)

        old_raw, new_raw = _peer_session(), _peer_session()
        m = self._managed(old_raw, new_raw)
        try:
            start_probe(m)
            M(v=1).send(session=m)  # populates the cache on old_raw
            wait(0.2)

            old_raw.close()
            with pytest.raises(ZearedError):
                M(v=2).send(session=m)

            # The reconnect worker runs off-thread; give it a beat.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and m.raw() is not new_raw:
                wait(0.05)
            assert m.raw() is new_raw, (
                'cached-publisher send failure did not drive lazy reconnect '
                'detection — _pub_put bypasses ManagedSession, so it must '
                'report the failure itself'
            )
        finally:
            with contextlib.suppress(Exception):
                m._teardown(call_close=False)
            with contextlib.suppress(Exception):
                new_raw.close()

    def test_failure_while_reconnecting_raises_session_dead_error(self):
        """The specific error, not a bare ``ZearedError`` — otherwise the
        retry/queue handler the README tells callers to write can't catch
        it. ``SessionDeadError`` subclasses ``ZearedError``, so generic
        handlers are unaffected."""

        @z.zeared
        class M(z.Message):
            TOPIC = 'detect/dead'
            v: int = z.Int(required=True)

        old_raw, new_raw = _peer_session(), _peer_session()
        m = self._managed(old_raw, new_raw)
        try:
            start_probe(m)
            M(v=1).send(session=m)
            wait(0.2)

            old_raw.close()
            with pytest.raises(SessionDeadError):
                M(v=2).send(session=m)
        finally:
            with contextlib.suppress(Exception):
                m._teardown(call_close=False)
            with contextlib.suppress(Exception):
                new_raw.close()

    def test_raw_session_failure_still_raises_plain_zeared_error(self, session):
        """A raw ``zenoh.Session`` has no state machine to consult — the
        error stays a plain ``ZearedError``."""

        @z.zeared
        class M(z.Message):
            TOPIC = 'detect/raw'
            v: int = z.Int(required=True)

        z.session = session
        M(v=1).send()
        session.close()

        with pytest.raises(ZearedError) as exc_info:
            M(v=2).send()
        assert not isinstance(exc_info.value, SessionDeadError)
