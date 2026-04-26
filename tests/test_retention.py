from __future__ import annotations

import pytest

import zeared as z
from zeared.retention import (
    _registry,
    clear_retention_cache,
    effective_retain,
    get_retention_cache,
)

from conftest import wait


class TestEffectiveRetain:
    def test_none_uses_class_default(self):
        class R:
            RETAINED = True
        assert effective_retain(R, None) is True

        class NR:
            RETAINED = False
        assert effective_retain(NR, None) is False

    def test_explicit_false_always_allowed(self):
        class R:
            RETAINED = True
        assert effective_retain(R, False) is False

        class NR:
            RETAINED = False
        assert effective_retain(NR, False) is False

    def test_retain_true_on_non_retained_raises(self):
        class NR:
            RETAINED = False

        with pytest.raises(z.TopicError, match='requires RETAINED = True'):
            effective_retain(NR, True)

    def test_retain_true_on_retained_ok(self):
        class R:
            RETAINED = True
        assert effective_retain(R, True) is True


class TestSendRoutesRetainedIntoCache:
    def test_default_retained_send_populates_cache(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/basic/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=1, v=10).send()
        Tele(id=2, v=20).send()
        Tele(id=1, v=11).send()       # overwrites id=1

        cache = get_retention_cache(Tele, session)
        assert cache.size == 2
        assert 'ret/basic/1' in cache._cache
        assert 'ret/basic/2' in cache._cache

    def test_retain_false_on_retained_class_bypasses_cache(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/nocache/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=1, v=1).send(retain=False)

        cache = get_retention_cache(Tele, session)
        assert cache.size == 0

    def test_retain_true_on_non_retained_class_raises(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'ret/plain/{id}'
            # RETAINED defaults to False
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with pytest.raises(z.TopicError, match='RETAINED = True'):
            Plain(id=1, v=1).send(retain=True)

    def test_non_retained_class_pays_zero_overhead(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'ret/zeroOver/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Plain(id=1, v=1).send()  # no cache interaction
        assert (Plain, id(session)) not in _registry


class TestQueryableDeclaredLazily:
    def test_no_queryable_until_first_retained_send(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/lazy/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session

        # Touch the cache without storing — no queryable yet.
        cache = get_retention_cache(Tele, session)
        assert cache._queryables == []

        Tele(id=1, v=1).send()
        assert len(cache._queryables) == 1   # one template

    def test_queryable_count_equals_template_count(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'ret/tmpl/{id}/a'
            EXTRA_TOPICS = ('ret/tmpl/{id}/b',)
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Status(id=1, v=1).send()

        cache = get_retention_cache(Status, session)
        assert len(cache._queryables) == 2   # canonical + one extra


class TestRetentionTTL:
    """Pin: ``RETENTION_TTL`` opts the class into lazy time-based
    expiration — entries older than the TTL are skipped (and pruned)
    at query time. No background thread; expiration is bounded by
    query frequency."""

    def test_ttl_none_keeps_entries_forever(self, session):
        from zeared.retention import get_retention_cache

        @z.zeared
        class M(z.Message):
            TOPIC = 'ttl/none/{n}'
            RETAINED = True
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        M(n=1, v='one').send()
        cache = get_retention_cache(M, session)
        assert 'ttl/none/1' in cache._cache

        # Time passes; entry stays.
        wait(0.1)
        assert 'ttl/none/1' in cache._cache

    def test_ttl_expires_on_query(self, session):
        """A query after TTL elapses prunes the expired entry inline."""
        from zeared.retention import get_retention_cache

        @z.zeared
        class M(z.Message):
            TOPIC = 'ttl/exp/{n}'
            RETAINED = True
            RETENTION_TTL = 0.05      # 50 ms
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        M(n=1, v='hi').send()
        cache = get_retention_cache(M, session)
        assert 'ttl/exp/1' in cache._cache

        # Wait past the TTL.
        wait(0.1)

        # Trigger _handle_query via session.get on the wildcard.
        replies = list(session.get('ttl/exp/**'))
        # Expired entry was pruned — cache no longer holds it.
        assert 'ttl/exp/1' not in cache._cache
        # No payload-bearing replies for the pruned topic.
        ok_keys = [
            str(r.ok.key_expr) for r in replies if hasattr(r, 'ok') and r.ok
        ]
        assert 'ttl/exp/1' not in ok_keys

    def test_ttl_keeps_fresh_entries(self, session):
        from zeared.retention import get_retention_cache

        @z.zeared
        class M(z.Message):
            TOPIC = 'ttl/fresh/{n}'
            RETAINED = True
            RETENTION_TTL = 5.0       # 5 seconds — well past any test runtime
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        M(n=1, v='hi').send()
        cache = get_retention_cache(M, session)

        # Trigger a query before TTL elapses.
        list(session.get('ttl/fresh/**'))
        # Entry survives.
        assert 'ttl/fresh/1' in cache._cache


class TestSessionRetentionTTL:
    """Pin: session-level ``retention_ttl`` factory kwarg sets a TTL
    fallback for classes that don't declare their own
    ``RETENTION_TTL``. Class-level always wins; session-level fills in
    when class is ``None``."""

    def test_factory_kwarg_stashed_on_managed(self):
        sess = z.peer(
            auto_reconnect=True, probe_interval=0,
            retention_ttl=42.0,
        )
        try:
            assert sess._retention_ttl == 42.0
        finally:
            z.release(session=sess)

    def test_factory_kwarg_default_is_none(self):
        sess = z.peer(auto_reconnect=True, probe_interval=0)
        try:
            assert sess._retention_ttl is None
        finally:
            z.release(session=sess)

    def test_factory_kwarg_on_raw_session_raises_typeerror(self):
        with pytest.raises(TypeError, match='retention_ttl'):
            z.peer(retention_ttl=10.0)            # auto_reconnect=False (default)

    def test_resolve_class_wins_over_session(self):
        from zeared.retention import _resolve_retention_ttl

        class StubSess:
            _retention_ttl = 100.0

        @z.zeared
        class Cls(z.Message):
            TOPIC = 'res/cls/{n}'
            RETENTION_TTL = 5.0
            n: int = z.Int(required=True)

        # Class wins — 5.0, not 100.0.
        assert _resolve_retention_ttl(Cls, StubSess()) == 5.0

    def test_resolve_session_used_when_class_is_none(self):
        from zeared.retention import _resolve_retention_ttl

        class StubSess:
            _retention_ttl = 30.0

        @z.zeared
        class Cls(z.Message):
            TOPIC = 'res/sess/{n}'
            # No RETENTION_TTL — falls through to session.
            n: int = z.Int(required=True)

        assert _resolve_retention_ttl(Cls, StubSess()) == 30.0

    def test_resolve_both_none_means_no_expiration(self):
        from zeared.retention import _resolve_retention_ttl

        class StubSess:
            _retention_ttl = None

        @z.zeared
        class Cls(z.Message):
            TOPIC = 'res/none/{n}'
            n: int = z.Int(required=True)

        assert _resolve_retention_ttl(Cls, StubSess()) is None

    def test_resolve_runtime_tunable(self):
        """Pin: mutating ``managed._retention_ttl`` post-construction
        propagates on the next resolve call (read at iteration, not
        captured at session-open)."""
        from zeared.retention import _resolve_retention_ttl

        sess = z.peer(auto_reconnect=True, probe_interval=0, retention_ttl=10.0)
        try:
            @z.zeared
            class Cls(z.Message):
                TOPIC = 'res/runtime/{n}'
                n: int = z.Int(required=True)

            assert _resolve_retention_ttl(Cls, sess) == 10.0
            sess._retention_ttl = 50.0
            assert _resolve_retention_ttl(Cls, sess) == 50.0
        finally:
            z.release(session=sess)

    def test_session_ttl_expires_unconfigured_class(self):
        """End-to-end pin: a class without ``RETENTION_TTL`` set
        respects the session-level fallback when published via a
        ``ManagedSession``, expiring values on query after the session
        TTL elapses."""
        from zeared.retention import get_retention_cache

        @z.zeared
        class M(z.Message):
            TOPIC = 'res/sess-ttl/{n}'
            RETAINED = True
            # No class-level RETENTION_TTL — relies on session fallback.
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        sess = z.peer(
            auto_reconnect=True, probe_interval=0,
            retention_ttl=0.05,
        )
        try:
            M(n=1, v='hi').send(session=sess)
            cache = get_retention_cache(M, sess)
            assert 'res/sess-ttl/1' in cache._cache

            wait(0.1)
            list(sess.get('res/sess-ttl/**'))
            # Expired via session-level TTL despite no class-level
            # ``RETENTION_TTL``.
            assert 'res/sess-ttl/1' not in cache._cache
        finally:
            z.release(session=sess)


class TestRedeclareRaceFlag:
    """Pin: ``_redeclaring`` flag closes the rebuild-vs-store() race.
    A concurrent ``store()`` while ``_redeclare_queryables`` is mid-flight
    must NOT enter ``_ensure_queryables`` declaration path — otherwise
    duplicate queryables answer the same query."""

    def test_ensure_queryables_skips_when_redeclaring(self, session):
        from zeared.retention import get_retention_cache

        @z.zeared
        class Reg(z.Message):
            TOPIC = 'race/{n}'
            RETAINED = True
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        Reg(n=1, v='one').send()

        cache = get_retention_cache(Reg, session)
        # Manually set the flag — simulates being mid-redeclare.
        cache._redeclaring = True
        # Drop the existing queryables so _ensure_queryables's first
        # short-circuit (queryables non-empty) doesn't fire and we
        # exercise the _redeclaring guard directly.
        for q in cache._queryables:
            try:
                q.undeclare()
            except Exception:
                pass
        cache._queryables = []

        # _ensure_queryables should NOT redeclare while flag is set.
        cache._ensure_queryables()
        assert cache._queryables == [], (
            'redeclaring flag failed to gate _ensure_queryables — '
            'concurrent store() would race the redeclare path and '
            'double-install queryables'
        )

        # Clear the flag — next call should declare normally.
        cache._redeclaring = False
        cache._ensure_queryables()
        assert len(cache._queryables) >= 1

    def test_redeclare_clears_flag_on_exception(self, session):
        """Pin: the try/finally clears _redeclaring even when the
        re-declare itself raises. Without the finally, an exception
        would leave the cache stuck-on, deadlocking subsequent store()
        calls into never declaring."""
        from zeared.retention import get_retention_cache

        @z.zeared
        class Reg(z.Message):
            TOPIC = 'race/exc/{n}'
            RETAINED = True
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        Reg(n=1, v='one').send()

        cache = get_retention_cache(Reg, session)
        # Force a crash inside the redeclare body by patching
        # zeared.retention.resolve_raw to return an object whose
        # declare_queryable raises. Restored after the test.
        from unittest.mock import patch

        class _BoomSess:
            def declare_queryable(self, *a, **kw):
                raise RuntimeError('simulated declare failure')

        with patch('zeared.retention.resolve_raw', return_value=_BoomSess()):
            with pytest.raises(Exception):
                cache._redeclare_queryables()

        # Flag must be cleared despite the raise.
        assert cache._redeclaring is False, (
            '_redeclaring flag stuck on after redeclare exception — '
            'try/finally regression'
        )


class TestQueryAnswersCachedValues:
    def test_session_get_returns_cached_payload(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/query/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=1, v=10).send()
        Tele(id=2, v=20).send()

        replies = list(session.get('ret/query/**'))
        assert len(replies) == 2
        # Each reply should decode back to one of our stored messages.
        keys = sorted(str(r.ok.key_expr) for r in replies if r.ok is not None)
        assert keys == ['ret/query/1', 'ret/query/2']


class TestClearRetentionCache:
    def test_clear_all_undeclares(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/clearall/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=1, v=1).send()
        assert (Tele, id(session)) in _registry

        clear_retention_cache()
        assert (Tele, id(session)) not in _registry

    def test_clear_by_session(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/clearbys/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        Tele(id=1, v=1).send(session=session_a)
        Tele(id=2, v=2).send(session=session_b)

        clear_retention_cache(session=session_a)
        assert (Tele, id(session_a)) not in _registry
        assert (Tele, id(session_b)) in _registry


class TestUnretain:
    def test_instance_form_removes_cache_entry(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/unr/inst/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=1, v=1).send()
        Tele(id=2, v=2).send()
        assert get_retention_cache(Tele, session).size == 2

        Tele(id=1, v=1).unretain()
        cache = get_retention_cache(Tele, session)
        assert cache.size == 1
        assert 'ret/unr/inst/2' in cache._cache
        assert 'ret/unr/inst/1' not in cache._cache

    def test_class_form_via_kwargs(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/unr/cls/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=5, v=50).send()
        assert get_retention_cache(Tele, session).size == 1

        Tele.unretain(id=5)
        assert get_retention_cache(Tele, session).size == 0

    def test_class_form_missing_key_raises(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/unr/missing/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with pytest.raises(z.TopicError, match='missing field'):
            Tele.unretain()  # no id=

    def test_unretain_on_non_retained_class_raises(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'ret/unr/noretain/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with pytest.raises(z.TopicError, match='RETAINED = True'):
            Plain(id=1, v=1).unretain()

        with pytest.raises(z.TopicError, match='RETAINED = True'):
            Plain.unretain(id=1)

    def test_unretain_in_batch_deferred(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/unr/batch/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        Tele(id=1, v=1).send()
        assert get_retention_cache(Tele, session).size == 1

        with z.batch():
            Tele.unretain(id=1)
            # Deferred — cache still populated mid-batch.
            assert get_retention_cache(Tele, session).size == 1

        # Flushed at __exit__.
        assert get_retention_cache(Tele, session).size == 0


class TestLateSubscribeFetch:
    def test_late_subscriber_receives_cached_value(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/late/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        # Publisher session A sends and caches.
        Tele(id=1, v=100).send(session=session_a)
        Tele(id=2, v=200).send(session=session_a)
        wait()

        # Subscriber on session B joins AFTER the publishes — should still
        # receive the cached values via retained-fetch.
        received: list[tuple[int, int]] = []
        sub = Tele.on_message(
            lambda m: received.append((m.id, m.v)),
            session=session_b,
        )
        wait(0.3)
        sub.close()

        assert sorted(received) == [(1, 100), (2, 200)]

    def test_retain_false_publish_not_visible_to_late_subscriber(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/nolate/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        # Publish without retention (retain=False) — goes live but isn't cached.
        Tele(id=1, v=10).send(session=session_a, retain=False)
        wait()

        received: list = []
        sub = Tele.on_message(received.append, session=session_b)
        wait(0.3)
        sub.close()

        # Late subscriber saw nothing retained AND missed the live send.
        assert received == []

    def test_tombstone_silences_topic(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/tomb/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        Tele(id=1, v=50).send(session=session_a)
        wait()
        Tele.unretain(id=1, session=session_a)
        wait()

        received: list = []
        sub = Tele.on_message(received.append, session=session_b)
        wait(0.3)
        sub.close()

        # Nothing cached anymore — late subscriber sees nothing.
        assert received == []

    def test_non_retained_class_skips_fetch(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'ret/skipfetch/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        # No retention machinery — sanity check no query is issued.
        received: list = []
        sub = Plain.on_message(received.append)
        wait()

        Plain(id=1, v=1).send()
        wait()
        sub.close()

        assert [m.id for m in received] == [1]

    def test_retained_with_extra_topics(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'ret/multi/robot/{id}/status'
            EXTRA_TOPICS = ('ret/multi/vehicle/{id}/status',)
            RETAINED = True
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        Status(id=1, status='robot-a').send(session=session_a)
        Status(id=2, status='veh-a').send(
            session=session_a, topic='ret/multi/vehicle/{id}/status',
        )
        wait()

        received: list[tuple[int, str]] = []
        sub = Status.on_message(
            lambda m, meta: received.append((m.id, m.status, meta.key_expr)),
            session=session_b,
        )
        wait(0.3)
        sub.close()

        key_exprs = sorted(r[2] for r in received)
        assert 'ret/multi/robot/1/status' in key_exprs
        assert 'ret/multi/vehicle/2/status' in key_exprs


class TestBatchWithRetention:
    def test_retained_sends_inside_batch_hit_cache_at_flush(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/batch/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with z.batch():
            Tele(id=1, v=1).send()
            Tele(id=2, v=2).send()
            # Cache empty mid-batch
            assert get_retention_cache(Tele, session).size == 0

        assert get_retention_cache(Tele, session).size == 2

    def test_batch_exception_discards_retention_writes(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'ret/batch_exc/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with pytest.raises(RuntimeError):
            with z.batch():
                Tele(id=1, v=1).send()
                Tele(id=2, v=2).send()
                raise RuntimeError('boom')

        # No queryable should have been declared either.
        assert (Tele, id(session)) not in _registry
