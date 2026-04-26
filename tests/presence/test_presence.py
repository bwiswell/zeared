from __future__ import annotations

import time

import pytest

import zeared as z
from zeared.presence import (
    ALIVE_PREFIX,
    WILL_PREFIX,
    _slug,
    _WillEnvelope,
    get_observer,
    get_presence,
)

from conftest import wait


# ---------------------------------------------------------------------------
# Unit-ish: presence state internals
# ---------------------------------------------------------------------------


class TestSlugDeterminism:
    def test_same_inputs_produce_same_slug(self):
        a = _slug('Foo.Bar', 'topic/one')
        b = _slug('Foo.Bar', 'topic/one')
        assert a == b

    def test_different_inputs_produce_different_slugs(self):
        a = _slug('Foo.Bar', 'topic/one')
        b = _slug('Foo.Bar', 'topic/two')
        c = _slug('Foo.Baz', 'topic/one')
        assert len({a, b, c}) == 3

    def test_slug_is_short_hex(self):
        s = _slug('x', 'y')
        assert len(s) == 16
        assert all(c in '0123456789abcdef' for c in s)


class TestRegisterWillRequiresLIVELINESS:
    def test_non_liveliness_class_raises(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'pres/nope/{name}'
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        z.session = session
        with pytest.raises(z.TopicError, match='LIVELINESS = True'):
            Plain(name='a', state='off').register_will()


class TestRegisterWillPublishesRetainedEnvelope:
    def test_will_reachable_via_session_get(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/status/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        z.session = session
        Status(name='alice', state='offline').register_will()
        wait()

        # The will envelope should be fetchable via session.get(__zeared/will/**).
        replies = list(session.get(f'{WILL_PREFIX}/**'))
        assert len(replies) >= 1
        # Check the envelope decodes and points at the right target.
        from zeared import _codec as codec
        found = False
        for r in replies:
            ok = getattr(r, 'ok', None)
            if ok is None:
                continue
            env_dict = codec.unpack(bytes(ok.payload), 'msgpack')
            env = _WillEnvelope.load(env_dict)
            if env.target_key_expr == 'pres/status/alice':
                assert env.encoding == 'msgpack'
                found = True
                break
        assert found

    def test_liveliness_token_declared(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/token/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        z.session = session
        Status(name='alice', state='offline').register_will()
        wait()

        # A liveliness subscriber with history=True should see the token.
        seen: list = []

        def on_alive(sample):
            seen.append(str(sample.key_expr))

        sub = session.liveliness().declare_subscriber(
            f'{ALIVE_PREFIX}/**', on_alive, history=True,
        )
        wait()
        sub.undeclare()

        assert any(k == f'{ALIVE_PREFIX}/{session.zid()}' for k in seen)


# ---------------------------------------------------------------------------
# Integration: will fires on peer session close
# ---------------------------------------------------------------------------


class TestWillFiresOnPeerClose:
    def test_subscriber_receives_synthesised_will(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/close/{name}'
            # RETAINED so the late subscriber sees the 'online' state too.
            RETAINED = True
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        # Producer on A: publish a retained online state and register a will.
        Status(name='alice', state='online').send(session=session_a)
        Status(name='alice', state='offline').register_will(session=session_a)
        wait(0.3)

        # Subscriber on B — joins after the publish + will.
        received: list[tuple[str, str]] = []
        sub = Status.on_message(
            lambda m: received.append((m.name, m.state)),
            session=session_b,
        )
        wait(0.5)

        # Retained 'online' state should have been fetched.
        assert ('alice', 'online') in received

        # Producer session closes → will fires on B.
        session_a.close()
        wait(0.5)

        sub.close()

        # The offline will should have been synthesised on B.
        assert ('alice', 'offline') in received

    def test_late_subscriber_still_receives_will(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/late/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        # Producer registers will BEFORE subscriber exists.
        Status(name='bob', state='offline').register_will(session=session_a)
        wait(0.3)

        # Subscriber joins AFTER the will was registered.
        received: list[tuple[str, str]] = []
        sub = Status.on_message(
            lambda m: received.append((m.name, m.state)),
            session=session_b,
        )
        wait(0.3)

        # Producer dies.
        session_a.close()
        wait(0.5)
        sub.close()

        assert ('bob', 'offline') in received


class TestMultipleWillsPerSession:
    def test_three_wills_all_fire(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class PeerStatus(z.Message):
            TOPIC = 'pres/multi/status/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        @z.zeared
        class Registry(z.Message):
            TOPIC = 'pres/multi/registry/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            detail: str = z.Str(missing='')

        PeerStatus(name='alice', state='offline').register_will(session=session_a)
        Registry(name='alice', detail='gone').register_will(session=session_a)
        wait(0.3)

        got_status = []
        got_registry = []
        sub1 = PeerStatus.on_message(
            lambda m: got_status.append((m.name, m.state)),
            session=session_b,
        )
        sub2 = Registry.on_message(
            lambda m: got_registry.append((m.name, m.detail)),
            session=session_b,
        )
        wait(0.3)

        session_a.close()
        wait(0.5)
        sub1.close()
        sub2.close()

        assert ('alice', 'offline') in got_status
        assert ('alice', 'gone') in got_registry


class TestWillWithMetaCaptures:
    def test_synthesised_sample_populates_meta_captures(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/meta/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        Status(name='charlie', state='offline').register_will(session=session_a)
        wait(0.3)

        captures_seen: list[dict] = []
        sub = Status.on_message(
            lambda m, meta: captures_seen.append(dict(meta.captures)),
            session=session_b,
        )
        wait(0.3)

        session_a.close()
        wait(0.5)
        sub.close()

        # At least one event with captures['name'] = 'charlie'
        assert any(c.get('name') == 'charlie' for c in captures_seen)


class TestWillBucketDelete:
    """Pin: explicit DELETE on a will key drops just that slug from the
    observer's bucket, not the whole bucket. (No public unregister_will
    API yet, so we exercise the path by directly poking at the
    observer's _on_will.)
    """
    def test_delete_drops_only_matching_slug(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/bucket/{name}'
            LIVELINESS = True
            name:  str = z.Str(required=True)
            state: str = z.Str(required=True)

        z.session = session
        # Register two wills for distinct concrete topics.
        Status(name='alice', state='offline').register_will()
        Status(name='bob', state='offline').register_will()
        wait(0.2)

        # Force the observer to start (LIVELINESS subscriber would do
        # this automatically; for the unit-style test we instantiate the
        # observer directly).
        from zeared.presence import _slug, get_observer
        observer = get_observer(session)
        observer.start()
        wait(0.3)   # initial fetch background thread populates the bucket

        peer_zid = str(session.zid())
        bucket = observer._wills_by_zid.get(peer_zid, {})
        # The local session's own wills get filtered out via _self_zid
        # check in _on_will. So we manually inject from the perspective
        # of a remote peer to test bucket drop.
        from zeared.presence import _WillEnvelope
        slug_a = _slug('Status', 'pres/bucket/alice')
        slug_b = _slug('Status', 'pres/bucket/bob')

        observer._wills_by_zid['fake_peer'] = {
            slug_a: _WillEnvelope(
                source_zid='fake_peer',
                target_key_expr='pres/bucket/alice',
                encoding='msgpack',
                payload=b'',
            ),
            slug_b: _WillEnvelope(
                source_zid='fake_peer',
                target_key_expr='pres/bucket/bob',
                encoding='msgpack',
                payload=b'',
            ),
        }

        # Synthesise a DELETE sample on the alice slug.
        import zenoh as _z

        class _DelSample:
            kind = _z.SampleKind.DELETE
            key_expr = f'__zeared/will/fake_peer/{slug_a}'
            payload = b''

        observer._on_will(_DelSample())

        # alice slug gone, bob slug remains.
        assert slug_a not in observer._wills_by_zid['fake_peer']
        assert slug_b in observer._wills_by_zid['fake_peer']


class TestOrphanedWillGC:
    """Pin: the GC daemon drops stash entries for peers no longer alive.

    Verifies the periodic sweep — without it, a missed liveliness DELETE
    (e.g. during a brief partition) would leak the entry forever.
    """
    def test_gc_drops_orphan(self, session):
        from zeared.presence import _WillEnvelope, get_observer

        observer = get_observer(session)
        # Tighten interval to keep the test fast.
        observer._gc_interval = 0.05
        observer.start()
        try:
            # Inject a stash entry for a peer that's NOT in _alive_zids.
            observer._wills_by_zid['ghost_peer'] = {
                'slug_x': _WillEnvelope(
                    source_zid='ghost_peer',
                    target_key_expr='gc/test/topic',
                    encoding='msgpack',
                    payload=b'',
                ),
            }
            # Inject one for a peer that IS alive — must be retained.
            observer._alive_zids.add('alive_peer')
            observer._wills_by_zid['alive_peer'] = {
                'slug_y': _WillEnvelope(
                    source_zid='alive_peer',
                    target_key_expr='gc/test/keep',
                    encoding='msgpack',
                    payload=b'',
                ),
            }
            wait(0.3)   # let GC sweep at least once

            assert 'ghost_peer' not in observer._wills_by_zid
            assert 'alive_peer' in observer._wills_by_zid
        finally:
            observer.stop()


class TestEnvelopeEncodingHonorsDebug:
    """Pin: when ``z.debug`` is True the will envelope wire encoding is JSON,
    and the subscriber decodes it correctly off ``sample.encoding``."""
    def test_envelope_is_json_under_debug(self, session):
        from zeared import _codec as codec

        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/debug/{name}'
            LIVELINESS = True
            name:  str = z.Str(required=True)
            state: str = z.Str(required=True)

        z.session = session
        prev_debug = z.debug
        z.debug = True
        try:
            Status(name='alice', state='offline').register_will()
            wait()

            replies = list(session.get(f'{WILL_PREFIX}/**'))
            assert replies
            decoded = False
            for r in replies:
                ok = getattr(r, 'ok', None)
                if ok is None:
                    continue
                # Wire encoding hint must signal JSON.
                assert 'json' in str(ok.encoding)
                env_dict = codec.unpack(bytes(ok.payload), 'json')
                env = _WillEnvelope.load(env_dict)
                if env.target_key_expr == 'pres/debug/alice':
                    decoded = True
                    break
            assert decoded
        finally:
            z.debug = prev_debug


class TestNoFalsePositive:
    def test_no_subscriber_event_without_peer_close(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'pres/nofalse/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        # Subscribe but don't close the peer.
        received: list = []
        sub = Status.on_message(
            lambda m: received.append((m.name, m.state)),
            session=session_b,
        )

        # Producer registers a will but stays alive.
        Status(name='dana', state='offline').register_will(session=session_a)
        wait(0.5)
        sub.close()

        # The will should NOT have fired — peer is still alive.
        assert received == []
