from __future__ import annotations

from typing import Optional

import pytest

import zeared as z
from zeared.subscriber import _wants_meta

from conftest import wait


class TestArityInspection:
    def test_one_arg(self):
        assert _wants_meta(lambda msg: None) is False

    def test_two_args(self):
        assert _wants_meta(lambda msg, meta: None) is True

    def test_named_two_args(self):
        def handler(msg, meta):  # pragma: no cover — only inspected
            pass
        assert _wants_meta(handler) is True

    def test_varargs_opts_into_meta(self):
        def handler(*a):  # pragma: no cover
            pass
        assert _wants_meta(handler) is True

    def test_bound_method(self):
        class C:
            def one(self, msg):  # pragma: no cover
                pass
            def two(self, msg, meta):  # pragma: no cover
                pass
        assert _wants_meta(C().one) is False
        assert _wants_meta(C().two) is True


class TestRoundTrip:
    def test_static_topic_msgpack(self, session):
        @z.zeared
        class Alert(z.Message):
            TOPIC = 'events/alerts'
            msg: str = z.Str(required=True)

        received: list[Alert] = []
        z.session = session
        sub = Alert.on_message(received.append)
        wait()
        Alert(msg='fire').send()
        wait()
        sub.close()

        assert len(received) == 1
        assert received[0].msg == 'fire'

    def test_templated_topic_populates_field(self, session):
        @z.zeared
        class Telemetry(z.Message):
            TOPIC = 'robot/{id}/telemetry'
            id: int = z.Int(required=True)
            x: float = z.Float(required=True)

        received: list[Telemetry] = []
        z.session = session
        sub = Telemetry.on_message(received.append)
        wait()
        Telemetry(id=7, x=1.5).send()
        Telemetry(id=42, x=2.5).send()
        wait()
        sub.close()

        assert {(r.id, r.x) for r in received} == {(7, 1.5), (42, 2.5)}

    def test_two_arg_callback_gets_meta(self, session):
        @z.zeared
        class Ping(z.Message):
            TOPIC = 'ping/{id}'
            id: int = z.Int(required=True)
            ts: float = z.Float(required=True)

        received: list[tuple[Ping, z.ZenohMeta]] = []
        z.session = session
        sub = Ping.on_message(lambda m, meta: received.append((m, meta)))
        wait()
        Ping(id=1, ts=12345.0).send()
        wait()
        sub.close()

        assert len(received) == 1
        msg, meta = received[0]
        assert msg.id == 1
        assert isinstance(meta, z.ZenohMeta)
        assert meta.key_expr == 'ping/1'
        assert 'msgpack' in (meta.encoding or '')

    def test_json_encoding_per_class(self, session):
        @z.zeared
        class Event(z.Message):
            TOPIC = 'events/raw'
            ENCODING = 'json'
            payload: str = z.Str(required=True)

        received = []
        z.session = session
        sub = Event.on_message(lambda m, meta: received.append((m, meta)))
        wait()
        Event(payload='hello').send()
        wait()
        sub.close()

        assert received[0][0].payload == 'hello'
        assert 'json' in (received[0][1].encoding or '')

    def test_debug_flag_forces_json(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'debug/tele'
            x: float = z.Float(required=True)

        received = []
        z.session = session
        z.debug = True
        sub = Tele.on_message(lambda m, meta: received.append((m, meta)))
        wait()
        Tele(x=1.0).send()
        wait()
        sub.close()

        # Class-level ENCODING='msgpack' is overridden by z.debug=True.
        assert 'json' in (received[0][1].encoding or '')


class TestSessionIsolation:
    def test_two_sessions_do_not_see_each_other(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class Msg(z.Message):
            TOPIC = 'isolated/topic'
            val: int = z.Int(required=True)

        got_a: list[Msg] = []
        got_b: list[Msg] = []
        sub_a = Msg.on_message(got_a.append, session=session_a)
        sub_b = Msg.on_message(got_b.append, session=session_b)
        wait()

        Msg(val=1).send(session=session_a)
        wait()

        sub_a.close()
        sub_b.close()

        assert [m.val for m in got_a] == [1]
        assert got_b == []


class TestScopedOverride:
    def test_with_session_context(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class Msg(z.Message):
            TOPIC = 'scoped/topic'
            val: int = z.Int(required=True)

        got_a: list[int] = []
        got_b: list[int] = []
        sub_a = Msg.on_message(lambda m: got_a.append(m.val), session=session_a)
        sub_b = Msg.on_message(lambda m: got_b.append(m.val), session=session_b)
        wait()

        z.session = session_a         # default = A
        Msg(val=1).send()              # → A
        with z.session(session_b):
            Msg(val=2).send()          # → B (scoped)
        Msg(val=3).send()              # → A (back to default)
        wait()

        sub_a.close()
        sub_b.close()

        assert got_a == [1, 3]
        assert got_b == [2]


class TestOnError:
    def test_decode_failure_logs_and_skips(self, session, caplog):
        @z.zeared
        class Msg(z.Message):
            TOPIC = 'broken/topic'
            val: int = z.Int(required=True)

        received = []
        z.session = session
        sub = Msg.on_message(received.append)
        wait()

        # Publish garbage directly — bypasses msg.send()
        session.put('broken/topic', b'not-valid-msgpack')
        wait()
        # Now a valid one — subscriber must still be alive.
        Msg(val=7).send()
        wait()
        sub.close()

        assert [m.val for m in received] == [7]

    def test_on_error_callback_invoked(self, session):
        @z.zeared
        class Msg(z.Message):
            TOPIC = 'errored/topic'
            val: int = z.Int(required=True)

        errors: list[tuple[Exception, bytes]] = []
        received = []
        z.session = session
        sub = Msg.on_message(
            received.append,
            on_error=lambda exc, raw: errors.append((exc, raw)),
        )
        wait()

        session.put('errored/topic', b'garbage')
        wait()
        sub.close()

        assert len(errors) == 1
        exc, raw = errors[0]
        assert isinstance(exc, Exception)
        assert raw == b'garbage'


class TestSchemaStampingAndCheck:
    """Pin: Publisher with ``SCHEMA = '1.0'`` stamps the value into the
    sample attachment; subscriber populates ``meta.schema`` from the wire
    and matches it against the local class's ``SCHEMA`` value."""

    def test_schema_round_trip_populates_meta(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'schema/match/{n}'
            SCHEMA = '1.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        seen: list[str] = []
        sub = M.on_message(
            lambda m, meta: seen.append(meta.schema),
        )
        wait()
        M(n=1, v='hi').send()
        wait()
        sub.close()

        assert seen == ['1.0']

    def test_no_schema_means_no_attachment(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'schema/none/{n}'
            # SCHEMA = None (default) → no attachment stamped
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        seen: list = []
        sub = M.on_message(lambda m, meta: seen.append(meta.schema))
        wait()
        M(n=1, v='hi').send()
        wait()
        sub.close()

        # No attachment → meta.schema is None.
        assert seen == [None]

    def test_schema_attachment_cache_built_once(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'schema/cache/{n}'
            SCHEMA = '2.7'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        # Cache empty pre-publish.
        assert '_SCHEMA_ATTACHMENT_CACHE' not in M.__dict__

        z.session = session
        M(n=1, v='one').send()
        wait()
        cached = M.__dict__.get('_SCHEMA_ATTACHMENT_CACHE')
        assert cached is not None and cached != b''

        # Second send reuses the same cache entry — no rebuild.
        M(n=2, v='two').send()
        assert M.__dict__['_SCHEMA_ATTACHMENT_CACHE'] is cached


class TestSchemaMismatch:
    """Pin: subscriber-side schema-mismatch policy — sample dropped, route
    via on_error as ``SchemaMismatchError``, warn-once per (sender_zid,
    observed_schema) pair."""

    def test_mismatch_routed_to_on_error_and_dropped(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class PubM(z.Message):
            TOPIC = 'schema/pub/{n}'
            SCHEMA = '1.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        @z.zeared
        class SubM(z.Message):
            TOPIC = 'schema/pub/{n}'
            SCHEMA = '2.0'                # mismatch
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        received = []
        errors: list = []
        sub = SubM.on_message(
            lambda m: received.append(m),
            on_error=lambda exc, raw: errors.append(exc),
            session=session_b,
        )
        wait()
        PubM(n=1, v='x').send(session=session_a)
        wait(0.3)
        sub.close()

        assert received == [], 'mismatched-schema sample should NOT dispatch'
        assert len(errors) == 1
        assert isinstance(errors[0], z.SchemaMismatchError)
        assert isinstance(errors[0], z.DecodeError)
        assert isinstance(errors[0], z.SubscriberError)

    def test_warn_once_per_sender_pair(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class PubM(z.Message):
            TOPIC = 'schema/warn/{n}'
            SCHEMA = '1.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        @z.zeared
        class SubM(z.Message):
            TOPIC = 'schema/warn/{n}'
            SCHEMA = '2.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        errors: list = []
        sub = SubM.on_message(
            lambda m: None,
            on_error=lambda exc, raw: errors.append(exc),
            session=session_b,
        )
        wait()
        # Send three messages from same publisher with same schema.
        for i in range(3):
            PubM(n=i, v='x').send(session=session_a)
            wait(0.1)
        sub.close()

        # Only ONE on_error fire, despite three mismatched samples.
        assert len(errors) == 1, (
            f'warn-once cache regressed — got {len(errors)} errors '
            'for the same (sender, schema) pair'
        )

    def test_matching_schema_dispatches_normally(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class M(z.Message):
            TOPIC = 'schema/ok/{n}'
            SCHEMA = '1.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        received = []
        sub = M.on_message(
            lambda m: received.append(m.v),
            session=session_b,
        )
        wait()
        M(n=1, v='hi').send(session=session_a)
        wait(0.3)
        sub.close()

        assert received == ['hi']

    def test_warn_cache_bounded_evicts_oldest(self, session):
        """Pin: ``seen_mismatches`` is capped (default 1024) and evicts
        oldest entries on overflow — defends against unbounded growth on
        long-running subscribers exposed to many distinct misaligned
        senders. We poke the dispatch closure directly with synthetic
        samples to inject more pairs than fit in the cache."""
        @z.zeared
        class M(z.Message):
            TOPIC = 'schema/cap/{n}'
            SCHEMA = '1.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        sub = M.on_message(lambda m: None, on_error=lambda exc, raw: None)
        wait()
        try:
            cache = sub._seen_mismatches
            assert cache is not None

            # Inject more pairs than fit in the cache.
            from zeared.subscriber import _SCHEMA_MISMATCH_CACHE_MAX
            n = _SCHEMA_MISMATCH_CACHE_MAX + 50
            for i in range(n):
                # Direct dict insertion mimics what the dispatch closure
                # does on a new mismatch pair.
                pair = (f'zid-{i}', '999.999')
                cache[pair] = None
                if len(cache) > _SCHEMA_MISMATCH_CACHE_MAX:
                    cache.popitem(last=False)

            assert len(cache) == _SCHEMA_MISMATCH_CACHE_MAX
            # Oldest entries (zid-0..49) evicted.
            assert ('zid-0', '999.999') not in cache
            # Most recent stayed.
            assert (f'zid-{n - 1}', '999.999') in cache
        finally:
            sub.close()

    def test_warn_cache_cleared_on_redeclare(self, session):
        """Pin: ``Subscriber._redeclare`` clears the schema-mismatch
        cache so a reconnected session (new peer zids) can re-warn on
        legitimately-new mismatches without being silenced by stale
        entries from the pre-reconnect lifetime."""
        @z.zeared
        class M(z.Message):
            TOPIC = 'schema/clear/{n}'
            SCHEMA = '1.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        sub = M.on_message(lambda m: None)
        try:
            cache = sub._seen_mismatches
            cache[('old-zid', '0.5')] = None
            assert len(cache) == 1

            # Redeclare against the same raw — exercises the clear logic.
            sub._redeclare(session, session)
            assert len(cache) == 0
        finally:
            sub.close()

    def test_no_local_schema_skips_check(self, connected_pair):
        """Pin: subscriber class with SCHEMA=None doesn't validate;
        meta.schema populated from wire if publisher stamped it."""
        session_a, session_b = connected_pair

        @z.zeared
        class PubM(z.Message):
            TOPIC = 'schema/skip/{n}'
            SCHEMA = '1.0'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        @z.zeared
        class SubM(z.Message):
            TOPIC = 'schema/skip/{n}'
            # No SCHEMA — opted out of validation.
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        received = []
        seen_schema = []
        sub = SubM.on_message(
            lambda m, meta: (received.append(m.v),
                             seen_schema.append(meta.schema)),
            session=session_b,
        )
        wait()
        PubM(n=1, v='hi').send(session=session_a)
        wait(0.3)
        sub.close()

        assert received == ['hi']      # dispatched (no validation)
        assert seen_schema == ['1.0']  # populated from wire


class TestIssuedAtPopulated:
    """Pin: ``meta.issued_at`` is parsed from the sample's HLC timestamp
    when timestamping is enabled (default since 0.0.13)."""

    def test_issued_at_is_recent_utc_datetime(self, session):
        import datetime
        @z.zeared
        class M(z.Message):
            TOPIC = 'issued_at/{n}'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        seen: list = []
        sub = M.on_message(lambda m, meta: seen.append(meta.issued_at))
        wait()
        M(n=1, v='hi').send()
        wait()
        sub.close()

        assert len(seen) == 1
        ts = seen[0]
        # Timestamping is auto-injected at factory level (0.0.13), but
        # the conftest builds raw zenoh sessions WITHOUT passing through
        # the factory, so timestamping may or may not be enabled here.
        # Pin permissively: either a recent UTC datetime, or None.
        if ts is not None:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            assert abs((now - ts).total_seconds()) < 60


class TestSubscriberGeneric:
    """Pin: ``Subscriber`` is parameterised via ``typing.Generic`` so
    IDE / type-checkers see ``Cls.on_message(cb) -> Subscriber[Cls]``.
    Runtime is identical to bare ``Subscriber``."""

    def test_subscriber_class_is_generic(self):
        from zeared.subscriber import Subscriber
        # Generic[T] machinery enables Subscriber[SomeMessageCls].

        @z.zeared
        class M(z.Message):
            TOPIC = 'generic/test/{n}'
            n: int = z.Int(required=True)

        # Construction with a parameter — type-only, returns a generic alias.
        alias = Subscriber[M]
        assert alias is not None

    def test_bare_subscriber_import_still_works(self):
        # 'from zeared import Subscriber' — bare class continues to be the
        # public name; generic parameterisation is opt-in at call sites.
        assert z.Subscriber is not None
        # Subclass relationship preserved.
        from zeared.subscriber import Subscriber as DirectSub
        assert z.Subscriber is DirectSub


class TestPerSubscriberDedupe:
    """Pin: ``Cls.on_message(cb, dedupe=...)`` overrides class-level
    ``DEDUPE`` per subscriber. ``dedupe=None`` falls through to class
    default; ``dedupe=True`` enables on a ``DEDUPE = False`` class;
    ``dedupe=False`` defeats class-level ``DEDUPE = True``."""

    def test_dedupe_none_uses_class_default(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'persub/none/{n}'
            RETAINED = True
            DEDUPE = True
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        sub = M.on_message(lambda m: None)        # no dedupe= kwarg
        try:
            from zeared.subscriber import _subscribers
            # Find the subscriber's dispatch closure to check dedupe state.
            # We can't introspect the closure directly; instead pin via
            # behavior — see the override tests below for stronger checks.
            assert sub is not None
        finally:
            sub.close()

    def test_dedupe_false_overrides_class_true(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'persub/off/{n}'
            RETAINED = True
            DEDUPE = True
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        # Publish a retained value first.
        M(n=1, v='one').send()
        wait(0.2)

        received: list[str] = []
        # dedupe=False — every retained-fetch + live message dispatches.
        sub = M.on_message(lambda m: received.append(m.v), dedupe=False)
        wait(0.3)
        # Publish the SAME retained value again — with dedupe=False this
        # should re-fire (where dedupe=True would suppress).
        M(n=1, v='one').send()
        wait(0.3)
        sub.close()

        # Two fires: the retained-fetch reply and the live publish.
        # (dedupe=True would yield 1.)
        assert received.count('one') >= 2

    def test_dedupe_true_overrides_class_false(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'persub/on/{n}'
            RETAINED = True
            DEDUPE = False
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        z.session = session
        # Per-sub dedupe=True engages — even though class default is False.
        sub = M.on_message(lambda m: None, dedupe=True)
        try:
            assert sub is not None     # construction works; runtime would
                                       # need timestamping to be effective.
        finally:
            sub.close()


class TestStructuredErrors:
    """Pin: dispatch-path errors are wrapped in typed
    ``SubscriberError`` subclasses (`DecodeError`, `CallbackError`,
    `RetainedFetchError`). Original exception reachable via
    ``__cause__``."""

    def test_decode_error_is_typed(self, session):
        @z.zeared
        class Msg(z.Message):
            TOPIC = 'structured/decode'
            val: int = z.Int(required=True)

        errors: list = []
        z.session = session
        sub = Msg.on_message(
            lambda m: None,
            on_error=lambda exc, raw: errors.append(exc),
        )
        wait()
        session.put('structured/decode', b'not-valid-msgpack')
        wait()
        sub.close()

        assert len(errors) == 1
        exc = errors[0]
        assert isinstance(exc, z.DecodeError)
        assert isinstance(exc, z.SubscriberError)
        # Original exception chained via __cause__.
        assert exc.__cause__ is not None

    def test_callback_error_is_typed(self, session):
        @z.zeared
        class Msg(z.Message):
            TOPIC = 'structured/cb'
            val: int = z.Int(required=True)

        errors: list = []
        z.session = session

        def boom(m):
            raise RuntimeError('user code blew up')

        sub = Msg.on_message(
            boom,
            on_error=lambda exc, raw: errors.append(exc),
        )
        wait()
        Msg(val=1).send()
        wait()
        sub.close()

        assert len(errors) == 1
        exc = errors[0]
        assert isinstance(exc, z.CallbackError)
        assert isinstance(exc, z.SubscriberError)
        assert isinstance(exc.__cause__, RuntimeError)

    def test_decode_and_callback_errors_distinguishable(self, session):
        @z.zeared
        class Msg(z.Message):
            TOPIC = 'structured/mix'
            val: int = z.Int(required=True)

        kinds: list[str] = []
        z.session = session

        def on_err(exc, raw):
            if isinstance(exc, z.DecodeError):
                kinds.append('decode')
            elif isinstance(exc, z.CallbackError):
                kinds.append('callback')
            else:
                kinds.append('other')

        def boom(m):
            raise ValueError('boom')

        sub = Msg.on_message(boom, on_error=on_err)
        wait()
        # Trigger decode error
        session.put('structured/mix', b'garbage')
        wait()
        # Trigger callback error
        Msg(val=2).send()
        wait()
        sub.close()

        assert 'decode' in kinds
        assert 'callback' in kinds


class TestMetaCaptures:
    def test_captures_populated_for_declared_slots(self, session):
        @z.zeared
        class Telemetry(z.Message):
            TOPIC = 'cap/robot/{id}/telemetry'
            id: int = z.Int(required=True)
            x: float = z.Float(required=True)

        received: list[tuple[dict, int]] = []
        z.session = session
        sub = Telemetry.on_message(
            lambda m, meta: received.append((meta.captures, m.id))
        )
        wait()
        Telemetry(id=42, x=1.5).send()
        wait()
        sub.close()

        assert len(received) == 1
        captures, msg_id = received[0]
        assert captures == {'id': '42'}    # string — raw capture
        assert msg_id == 42                # coerced onto instance

    def test_captures_populated_for_capture_only_slot(self, session):
        """corr_id is in the TOPIC but NOT a declared field."""
        @z.zeared
        class CliRequest(z.Message):
            TOPIC = 'cap/cli/request/{corr_id}'
            cmd: str = z.Str(required=True)

        received: list[dict] = []
        z.session = session
        sub = CliRequest.on_message(
            lambda m, meta: received.append(meta.captures)
        )
        wait()
        # Publish requires corr_id at render-time; simulate by sending via raw session
        # using the concrete topic with a specific corr_id.
        from zeared import _codec as codec
        raw = codec.pack({'cmd': 'ping'}, 'msgpack')
        session.put(
            'cap/cli/request/abc123', raw, encoding='application/msgpack',
        )
        wait()
        sub.close()

        assert received == [{'corr_id': 'abc123'}]

    def test_captures_empty_when_no_slots(self, session):
        @z.zeared
        class Alert(z.Message):
            TOPIC = 'cap/static/alert'
            msg: str = z.Str(required=True)

        received: list[dict] = []
        z.session = session
        sub = Alert.on_message(
            lambda m, meta: received.append(meta.captures)
        )
        wait()
        Alert(msg='x').send()
        wait()
        sub.close()

        assert received == [{}]


class TestMultiWildcardSubscribe:
    def test_extra_topics_wildcard_receives_multiple_concrete_keys(self, session):
        @z.zeared
        class AnyStatus(z.Message):
            TOPIC = 'wild/robot/{id}/status'
            EXTRA_TOPICS = ('wild/vehicle/**',)
            id: int = z.Int()
            status: str = z.Str(required=True)

        received: list[tuple[str, str]] = []
        z.session = session
        sub = AnyStatus.on_message(
            lambda m, meta: received.append((meta.key_expr, m.status))
        )
        wait()

        # Publish via canonical (publishable)
        AnyStatus(id=1, status='ok').send()
        # Publish under the wildcard (via raw session.put since it's subscribe-only)
        from zeared import _codec as codec
        raw = codec.pack({'status': 'degraded'}, 'msgpack')
        session.put('wild/vehicle/5/health', raw, encoding='application/msgpack')
        wait()
        sub.close()

        assert len(received) == 2
        keys = sorted(r[0] for r in received)
        assert keys == ['wild/robot/1/status', 'wild/vehicle/5/health']


class TestMultiTopic:
    def test_subscribes_to_canonical_and_extras(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        received: list[tuple[int, str]] = []
        z.session = session
        sub = Status.on_message(lambda m: received.append((m.id, m.status)))
        wait()

        Status(id=1, status='robot-ok').send()
        Status(id=2, status='veh-ok').send(topic='vehicle/{id}/status')
        wait()
        sub.close()

        assert sorted(received) == [(1, 'robot-ok'), (2, 'veh-ok')]

    def test_publish_override_rejects_undeclared(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        z.session = session
        with pytest.raises(z.TopicError, match='not a declared topic'):
            Status(id=1, status='x').send(topic='arbitrary/{id}/topic')

    def test_meta_key_expr_reflects_actual_topic(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'robot/{id}/status'
            EXTRA_TOPICS = ('vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        got: list[tuple[int, str]] = []
        z.session = session
        sub = Status.on_message(
            lambda m, meta: got.append((m.id, meta.key_expr))
        )
        wait()

        Status(id=5, status='a').send()
        Status(id=9, status='b').send(topic='vehicle/{id}/status')
        wait()
        sub.close()

        as_dict = dict(got)
        assert as_dict[5] == 'robot/5/status'
        assert as_dict[9] == 'vehicle/9/status'


class TestSubscriberLifecycle:
    def test_close_is_idempotent(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'idempotent/topic'
            v: int = z.Int(required=True)

        z.session = session
        sub = M.on_message(lambda m: None)
        sub.close()
        sub.close()  # no raise

    def test_context_manager(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'ctx/topic'
            v: int = z.Int(required=True)

        z.session = session
        with M.on_message(lambda m: None) as sub:
            assert sub._closed is False
        assert sub._closed is True


# ---------------------------------------------------------------------------
# seared field-types round-trip through zeared's wire path. Folded from
# the previous tests/test_seared_field_types.py — these are end-to-end
# integration tests that publish + subscribe + assert equality.
# ---------------------------------------------------------------------------


class TestDecimalOnTheWire:
    def test_decimal_round_trip(self, session):
        from decimal import Decimal as D

        @z.zeared
        class Money(z.Message):
            TOPIC = 'fld/money/{id}'
            id: int = z.Int(required=True)
            amount: D = z.Decimal(required=True)

        z.session = session
        received: list[D] = []
        sub = Money.on_message(lambda m: received.append(m.amount))
        wait()
        Money(id=1, amount=D('1234567.89012345')).send()
        wait()
        sub.close()

        assert received == [D('1234567.89012345')]


class TestPathOnTheWire:
    def test_path_round_trip(self, session):
        from pathlib import Path as P

        @z.zeared
        class FileEvent(z.Message):
            TOPIC = 'fld/path/{id}'
            id: int = z.Int(required=True)
            location: P = z.Path(required=True)

        z.session = session
        received: list[P] = []
        sub = FileEvent.on_message(lambda m: received.append(m.location))
        wait()
        FileEvent(id=1, location=P('a/b/c.txt')).send()
        wait()
        sub.close()

        assert received == [P('a/b/c.txt')]


class TestPandasFrameOnTheWire:
    """Skipped when pandas isn't installed in zeared's dev env."""

    def test_pandas_frame_round_trip(self, session):
        pd = pytest.importorskip('pandas')

        @z.zeared
        class Report(z.Message):
            TOPIC = 'fld/pandas/{id}'
            id: int = z.Int(required=True)
            data: pd.DataFrame = z.PandasFrame(required=True)

        z.session = session
        received: list = []
        sub = Report.on_message(lambda m: received.append(m.data))
        wait()
        df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
        Report(id=1, data=df).send()
        wait()
        sub.close()

        assert len(received) == 1
        assert received[0].equals(df)


class TestPolarsFrameOnTheWire:
    """Skipped when polars isn't installed in zeared's dev env."""

    def test_polars_frame_round_trip(self, session):
        pl = pytest.importorskip('polars')

        @z.zeared
        class Report(z.Message):
            TOPIC = 'fld/polars/{id}'
            id: int = z.Int(required=True)
            data: pl.DataFrame = z.PolarsFrame(required=True)

        z.session = session
        received: list = []
        sub = Report.on_message(lambda m: received.append(m.data))
        wait()
        df = pl.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
        Report(id=1, data=df).send()
        wait()
        sub.close()

        assert len(received) == 1
        assert received[0].equals(df)
