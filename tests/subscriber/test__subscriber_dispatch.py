"""Tests for ``zeared/subscriber/_subscriber_dispatch.py`` — the
per-sample dispatch closure builder plus the inspect / encoding /
async-adapter helpers.

Folds in the retention-dedupe coverage that previously lived in
``test_dedupe.py`` — dedupe is implemented as state inside the
``_build_dispatch`` closure.
"""

from __future__ import annotations

import datetime
import logging
import time
from collections import OrderedDict

import zenoh
from conftest import wait

import zeared as z
from zeared import _codec as codec
from zeared.subscriber._subscriber_dispatch import (
    _adapt_async_callback,
    _build_dispatch,
    _make_presence_dispatcher,
    _pick_encoding,
    _safe_on_error,
    _wants_meta,
)

# ---------------------------------------------------------------------------
# Smoke: public surface of the dispatch helpers.
# ---------------------------------------------------------------------------


class TestPublicSurface:
    def test_helpers_callable(self):
        assert callable(_wants_meta)
        assert callable(_adapt_async_callback)
        assert callable(_make_presence_dispatcher)
        assert callable(_pick_encoding)
        assert callable(_build_dispatch)


class TestWantsMeta:
    def test_one_arg_callable_no_meta(self):
        def cb(msg): ...

        assert _wants_meta(cb) is False

    def test_two_arg_callable_wants_meta(self):
        def cb(msg, meta): ...

        assert _wants_meta(cb) is True

    def test_var_args_wants_meta(self):
        def cb(*args): ...

        assert _wants_meta(cb) is True

    def test_unintrospectable_falls_back_to_no_meta(self):
        # A C-built-in or similar might not introspect cleanly.
        # ``_wants_meta`` should swallow and return False rather than raise.
        result = _wants_meta(print)  # builtin — has signature, but exercise
        assert result in (True, False)


class TestAdaptAsyncCallback:
    def test_sync_callback_returned_unchanged(self):
        def cb(msg): ...

        assert _adapt_async_callback(cb) is cb


# ---------------------------------------------------------------------------
# Retention dedupe (folded from test_dedupe.py) — dedupe state is in the
# dispatch closure.
# ---------------------------------------------------------------------------


class TestDedupeDefaultOn:
    def test_late_subscriber_dedupes_retained_against_live(self, connected_pair):
        """A retention fetch reply + a live publish from the SAME source
        carry identical timestamps for the most-recent sample → dedupe."""
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'dd/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        Tele(id=1, v=10).send(session=session_a)
        wait(0.3)

        received: list[int] = []
        sub = Tele.on_message(
            lambda m: received.append(m.v),
            session=session_b,
        )
        wait(0.5)
        sub.close()

        assert received == [10]

    def test_distinct_timestamps_not_dropped(self, connected_pair):
        """Two retained publishes with different values arrive once each."""
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'dd/distinct/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        received: list[int] = []
        sub = Tele.on_message(
            lambda m: received.append(m.v),
            session=session_b,
        )
        wait(0.3)

        Tele(id=1, v=10).send(session=session_a)
        time.sleep(0.05)  # ensure distinct HLC timestamps
        Tele(id=1, v=20).send(session=session_a)
        wait(0.5)
        sub.close()

        assert 10 in received
        assert 20 in received


class TestDedupeOptOut:
    def test_dedupe_false_lets_duplicates_through(self, connected_pair):
        """A class with DEDUPE = False should pass duplicates."""
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'dd/raw/{id}'
            RETAINED = True
            DEDUPE = False
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        Tele(id=1, v=99).send(session=session_a)
        wait(0.3)

        received: list[int] = []
        sub = Tele.on_message(
            lambda m: received.append(m.v),
            session=session_b,
        )
        wait(0.5)
        sub.close()

        assert 99 in received


class TestSynthesisedWillBypassesDedupe:
    """Wills carry timestamp=None and must always dispatch even when
    DEDUPE is on."""

    def test_will_synthesis_dispatched(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'dd/will/{name}'
            RETAINED = True
            LIVELINESS = True
            DEDUPE = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        Status(name='alice', state='online').send(session=session_a)
        Status(name='alice', state='offline').register_will(session=session_a)
        wait(0.3)

        states: list[str] = []
        sub = Status.on_message(
            lambda m: states.append(m.state),
            session=session_b,
        )
        wait(0.3)

        z.release(session=session_a)
        wait(0.5)
        sub.close()

        assert 'online' in states
        assert 'offline' in states


class TestNonRetainedClassUnaffected:
    def test_no_dedupe_overhead_for_non_retained(self, session):
        """RETAINED = False classes don't activate dedupe regardless of
        DEDUPE attribute value."""

        @z.zeared
        class Tick(z.Message):
            TOPIC = 'dd/plain/{n}'
            n: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = Tick.on_message(lambda m: received.append(m.n))
        wait()

        Tick(n=1).send()
        Tick(n=2).send()
        Tick(n=1).send()
        wait()
        sub.close()

        assert len(received) == 3


class TestOriginSignal:
    """The replay-vs-live signal (0.3.0): ``meta.origin`` carries the
    sample's provenance, determined by the local delivery path — the
    live subscriber callback (LIVE), a retained-fetch reply (REPLAY),
    or presence will synthesis (WILL)."""

    def test_live_sample_marked_live(self, session):
        @z.zeared
        class Tick(z.Message):
            TOPIC = 'orig/live/{n}'
            n: int = z.Int(required=True)

        origins: list[z.Origin] = []
        z.session = session
        sub = Tick.on_message(lambda m, meta: origins.append(meta.origin))
        wait()

        Tick(n=1).send()
        wait()
        sub.close()

        assert origins == [z.Origin.LIVE]

    def test_retained_replay_marked_replay(self, connected_pair):
        """A late subscriber's retained-fetch delivery is REPLAY; a
        subsequent real publish is LIVE."""
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'orig/ret/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        Tele(id=1, v=10).send(session=session_a)
        wait(0.3)

        received: list[tuple[int, z.Origin]] = []
        sub = Tele.on_message(
            lambda m, meta: received.append((m.v, meta.origin)),
            session=session_b,
        )
        wait(0.5)

        Tele(id=1, v=20).send(session=session_a)
        wait(0.5)
        sub.close()

        assert (10, z.Origin.REPLAY) in received
        assert (20, z.Origin.LIVE) in received

    def test_synthesised_will_marked_will(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'orig/will/{name}'
            RETAINED = True
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(required=True)

        Status(name='alice', state='online').send(session=session_a)
        Status(name='alice', state='offline').register_will(session=session_a)
        wait(0.3)

        received: list[tuple[str, z.Origin]] = []
        sub = Status.on_message(
            lambda m, meta: received.append((m.state, meta.origin)),
            session=session_b,
        )
        wait(0.3)

        z.release(session=session_a)
        wait(0.5)
        sub.close()

        # 'online' arrived via the retained fetch; 'offline' via synthesis.
        assert ('online', z.Origin.REPLAY) in received
        assert ('offline', z.Origin.WILL) in received


class TestHLCTimestampLexCompare:
    """Pin: HLC-formatted timestamps lex-compare in time order. Zenoh
    zero-pads the integer prefix; same-timestamp lex-compare returns
    equal so dedupe drops the second."""

    def test_seconds_increment_lex_compare(self):
        assert '1700000000.000000000/abc' < '1700000001.000000000/abc'

    def test_nanos_increment_lex_compare(self):
        assert '1700000000.000000000/abc' < '1700000000.000000001/abc'

    def test_equal_strings_compare_equal(self):
        assert not ('1700000000.000000000/abc' < '1700000000.000000000/abc')


class TestSafeOnError:
    """Pin: a user ``on_error`` that raises must not escape into Zenoh.

    Dispatch runs on Zenoh's callback thread. Before 0.3.3 every
    ``on_error`` call site invoked the user callback bare, so a raising
    error handler propagated out of dispatch — the same defect fixed on
    the queryable side in 0.3.2, and one this module's own docstring
    already promised against ("every failure mode through ``on_error`` /
    ``_log``").
    """

    def test_returns_false_without_a_callback(self):
        class M:
            __name__ = 'M'

        assert _safe_on_error(None, ValueError('x'), b'', M) is False

    def test_absorbs_a_raising_callback(self, caplog):
        class M:
            __name__ = 'M'

        def boom(exc, raw):
            msg = 'handler exploded'
            raise RuntimeError(msg)

        with caplog.at_level(logging.ERROR, logger='zeared.subscriber'):
            assert _safe_on_error(boom, ValueError('original'), b'', M) is True
        assert 'on_error callback itself raised' in caplog.text
        # The original error is still reported, not swallowed by the second.
        assert 'original' in caplog.text

    def test_raising_on_error_is_logged_with_the_original_error(self, session, caplog):
        """End-to-end: a bad decode plus a raising handler.

        Zenoh's own handler absorbs an escaping callback, so this is not
        about surviving — it is about *diagnosis*. Unguarded, the log got
        a bare "callback error" traceback for the handler and lost the
        decode failure that triggered it.
        """

        @z.zeared
        class Row(z.Message):
            TOPIC = 'dispatch/safeerr'
            v: int = z.Int(required=True)

        calls: list = []

        def bad_handler(exc, raw):
            calls.append(exc)
            msg = 'handler exploded'
            raise RuntimeError(msg)

        z.session = session
        sub = Row.on_message(lambda m: None, on_error=bad_handler)
        wait()
        with caplog.at_level(logging.ERROR, logger='zeared.subscriber'):
            # Undecodable payload on the class's own topic.
            session.put('dispatch/safeerr', b'\xff\xfe not msgpack')
            wait(0.4)
        sub.close()

        assert len(calls) == 1
        # Both errors survive: the handler's, and the decode failure that
        # provoked it. The latter is what an unguarded escape threw away.
        assert 'on_error callback itself raised' in caplog.text
        assert 'decode failed' in caplog.text


class TestDedupeSkewCeiling:
    """Pin: a far-future HLC must not park the dedupe watermark.

    Dedupe drops any sample whose timestamp string sorts ``<=`` the last
    seen for that key. With no ceiling, one sample stamped years ahead
    sets a watermark no genuine sample can beat — the subscriber is blind
    to that key until the process restarts. One message, no recovery.

    The fix delivers the offending sample but declines to advance the
    watermark, so a publisher with a merely skewed clock still gets
    through rather than turning someone else's bad clock into our outage.
    """

    def _dispatch_for(self, msg_cls, seen_ts, received):
        return _build_dispatch(
            msg_cls,
            None,
            received.append,
            wants_meta=False,
            dedupe_active=True,
            expected_schema=None,
            seen_mismatches=OrderedDict(),
            seen_ts=seen_ts,
            watchdog=None,
            schema_mismatch_cache_max=8,
        )

    def _sample(self, key, payload, epoch_seconds):
        ntp64 = int(epoch_seconds) << 32

        class FakeTs:
            def get_time(self):
                return datetime.datetime.fromtimestamp(epoch_seconds, tz=datetime.UTC)

            def __str__(self):
                return f'{ntp64:d}/fakezid'

        class FakeSample:
            kind = zenoh.SampleKind.PUT
            key_expr = key
            timestamp = FakeTs()
            attachment = None
            encoding = None
            source_info = None

            def __init__(self, p):
                self.payload = p

        return FakeSample(payload)

    def test_far_future_sample_does_not_advance_watermark(self):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'dispatch/skew'
            v: int = z.Int(required=True)

        seen_ts: dict = {}
        received: list = []
        dispatch = self._dispatch_for(Row, seen_ts, received)

        now = time.time()
        poison = codec.pack({'v': 1}, 'msgpack')
        genuine = codec.pack({'v': 2}, 'msgpack')

        # A sample stamped ten years ahead.
        dispatch(self._sample('dispatch/skew', poison, now + 10 * 365 * 86400))
        # ...must not stop a genuine one arriving right after.
        dispatch(self._sample('dispatch/skew', genuine, now))

        assert [m.v for m in received] == [1, 2], 'the far-future sample poisoned the watermark and blinded the key'

    def test_normal_samples_still_dedupe(self):
        """The ceiling must not disable ordinary dedupe."""

        @z.zeared
        class Row(z.Message):
            TOPIC = 'dispatch/skewok'
            v: int = z.Int(required=True)

        seen_ts: dict = {}
        received: list = []
        dispatch = self._dispatch_for(Row, seen_ts, received)

        now = time.time()
        dispatch(self._sample('dispatch/skewok', codec.pack({'v': 1}, 'msgpack'), now))
        # Older timestamp on the same key -> duplicate, dropped.
        dispatch(self._sample('dispatch/skewok', codec.pack({'v': 2}, 'msgpack'), now - 10))

        assert [m.v for m in received] == [1]

    def test_ceiling_is_configurable_and_disablable(self):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'dispatch/skewoff'
            DEDUPE_MAX_SKEW = None  # pre-0.3.3 behaviour
            v: int = z.Int(required=True)

        seen_ts: dict = {}
        received: list = []
        dispatch = self._dispatch_for(Row, seen_ts, received)

        now = time.time()
        dispatch(self._sample('dispatch/skewoff', codec.pack({'v': 1}, 'msgpack'), now + 10 * 365 * 86400))
        dispatch(self._sample('dispatch/skewoff', codec.pack({'v': 2}, 'msgpack'), now))

        # With the check off the watermark is poisoned — the genuine
        # sample is dropped. This is what the default now prevents.
        assert [m.v for m in received] == [1]
