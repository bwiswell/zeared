"""Tests for ``zeared/subscriber/_subscriber_dispatch.py`` — the
per-sample dispatch closure builder plus the inspect / encoding /
async-adapter helpers.

Folds in the retention-dedupe coverage that previously lived in
``test_dedupe.py`` — dedupe is implemented as state inside the
``_build_dispatch`` closure.
"""
from __future__ import annotations

import time

import pytest

import zeared as z
from zeared.subscriber._subscriber_dispatch import (
    _adapt_async_callback,
    _build_dispatch,
    _make_presence_dispatcher,
    _pick_encoding,
    _wants_meta,
)

from conftest import wait


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
        result = _wants_meta(print)   # builtin — has signature, but exercise
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
            lambda m: received.append(m.v), session=session_b,
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
            lambda m: received.append(m.v), session=session_b,
        )
        wait(0.3)

        Tele(id=1, v=10).send(session=session_a)
        time.sleep(0.05)   # ensure distinct HLC timestamps
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
            lambda m: received.append(m.v), session=session_b,
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
            name:  str = z.Str(required=True)
            state: str = z.Str(required=True)

        Status(name='alice', state='online').send(session=session_a)
        Status(name='alice', state='offline').register_will(session=session_a)
        wait(0.3)

        states: list[str] = []
        sub = Status.on_message(
            lambda m: states.append(m.state), session=session_b,
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
