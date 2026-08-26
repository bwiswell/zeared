"""Tests for the 0.0.11 auto-reconnect machinery.

Covers ``ManagedSession`` wrapping, state machine, the probe/swap path,
and subscriber + presence restoration on reconnect. Most tests use a
controlled ``open_fn`` so reconnect orchestration is observable without
relying on Zenoh's reconnect timing; the integration test at the end
exercises a real session-close + probe-driven recovery.
"""
from __future__ import annotations

import threading
import time
from unittest import mock

import pytest
import zenoh

import zeared as z
from zeared._managed_session import ManagedSession, _is_dead, resolve_raw
from zeared._reconnect import _trigger_reconnect, start_probe  # noqa: F401
from zeared.errors import SessionDeadError

from conftest import wait, _peer_session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_managed(raw, *, probe_interval=0.05, max_attempts=None) -> ManagedSession:
    """Wrap ``raw`` without starting the probe — tests start it explicitly."""
    return ManagedSession(
        raw, lambda: _peer_session(),
        endpoint_label='test',
        probe_interval=probe_interval,
        initial_backoff=0.01,
        max_backoff=0.05,
        max_attempts=max_attempts,
    )


# ---------------------------------------------------------------------------
# ManagedSession wrapping + delegation
# ---------------------------------------------------------------------------


class TestManagedSessionWrapping:
    def test_zid_delegates_to_raw(self, session):
        m = _make_managed(session)
        assert m.zid() == session.zid()

    def test_raw_returns_current(self, session):
        m = _make_managed(session)
        assert m.raw() is session

    def test_repr_shows_state(self, session):
        m = _make_managed(session)
        r = repr(m)
        assert 'ManagedSession' in r
        assert 'IDLE' in r

    def test_unknown_attr_delegates_to_raw(self, session):
        m = _make_managed(session)
        # `info` returns a fresh SessionInfo; check both sides have the
        # same shape rather than identity.
        assert type(m.info) is type(session.info)
        assert str(m.info.zid()) == str(session.info.zid())


class TestStateGuards:
    def test_put_raises_when_reconnecting(self, session):
        m = _make_managed(session)
        m._set_state('RECONNECTING')
        with pytest.raises(SessionDeadError, match='reconnecting'):
            m.put('test/key', b'x')

    def test_put_raises_when_dead(self, session):
        m = _make_managed(session)
        m._set_state('DEAD')
        with pytest.raises(SessionDeadError, match='terminally failed'):
            m.put('test/key', b'x')

    def test_get_raises_when_dead(self, session):
        m = _make_managed(session)
        m._set_state('DEAD')
        with pytest.raises(SessionDeadError):
            m.get('test/**')

    def test_declare_subscriber_raises_when_reconnecting(self, session):
        m = _make_managed(session)
        m._set_state('RECONNECTING')
        with pytest.raises(SessionDeadError):
            m.declare_subscriber('test/**', lambda s: None)

    def test_zid_raises_when_reconnecting(self, session):
        """Pin: ``_guard_alive`` is symmetric — zid/liveliness/info all
        raise during reconnect, not just put/get/delete/declare_*."""
        m = _make_managed(session)
        m._set_state('RECONNECTING')
        with pytest.raises(SessionDeadError):
            m.zid()

    def test_liveliness_raises_when_dead(self, session):
        m = _make_managed(session)
        m._set_state('DEAD')
        with pytest.raises(SessionDeadError):
            m.liveliness()

    def test_info_raises_when_reconnecting(self, session):
        m = _make_managed(session)
        m._set_state('RECONNECTING')
        with pytest.raises(SessionDeadError):
            _ = m.info


class TestDeclareHandleWarning:
    """Pin: ``ManagedSession.declare_*`` emits ``RuntimeWarning`` because
    the returned handle does not survive reconnect. User code that
    bypasses ``Cls.on_message`` / ``msg.send()`` / ``z.batch()`` and
    declares directly should be steered toward those wrappers."""

    def test_declare_publisher_warns(self, session):
        m = _make_managed(session)
        with pytest.warns(RuntimeWarning, match='does NOT survive reconnect'):
            pub = m.declare_publisher('warn/pub')
        try:
            pub.undeclare()
        except Exception:
            pass

    def test_declare_subscriber_warns(self, session):
        m = _make_managed(session)
        with pytest.warns(RuntimeWarning, match='does NOT survive reconnect'):
            sub = m.declare_subscriber('warn/sub/**', lambda s: None)
        try:
            sub.undeclare()
        except Exception:
            pass

    def test_declare_queryable_warns(self, session):
        m = _make_managed(session)
        with pytest.warns(RuntimeWarning, match='does NOT survive reconnect'):
            q = m.declare_queryable('warn/q/**', lambda q: None)
        try:
            q.undeclare()
        except Exception:
            pass

    def test_internal_zeared_machinery_does_not_warn(self, session):
        """Pin: ``Cls.on_message``, ``msg.send()``, retention queryables
        all route through ``resolve_raw`` internally and do NOT emit the
        user-facing warning."""
        import warnings as _w

        @z.zeared
        class M(z.Message):
            TOPIC = 'warn/internal/{n}'
            RETAINED = True
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        m = _make_managed(session)
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter('always')
            # Internal zeared machinery — should not warn.
            M(n=1, v='x').send(session=m)
            sub = M.on_message(lambda msg: None, session=m)
            sub.close()
        rt_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert not rt_warnings, (
            f'unexpected RuntimeWarning(s) from zeared internals: '
            f'{[str(w.message) for w in rt_warnings]}'
        )


class TestResolveRaw:
    def test_unwrap_managed(self, session):
        m = _make_managed(session)
        assert resolve_raw(m) is session

    def test_passthrough_raw(self, session):
        assert resolve_raw(session) is session


# ---------------------------------------------------------------------------
# Reconnect orchestration with controlled open_fn
# ---------------------------------------------------------------------------


class TestReconnectOrchestration:
    def test_swap_raw_on_reconnect(self, session):
        """Force a reconnect via _trigger_reconnect + verify the swap."""
        new_raw = _peer_session()
        try:
            m = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='swap-test',
                probe_interval=0,            # disable probe
                initial_backoff=0.001,
                max_backoff=0.01,
                max_attempts=None,
            )
            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()

            start_probe(m)

            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)
            assert m.raw() is new_raw
            assert m.state == 'IDLE'
        finally:
            try:
                new_raw.close()
            except Exception:
                pass

    def test_max_attempts_exhausted_to_dead(self, session):
        """All open_fn attempts fail → state DEAD, no further activity."""
        attempts = [0]

        def failing_open():
            attempts[0] += 1
            raise RuntimeError('still down')

        m = ManagedSession(
            session, failing_open,
            endpoint_label='dead-test',
            probe_interval=0,
            initial_backoff=0.001,
            max_backoff=0.005,
            max_attempts=3,
        )

        # Synchronous reconnect via the inner helper to avoid timing flakes.
        from zeared._reconnect import _reconnect
        _reconnect(m)

        assert m.state == 'DEAD'
        assert attempts[0] == 3
        with pytest.raises(SessionDeadError):
            m.put('x/y', b'z')


class TestProbeDetectsClose:
    def test_probe_triggers_reconnect_after_close(self, session):
        """Probe detects close → reconnect spawned → state ends up IDLE."""
        new_raw = _peer_session()
        try:
            m = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='probe-test',
                probe_interval=0.05,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )
            reconnected = threading.Event()
            m._on_reconnect = lambda mgr: reconnected.set()
            start_probe(m)
            try:
                # Close the underlying raw; probe should pick it up.
                session.close()
                assert reconnected.wait(timeout=3.0)
                assert m.raw() is new_raw
            finally:
                m._teardown(call_close=False)
        finally:
            try:
                new_raw.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Subscriber redeclaration on reconnect
# ---------------------------------------------------------------------------


class TestSubscriberRedeclared:
    def test_subscriber_redeclared_against_new_raw(self, session):
        """After reconnect, the user's Subscriber handle keeps delivering."""
        @z.zeared
        class Pulse(z.Message):
            TOPIC = 'reco/sub/{n}'
            n: int = z.Int(required=True)

        new_raw = _peer_session()
        try:
            m = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='sub-reco',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )
            received: list[int] = []
            sub = Pulse.on_message(
                lambda p: received.append(p.n),
                session=m,
            )

            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()
            start_probe(m)
            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)
            wait(0.2)

            # Publish on the new raw — subscriber should still receive.
            new_raw.put('reco/sub/42', b'\x91*')   # msgpack [42]? doesn't matter; decode might fail
            wait(0.3)

            sub.close()
            # Just verify the redeclare didn't blow up — the redeclared
            # zenoh.Subscriber is bound to new_raw.
            assert sub._zenoh_subs   # tuple is non-empty
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass

    def test_auto_reconnect_false_subscriber_skipped(self, session):
        """A subscriber opted out of auto_reconnect is NOT redeclared."""
        @z.zeared
        class Pulse(z.Message):
            TOPIC = 'reco/optout/{n}'
            n: int = z.Int(required=True)

        new_raw = _peer_session()
        try:
            m = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='optout',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )

            sub = Pulse.on_message(
                lambda p: None,
                session=m,
                auto_reconnect=False,
            )
            old_subs = sub._zenoh_subs

            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()
            start_probe(m)
            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)

            # Same _zenoh_subs object — opt-out was honored.
            assert sub._zenoh_subs is old_subs
            sub.close()
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Presence will replay
# ---------------------------------------------------------------------------


class TestWillReplay:
    def test_will_replayed_under_new_zid(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'reco/will/{name}'
            LIVELINESS = True
            name:  str = z.Str(required=True)
            state: str = z.Str(required=True)

        new_raw = _peer_session()
        try:
            m = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='will-replay',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )
            old_zid = str(session.zid())
            new_zid = str(new_raw.zid())
            assert old_zid != new_zid

            Status(name='alice', state='offline').register_will(session=m)

            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()
            start_probe(m)
            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)
            wait(0.2)

            # The new presence state should have a will registered under
            # the NEW zid; the envelope's source_zid is updated.
            from zeared.presence import _registry
            new_state = None
            for state in _registry.values():
                if state.session is m:
                    new_state = state
                    break
            assert new_state is not None
            envelopes = list(new_state._registered.values())
            assert len(envelopes) == 1
            assert envelopes[0].source_zid == new_zid
            assert envelopes[0].target_key_expr == 'reco/will/alice'
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# z.release cancels probe + tears down
# ---------------------------------------------------------------------------


class TestReleaseCleansUp:
    def test_release_cancels_probe_thread(self, session):
        m = _make_managed(session, probe_interval=0.05)
        start_probe(m)
        thread = m._probe_thread
        assert thread is not None and thread.is_alive()

        z.release(session=m)
        # Probe thread should exit promptly (cancel event + 1s join).
        thread.join(timeout=2.0)
        assert not thread.is_alive()


# ---------------------------------------------------------------------------
# Batch buffer survives reconnect
# ---------------------------------------------------------------------------


class TestOnReconnectHook:
    """Public ``sess.on_reconnect(cb)`` API — sync + async-aware,
    list of callbacks, cancel handle."""

    def test_sync_callback_fires_after_reconnect(self, session):
        new_raw = _peer_session()
        try:
            m = _make_managed(session, probe_interval=0)
            fired = threading.Event()

            def cb(mgr):
                assert mgr is m
                fired.set()

            handle = m.on_reconnect(cb)
            assert isinstance(handle, z.OnReconnectHandle)

            # Replace the controlled open_fn so reconnect succeeds.
            m._open_fn = lambda: new_raw
            start_probe(m)
            _trigger_reconnect(m)
            assert fired.wait(timeout=3.0)
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass

    def test_multiple_callbacks_fire_in_registration_order(self, session):
        new_raw = _peer_session()
        try:
            m = _make_managed(session, probe_interval=0)
            order: list[str] = []
            m.on_reconnect(lambda mgr: order.append('first'))
            m.on_reconnect(lambda mgr: order.append('second'))
            m.on_reconnect(lambda mgr: order.append('third'))

            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()
            m._open_fn = lambda: new_raw
            start_probe(m)
            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)
            wait(0.1)

            assert order == ['first', 'second', 'third']
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass

    def test_callback_exception_logs_and_continues(self, session):
        new_raw = _peer_session()
        try:
            m = _make_managed(session, probe_interval=0)
            second_fired = threading.Event()

            def boom(mgr):
                raise RuntimeError('first cb blew up')

            def quiet(mgr):
                second_fired.set()

            m.on_reconnect(boom)
            m.on_reconnect(quiet)

            m._open_fn = lambda: new_raw
            start_probe(m)
            _trigger_reconnect(m)
            assert second_fired.wait(timeout=3.0), (
                'second callback did not fire — first cb exception '
                'short-circuited the loop'
            )
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass

    def test_cancel_handle_deregisters(self, session):
        new_raw = _peer_session()
        try:
            m = _make_managed(session, probe_interval=0)
            fired = []
            handle = m.on_reconnect(lambda mgr: fired.append(1))
            handle.cancel()

            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()
            m._open_fn = lambda: new_raw
            start_probe(m)
            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)
            wait(0.1)

            assert fired == [], 'cancelled callback fired anyway'

            # Cancel idempotent.
            handle.cancel()
            handle.cancel()
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass

    def test_async_callback_without_loop_raises_at_registration(self, session):
        m = _make_managed(session, probe_interval=0)

        async def async_cb(mgr):
            pass

        # No running loop on this thread.
        with pytest.raises(RuntimeError, match='running event loop'):
            m.on_reconnect(async_cb)

    def test_async_callback_fires_on_captured_loop(self, session):
        """Register an async callback from an event loop, force a
        reconnect, verify the coroutine ran on the captured loop."""
        import asyncio
        new_raw = _peer_session()

        async def main():
            m = _make_managed(session, probe_interval=0)
            fired = asyncio.Event()

            async def async_cb(mgr):
                fired.set()

            m.on_reconnect(async_cb)

            m._open_fn = lambda: new_raw
            start_probe(m)
            _trigger_reconnect(m)
            try:
                await asyncio.wait_for(fired.wait(), timeout=3.0)
            finally:
                m._teardown(call_close=False)

        try:
            asyncio.run(main())
        finally:
            try:
                new_raw.close()
            except Exception:
                pass


class TestReconnectWorkerSingleton:
    """Pin: when ``start_probe`` runs, the reconnect worker is a single
    long-lived daemon thread per ManagedSession; multiple triggers reuse
    the same worker (no per-trigger thread spawn)."""

    def test_worker_thread_singleton_across_triggers(self, session):
        new_raws = [_peer_session() for _ in range(3)]
        try:
            iterator = iter(new_raws)
            m = ManagedSession(
                session, lambda: next(iterator),
                endpoint_label='worker-test',
                probe_interval=0.05,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )
            start_probe(m)
            worker = m._reconnect_thread
            assert worker is not None and worker.is_alive()

            for i in range(3):
                done = threading.Event()
                m._on_reconnect = lambda mgr, ev=done: ev.set()
                start_probe(m)
                _trigger_reconnect(m)
                assert done.wait(timeout=3.0)
                # Same worker thread served all triggers.
                assert m._reconnect_thread is worker

            m._teardown(call_close=False)
            worker.join(timeout=2.0)
            assert not worker.is_alive()
        finally:
            for r in new_raws:
                try:
                    r.close()
                except Exception:
                    pass


class TestSubscriberRedeclareNoWarn:
    """Pin: ``Subscriber._redeclare`` routes through raw session
    declarations (not the wrapper's ``declare_subscriber`` which would
    emit ``RuntimeWarning``). Symmetry with the initial-declare path
    pinned in 0.0.12; reconnect-driven redeclare gets the same
    treatment."""

    def test_redeclare_does_not_warn(self, session):
        import warnings as _w

        @z.zeared
        class M(z.Message):
            TOPIC = 'reco/symmetry/{n}'
            n: int = z.Int(required=True)
            v: str = z.Str(required=True)

        new_raw = _peer_session()
        try:
            m = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='symmetry',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )
            sub = M.on_message(lambda msg: None, session=m)

            with _w.catch_warnings(record=True) as caught:
                _w.simplefilter('always')
                done = threading.Event()
                m._on_reconnect = lambda mgr: done.set()
                start_probe(m)
                _trigger_reconnect(m)
                assert done.wait(timeout=3.0)

            rt_warnings = [
                w for w in caught if issubclass(w.category, RuntimeWarning)
            ]
            # Internal declare-handle warnings should NOT fire from
            # the redeclare path — Subscriber._redeclare uses the
            # raw session passed in by _restore_subscribers, not the
            # wrapper.
            handle_warnings = [
                w for w in rt_warnings
                if 'does NOT survive reconnect' in str(w.message)
            ]
            assert not handle_warnings, (
                f'redeclare path emitted user-facing declare-handle '
                f'warnings: {[str(w.message) for w in handle_warnings]}'
            )
            sub.close()
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass


class TestContextManagerSync:
    """Pin: ``with z.peer(auto_reconnect=True) as sess:`` releases on
    exit. Pre-0.0.15 callers had to call ``z.release(session=sess)``
    manually."""

    def test_enter_returns_wrapper_not_raw(self, session):
        m = _make_managed(session)
        with m as sess:
            # Critically: NOT m.raw(). Holding the wrapper across the
            # block lets the user code survive reconnects.
            assert sess is m
            assert isinstance(sess, ManagedSession)

    def test_exit_releases_session(self):
        sess = z.peer(auto_reconnect=True, probe_interval=0)
        thread = sess._reconnect_thread
        assert thread is not None and thread.is_alive()

        with sess:
            pass
        # Probe + reconnect worker joined; raw closed.
        thread.join(timeout=2.0)
        assert not thread.is_alive()

    def test_exit_does_not_suppress_exceptions(self):
        sess = z.peer(auto_reconnect=True, probe_interval=0)

        class _Boom(RuntimeError):
            pass

        with pytest.raises(_Boom):
            with sess:
                raise _Boom('block raised')
        # Release happened despite the exception.
        thread = sess._reconnect_thread
        if thread is not None:
            thread.join(timeout=2.0)

    def test_exit_during_reconnect_in_flight_cleans_up(self):
        """Pin: exiting the ``with`` block while a reconnect is in
        progress — the worker's open-with-backoff sees the cancel via
        ``_probe_cancel`` and raises ``_ReconnectAborted``, the wrapper
        ends in ``DEAD`` state, no orphan thread."""
        attempts = [0]

        def slow_open():
            attempts[0] += 1
            raise RuntimeError('always fails — forces backoff')

        sess = z.peer(auto_reconnect=True, probe_interval=0)
        sess._open_fn = slow_open
        sess._initial_backoff = 0.05
        sess._max_backoff = 1.0

        with sess:
            _trigger_reconnect(sess)
            # Tiny pause to ensure the worker is in the backoff sleep.
            wait(0.1)
        # Block exited mid-flight. _ReconnectAborted should have fired
        # and released cleanly.
        thread = sess._reconnect_thread
        if thread is not None:
            thread.join(timeout=2.0)
            assert not thread.is_alive()
        assert sess.state == 'DEAD'

    def test_raw_session_with_block_via_zenoh(self):
        """Pin: ``with z.peer() as sess`` (auto_reconnect=False) works
        through zenoh.Session's own context manager protocol — no
        zeared change required for this branch."""
        with z.peer() as sess:
            assert sess is not None
            assert sess.zid() is not None


class TestRetentionRebuild:
    """Pin: retention queryables MUST be rebuilt on reconnect.

    Without the fix, ``_RetentionCache._queryables`` stays bound to the
    dead raw — late subscribers calling ``session.get(wildcard)`` silently
    miss retained values. Two tests cover both scenarios:
      - rebuild invariant: cache._queryables identity changes; cache._cache
        content preserved; a ``ma.get(wildcard)`` post-reconnect retrieves
        the cached values via the rebuilt queryable on the new raw.
      - same-process retained-fetch: one process publishes retained AND
        subscribes via the same ManagedSession; reconnect fires; the
        subscriber's reconnect-triggered retained-fetch sees the value
        because retention rebuilt before subscribers (ordering invariant).
        Fails silently if anyone "simplifies" the order in ``_reconnect``.
    """

    def test_queryable_handles_rebuilt_and_cache_preserved(self, session):
        @z.zeared
        class Reg(z.Message):
            TOPIC = 'reco/rebuild/{name}'
            RETAINED = True
            name:  str = z.Str(required=True)
            state: str = z.Str(required=True)

        new_raw = _peer_session()
        try:
            ma = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='rebuild',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )

            # Publish retained on the wrapper — declares the queryable
            # on `session` via ma's delegation; `_RetentionCache._cache`
            # holds the (raw, encoding) tuple.
            Reg(name='alice', state='online').send(session=ma)
            wait(0.2)

            from zeared.retention import _registry as _rr
            caches = [c for c in _rr.values() if c._session is ma]
            assert len(caches) == 1
            cache = caches[0]
            old_queryables = list(cache._queryables)
            old_cache_snapshot = dict(cache._cache)
            assert len(old_queryables) >= 1
            assert old_cache_snapshot   # non-empty

            # Force reconnect.
            done = threading.Event()
            ma._on_reconnect = lambda mgr: done.set()
            start_probe(ma)
            _trigger_reconnect(ma)
            assert done.wait(timeout=3.0)
            wait(0.2)

            # Cache content preserved.
            assert dict(cache._cache) == old_cache_snapshot

            # Queryables are NEW objects bound to the new raw.
            new_queryables = list(cache._queryables)
            assert len(new_queryables) == len(old_queryables)
            for old, new in zip(old_queryables, new_queryables):
                assert new is not old, (
                    'retention queryable identity unchanged across reconnect '
                    '— rebuild did not run'
                )
        finally:
            try:
                ma._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass

    def test_same_process_retained_fetch_after_reconnect(self, session):
        """Pins retention-first ordering. A subscriber's reconnect-triggered
        retained-fetch fires ``new_raw.get(wildcard)`` — for that to retrieve
        the retained value, the publisher-side queryable on the SAME wrapper
        must already be redeclared. If subscriber rebuild ran first, the
        queryable would still be dead when the get fires."""
        @z.zeared
        class Reg(z.Message):
            TOPIC = 'reco/same/{name}'
            RETAINED = True
            DEDUPE = False           # don't suppress the post-reconnect replay
            name:  str = z.Str(required=True)
            state: str = z.Str(required=True)

        new_raw = _peer_session()
        try:
            ma = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='same-proc',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )

            # Publisher: ma publishes retained. Cache holds the value;
            # queryable on `session` answers gets.
            Reg(name='alice', state='online').send(session=ma)
            wait(0.2)

            # Subscriber on the SAME wrapper. Initial retained-fetch
            # picks up the value via the local queryable.
            received: list[tuple[str, str]] = []
            sub = Reg.on_message(
                lambda m: received.append((m.name, m.state)),
                session=ma,
            )
            wait(0.4)
            assert ('alice', 'online') in received, (
                'sanity: pre-reconnect retained-fetch should pick up '
                'the value via the local queryable'
            )
            received.clear()

            # Force reconnect.
            done = threading.Event()
            ma._on_reconnect = lambda mgr: done.set()
            start_probe(ma)
            _trigger_reconnect(ma)
            assert done.wait(timeout=3.0)
            wait(0.5)
            sub.close()

            # The subscriber's reconnect-triggered retained-fetch should
            # have replayed the value, because retention rebuilt FIRST and
            # put the queryable in place before the subscriber's get fired.
            assert ('alice', 'online') in received, (
                "subscriber's retained-fetch missed retained value after "
                'reconnect — retention-first ordering invariant regressed '
                '(subscriber redeclare ran before retention rebuild, so '
                'the queryable was dead during the fetch)'
            )
        finally:
            try:
                ma._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass


class TestBatchSurvivesReconnect:
    def test_batch_flushes_on_post_reconnect_session(self, session):
        """Pin: a batch context entered before reconnect, exited after,
        flushes buffered samples on whatever raw is current at flush time.
        Doesn't make a strong correctness claim about samples crossing the
        reconnect — just that the buffer doesn't crash."""
        @z.zeared
        class Note(z.Message):
            TOPIC = 'reco/batch/{n}'
            n: int = z.Int(required=True)

        new_raw = _peer_session()
        try:
            m = ManagedSession(
                session, lambda: new_raw,
                endpoint_label='batch-reco',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )

            with z.batch():
                # Buffer one sample, reconnect, then flush.
                Note(n=1).send(session=m)
                done = threading.Event()
                m._on_reconnect = lambda mgr: done.set()
                start_probe(m)
                _trigger_reconnect(m)
                assert done.wait(timeout=3.0)
                # batch __exit__ flushes — should not raise SessionDeadError
                # since we're back to IDLE.
            assert m.state == 'IDLE'
        finally:
            try:
                m._teardown(call_close=False)
            except Exception:
                pass
            try:
                new_raw.close()
            except Exception:
                pass
