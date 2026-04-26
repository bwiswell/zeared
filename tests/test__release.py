from __future__ import annotations

import time

import pytest

import zeared as z
from zeared.publisher import _registry as _pub_registry
from zeared.retention import _registry as _ret_registry
from zeared.presence import _registry as _pres_registry, _observer_registry
from zeared.subscriber import _subscribers

from conftest import wait


class TestReleaseRequiresKwarg:
    def test_no_args_raises(self):
        with pytest.raises(TypeError):
            z.release()

    def test_positional_session_rejected(self, session):
        # session= is keyword-only.
        with pytest.raises(TypeError):
            z.release(session)


class TestReleaseWalksAllResources:
    def test_full_walk(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'rel/walk/{id}'
            RETAINED = True
            LIVELINESS = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        # Populate all five registries.
        sub = M.on_message(lambda m: None)            # subscribers
        wait()
        M(id=1, v=1).send()                           # publisher cache + retention
        M(id=1, v=1).register_will()                  # presence state
        wait()

        sid = id(session)
        assert sid in {k[1] for k in _pub_registry}
        assert sid in {k[1] for k in _ret_registry}
        assert sid in _pres_registry
        assert sid in _observer_registry
        assert sid in _subscribers

        z.release(session=session)

        # Every registry should be empty for this session.
        assert sid not in {k[1] for k in _pub_registry}
        assert sid not in {k[1] for k in _ret_registry}
        assert sid not in _pres_registry
        assert sid not in _observer_registry
        assert sid not in _subscribers


class TestReleaseIdempotent:
    def test_double_release(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'rel/idempotent/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        z.session = session
        M(id=1, v=1).send()

        z.release(session=session)
        # Second call should be a clean no-op.
        z.release(session=session)


class TestReleaseDoesNotUseDefault:
    def test_default_session_does_not_satisfy_kwarg(self, session):
        z.session = session   # set the module-level default
        with pytest.raises(TypeError):
            z.release()       # explicit-always: must pass session=


class TestReleasePropagatesPeerWill:
    """Verify the step-5-before-step-6 invariant empirically.

    A peer subscribed to a LIVELINESS class on session B should observe
    A's registered will SYNTHESISED locally when ``z.release(session=A)``
    is called — proving that the liveliness DELETE escaped the transport
    before the session.close().
    """

    def test_will_propagates_through_release(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Status(z.Message):
            TOPIC = 'rel/peer/{name}/status'
            LIVELINESS = True
            name:  str = z.Str(required=True)
            state: str = z.Str(required=True)

        Status(name='alice', state='offline').register_will(session=session_a)
        wait(0.3)

        received: list[tuple[str, str]] = []
        sub = Status.on_message(
            lambda m: received.append((m.name, m.state)),
            session=session_b,
        )
        wait(0.3)

        # Tear A down via the unified release primitive.
        z.release(session=session_a)
        wait(0.5)
        sub.close()

        assert ('alice', 'offline') in received


class TestReleaseClosesSubscribers:
    def test_subscriber_close_cancels_watchdog(self, session):
        """Released subscribers are properly closed — pending watchdogs
        cancelled, registry deregistered."""
        @z.zeared
        class M(z.Message):
            TOPIC = 'rel/wd/{id}'
            id: int = z.Int(required=True)

        z.session = session
        fired: list[str] = []
        sub = M.on_message(
            lambda m: None,
            expected_interval=0.5,
            on_quiet=lambda: fired.append('quiet'),
        )
        wait()
        M(id=1).send()
        wait(0.1)

        z.release(session=session)
        # Past the watchdog interval — should NOT fire because release()
        # cancelled it.
        wait(0.7)
        assert fired == []


class TestReleaseAll:
    """Pin: ``z.release_all()`` walks every per-session registry, dedupes
    session refs, and calls ``release(session=...)`` on each."""

    def test_releases_multiple_sessions(self):
        """Open three sessions, register varied state on each, call
        release_all, assert all registries empty."""
        from zeared.publisher import _registry as _pub_registry
        from zeared.retention import _registry as _ret_registry
        from zeared.presence import (
            _registry as _pres_registry,
            _observer_registry as _obs_registry,
        )
        from zeared.subscriber import _subscribers

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'rel-all/tele/{n}'
            n: int = z.Int(required=True)
            v: int = z.Int(required=True)

        # Open three peer sessions (multicast disabled — they're isolated).
        from conftest import _peer_session
        sessions = [_peer_session(), _peer_session(), _peer_session()]

        try:
            # Register varied state.
            # Session 0: subscriber.
            sub = Tele.on_message(lambda m: None, session=sessions[0])
            # Session 1: publisher cache + retention.
            Tele(n=1, v=1).send(session=sessions[1])
            # Session 2: also a subscriber.
            sub2 = Tele.on_message(lambda m: None, session=sessions[2])
            wait(0.1)

            # Sanity: registries non-empty.
            assert _subscribers != {}
            assert _pub_registry != {}

            z.release_all()

            # Every per-session registry now empty.
            assert _subscribers == {}
            assert _pub_registry == {}
            assert _ret_registry == {}
            assert _pres_registry == {}
            assert _obs_registry == {}

            # Calling again is a no-op — idempotent.
            z.release_all()
            assert _subscribers == {}
        finally:
            # If release_all worked, sessions are already closed.
            # Best-effort cleanup just in case the test failed mid-run.
            for s in sessions:
                try:
                    s.close()
                except Exception:
                    pass

    def test_no_args(self):
        """Pin: ``release_all`` takes no arguments."""
        # Trivially pass with empty registries — just verifies signature.
        z.release_all()
        # Twice — idempotent.
        z.release_all()

    def test_managed_session_without_registered_resources_released(self):
        """Pin (regression): ``release_all`` tears down the probe +
        reconnect threads of a ``ManagedSession`` even when nothing
        has registered against it (no subscribers, publishers,
        retention caches, or presence state). Pre-0.0.17, ``release_all``
        only iterated per-resource registries — a wrapper with no
        zeared state was invisible. The 0.0.17 ``_managed_sessions``
        WeakSet closes that gap.
        """
        sess = z.peer(auto_reconnect=True, probe_interval=0)
        try:
            # Pre-condition: no per-resource registry entries.
            from zeared.subscriber import _subscribers
            from zeared.publisher import _registry as _pub_registry
            from zeared.retention import _registry as _ret_registry
            assert _subscribers == {}
            assert _pub_registry == {}
            assert _ret_registry == {}

            # The reconnect worker is alive by construction.
            worker = sess._reconnect_thread
            assert worker is not None and worker.is_alive()

            # Pre-0.0.17, release_all here would do nothing — wrapper
            # not in any walked registry. Post-0.0.17, the WeakSet walk
            # picks it up.
            z.release_all()

            worker.join(timeout=2.0)
            assert not worker.is_alive(), (
                'managed wrapper without registered zeared resources '
                'was not released by release_all — _managed_sessions '
                'WeakSet walk regressed'
            )
        finally:
            try:
                sess._teardown(call_close=True)
            except Exception:
                pass

    def test_managed_session_in_weakset_after_construction(self):
        """Pin: every ManagedSession registers itself in the WeakSet."""
        from zeared._managed_session import _managed_sessions

        sess = z.peer(auto_reconnect=True, probe_interval=0)
        try:
            assert sess in _managed_sessions
        finally:
            z.release(session=sess)
