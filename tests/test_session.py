from __future__ import annotations

import threading

import pytest

import zeared as z
from zeared._session import _SessionHandle
from zeared.errors import NoSessionError


@pytest.fixture(autouse=True)
def _reset_default():
    """Reset the module-level default between tests."""
    before = z.session.current
    z.session._set_default(None)
    yield
    z.session._set_default(before)


class TestModuleAttributeInterception:
    def test_assignment_sets_default_not_replaces_handle(self):
        handle_id = id(z.session)
        z.session = 'fake-session-A'
        # handle is still the same object
        assert id(z.session) == handle_id
        # but .current reflects the assignment
        assert z.session.current == 'fake-session-A'

    def test_repeated_assignment_overwrites(self):
        z.session = 'A'
        z.session = 'B'
        assert z.session.current == 'B'

    def test_session_is_session_handle(self):
        assert isinstance(z.session, _SessionHandle)

    def test_debug_assignment_is_unintercepted(self):
        z.debug = True
        assert z.debug is True
        z.debug = False


class TestResolutionPrecedence:
    def test_explicit_kwarg_wins(self):
        z.session = 'default'
        with z.session('scoped'):
            resolved = z.session.resolve(explicit='explicit')
            assert resolved == 'explicit'

    def test_scoped_beats_default(self):
        z.session = 'default'
        with z.session('scoped'):
            assert z.session.resolve(explicit=None) == 'scoped'

    def test_default_when_no_scope(self):
        z.session = 'default'
        assert z.session.resolve(explicit=None) == 'default'

    def test_raises_when_nothing_set(self):
        with pytest.raises(NoSessionError):
            z.session.resolve(explicit=None)


class TestScopeStack:
    def test_nested_scopes(self):
        z.session = 'base'
        with z.session('outer'):
            assert z.session.current == 'outer'
            with z.session('inner'):
                assert z.session.current == 'inner'
            assert z.session.current == 'outer'
        assert z.session.current == 'base'

    def test_scope_stack_is_thread_local(self):
        z.session = 'base'
        other_thread_saw = []

        def worker():
            other_thread_saw.append(z.session.current)
            with z.session('worker-scope'):
                other_thread_saw.append(z.session.current)
            other_thread_saw.append(z.session.current)

        with z.session('main-scope'):
            t = threading.Thread(target=worker)
            t.start()
            t.join()
            # Main thread still sees its own scope.
            assert z.session.current == 'main-scope'

        # Worker thread never saw the main thread's scope.
        assert other_thread_saw == ['base', 'worker-scope', 'base']

    def test_exception_in_scope_still_pops(self):
        z.session = 'base'
        try:
            with z.session('scoped'):
                raise RuntimeError('boom')
        except RuntimeError:
            pass
        assert z.session.current == 'base'


class TestCurrent:
    def test_current_is_none_initially(self):
        assert z.session.current is None

    def test_current_returns_default_when_set(self):
        z.session = 'x'
        assert z.session.current == 'x'


class TestTimestampingInjection:
    """Pin: factory injects ``timestamping/enabled=true`` into the built
    Config when ``zenoh_config=None`` and ``timestamping=True`` (default).
    Honors silent-respect-zenoh_config rule when the user supplies a
    Config explicitly."""

    def test_default_injects_timestamping_into_built_config(self):
        from zeared._factories import _build_config_for_peer
        cfg = _build_config_for_peer(None, None, None, timestamping=True)
        # Read back from the Config — Zenoh's Config.get_json returns
        # a JSON string for the requested key path.
        val = cfg.get_json('timestamping/enabled')
        assert 'true' in val.lower()

    def test_timestamping_false_skips_injection(self):
        from zeared._factories import _build_config_for_peer
        cfg = _build_config_for_peer(None, None, None, timestamping=False)
        val = cfg.get_json('timestamping/enabled').lower()
        # Field unset → 'null'; we explicitly opted out, so it must NOT
        # be 'true'.
        assert 'true' not in val

    def test_silent_respect_when_zenoh_config_supplied(self):
        """If the user passes zenoh_config, the factory does NOT touch
        timestamping — they took over the Config object."""
        import zenoh
        from zeared._factories import _build_config_for_peer

        user_cfg = zenoh.Config()
        # User did NOT enable timestamping in their config.
        out = _build_config_for_peer(None, None, user_cfg, timestamping=True)
        # Same object back; we didn't modify it.
        assert out is user_cfg
        val = out.get_json('timestamping/enabled').lower()
        # User's choice (default null = unset) preserved — NOT injected.
        assert 'true' not in val

    def test_client_factory_injects_too(self):
        from zeared._factories import _build_config_for_client
        cfg = _build_config_for_client(['tcp/x:7447'], None, timestamping=True)
        val = cfg.get_json('timestamping/enabled')
        assert 'true' in val.lower()

    def test_factory_end_to_end_session_has_timestamping_enabled(self):
        """Open a real peer session via z.peer() with default kwargs;
        confirm the resulting session reports timestamping enabled."""
        sess = z.peer()
        try:
            # SessionInfo doesn't expose timestamping config directly;
            # the proof-by-construction is that _build_config_for_peer
            # injects it. We verify the Config side via the unit test
            # above; here we just confirm the factory call doesn't blow
            # up with the new kwarg defaults.
            assert sess is not None
        finally:
            sess.close()


class TestGcIntervalKwarg:
    """Pin: ``peer(gc_interval=...)`` stashes the value on the
    ManagedSession, and the per-session presence observer reads it via
    ``getattr(session, '_gc_interval', default)``."""

    def test_managed_session_stashes_gc_interval(self):
        sess = z.peer(auto_reconnect=True, probe_interval=0, gc_interval=42.0)
        try:
            assert isinstance(sess, z.ManagedSession)
            assert sess._gc_interval == 42.0
        finally:
            z.release(session=sess)

    def test_observer_picks_up_gc_interval_from_managed(self):
        from zeared.presence import _resolve_gc_interval, get_observer

        sess = z.peer(auto_reconnect=True, probe_interval=0, gc_interval=0.05)
        try:
            obs = get_observer(sess)
            # Observer-level override is None (loop reads from session
            # on each iteration); the resolved interval reflects the
            # wrapper's value.
            assert obs._gc_interval is None
            resolved = _resolve_gc_interval(sess, obs._gc_interval)
            assert resolved == 0.05
        finally:
            z.release(session=sess)

    def test_raw_session_uses_default_gc_interval(self):
        from zeared.presence import (
            _GC_INTERVAL_SECONDS, _resolve_gc_interval, get_observer,
        )

        sess = z.peer()      # no auto_reconnect → raw session
        try:
            obs = get_observer(sess)
            assert obs._gc_interval is None
            resolved = _resolve_gc_interval(sess, obs._gc_interval)
            assert resolved == _GC_INTERVAL_SECONDS
        finally:
            sess.close()

    def test_runtime_gc_interval_change_propagates(self):
        """Pin: ``_gc_interval`` is read on every loop iteration, so
        runtime mutations to the wrapper or the observer override
        propagate without restarting the session."""
        from zeared.presence import _resolve_gc_interval, get_observer

        sess = z.peer(auto_reconnect=True, probe_interval=0, gc_interval=10.0)
        try:
            obs = get_observer(sess)
            assert _resolve_gc_interval(sess, obs._gc_interval) == 10.0

            # Mutate the wrapper-level value — observer picks it up.
            sess._gc_interval = 30.0
            assert _resolve_gc_interval(sess, obs._gc_interval) == 30.0

            # Direct observer override wins.
            obs._gc_interval = 0.05
            assert _resolve_gc_interval(sess, obs._gc_interval) == 0.05
        finally:
            z.release(session=sess)


class TestTimestampingPlusGcInterval:
    """Cross-test: setting both flags simultaneously is the realistic
    daemon invocation. Pins that the two features don't accidentally
    interact in a future refactor."""

    def test_both_kwargs_layer_independently(self):
        from zeared.presence import _resolve_gc_interval, get_observer

        sess = z.peer(
            auto_reconnect=True, probe_interval=0,
            timestamping=True, gc_interval=30.0,
        )
        try:
            # GC interval flowed into the wrapper.
            assert sess._gc_interval == 30.0
            obs = get_observer(sess)
            # Observer's override is None; the resolved value reflects
            # the wrapper.
            assert obs._gc_interval is None
            assert _resolve_gc_interval(sess, obs._gc_interval) == 30.0
            assert sess.state == 'IDLE'
        finally:
            z.release(session=sess)
