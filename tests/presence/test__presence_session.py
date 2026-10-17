"""Smoke tests for ``zeared/presence/_presence_session.py`` — the
``_SessionPresence`` class and the per-session presence registry.

Comprehensive end-to-end coverage lives in ``test_presence.py``; this
file confirms the file's public surface is importable and that the
core constructor / registry helpers behave.
"""

from __future__ import annotations

from conftest import _peer_session

from zeared.presence._presence_session import (
    _registry,
    _registry_lock,
    _SessionPresence,
    clear_presence_state,
    get_presence,
)


class TestSessionPresence:
    def test_class_importable(self):
        assert _SessionPresence is not None
        assert hasattr(_SessionPresence, '__slots__')

    def test_registry_is_dict(self):
        assert isinstance(_registry, dict)

    def test_registry_lock_is_lock(self):
        # threading.Lock is a factory; the result is a primitive — no
        # public class to isinstance-check. ``acquire`` + ``release``
        # presence is the durable contract.
        assert hasattr(_registry_lock, 'acquire')
        assert hasattr(_registry_lock, 'release')


class TestRegistryHelpers:
    def test_get_presence_callable(self):
        assert callable(get_presence)

    def test_clear_presence_state_callable(self):
        assert callable(clear_presence_state)

    def test_clear_with_no_session_clears_all(self):
        # Smoke: doesn't raise when the registry is empty.
        clear_presence_state()
        assert _registry == {}


class TestHeldTokens:
    """0.4.0: one Zenoh liveliness token per concrete key, behind a stable handle."""

    def _held(self, sess, pattern='held/**'):
        return sorted(str(r.ok.key_expr) for r in sess.liveliness().get(pattern, timeout=1.0) if r.ok is not None)

    def test_declare_holds_and_undeclare_withdraws(self, session):
        state = get_presence(session)
        token = state.declare('held/a')
        assert token.active
        assert state.keys() == ['held/a']
        assert self._held(session) == ['held/a']
        token.undeclare()
        token.undeclare()  # idempotent
        assert not token.active
        assert self._held(session) == []

    def test_same_key_same_handle_and_one_token(self, session):
        state = get_presence(session)
        first = state.declare('held/b')
        assert state.declare('held/b') is first
        first.undeclare()  # one undeclare withdraws it: there was only one token
        assert self._held(session) == []

    def test_a_stale_handle_cannot_withdraw_a_newer_token(self, session):
        state = get_presence(session)
        old = state.declare('held/c')
        old.undeclare()
        new = state.declare('held/c')
        old.undeclare()  # no-op: ``new`` holds the key now
        assert new.active
        assert self._held(session) == ['held/c']

    def test_context_manager_withdraws(self, session):
        with get_presence(session).declare('held/d') as token:
            assert token.active
        assert not token.active

    def test_replay_to_redeclares_and_keeps_handles(self, session):
        state = get_presence(session)
        token = state.declare('held/e')
        other = _peer_session()
        try:
            state.replay_to(other)
            assert token.active
            assert state.session is other
            assert self._held(other) == ['held/e']
        finally:
            state.drop()
            other.close()

    def test_drop_and_clear_withdraw_everything(self, session):
        state = get_presence(session)
        state.declare('held/f')
        state.declare('held/g')
        clear_presence_state(session=session)
        assert self._held(session) == []
        assert id(session) not in _registry
