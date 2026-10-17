"""Tests for ``zeared/message/_message_unretain.py`` — the
``_UnretainDescriptor`` instance-vs-class dispatch and the
``_unretain_impl`` shared implementation.

End-to-end retention/tombstone coverage lives in ``test_retention.py``.
This file confirms the descriptor's instance-vs-class dispatch and
the RETAINED-required guard.
"""

from __future__ import annotations

import pytest

import zeared as z
from zeared.message._message_unretain import _unretain_impl, _UnretainDescriptor


class TestPublicSurface:
    def test_descriptor_class_importable(self):
        assert _UnretainDescriptor is not None

    def test_impl_callable(self):
        assert callable(_unretain_impl)


class TestRetainedRequiredGuard:
    """Pin: ``unretain`` requires ``RETAINED = True`` on the class.
    Both instance and class forms raise ``TopicError`` otherwise."""

    def test_instance_form_rejects_non_retained(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'untrn/plain/{n}'
            # RETAINED defaults to False.
            n: int = z.Int(required=True)

        z.session = session
        msg = Plain(n=1)
        with pytest.raises(z.TopicError, match='RETAINED = True'):
            msg.unretain()

    def test_class_form_rejects_non_retained(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'untrn/plain2/{n}'
            n: int = z.Int(required=True)

        z.session = session
        with pytest.raises(z.TopicError, match='RETAINED = True'):
            Plain.unretain(n=1)


class TestDescriptorDispatch:
    """Pin: instance access produces a no-kwargs callable; class access
    produces one that takes ``**key_fields``."""

    def test_instance_callable_no_kwargs(self, session):
        @z.zeared
        class R(z.Message):
            TOPIC = 'untrn/dispatch/{n}'
            RETAINED = True
            n: int = z.Int(required=True)

        z.session = session
        R(n=1).send()
        msg = R(n=1)
        # Doesn't raise on call (RETAINED is True).
        msg.unretain()

    def test_class_callable_takes_key_fields(self, session):
        @z.zeared
        class R(z.Message):
            TOPIC = 'untrn/dispatch2/{n}'
            RETAINED = True
            n: int = z.Int(required=True)

        z.session = session
        R(n=2).send()
        # Class form: pass key fields as kwargs.
        R.unretain(n=2)


class TestTombstoneDuringReconnect:
    """A tombstone raises ``SessionDeadError`` while the managed session is down, as a
    send does. The ``SessionDeadError`` ``ManagedSession.delete`` raises used to be
    rewrapped in a plain ``ZearedError``, which the retry handler can't catch."""

    @pytest.mark.parametrize('batched', [False, True], ids=['direct', 'batched'])
    def test_raises_session_dead_error(self, batched):
        from conftest import _peer_session

        from zeared._managed_session import ManagedSession
        from zeared.errors import SessionDeadError

        @z.zeared
        class M(z.Message):
            TOPIC = 'unretain/dead/{k}'
            RETAINED = True
            k: str = z.Str(required=True)

        raw = _peer_session()
        m = ManagedSession(
            raw,
            lambda: raw,
            endpoint_label='tomb-dead',
            probe_interval=0,
            initial_backoff=0.001,
            max_backoff=0.005,
            max_attempts=None,
        )
        try:
            M(k='a').send(session=m)
            m._set_state('RECONNECTING')
            with pytest.raises(SessionDeadError):
                if batched:
                    with z.batch():
                        M.unretain(k='a', session=m)
                else:
                    M.unretain(k='a', session=m)
        finally:
            m._teardown(call_close=True)
