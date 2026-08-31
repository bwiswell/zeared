"""Smoke tests for ``zeared/subscriber/_subscriber_retained_fetch.py``
— the helper that issues ``session.get(wildcard)`` per declared
template and routes reply samples through the subscriber's dispatch
path.

End-to-end retained-fetch behaviour is covered by ``test_retention.py``
and ``test_subscriber.py``; this file confirms the module's public
surface.
"""
from __future__ import annotations

from zeared.meta import Origin
from zeared.subscriber._subscriber_retained_fetch import _fetch_retained


class TestPublicSurface:
    def test_callable(self):
        assert callable(_fetch_retained)


class _FakeReply:
    def __init__(self, ok):
        self.ok = ok


class _FakeSession:
    """Stub with just enough surface for ``_fetch_retained``: ``get``
    returns one canned reply per call."""
    def __init__(self, replies):
        self._replies = replies

    def get(self, _wildcard):
        return list(self._replies)


class _FakeTemplate:
    wildcard = 'fake/*'


class TestReplayOrigin:
    def test_replies_dispatched_with_origin_replay(self):
        """Every reply routed through dispatch carries origin=REPLAY —
        the subscribe-time AND post-reconnect fetches share this helper,
        so this single seam covers both replay paths."""
        seen: list[Origin] = []

        def dispatch(sample, *, origin=Origin.LIVE):
            seen.append(origin)

        session = _FakeSession([_FakeReply(ok=object()), _FakeReply(ok=object())])
        _fetch_retained(session, [_FakeTemplate()], dispatch, type('C', (), {}), None)

        assert seen == [Origin.REPLAY, Origin.REPLAY]

    def test_error_replies_skipped(self):
        seen: list[Origin] = []

        def dispatch(sample, *, origin=Origin.LIVE):
            seen.append(origin)

        session = _FakeSession([_FakeReply(ok=None), _FakeReply(ok=object())])
        _fetch_retained(session, [_FakeTemplate()], dispatch, type('C', (), {}), None)

        assert seen == [Origin.REPLAY]
