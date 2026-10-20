"""Smoke tests for ``zeared/subscriber/_subscriber_retained_fetch.py``
— the helper that issues ``session.get(wildcard)`` per declared
template and routes reply samples through the subscriber's dispatch
path.

End-to-end retained-fetch behaviour is covered by ``test_retention.py``
and ``test_subscriber.py``; this file confirms the module's public
surface.
"""

from __future__ import annotations

import logging

from zeared.errors import RetainedFetchError
from zeared.meta import Origin
from zeared.subscriber._subscriber_retained_fetch import _collect_retained, _fetch_retained, _route_error_reply


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


class _FakeErr:
    def __init__(self, payload):
        self.payload = payload


class _FakeErrorReply:
    def __init__(self, payload=b'Timeout'):
        self.ok = None
        self.err = None if payload is None else _FakeErr(payload)


class TestErrorReplyRouting:
    """``fetch_retained``'s error replies (0.4.4): to ``on_error`` as ``query``'s are, else the log."""

    def test_on_error_hears_a_retained_fetch_error_with_the_payload(self):
        heard: list[tuple[Exception, bytes]] = []
        out = _collect_retained(
            _FakeSession([_FakeErrorReply(b'Timeout')]),
            [_FakeTemplate()],
            type('Unit', (), {}),
            lambda exc, raw: heard.append((exc, raw)),
        )
        assert out == []
        ((exc, raw),) = heard
        assert isinstance(exc, RetainedFetchError)
        assert exc.__cause__ is None
        assert raw == b'Timeout'
        assert str(exc) == "Unit: error reply to retained fetch 'fake/*': 'Timeout'"

    def test_without_on_error_it_is_logged(self, caplog):
        with caplog.at_level(logging.WARNING, logger='zeared.subscriber'):
            _collect_retained(_FakeSession([_FakeErrorReply(b'nope')]), [_FakeTemplate()], type('Unit', (), {}), None)
        assert "Unit: error reply to retained fetch 'fake/*': 'nope'" in caplog.text

    def test_an_error_reply_without_a_payload_reads_as_empty(self):
        heard: list[bytes] = []
        _route_error_reply(_FakeErrorReply(None), type('Unit', (), {}), 'fake/*', lambda exc, raw: heard.append(raw))
        assert heard == [b'']

    def test_each_error_reply_is_heard_in_order(self):
        heard: list[bytes] = []
        _collect_retained(
            _FakeSession([_FakeErrorReply(b'one'), _FakeErrorReply(b'two')]),
            [_FakeTemplate()],
            type('Unit', (), {}),
            lambda exc, raw: heard.append(raw),
        )
        assert heard == [b'one', b'two']
