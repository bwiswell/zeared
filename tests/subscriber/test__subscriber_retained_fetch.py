"""Smoke tests for ``zeared/subscriber/_subscriber_retained_fetch.py``
— the helper that issues ``session.get(wildcard)`` per declared
template and routes reply samples through the subscriber's dispatch
path.

End-to-end retained-fetch behaviour is covered by ``test_retention.py``
and ``test_subscriber.py``; this file confirms the module's public
surface.
"""
from __future__ import annotations

from zeared.subscriber._subscriber_retained_fetch import _fetch_retained


class TestPublicSurface:
    def test_callable(self):
        assert callable(_fetch_retained)
