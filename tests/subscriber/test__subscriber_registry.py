"""Smoke tests for ``zeared/subscriber/_subscriber_registry.py`` — the
module-level subscriber registry walked by ``z.release(session=)``."""
from __future__ import annotations

from zeared.subscriber._subscriber_registry import (
    _SCHEMA_MISMATCH_CACHE_MAX,
    _close_subscribers_for,
    _deregister_subscriber,
    _register_subscriber,
    _subscribers,
    _subscribers_lock,
)


class TestPublicSurface:
    def test_constants(self):
        assert _SCHEMA_MISMATCH_CACHE_MAX == 1024

    def test_registry(self):
        assert isinstance(_subscribers, dict)
        assert hasattr(_subscribers_lock, 'acquire')

    def test_helpers_callable(self):
        assert callable(_register_subscriber)
        assert callable(_deregister_subscriber)
        assert callable(_close_subscribers_for)


class TestRegistryHelpers:
    def test_close_subscribers_no_session_key_no_op(self):
        # Calling against a session id that's not registered is a no-op.
        class _Sentinel:
            pass
        _close_subscribers_for(_Sentinel())

    def test_deregister_with_none_session_no_op(self):
        # Deregistering against ``None`` short-circuits.
        class _Sentinel:
            pass
        _deregister_subscriber(None, _Sentinel())
