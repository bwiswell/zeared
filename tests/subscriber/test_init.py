"""Smoke tests for ``zeared/subscriber/__init__.py`` — verifies the
public surface of the namespace re-exports."""
from __future__ import annotations

from zeared.subscriber import (
    M,
    _SCHEMA_MISMATCH_CACHE_MAX,
    _adapt_async_callback,
    _build_dispatch,
    _close_subscribers_for,
    _deregister_subscriber,
    _fetch_retained,
    _make_presence_dispatcher,
    _pick_encoding,
    _register_subscriber,
    _subscribers,
    _subscribers_lock,
    _wants_meta,
    Subscriber,
)


class TestReExports:
    def test_subscriber_class(self):
        assert Subscriber is not None
        assert hasattr(Subscriber, '__slots__')

    def test_type_var(self):
        assert M is not None

    def test_schema_cache_max_constant(self):
        assert _SCHEMA_MISMATCH_CACHE_MAX == 1024

    def test_registry(self):
        assert isinstance(_subscribers, dict)
        assert hasattr(_subscribers_lock, 'acquire')

    def test_helpers_callable(self):
        assert callable(_register_subscriber)
        assert callable(_deregister_subscriber)
        assert callable(_close_subscribers_for)
        assert callable(_wants_meta)
        assert callable(_adapt_async_callback)
        assert callable(_make_presence_dispatcher)
        assert callable(_pick_encoding)
        assert callable(_build_dispatch)
        assert callable(_fetch_retained)
