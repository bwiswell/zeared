"""Smoke tests for ``zeared/_managed_session/__init__.py`` — the
namespace re-exports for the reconnect-aware session wrapper Pattern B
subdir."""
from __future__ import annotations

from zeared._managed_session import (
    _DECLARE_HANDLE_WARNING,
    _DEFAULT_PROBE_INTERVAL,
    _is_dead,
    _managed_sessions,
    _warn_declare_handle,
    ManagedSession,
    OnReconnectHandle,
    resolve_raw,
)


class TestReExports:
    def test_classes(self):
        assert ManagedSession is not None
        assert OnReconnectHandle is not None

    def test_helpers_callable(self):
        assert callable(_is_dead)
        assert callable(resolve_raw)
        assert callable(_warn_declare_handle)

    def test_constants(self):
        assert isinstance(_DECLARE_HANDLE_WARNING, str)
        assert isinstance(_DEFAULT_PROBE_INTERVAL, float)
        assert _DEFAULT_PROBE_INTERVAL == 10.0

    def test_managed_sessions_weakset(self):
        # WeakSet — len() works on it.
        assert hasattr(_managed_sessions, '__contains__')
