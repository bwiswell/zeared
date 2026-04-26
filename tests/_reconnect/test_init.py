"""Smoke tests for ``zeared/_reconnect/__init__.py`` — the namespace
re-exports for the reconnect orchestration Pattern B subdir."""
from __future__ import annotations

from zeared._reconnect import (
    _ReconnectAborted,
    _open_with_backoff,
    _probe_loop,
    _reconnect,
    _reconnect_worker,
    _restore_retention,
    _restore_subscribers,
    _restore_wills,
    _trigger_reconnect,
    start_probe,
)


class TestReExports:
    def test_orchestration_callables(self):
        assert callable(start_probe)
        assert callable(_probe_loop)
        assert callable(_reconnect_worker)
        assert callable(_trigger_reconnect)
        assert callable(_reconnect)

    def test_restore_callables(self):
        assert callable(_open_with_backoff)
        assert callable(_restore_retention)
        assert callable(_restore_subscribers)
        assert callable(_restore_wills)

    def test_reconnect_aborted_is_exception(self):
        assert issubclass(_ReconnectAborted, Exception)
